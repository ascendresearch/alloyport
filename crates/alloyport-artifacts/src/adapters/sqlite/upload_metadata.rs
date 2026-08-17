//! `SQLite` implementation of Artifact metadata, authorization, and durable references.

use super::upload_quota::{artifact_size, enforce_quota, owner_reserved_bytes, owner_stored_bytes};
use super::upload_records::{session_by_key, to_i64};
use super::upload_references::{
    garbage_collection_pending, has_active_owner_reference, has_other_active_owner_reference,
    insert_reference, reference_by_key, reference_matches_grant, validate_reference_grant,
    validate_reference_identity,
};
use super::upload_store::SqliteUploadStore;
use crate::ArtifactIdentity;
use crate::Sha256Digest;
use crate::upload::{
    ArtifactMetadataStore, ArtifactReference, GrantArtifactReference, QuotaScope, UploadError,
    UploadSession, UploadState,
};
use rusqlite::{TransactionBehavior, params};

#[allow(clippy::missing_errors_doc)]
impl SqliteUploadStore {
    /// Returns the finalized identity for one owner-scoped idempotency key.
    pub fn completed_upload_by_key(
        &self,
        owner_id: &str,
        upload_key: &str,
    ) -> Result<Option<crate::ArtifactIdentity>, UploadError> {
        self.completed_upload_session_by_key(owner_id, upload_key)
            .map(|session| session.and_then(|session| session.artifact))
    }

    pub fn completed_upload_session_by_key(
        &self,
        owner_id: &str,
        upload_key: &str,
    ) -> Result<Option<UploadSession>, UploadError> {
        let database = self.connection()?;
        Ok(session_by_key(&database, owner_id, upload_key)?
            .filter(|session| session.state == UploadState::Completed))
    }

    pub fn owns_completed_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
    ) -> Result<bool, UploadError> {
        let database = self.connection()?;
        let found = database.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM artifact_owner_references
                WHERE owner_id = ?1 AND digest = ?2
            )",
            params![owner_id, digest.to_string()],
            |row| row.get(0),
        )?;
        Ok(found)
    }

    pub fn can_read_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
    ) -> Result<bool, UploadError> {
        self.owns_completed_artifact(owner_id, digest)
    }

    pub fn artifact_size_bytes(&self, digest: Sha256Digest) -> Result<Option<u64>, UploadError> {
        let database = self.connection()?;
        artifact_size(&database, digest)
    }

    /// Records a CAS object the controller wrote itself, under the same quotas as an upload.
    pub fn record_local_artifact(
        &self,
        owner_id: &str,
        artifact: ArtifactIdentity,
    ) -> Result<(), UploadError> {
        if owner_id.trim().is_empty() {
            return Err(UploadError::Corrupt(
                "local artifact owner is empty".to_owned(),
            ));
        }
        let now_ms = super::upload_records::now_ms()?;
        let _artifact_guard = self.artifact_guard()?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if super::upload_quota::artifact_size(&transaction, artifact.digest)?.is_none() {
            super::upload_quota::enforce_local_artifact_quota(
                &transaction,
                owner_id,
                artifact.size_bytes,
                self.quotas,
                now_ms,
            )?;
        }
        super::upload_records::record_local_artifact(&transaction, owner_id, artifact, now_ms)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn grant_reference(
        &self,
        request: &GrantArtifactReference,
    ) -> Result<ArtifactReference, UploadError> {
        validate_reference_grant(request)?;
        let _artifact_guard = self.artifact_guard()?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            reference_by_key(&transaction, &request.owner_id, &request.reference_key)?
        {
            if reference_matches_grant(&existing, request) {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(if existing.revoked_at_ms.is_some() {
                UploadError::ReferenceRevoked
            } else {
                UploadError::ConflictingReferenceKey
            });
        }
        if garbage_collection_pending(&transaction, request.digest)? {
            return Err(UploadError::GarbageCollectionPending(request.digest));
        }
        let size_bytes = artifact_size(&transaction, request.digest)?
            .ok_or_else(|| UploadError::NotFound(request.digest.to_string()))?;
        let creates_owner_usage =
            !has_active_owner_reference(&transaction, &request.owner_id, request.digest)?;
        if creates_owner_usage {
            let used = owner_stored_bytes(&transaction, &request.owner_id)?.saturating_add(
                owner_reserved_bytes(&transaction, &request.owner_id, request.now_ms)?,
            );
            enforce_quota(
                QuotaScope::Owner,
                self.quotas.per_owner_bytes,
                used,
                size_bytes,
            )?;
        }
        insert_reference(&transaction, request, request.kind)?;
        if creates_owner_usage {
            transaction.execute(
                "INSERT INTO artifact_owner_references(owner_id, digest, size_bytes)
                 VALUES (?1, ?2, ?3)",
                params![
                    request.owner_id,
                    request.digest.to_string(),
                    to_i64(size_bytes)?
                ],
            )?;
        }
        let reference = reference_by_key(&transaction, &request.owner_id, &request.reference_key)?
            .ok_or_else(|| {
                UploadError::Corrupt("inserted artifact reference disappeared".into())
            })?;
        transaction.commit()?;
        Ok(reference)
    }

    pub fn revoke_reference(
        &self,
        owner_id: &str,
        reference_key: &str,
        now_ms: u64,
    ) -> Result<ArtifactReference, UploadError> {
        validate_reference_identity(owner_id, reference_key)?;
        let _artifact_guard = self.artifact_guard()?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let reference = reference_by_key(&transaction, owner_id, reference_key)?
            .ok_or_else(|| UploadError::NotFound(reference_key.to_owned()))?;
        if reference.revoked_at_ms.is_some() {
            transaction.commit()?;
            return Ok(reference);
        }
        transaction.execute(
            "UPDATE artifact_references SET revoked_at_ms = ?3
             WHERE owner_id = ?1 AND reference_key = ?2",
            params![owner_id, reference_key, to_i64(now_ms)?],
        )?;
        if !has_other_active_owner_reference(
            &transaction,
            owner_id,
            reference.digest,
            reference_key,
        )? {
            transaction.execute(
                "DELETE FROM artifact_owner_references WHERE owner_id = ?1 AND digest = ?2",
                params![owner_id, reference.digest.to_string()],
            )?;
        }
        let revoked = reference_by_key(&transaction, owner_id, reference_key)?
            .ok_or_else(|| UploadError::Corrupt("revoked artifact reference disappeared".into()))?;
        transaction.commit()?;
        Ok(revoked)
    }

    pub fn reference(
        &self,
        owner_id: &str,
        reference_key: &str,
    ) -> Result<ArtifactReference, UploadError> {
        let database = self.connection()?;
        reference_by_key(&database, owner_id, reference_key)?
            .ok_or_else(|| UploadError::NotFound(reference_key.to_owned()))
    }
}

impl ArtifactMetadataStore for SqliteUploadStore {
    fn completed_upload_session_by_key(
        &self,
        owner_id: &str,
        upload_key: &str,
    ) -> Result<Option<UploadSession>, UploadError> {
        Self::completed_upload_session_by_key(self, owner_id, upload_key)
    }

    fn can_read_artifact(&self, owner_id: &str, digest: Sha256Digest) -> Result<bool, UploadError> {
        Self::can_read_artifact(self, owner_id, digest)
    }

    fn artifact_size_bytes(&self, digest: Sha256Digest) -> Result<Option<u64>, UploadError> {
        Self::artifact_size_bytes(self, digest)
    }

    fn record_local_artifact(
        &self,
        owner_id: &str,
        artifact: ArtifactIdentity,
    ) -> Result<(), UploadError> {
        Self::record_local_artifact(self, owner_id, artifact)
    }

    fn grant_reference(
        &self,
        request: &GrantArtifactReference,
    ) -> Result<ArtifactReference, UploadError> {
        Self::grant_reference(self, request)
    }
}
