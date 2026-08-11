//! `SQLite` and filesystem implementation of Artifact upload metadata and staging.

use super::upload_gc::{artifact_is_reachable, pending_garbage, stage_garbage_candidates};
use super::upload_quota::{
    artifact_size, enforce_quota, owner_reserved_bytes, owner_stored_bytes, reserve_quota,
    validate_quotas,
};
use super::upload_records::{
    append_file, authorize, ensure_open, is_terminal_artifact_error, record_completed_artifact,
    remove_if_present, session_by_id, session_by_key, to_i64, unique_seed, validate_begin,
};
use super::upload_references::{
    garbage_collection_pending, has_active_owner_reference, has_other_active_owner_reference,
    insert_reference, reference_by_key, reference_matches_grant, validate_reference_grant,
    validate_reference_identity,
};
use super::upload_schema::{SCHEMA, migrate_quota_schema, migrate_reference_schema};
use crate::upload::{
    ArtifactReference, BeginUpload, GarbageCollectionReport, GrantArtifactReference, QuotaScope,
    UploadError, UploadQuotas, UploadSession, UploadState,
};
use crate::{
    ArtifactIdentity, ArtifactReader, ArtifactStore, FilesystemArtifactStore, IngestRequest,
    Sha256Digest,
};
use rusqlite::{Connection, TransactionBehavior, params};
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

impl From<rusqlite::Error> for UploadError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

struct LeasedArtifactReader {
    reader: ArtifactReader,
    digest: Sha256Digest,
    active_readers: Arc<Mutex<BTreeMap<Sha256Digest, u64>>>,
}

impl Read for LeasedArtifactReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buffer)
    }
}

impl Drop for LeasedArtifactReader {
    fn drop(&mut self) {
        let Ok(mut readers) = self.active_readers.lock() else {
            return;
        };
        let Some(count) = readers.get_mut(&self.digest) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            readers.remove(&self.digest);
        }
    }
}

pub struct SqliteUploadStore {
    connection: Mutex<Connection>,
    finalize_lock: Mutex<()>,
    active_readers: Arc<Mutex<BTreeMap<Sha256Digest, u64>>>,
    upload_root: PathBuf,
    max_upload_bytes: u64,
    max_chunk_bytes: usize,
    quotas: UploadQuotas,
    counter: AtomicU64,
}

impl Debug for SqliteUploadStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteUploadStore")
            .field("upload_root", &self.upload_root)
            .field("max_upload_bytes", &self.max_upload_bytes)
            .field("max_chunk_bytes", &self.max_chunk_bytes)
            .field("quotas", &self.quotas)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::missing_errors_doc)]
impl SqliteUploadStore {
    pub fn open(
        database: impl AsRef<Path>,
        upload_root: impl AsRef<Path>,
        max_upload_bytes: u64,
        max_chunk_bytes: usize,
    ) -> Result<Self, UploadError> {
        Self::open_with_quotas(
            database,
            upload_root,
            max_upload_bytes,
            max_chunk_bytes,
            UploadQuotas::unbounded(),
        )
    }

    pub fn open_with_quotas(
        database: impl AsRef<Path>,
        upload_root: impl AsRef<Path>,
        max_upload_bytes: u64,
        max_chunk_bytes: usize,
        quotas: UploadQuotas,
    ) -> Result<Self, UploadError> {
        if max_chunk_bytes == 0 {
            return Err(UploadError::InvalidRequest("chunk limit must be positive"));
        }
        validate_quotas(quotas)?;
        fs::create_dir_all(upload_root.as_ref()).map_err(|source| UploadError::Io {
            operation: "create upload data directory",
            source,
        })?;
        let mut connection = Connection::open(database)?;
        connection.execute_batch(SCHEMA)?;
        migrate_quota_schema(&mut connection)?;
        migrate_reference_schema(&mut connection)?;
        let store = Self {
            connection: Mutex::new(connection),
            finalize_lock: Mutex::new(()),
            active_readers: Arc::new(Mutex::new(BTreeMap::new())),
            upload_root: upload_root.as_ref().to_path_buf(),
            max_upload_bytes,
            max_chunk_bytes,
            quotas,
            counter: AtomicU64::new(unique_seed()),
        };
        store.cleanup_completed_data()?;
        Ok(store)
    }

    pub fn begin(&self, request: &BeginUpload) -> Result<UploadSession, UploadError> {
        validate_begin(request, self.max_upload_bytes)?;
        let _artifact_guard = self.artifact_guard()?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            session_by_key(&transaction, &request.owner_id, &request.upload_key)?
        {
            if existing.expected_digest != request.expected_digest
                || existing.expected_size_bytes != request.expected_size_bytes
                || existing.media_type != request.media_type
            {
                return Err(UploadError::ConflictingUploadKey);
            }
            transaction.commit()?;
            return Ok(existing);
        }
        reserve_quota(&transaction, request, self.quotas)?;
        transaction.execute(
            "DELETE FROM artifact_gc_pending WHERE digest = ?1",
            [request.expected_digest.to_string()],
        )?;
        let upload_id = self.next_upload_id(&transaction)?;
        transaction.execute(
            "INSERT INTO upload_sessions(upload_id, owner_id, upload_key, expected_digest,
                 expected_size_bytes, media_type, committed_offset, state, created_at_ms,
                 updated_at_ms, expires_at_ms, quota_reserved_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?8, ?9, ?5)",
            params![
                upload_id,
                request.owner_id,
                request.upload_key,
                request.expected_digest.to_string(),
                to_i64(request.expected_size_bytes)?,
                request.media_type,
                UploadState::Open as i64,
                to_i64(request.now_ms)?,
                to_i64(request.expires_at_ms)?
            ],
        )?;
        let session = session_by_id(&transaction, &upload_id)?
            .ok_or_else(|| UploadError::Corrupt("inserted upload disappeared".to_owned()))?;
        transaction.commit()?;
        Ok(session)
    }

    pub fn status(&self, owner_id: &str, upload_id: &str) -> Result<UploadSession, UploadError> {
        let database = self.connection()?;
        let session = session_by_id(&database, upload_id)?
            .ok_or_else(|| UploadError::NotFound(upload_id.to_owned()))?;
        authorize(&session, owner_id)?;
        Ok(session)
    }

    /// Returns the finalized identity for one owner-scoped idempotency key.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata database cannot be read or contains invalid values.
    pub fn completed_upload_by_key(
        &self,
        owner_id: &str,
        upload_key: &str,
    ) -> Result<Option<ArtifactIdentity>, UploadError> {
        self.completed_upload_session_by_key(owner_id, upload_key)
            .map(|session| session.and_then(|session| session.artifact))
    }

    /// Returns a completed owner-scoped upload session by its stable idempotency key.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata database cannot be read or contains invalid values.
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

    /// Returns the durable byte size of a managed Artifact, if present.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be read or contains an invalid stored size.
    pub fn artifact_size_bytes(&self, digest: Sha256Digest) -> Result<Option<u64>, UploadError> {
        let database = self.connection()?;
        artifact_size(&database, digest)
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

    pub fn open_referenced_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
        artifacts: &FilesystemArtifactStore,
    ) -> Result<ArtifactReader, UploadError> {
        let mut readers = self
            .active_readers
            .lock()
            .map_err(|_| UploadError::Corrupt("artifact reader lock poisoned".into()))?;
        if !self.can_read_artifact(owner_id, digest)? {
            return Err(UploadError::OwnerMismatch);
        }
        let reader = artifacts.open(digest)?;
        let count = readers.entry(digest).or_default();
        *count = count.saturating_add(1);
        let identity = reader.identity();
        Ok(ArtifactReader::new(
            identity,
            LeasedArtifactReader {
                reader,
                digest,
                active_readers: Arc::clone(&self.active_readers),
            },
        ))
    }

    pub fn collect_garbage(
        &self,
        artifacts: &FilesystemArtifactStore,
        now_ms: u64,
        limit: usize,
    ) -> Result<GarbageCollectionReport, UploadError> {
        if limit == 0 {
            return Err(UploadError::InvalidRequest(
                "garbage collection limit must be positive",
            ));
        }
        let _artifact_guard = self.artifact_guard()?;
        let readers = self
            .active_readers
            .lock()
            .map_err(|_| UploadError::Corrupt("artifact reader lock poisoned".into()))?;
        let mut database = self.connection()?;
        stage_garbage_candidates(&mut database, now_ms, limit)?;
        let candidates = pending_garbage(&database, limit)?;
        let mut report = GarbageCollectionReport::default();
        for (digest, size_bytes) in candidates {
            if readers.get(&digest).copied().unwrap_or_default() != 0 {
                database.execute(
                    "DELETE FROM artifact_gc_pending WHERE digest = ?1",
                    [digest.to_string()],
                )?;
                report.skipped_active_readers = report.skipped_active_readers.saturating_add(1);
                continue;
            }
            let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if artifact_is_reachable(&transaction, digest, now_ms)? {
                transaction.execute(
                    "DELETE FROM artifact_gc_pending WHERE digest = ?1",
                    [digest.to_string()],
                )?;
                transaction.commit()?;
                continue;
            }
            artifacts.remove_unreachable(digest)?;
            transaction.execute(
                "DELETE FROM artifact_owner_references WHERE digest = ?1",
                [digest.to_string()],
            )?;
            transaction.execute(
                "DELETE FROM artifact_objects WHERE digest = ?1",
                [digest.to_string()],
            )?;
            transaction.execute(
                "DELETE FROM artifact_gc_pending WHERE digest = ?1",
                [digest.to_string()],
            )?;
            transaction.commit()?;
            report.collected_objects = report.collected_objects.saturating_add(1);
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(size_bytes);
        }
        Ok(report)
    }

    pub fn append(
        &self,
        owner_id: &str,
        upload_id: &str,
        offset: u64,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<u64, UploadError> {
        if bytes.len() > self.max_chunk_bytes {
            return Err(UploadError::ChunkTooLarge {
                limit: self.max_chunk_bytes,
                received: bytes.len(),
            });
        }
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = session_by_id(&transaction, upload_id)?
            .ok_or_else(|| UploadError::NotFound(upload_id.to_owned()))?;
        authorize(&session, owner_id)?;
        ensure_open(&session, now_ms)?;
        if offset != session.committed_offset {
            return Err(UploadError::OffsetConflict {
                expected: session.committed_offset,
                received: offset,
            });
        }
        let next = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if next > session.expected_size_bytes || next > self.max_upload_bytes {
            return Err(UploadError::SizeLimitExceeded {
                limit: session.expected_size_bytes.min(self.max_upload_bytes),
                attempted: next,
            });
        }
        let path = self.data_path(upload_id)?;
        append_file(&path, offset, bytes)?;
        transaction.execute(
            "UPDATE upload_sessions SET committed_offset = ?2, updated_at_ms = ?3
             WHERE upload_id = ?1",
            params![upload_id, to_i64(next)?, to_i64(now_ms)?],
        )?;
        transaction.commit()?;
        Ok(next)
    }

    pub fn finalize(
        &self,
        owner_id: &str,
        upload_id: &str,
        artifact_store: &dyn ArtifactStore,
        now_ms: u64,
    ) -> Result<ArtifactIdentity, UploadError> {
        let _finalize_guard = self
            .finalize_lock
            .lock()
            .map_err(|_| UploadError::Corrupt("finalize lock poisoned".to_owned()))?;
        let session = {
            let mut database = self.connection()?;
            let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let session = session_by_id(&transaction, upload_id)?
                .ok_or_else(|| UploadError::NotFound(upload_id.to_owned()))?;
            authorize(&session, owner_id)?;
            if session.state == UploadState::Completed {
                return session.artifact.ok_or_else(|| {
                    UploadError::Corrupt("completed upload lacks artifact".to_owned())
                });
            }
            if !matches!(session.state, UploadState::Open | UploadState::Finalizing) {
                return Err(UploadError::InvalidState(session.state));
            }
            if now_ms >= session.expires_at_ms {
                return Err(UploadError::Expired);
            }
            if session.committed_offset != session.expected_size_bytes {
                return Err(UploadError::Incomplete {
                    expected: session.expected_size_bytes,
                    committed: session.committed_offset,
                });
            }
            transaction.execute(
                "UPDATE upload_sessions SET state = ?2, updated_at_ms = ?3 WHERE upload_id = ?1",
                params![upload_id, UploadState::Finalizing as i64, to_i64(now_ms)?],
            )?;
            transaction.commit()?;
            session
        };
        let path = self.data_path(upload_id)?;
        let mut source: Box<dyn Read> = match File::open(&path) {
            Ok(file) => Box::new(file),
            Err(error)
                if error.kind() == io::ErrorKind::NotFound && session.expected_size_bytes == 0 =>
            {
                Box::new(io::empty())
            }
            Err(source) => {
                return Err(UploadError::Io {
                    operation: "open upload data for finalization",
                    source,
                });
            }
        };
        let artifact = match artifact_store.ingest(
            source.as_mut(),
            IngestRequest {
                expected_digest: Some(session.expected_digest),
                expected_size_bytes: Some(session.expected_size_bytes),
            },
        ) {
            Ok(result) => result.artifact,
            Err(error) => {
                if is_terminal_artifact_error(&error) {
                    self.mark_failed(upload_id, now_ms)?;
                }
                return Err(error.into());
            }
        };
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        record_completed_artifact(&transaction, &session.owner_id, upload_id, artifact, now_ms)?;
        transaction.execute(
            "UPDATE upload_sessions
             SET state = ?2, artifact_digest = ?3, updated_at_ms = ?4, quota_reserved_bytes = 0
             WHERE upload_id = ?1",
            params![
                upload_id,
                UploadState::Completed as i64,
                artifact.digest.to_string(),
                to_i64(now_ms)?
            ],
        )?;
        transaction.commit()?;
        remove_if_present(&path)?;
        Ok(artifact)
    }

    pub fn prune_expired(&self, now_ms: u64) -> Result<usize, UploadError> {
        let _artifact_guard = self.artifact_guard()?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ids = {
            let mut statement = transaction.prepare(
                "SELECT upload_id FROM upload_sessions WHERE state != ?1 AND expires_at_ms <= ?2",
            )?;
            statement
                .query_map(
                    params![UploadState::Completed as i64, to_i64(now_ms)?],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        for id in &ids {
            remove_if_present(&self.data_path(id)?)?;
        }
        for id in &ids {
            transaction.execute("DELETE FROM upload_sessions WHERE upload_id = ?1", [id])?;
        }
        transaction.commit()?;
        Ok(ids.len())
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, UploadError> {
        self.connection
            .lock()
            .map_err(|_| UploadError::Corrupt("database lock poisoned".to_owned()))
    }

    fn artifact_guard(&self) -> Result<std::sync::MutexGuard<'_, ()>, UploadError> {
        self.finalize_lock
            .lock()
            .map_err(|_| UploadError::Corrupt("artifact lifecycle lock poisoned".into()))
    }

    fn next_upload_id(
        &self,
        transaction: &rusqlite::Transaction<'_>,
    ) -> Result<String, UploadError> {
        for _ in 0..32 {
            let id = format!("upload-{}", self.counter.fetch_add(1, Ordering::Relaxed));
            if session_by_id(transaction, &id)?.is_none() {
                return Ok(id);
            }
        }
        Err(UploadError::Corrupt(
            "unable to allocate upload ID".to_owned(),
        ))
    }

    fn data_path(&self, upload_id: &str) -> Result<PathBuf, UploadError> {
        if upload_id.is_empty()
            || !upload_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(UploadError::Corrupt("unsafe upload ID".to_owned()));
        }
        Ok(self.upload_root.join(upload_id))
    }

    fn mark_failed(&self, upload_id: &str, now_ms: u64) -> Result<(), UploadError> {
        self.connection()?.execute(
            "UPDATE upload_sessions
             SET state = ?2, updated_at_ms = ?3, quota_reserved_bytes = 0
             WHERE upload_id = ?1",
            params![upload_id, UploadState::Failed as i64, to_i64(now_ms)?],
        )?;
        Ok(())
    }

    fn cleanup_completed_data(&self) -> Result<(), UploadError> {
        let database = self.connection()?;
        let mut statement =
            database.prepare("SELECT upload_id FROM upload_sessions WHERE state = ?1")?;
        let ids = statement
            .query_map([UploadState::Completed as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(database);
        for id in ids {
            remove_if_present(&self.data_path(&id)?)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "upload_store_tests.rs"]
mod tests;
