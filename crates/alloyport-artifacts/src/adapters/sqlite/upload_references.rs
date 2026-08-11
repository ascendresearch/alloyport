//! Durable Artifact reference validation and lookup operations.

use super::upload_records::{from_i64, to_i64};
use crate::Sha256Digest;
use crate::upload::{
    ArtifactReference, ArtifactReferenceKind, GrantArtifactReference, UploadError,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::str::FromStr;

pub(super) fn garbage_collection_pending(
    connection: &Connection,
    digest: Sha256Digest,
) -> Result<bool, UploadError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM artifact_gc_pending WHERE digest = ?1)",
        [digest.to_string()],
        |row| row.get(0),
    )?)
}

pub(super) fn has_active_owner_reference(
    connection: &Connection,
    owner_id: &str,
    digest: Sha256Digest,
) -> Result<bool, UploadError> {
    Ok(connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM artifact_references
            WHERE owner_id = ?1 AND digest = ?2 AND revoked_at_ms IS NULL
        )",
        params![owner_id, digest.to_string()],
        |row| row.get(0),
    )?)
}

pub(super) fn has_other_active_owner_reference(
    connection: &Connection,
    owner_id: &str,
    digest: Sha256Digest,
    excluded_key: &str,
) -> Result<bool, UploadError> {
    Ok(connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM artifact_references
            WHERE owner_id = ?1 AND digest = ?2 AND reference_key != ?3
              AND revoked_at_ms IS NULL
        )",
        params![owner_id, digest.to_string(), excluded_key],
        |row| row.get(0),
    )?)
}

pub(super) fn validate_reference_identity(
    owner_id: &str,
    reference_key: &str,
) -> Result<(), UploadError> {
    if owner_id.trim().is_empty() {
        return Err(UploadError::InvalidRequest("reference owner is missing"));
    }
    if reference_key.trim().is_empty() {
        return Err(UploadError::InvalidRequest("reference key is missing"));
    }
    Ok(())
}

pub(super) fn validate_reference_grant(
    request: &GrantArtifactReference,
) -> Result<(), UploadError> {
    validate_reference_identity(&request.owner_id, &request.reference_key)?;
    if request.kind == ArtifactReferenceKind::Upload {
        return Err(UploadError::InvalidRequest(
            "upload references are created only by finalization",
        ));
    }
    if request.purpose.trim().is_empty() {
        return Err(UploadError::InvalidRequest("reference purpose is missing"));
    }
    if request
        .retained_until_ms
        .is_some_and(|retained_until_ms| retained_until_ms <= request.now_ms)
    {
        return Err(UploadError::InvalidRequest(
            "reference retention must end in the future",
        ));
    }
    Ok(())
}

pub(super) fn reference_matches_grant(
    reference: &ArtifactReference,
    request: &GrantArtifactReference,
) -> bool {
    reference.digest == request.digest
        && reference.kind == request.kind
        && reference.purpose == request.purpose
        && reference.retained_until_ms == request.retained_until_ms
        && reference.revoked_at_ms.is_none()
}

pub(super) fn insert_reference(
    transaction: &rusqlite::Transaction<'_>,
    request: &GrantArtifactReference,
    kind: ArtifactReferenceKind,
) -> Result<(), UploadError> {
    transaction.execute(
        "INSERT INTO artifact_references(
            owner_id, reference_key, digest, kind, purpose, created_at_ms,
            retained_until_ms, revoked_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
        params![
            request.owner_id,
            request.reference_key,
            request.digest.to_string(),
            kind as i64,
            request.purpose,
            to_i64(request.now_ms)?,
            request.retained_until_ms.map(to_i64).transpose()?
        ],
    )?;
    Ok(())
}

pub(super) fn reference_by_key(
    connection: &Connection,
    owner_id: &str,
    reference_key: &str,
) -> Result<Option<ArtifactReference>, UploadError> {
    let row = connection
        .query_row(
            "SELECT owner_id, reference_key, digest, kind, purpose, created_at_ms,
                    retained_until_ms, revoked_at_ms
             FROM artifact_references WHERE owner_id = ?1 AND reference_key = ?2",
            params![owner_id, reference_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| {
        Ok(ArtifactReference {
            owner_id: row.0,
            reference_key: row.1,
            digest: Sha256Digest::from_str(&row.2)
                .map_err(|error| UploadError::Corrupt(error.to_string()))?,
            kind: ArtifactReferenceKind::from_i64(row.3)?,
            purpose: row.4,
            created_at_ms: from_i64(row.5)?,
            retained_until_ms: row.6.map(from_i64).transpose()?,
            revoked_at_ms: row.7.map(from_i64).transpose()?,
        })
    })
    .transpose()
}
