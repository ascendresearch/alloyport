//! Upload-session records, value conversion, and staging-file helpers.

use crate::upload::{ArtifactReferenceKind, BeginUpload, UploadError, UploadSession, UploadState};
use crate::{ArtifactIdentity, ArtifactStoreError, Sha256Digest};
use rusqlite::{Connection, OptionalExtension, params};
use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// Records an object the controller minted, with a retention reference of its own.
///
/// Mirrors `record_completed_artifact` except for provenance: there is no upload session, and the
/// reference is a retention root rather than an upload, so conservative GC keeps it until the
/// assignment that needs it has been granted its own reference.
pub(super) fn record_local_artifact(
    transaction: &rusqlite::Transaction<'_>,
    owner_id: &str,
    artifact: ArtifactIdentity,
    now_ms: u64,
) -> Result<(), UploadError> {
    let digest = artifact.digest.to_string();
    let size = to_i64(artifact.size_bytes)?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_objects(digest, size_bytes) VALUES (?1, ?2)",
        params![digest, size],
    )?;
    let stored_size = transaction.query_row(
        "SELECT size_bytes FROM artifact_objects WHERE digest = ?1",
        [&digest],
        |row| row.get::<_, i64>(0),
    )?;
    if stored_size != size {
        return Err(UploadError::Corrupt(format!(
            "artifact {digest} has conflicting recorded sizes"
        )));
    }
    transaction.execute(
        "DELETE FROM artifact_gc_pending WHERE digest = ?1",
        [&digest],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_references(
            owner_id, reference_key, digest, kind, purpose, created_at_ms,
            retained_until_ms, revoked_at_ms
         ) VALUES (?1, ?2, ?3, ?4, 'controller artifact', ?5, NULL, NULL)",
        params![
            owner_id,
            format!("controller:{digest}"),
            digest,
            ArtifactReferenceKind::RetentionRoot as i64,
            to_i64(now_ms)?
        ],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_owner_references(owner_id, digest, size_bytes)
         VALUES (?1, ?2, ?3)",
        params![owner_id, digest, size],
    )?;
    Ok(())
}

pub(super) fn now_ms() -> Result<u64, UploadError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .map_err(|error| UploadError::Corrupt(error.to_string()))
}

pub(super) fn record_completed_artifact(
    transaction: &rusqlite::Transaction<'_>,
    owner_id: &str,
    upload_id: &str,
    artifact: ArtifactIdentity,
    now_ms: u64,
) -> Result<(), UploadError> {
    let digest = artifact.digest.to_string();
    let size = to_i64(artifact.size_bytes)?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_objects(digest, size_bytes) VALUES (?1, ?2)",
        params![digest, size],
    )?;
    let stored_size = transaction.query_row(
        "SELECT size_bytes FROM artifact_objects WHERE digest = ?1",
        [&digest],
        |row| row.get::<_, i64>(0),
    )?;
    if stored_size != size {
        return Err(UploadError::Corrupt(format!(
            "artifact {digest} has conflicting recorded sizes"
        )));
    }
    transaction.execute(
        "DELETE FROM artifact_gc_pending WHERE digest = ?1",
        [&digest],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_references(
            owner_id, reference_key, digest, kind, purpose, created_at_ms,
            retained_until_ms, revoked_at_ms
         ) VALUES (?1, ?2, ?3, ?4, 'completed upload', ?5, NULL, NULL)",
        params![
            owner_id,
            format!("upload:{upload_id}"),
            digest,
            ArtifactReferenceKind::Upload as i64,
            to_i64(now_ms)?
        ],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_owner_references(owner_id, digest, size_bytes)
         VALUES (?1, ?2, ?3)",
        params![owner_id, digest, size],
    )?;
    let owner_size = transaction.query_row(
        "SELECT size_bytes FROM artifact_owner_references WHERE owner_id = ?1 AND digest = ?2",
        params![owner_id, digest],
        |row| row.get::<_, i64>(0),
    )?;
    if owner_size != size {
        return Err(UploadError::Corrupt(format!(
            "owner {owner_id} has a conflicting size for artifact {digest}"
        )));
    }
    Ok(())
}

pub(super) fn validate_begin(request: &BeginUpload, max: u64) -> Result<(), UploadError> {
    if request.owner_id.trim().is_empty() {
        return Err(UploadError::InvalidRequest("owner is missing"));
    }
    if request.upload_key.trim().is_empty() {
        return Err(UploadError::InvalidRequest("upload key is missing"));
    }
    if request.media_type.trim().is_empty() {
        return Err(UploadError::InvalidRequest("media type is missing"));
    }
    if request.expires_at_ms <= request.now_ms {
        return Err(UploadError::InvalidRequest("expiry must be in the future"));
    }
    if request.expected_size_bytes > max {
        return Err(UploadError::SizeLimitExceeded {
            limit: max,
            attempted: request.expected_size_bytes,
        });
    }
    Ok(())
}

pub(super) const fn is_terminal_artifact_error(error: &ArtifactStoreError) -> bool {
    matches!(
        error,
        ArtifactStoreError::SizeLimitExceeded { .. }
            | ArtifactStoreError::SizeMismatch { .. }
            | ArtifactStoreError::DigestMismatch { .. }
            | ArtifactStoreError::IntegrityViolation { .. }
    )
}

pub(super) fn ensure_open(session: &UploadSession, now_ms: u64) -> Result<(), UploadError> {
    if session.state != UploadState::Open {
        return Err(UploadError::InvalidState(session.state));
    }
    if now_ms >= session.expires_at_ms {
        return Err(UploadError::Expired);
    }
    Ok(())
}

pub(super) fn authorize(session: &UploadSession, owner_id: &str) -> Result<(), UploadError> {
    if session.owner_id == owner_id {
        Ok(())
    } else {
        Err(UploadError::OwnerMismatch)
    }
}

pub(super) fn append_file(path: &Path, committed: u64, bytes: &[u8]) -> Result<(), UploadError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| UploadError::Io {
            operation: "open upload data",
            source,
        })?;
    let length = file
        .metadata()
        .map_err(|source| UploadError::Io {
            operation: "inspect upload data",
            source,
        })?
        .len();
    if length < committed {
        return Err(UploadError::Corrupt(format!(
            "upload data length {length} is behind committed offset {committed}"
        )));
    }
    if length > committed {
        file.set_len(committed).map_err(|source| UploadError::Io {
            operation: "truncate uncommitted upload tail",
            source,
        })?;
    }
    file.seek(SeekFrom::Start(committed))
        .map_err(|source| UploadError::Io {
            operation: "seek upload data",
            source,
        })?;
    file.write_all(bytes).map_err(|source| UploadError::Io {
        operation: "append upload data",
        source,
    })?;
    file.sync_all().map_err(|source| UploadError::Io {
        operation: "sync upload data",
        source,
    })
}

pub(super) fn remove_if_present(path: &Path) -> Result<(), UploadError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(UploadError::Io {
            operation: "remove upload data",
            source,
        }),
    }
}

pub(super) fn session_by_key(
    connection: &Connection,
    owner_id: &str,
    upload_key: &str,
) -> Result<Option<UploadSession>, UploadError> {
    query_session(
        connection,
        "owner_id = ?1 AND upload_key = ?2",
        params![owner_id, upload_key],
    )
}

pub(super) fn session_by_id(
    connection: &Connection,
    upload_id: &str,
) -> Result<Option<UploadSession>, UploadError> {
    query_session(connection, "upload_id = ?1", params![upload_id])
}

fn query_session(
    connection: &Connection,
    predicate: &str,
    parameters: impl rusqlite::Params,
) -> Result<Option<UploadSession>, UploadError> {
    let sql = format!(
        "SELECT upload_id, owner_id, upload_key, expected_digest,
        expected_size_bytes, media_type, committed_offset, state, expires_at_ms, artifact_digest
        FROM upload_sessions WHERE {predicate}"
    );
    let row = connection
        .query_row(&sql, parameters, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })
        .optional()?;
    row.map(|row| {
        let expected_digest = Sha256Digest::from_str(&row.3)
            .map_err(|error| UploadError::Corrupt(error.to_string()))?;
        let expected_size_bytes = from_i64(row.4)?;
        let artifact = row
            .9
            .map(|digest| {
                Sha256Digest::from_str(&digest).map(|digest| ArtifactIdentity {
                    digest,
                    size_bytes: expected_size_bytes,
                })
            })
            .transpose()
            .map_err(|error| UploadError::Corrupt(error.to_string()))?;
        Ok(UploadSession {
            upload_id: row.0,
            owner_id: row.1,
            upload_key: row.2,
            expected_digest,
            expected_size_bytes,
            media_type: row.5,
            committed_offset: from_i64(row.6)?,
            state: UploadState::from_i64(row.7)?,
            expires_at_ms: from_i64(row.8)?,
            artifact,
        })
    })
    .transpose()
}

pub(super) fn to_i64(value: u64) -> Result<i64, UploadError> {
    i64::try_from(value)
        .map_err(|_| UploadError::Corrupt(format!("value {value} exceeds SQLite range")))
}

pub(super) fn from_i64(value: i64) -> Result<u64, UploadError> {
    u64::try_from(value).map_err(|_| UploadError::Corrupt(format!("negative stored value {value}")))
}

pub(super) fn unique_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}
