//! Reachability queries used by Artifact garbage collection.

use super::upload_records::{from_i64, to_i64};
use crate::Sha256Digest;
use crate::upload::{UploadError, UploadState};
use rusqlite::{Connection, TransactionBehavior, params};
use std::str::FromStr;

pub(super) fn stage_garbage_candidates(
    connection: &mut Connection,
    now_ms: u64,
    limit: usize,
) -> Result<(), UploadError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_gc_pending(digest, marked_at_ms)
         SELECT object.digest, ?1 FROM artifact_objects AS object
         WHERE NOT EXISTS (
             SELECT 1 FROM artifact_references AS reference
             WHERE reference.digest = object.digest AND reference.revoked_at_ms IS NULL
         )
         AND NOT EXISTS (
             SELECT 1 FROM artifact_references AS reference
             WHERE reference.digest = object.digest
               AND reference.retained_until_ms IS NOT NULL
               AND reference.retained_until_ms > ?1
         )
         AND NOT EXISTS (
             SELECT 1 FROM upload_sessions AS session
             WHERE session.expected_digest = object.digest
               AND session.state IN (?2, ?3) AND session.expires_at_ms > ?1
         )
         LIMIT ?4",
        params![
            to_i64(now_ms)?,
            UploadState::Open as i64,
            UploadState::Finalizing as i64,
            i64::try_from(limit).map_err(|_| UploadError::InvalidRequest(
                "garbage collection limit is too large"
            ))?
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn pending_garbage(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<(Sha256Digest, u64)>, UploadError> {
    let mut statement = connection.prepare(
        "SELECT pending.digest, object.size_bytes
         FROM artifact_gc_pending AS pending
         JOIN artifact_objects AS object ON object.digest = pending.digest
         ORDER BY pending.marked_at_ms, pending.digest LIMIT ?1",
    )?;
    let rows = statement
        .query_map(
            [i64::try_from(limit).map_err(|_| {
                UploadError::InvalidRequest("garbage collection limit is too large")
            })?],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(digest, size)| {
            Ok((
                Sha256Digest::from_str(&digest)
                    .map_err(|error| UploadError::Corrupt(error.to_string()))?,
                from_i64(size)?,
            ))
        })
        .collect()
}

pub(super) fn artifact_is_reachable(
    connection: &Connection,
    digest: Sha256Digest,
    now_ms: u64,
) -> Result<bool, UploadError> {
    Ok(connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM artifact_references
             WHERE digest = ?1 AND (
                 revoked_at_ms IS NULL OR
                 (retained_until_ms IS NOT NULL AND retained_until_ms > ?2)
             )
         ) OR EXISTS(
             SELECT 1 FROM upload_sessions
             WHERE expected_digest = ?1 AND state IN (?3, ?4) AND expires_at_ms > ?2
         )",
        params![
            digest.to_string(),
            to_i64(now_ms)?,
            UploadState::Open as i64,
            UploadState::Finalizing as i64
        ],
        |row| row.get(0),
    )?)
}
