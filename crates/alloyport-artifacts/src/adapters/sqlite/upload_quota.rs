//! Artifact capacity accounting and quota enforcement queries.

use super::upload_records::{from_i64, to_i64};
use crate::Sha256Digest;
use crate::upload::{BeginUpload, QuotaScope, UploadError, UploadQuotas, UploadState};
use rusqlite::{Connection, OptionalExtension, params};

pub(super) fn validate_quotas(quotas: UploadQuotas) -> Result<(), UploadError> {
    if quotas.total_bytes == 0 || quotas.per_owner_bytes == 0 {
        return Err(UploadError::InvalidRequest(
            "artifact quota limits must be positive",
        ));
    }
    if quotas.total_bytes > i64::MAX as u64 || quotas.per_owner_bytes > i64::MAX as u64 {
        return Err(UploadError::InvalidRequest(
            "artifact quota limits exceed SQLite range",
        ));
    }
    if quotas.per_owner_bytes > quotas.total_bytes {
        return Err(UploadError::InvalidRequest(
            "per-owner artifact quota exceeds total quota",
        ));
    }
    Ok(())
}

pub(super) fn reserve_quota(
    transaction: &rusqlite::Transaction<'_>,
    request: &BeginUpload,
    quotas: UploadQuotas,
) -> Result<(), UploadError> {
    let total_stored = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(size_bytes), 0) FROM artifact_objects",
        [],
    )?;
    let total_reserved = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(quota_reserved_bytes), 0) FROM upload_sessions
         WHERE state IN (?1, ?2) AND expires_at_ms > ?3",
        params![
            UploadState::Open as i64,
            UploadState::Finalizing as i64,
            to_i64(request.now_ms)?
        ],
    )?;
    enforce_quota(
        QuotaScope::Total,
        quotas.total_bytes,
        total_stored.saturating_add(total_reserved),
        request.expected_size_bytes,
    )?;

    let owner_stored = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(size_bytes), 0) FROM artifact_owner_references
         WHERE owner_id = ?1",
        [&request.owner_id],
    )?;
    let owner_reserved = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(quota_reserved_bytes), 0) FROM upload_sessions
         WHERE owner_id = ?1 AND state IN (?2, ?3) AND expires_at_ms > ?4",
        params![
            request.owner_id,
            UploadState::Open as i64,
            UploadState::Finalizing as i64,
            to_i64(request.now_ms)?
        ],
    )?;
    enforce_quota(
        QuotaScope::Owner,
        quotas.per_owner_bytes,
        owner_stored.saturating_add(owner_reserved),
        request.expected_size_bytes,
    )
}

fn query_bytes(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<u64, UploadError> {
    let value = connection.query_row(sql, parameters, |row| row.get::<_, i64>(0))?;
    from_i64(value)
}

pub(super) fn enforce_quota(
    scope: QuotaScope,
    limit: u64,
    used: u64,
    requested: u64,
) -> Result<(), UploadError> {
    if used.saturating_add(requested) > limit {
        Err(UploadError::QuotaExceeded {
            scope,
            limit,
            used,
            requested,
        })
    } else {
        Ok(())
    }
}

pub(super) fn owner_stored_bytes(
    connection: &Connection,
    owner_id: &str,
) -> Result<u64, UploadError> {
    query_bytes(
        connection,
        "SELECT COALESCE(SUM(size_bytes), 0) FROM artifact_owner_references
         WHERE owner_id = ?1",
        [owner_id],
    )
}

pub(super) fn owner_reserved_bytes(
    connection: &Connection,
    owner_id: &str,
    now_ms: u64,
) -> Result<u64, UploadError> {
    query_bytes(
        connection,
        "SELECT COALESCE(SUM(quota_reserved_bytes), 0) FROM upload_sessions
         WHERE owner_id = ?1 AND state IN (?2, ?3) AND expires_at_ms > ?4",
        params![
            owner_id,
            UploadState::Open as i64,
            UploadState::Finalizing as i64,
            to_i64(now_ms)?
        ],
    )
}

pub(super) fn artifact_size(
    connection: &Connection,
    digest: Sha256Digest,
) -> Result<Option<u64>, UploadError> {
    connection
        .query_row(
            "SELECT size_bytes FROM artifact_objects WHERE digest = ?1",
            [digest.to_string()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map(from_i64)
        .transpose()
}
