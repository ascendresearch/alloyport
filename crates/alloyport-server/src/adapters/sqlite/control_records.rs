//! `SQLite` row mapping and assignment-state persistence helpers.

use crate::storage::{AssignmentRecord, AttemptState, RepositoryError};
use alloyport_core::AttemptId;
use rusqlite::{OptionalExtension, Transaction, params};

pub(super) fn assignment_in_transaction(
    transaction: &Transaction<'_>,
    attempt_id: &str,
) -> Result<AssignmentRecord, RepositoryError> {
    transaction
        .query_row(
            "SELECT worker_id, contract_json, state, created_at_ms, updated_at_ms,
                    cancellation_reason
             FROM assignments WHERE attempt_id = ?1",
            [attempt_id],
            assignment_from_row,
        )
        .optional()?
        .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?
}

pub(super) fn existing_reassignment(
    transaction: &Transaction<'_>,
    expired_attempt_id: &str,
    replacement_worker_id: &str,
    replacement_attempt_id: &str,
) -> Result<Option<AssignmentRecord>, RepositoryError> {
    let existing = transaction
        .query_row(
            "SELECT replacement_attempt_id, replacement_worker_id
             FROM attempt_reassignments WHERE expired_attempt_id = ?1",
            [expired_attempt_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((existing_attempt, existing_worker)) = existing else {
        return Ok(None);
    };
    if existing_attempt != replacement_attempt_id || existing_worker != replacement_worker_id {
        return Err(RepositoryError::ConflictingAttempt(
            expired_attempt_id.to_owned(),
        ));
    }
    assignment_in_transaction(transaction, replacement_attempt_id).map(Some)
}

pub(super) fn insert_reassignment(
    transaction: &Transaction<'_>,
    mut replacement: AssignmentRecord,
    expired_attempt_id: &str,
    replacement_worker_id: &str,
    replacement_attempt_id: &str,
    at_ms: u64,
) -> Result<AssignmentRecord, RepositoryError> {
    if transaction
        .query_row(
            "SELECT 1 FROM assignments WHERE attempt_id = ?1",
            [replacement_attempt_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some()
    {
        return Err(RepositoryError::ConflictingAttempt(
            replacement_attempt_id.to_owned(),
        ));
    }
    replacement_worker_id.clone_into(&mut replacement.worker_id);
    replacement.contract.attempt_id = AttemptId::try_from(replacement_attempt_id)
        .map_err(|error| RepositoryError::InvalidIdentity(error.to_string()))?;
    replacement.contract.attempt_number = replacement.contract.attempt_number.saturating_add(1);
    replacement.state = AttemptState::Preparing;
    replacement.created_at_ms = at_ms;
    replacement.updated_at_ms = at_ms;
    replacement.cancellation_reason = None;
    let contract_json = serde_json::to_string(&replacement.contract)?;
    transaction.execute(
        "INSERT INTO assignments(
             attempt_id, assignment_id, worker_id, contract_json, state,
             created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![
            replacement.contract.attempt_id.as_str(),
            replacement.contract.assignment_id,
            replacement.worker_id,
            contract_json,
            AttemptState::Preparing as i64,
            to_i64(at_ms)?
        ],
    )?;
    transaction.execute(
        "INSERT INTO attempt_reassignments(
             expired_attempt_id, replacement_attempt_id, replacement_worker_id, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4)",
        params![
            expired_attempt_id,
            replacement_attempt_id,
            replacement_worker_id,
            to_i64(at_ms)?
        ],
    )?;
    Ok(replacement)
}

pub(super) fn assignment_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<AssignmentRecord, RepositoryError>> {
    let worker_id = row.get::<_, String>(0)?;
    let contract_json = row.get::<_, String>(1)?;
    let state = row.get::<_, i64>(2)?;
    let created_at_ms = row.get::<_, i64>(3)?;
    let updated_at_ms = row.get::<_, i64>(4)?;
    let cancellation_reason = row.get::<_, Option<String>>(5)?;
    Ok((|| {
        Ok(AssignmentRecord {
            worker_id,
            contract: serde_json::from_str(&contract_json)?,
            state: AttemptState::from_i64(state)?,
            created_at_ms: from_i64(created_at_ms)?,
            updated_at_ms: from_i64(updated_at_ms)?,
            cancellation_reason,
        })
    })())
}

pub(super) fn assignment_identity(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    worker_id: &str,
    assignment_id: Option<&str>,
) -> Result<AttemptState, RepositoryError> {
    let identity = transaction
        .query_row(
            "SELECT assignment_id, worker_id, state FROM assignments WHERE attempt_id = ?1",
            [attempt_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?;
    if identity.1 != worker_id || assignment_id.is_some_and(|expected| identity.0 != expected) {
        return Err(RepositoryError::IdentityMismatch(attempt_id.to_owned()));
    }
    AttemptState::from_i64(identity.2)
}

pub(super) const fn transition_allowed(from: AttemptState, to: AttemptState) -> bool {
    matches!(
        (from, to),
        (
            AttemptState::Sent,
            AttemptState::Accepted | AttemptState::Rejected
        ) | (
            AttemptState::Accepted,
            AttemptState::Running | AttemptState::Finished
        ) | (
            AttemptState::Running | AttemptState::CancelRequested,
            AttemptState::Finished
        ) | (AttemptState::CancelRequested, AttemptState::Rejected)
    )
}

pub(super) fn expire_one(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    now_ms: u64,
) -> Result<(), RepositoryError> {
    transaction.execute(
        "UPDATE attempt_leases SET expired_at_ms = COALESCE(expired_at_ms, ?2)
         WHERE attempt_id = ?1",
        params![attempt_id, to_i64(now_ms)?],
    )?;
    transaction.execute(
        "UPDATE assignments SET state = ?2, updated_at_ms = ?3
         WHERE attempt_id = ?1 AND state IN (2, 3, 4, 8)",
        params![
            attempt_id,
            AttemptState::LeaseExpired as i64,
            to_i64(now_ms)?
        ],
    )?;
    Ok(())
}

pub(super) fn to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::Corrupt(format!("timestamp {value} exceeds SQLite range")))
}

pub(super) fn from_i64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value)
        .map_err(|_| RepositoryError::Corrupt(format!("negative stored timestamp {value}")))
}
