//! Atomic `SQLite` implementation of assignment delivery preparation.

use crate::storage::{
    AssignmentContract, AssignmentDeliveryPreparation, AssignmentRecord, AttemptState,
    RepositoryError, ServerFrameKind,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

pub(crate) fn preparing_count(connection: &Connection) -> Result<usize, RepositoryError> {
    let count = connection.query_row(
        "SELECT COUNT(*) FROM assignments WHERE state = 10",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    usize::try_from(count)
        .map_err(|_| RepositoryError::Corrupt(format!("invalid preparing count {count}")))
}

pub(crate) fn load_preparing(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<AssignmentRecord>, RepositoryError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).map_err(|_| {
        RepositoryError::Corrupt(format!("batch limit {limit} exceeds SQLite range"))
    })?;
    let mut statement = connection.prepare(
        "SELECT worker_id, contract_json, created_at_ms, updated_at_ms, cancellation_reason
         FROM assignments WHERE state = 10 ORDER BY updated_at_ms, attempt_id LIMIT ?1",
    )?;
    let rows = statement.query_map([limit], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;
    let mut assignments = Vec::new();
    for row in rows {
        let (worker_id, contract_json, created_at_ms, updated_at_ms, cancellation_reason) = row?;
        assignments.push(AssignmentRecord {
            worker_id,
            contract: serde_json::from_str(&contract_json)?,
            state: AttemptState::Preparing,
            created_at_ms: from_i64(created_at_ms)?,
            updated_at_ms: from_i64(updated_at_ms)?,
            cancellation_reason,
        });
    }
    Ok(assignments)
}

pub(crate) fn defer_preparation(
    connection: &Connection,
    attempt_id: &str,
    worker_id: &str,
    retry_at_ms: u64,
) -> Result<bool, RepositoryError> {
    let updated = connection.execute(
        "UPDATE assignments SET updated_at_ms = ?3
         WHERE attempt_id = ?1 AND worker_id = ?2 AND state = 10",
        params![attempt_id, worker_id, to_i64(retry_at_ms)?],
    )?;
    Ok(updated == 1)
}

pub(crate) fn prepare(
    transaction: &Transaction<'_>,
    preparation: &AssignmentDeliveryPreparation,
) -> Result<AssignmentContract, RepositoryError> {
    let attempt_id = preparation
        .frame
        .attempt_id
        .as_deref()
        .ok_or_else(|| RepositoryError::Corrupt("assignment frame has no attempt ID".into()))?;
    if preparation.frame.kind != ServerFrameKind::Assignment {
        return Err(RepositoryError::Corrupt(
            "assignment delivery preparation received a non-assignment frame".into(),
        ));
    }

    let (contract, state) = load_assignment(transaction, preparation, attempt_id)?;
    if !is_replayable(state) {
        return Err(RepositoryError::InvalidTransition {
            from: state,
            to: AttemptState::Sent,
        });
    }

    reject_expired_lease(transaction, attempt_id, preparation.now_ms)?;

    let next_state = if state == AttemptState::Dispatchable {
        AttemptState::Sent
    } else {
        state
    };
    transaction.execute(
        "UPDATE assignments
         SET state = ?2, updated_at_ms = ?3, last_sent_at_ms = ?3
         WHERE attempt_id = ?1",
        params![attempt_id, next_state as i64, to_i64(preparation.now_ms)?],
    )?;
    let expires_at_ms = preparation
        .now_ms
        .saturating_add(preparation.lease_duration_ms);
    transaction.execute(
        "INSERT INTO attempt_leases(
             attempt_id, lease_id, worker_id, granted_at_ms, renewed_at_ms, expires_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?4, ?5)
         ON CONFLICT(attempt_id) DO UPDATE SET
             renewed_at_ms = excluded.renewed_at_ms,
             expires_at_ms = excluded.expires_at_ms,
             expired_at_ms = NULL",
        params![
            attempt_id,
            preparation.lease_id,
            preparation.frame.worker_id,
            to_i64(preparation.now_ms)?,
            to_i64(expires_at_ms)?
        ],
    )?;
    transaction.execute(
        "INSERT INTO server_outbox_frames(
             connection_id, sequence, message_id, worker_id, kind, attempt_id, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            preparation.frame.connection_id,
            to_i64(preparation.frame.sequence)?,
            preparation.frame.message_id,
            preparation.frame.worker_id,
            preparation.frame.kind as i64,
            preparation.frame.attempt_id,
            to_i64(preparation.now_ms)?
        ],
    )?;
    let updated = transaction.execute(
        "UPDATE worker_connections
         SET last_worker_sequence = ?3, last_server_sequence = ?4,
             last_server_acknowledged_by_worker = ?5
         WHERE connection_id = ?1 AND worker_id = ?2 AND disconnected_at_ms IS NULL",
        params![
            preparation.frame.connection_id,
            preparation.frame.worker_id,
            to_i64(preparation.last_worker_sequence)?,
            to_i64(preparation.frame.sequence)?,
            to_i64(preparation.last_server_acknowledged_by_worker)?
        ],
    )?;
    if updated != 1 {
        return Err(RepositoryError::Corrupt(format!(
            "active connection {} for worker {} is missing",
            preparation.frame.connection_id, preparation.frame.worker_id
        )));
    }
    transaction.execute(
        "UPDATE workers SET last_seen_at_ms = ?2 WHERE worker_id = ?1",
        params![preparation.frame.worker_id, to_i64(preparation.now_ms)?],
    )?;
    Ok(contract)
}

fn load_assignment(
    transaction: &Transaction<'_>,
    preparation: &AssignmentDeliveryPreparation,
    attempt_id: &str,
) -> Result<(AssignmentContract, AttemptState), RepositoryError> {
    let stored = transaction
        .query_row(
            "SELECT worker_id, contract_json, state FROM assignments WHERE attempt_id = ?1",
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
    if stored.0 != preparation.frame.worker_id {
        return Err(RepositoryError::IdentityMismatch(attempt_id.to_owned()));
    }
    Ok((serde_json::from_str(&stored.1)?, attempt_state(stored.2)?))
}

fn reject_expired_lease(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    now_ms: u64,
) -> Result<(), RepositoryError> {
    let existing_expiry = transaction
        .query_row(
            "SELECT expires_at_ms FROM attempt_leases WHERE attempt_id = ?1",
            [attempt_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map(from_i64)
        .transpose()?;
    if existing_expiry.is_some_and(|expiry| expiry <= now_ms) {
        expire(transaction, attempt_id, now_ms)?;
        return Err(RepositoryError::InvalidTransition {
            from: AttemptState::LeaseExpired,
            to: AttemptState::Sent,
        });
    }
    Ok(())
}

fn attempt_state(value: i64) -> Result<AttemptState, RepositoryError> {
    match value {
        1 => Ok(AttemptState::Dispatchable),
        2 => Ok(AttemptState::Sent),
        3 => Ok(AttemptState::Accepted),
        4 => Ok(AttemptState::Running),
        5 => Ok(AttemptState::Finished),
        6 => Ok(AttemptState::Rejected),
        7 => Ok(AttemptState::LeaseExpired),
        8 => Ok(AttemptState::CancelRequested),
        9 => Ok(AttemptState::Cancelled),
        10 => Ok(AttemptState::Preparing),
        _ => Err(RepositoryError::Corrupt(format!(
            "unknown attempt state {value}"
        ))),
    }
}

const fn is_replayable(state: AttemptState) -> bool {
    matches!(
        state,
        AttemptState::Dispatchable
            | AttemptState::Sent
            | AttemptState::Accepted
            | AttemptState::Running
            | AttemptState::CancelRequested
    )
}

fn expire(
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

fn to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::Corrupt(format!("timestamp {value} exceeds SQLite range")))
}

fn from_i64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value)
        .map_err(|_| RepositoryError::Corrupt(format!("negative stored timestamp {value}")))
}
