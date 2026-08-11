//! `SQLite` implementation of attempt observations, cancellation, and leases.

use super::control_records::{
    assignment_identity, expire_one, from_i64, to_i64, transition_allowed,
};
use super::control_repository::SqliteControlRepository;
use crate::storage::{
    AttemptLifecycleRepository, AttemptState, CancellationRecord, CancellationStoreOutcome,
    LeaseRecord, ObservationDisposition, ObservedAttempt, RepositoryError,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl AttemptLifecycleRepository for SqliteControlRepository {
    fn observe_attempt(
        &self,
        observation: &ObservedAttempt,
    ) -> Result<ObservationDisposition, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = assignment_identity(
            &transaction,
            &observation.attempt_id,
            &observation.worker_id,
            Some(&observation.assignment_id),
        )?;
        let target = observation.observation.target_state();
        let lease_expiry = transaction
            .query_row(
                "SELECT expires_at_ms FROM attempt_leases WHERE attempt_id = ?1",
                [&observation.attempt_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(from_i64)
            .transpose()?;
        let disposition = match target {
            None => match current {
                AttemptState::CancelRequested => ObservationDisposition::Applied,
                AttemptState::Finished | AttemptState::Cancelled => {
                    ObservationDisposition::Duplicate
                }
                AttemptState::LeaseExpired => ObservationDisposition::Stale,
                _ => {
                    return Err(RepositoryError::InvalidTransition {
                        from: current,
                        to: AttemptState::CancelRequested,
                    });
                }
            },
            Some(target) => {
                let is_late = current == AttemptState::LeaseExpired
                    || (current == AttemptState::Rejected && target == AttemptState::Finished)
                    || (target == AttemptState::Finished
                        && current != AttemptState::Finished
                        && lease_expiry.is_some_and(|expiry| expiry <= observation.observed_at_ms));
                if is_late {
                    expire_one(
                        &transaction,
                        &observation.attempt_id,
                        observation.observed_at_ms,
                    )?;
                    ObservationDisposition::Stale
                } else if current == target
                    || current == AttemptState::Finished
                    || (current == AttemptState::Running && target == AttemptState::Accepted)
                    || (current == AttemptState::CancelRequested
                        && target == AttemptState::Accepted)
                {
                    ObservationDisposition::Duplicate
                } else if transition_allowed(current, target) {
                    transaction.execute(
                        "UPDATE assignments SET state = ?2, updated_at_ms = ?3 WHERE attempt_id = ?1",
                        params![
                            observation.attempt_id,
                            target as i64,
                            to_i64(observation.observed_at_ms)?
                        ],
                    )?;
                    ObservationDisposition::Applied
                } else {
                    return Err(RepositoryError::InvalidTransition {
                        from: current,
                        to: target,
                    });
                }
            }
        };

        let observation_json = serde_json::to_string(&observation.observation)?;
        transaction.execute(
            "INSERT INTO attempt_observations(
                 attempt_id, worker_id, observed_at_ms, disposition, observation_json
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                observation.attempt_id,
                observation.worker_id,
                to_i64(observation.observed_at_ms)?,
                disposition as i64,
                observation_json
            ],
        )?;
        transaction.commit()?;
        Ok(disposition)
    }

    fn renew_active_leases(
        &self,
        worker_id: &str,
        attempt_ids: &[String],
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for attempt_id in attempt_ids {
            let state = assignment_identity(&transaction, attempt_id, worker_id, None)?;
            if state.is_replayable() {
                let lease = transaction
                    .query_row(
                        "SELECT expires_at_ms, expired_at_ms
                         FROM attempt_leases WHERE attempt_id = ?1",
                        [attempt_id],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
                    )
                    .optional()?;
                if let Some((expires_at_ms, expired_at_ms)) = lease {
                    if expired_at_ms.is_some() || from_i64(expires_at_ms)? <= now_ms {
                        expire_one(&transaction, attempt_id, now_ms)?;
                    } else {
                        transaction.execute(
                            "UPDATE attempt_leases
                             SET renewed_at_ms = ?2, expires_at_ms = ?3
                             WHERE attempt_id = ?1",
                            params![
                                attempt_id,
                                to_i64(now_ms)?,
                                to_i64(now_ms.saturating_add(lease_duration_ms))?
                            ],
                        )?;
                    }
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn expire_leases(&self, now_ms: u64) -> Result<Vec<String>, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let expired = {
            let mut statement = transaction.prepare(
                "SELECT leases.attempt_id
                 FROM attempt_leases AS leases
                 JOIN assignments USING(attempt_id)
                 WHERE leases.expired_at_ms IS NULL
                   AND leases.expires_at_ms <= ?1
                   AND assignments.state IN (2, 3, 4, 8)
                 ORDER BY leases.attempt_id",
            )?;
            statement
                .query_map([to_i64(now_ms)?], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for attempt_id in &expired {
            expire_one(&transaction, attempt_id, now_ms)?;
        }
        transaction.commit()?;
        Ok(expired)
    }

    fn request_cancellation(
        &self,
        attempt_id: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<CancellationRecord, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (worker_id, state_value) = transaction
            .query_row(
                "SELECT worker_id, state FROM assignments WHERE attempt_id = ?1",
                [attempt_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?;
        let state = AttemptState::from_i64(state_value)?;
        let (next_state, outcome) = match state {
            AttemptState::Preparing | AttemptState::Dispatchable => (
                AttemptState::Cancelled,
                CancellationStoreOutcome::CancelledBeforeSend,
            ),
            AttemptState::Sent | AttemptState::Accepted | AttemptState::Running => (
                AttemptState::CancelRequested,
                CancellationStoreOutcome::Requested,
            ),
            AttemptState::CancelRequested => (
                AttemptState::CancelRequested,
                CancellationStoreOutcome::Duplicate,
            ),
            AttemptState::Finished
            | AttemptState::Rejected
            | AttemptState::LeaseExpired
            | AttemptState::Cancelled => (state, CancellationStoreOutcome::AlreadyTerminal),
        };
        if !matches!(outcome, CancellationStoreOutcome::AlreadyTerminal) {
            transaction.execute(
                "UPDATE assignments
                 SET state = ?2, updated_at_ms = ?3, cancellation_reason = ?4
                 WHERE attempt_id = ?1",
                params![attempt_id, next_state as i64, to_i64(now_ms)?, reason],
            )?;
        }
        transaction.commit()?;
        Ok(CancellationRecord { worker_id, outcome })
    }

    fn lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError> {
        self.connection()?
            .query_row(
                "SELECT attempt_id, lease_id, worker_id, granted_at_ms, renewed_at_ms,
                        expires_at_ms, expired_at_ms
                 FROM attempt_leases WHERE attempt_id = ?1",
                [attempt_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                    ))
                },
            )
            .optional()?
            .map(|row| {
                Ok(LeaseRecord {
                    attempt_id: row.0,
                    lease_id: row.1,
                    worker_id: row.2,
                    granted_at_ms: from_i64(row.3)?,
                    renewed_at_ms: from_i64(row.4)?,
                    expires_at_ms: from_i64(row.5)?,
                    expired_at_ms: row.6.map(from_i64).transpose()?,
                })
            })
            .transpose()
    }
}
