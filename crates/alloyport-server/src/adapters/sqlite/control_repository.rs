//! `SQLite` implementation of the durable control repository.

use super::control_records::{
    assignment_from_row, assignment_identity, assignment_in_transaction, existing_reassignment,
    expire_one, from_i64, insert_reassignment, to_i64, transition_allowed,
};
#[cfg(test)]
use crate::storage::{
    ArtifactIdentity, AttemptObservation, ExecutionContract, FinishedObservation, ServerFrameKind,
    WorkerCapabilities,
};
use crate::storage::{
    AssignmentContract, AssignmentDeliveryPreparation, AssignmentRecord, AssignmentRepository,
    AttemptLifecycleRepository, AttemptState, CancellationRecord, CancellationStoreOutcome,
    LeaseRecord, ObservationDisposition, ObservedAttempt, ReassignmentRecord, RepositoryError,
    StoreAssignmentOutcome,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::Mutex;

impl From<rusqlite::Error> for RepositoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl From<serde_json::Error> for RepositoryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(Box::new(error))
    }
}

/// SQLite-backed control repository with explicit schema migrations and transactions.
pub struct SqliteControlRepository {
    connection: Mutex<Connection>,
}

impl Debug for SqliteControlRepository {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteControlRepository")
            .finish_non_exhaustive()
    }
}

impl SqliteControlRepository {
    /// Opens or creates a durable repository and applies migrations.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` cannot open or migrate the database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Creates a process-local `SQLite` repository for tests and ephemeral service construction.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` initialization fails.
    pub fn in_memory() -> Result<Self, RepositoryError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(mut connection: Connection) -> Result<Self, RepositoryError> {
        super::control_schema::migrate(&mut connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(super) fn connection(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Connection>, RepositoryError> {
        self.connection
            .lock()
            .map_err(|_| RepositoryError::LockPoisoned)
    }
}

impl AssignmentRepository for SqliteControlRepository {
    fn store_assignment(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        at_ms: u64,
    ) -> Result<StoreAssignmentOutcome, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT worker_id, contract_json FROM assignments WHERE attempt_id = ?1",
                [&contract.attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let contract_json = serde_json::to_string(contract)?;
        if let Some((existing_worker, existing_contract)) = existing {
            let outcome = if existing_worker == worker_id && existing_contract == contract_json {
                StoreAssignmentOutcome::Duplicate
            } else {
                return Err(RepositoryError::ConflictingAttempt(
                    contract.attempt_id.clone(),
                ));
            };
            transaction.commit()?;
            return Ok(outcome);
        }
        transaction.execute(
            "INSERT INTO assignments(
                 attempt_id, assignment_id, worker_id, contract_json, state,
                 created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![
                contract.attempt_id,
                contract.assignment_id,
                worker_id,
                contract_json,
                AttemptState::Preparing as i64,
                to_i64(at_ms)?
            ],
        )?;
        transaction.commit()?;
        Ok(StoreAssignmentOutcome::Inserted)
    }

    fn mark_assignment_dispatchable(
        &self,
        attempt_id: &str,
        worker_id: &str,
        at_ms: u64,
    ) -> Result<bool, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = assignment_identity(&transaction, attempt_id, worker_id, None)?;
        let transitioned = if state == AttemptState::Preparing {
            transaction.execute(
                "UPDATE assignments SET state = ?2, updated_at_ms = ?3 WHERE attempt_id = ?1",
                params![
                    attempt_id,
                    AttemptState::Dispatchable as i64,
                    to_i64(at_ms)?
                ],
            )?;
            true
        } else {
            false
        };
        transaction.commit()?;
        Ok(transitioned)
    }

    fn assignment(&self, attempt_id: &str) -> Result<Option<AssignmentRecord>, RepositoryError> {
        self.connection()?
            .query_row(
                "SELECT worker_id, contract_json, state, created_at_ms, updated_at_ms,
                        cancellation_reason
                 FROM assignments WHERE attempt_id = ?1",
                [attempt_id],
                assignment_from_row,
            )
            .optional()
            .map_err(RepositoryError::from)
            .and_then(Option::transpose)
    }

    fn preparing_assignments(
        &self,
        limit: usize,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError> {
        let database = self.connection()?;
        crate::adapters::sqlite::assignment_delivery::load_preparing(&database, limit)
    }

    fn preparing_assignment_count(&self) -> Result<usize, RepositoryError> {
        let database = self.connection()?;
        crate::adapters::sqlite::assignment_delivery::preparing_count(&database)
    }

    fn defer_assignment_preparation(
        &self,
        attempt_id: &str,
        worker_id: &str,
        retry_at_ms: u64,
    ) -> Result<bool, RepositoryError> {
        let database = self.connection()?;
        crate::adapters::sqlite::assignment_delivery::defer_preparation(
            &database,
            attempt_id,
            worker_id,
            retry_at_ms,
        )
    }

    fn replayable_assignments(
        &self,
        worker_id: &str,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError> {
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT worker_id, contract_json, state, created_at_ms, updated_at_ms,
                    cancellation_reason
             FROM assignments WHERE worker_id = ?1 AND state IN (1, 2, 3, 4, 8)
             ORDER BY created_at_ms, attempt_id",
        )?;
        let records = statement.query_map([worker_id], assignment_from_row)?;
        records
            .map(|record| {
                record
                    .map_err(RepositoryError::from)
                    .and_then(|value| value)
            })
            .collect()
    }

    fn reassign_expired(
        &self,
        expired_attempt_id: &str,
        replacement_worker_id: &str,
        replacement_attempt_id: &str,
        at_ms: u64,
    ) -> Result<ReassignmentRecord, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(assignment) = existing_reassignment(
            &transaction,
            expired_attempt_id,
            replacement_worker_id,
            replacement_attempt_id,
        )? {
            transaction.commit()?;
            return Ok(ReassignmentRecord {
                outcome: StoreAssignmentOutcome::Duplicate,
                assignment,
            });
        }

        let original = assignment_in_transaction(&transaction, expired_attempt_id)?;
        if original.state != AttemptState::LeaseExpired {
            return Err(RepositoryError::InvalidTransition {
                from: original.state,
                to: AttemptState::Dispatchable,
            });
        }
        if replacement_attempt_id.is_empty() || replacement_attempt_id == expired_attempt_id {
            return Err(RepositoryError::ConflictingAttempt(
                replacement_attempt_id.to_owned(),
            ));
        }
        let replacement = insert_reassignment(
            &transaction,
            original,
            expired_attempt_id,
            replacement_worker_id,
            replacement_attempt_id,
            at_ms,
        )?;
        transaction.commit()?;
        Ok(ReassignmentRecord {
            outcome: StoreAssignmentOutcome::Inserted,
            assignment: replacement,
        })
    }

    fn prepare_assignment_delivery(
        &self,
        preparation: &AssignmentDeliveryPreparation,
    ) -> Result<AssignmentContract, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        match crate::adapters::sqlite::assignment_delivery::prepare(&transaction, preparation) {
            Ok(contract) => {
                transaction.commit()?;
                Ok(contract)
            }
            Err(
                error @ RepositoryError::InvalidTransition {
                    from: AttemptState::LeaseExpired,
                    ..
                },
            ) => {
                transaction.commit()?;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

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

#[cfg(test)]
#[path = "control_repository_tests.rs"]
mod tests;
