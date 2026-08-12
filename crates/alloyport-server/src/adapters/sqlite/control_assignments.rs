//! `SQLite` implementation of assignment preparation, replay, and reassignment.

use super::control_records::{
    assignment_from_row, assignment_identity, assignment_in_transaction, existing_reassignment,
    insert_reassignment, to_i64,
};
use super::control_repository::SqliteControlRepository;
use crate::storage::{
    AssignmentContract, AssignmentDeliveryPreparation, AssignmentReadRepository, AssignmentRecord,
    AssignmentWriteRepository, AttemptObservation, AttemptState, FinishedObservation,
    ReassignmentRecord, RepositoryError, StoreAssignmentOutcome,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl AssignmentWriteRepository for SqliteControlRepository {
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
                [contract.attempt_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let contract_json = serde_json::to_string(contract)?;
        if let Some((existing_worker, existing_contract)) = existing {
            let outcome = if existing_worker == worker_id && existing_contract == contract_json {
                StoreAssignmentOutcome::Duplicate
            } else {
                return Err(RepositoryError::ConflictingAttempt(
                    contract.attempt_id.to_string(),
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
                contract.attempt_id.as_str(),
                contract.assignment_id.as_str(),
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

    fn defer_assignment_preparation(
        &self,
        attempt_id: &str,
        worker_id: &str,
        retry_at_ms: u64,
    ) -> Result<bool, RepositoryError> {
        let database = self.connection()?;
        super::assignment_delivery::defer_preparation(&database, attempt_id, worker_id, retry_at_ms)
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
        match super::assignment_delivery::prepare(&transaction, preparation) {
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

impl AssignmentReadRepository for SqliteControlRepository {
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

    fn finished_observation(
        &self,
        attempt_id: &str,
    ) -> Result<Option<FinishedObservation>, RepositoryError> {
        let database = self.connection()?;
        let state = database
            .query_row(
                "SELECT state FROM assignments WHERE attempt_id = ?1",
                [attempt_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if state != Some(AttemptState::Finished as i64) {
            return Ok(None);
        }
        let mut statement = database.prepare(
            "SELECT observation_json FROM attempt_observations
             WHERE attempt_id = ?1 ORDER BY observation_id DESC",
        )?;
        let rows = statement.query_map([attempt_id], |row| row.get::<_, String>(0))?;
        for row in rows {
            if let AttemptObservation::Finished(finished) = serde_json::from_str(&row?)? {
                return Ok(Some(*finished));
            }
        }
        Err(RepositoryError::Corrupt(format!(
            "finished attempt {attempt_id} has no terminal observation"
        )))
    }

    fn preparing_assignments(
        &self,
        limit: usize,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError> {
        let database = self.connection()?;
        super::assignment_delivery::load_preparing(&database, limit)
    }

    fn preparing_assignment_count(&self) -> Result<usize, RepositoryError> {
        let database = self.connection()?;
        super::assignment_delivery::preparing_count(&database)
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
}
