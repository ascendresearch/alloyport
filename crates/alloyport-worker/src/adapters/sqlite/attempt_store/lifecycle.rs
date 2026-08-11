//! Attempt identity and lifecycle persistence.

use super::SqliteAttemptStore;
use super::outbox::{enqueue_outbox_transaction, to_i64};
use crate::journal::{
    AttemptLifecycleStore, AttemptStoreError, LocalAttemptPhase, LocalAttemptRecord,
    StoreAdmissionOutcome, StoredAssignment, StoredFinished, WorkerOutboxMessage,
    WorkerOutboxPayload,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl AttemptLifecycleStore for SqliteAttemptStore {
    fn bind_worker(&self, worker_id: &str) -> Result<(), AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored = transaction
            .query_row(
                "SELECT value FROM journal_metadata WHERE key = 'worker_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(stored) = stored {
            if stored != worker_id {
                return Err(AttemptStoreError::WorkerIdentityMismatch {
                    stored,
                    requested: worker_id.to_owned(),
                });
            }
        } else {
            transaction.execute(
                "INSERT INTO journal_metadata(key, value) VALUES ('worker_id', ?1)",
                [worker_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn admit(
        &self,
        assignment: &StoredAssignment,
        admitted_at_ms: u64,
    ) -> Result<StoreAdmissionOutcome, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let assignment_json = serde_json::to_string(assignment)?;
        let existing = transaction
            .query_row(
                "SELECT assignment_json FROM attempts WHERE attempt_id = ?1",
                [assignment.attempt_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let outcome = if let Some(existing) = existing {
            if existing != assignment_json {
                return Err(AttemptStoreError::ConflictingAttempt(
                    assignment.attempt_id.to_string(),
                ));
            }
            StoreAdmissionOutcome::Duplicate
        } else {
            transaction.execute(
                "INSERT INTO attempts(
                     attempt_id, assignment_id, assignment_json, phase, admitted_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                params![
                    assignment.attempt_id.as_str(),
                    assignment.assignment_id.as_str(),
                    assignment_json,
                    LocalAttemptPhase::Accepted as i64,
                    to_i64(admitted_at_ms)?
                ],
            )?;
            StoreAdmissionOutcome::Inserted
        };
        enqueue_outbox_transaction(
            &transaction,
            &WorkerOutboxMessage {
                message_id: format!("assignment-accepted:{}", assignment.attempt_id),
                attempt_id: assignment.attempt_id.to_string(),
                payload: WorkerOutboxPayload::AssignmentAccepted {
                    assignment_id: assignment.assignment_id.to_string(),
                    attempt_id: assignment.attempt_id.to_string(),
                    already_known: outcome == StoreAdmissionOutcome::Duplicate,
                },
            },
            admitted_at_ms,
        )?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn attempt(&self, attempt_id: &str) -> Result<Option<LocalAttemptRecord>, AttemptStoreError> {
        self.connection()?
            .query_row(
                "SELECT assignment_json, phase, admitted_at_ms, updated_at_ms, finished_json
                 FROM attempts WHERE attempt_id = ?1",
                [attempt_id],
                record_from_row,
            )
            .optional()
            .map_err(AttemptStoreError::from)
            .and_then(Option::transpose)
    }

    fn attempts(&self) -> Result<Vec<LocalAttemptRecord>, AttemptStoreError> {
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT assignment_json, phase, admitted_at_ms, updated_at_ms, finished_json
             FROM attempts ORDER BY admitted_at_ms, attempt_id",
        )?;
        statement
            .query_map([], record_from_row)?
            .map(|record| {
                record
                    .map_err(AttemptStoreError::from)
                    .and_then(|value| value)
            })
            .collect()
    }

    fn mark_running(&self, attempt_id: &str, at_ms: u64) -> Result<(), AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let phase = phase(&transaction, attempt_id)?;
        match phase {
            LocalAttemptPhase::Accepted => {
                transaction.execute(
                    "UPDATE attempts SET phase = ?2, updated_at_ms = ?3 WHERE attempt_id = ?1",
                    params![
                        attempt_id,
                        LocalAttemptPhase::Running as i64,
                        to_i64(at_ms)?
                    ],
                )?;
            }
            LocalAttemptPhase::Running => {}
            LocalAttemptPhase::Finished => {
                return Err(AttemptStoreError::InvalidTransition {
                    from: phase,
                    to: LocalAttemptPhase::Running,
                });
            }
        }
        let assignment_id: String = transaction.query_row(
            "SELECT assignment_id FROM attempts WHERE attempt_id = ?1",
            [attempt_id],
            |row| row.get(0),
        )?;
        enqueue_outbox_transaction(
            &transaction,
            &WorkerOutboxMessage {
                message_id: format!("execution-started:{attempt_id}"),
                attempt_id: attempt_id.to_owned(),
                payload: WorkerOutboxPayload::ExecutionStarted {
                    assignment_id,
                    attempt_id: attempt_id.to_owned(),
                },
            },
            at_ms,
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn mark_finished(
        &self,
        attempt_id: &str,
        finished: &StoredFinished,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let phase = phase(&transaction, attempt_id)?;
        let finished_json = serde_json::to_string(finished)?;
        if phase == LocalAttemptPhase::Finished {
            let existing: String = transaction.query_row(
                "SELECT finished_json FROM attempts WHERE attempt_id = ?1",
                [attempt_id],
                |row| row.get(0),
            )?;
            if existing != finished_json {
                return Err(AttemptStoreError::ConflictingFinished(
                    attempt_id.to_owned(),
                ));
            }
        } else {
            transaction.execute(
                "UPDATE attempts
                 SET phase = ?2, updated_at_ms = ?3, finished_json = ?4
                 WHERE attempt_id = ?1",
                params![
                    attempt_id,
                    LocalAttemptPhase::Finished as i64,
                    to_i64(at_ms)?,
                    finished_json
                ],
            )?;
        }
        let assignment_id: String = transaction.query_row(
            "SELECT assignment_id FROM attempts WHERE attempt_id = ?1",
            [attempt_id],
            |row| row.get(0),
        )?;
        enqueue_outbox_transaction(
            &transaction,
            &WorkerOutboxMessage {
                message_id: format!("execution-finished:{attempt_id}"),
                attempt_id: attempt_id.to_owned(),
                payload: WorkerOutboxPayload::ExecutionFinished {
                    assignment_id,
                    attempt_id: attempt_id.to_owned(),
                    finished: finished.clone(),
                },
            },
            at_ms,
        )?;
        transaction.commit()?;
        Ok(())
    }
}

fn phase(
    transaction: &rusqlite::Transaction<'_>,
    attempt_id: &str,
) -> Result<LocalAttemptPhase, AttemptStoreError> {
    let value = transaction
        .query_row(
            "SELECT phase FROM attempts WHERE attempt_id = ?1",
            [attempt_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_owned()))?;
    LocalAttemptPhase::from_i64(value)
}

fn record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<LocalAttemptRecord, AttemptStoreError>> {
    let assignment_json = row.get::<_, String>(0)?;
    let phase = row.get::<_, i64>(1)?;
    let admitted_at_ms = row.get::<_, i64>(2)?;
    let updated_at_ms = row.get::<_, i64>(3)?;
    let finished_json = row.get::<_, Option<String>>(4)?;
    Ok((|| {
        Ok(LocalAttemptRecord {
            assignment: serde_json::from_str(&assignment_json)?,
            phase: LocalAttemptPhase::from_i64(phase)?,
            admitted_at_ms: from_i64(admitted_at_ms)?,
            updated_at_ms: from_i64(updated_at_ms)?,
            finished: finished_json
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
        })
    })())
}

fn from_i64(value: i64) -> Result<u64, AttemptStoreError> {
    u64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("negative stored timestamp {value}")))
}
