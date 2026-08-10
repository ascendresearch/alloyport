//! Worker-local durable attempt admission and lifecycle journal.
//!
//! Generated RPC messages are translated into these storage-domain records at the worker edge.

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);
CREATE TABLE IF NOT EXISTS journal_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    assignment_id TEXT NOT NULL,
    assignment_json TEXT NOT NULL,
    phase INTEGER NOT NULL,
    admitted_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    finished_json TEXT
);
CREATE INDEX IF NOT EXISTS attempts_phase ON attempts(phase, admitted_at_ms);
CREATE TABLE IF NOT EXISTS worker_outbox_messages (
    message_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_outbox_attempt
    ON worker_outbox_messages(attempt_id, created_at_ms);
CREATE TABLE IF NOT EXISTS worker_outbox_deliveries (
    connection_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    message_id TEXT NOT NULL REFERENCES worker_outbox_messages(message_id) ON DELETE CASCADE,
    delivered_at_ms INTEGER NOT NULL,
    PRIMARY KEY(connection_id, sequence)
);
CREATE INDEX IF NOT EXISTS worker_outbox_delivery_message
    ON worker_outbox_deliveries(message_id, delivered_at_ms);
COMMIT;
";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredAssignment {
    pub assignment_id: String,
    pub attempt_id: String,
    pub attempt_number: u32,
    pub idempotency_key: String,
    pub task_id: String,
    pub candidate_id: String,
    pub execution: StoredExecution,
    pub required_features: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredExecution {
    pub executor_kind: i32,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub environment: Vec<StoredEnvironment>,
    pub timeout_ms: u64,
    pub bundle: StoredArtifact,
    pub image: StoredArtifact,
    pub limits: Option<StoredLimits>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredEnvironment {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredArtifact {
    pub digest: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredLimits {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
    pub device_count: u32,
    pub network: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum LocalAttemptPhase {
    Accepted = 1,
    Running = 2,
    Finished = 3,
}

impl LocalAttemptPhase {
    fn from_i64(value: i64) -> Result<Self, AttemptStoreError> {
        match value {
            1 => Ok(Self::Accepted),
            2 => Ok(Self::Running),
            3 => Ok(Self::Finished),
            _ => Err(AttemptStoreError::Corrupt(format!(
                "unknown local attempt phase {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredFinished {
    pub outcome: i32,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub receipt: Option<StoredArtifact>,
    pub stdout: Option<StoredArtifact>,
    pub stderr: Option<StoredArtifact>,
    pub detail: String,
}

/// Storage-domain lifecycle payload retained until a server cumulatively acknowledges a delivery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerOutboxPayload {
    AssignmentAccepted {
        assignment_id: String,
        attempt_id: String,
        already_known: bool,
    },
    AssignmentRejected {
        assignment_id: String,
        attempt_id: String,
        reason: i32,
        detail: String,
    },
    ExecutionStarted {
        assignment_id: String,
        attempt_id: String,
    },
    ExecutionFinished {
        assignment_id: String,
        attempt_id: String,
        finished: StoredFinished,
    },
    CancellationAcknowledged {
        assignment_id: String,
        attempt_id: String,
        already_terminal: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerOutboxMessage {
    pub message_id: String,
    pub attempt_id: String,
    pub payload: WorkerOutboxPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAttemptRecord {
    pub assignment: StoredAssignment,
    pub phase: LocalAttemptPhase,
    pub admitted_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished: Option<StoredFinished>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreAdmissionOutcome {
    Inserted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOutboxOutcome {
    Inserted,
    Duplicate,
}

#[derive(Debug)]
pub enum AttemptStoreError {
    Sqlite(rusqlite::Error),
    Serialization(serde_json::Error),
    LockPoisoned,
    NotFound(String),
    ConflictingAttempt(String),
    InvalidTransition {
        from: LocalAttemptPhase,
        to: LocalAttemptPhase,
    },
    ConflictingFinished(String),
    ConflictingOutboxMessage(String),
    WorkerIdentityMismatch {
        stored: String,
        requested: String,
    },
    Corrupt(String),
}

impl Display for AttemptStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => Display::fmt(error, formatter),
            Self::Serialization(error) => Display::fmt(error, formatter),
            Self::LockPoisoned => write!(formatter, "worker attempt journal lock is poisoned"),
            Self::NotFound(attempt) => write!(formatter, "local attempt {attempt} is unknown"),
            Self::ConflictingAttempt(attempt) => {
                write!(formatter, "local attempt {attempt} has conflicting content")
            }
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid local transition from {from:?} to {to:?}"
                )
            }
            Self::ConflictingFinished(attempt) => {
                write!(
                    formatter,
                    "local attempt {attempt} has conflicting terminal results"
                )
            }
            Self::ConflictingOutboxMessage(message_id) => {
                write!(
                    formatter,
                    "outbox message {message_id} has conflicting content"
                )
            }
            Self::WorkerIdentityMismatch { stored, requested } => write!(
                formatter,
                "worker journal belongs to {stored}, not requested worker {requested}"
            ),
            Self::Corrupt(detail) => write!(formatter, "corrupt worker attempt journal: {detail}"),
        }
    }
}

impl Error for AttemptStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Serialization(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for AttemptStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for AttemptStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

#[allow(clippy::missing_errors_doc)]
pub trait AttemptStore: Debug + Send + Sync {
    fn bind_worker(&self, worker_id: &str) -> Result<(), AttemptStoreError>;

    fn admit(
        &self,
        assignment: &StoredAssignment,
        admitted_at_ms: u64,
    ) -> Result<StoreAdmissionOutcome, AttemptStoreError>;

    fn attempt(&self, attempt_id: &str) -> Result<Option<LocalAttemptRecord>, AttemptStoreError>;

    fn attempts(&self) -> Result<Vec<LocalAttemptRecord>, AttemptStoreError>;

    fn mark_running(&self, attempt_id: &str, at_ms: u64) -> Result<(), AttemptStoreError>;

    fn mark_finished(
        &self,
        attempt_id: &str,
        finished: &StoredFinished,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError>;

    fn enqueue_outbox(
        &self,
        message: &WorkerOutboxMessage,
        at_ms: u64,
    ) -> Result<StoreOutboxOutcome, AttemptStoreError>;

    fn pending_outbox(&self) -> Result<Vec<WorkerOutboxMessage>, AttemptStoreError>;

    fn record_outbox_delivery(
        &self,
        connection_id: &str,
        sequence: u64,
        message_id: &str,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError>;

    fn acknowledge_outbox(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
    ) -> Result<usize, AttemptStoreError>;

    fn prune_outbox_deliveries(&self, older_than_ms: u64) -> Result<usize, AttemptStoreError>;

    fn outbox_len(&self) -> Result<usize, AttemptStoreError>;
}

pub struct SqliteAttemptStore {
    connection: Mutex<Connection>,
}

impl Debug for SqliteAttemptStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteAttemptStore")
            .finish_non_exhaustive()
    }
}

impl SqliteAttemptStore {
    /// Opens or creates a durable worker journal.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot open or migrate the journal.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AttemptStoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Creates an ephemeral journal using the same schema and transactions as production.
    ///
    /// # Errors
    ///
    /// Returns an error when the in-memory database cannot be initialized.
    pub fn in_memory() -> Result<Self, AttemptStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, AttemptStoreError> {
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, AttemptStoreError> {
        self.connection
            .lock()
            .map_err(|_| AttemptStoreError::LockPoisoned)
    }
}

impl AttemptStore for SqliteAttemptStore {
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
                [&assignment.attempt_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let outcome = if let Some(existing) = existing {
            if existing != assignment_json {
                return Err(AttemptStoreError::ConflictingAttempt(
                    assignment.attempt_id.clone(),
                ));
            }
            StoreAdmissionOutcome::Duplicate
        } else {
            transaction.execute(
                "INSERT INTO attempts(
                     attempt_id, assignment_id, assignment_json, phase, admitted_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                params![
                    assignment.attempt_id,
                    assignment.assignment_id,
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
                attempt_id: assignment.attempt_id.clone(),
                payload: WorkerOutboxPayload::AssignmentAccepted {
                    assignment_id: assignment.assignment_id.clone(),
                    attempt_id: assignment.attempt_id.clone(),
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

    fn enqueue_outbox(
        &self,
        message: &WorkerOutboxMessage,
        at_ms: u64,
    ) -> Result<StoreOutboxOutcome, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = enqueue_outbox_transaction(&transaction, message, at_ms)?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn pending_outbox(&self) -> Result<Vec<WorkerOutboxMessage>, AttemptStoreError> {
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT message_id, attempt_id, payload_json
             FROM worker_outbox_messages ORDER BY created_at_ms, message_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .map(|row| {
                let (message_id, attempt_id, payload_json) = row?;
                Ok(WorkerOutboxMessage {
                    message_id,
                    attempt_id,
                    payload: serde_json::from_str(&payload_json)?,
                })
            })
            .collect()
    }

    fn record_outbox_delivery(
        &self,
        connection_id: &str,
        sequence: u64,
        message_id: &str,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError> {
        self.connection()?.execute(
            "INSERT INTO worker_outbox_deliveries(
                 connection_id, sequence, message_id, delivered_at_ms
             ) VALUES (?1, ?2, ?3, ?4)",
            params![connection_id, to_i64(sequence)?, message_id, to_i64(at_ms)?],
        )?;
        Ok(())
    }

    fn acknowledge_outbox(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
    ) -> Result<usize, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let message_ids = {
            let mut statement = transaction.prepare(
                "SELECT message_id FROM worker_outbox_deliveries
                 WHERE connection_id = ?1 AND sequence <= ?2",
            )?;
            statement
                .query_map(
                    params![connection_id, to_i64(acknowledged_through)?],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut deleted = 0;
        for message_id in message_ids {
            deleted += transaction.execute(
                "DELETE FROM worker_outbox_messages WHERE message_id = ?1",
                [message_id],
            )?;
        }
        transaction.commit()?;
        Ok(deleted)
    }

    fn prune_outbox_deliveries(&self, older_than_ms: u64) -> Result<usize, AttemptStoreError> {
        self.connection()?
            .execute(
                "DELETE FROM worker_outbox_deliveries WHERE delivered_at_ms < ?1",
                [to_i64(older_than_ms)?],
            )
            .map_err(AttemptStoreError::from)
    }

    fn outbox_len(&self) -> Result<usize, AttemptStoreError> {
        let count = self.connection()?.query_row(
            "SELECT COUNT(*) FROM worker_outbox_messages",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        usize::try_from(count)
            .map_err(|_| AttemptStoreError::Corrupt(format!("negative outbox count {count}")))
    }
}

fn enqueue_outbox_transaction(
    transaction: &rusqlite::Transaction<'_>,
    message: &WorkerOutboxMessage,
    at_ms: u64,
) -> Result<StoreOutboxOutcome, AttemptStoreError> {
    let payload_json = serde_json::to_string(&message.payload)?;
    let existing = transaction
        .query_row(
            "SELECT attempt_id, payload_json
             FROM worker_outbox_messages WHERE message_id = ?1",
            [&message.message_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((attempt_id, existing_payload_json)) = existing {
        let compatible_admission = if attempt_id == message.attempt_id {
            let existing_payload: WorkerOutboxPayload =
                serde_json::from_str(&existing_payload_json)?;
            matches!(
                (&existing_payload, &message.payload),
                (
                    WorkerOutboxPayload::AssignmentAccepted {
                        assignment_id: left_assignment,
                        attempt_id: left_attempt,
                        ..
                    },
                    WorkerOutboxPayload::AssignmentAccepted {
                        assignment_id: right_assignment,
                        attempt_id: right_attempt,
                        ..
                    }
                ) if left_assignment == right_assignment && left_attempt == right_attempt
            )
        } else {
            false
        };
        if existing_payload_json != payload_json && !compatible_admission {
            return Err(AttemptStoreError::ConflictingOutboxMessage(
                message.message_id.clone(),
            ));
        }
        return Ok(StoreOutboxOutcome::Duplicate);
    }
    transaction.execute(
        "INSERT INTO worker_outbox_messages(
             message_id, attempt_id, payload_json, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4)",
        params![
            message.message_id,
            message.attempt_id,
            payload_json,
            to_i64(at_ms)?
        ],
    )?;
    Ok(StoreOutboxOutcome::Inserted)
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

fn to_i64(value: u64) -> Result<i64, AttemptStoreError> {
    i64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("timestamp {value} exceeds SQLite range")))
}

fn from_i64(value: i64) -> Result<u64, AttemptStoreError> {
    u64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("negative stored timestamp {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_compacts_only_deliveries_within_the_cumulative_ack() -> Result<(), Box<dyn Error>> {
        let store = SqliteAttemptStore::in_memory()?;
        let first = accepted_message("attempt-1");
        let second = accepted_message("attempt-2");
        store.enqueue_outbox(&first, 1_000)?;
        store.enqueue_outbox(&second, 1_001)?;
        store.record_outbox_delivery("connection-1", 2, &first.message_id, 1_002)?;
        store.record_outbox_delivery("connection-1", 4, &second.message_id, 1_003)?;

        assert_eq!(store.acknowledge_outbox("connection-1", 3)?, 1);
        assert_eq!(store.pending_outbox()?, vec![second.clone()]);
        assert_eq!(store.acknowledge_outbox("connection-1", 4)?, 1);
        assert_eq!(store.outbox_len()?, 0);
        Ok(())
    }

    #[test]
    fn pruning_orphaned_deliveries_never_discards_the_logical_message() -> Result<(), Box<dyn Error>>
    {
        let store = SqliteAttemptStore::in_memory()?;
        let message = accepted_message("attempt-1");
        store.enqueue_outbox(&message, 1_000)?;
        store.record_outbox_delivery("old-connection", 2, &message.message_id, 1_001)?;

        assert_eq!(store.prune_outbox_deliveries(1_002)?, 1);
        assert_eq!(store.pending_outbox()?, vec![message.clone()]);

        store.record_outbox_delivery("new-connection", 2, &message.message_id, 1_003)?;
        assert_eq!(store.acknowledge_outbox("new-connection", 2)?, 1);
        assert_eq!(store.outbox_len()?, 0);
        Ok(())
    }

    #[test]
    fn admission_and_lifecycle_transitions_atomically_create_outbox_messages()
    -> Result<(), Box<dyn Error>> {
        let store = SqliteAttemptStore::in_memory()?;
        let assignment = stored_assignment();
        assert_eq!(
            store.admit(&assignment, 1_000)?,
            StoreAdmissionOutcome::Inserted
        );
        let admission = store.pending_outbox()?;
        assert_eq!(admission.len(), 1);
        assert!(matches!(
            admission[0].payload,
            WorkerOutboxPayload::AssignmentAccepted {
                already_known: false,
                ..
            }
        ));
        store.record_outbox_delivery("connection-1", 2, &admission[0].message_id, 1_001)?;
        store.acknowledge_outbox("connection-1", 2)?;

        assert_eq!(
            store.admit(&assignment, 1_002)?,
            StoreAdmissionOutcome::Duplicate
        );
        assert!(matches!(
            store.pending_outbox()?[0].payload,
            WorkerOutboxPayload::AssignmentAccepted {
                already_known: true,
                ..
            }
        ));
        store.mark_running("attempt-1", 1_003)?;
        assert_eq!(store.outbox_len()?, 2);
        Ok(())
    }

    fn accepted_message(attempt_id: &str) -> WorkerOutboxMessage {
        WorkerOutboxMessage {
            message_id: format!("assignment-accepted:{attempt_id}"),
            attempt_id: attempt_id.to_owned(),
            payload: WorkerOutboxPayload::AssignmentAccepted {
                assignment_id: format!("assignment-{attempt_id}"),
                attempt_id: attempt_id.to_owned(),
                already_known: false,
            },
        }
    }

    fn stored_assignment() -> StoredAssignment {
        StoredAssignment {
            assignment_id: "assignment-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            attempt_number: 1,
            idempotency_key: "task-1:build".to_owned(),
            task_id: "task-1".to_owned(),
            candidate_id: "candidate-1".to_owned(),
            execution: StoredExecution {
                executor_kind: 2,
                argv: vec!["true".to_owned()],
                working_directory: "source".to_owned(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: StoredArtifact {
                    digest: format!("sha256:{}", "a".repeat(64)),
                    size_bytes: 1,
                    media_type: "application/octet-stream".to_owned(),
                },
                image: StoredArtifact {
                    digest: format!("sha256:{}", "b".repeat(64)),
                    size_bytes: 1,
                    media_type: "application/octet-stream".to_owned(),
                },
                limits: None,
            },
            required_features: Vec::new(),
        }
    }
}
