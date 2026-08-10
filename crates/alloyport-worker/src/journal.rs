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
        if let Some(existing) = existing {
            if existing != assignment_json {
                return Err(AttemptStoreError::ConflictingAttempt(
                    assignment.attempt_id.clone(),
                ));
            }
            transaction.commit()?;
            return Ok(StoreAdmissionOutcome::Duplicate);
        }
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
        transaction.commit()?;
        Ok(StoreAdmissionOutcome::Inserted)
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

fn to_i64(value: u64) -> Result<i64, AttemptStoreError> {
    i64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("timestamp {value} exceeds SQLite range")))
}

fn from_i64(value: i64) -> Result<u64, AttemptStoreError> {
    u64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("negative stored timestamp {value}")))
}
