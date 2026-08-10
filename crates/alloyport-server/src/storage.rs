//! Durable control-plane domain records and their `SQLite` repository.
//!
//! These types intentionally do not depend on generated Protobuf messages. The RPC edge translates
//! validated wire values into this storage domain before any durable state is changed.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);

CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    registration_json TEXT NOT NULL,
    registered_at_ms INTEGER NOT NULL,
    last_seen_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS worker_connections (
    connection_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers(worker_id),
    instance_id TEXT NOT NULL,
    connected_at_ms INTEGER NOT NULL,
    disconnected_at_ms INTEGER,
    last_worker_sequence INTEGER NOT NULL,
    last_server_sequence INTEGER NOT NULL,
    last_server_acknowledged_by_worker INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_connections_worker
    ON worker_connections(worker_id, connected_at_ms);

CREATE TABLE IF NOT EXISTS assignments (
    attempt_id TEXT PRIMARY KEY,
    assignment_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    contract_json TEXT NOT NULL,
    state INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    last_sent_at_ms INTEGER,
    cancellation_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS assignments_assignment_attempt
    ON assignments(assignment_id, attempt_id);
CREATE INDEX IF NOT EXISTS assignments_worker_state
    ON assignments(worker_id, state, created_at_ms);

CREATE TABLE IF NOT EXISTS attempt_leases (
    attempt_id TEXT PRIMARY KEY REFERENCES assignments(attempt_id),
    lease_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    granted_at_ms INTEGER NOT NULL,
    renewed_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    expired_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS attempt_leases_expiry
    ON attempt_leases(expires_at_ms, expired_at_ms);

CREATE TABLE IF NOT EXISTS attempt_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL REFERENCES assignments(attempt_id),
    worker_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    disposition INTEGER NOT NULL,
    observation_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS attempt_observations_attempt
    ON attempt_observations(attempt_id, observation_id);
COMMIT;
";

/// Source of wall-clock timestamps used by durable lease decisions.
pub trait Clock: Debug + Send + Sync {
    fn now_unix_ms(&self) -> u64;
}

/// Production wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

/// Deterministic clock for state-machine and restart tests.
#[derive(Clone, Debug)]
pub struct ManualClock {
    now_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl ManualClock {
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: Arc::new(std::sync::atomic::AtomicU64::new(now_ms)),
        }
    }

    pub fn advance(&self, duration_ms: u64) {
        self.now_ms
            .fetch_add(duration_ms, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_ms.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Worker registration persisted independently of its current network session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub protocol_major: u32,
    pub protocol_minor: u32,
    pub worker_id: String,
    pub instance_id: String,
    pub worker_version: String,
    pub features: Vec<String>,
    pub capabilities: WorkerCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub backend: i32,
    pub architecture: String,
    pub device_count: u32,
    pub max_concurrency: u32,
    pub driver_version: String,
    pub toolkit_version: String,
    pub container_runtime: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionRegistration {
    pub connection_id: String,
    pub worker_id: String,
    pub instance_id: String,
    pub connected_at_ms: u64,
}

/// Storage-domain form of an immutable assignment contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssignmentContract {
    pub assignment_id: String,
    pub attempt_id: String,
    pub attempt_number: u32,
    pub idempotency_key: String,
    pub task_id: String,
    pub candidate_id: String,
    pub execution: ExecutionContract,
    pub required_features: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionContract {
    pub executor_kind: i32,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub environment: Vec<EnvironmentEntry>,
    pub timeout_ms: u64,
    pub bundle: ArtifactIdentity,
    pub image: ArtifactIdentity,
    pub limits: Option<ResourceContract>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentEntry {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactIdentity {
    pub digest: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceContract {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
    pub device_count: u32,
    pub network: i32,
}

/// Durable server-side lifecycle for a process attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i64)]
pub enum AttemptState {
    Queued = 1,
    Sent = 2,
    Accepted = 3,
    Running = 4,
    Finished = 5,
    Rejected = 6,
    LeaseExpired = 7,
    CancelRequested = 8,
    Cancelled = 9,
}

impl AttemptState {
    fn from_i64(value: i64) -> Result<Self, RepositoryError> {
        match value {
            1 => Ok(Self::Queued),
            2 => Ok(Self::Sent),
            3 => Ok(Self::Accepted),
            4 => Ok(Self::Running),
            5 => Ok(Self::Finished),
            6 => Ok(Self::Rejected),
            7 => Ok(Self::LeaseExpired),
            8 => Ok(Self::CancelRequested),
            9 => Ok(Self::Cancelled),
            _ => Err(RepositoryError::Corrupt(format!(
                "unknown attempt state {value}"
            ))),
        }
    }

    const fn is_replayable(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Sent | Self::Accepted | Self::Running | Self::CancelRequested
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentRecord {
    pub worker_id: String,
    pub contract: AssignmentContract,
    pub state: AttemptState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub cancellation_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseRecord {
    pub attempt_id: String,
    pub lease_id: String,
    pub worker_id: String,
    pub granted_at_ms: u64,
    pub renewed_at_ms: u64,
    pub expires_at_ms: u64,
    pub expired_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AttemptObservation {
    Accepted { already_known: bool },
    Rejected { reason: i32, detail: String },
    Started,
    Finished(FinishedObservation),
    CancellationAcknowledged { already_terminal: bool },
}

impl AttemptObservation {
    const fn target_state(&self) -> Option<AttemptState> {
        match self {
            Self::Accepted { .. } => Some(AttemptState::Accepted),
            Self::Rejected { .. } => Some(AttemptState::Rejected),
            Self::Started => Some(AttemptState::Running),
            Self::Finished(_) => Some(AttemptState::Finished),
            Self::CancellationAcknowledged { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FinishedObservation {
    pub outcome: i32,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub receipt: Option<ArtifactIdentity>,
    pub stdout: Option<ArtifactIdentity>,
    pub stderr: Option<ArtifactIdentity>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedAttempt {
    pub assignment_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub observed_at_ms: u64,
    pub observation: AttemptObservation,
}

/// Whether a worker observation advanced authoritative attempt state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum ObservationDisposition {
    Applied = 1,
    Duplicate = 2,
    Stale = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreAssignmentOutcome {
    Inserted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationStoreOutcome {
    Requested,
    Duplicate,
    CancelledBeforeSend,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancellationRecord {
    pub worker_id: String,
    pub outcome: CancellationStoreOutcome,
}

/// Storage failures are kept distinct from RPC validation failures.
#[derive(Debug)]
pub enum RepositoryError {
    Sqlite(rusqlite::Error),
    Serialization(serde_json::Error),
    LockPoisoned,
    NotFound(String),
    IdentityMismatch(String),
    InvalidTransition {
        from: AttemptState,
        to: AttemptState,
    },
    ConflictingAttempt(String),
    Corrupt(String),
}

impl Display for RepositoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => Display::fmt(error, formatter),
            Self::Serialization(error) => Display::fmt(error, formatter),
            Self::LockPoisoned => write!(formatter, "control repository lock is poisoned"),
            Self::NotFound(attempt) => write!(formatter, "attempt {attempt} is not assigned"),
            Self::IdentityMismatch(attempt) => {
                write!(
                    formatter,
                    "attempt {attempt} identity does not match worker"
                )
            }
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid attempt transition from {from:?} to {to:?}"
                )
            }
            Self::ConflictingAttempt(attempt) => {
                write!(
                    formatter,
                    "attempt {attempt} was reused with different content"
                )
            }
            Self::Corrupt(detail) => write!(formatter, "corrupt control repository: {detail}"),
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Serialization(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for RepositoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for RepositoryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

/// Durable operations needed by the worker control service.
#[allow(clippy::missing_errors_doc)]
pub trait ControlRepository: Debug + Send + Sync {
    fn register_worker(
        &self,
        registration: &WorkerRegistration,
        connection: &ConnectionRegistration,
    ) -> Result<(), RepositoryError>;

    fn update_connection_sequences(
        &self,
        connection_id: &str,
        last_worker_sequence: u64,
        last_server_sequence: u64,
        last_server_acknowledged_by_worker: u64,
        observed_at_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn disconnect(&self, connection_id: &str, at_ms: u64) -> Result<(), RepositoryError>;

    fn store_assignment(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        at_ms: u64,
    ) -> Result<StoreAssignmentOutcome, RepositoryError>;

    fn assignment(&self, attempt_id: &str) -> Result<Option<AssignmentRecord>, RepositoryError>;

    fn replayable_assignments(
        &self,
        worker_id: &str,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError>;

    fn mark_sent_and_grant_lease(
        &self,
        attempt_id: &str,
        worker_id: &str,
        lease_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn observe_attempt(
        &self,
        observation: &ObservedAttempt,
    ) -> Result<ObservationDisposition, RepositoryError>;

    fn renew_active_leases(
        &self,
        worker_id: &str,
        attempt_ids: &[String],
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn expire_leases(&self, now_ms: u64) -> Result<Vec<String>, RepositoryError>;

    fn request_cancellation(
        &self,
        attempt_id: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<CancellationRecord, RepositoryError>;

    fn lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError>;
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
        connection.execute_batch(SCHEMA)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !column_exists(
            &transaction,
            "worker_connections",
            "last_server_acknowledged_by_worker",
        )? {
            transaction.execute_batch(
                "ALTER TABLE worker_connections
                 ADD COLUMN last_server_acknowledged_by_worker INTEGER NOT NULL DEFAULT 0;",
            )?;
        }
        if !column_exists(&transaction, "assignments", "cancellation_reason")? {
            transaction
                .execute_batch("ALTER TABLE assignments ADD COLUMN cancellation_reason TEXT;")?;
        }
        transaction.execute(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES (2)",
            [],
        )?;
        transaction.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, RepositoryError> {
        self.connection
            .lock()
            .map_err(|_| RepositoryError::LockPoisoned)
    }
}

impl ControlRepository for SqliteControlRepository {
    fn register_worker(
        &self,
        registration: &WorkerRegistration,
        connection: &ConnectionRegistration,
    ) -> Result<(), RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let registration_json = serde_json::to_string(registration)?;
        transaction.execute(
            "INSERT INTO workers(worker_id, registration_json, registered_at_ms, last_seen_at_ms)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(worker_id) DO UPDATE SET
                 registration_json = excluded.registration_json,
                 last_seen_at_ms = excluded.last_seen_at_ms",
            params![
                registration.worker_id,
                registration_json,
                to_i64(connection.connected_at_ms)?
            ],
        )?;
        transaction.execute(
            "INSERT INTO worker_connections(
                 connection_id, worker_id, instance_id, connected_at_ms,
                 last_worker_sequence, last_server_sequence,
                 last_server_acknowledged_by_worker
             ) VALUES (?1, ?2, ?3, ?4, 1, 1, 0)",
            params![
                connection.connection_id,
                connection.worker_id,
                connection.instance_id,
                to_i64(connection.connected_at_ms)?
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn update_connection_sequences(
        &self,
        connection_id: &str,
        last_worker_sequence: u64,
        last_server_sequence: u64,
        last_server_acknowledged_by_worker: u64,
        observed_at_ms: u64,
    ) -> Result<(), RepositoryError> {
        let database = self.connection()?;
        database.execute(
            "UPDATE worker_connections
             SET last_worker_sequence = ?2, last_server_sequence = ?3,
                 last_server_acknowledged_by_worker = ?4
             WHERE connection_id = ?1",
            params![
                connection_id,
                to_i64(last_worker_sequence)?,
                to_i64(last_server_sequence)?,
                to_i64(last_server_acknowledged_by_worker)?
            ],
        )?;
        database.execute(
            "UPDATE workers SET last_seen_at_ms = ?2
             WHERE worker_id = (
                 SELECT worker_id FROM worker_connections WHERE connection_id = ?1
             )",
            params![connection_id, to_i64(observed_at_ms)?],
        )?;
        Ok(())
    }

    fn disconnect(&self, connection_id: &str, at_ms: u64) -> Result<(), RepositoryError> {
        self.connection()?.execute(
            "UPDATE worker_connections SET disconnected_at_ms = COALESCE(disconnected_at_ms, ?2)
             WHERE connection_id = ?1",
            params![connection_id, to_i64(at_ms)?],
        )?;
        Ok(())
    }

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
                AttemptState::Queued as i64,
                to_i64(at_ms)?
            ],
        )?;
        transaction.commit()?;
        Ok(StoreAssignmentOutcome::Inserted)
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

    fn mark_sent_and_grant_lease(
        &self,
        attempt_id: &str,
        worker_id: &str,
        lease_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = assignment_identity(&transaction, attempt_id, worker_id, None)?;
        if !state.is_replayable() {
            return Err(RepositoryError::InvalidTransition {
                from: state,
                to: AttemptState::Sent,
            });
        }
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
            expire_one(&transaction, attempt_id, now_ms)?;
            transaction.commit()?;
            return Err(RepositoryError::InvalidTransition {
                from: AttemptState::LeaseExpired,
                to: AttemptState::Sent,
            });
        }
        let next_state = if state == AttemptState::Queued {
            AttemptState::Sent
        } else {
            state
        };
        transaction.execute(
            "UPDATE assignments
             SET state = ?2, updated_at_ms = ?3, last_sent_at_ms = ?3
             WHERE attempt_id = ?1",
            params![attempt_id, next_state as i64, to_i64(now_ms)?],
        )?;
        let expires_at_ms = now_ms.saturating_add(lease_duration_ms);
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
                lease_id,
                worker_id,
                to_i64(now_ms)?,
                to_i64(expires_at_ms)?
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

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
            AttemptState::Queued => (
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

fn assignment_from_row(
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

fn column_exists(
    transaction: &Transaction<'_>,
    table: &str,
    column: &str,
) -> Result<bool, RepositoryError> {
    let query = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let count = transaction.query_row(&query, [column], |row| row.get::<_, i64>(0))?;
    Ok(count == 1)
}

fn assignment_identity(
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

const fn transition_allowed(from: AttemptState, to: AttemptState) -> bool {
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

fn expire_one(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_assignment_is_idempotent_and_survives_reopen() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("control.sqlite3");
        let contract = contract();
        {
            let repository = SqliteControlRepository::open(&database)?;
            assert_eq!(
                repository.store_assignment("worker-1", &contract, 1_000)?,
                StoreAssignmentOutcome::Inserted
            );
            assert_eq!(
                repository.store_assignment("worker-1", &contract, 1_001)?,
                StoreAssignmentOutcome::Duplicate
            );
            let mut changed = contract.clone();
            changed.execution.argv = vec!["different".to_owned()];
            assert!(matches!(
                repository.store_assignment("worker-1", &changed, 1_002),
                Err(RepositoryError::ConflictingAttempt(attempt)) if attempt == "attempt-1"
            ));
        }

        let reopened = SqliteControlRepository::open(&database)?;
        let recovered = reopened
            .assignment("attempt-1")?
            .expect("stored assignment is recovered");
        assert_eq!(recovered.contract, contract);
        assert_eq!(recovered.state, AttemptState::Queued);
        Ok(())
    }

    #[test]
    fn migration_upgrades_the_initial_server_schema() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("old.sqlite3");
        let connection = Connection::open(&database)?;
        connection.execute_batch(
            "CREATE TABLE worker_connections (
                 connection_id TEXT PRIMARY KEY,
                 worker_id TEXT NOT NULL,
                 instance_id TEXT NOT NULL,
                 connected_at_ms INTEGER NOT NULL,
                 disconnected_at_ms INTEGER,
                 last_worker_sequence INTEGER NOT NULL,
                 last_server_sequence INTEGER NOT NULL
             );
             CREATE TABLE assignments (
                 attempt_id TEXT PRIMARY KEY,
                 assignment_id TEXT NOT NULL,
                 worker_id TEXT NOT NULL,
                 contract_json TEXT NOT NULL,
                 state INTEGER NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 last_sent_at_ms INTEGER
             );",
        )?;
        drop(connection);

        let repository = SqliteControlRepository::open(&database)?;
        let database = repository.connection()?;
        let version_two: i64 = database.query_row(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = 2",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(version_two, 1);
        assert!(column_exists_raw(
            &database,
            "worker_connections",
            "last_server_acknowledged_by_worker"
        )?);
        assert!(column_exists_raw(
            &database,
            "assignments",
            "cancellation_reason"
        )?);
        Ok(())
    }

    #[test]
    fn lease_expiry_retains_a_late_result_as_stale() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        let contract = contract();
        repository.store_assignment("worker-1", &contract, 1_000)?;
        repository.mark_sent_and_grant_lease("attempt-1", "worker-1", "lease-1", 1_000, 100)?;
        assert_eq!(
            repository.observe_attempt(&observation(
                1_001,
                AttemptObservation::Accepted {
                    already_known: false,
                },
            ))?,
            ObservationDisposition::Applied
        );

        assert_eq!(repository.expire_leases(1_100)?, vec!["attempt-1"]);
        assert_eq!(
            repository
                .assignment("attempt-1")?
                .expect("attempt remains stored")
                .state,
            AttemptState::LeaseExpired
        );
        let disposition = repository.observe_attempt(&observation(
            1_101,
            AttemptObservation::Finished(FinishedObservation {
                outcome: 1,
                exit_code: Some(0),
                elapsed_ms: 90,
                receipt: Some(artifact('c')),
                stdout: None,
                stderr: None,
                detail: "late success".to_owned(),
            }),
        ))?;
        assert_eq!(disposition, ObservationDisposition::Stale);
        assert_eq!(
            repository
                .assignment("attempt-1")?
                .expect("stale result cannot remove attempt")
                .state,
            AttemptState::LeaseExpired
        );
        let observation_count: i64 = repository.connection()?.query_row(
            "SELECT COUNT(*) FROM attempt_observations WHERE attempt_id = 'attempt-1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(observation_count, 2);
        Ok(())
    }

    #[test]
    fn active_heartbeat_renews_the_lease() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        repository.mark_sent_and_grant_lease("attempt-1", "worker-1", "lease-1", 1_000, 100)?;
        repository.renew_active_leases("worker-1", &["attempt-1".to_owned()], 1_050, 100)?;
        assert!(repository.expire_leases(1_100)?.is_empty());
        assert_eq!(repository.expire_leases(1_150)?, vec!["attempt-1"]);
        Ok(())
    }

    #[test]
    fn heartbeat_after_expiry_cannot_resurrect_a_lease() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        repository.mark_sent_and_grant_lease("attempt-1", "worker-1", "lease-1", 1_000, 100)?;
        repository.renew_active_leases("worker-1", &["attempt-1".to_owned()], 1_101, 100)?;
        assert_eq!(
            repository
                .assignment("attempt-1")?
                .expect("attempt remains durable")
                .state,
            AttemptState::LeaseExpired
        );
        assert_eq!(
            repository
                .lease("attempt-1")?
                .expect("lease remains auditable")
                .expired_at_ms,
            Some(1_101)
        );
        Ok(())
    }

    #[test]
    fn cancellation_cannot_resurrect_expired_work() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        assert_eq!(
            repository
                .request_cancellation("attempt-1", "cancel queued", 1_001)?
                .outcome,
            CancellationStoreOutcome::CancelledBeforeSend
        );
        assert_eq!(
            repository
                .assignment("attempt-1")?
                .expect("cancelled assignment remains auditable")
                .state,
            AttemptState::Cancelled
        );

        let mut second = contract();
        second.assignment_id = "assignment-2".to_owned();
        second.attempt_id = "attempt-2".to_owned();
        repository.store_assignment("worker-1", &second, 2_000)?;
        repository.mark_sent_and_grant_lease("attempt-2", "worker-1", "lease-2", 2_000, 100)?;
        assert_eq!(repository.expire_leases(2_100)?, vec!["attempt-2"]);
        assert_eq!(
            repository
                .request_cancellation("attempt-2", "too late", 2_101)?
                .outcome,
            CancellationStoreOutcome::AlreadyTerminal
        );
        assert_eq!(
            repository
                .assignment("attempt-2")?
                .expect("expired attempt remains auditable")
                .state,
            AttemptState::LeaseExpired
        );
        Ok(())
    }

    fn observation(at_ms: u64, observation: AttemptObservation) -> ObservedAttempt {
        ObservedAttempt {
            assignment_id: "assignment-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            worker_id: "worker-1".to_owned(),
            observed_at_ms: at_ms,
            observation,
        }
    }

    fn contract() -> AssignmentContract {
        AssignmentContract {
            assignment_id: "assignment-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            attempt_number: 1,
            idempotency_key: "task-1:build".to_owned(),
            task_id: "task-1".to_owned(),
            candidate_id: "candidate-1".to_owned(),
            execution: ExecutionContract {
                executor_kind: 2,
                argv: vec!["true".to_owned()],
                working_directory: "source".to_owned(),
                environment: Vec::new(),
                timeout_ms: 30_000,
                bundle: artifact('a'),
                image: artifact('b'),
                limits: None,
            },
            required_features: Vec::new(),
        }
    }

    fn artifact(byte: char) -> ArtifactIdentity {
        ArtifactIdentity {
            digest: format!("sha256:{}", byte.to_string().repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".to_owned(),
        }
    }

    fn column_exists_raw(
        connection: &Connection,
        table: &str,
        column: &str,
    ) -> Result<bool, rusqlite::Error> {
        let query = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
        connection
            .query_row(&query, [column], |row| row.get::<_, i64>(0))
            .map(|count| count == 1)
    }
}
