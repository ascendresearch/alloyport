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
CREATE TABLE IF NOT EXISTS attempt_reassignments (
    expired_attempt_id TEXT PRIMARY KEY REFERENCES assignments(attempt_id),
    replacement_attempt_id TEXT NOT NULL UNIQUE REFERENCES assignments(attempt_id),
    replacement_worker_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

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
CREATE TABLE IF NOT EXISTS server_outbox_frames (
    connection_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    kind INTEGER NOT NULL,
    attempt_id TEXT,
    created_at_ms INTEGER NOT NULL,
    acknowledged_at_ms INTEGER,
    PRIMARY KEY(connection_id, sequence)
);
CREATE INDEX IF NOT EXISTS server_outbox_unacknowledged
    ON server_outbox_frames(connection_id, acknowledged_at_ms, sequence);
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
    Preparing = 10,
    Dispatchable = 1,
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
            1 => Ok(Self::Dispatchable),
            2 => Ok(Self::Sent),
            3 => Ok(Self::Accepted),
            4 => Ok(Self::Running),
            5 => Ok(Self::Finished),
            6 => Ok(Self::Rejected),
            7 => Ok(Self::LeaseExpired),
            8 => Ok(Self::CancelRequested),
            9 => Ok(Self::Cancelled),
            10 => Ok(Self::Preparing),
            _ => Err(RepositoryError::Corrupt(format!(
                "unknown attempt state {value}"
            ))),
        }
    }

    const fn is_replayable(self) -> bool {
        matches!(
            self,
            Self::Dispatchable
                | Self::Sent
                | Self::Accepted
                | Self::Running
                | Self::CancelRequested
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReassignmentRecord {
    pub outcome: StoreAssignmentOutcome,
    pub assignment: AssignmentRecord,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum ServerFrameKind {
    Assignment = 1,
    Cancel = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerOutboxFrame {
    pub connection_id: String,
    pub sequence: u64,
    pub message_id: String,
    pub worker_id: String,
    pub kind: ServerFrameKind,
    pub attempt_id: Option<String>,
}

/// All durable inputs required before an assignment frame may be published.
///
/// Repository implementations must apply the attempt transition, lease grant, outbox insert, and
/// connection sequence update in one transaction. Returning successfully is the application's
/// permission to place the corresponding frame on the network.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentDeliveryPreparation {
    pub frame: ServerOutboxFrame,
    pub lease_id: String,
    pub last_worker_sequence: u64,
    pub last_server_acknowledged_by_worker: u64,
    pub now_ms: u64,
    pub lease_duration_ms: u64,
}

/// Storage failures are kept distinct from RPC validation failures.
#[derive(Debug)]
pub enum RepositoryError {
    Storage(Box<dyn Error + Send + Sync>),
    Encoding(Box<dyn Error + Send + Sync>),
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
            Self::Storage(error) => write!(formatter, "control repository storage error: {error}"),
            Self::Encoding(error) => {
                write!(formatter, "control repository encoding error: {error}")
            }
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
            Self::Storage(error) | Self::Encoding(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

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

    /// Makes a fully prepared assignment eligible for dispatch and replay.
    ///
    /// Returns `true` only when this call performs the `Preparing -> Dispatchable` transition.
    fn mark_assignment_dispatchable(
        &self,
        attempt_id: &str,
        worker_id: &str,
        at_ms: u64,
    ) -> Result<bool, RepositoryError>;

    fn assignment(&self, attempt_id: &str) -> Result<Option<AssignmentRecord>, RepositoryError>;

    fn preparing_assignments(&self, limit: usize)
    -> Result<Vec<AssignmentRecord>, RepositoryError>;

    fn preparing_assignment_count(&self) -> Result<usize, RepositoryError>;

    fn defer_assignment_preparation(
        &self,
        attempt_id: &str,
        worker_id: &str,
        retry_at_ms: u64,
    ) -> Result<bool, RepositoryError>;

    fn replayable_assignments(
        &self,
        worker_id: &str,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError>;

    fn reassign_expired(
        &self,
        expired_attempt_id: &str,
        replacement_worker_id: &str,
        replacement_attempt_id: &str,
        at_ms: u64,
    ) -> Result<ReassignmentRecord, RepositoryError>;

    fn prepare_assignment_delivery(
        &self,
        preparation: &AssignmentDeliveryPreparation,
    ) -> Result<AssignmentContract, RepositoryError>;

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

    fn record_server_frame(
        &self,
        frame: &ServerOutboxFrame,
        now_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn compact_server_frames(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
        now_ms: u64,
    ) -> Result<usize, RepositoryError>;

    fn server_outbox_len(&self, connection_id: &str) -> Result<usize, RepositoryError>;

    fn prune_orphaned_server_frames(
        &self,
        disconnected_before_ms: u64,
    ) -> Result<usize, RepositoryError>;

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
        if !column_exists(&transaction, "server_outbox_frames", "message_id")? {
            transaction.execute_batch(
                "ALTER TABLE server_outbox_frames
                 ADD COLUMN message_id TEXT NOT NULL DEFAULT '';",
            )?;
        }
        transaction.execute_batch(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);
             INSERT OR IGNORE INTO schema_migrations(version) VALUES (3);",
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

    fn record_server_frame(
        &self,
        frame: &ServerOutboxFrame,
        now_ms: u64,
    ) -> Result<(), RepositoryError> {
        self.connection()?.execute(
            "INSERT INTO server_outbox_frames(
                 connection_id, sequence, message_id, worker_id, kind, attempt_id, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                frame.connection_id,
                to_i64(frame.sequence)?,
                frame.message_id,
                frame.worker_id,
                frame.kind as i64,
                frame.attempt_id,
                to_i64(now_ms)?
            ],
        )?;
        Ok(())
    }

    fn compact_server_frames(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
        now_ms: u64,
    ) -> Result<usize, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE server_outbox_frames SET acknowledged_at_ms = ?3
             WHERE connection_id = ?1 AND sequence <= ?2 AND acknowledged_at_ms IS NULL",
            params![
                connection_id,
                to_i64(acknowledged_through)?,
                to_i64(now_ms)?
            ],
        )?;
        let deleted = transaction.execute(
            "DELETE FROM server_outbox_frames
             WHERE connection_id = ?1 AND acknowledged_at_ms IS NOT NULL",
            [connection_id],
        )?;
        transaction.commit()?;
        Ok(deleted)
    }

    fn server_outbox_len(&self, connection_id: &str) -> Result<usize, RepositoryError> {
        let count = self.connection()?.query_row(
            "SELECT COUNT(*) FROM server_outbox_frames WHERE connection_id = ?1",
            [connection_id],
            |row| row.get::<_, i64>(0),
        )?;
        usize::try_from(count)
            .map_err(|_| RepositoryError::Corrupt(format!("negative outbox count {count}")))
    }

    fn prune_orphaned_server_frames(
        &self,
        disconnected_before_ms: u64,
    ) -> Result<usize, RepositoryError> {
        self.connection()?
            .execute(
                "DELETE FROM server_outbox_frames
                 WHERE connection_id IN (
                     SELECT connection_id FROM worker_connections
                     WHERE disconnected_at_ms IS NOT NULL AND disconnected_at_ms < ?1
                 )",
                [to_i64(disconnected_before_ms)?],
            )
            .map_err(RepositoryError::from)
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

fn assignment_in_transaction(
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

fn existing_reassignment(
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

fn insert_reassignment(
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
    replacement_attempt_id.clone_into(&mut replacement.contract.attempt_id);
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
            replacement.contract.attempt_id,
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
        assert_eq!(recovered.state, AttemptState::Preparing);
        Ok(())
    }

    #[test]
    fn preparing_assignment_is_not_replayable_until_side_effects_complete()
    -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;

        assert!(repository.replayable_assignments("worker-1")?.is_empty());
        assert!(repository.mark_assignment_dispatchable("attempt-1", "worker-1", 1_001)?);
        assert!(!repository.mark_assignment_dispatchable("attempt-1", "worker-1", 1_002)?);
        assert_eq!(repository.replayable_assignments("worker-1")?.len(), 1);
        Ok(())
    }

    #[test]
    fn deferred_preparation_rotates_behind_newer_work() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        let mut second = contract();
        second.assignment_id = "assignment-2".into();
        second.attempt_id = "attempt-2".into();
        repository.store_assignment("worker-1", &second, 1_001)?;

        assert_eq!(
            repository.preparing_assignments(1)?[0].contract.attempt_id,
            "attempt-1"
        );
        assert!(repository.defer_assignment_preparation("attempt-1", "worker-1", 2_000)?);
        assert_eq!(
            repository.preparing_assignments(1)?[0].contract.attempt_id,
            "attempt-2"
        );
        Ok(())
    }

    #[test]
    fn assignment_delivery_rolls_back_lease_and_state_when_outbox_insert_fails()
    -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;

        let mut second = contract();
        second.assignment_id = "assignment-2".into();
        second.attempt_id = "attempt-2".into();
        repository.store_assignment("worker-1", &second, 1_001)?;
        repository.mark_assignment_dispatchable("attempt-2", "worker-1", 1_001)?;
        let failed = repository.prepare_assignment_delivery(&AssignmentDeliveryPreparation {
            frame: ServerOutboxFrame {
                connection_id: "test-connection".into(),
                sequence: 1_000,
                message_id: "assignment:attempt-2".into(),
                worker_id: "worker-1".into(),
                kind: ServerFrameKind::Assignment,
                attempt_id: Some("attempt-2".into()),
            },
            lease_id: "lease:attempt-2".into(),
            last_worker_sequence: 1,
            last_server_acknowledged_by_worker: 0,
            now_ms: 1_001,
            lease_duration_ms: 100,
        });
        assert!(failed.is_err());
        assert_eq!(
            repository
                .assignment("attempt-2")?
                .expect("failed preparation keeps the assignment")
                .state,
            AttemptState::Dispatchable
        );
        assert!(repository.lease("attempt-2")?.is_none());
        let persisted_sequence = repository.connection()?.query_row(
            "SELECT last_server_sequence FROM worker_connections WHERE connection_id = ?1",
            ["test-connection"],
            |row| row.get::<_, i64>(0),
        )?;
        assert_eq!(persisted_sequence, 1_000);
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
        assert!(column_exists_raw(
            &database,
            "server_outbox_frames",
            "message_id"
        )?);
        let version_three: i64 = database.query_row(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = 3",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(version_three, 1);
        Ok(())
    }

    #[test]
    fn lease_expiry_retains_a_late_result_as_stale() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        let contract = contract();
        repository.store_assignment("worker-1", &contract, 1_000)?;
        prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
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
    fn expired_attempt_reassignment_creates_a_fresh_linked_contract() -> Result<(), Box<dyn Error>>
    {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
        assert!(matches!(
            repository.reassign_expired("attempt-1", "worker-2", "attempt-2", 1_050),
            Err(RepositoryError::InvalidTransition {
                from: AttemptState::Sent,
                to: AttemptState::Dispatchable,
            })
        ));
        assert_eq!(repository.expire_leases(1_100)?, vec!["attempt-1"]);

        let reassigned =
            repository.reassign_expired("attempt-1", "worker-2", "attempt-2", 1_101)?;
        assert_eq!(reassigned.outcome, StoreAssignmentOutcome::Inserted);
        assert_eq!(reassigned.assignment.worker_id, "worker-2");
        assert_eq!(reassigned.assignment.contract.attempt_id, "attempt-2");
        assert_eq!(reassigned.assignment.contract.attempt_number, 2);
        assert_eq!(reassigned.assignment.state, AttemptState::Preparing);
        assert_eq!(
            repository
                .assignment("attempt-1")?
                .expect("expired source remains auditable")
                .state,
            AttemptState::LeaseExpired
        );
        assert_eq!(
            repository
                .reassign_expired("attempt-1", "worker-2", "attempt-2", 1_102)?
                .outcome,
            StoreAssignmentOutcome::Duplicate
        );

        assert_eq!(
            repository.observe_attempt(&observation(
                1_103,
                AttemptObservation::Finished(FinishedObservation {
                    outcome: 1,
                    exit_code: Some(0),
                    elapsed_ms: 100,
                    receipt: None,
                    stdout: None,
                    stderr: None,
                    detail: "late old result".to_owned(),
                }),
            ))?,
            ObservationDisposition::Stale
        );
        Ok(())
    }

    #[test]
    fn active_heartbeat_renews_the_lease() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
        repository.renew_active_leases("worker-1", &["attempt-1".to_owned()], 1_050, 100)?;
        assert!(repository.expire_leases(1_100)?.is_empty());
        assert_eq!(repository.expire_leases(1_150)?, vec!["attempt-1"]);
        Ok(())
    }

    #[test]
    fn heartbeat_after_expiry_cannot_resurrect_a_lease() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.store_assignment("worker-1", &contract(), 1_000)?;
        prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
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
        prepare_test_assignment(&repository, "attempt-2", 2_000, 100)?;
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

    #[test]
    fn server_outbox_compacts_only_cumulatively_acknowledged_frames() -> Result<(), Box<dyn Error>>
    {
        let repository = SqliteControlRepository::in_memory()?;
        for (sequence, kind) in [
            (2, ServerFrameKind::Assignment),
            (3, ServerFrameKind::Cancel),
        ] {
            repository.record_server_frame(
                &ServerOutboxFrame {
                    connection_id: "connection-1".to_owned(),
                    sequence,
                    message_id: format!("fixture:{sequence}"),
                    worker_id: "worker-1".to_owned(),
                    kind,
                    attempt_id: Some("attempt-1".to_owned()),
                },
                1_000 + sequence,
            )?;
        }
        assert_eq!(repository.server_outbox_len("connection-1")?, 2);
        assert_eq!(
            repository.compact_server_frames("connection-1", 2, 1_010)?,
            1
        );
        assert_eq!(repository.server_outbox_len("connection-1")?, 1);
        assert_eq!(
            repository.compact_server_frames("connection-1", 3, 1_011)?,
            1
        );
        assert_eq!(repository.server_outbox_len("connection-1")?, 0);
        Ok(())
    }

    #[test]
    fn orphaned_server_frames_are_retained_until_the_policy_cutoff() -> Result<(), Box<dyn Error>> {
        let repository = SqliteControlRepository::in_memory()?;
        repository.register_worker(
            &WorkerRegistration {
                protocol_major: 1,
                protocol_minor: 2,
                worker_id: "worker-1".to_owned(),
                instance_id: "instance-1".to_owned(),
                worker_version: "test".to_owned(),
                features: Vec::new(),
                capabilities: WorkerCapabilities {
                    backend: 1,
                    architecture: "test".to_owned(),
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: "test".to_owned(),
                    toolkit_version: "test".to_owned(),
                    container_runtime: "test".to_owned(),
                },
            },
            &ConnectionRegistration {
                connection_id: "connection-1".to_owned(),
                worker_id: "worker-1".to_owned(),
                instance_id: "instance-1".to_owned(),
                connected_at_ms: 1_000,
            },
        )?;
        repository.record_server_frame(
            &ServerOutboxFrame {
                connection_id: "connection-1".to_owned(),
                sequence: 2,
                message_id: "assignment:attempt-1".to_owned(),
                worker_id: "worker-1".to_owned(),
                kind: ServerFrameKind::Assignment,
                attempt_id: Some("attempt-1".to_owned()),
            },
            1_001,
        )?;
        repository.disconnect("connection-1", 1_010)?;

        assert_eq!(repository.prune_orphaned_server_frames(1_010)?, 0);
        assert_eq!(repository.server_outbox_len("connection-1")?, 1);
        assert_eq!(repository.prune_orphaned_server_frames(1_011)?, 1);
        assert_eq!(repository.server_outbox_len("connection-1")?, 0);
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

    fn prepare_test_assignment(
        repository: &SqliteControlRepository,
        attempt_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError> {
        let connection_id = "test-connection";
        let connection_exists = repository.connection()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM worker_connections WHERE connection_id = ?1)",
            [connection_id],
            |row| row.get::<_, bool>(0),
        )?;
        if !connection_exists {
            repository.register_worker(
                &WorkerRegistration {
                    protocol_major: 1,
                    protocol_minor: 0,
                    worker_id: "worker-1".into(),
                    instance_id: "test-instance".into(),
                    worker_version: "test".into(),
                    features: Vec::new(),
                    capabilities: WorkerCapabilities {
                        backend: 1,
                        architecture: "test".into(),
                        device_count: 1,
                        max_concurrency: 1,
                        driver_version: "test".into(),
                        toolkit_version: "test".into(),
                        container_runtime: "test".into(),
                    },
                },
                &ConnectionRegistration {
                    connection_id: connection_id.into(),
                    worker_id: "worker-1".into(),
                    instance_id: "test-instance".into(),
                    connected_at_ms: now_ms,
                },
            )?;
        }
        repository.mark_assignment_dispatchable(attempt_id, "worker-1", now_ms)?;
        repository.prepare_assignment_delivery(&AssignmentDeliveryPreparation {
            frame: ServerOutboxFrame {
                connection_id: connection_id.into(),
                sequence: now_ms,
                message_id: format!("assignment:{attempt_id}"),
                worker_id: "worker-1".into(),
                kind: ServerFrameKind::Assignment,
                attempt_id: Some(attempt_id.into()),
            },
            lease_id: format!("lease:{attempt_id}"),
            last_worker_sequence: 1,
            last_server_acknowledged_by_worker: 0,
            now_ms,
            lease_duration_ms,
        })?;
        Ok(())
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
