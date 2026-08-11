//! Server-side worker sessions backed by a crash-durable control repository.

pub mod adapters;
pub mod artifact;
mod assignment_coordinator;
mod attempt_observer;
mod control_transport;
pub mod identity;
pub mod interaction;
pub mod interaction_service;
pub mod storage;

use adapters::sqlite::{SqliteControlRepository, SqliteInteractionStore};
use alloyport_artifacts::upload::{ArtifactReferenceKind, GrantArtifactReference};
use alloyport_artifacts::{Sha256Digest, SqliteUploadStore};
use alloyport_events::{
    Authority, Event, EventEnvelope, OutputStream as EventOutputStream, Producer, ProducerEvent,
    Visibility,
};
use alloyport_proto::v1::{
    Assignment, AssignmentAccepted, AssignmentRejected, CancelAttempt, CancellationAcknowledged,
    ControlAcknowledgement, ExecutionFinished, ExecutionStarted, ExecutorKind, Heartbeat,
    OutputChunk, OutputStream as WorkerOutputStream, ServerToWorker, ServerWelcome, WorkerHello,
    WorkerStatus, WorkerToServer, server_to_worker, worker_to_server,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR, ValidationError, validate_assignment};
use control_transport::{
    artifact_to_identity, assignment_to_contract, contract_to_assignment, event_artifact,
    expected_worker_message_id, hello_to_registration, interaction_status, repository_status,
    worker_event,
};
use identity::{ConnectionIdentityResolver, ResolvedConnectionIdentity};
use interaction::{
    AppendOutcome, InteractionError, InteractionHub, InteractionStore, RunGrantOutcome,
    RunRevokeOutcome,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use storage::{
    AssignmentContract, AssignmentDeliveryPreparation, AttemptObservation,
    CancellationStoreOutcome, Clock, ConnectionRegistration, ControlRepository,
    FinishedObservation, ObservationDisposition, ObservedAttempt, RepositoryError, ServerFrameKind,
    ServerOutboxFrame, StoreAssignmentOutcome, SystemClock,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

const HEARTBEAT_INTERVAL_MS: u64 = 5_000;
const ATTEMPT_LEASE_MS: u64 = 30_000;
const LEASE_REAPER_INTERVAL_MS: u64 = 1_000;
const PREPARATION_RECONCILE_INTERVAL_MS: u64 = 5_000;
const PREPARATION_RECONCILE_BATCH_SIZE: usize = 128;
const OUTBOX_ORPHAN_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const INTERACTION_LIVE_CAPACITY: usize = 1_024;
const INTERACTION_REPLAY_BATCH_SIZE: usize = 256;

pub use storage::{AttemptState as AssignmentState, LeaseRecord, ManualClock};

/// Read-only worker registry view for scheduling and diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerSnapshot {
    pub worker_id: String,
    pub instance_id: String,
    pub connection_id: String,
    pub connected: bool,
    pub last_worker_sequence: u64,
    pub backend: i32,
}

/// Result of submitting an immutable attempt to one worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    Sent,
    Pending,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    Sent,
    Pending,
    CancelledBeforeSend,
    AlreadyTerminal,
}

/// One assignment that could not complete durable preparation during a reconciliation pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparationReconciliationFailure {
    pub attempt_id: String,
    pub detail: String,
}

/// Observable result of one bounded assignment-preparation reconciliation pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreparationReconciliationReport {
    pub scanned: usize,
    pub recovered: usize,
    pub sent: usize,
    pub pending_delivery: usize,
    pub failures: Vec<PreparationReconciliationFailure>,
}

/// A server-side assignment cannot be admitted.
#[derive(Debug)]
pub enum EnqueueError {
    Invalid(ValidationError),
    ConflictingAttempt(String),
    Repository(RepositoryError),
    Interaction(InteractionError),
    Artifact(String),
}

impl Display for EnqueueError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => Display::fmt(error, formatter),
            Self::ConflictingAttempt(attempt_id) => {
                write!(
                    formatter,
                    "attempt {attempt_id} was reused with different content"
                )
            }
            Self::Repository(error) => Display::fmt(error, formatter),
            Self::Interaction(error) => Display::fmt(error, formatter),
            Self::Artifact(detail) => write!(formatter, "assignment Artifact error: {detail}"),
        }
    }
}

impl Error for EnqueueError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::Interaction(error) => Some(error),
            Self::ConflictingAttempt(_) | Self::Artifact(_) => None,
        }
    }
}

impl From<ValidationError> for EnqueueError {
    fn from(error: ValidationError) -> Self {
        Self::Invalid(error)
    }
}

impl From<RepositoryError> for EnqueueError {
    fn from(error: RepositoryError) -> Self {
        match error {
            RepositoryError::ConflictingAttempt(attempt_id) => Self::ConflictingAttempt(attempt_id),
            other => Self::Repository(other),
        }
    }
}

impl From<InteractionError> for EnqueueError {
    fn from(error: InteractionError) -> Self {
        Self::Interaction(error)
    }
}

#[derive(Debug)]
struct WorkerRecord {
    hello: WorkerHello,
    connection_id: String,
    connected: bool,
    last_worker_sequence: u64,
    last_server_sequence_acknowledged: u64,
    next_server_sequence: u64,
    sender: mpsc::Sender<Result<ServerToWorker, Status>>,
}

#[derive(Debug, Default)]
struct ControlState {
    workers: BTreeMap<String, WorkerRecord>,
}

/// Cloneable implementation of the worker-facing gRPC service.
#[derive(Clone, Debug)]
pub struct WorkerControlService {
    state: Arc<Mutex<ControlState>>,
    repository: Arc<dyn ControlRepository>,
    clock: Arc<dyn Clock>,
    identity_resolver: Option<Arc<dyn ConnectionIdentityResolver>>,
    artifact_metadata: Option<Arc<SqliteUploadStore>>,
    interactions: Arc<dyn InteractionStore>,
    connection_counter: Arc<AtomicU64>,
    lease_counter: Arc<AtomicU64>,
}

impl Default for WorkerControlService {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerControlService {
    /// Creates an ephemeral service. The server binary uses [`Self::open_sqlite`] instead.
    ///
    /// # Panics
    ///
    /// Panics only if the bundled `SQLite` library cannot create an in-memory database.
    #[must_use]
    pub fn new() -> Self {
        let repository = SqliteControlRepository::in_memory()
            .expect("an in-memory SQLite control repository must initialize");
        Self::with_repository(Arc::new(repository), Arc::new(SystemClock))
    }

    /// Opens a service whose control state survives process restart.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` cannot open or migrate the database.
    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        Self::open_sqlite_with_interaction_hub(path).map(|(service, _)| service)
    }

    /// Opens durable control state and a shared replay-to-live interaction hub.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` or the interaction hub cannot initialize.
    pub fn open_sqlite_with_interaction_hub(
        path: impl AsRef<Path>,
    ) -> Result<(Self, Arc<InteractionHub>), RepositoryError> {
        let path = path.as_ref();
        let repository = Arc::new(SqliteControlRepository::open(path)?);
        let durable: Arc<dyn InteractionStore> =
            Arc::new(SqliteInteractionStore::open(path).map_err(|error| {
                RepositoryError::Corrupt(format!("initialize interaction store: {error}"))
            })?);
        let hub = Arc::new(
            InteractionHub::new(
                durable,
                INTERACTION_LIVE_CAPACITY,
                INTERACTION_REPLAY_BATCH_SIZE,
            )
            .map_err(|error| {
                RepositoryError::Corrupt(format!("initialize interaction hub: {error}"))
            })?,
        );
        let interactions: Arc<dyn InteractionStore> = hub.clone();
        let service = Self::with_repositories(repository, interactions, Arc::new(SystemClock));
        Ok((service, hub))
    }

    /// Builds a service around an injected repository and clock.
    ///
    /// # Panics
    ///
    /// Panics only if the bundled `SQLite` library cannot initialize an in-memory event store.
    #[must_use]
    pub fn with_repository(repository: Arc<dyn ControlRepository>, clock: Arc<dyn Clock>) -> Self {
        let interactions = SqliteInteractionStore::in_memory()
            .expect("an in-memory SQLite interaction store must initialize");
        Self::with_repositories(repository, Arc::new(interactions), clock)
    }

    /// Builds a service around injected control and interaction repositories.
    #[must_use]
    pub fn with_repositories(
        repository: Arc<dyn ControlRepository>,
        interactions: Arc<dyn InteractionStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ControlState::default())),
            repository,
            clock,
            identity_resolver: None,
            artifact_metadata: None,
            interactions,
            connection_counter: Arc::new(AtomicU64::new(unique_seed())),
            lease_counter: Arc::new(AtomicU64::new(unique_seed())),
        }
    }

    /// Returns the canonical interaction stream for one task/run.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable event repository cannot be read.
    pub fn interaction_events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError> {
        self.interactions.events(run_id)
    }

    /// Requires every worker stream to match a verified, enrolled connection identity.
    #[must_use]
    pub fn require_identity_resolver(
        mut self,
        identity_resolver: Arc<dyn ConnectionIdentityResolver>,
    ) -> Self {
        self.identity_resolver = Some(identity_resolver);
        self
    }

    /// Requires terminal Artifact identities to match finalized uploads and creates typed roots.
    #[must_use]
    pub fn with_artifact_metadata(mut self, uploads: Arc<SqliteUploadStore>) -> Self {
        self.artifact_metadata = Some(uploads);
        self
    }

    /// Returns the latest in-process registry record for a logical worker.
    pub async fn worker_snapshot(&self, worker_id: &str) -> Option<WorkerSnapshot> {
        let state = self.state.lock().await;
        state.workers.get(worker_id).map(|worker| WorkerSnapshot {
            worker_id: worker.hello.worker_id.clone(),
            instance_id: worker.hello.instance_id.clone(),
            connection_id: worker.connection_id.clone(),
            connected: worker.connected,
            last_worker_sequence: worker.last_worker_sequence,
            backend: worker
                .hello
                .capabilities
                .as_ref()
                .map_or(0, |capabilities| capabilities.backend),
        })
    }

    /// Returns the durable lifecycle state for an attempt.
    ///
    /// # Errors
    ///
    /// Returns a repository error rather than treating a failed read as a missing attempt.
    pub fn assignment_state(
        &self,
        attempt_id: &str,
    ) -> Result<Option<AssignmentState>, RepositoryError> {
        self.repository
            .assignment(attempt_id)
            .map(|record| record.map(|record| record.state))
    }

    /// Returns the current durable lease for an attempt.
    ///
    /// # Errors
    ///
    /// Returns a repository error when the lease cannot be read.
    pub fn lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError> {
        self.repository.lease(attempt_id)
    }

    /// Expires every due non-terminal lease according to the injected clock.
    ///
    /// # Errors
    ///
    /// Returns a repository error if expiry cannot be committed atomically.
    pub fn expire_leases(&self) -> Result<Vec<String>, RepositoryError> {
        self.repository.expire_leases(self.clock.now_unix_ms())
    }

    /// Runs the periodic durable lease-expiry loop until its task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns the first repository error instead of silently stopping lease expiry.
    pub async fn run_lease_reaper(&self) -> Result<(), RepositoryError> {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(LEASE_REAPER_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.expire_leases()?;
        }
    }

    /// Completes one bounded batch of assignments left in `Preparing` by a failed enqueue or
    /// process crash.
    ///
    /// Per-assignment dependency failures are reported and remain retryable. Repository failures
    /// that prevent a trustworthy scan or state transition abort the pass.
    ///
    /// # Errors
    ///
    /// Returns a repository error if the durable preparation set cannot be read or updated.
    async fn register(
        &self,
        hello: WorkerHello,
        sender: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) -> Result<(String, Vec<ServerToWorker>), RepositoryError> {
        let number = self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let connection_id = format!("connection-{number}");
        let worker_id = hello.worker_id.clone();
        let negotiated_protocol_minor = hello.protocol_minor.min(PROTOCOL_MINOR);
        let now_ms = self.clock.now_unix_ms();
        self.repository.expire_leases(now_ms)?;
        self.repository
            .prune_orphaned_server_frames(now_ms.saturating_sub(OUTBOX_ORPHAN_RETENTION_MS))?;
        self.repository.register_worker(
            &hello_to_registration(&hello),
            &ConnectionRegistration {
                connection_id: connection_id.clone(),
                worker_id: worker_id.clone(),
                instance_id: hello.instance_id.clone(),
                connected_at_ms: now_ms,
            },
        )?;

        {
            let mut state = self.state.lock().await;
            state.workers.insert(
                worker_id.clone(),
                WorkerRecord {
                    hello,
                    connection_id: connection_id.clone(),
                    connected: true,
                    last_worker_sequence: 1,
                    last_server_sequence_acknowledged: 0,
                    next_server_sequence: 2,
                    sender,
                },
            );
        }

        let mut messages = vec![ServerToWorker {
            sequence: 1,
            acknowledges_worker_through: 1,
            message_id: String::new(),
            message: Some(server_to_worker::Message::Welcome(ServerWelcome {
                connection_id: connection_id.clone(),
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: negotiated_protocol_minor,
                heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
                attempt_lease_ms: ATTEMPT_LEASE_MS,
            })),
        }];
        let pending = self.repository.replayable_assignments(&worker_id)?;
        for assignment in pending {
            let cancellation_reason = assignment.cancellation_reason.clone();
            if let Some((_, message)) = self
                .prepare_assignment(&worker_id, &assignment.contract.attempt_id)
                .await?
            {
                messages.push(message);
            }
            if let Some(reason) = cancellation_reason
                && let Some((_, message)) = self
                    .prepare_cancel(&worker_id, &assignment.contract.attempt_id, &reason)
                    .await?
            {
                messages.push(message);
            }
        }
        Ok((connection_id, messages))
    }

    async fn disconnect(&self, worker_id: &str, connection_id: &str) {
        let _ = self
            .repository
            .disconnect(connection_id, self.clock.now_unix_ms());
        let mut state = self.state.lock().await;
        if let Some(worker) = state.workers.get_mut(worker_id)
            && worker.connection_id == connection_id
        {
            worker.connected = false;
        }
    }

    async fn consume_stream(
        self,
        worker_id: String,
        connection_id: String,
        authenticated_identity: Option<ResolvedConnectionIdentity>,
        mut inbound: Streaming<WorkerToServer>,
        outbound: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) {
        loop {
            match inbound.next().await {
                Some(Ok(frame)) => {
                    if let Some(identity) = authenticated_identity.as_ref()
                        && let Some(resolver) = self.identity_resolver.as_ref()
                        && let Err(status) = resolver.revalidate(identity)
                    {
                        let _ = outbound.send(Err(status)).await;
                        break;
                    }
                    match self.ingest(&worker_id, &connection_id, frame).await {
                        Ok(true) => {
                            match self.prepare_transport_ack(&worker_id, &connection_id).await {
                                Ok(Some((sender, message))) => {
                                    if sender.send(Ok(message)).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(error) => {
                                    let _ = outbound.send(Err(repository_status(error))).await;
                                    break;
                                }
                            }
                        }
                        Ok(false) => {}
                        Err(status) => {
                            let _ = outbound.send(Err(status)).await;
                            break;
                        }
                    }
                }
                Some(Err(status)) => {
                    let _ = outbound.send(Err(status)).await;
                    break;
                }
                None => break,
            }
        }
        self.disconnect(&worker_id, &connection_id).await;
    }
}

fn validate_and_grant_finished_artifacts(
    uploads: &SqliteUploadStore,
    worker_id: &str,
    attempt_id: &str,
    finished: &ExecutionFinished,
    now_ms: u64,
) -> Result<(), Status> {
    for (artifact, reference_key, kind, purpose) in [
        (
            finished.stdout.as_ref(),
            format!("output:{attempt_id}:stdout"),
            ArtifactReferenceKind::AssignmentOutput,
            "complete attempt stdout",
        ),
        (
            finished.stderr.as_ref(),
            format!("output:{attempt_id}:stderr"),
            ArtifactReferenceKind::AssignmentOutput,
            "complete attempt stderr",
        ),
        (
            finished.receipt.as_ref(),
            format!("receipt:{attempt_id}"),
            ArtifactReferenceKind::Receipt,
            "attempt run receipt",
        ),
    ] {
        let artifact = artifact.ok_or_else(|| {
            Status::failed_precondition(format!(
                "terminal execution is missing required Artifact {reference_key}"
            ))
        })?;
        let digest = Sha256Digest::from_str(&artifact.digest)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let uploaded = uploads
            .completed_upload_session_by_key(worker_id, &reference_key)
            .map_err(artifact::upload_status)?
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "terminal Artifact {reference_key} is not finalized by worker {worker_id}"
                ))
            })?;
        let uploaded_identity = uploaded.artifact.ok_or_else(|| {
            Status::data_loss(format!(
                "completed upload {reference_key} lacks its Artifact identity"
            ))
        })?;
        if uploaded_identity.digest != digest
            || uploaded_identity.size_bytes != artifact.size_bytes
            || uploaded.media_type != artifact.media_type
        {
            return Err(Status::data_loss(format!(
                "terminal Artifact {reference_key} does not match its finalized upload"
            )));
        }
        uploads
            .grant_reference(&GrantArtifactReference {
                owner_id: worker_id.to_owned(),
                reference_key,
                digest,
                kind,
                purpose: purpose.to_owned(),
                now_ms,
                retained_until_ms: None,
            })
            .map_err(artifact::upload_status)?;
    }
    Ok(())
}

fn unique_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn validate_worker_acknowledgement(
    acknowledged: u64,
    last_acknowledged: u64,
    sent_server_through: u64,
) -> Result<(), Status> {
    if acknowledged < last_acknowledged {
        return Err(Status::invalid_argument(format!(
            "worker acknowledgement regressed from {last_acknowledged} to {acknowledged}"
        )));
    }
    if acknowledged > sent_server_through {
        return Err(Status::invalid_argument(format!(
            "worker acknowledged server sequence {acknowledged} beyond sent sequence {sent_server_through}"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
