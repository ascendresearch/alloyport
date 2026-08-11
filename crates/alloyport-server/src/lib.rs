//! Server-side worker sessions backed by a crash-durable control repository.

pub mod adapters;
pub mod artifact;
pub mod identity;
pub mod interaction;
pub mod interaction_service;
pub mod storage;

use adapters::sqlite::{SqliteControlRepository, SqliteInteractionStore};
use alloyport_artifacts::upload::{ArtifactReferenceKind, GrantArtifactReference};
use alloyport_artifacts::{Sha256Digest, SqliteUploadStore};
use alloyport_events::{
    ArtifactRef as EventArtifactRef, Authority, Event, EventEnvelope,
    OutputStream as EventOutputStream, Producer, ProducerEvent, Visibility,
};
use alloyport_proto::v1::worker_control_server::WorkerControl;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, AssignmentAccepted, AssignmentRejected, CancelAttempt,
    CancellationAcknowledged, ControlAcknowledgement, EnvironmentVariable, ExecutionFinished,
    ExecutionSpec, ExecutionStarted, ExecutorKind, Heartbeat, OutputChunk,
    OutputStream as WorkerOutputStream, ResourceLimits, ServerToWorker, ServerWelcome, WorkerHello,
    WorkerStatus, WorkerToServer, server_to_worker, worker_to_server,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR, ValidationError, validate_assignment};
use identity::{ConnectionIdentityResolver, ResolvedConnectionIdentity};
use interaction::{
    AppendOutcome, InteractionError, InteractionHub, InteractionStore, RunGrantOutcome,
    RunRevokeOutcome,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use storage::{
    ArtifactIdentity, AssignmentContract, AssignmentDeliveryPreparation, AttemptObservation,
    CancellationStoreOutcome, Clock, ConnectionRegistration, ControlRepository, EnvironmentEntry,
    ExecutionContract, FinishedObservation, ObservationDisposition, ObservedAttempt,
    RepositoryError, ResourceContract, ServerFrameKind, ServerOutboxFrame, StoreAssignmentOutcome,
    SystemClock, WorkerCapabilities, WorkerRegistration,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

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
    pub async fn reconcile_preparing_assignments(
        &self,
    ) -> Result<PreparationReconciliationReport, RepositoryError> {
        let assignments = self
            .repository
            .preparing_assignments(PREPARATION_RECONCILE_BATCH_SIZE)?;
        let mut report = PreparationReconciliationReport {
            scanned: assignments.len(),
            ..PreparationReconciliationReport::default()
        };
        for assignment in assignments {
            let attempt_id = assignment.contract.attempt_id.clone();
            let now_ms = self.clock.now_unix_ms();
            if let Err(error) = self.grant_cuda_assignment_input(
                &assignment.worker_id,
                &assignment.contract,
                now_ms,
            ) {
                self.repository.defer_assignment_preparation(
                    &assignment.contract.attempt_id,
                    &assignment.worker_id,
                    now_ms,
                )?;
                report.failures.push(PreparationReconciliationFailure {
                    attempt_id,
                    detail: error.to_string(),
                });
                continue;
            }
            if let Err(error) = self.record_run_started(&assignment.contract, now_ms) {
                self.repository.defer_assignment_preparation(
                    &assignment.contract.attempt_id,
                    &assignment.worker_id,
                    now_ms,
                )?;
                report.failures.push(PreparationReconciliationFailure {
                    attempt_id,
                    detail: error.to_string(),
                });
                continue;
            }
            if !self.repository.mark_assignment_dispatchable(
                &assignment.contract.attempt_id,
                &assignment.worker_id,
                now_ms,
            )? {
                continue;
            }
            report.recovered += 1;
            match self
                .prepare_assignment(&assignment.worker_id, &assignment.contract.attempt_id)
                .await
            {
                Ok(Some((sender, message))) => {
                    if sender.send(Ok(message)).await.is_ok() {
                        report.sent += 1;
                    } else {
                        self.mark_send_failed(&assignment.worker_id).await;
                        report.pending_delivery += 1;
                    }
                }
                Ok(None) => report.pending_delivery += 1,
                Err(error) => report.failures.push(PreparationReconciliationFailure {
                    attempt_id,
                    detail: error.to_string(),
                }),
            }
        }
        Ok(report)
    }

    /// Reconciles every assignment that was preparing when startup began, using bounded queries.
    /// Rows deferred by one pass are rotated behind unseen work, preventing one unavailable
    /// Artifact from starving the rest of the startup recovery set.
    ///
    /// # Errors
    ///
    /// Returns a repository error if the recovery set cannot be counted, read, or updated.
    pub async fn reconcile_preparing_assignments_at_startup(
        &self,
    ) -> Result<PreparationReconciliationReport, RepositoryError> {
        let count = self.repository.preparing_assignment_count()?;
        let passes = count.div_ceil(PREPARATION_RECONCILE_BATCH_SIZE);
        let mut aggregate = PreparationReconciliationReport::default();
        for _ in 0..passes {
            let report = self.reconcile_preparing_assignments().await?;
            aggregate.scanned += report.scanned;
            aggregate.recovered += report.recovered;
            aggregate.sent += report.sent;
            aggregate.pending_delivery += report.pending_delivery;
            aggregate.failures.extend(report.failures);
        }
        Ok(aggregate)
    }

    /// Reconciles abandoned assignment preparation periodically until cancelled.
    ///
    /// # Errors
    ///
    /// Returns the first repository failure that prevents a trustworthy reconciliation pass.
    pub async fn run_preparation_reconciler(&self) -> Result<(), RepositoryError> {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            PREPARATION_RECONCILE_INTERVAL_MS,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let _report = self.reconcile_preparing_assignments().await?;
        }
    }

    /// Persists and, if connected, sends an assignment to a named worker.
    ///
    /// The immutable contract is committed before a send is prepared. Preparing the send commits
    /// its lease and `Sent` observation before the frame is placed on the network channel.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] for an invalid assignment, repository failure, or an attempt
    /// identifier reused with different content.
    pub async fn enqueue_assignment(
        &self,
        worker_id: impl Into<String>,
        assignment: Assignment,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        validate_assignment(&assignment)?;
        let worker_id = worker_id.into();
        let contract = assignment_to_contract(&assignment);
        let now_ms = self.clock.now_unix_ms();
        let stored = self
            .repository
            .store_assignment(&worker_id, &contract, now_ms)?;
        self.grant_cuda_assignment_input(&worker_id, &contract, now_ms)?;
        self.record_run_started(&contract, now_ms)?;
        let became_dispatchable = self.repository.mark_assignment_dispatchable(
            &contract.attempt_id,
            &worker_id,
            self.clock.now_unix_ms(),
        )?;
        if stored == StoreAssignmentOutcome::Duplicate && !became_dispatchable {
            return Ok(EnqueueOutcome::Duplicate);
        }

        let outbound = self
            .prepare_assignment(&worker_id, &contract.attempt_id)
            .await?;
        let Some((sender, message)) = outbound else {
            return Ok(EnqueueOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&worker_id).await;
            return Ok(EnqueueOutcome::Pending);
        }
        Ok(EnqueueOutcome::Sent)
    }

    /// Grants one owner access to the run, then persists and dispatches its assignment.
    ///
    /// The explicit owner comes from the trusted controller call site, never from a worker frame.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] for invalid input, a terminally revoked grant, or enqueue failure.
    pub async fn enqueue_assignment_for_owner(
        &self,
        owner_id: &str,
        worker_id: impl Into<String>,
        assignment: Assignment,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        validate_assignment(&assignment)?;
        self.interactions.grant_run_access(
            &assignment.task_id,
            owner_id,
            self.clock.now_unix_ms(),
        )?;
        self.enqueue_assignment(worker_id, assignment).await
    }

    /// Adds an idempotent public-read grant for an existing or planned run.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, storage failure, or a terminally revoked grant.
    pub fn grant_interaction_access(
        &self,
        run_id: &str,
        owner_id: &str,
    ) -> Result<RunGrantOutcome, InteractionError> {
        self.interactions
            .grant_run_access(run_id, owner_id, self.clock.now_unix_ms())
    }

    /// Revokes an existing public-read grant idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error when the grant is unknown or cannot be durably updated.
    pub fn revoke_interaction_access(
        &self,
        run_id: &str,
        owner_id: &str,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        self.interactions
            .revoke_run_access(run_id, owner_id, self.clock.now_unix_ms())
    }

    /// Creates and dispatches a new process attempt for one durably expired attempt.
    ///
    /// The replacement copies the immutable assignment contract, increments its attempt number,
    /// and uses the caller-supplied fresh attempt ID. The expired record remains authoritative for
    /// classifying any late observations from the old worker.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] unless the source attempt is lease-expired, the replacement ID is
    /// fresh, and the copied assignment remains valid.
    pub async fn reassign_expired_attempt(
        &self,
        expired_attempt_id: &str,
        replacement_worker_id: impl Into<String>,
        replacement_attempt_id: impl Into<String>,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        let replacement_worker_id = replacement_worker_id.into();
        let replacement_attempt_id = replacement_attempt_id.into();
        let reassignment = self.repository.reassign_expired(
            expired_attempt_id,
            &replacement_worker_id,
            &replacement_attempt_id,
            self.clock.now_unix_ms(),
        )?;
        validate_assignment(&contract_to_assignment(&reassignment.assignment.contract))?;
        self.grant_cuda_assignment_input(
            &replacement_worker_id,
            &reassignment.assignment.contract,
            self.clock.now_unix_ms(),
        )?;
        self.record_run_started(&reassignment.assignment.contract, self.clock.now_unix_ms())?;
        let became_dispatchable = self.repository.mark_assignment_dispatchable(
            &replacement_attempt_id,
            &replacement_worker_id,
            self.clock.now_unix_ms(),
        )?;
        if reassignment.outcome == StoreAssignmentOutcome::Duplicate && !became_dispatchable {
            return Ok(EnqueueOutcome::Duplicate);
        }
        let outbound = self
            .prepare_assignment(&replacement_worker_id, &replacement_attempt_id)
            .await?;
        let Some((sender, message)) = outbound else {
            return Ok(EnqueueOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&replacement_worker_id).await;
            return Ok(EnqueueOutcome::Pending);
        }
        Ok(EnqueueOutcome::Sent)
    }

    fn grant_cuda_assignment_input(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<(), EnqueueError> {
        if ExecutorKind::try_from(contract.execution.executor_kind)
            .unwrap_or(ExecutorKind::Unspecified)
            != ExecutorKind::CudaFixture
        {
            return Ok(());
        }
        let uploads = self.artifact_metadata.as_ref().ok_or_else(|| {
            EnqueueError::Artifact(
                "CUDA fixture assignments require the Artifact metadata service".into(),
            )
        })?;
        let digest = Sha256Digest::from_str(&contract.execution.bundle.digest)
            .map_err(|error| EnqueueError::Artifact(error.to_string()))?;
        let stored_size = uploads
            .artifact_size_bytes(digest)
            .map_err(|error| EnqueueError::Artifact(error.to_string()))?
            .ok_or_else(|| {
                EnqueueError::Artifact(format!("input bundle {digest} is not published"))
            })?;
        if stored_size != contract.execution.bundle.size_bytes {
            return Err(EnqueueError::Artifact(format!(
                "input bundle {digest} has size {stored_size}, assignment declares {}",
                contract.execution.bundle.size_bytes
            )));
        }
        uploads
            .grant_reference(&GrantArtifactReference {
                owner_id: worker_id.to_owned(),
                reference_key: format!("input:{}:bundle", contract.attempt_id),
                digest,
                kind: ArtifactReferenceKind::AssignmentInput,
                purpose: "CUDA fixture input bundle".into(),
                now_ms,
                retained_until_ms: None,
            })
            .map_err(|error| EnqueueError::Artifact(error.to_string()))?;
        Ok(())
    }

    /// Durably requests cancellation and sends it when the owning worker is connected.
    ///
    /// # Errors
    ///
    /// Returns a repository error when the attempt is unknown or the request cannot be committed.
    pub async fn cancel_attempt(
        &self,
        attempt_id: &str,
        reason: impl Into<String>,
    ) -> Result<CancelOutcome, RepositoryError> {
        let reason = reason.into();
        let cancellation =
            self.repository
                .request_cancellation(attempt_id, &reason, self.clock.now_unix_ms())?;
        match cancellation.outcome {
            CancellationStoreOutcome::CancelledBeforeSend => {
                return Ok(CancelOutcome::CancelledBeforeSend);
            }
            CancellationStoreOutcome::AlreadyTerminal => {
                return Ok(CancelOutcome::AlreadyTerminal);
            }
            CancellationStoreOutcome::Requested | CancellationStoreOutcome::Duplicate => {}
        }
        let outbound = self
            .prepare_cancel(&cancellation.worker_id, attempt_id, &reason)
            .await?;
        let Some((sender, message)) = outbound else {
            return Ok(CancelOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&cancellation.worker_id).await;
            return Ok(CancelOutcome::Pending);
        }
        Ok(CancelOutcome::Sent)
    }

    async fn prepare_assignment(
        &self,
        worker_id: &str,
        attempt_id: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if !worker.connected {
            return Ok(None);
        }
        let sequence = worker.next_server_sequence;
        let lease_number = self.lease_counter.fetch_add(1, Ordering::Relaxed);
        let lease_id = format!("lease-{lease_number}");
        let now_ms = self.clock.now_unix_ms();
        let message_id = format!("assignment:{attempt_id}");
        let contract =
            self.repository
                .prepare_assignment_delivery(&AssignmentDeliveryPreparation {
                    frame: ServerOutboxFrame {
                        connection_id: worker.connection_id.clone(),
                        sequence,
                        message_id: message_id.clone(),
                        worker_id: worker_id.to_owned(),
                        kind: ServerFrameKind::Assignment,
                        attempt_id: Some(attempt_id.to_owned()),
                    },
                    lease_id,
                    last_worker_sequence: worker.last_worker_sequence,
                    last_server_acknowledged_by_worker: worker.last_server_sequence_acknowledged,
                    now_ms,
                    lease_duration_ms: ATTEMPT_LEASE_MS,
                })?;
        worker.next_server_sequence += 1;
        Ok(Some((
            worker.sender.clone(),
            ServerToWorker {
                sequence,
                acknowledges_worker_through: worker.last_worker_sequence,
                message_id,
                message: Some(server_to_worker::Message::Assignment(
                    contract_to_assignment(&contract),
                )),
            },
        )))
    }

    async fn mark_send_failed(&self, worker_id: &str) {
        let mut state = self.state.lock().await;
        if let Some(worker) = state.workers.get_mut(worker_id) {
            worker.connected = false;
        }
    }

    async fn prepare_cancel(
        &self,
        worker_id: &str,
        attempt_id: &str,
        reason: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if !worker.connected {
            return Ok(None);
        }
        let sequence = worker.next_server_sequence;
        let now_ms = self.clock.now_unix_ms();
        self.repository.record_server_frame(
            &ServerOutboxFrame {
                connection_id: worker.connection_id.clone(),
                sequence,
                message_id: format!("cancel:{attempt_id}"),
                worker_id: worker_id.to_owned(),
                kind: ServerFrameKind::Cancel,
                attempt_id: Some(attempt_id.to_owned()),
            },
            now_ms,
        )?;
        worker.next_server_sequence += 1;
        self.repository.update_connection_sequences(
            &worker.connection_id,
            worker.last_worker_sequence,
            sequence,
            worker.last_server_sequence_acknowledged,
            now_ms,
        )?;
        Ok(Some((
            worker.sender.clone(),
            ServerToWorker {
                sequence,
                acknowledges_worker_through: worker.last_worker_sequence,
                message_id: format!("cancel:{attempt_id}"),
                message: Some(server_to_worker::Message::Cancel(CancelAttempt {
                    attempt_id: attempt_id.to_owned(),
                    reason: reason.to_owned(),
                })),
            },
        )))
    }

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

    async fn ingest(
        &self,
        worker_id: &str,
        connection_id: &str,
        frame: WorkerToServer,
    ) -> Result<bool, Status> {
        let mut state = self.state.lock().await;
        let worker = state
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| Status::failed_precondition("worker is not registered"))?;
        if worker.connection_id != connection_id || !worker.connected {
            return Err(Status::aborted("worker connection was superseded"));
        }
        if frame.sequence != worker.last_worker_sequence + 1 {
            return Err(Status::invalid_argument(format!(
                "worker sequence gap: expected {}, got {}",
                worker.last_worker_sequence + 1,
                frame.sequence
            )));
        }
        let sent_server_through = worker.next_server_sequence.saturating_sub(1);
        validate_worker_acknowledgement(
            frame.acknowledges_server_through,
            worker.last_server_sequence_acknowledged,
            sent_server_through,
        )?;

        let durable_message_id = expected_worker_message_id(frame.message.as_ref());
        let supports_durable_message_ids = worker.hello.protocol_minor >= 2;
        if supports_durable_message_ids {
            if let Some(expected) = durable_message_id.as_ref()
                && frame.message_id != *expected
            {
                return Err(Status::invalid_argument(format!(
                    "worker message ID must be {expected}"
                )));
            }
            if durable_message_id.is_none() && !frame.message_id.is_empty() {
                return Err(Status::invalid_argument(
                    "ephemeral worker frame cannot carry a message ID",
                ));
            }
        }

        let now_ms = self.clock.now_unix_ms();
        match frame.message {
            Some(worker_to_server::Message::Heartbeat(heartbeat)) => {
                self.observe_heartbeat(worker_id, &heartbeat, now_ms)?;
            }
            Some(worker_to_server::Message::Status(status)) => {
                Self::observe_status(worker, status);
            }
            Some(worker_to_server::Message::AssignmentAccepted(accepted)) => {
                self.observe_accepted(worker_id, accepted, now_ms)?;
            }
            Some(worker_to_server::Message::AssignmentRejected(rejected)) => {
                self.observe_rejection(worker_id, rejected, now_ms)?;
            }
            Some(worker_to_server::Message::ExecutionStarted(started)) => {
                self.observe_started(worker_id, started, now_ms)?;
            }
            Some(worker_to_server::Message::ExecutionFinished(finished)) => {
                self.observe_finished(worker_id, &finished, now_ms)?;
            }
            Some(worker_to_server::Message::CancellationAcknowledged(acknowledged)) => {
                self.observe_cancellation_acknowledged(worker_id, acknowledged, now_ms)?;
            }
            Some(worker_to_server::Message::OutputChunk(output)) => {
                self.observe_output(worker_id, &output, now_ms)?;
            }
            Some(worker_to_server::Message::Hello(_)) => {
                return Err(Status::invalid_argument(
                    "hello is only valid as the first frame",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "worker message payload is missing",
                ));
            }
        }

        worker.last_worker_sequence = frame.sequence;
        worker.last_server_sequence_acknowledged = frame.acknowledges_server_through;
        self.repository
            .compact_server_frames(connection_id, frame.acknowledges_server_through, now_ms)
            .map_err(repository_status)?;
        self.repository
            .update_connection_sequences(
                connection_id,
                worker.last_worker_sequence,
                worker.next_server_sequence.saturating_sub(1),
                worker.last_server_sequence_acknowledged,
                now_ms,
            )
            .map_err(repository_status)?;
        Ok(supports_durable_message_ids && durable_message_id.is_some())
    }

    async fn prepare_transport_ack(
        &self,
        worker_id: &str,
        connection_id: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if !worker.connected || worker.connection_id != connection_id {
            return Ok(None);
        }
        let sequence = worker.next_server_sequence;
        worker.next_server_sequence += 1;
        self.repository.update_connection_sequences(
            connection_id,
            worker.last_worker_sequence,
            sequence,
            worker.last_server_sequence_acknowledged,
            self.clock.now_unix_ms(),
        )?;
        Ok(Some((
            worker.sender.clone(),
            ServerToWorker {
                sequence,
                acknowledges_worker_through: worker.last_worker_sequence,
                message_id: String::new(),
                message: Some(server_to_worker::Message::Acknowledgement(
                    ControlAcknowledgement {},
                )),
            },
        )))
    }

    fn record_run_started(
        &self,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<AppendOutcome, InteractionError> {
        let mut frame = ProducerEvent::new(
            contract.task_id.clone(),
            Producer::new("alloyport-server", "controller"),
            Event::RunStarted {
                task: contract.task_id.clone(),
            },
        );
        frame.task_id = Some(contract.task_id.clone());
        frame.emitted_at_unix_ms = now_ms;
        frame.authority = Authority::Observed;
        frame.visibility = Visibility::User;
        self.interactions
            .append(&format!("task:{}:run-started", contract.task_id), &frame)
    }

    fn record_command_started(
        &self,
        worker_id: &str,
        attempt_id: &str,
        now_ms: u64,
    ) -> Result<AppendOutcome, Status> {
        let assignment = self
            .repository
            .assignment(attempt_id)
            .map_err(repository_status)?
            .ok_or_else(|| Status::failed_precondition("started attempt is unknown"))?;
        let frame = worker_event(
            &assignment.contract,
            worker_id,
            now_ms,
            Event::CommandStarted {
                command: assignment.contract.execution.argv.join(" "),
                cwd: Some(assignment.contract.execution.working_directory.clone()),
                execution_site: worker_id.to_owned(),
                description: Some("worker assignment execution".into()),
            },
        );
        self.interactions
            .append(&format!("attempt:{attempt_id}:command-started"), &frame)
            .map_err(|error| interaction_status(&error))
    }

    fn record_command_finished(
        &self,
        worker_id: &str,
        finished: &ExecutionFinished,
        now_ms: u64,
    ) -> Result<(), Status> {
        self.record_command_started(worker_id, &finished.attempt_id, now_ms)?;
        let assignment = self
            .repository
            .assignment(&finished.attempt_id)
            .map_err(repository_status)?
            .ok_or_else(|| Status::failed_precondition("finished attempt is unknown"))?;
        for (artifact, reference, suffix) in [
            (finished.stdout.as_ref(), "stdout", "stdout"),
            (finished.stderr.as_ref(), "stderr", "stderr"),
            (finished.receipt.as_ref(), "receipt", "receipt"),
        ] {
            let Some(artifact) = artifact else {
                continue;
            };
            let frame = worker_event(
                &assignment.contract,
                worker_id,
                now_ms,
                Event::ArtifactProduced {
                    artifact: event_artifact(artifact, reference),
                },
            );
            self.interactions
                .append(
                    &format!("attempt:{}:artifact:{suffix}", finished.attempt_id),
                    &frame,
                )
                .map_err(|error| interaction_status(&error))?;
        }
        let completion = worker_event(
            &assignment.contract,
            worker_id,
            now_ms,
            Event::CommandCompleted {
                exit_code: finished.exit_code.unwrap_or(-1),
                elapsed_ms: finished.elapsed_ms,
                timed_out: finished.outcome
                    == i32::from(alloyport_proto::v1::AttemptOutcome::TimedOut),
                output_artifact: finished
                    .stdout
                    .as_ref()
                    .map(|artifact| event_artifact(artifact, "stdout")),
            },
        );
        self.interactions
            .append(
                &format!("attempt:{}:command-completed", finished.attempt_id),
                &completion,
            )
            .map_err(|error| interaction_status(&error))?;
        Ok(())
    }

    fn observe_heartbeat(
        &self,
        worker_id: &str,
        heartbeat: &Heartbeat,
        now_ms: u64,
    ) -> Result<(), Status> {
        let active_attempts = heartbeat
            .active_attempts
            .iter()
            .map(|attempt| attempt.attempt_id.clone())
            .collect::<Vec<_>>();
        self.repository
            .renew_active_leases(worker_id, &active_attempts, now_ms, ATTEMPT_LEASE_MS)
            .map_err(repository_status)
    }

    fn observe_status(_worker: &mut WorkerRecord, _status: WorkerStatus) {}

    fn observe_accepted(
        &self,
        worker_id: &str,
        accepted: AssignmentAccepted,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            accepted.assignment_id,
            accepted.attempt_id,
            now_ms,
            AttemptObservation::Accepted {
                already_known: accepted.already_known,
            },
        )
    }

    fn observe_rejection(
        &self,
        worker_id: &str,
        rejected: AssignmentRejected,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            rejected.assignment_id,
            rejected.attempt_id,
            now_ms,
            AttemptObservation::Rejected {
                reason: rejected.reason,
                detail: rejected.detail,
            },
        )
    }

    fn observe_started(
        &self,
        worker_id: &str,
        started: ExecutionStarted,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        let attempt_id = started.attempt_id.clone();
        let disposition = self.observe(
            worker_id,
            started.assignment_id,
            started.attempt_id,
            now_ms,
            AttemptObservation::Started,
        )?;
        self.record_command_started(worker_id, &attempt_id, now_ms)?;
        Ok(disposition)
    }

    fn observe_finished(
        &self,
        worker_id: &str,
        finished: &ExecutionFinished,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        if let Some(uploads) = self.artifact_metadata.as_ref() {
            validate_and_grant_finished_artifacts(
                uploads,
                worker_id,
                &finished.attempt_id,
                finished,
                now_ms,
            )?;
        }
        let observation = FinishedObservation {
            outcome: finished.outcome,
            exit_code: finished.exit_code,
            elapsed_ms: finished.elapsed_ms,
            receipt: finished.receipt.as_ref().map(artifact_to_identity),
            stdout: finished.stdout.as_ref().map(artifact_to_identity),
            stderr: finished.stderr.as_ref().map(artifact_to_identity),
            detail: finished.detail.clone(),
        };
        let disposition = self.observe(
            worker_id,
            finished.assignment_id.clone(),
            finished.attempt_id.clone(),
            now_ms,
            AttemptObservation::Finished(observation),
        )?;
        self.record_command_finished(worker_id, finished, now_ms)?;
        Ok(disposition)
    }

    fn observe_output(
        &self,
        worker_id: &str,
        output: &OutputChunk,
        now_ms: u64,
    ) -> Result<(), Status> {
        let assignment = self
            .repository
            .assignment(&output.attempt_id)
            .map_err(repository_status)?
            .ok_or_else(|| Status::failed_precondition("output attempt is unknown"))?;
        if assignment.worker_id != worker_id {
            return Err(Status::permission_denied(format!(
                "attempt {} belongs to another worker",
                output.attempt_id
            )));
        }
        if assignment.state != AssignmentState::Running {
            return Err(Status::failed_precondition(format!(
                "output attempt {} is not running",
                output.attempt_id
            )));
        }
        let stream =
            WorkerOutputStream::try_from(output.stream).unwrap_or(WorkerOutputStream::Unspecified);
        let event_stream = match stream {
            WorkerOutputStream::Stdout => EventOutputStream::Stdout,
            WorkerOutputStream::Stderr => EventOutputStream::Stderr,
            WorkerOutputStream::Unspecified => {
                return Err(Status::invalid_argument("output stream is unspecified"));
            }
        };
        let text = String::from_utf8_lossy(&output.payload);
        let display_sanitized =
            output.display_sanitized || matches!(text, std::borrow::Cow::Owned(_));
        let frame = worker_event(
            &assignment.contract,
            worker_id,
            now_ms,
            Event::CommandOutput {
                stream: event_stream,
                byte_offset: output.byte_offset,
                text: text.into_owned(),
                display_sanitized,
            },
        );
        let appended = self
            .interactions
            .append_output(
                &format!(
                    "attempt:{}:output:{}:{}",
                    output.attempt_id, output.stream, output.byte_offset
                ),
                &output.attempt_id,
                output.stream,
                output.byte_offset,
                &output.payload,
                &frame,
            )
            .map_err(|error| interaction_status(&error))?;
        if appended.missing_bytes_before != 0 {
            let expected = output
                .byte_offset
                .saturating_sub(appended.missing_bytes_before);
            let warning = worker_event(
                &assignment.contract,
                worker_id,
                now_ms,
                Event::Warning {
                    message: format!(
                        "live {stream:?} preview omitted bytes {expected}..{}; complete output remains in the terminal Artifact",
                        output.byte_offset
                    ),
                },
            );
            self.interactions
                .append(
                    &format!(
                        "attempt:{}:output-gap:{}:{expected}:{}",
                        output.attempt_id, output.stream, output.byte_offset
                    ),
                    &warning,
                )
                .map_err(|error| interaction_status(&error))?;
        }
        Ok(())
    }

    fn observe_cancellation_acknowledged(
        &self,
        worker_id: &str,
        acknowledged: CancellationAcknowledged,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            acknowledged.assignment_id,
            acknowledged.attempt_id,
            now_ms,
            AttemptObservation::CancellationAcknowledged {
                already_terminal: acknowledged.already_terminal,
            },
        )
    }

    fn observe(
        &self,
        worker_id: &str,
        assignment_id: String,
        attempt_id: String,
        observed_at_ms: u64,
        observation: AttemptObservation,
    ) -> Result<ObservationDisposition, Status> {
        self.repository
            .observe_attempt(&ObservedAttempt {
                assignment_id,
                attempt_id,
                worker_id: worker_id.to_owned(),
                observed_at_ms,
                observation,
            })
            .map_err(repository_status)
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

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    type OpenControlStreamStream =
        Pin<Box<dyn Stream<Item = Result<ServerToWorker, Status>> + Send + 'static>>;

    async fn open_control_stream(
        &self,
        request: Request<Streaming<WorkerToServer>>,
    ) -> Result<Response<Self::OpenControlStreamStream>, Status> {
        let authenticated_identity = self
            .identity_resolver
            .as_ref()
            .map(|resolver| resolver.resolve_identity(request.extensions()))
            .transpose()?;
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("worker stream ended before hello"))?;
        if first.sequence != 1 {
            return Err(Status::invalid_argument("hello must have sequence 1"));
        }
        if first.acknowledges_server_through != 0 {
            return Err(Status::invalid_argument(
                "hello cannot acknowledge a server connection that is not open",
            ));
        }
        if !first.message_id.is_empty() {
            return Err(Status::invalid_argument("hello cannot carry a message ID"));
        }
        let Some(worker_to_server::Message::Hello(hello)) = first.message else {
            return Err(Status::invalid_argument(
                "first worker message must be hello",
            ));
        };
        alloyport_proto::validate_worker_hello(&hello)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if authenticated_identity
            .as_ref()
            .is_some_and(|identity| identity.owner_id != hello.worker_id)
        {
            return Err(Status::permission_denied(
                "worker hello identity does not match the enrolled client certificate",
            ));
        }
        if let Some(identity) = authenticated_identity.as_ref()
            && let Some(resolver) = self.identity_resolver.as_ref()
        {
            resolver.revalidate(identity)?;
        }

        let worker_id = hello.worker_id.clone();
        let (outbound, receiver) = mpsc::channel(64);
        let (connection_id, initial_messages) = self
            .register(hello, outbound.clone())
            .await
            .map_err(repository_status)?;
        for message in initial_messages {
            outbound
                .send(Ok(message))
                .await
                .map_err(|_| Status::unavailable("worker response stream closed"))?;
        }

        tokio::spawn(self.clone().consume_stream(
            worker_id,
            connection_id,
            authenticated_identity,
            inbound,
            outbound,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

fn repository_status(error: RepositoryError) -> Status {
    match error {
        RepositoryError::NotFound(detail) => Status::not_found(detail),
        RepositoryError::IdentityMismatch(detail) => Status::permission_denied(detail),
        RepositoryError::InvalidTransition { .. } => Status::failed_precondition(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

fn expected_worker_message_id(message: Option<&worker_to_server::Message>) -> Option<String> {
    let (kind, attempt_id) = match message? {
        worker_to_server::Message::AssignmentAccepted(accepted) => {
            ("assignment-accepted", accepted.attempt_id.as_str())
        }
        worker_to_server::Message::AssignmentRejected(rejected) => {
            ("assignment-rejected", rejected.attempt_id.as_str())
        }
        worker_to_server::Message::ExecutionStarted(started) => {
            ("execution-started", started.attempt_id.as_str())
        }
        worker_to_server::Message::ExecutionFinished(finished) => {
            ("execution-finished", finished.attempt_id.as_str())
        }
        worker_to_server::Message::CancellationAcknowledged(acknowledged) => (
            "cancellation-acknowledged",
            acknowledged.attempt_id.as_str(),
        ),
        worker_to_server::Message::Hello(_)
        | worker_to_server::Message::Heartbeat(_)
        | worker_to_server::Message::OutputChunk(_)
        | worker_to_server::Message::Status(_) => return None,
    };
    Some(format!("{kind}:{attempt_id}"))
}

fn hello_to_registration(hello: &WorkerHello) -> WorkerRegistration {
    let capabilities = hello
        .capabilities
        .as_ref()
        .expect("validated worker hello contains capabilities");
    WorkerRegistration {
        protocol_major: hello.protocol_major,
        protocol_minor: hello.protocol_minor,
        worker_id: hello.worker_id.clone(),
        instance_id: hello.instance_id.clone(),
        worker_version: hello.worker_version.clone(),
        features: hello.features.clone(),
        capabilities: WorkerCapabilities {
            backend: capabilities.backend,
            architecture: capabilities.architecture.clone(),
            device_count: capabilities.device_count,
            max_concurrency: capabilities.max_concurrency,
            driver_version: capabilities.driver_version.clone(),
            toolkit_version: capabilities.toolkit_version.clone(),
            container_runtime: capabilities.container_runtime.clone(),
        },
    }
}

fn assignment_to_contract(assignment: &Assignment) -> AssignmentContract {
    let execution = assignment
        .execution
        .as_ref()
        .expect("validated assignment contains execution");
    AssignmentContract {
        assignment_id: assignment.assignment_id.clone(),
        attempt_id: assignment.attempt_id.clone(),
        attempt_number: assignment.attempt_number,
        idempotency_key: assignment.idempotency_key.clone(),
        task_id: assignment.task_id.clone(),
        candidate_id: assignment.candidate_id.clone(),
        execution: ExecutionContract {
            executor_kind: execution.executor_kind,
            argv: execution.argv.clone(),
            working_directory: execution.working_directory.clone(),
            environment: execution
                .environment
                .iter()
                .map(|entry| EnvironmentEntry {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: execution.timeout_ms,
            bundle: artifact_to_identity(
                execution
                    .bundle
                    .as_ref()
                    .expect("validated assignment contains bundle"),
            ),
            image: artifact_to_identity(
                execution
                    .image
                    .as_ref()
                    .expect("validated assignment contains image"),
            ),
            limits: execution.limits.as_ref().map(|limits| ResourceContract {
                cpu_millis: limits.cpu_millis,
                memory_bytes: limits.memory_bytes,
                disk_bytes: limits.disk_bytes,
                process_count: limits.process_count,
                output_bytes: limits.output_bytes,
                device_count: limits.device_count,
                network: limits.network,
            }),
        },
        required_features: assignment.required_features.clone(),
    }
}

fn contract_to_assignment(contract: &AssignmentContract) -> Assignment {
    Assignment {
        assignment_id: contract.assignment_id.clone(),
        attempt_id: contract.attempt_id.clone(),
        attempt_number: contract.attempt_number,
        idempotency_key: contract.idempotency_key.clone(),
        task_id: contract.task_id.clone(),
        candidate_id: contract.candidate_id.clone(),
        execution: Some(ExecutionSpec {
            executor_kind: contract.execution.executor_kind,
            argv: contract.execution.argv.clone(),
            working_directory: contract.execution.working_directory.clone(),
            environment: contract
                .execution
                .environment
                .iter()
                .map(|entry| EnvironmentVariable {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: contract.execution.timeout_ms,
            bundle: Some(identity_to_artifact(&contract.execution.bundle)),
            image: Some(identity_to_artifact(&contract.execution.image)),
            limits: contract
                .execution
                .limits
                .as_ref()
                .map(|limits| ResourceLimits {
                    cpu_millis: limits.cpu_millis,
                    memory_bytes: limits.memory_bytes,
                    disk_bytes: limits.disk_bytes,
                    process_count: limits.process_count,
                    output_bytes: limits.output_bytes,
                    device_count: limits.device_count,
                    network: limits.network,
                }),
        }),
        required_features: contract.required_features.clone(),
    }
}

fn artifact_to_identity(artifact: &ArtifactRef) -> ArtifactIdentity {
    ArtifactIdentity {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

fn identity_to_artifact(identity: &ArtifactIdentity) -> ArtifactRef {
    ArtifactRef {
        digest: identity.digest.clone(),
        size_bytes: identity.size_bytes,
        media_type: identity.media_type.clone(),
    }
}

fn event_artifact(artifact: &ArtifactRef, reference: &str) -> EventArtifactRef {
    EventArtifactRef {
        digest: artifact.digest.clone(),
        media_type: artifact.media_type.clone(),
        size_bytes: artifact.size_bytes,
        reference: reference.into(),
    }
}

fn worker_event(
    contract: &AssignmentContract,
    worker_id: &str,
    emitted_at_unix_ms: u64,
    mut event: Event,
) -> ProducerEvent {
    interaction::redact_worker_event(&mut event);
    let mut frame = ProducerEvent::new(
        contract.task_id.clone(),
        Producer::new("alloyport-worker", worker_id),
        event,
    );
    frame.task_id = Some(contract.task_id.clone());
    frame.operation_id = Some(contract.attempt_id.clone());
    frame.emitted_at_unix_ms = emitted_at_unix_ms;
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame
}

fn interaction_status(error: &InteractionError) -> Status {
    let detail = error.to_string();
    match error {
        InteractionError::InvalidFrame(_)
        | InteractionError::ConflictingDedupKey(_)
        | InteractionError::ConflictingOutput { .. }
        | InteractionError::InvalidCursor { .. }
        | InteractionError::RevokedRunGrant { .. }
        | InteractionError::MissingRunGrant { .. }
        | InteractionError::ValueOutOfRange(_) => Status::invalid_argument(detail),
        InteractionError::Storage(_)
        | InteractionError::Encoding(_)
        | InteractionError::InvalidSubscriptionCapacity
        | InteractionError::LockPoisoned => Status::internal(detail),
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
mod tests {
    use super::*;
    use alloyport_artifacts::FilesystemArtifactStore;
    use alloyport_artifacts::upload::BeginUpload;

    #[test]
    fn worker_acknowledgement_must_be_monotonic_and_not_future() {
        assert!(validate_worker_acknowledgement(3, 2, 3).is_ok());
        assert_eq!(
            validate_worker_acknowledgement(1, 2, 3)
                .expect_err("regression is rejected")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            validate_worker_acknowledgement(4, 2, 3)
                .expect_err("future acknowledgement is rejected")
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn reconciliation_recovers_restart_residue_without_blocking_on_another_attempt()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("control.sqlite3");
        {
            let repository = SqliteControlRepository::open(&database)?;
            repository.store_assignment(
                "worker-1",
                &stored_contract("fake-attempt", ExecutorKind::Process),
                1_000,
            )?;
            repository.store_assignment(
                "worker-1",
                &stored_contract("cuda-attempt", ExecutorKind::CudaFixture),
                1_001,
            )?;
        }

        let service = WorkerControlService::open_sqlite(&database)?;
        let report = service.reconcile_preparing_assignments().await?;
        assert_eq!(report.scanned, 2);
        assert_eq!(report.recovered, 1);
        assert_eq!(report.pending_delivery, 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].attempt_id, "cuda-attempt");
        assert_eq!(
            service.assignment_state("fake-attempt")?,
            Some(AssignmentState::Dispatchable)
        );
        assert_eq!(
            service.assignment_state("cuda-attempt")?,
            Some(AssignmentState::Preparing)
        );
        assert_eq!(service.interaction_events("task-fake-attempt")?.len(), 1);

        let second = service.reconcile_preparing_assignments().await?;
        assert_eq!(second.scanned, 1);
        assert_eq!(second.recovered, 0);
        assert_eq!(second.failures.len(), 1);
        assert_eq!(service.interaction_events("task-fake-attempt")?.len(), 1);
        Ok(())
    }

    #[test]
    fn terminal_artifacts_must_be_finalized_by_the_reporting_worker() -> Result<(), Box<dyn Error>>
    {
        let directory = tempfile::tempdir()?;
        let uploads = SqliteUploadStore::open(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("uploads"),
            1_024,
            8,
        )?;
        let artifact = ArtifactRef {
            digest: format!("sha256:{}", "a".repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".into(),
        };
        let error = validate_and_grant_finished_artifacts(
            &uploads,
            "worker-1",
            "attempt-1",
            &ExecutionFinished {
                assignment_id: "assignment-1".into(),
                attempt_id: "attempt-1".into(),
                outcome: alloyport_proto::v1::AttemptOutcome::Succeeded.into(),
                exit_code: Some(0),
                elapsed_ms: 1,
                receipt: Some(artifact.clone()),
                stdout: Some(artifact.clone()),
                stderr: Some(artifact),
                detail: "untrusted terminal".into(),
            },
            1,
        )
        .expect_err("a wire digest cannot manufacture remote Artifact evidence");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        Ok(())
    }

    #[tokio::test]
    async fn cuda_assignment_grants_only_a_published_size_matched_input_bundle()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let uploads = Arc::new(SqliteUploadStore::open(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("upload-data"),
            1_024,
            1_024,
        )?);
        let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 1_024)?;
        let bytes = b"fixture bundle";
        let digest = Sha256Digest::digest_bytes(bytes);
        let session = uploads.begin(&BeginUpload {
            owner_id: "controller".into(),
            upload_key: "fixture:cuda-vectoradd-v1".into(),
            expected_digest: digest,
            expected_size_bytes: u64::try_from(bytes.len())?,
            media_type: "application/vnd.alloyport.cuda-fixture.v1+json".into(),
            now_ms: 1,
            expires_at_ms: 1_001,
        })?;
        uploads.append("controller", &session.upload_id, 0, bytes, 2)?;
        uploads.finalize("controller", &session.upload_id, &cas, 3)?;

        let assignment = Assignment {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            attempt_number: 1,
            idempotency_key: "cuda-vectoradd-v1".into(),
            task_id: "task-1".into(),
            candidate_id: "candidate-1".into(),
            execution: Some(ExecutionSpec {
                executor_kind: ExecutorKind::CudaFixture.into(),
                argv: vec!["cuda-vectoradd-v1".into()],
                working_directory: ".".into(),
                environment: Vec::new(),
                timeout_ms: 30_000,
                bundle: Some(ArtifactRef {
                    digest: digest.to_string(),
                    size_bytes: u64::try_from(bytes.len())?,
                    media_type: "application/vnd.alloyport.cuda-fixture.v1+json".into(),
                }),
                image: Some(ArtifactRef {
                    digest: format!("sha256:{}", "b".repeat(64)),
                    size_bytes: 0,
                    media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                }),
                limits: Some(ResourceLimits {
                    cpu_millis: 1_000,
                    memory_bytes: 1_024,
                    disk_bytes: 1_024,
                    process_count: 1,
                    output_bytes: 1_024,
                    device_count: 1,
                    network: alloyport_proto::v1::NetworkPolicy::Disabled.into(),
                }),
            }),
            required_features: vec!["cuda-fixture-v1".into()],
        };
        assert!(matches!(
            WorkerControlService::new()
                .enqueue_assignment("cuda-1", assignment.clone())
                .await,
            Err(EnqueueError::Artifact(_))
        ));
        let service = WorkerControlService::new().with_artifact_metadata(Arc::clone(&uploads));
        assert_eq!(
            service.enqueue_assignment("cuda-1", assignment).await?,
            EnqueueOutcome::Pending
        );
        let reference = uploads.reference("cuda-1", "input:attempt-1:bundle")?;
        assert_eq!(reference.digest, digest);
        assert_eq!(reference.kind, ArtifactReferenceKind::AssignmentInput);
        Ok(())
    }

    fn stored_contract(attempt_id: &str, executor_kind: ExecutorKind) -> AssignmentContract {
        AssignmentContract {
            assignment_id: format!("assignment-{attempt_id}"),
            attempt_id: attempt_id.into(),
            attempt_number: 1,
            idempotency_key: format!("key-{attempt_id}"),
            task_id: format!("task-{attempt_id}"),
            candidate_id: "candidate-1".into(),
            execution: ExecutionContract {
                executor_kind: executor_kind.into(),
                argv: vec!["fixture".into()],
                working_directory: ".".into(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: ArtifactIdentity {
                    digest: format!("sha256:{}", "a".repeat(64)),
                    size_bytes: 1,
                    media_type: "application/octet-stream".into(),
                },
                image: ArtifactIdentity {
                    digest: format!("sha256:{}", "b".repeat(64)),
                    size_bytes: 0,
                    media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                },
                limits: None,
            },
            required_features: Vec::new(),
        }
    }
}
