//! Outbound worker client and local assignment admission state.

pub mod artifact_download;
pub mod artifact_upload;
pub mod cuda;
pub mod cuda_docker;
pub mod cuda_runtime;
pub mod cuda_supervisor;
pub mod executor;
pub mod journal;

use alloyport_proto::v1::worker_control_client::WorkerControlClient;
use alloyport_proto::v1::{
    ActiveAttempt, ArtifactRef, Assignment, AssignmentAccepted, AssignmentRejected, AttemptOutcome,
    AttemptPhase, Backend, CancellationAcknowledged, ExecutionFinished, ExecutorKind, Heartbeat,
    OutputChunk, OutputStream, RejectionReason, ServerToWorker, WorkerHealth, WorkerHello,
    WorkerToServer, server_to_worker, worker_to_server,
};
use alloyport_proto::{ValidationError, validate_assignment, validate_worker_hello};
use cuda::CUDA_FIXTURE_FEATURE;
use cuda_runtime::CudaExecutionRuntime;
use executor::{
    ArtifactPublisher, CancellationToken, ExecutionObservation, ExecutionStream,
    FakeExecutionRuntime, FakeExecutor, terminal_reference_intents,
};
use journal::{
    AttemptStore, AttemptStoreError, LocalAttemptPhase, LocalAttemptRecord, SqliteAttemptStore,
    StoreAdmissionOutcome, StoredArtifact, StoredAssignment, StoredEnvironment, StoredExecution,
    StoredLimits, WorkerOutboxMessage, WorkerOutboxPayload,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Endpoint;

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const OUTBOX_DELIVERY_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

pub use journal::StoredFinished;

/// Whether an admitted attempt is new or an idempotent replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    New,
    Duplicate,
}

/// A worker cannot admit or communicate an assignment.
#[derive(Debug)]
pub enum WorkerError {
    InvalidHello(ValidationError),
    InvalidAssignment(ValidationError),
    ConflictingAttempt(String),
    PolicyViolation(String),
    AttemptStore(AttemptStoreError),
    Transport(tonic::transport::Error),
    Rpc(tonic::Status),
    Execution(String),
    Protocol(String),
    StreamClosed,
}

impl Display for WorkerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHello(error) | Self::InvalidAssignment(error) => {
                Display::fmt(error, formatter)
            }
            Self::ConflictingAttempt(attempt_id) => {
                write!(
                    formatter,
                    "attempt {attempt_id} was replayed with different content"
                )
            }
            Self::PolicyViolation(detail) => write!(formatter, "worker policy rejected: {detail}"),
            Self::AttemptStore(error) => Display::fmt(error, formatter),
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Rpc(error) => Display::fmt(error, formatter),
            Self::Execution(detail) => write!(formatter, "worker execution failed: {detail}"),
            Self::Protocol(detail) => write!(formatter, "worker protocol error: {detail}"),
            Self::StreamClosed => write!(formatter, "worker control stream closed"),
        }
    }
}

impl Error for WorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHello(error) | Self::InvalidAssignment(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Rpc(error) => Some(error),
            Self::AttemptStore(error) => Some(error),
            Self::ConflictingAttempt(_)
            | Self::PolicyViolation(_)
            | Self::Execution(_)
            | Self::Protocol(_)
            | Self::StreamClosed => None,
        }
    }
}

impl From<tonic::transport::Error> for WorkerError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<tonic::Status> for WorkerError {
    fn from(error: tonic::Status) -> Self {
        Self::Rpc(error)
    }
}

impl From<AttemptStoreError> for WorkerError {
    fn from(error: AttemptStoreError) -> Self {
        match error {
            AttemptStoreError::ConflictingAttempt(attempt_id) => {
                Self::ConflictingAttempt(attempt_id)
            }
            other => Self::AttemptStore(other),
        }
    }
}

/// Local rules that remain authoritative even for an authenticated server.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdmissionPolicy {
    allow_shell: bool,
    allow_cuda_fixture: bool,
    cuda_fixture_only: bool,
}

impl AdmissionPolicy {
    /// Returns a policy that permits the explicitly policy-gated shell executor.
    #[must_use]
    pub const fn allowing_shell(mut self) -> Self {
        self.allow_shell = true;
        self
    }

    /// Returns a policy that permits the dedicated, locally constrained CUDA fixture executor.
    #[must_use]
    pub const fn allowing_cuda_fixture(mut self) -> Self {
        self.allow_cuda_fixture = true;
        self
    }

    /// Returns a policy that permits only the locally constrained CUDA fixture executor.
    #[must_use]
    pub const fn cuda_fixture_only(mut self) -> Self {
        self.allow_cuda_fixture = true;
        self.cuda_fixture_only = true;
        self
    }
}

/// Worker-local policy facade over a durable attempt journal.
#[derive(Clone, Debug)]
pub struct WorkerState {
    policy: AdmissionPolicy,
    store: Arc<dyn AttemptStore>,
}

impl Default for WorkerState {
    fn default() -> Self {
        Self::with_policy(AdmissionPolicy::default())
    }
}

impl WorkerState {
    /// Creates an ephemeral journal with the supplied policy.
    ///
    /// # Panics
    ///
    /// Panics only if the bundled `SQLite` library cannot create an in-memory journal.
    #[must_use]
    pub fn with_policy(policy: AdmissionPolicy) -> Self {
        let store = SqliteAttemptStore::in_memory()
            .expect("an in-memory worker attempt journal must initialize");
        Self::with_store(policy, Arc::new(store))
    }

    #[must_use]
    pub fn with_store(policy: AdmissionPolicy, store: Arc<dyn AttemptStore>) -> Self {
        Self { policy, store }
    }

    /// Opens a crash-durable worker attempt journal.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` cannot open or migrate the journal.
    pub fn open_sqlite(
        policy: AdmissionPolicy,
        path: impl AsRef<Path>,
    ) -> Result<Self, WorkerError> {
        Ok(Self::with_store(
            policy,
            Arc::new(SqliteAttemptStore::open(path)?),
        ))
    }

    /// Validates and records an immutable attempt before acknowledging it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] if validation fails or the same attempt ID is reused for other bytes.
    pub fn admit(&self, assignment: &Assignment) -> Result<AdmissionOutcome, WorkerError> {
        validate_assignment(assignment).map_err(WorkerError::InvalidAssignment)?;
        if let Some(execution) = assignment.execution.as_ref() {
            let executor = ExecutorKind::try_from(execution.executor_kind)
                .unwrap_or(ExecutorKind::Unspecified);
            if self.policy.cuda_fixture_only && executor != ExecutorKind::CudaFixture {
                return Err(WorkerError::PolicyViolation(
                    "only the CUDA fixture executor is enabled".to_owned(),
                ));
            }
            if executor == ExecutorKind::Shell && !self.policy.allow_shell {
                return Err(WorkerError::PolicyViolation(
                    "shell executor is disabled".to_owned(),
                ));
            }
            if executor == ExecutorKind::CudaFixture && !self.policy.allow_cuda_fixture {
                return Err(WorkerError::PolicyViolation(
                    "CUDA fixture executor is disabled".to_owned(),
                ));
            }
        }
        let stored = assignment_to_stored(assignment);
        let outcome = self.store.admit(&stored, now_unix_ms())?;
        let admission = match outcome {
            StoreAdmissionOutcome::Inserted => AdmissionOutcome::New,
            StoreAdmissionOutcome::Duplicate => AdmissionOutcome::Duplicate,
        };
        Ok(admission)
    }

    /// Checks durable local attempt knowledge.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn contains_attempt(&self, attempt_id: &str) -> Result<bool, WorkerError> {
        self.store
            .attempt(attempt_id)
            .map(|attempt| attempt.is_some())
            .map_err(WorkerError::from)
    }

    /// Persists the transition that must precede starting an executor.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown attempt, invalid transition, or journal failure.
    pub fn mark_running(&self, attempt_id: &str) -> Result<(), WorkerError> {
        self.store
            .mark_running(attempt_id, now_unix_ms())
            .map_err(WorkerError::from)
    }

    /// Persists terminal result data before it can be reported to the server.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown attempt, conflicting terminal result, or journal failure.
    pub fn mark_finished(
        &self,
        attempt_id: &str,
        finished: &StoredFinished,
    ) -> Result<(), WorkerError> {
        self.store
            .mark_finished(attempt_id, finished, now_unix_ms())
            .map_err(WorkerError::from)
    }

    fn enqueue_lifecycle(&self, payload: WorkerOutboxPayload) -> Result<String, WorkerError> {
        let (message_id, attempt_id) = lifecycle_identity(&payload);
        self.store.enqueue_outbox(
            &WorkerOutboxMessage {
                message_id: message_id.clone(),
                attempt_id,
                payload,
            },
            now_unix_ms(),
        )?;
        Ok(message_id)
    }

    fn pending_outbox(&self) -> Result<Vec<WorkerOutboxMessage>, WorkerError> {
        self.store.pending_outbox().map_err(WorkerError::from)
    }

    fn record_delivery(
        &self,
        connection_id: &str,
        sequence: u64,
        message_id: &str,
    ) -> Result<(), WorkerError> {
        self.store
            .record_outbox_delivery(connection_id, sequence, message_id, now_unix_ms())
            .map_err(WorkerError::from)
    }

    fn acknowledge_outbox(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
    ) -> Result<usize, WorkerError> {
        self.store
            .acknowledge_outbox(connection_id, acknowledged_through)
            .map_err(WorkerError::from)
    }

    fn prune_old_deliveries(&self) -> Result<usize, WorkerError> {
        let retention_ms = u64::try_from(OUTBOX_DELIVERY_RETENTION.as_millis()).unwrap_or(u64::MAX);
        self.store
            .prune_outbox_deliveries(now_unix_ms().saturating_sub(retention_ms))
            .map_err(WorkerError::from)
    }

    /// Returns the number of durable lifecycle messages awaiting acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn outbox_len(&self) -> Result<usize, WorkerError> {
        self.store.outbox_len().map_err(WorkerError::from)
    }

    /// Returns durable terminal data for a locally known attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn finished_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<StoredFinished>, WorkerError> {
        self.attempt(attempt_id)
            .map(|attempt| attempt.and_then(|record| record.finished))
    }

    fn attempt(&self, attempt_id: &str) -> Result<Option<LocalAttemptRecord>, WorkerError> {
        self.store.attempt(attempt_id).map_err(WorkerError::from)
    }

    fn active_attempts(&self) -> Result<Vec<ActiveAttempt>, WorkerError> {
        self.store
            .attempts()?
            .into_iter()
            .map(|attempt| {
                Ok(ActiveAttempt {
                    assignment_id: attempt.assignment.assignment_id,
                    attempt_id: attempt.assignment.attempt_id,
                    phase: match attempt.phase {
                        LocalAttemptPhase::Accepted => AttemptPhase::Accepted,
                        LocalAttemptPhase::Running => AttemptPhase::Running,
                        LocalAttemptPhase::Finished => AttemptPhase::Finished,
                    }
                    .into(),
                })
            })
            .collect()
    }

    fn attempt_count(&self) -> Result<usize, WorkerError> {
        self.store
            .attempts()
            .map(|attempts| {
                attempts
                    .iter()
                    .filter(|attempt| attempt.phase != LocalAttemptPhase::Finished)
                    .count()
            })
            .map_err(WorkerError::from)
    }
}

/// One outbound worker identity with attempt state that survives stream reconnects in-process.
#[derive(Clone, Debug)]
pub struct OutboundWorker {
    endpoint: Endpoint,
    hello: WorkerHello,
    state: Arc<Mutex<WorkerState>>,
    execution: Option<Arc<ExecutionIntegration>>,
    artifact_publisher: Option<Arc<dyn ArtifactPublisher>>,
    execution_updates: broadcast::Sender<ExecutionUpdate>,
}

#[derive(Debug)]
struct ExecutionIntegration {
    attached: AttachedRuntime,
    active: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
}

#[derive(Debug)]
enum AttachedRuntime {
    Fake {
        runtime: Arc<FakeExecutionRuntime>,
        executor: Arc<FakeExecutor>,
    },
    Cuda(Arc<CudaExecutionRuntime>),
}

impl AttachedRuntime {
    fn supports(&self, executor: ExecutorKind) -> bool {
        match self {
            Self::Fake { .. } => executor != ExecutorKind::CudaFixture,
            Self::Cuda(_) => executor == ExecutorKind::CudaFixture,
        }
    }
}

#[derive(Clone, Debug)]
enum ExecutionUpdate {
    Observation {
        attempt_id: String,
        observation: ExecutionObservation,
    },
    Completed {
        attempt_id: String,
        result: Result<(), String>,
    },
}

impl OutboundWorker {
    /// Constructs a worker after validating its immutable hello contract.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] when the worker identity or capabilities are invalid.
    pub fn new(endpoint: Endpoint, hello: WorkerHello) -> Result<Self, WorkerError> {
        Self::with_state(endpoint, hello, WorkerState::default())
    }

    /// Constructs a worker whose attempt knowledge survives process restart.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid hello or a journal open/migration failure.
    pub fn open_sqlite(
        endpoint: Endpoint,
        hello: WorkerHello,
        path: impl AsRef<Path>,
    ) -> Result<Self, WorkerError> {
        Self::with_state(
            endpoint,
            hello,
            WorkerState::open_sqlite(AdmissionPolicy::default(), path)?,
        )
    }

    fn with_state(
        endpoint: Endpoint,
        hello: WorkerHello,
        state: WorkerState,
    ) -> Result<Self, WorkerError> {
        validate_worker_hello(&hello).map_err(WorkerError::InvalidHello)?;
        state.store.bind_worker(&hello.worker_id)?;
        let (execution_updates, _) = broadcast::channel(128);
        Ok(Self {
            endpoint,
            hello,
            state: Arc::new(Mutex::new(state)),
            execution: None,
            artifact_publisher: None,
            execution_updates,
        })
    }

    /// Attaches the deterministic fake executor to assignments admitted by this worker.
    ///
    /// This is an explicit integration seam for control-plane and restart testing. Production
    /// process/container executors will use the same task-registry and cancellation contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime would record receipts for a different logical worker.
    pub fn with_fake_executor(
        mut self,
        runtime: Arc<FakeExecutionRuntime>,
        executor: Arc<FakeExecutor>,
    ) -> Result<Self, WorkerError> {
        if self.execution.is_some() {
            return Err(WorkerError::Execution(
                "an execution runtime is already attached".into(),
            ));
        }
        if runtime.worker_id() != self.hello.worker_id {
            return Err(WorkerError::Execution(format!(
                "fake runtime identity {} does not match worker {}",
                runtime.worker_id(),
                self.hello.worker_id
            )));
        }
        self.execution = Some(Arc::new(ExecutionIntegration {
            attached: AttachedRuntime::Fake { runtime, executor },
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }));
        Ok(self)
    }

    /// Attaches the policy-bound CUDA runtime and restricts admission to its executor kind.
    ///
    /// # Errors
    ///
    /// Returns an error for a worker/runtime identity mismatch, non-CUDA capabilities, mismatched
    /// environment facts, or a state handle already shared before runtime configuration.
    pub fn with_cuda_executor(
        mut self,
        runtime: Arc<CudaExecutionRuntime>,
    ) -> Result<Self, WorkerError> {
        if self.execution.is_some() {
            return Err(WorkerError::Execution(
                "an execution runtime is already attached".into(),
            ));
        }
        if runtime.worker_id() != self.hello.worker_id {
            return Err(WorkerError::Execution(format!(
                "CUDA runtime identity {} does not match worker {}",
                runtime.worker_id(),
                self.hello.worker_id
            )));
        }
        let capabilities =
            self.hello.capabilities.as_ref().ok_or_else(|| {
                WorkerError::Execution("CUDA worker capabilities are missing".into())
            })?;
        if Backend::try_from(capabilities.backend).unwrap_or(Backend::Unspecified) != Backend::Cuda
        {
            return Err(WorkerError::Execution(
                "CUDA runtime requires CUDA worker capabilities".into(),
            ));
        }
        let environment = runtime.environment();
        if capabilities.architecture != environment.architecture
            || capabilities.driver_version != environment.driver_version
            || capabilities.toolkit_version != environment.toolkit_version
        {
            return Err(WorkerError::Execution(
                "CUDA runtime environment facts do not match worker capabilities".into(),
            ));
        }
        if !self
            .hello
            .features
            .iter()
            .any(|feature| feature == CUDA_FIXTURE_FEATURE)
        {
            self.hello.features.push(CUDA_FIXTURE_FEATURE.into());
        }
        let state = Arc::get_mut(&mut self.state).ok_or_else(|| {
            WorkerError::Execution(
                "CUDA runtime must be attached before sharing the worker state".into(),
            )
        })?;
        state.get_mut().policy = state.get_mut().policy.cuda_fixture_only();
        self.execution = Some(Arc::new(ExecutionIntegration {
            attached: AttachedRuntime::Cuda(runtime),
            active: Arc::new(Mutex::new(BTreeMap::new())),
        }));
        Ok(self)
    }

    /// Requires execution artifacts to be published before terminal journal state is committed.
    #[must_use]
    pub fn with_artifact_publisher(mut self, publisher: Arc<dyn ArtifactPublisher>) -> Self {
        self.artifact_publisher = Some(publisher);
        self
    }

    #[must_use]
    pub fn state(&self) -> Arc<Mutex<WorkerState>> {
        Arc::clone(&self.state)
    }

    /// Opens one gRPC session and processes messages until the stream closes.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] on transport, framing, validation or identity failures. A supervisor
    /// may reconnect this same value; its in-process attempt map is retained.
    pub async fn run_session(&self) -> Result<(), WorkerError> {
        self.publish_pending_terminal_artifacts().await?;
        self.retry_terminal_cuda_cleanup().await?;
        self.run_control_session().await
    }

    async fn retry_terminal_cuda_cleanup(&self) -> Result<(), WorkerError> {
        let Some(integration) = self.execution.as_ref() else {
            return Ok(());
        };
        let AttachedRuntime::Cuda(runtime) = &integration.attached else {
            return Ok(());
        };
        let state = self.state.lock().await.clone();
        let terminal_attempts = state
            .store
            .attempts()?
            .into_iter()
            .filter(|attempt| {
                attempt.phase == LocalAttemptPhase::Finished
                    && ExecutorKind::try_from(attempt.assignment.execution.executor_kind)
                        .unwrap_or(ExecutorKind::Unspecified)
                        == ExecutorKind::CudaFixture
            })
            .map(|attempt| attempt.assignment.attempt_id)
            .collect::<Vec<_>>();
        for attempt_id in terminal_attempts {
            // Cleanup is deliberately best effort here: a stale container cannot prevent durable
            // terminal outbox delivery. A later session retries the same idempotent removal.
            let _ = runtime
                .run(&state, &attempt_id, &CancellationToken::new())
                .await;
        }
        Ok(())
    }

    async fn run_control_session(&self) -> Result<(), WorkerError> {
        let channel = self.endpoint.clone().connect().await?;
        let mut client = WorkerControlClient::new(channel);
        let (outbound, receiver) = mpsc::channel(64);
        let mut execution_updates = self.execution_updates.subscribe();

        let mut hello = self.hello.clone();
        hello.active_attempts = self.state.lock().await.active_attempts()?;
        outbound
            .send(WorkerToServer {
                sequence: 1,
                acknowledges_server_through: 0,
                message_id: String::new(),
                message: Some(worker_to_server::Message::Hello(hello)),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)?;

        let response = client
            .open_control_stream(Request::new(ReceiverStream::new(receiver)))
            .await?;
        let mut inbound = response.into_inner();
        let welcome_frame = inbound.message().await?.ok_or(WorkerError::StreamClosed)?;
        Self::validate_server_frame(&welcome_frame, 0, 0, 1, false)?;
        let (connection_id, negotiated_protocol_minor) = self.welcome_identity(&welcome_frame)?;
        let mut heartbeat = tokio::time::interval(DEFAULT_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut next_worker_sequence = 2;
        let mut last_server_sequence = welcome_frame.sequence;
        let mut last_worker_sequence_acknowledged = welcome_frame.acknowledges_worker_through;
        let mut delivered_message_ids = BTreeSet::new();
        let require_message_ids = negotiated_protocol_minor >= 2;
        {
            let state = self.state.lock().await;
            state.acknowledge_outbox(&connection_id, last_worker_sequence_acknowledged)?;
            state.prune_old_deliveries()?;
        }
        self.send_pending_outbox(
            &connection_id,
            &outbound,
            &mut next_worker_sequence,
            last_server_sequence,
            &mut delivered_message_ids,
        )
        .await?;

        loop {
            tokio::select! {
                incoming = inbound.message() => {
                    let message = incoming?.ok_or(WorkerError::StreamClosed)?;
                    Self::validate_server_frame(
                        &message,
                        last_server_sequence,
                        last_worker_sequence_acknowledged,
                        next_worker_sequence - 1,
                        require_message_ids,
                    )?;
                    let server_sequence = message.sequence;
                    let acknowledges_worker_through = message.acknowledges_worker_through;
                    if self.handle_server_message(
                        message,
                        &connection_id,
                        &outbound,
                        &mut next_worker_sequence,
                        server_sequence,
                        &mut delivered_message_ids,
                    ).await? {
                        return Ok(());
                    }
                    self.state.lock().await.acknowledge_outbox(
                        &connection_id,
                        acknowledges_worker_through,
                    )?;
                    last_server_sequence = server_sequence;
                    last_worker_sequence_acknowledged = acknowledges_worker_through;
                }
                _ = heartbeat.tick() => {
                    let active_attempts = self.state.lock().await.active_attempts()?;
                    Self::send_ephemeral(
                        &outbound,
                        &mut next_worker_sequence,
                        last_server_sequence,
                        worker_to_server::Message::Heartbeat(Heartbeat {
                            active_attempts,
                            available_slots: self.available_slots().await?,
                            health: WorkerHealth::Ready.into(),
                        }),
                    ).await?;
                }
                update = execution_updates.recv(), if self.execution.is_some() => {
                    self.handle_execution_receive(
                        update,
                        &connection_id,
                        &outbound,
                        &mut next_worker_sequence,
                        last_server_sequence,
                        &mut delivered_message_ids,
                    ).await?;
                }
            }
        }
    }

    fn welcome_identity(&self, frame: &ServerToWorker) -> Result<(String, u32), WorkerError> {
        let Some(server_to_worker::Message::Welcome(welcome)) = frame.message.as_ref() else {
            return Err(WorkerError::Protocol(
                "first server frame must be welcome".to_owned(),
            ));
        };
        if welcome.protocol_major != self.hello.protocol_major {
            return Err(WorkerError::Protocol(format!(
                "server selected unsupported protocol major {}",
                welcome.protocol_major
            )));
        }
        Ok((welcome.connection_id.clone(), welcome.protocol_minor))
    }

    fn validate_server_frame(
        message: &ServerToWorker,
        last_server_sequence: u64,
        last_worker_sequence_acknowledged: u64,
        sent_worker_through: u64,
        require_message_ids: bool,
    ) -> Result<(), WorkerError> {
        if message.sequence != last_server_sequence + 1 {
            return Err(WorkerError::Protocol(format!(
                "server sequence gap: expected {}, got {}",
                last_server_sequence + 1,
                message.sequence
            )));
        }
        if message.acknowledges_worker_through < last_worker_sequence_acknowledged {
            return Err(WorkerError::Protocol(format!(
                "server acknowledgement regressed from {last_worker_sequence_acknowledged} to {}",
                message.acknowledges_worker_through
            )));
        }
        if message.acknowledges_worker_through > sent_worker_through {
            return Err(WorkerError::Protocol(format!(
                "server acknowledged worker sequence {} beyond sent sequence {sent_worker_through}",
                message.acknowledges_worker_through
            )));
        }
        if require_message_ids {
            let expected_message_id = expected_server_message_id(message.message.as_ref());
            if let Some(expected) = expected_message_id {
                if message.message_id != expected {
                    return Err(WorkerError::Protocol(format!(
                        "server message ID must be {expected}"
                    )));
                }
            } else if !message.message_id.is_empty() {
                return Err(WorkerError::Protocol(
                    "ephemeral server frame cannot carry a message ID".to_owned(),
                ));
            }
        }
        Ok(())
    }

    async fn handle_server_message(
        &self,
        frame: ServerToWorker,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<bool, WorkerError> {
        match frame.message {
            Some(server_to_worker::Message::Welcome(welcome)) => {
                if welcome.protocol_major != self.hello.protocol_major {
                    return Err(WorkerError::Protocol(format!(
                        "server selected unsupported protocol major {}",
                        welcome.protocol_major
                    )));
                }
                Ok(false)
            }
            Some(server_to_worker::Message::Assignment(assignment)) => {
                self.handle_assignment(
                    assignment,
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await?;
                Ok(false)
            }
            Some(server_to_worker::Message::Drain(_)) => Ok(true),
            Some(server_to_worker::Message::Cancel(cancel)) => {
                self.handle_cancel(
                    cancel,
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await?;
                Ok(false)
            }
            Some(server_to_worker::Message::Acknowledgement(_)) => Ok(false),
            None => Err(WorkerError::Protocol(
                "server message payload is missing".to_owned(),
            )),
        }
    }

    async fn handle_assignment(
        &self,
        assignment: Assignment,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let assignment_id = assignment.assignment_id.clone();
        let attempt_id = assignment.attempt_id.clone();
        let admitted = match self.state.lock().await.admit(&assignment) {
            Ok(_) => true,
            Err(WorkerError::InvalidAssignment(error)) => {
                self.state.lock().await.enqueue_lifecycle(
                    WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Invalid.into(),
                        detail: error.to_string(),
                    },
                )?;
                false
            }
            Err(WorkerError::ConflictingAttempt(_)) => {
                self.state.lock().await.enqueue_lifecycle(
                    WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Conflict.into(),
                        detail: "attempt ID conflicts with locally admitted content".to_owned(),
                    },
                )?;
                false
            }
            Err(WorkerError::PolicyViolation(detail)) => {
                self.state.lock().await.enqueue_lifecycle(
                    WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Policy.into(),
                        detail,
                    },
                )?;
                false
            }
            Err(error) => return Err(error),
        };
        if admitted {
            let state = self.state.lock().await;
            let attempt = state.attempt(&attempt_id)?.ok_or_else(|| {
                WorkerError::Protocol(format!("admitted attempt {attempt_id} is missing"))
            })?;
            match (attempt.phase, attempt.finished) {
                (LocalAttemptPhase::Running, _) => state.mark_running(&attempt_id)?,
                (LocalAttemptPhase::Finished, Some(finished)) => {
                    state.mark_finished(&attempt_id, &finished)?;
                }
                (LocalAttemptPhase::Finished, None) => {
                    return Err(WorkerError::Protocol(format!(
                        "finished attempt {attempt_id} lacks terminal journal data"
                    )));
                }
                (LocalAttemptPhase::Accepted, _) => {}
            }
        }
        self.send_pending_outbox(
            connection_id,
            outbound,
            next_worker_sequence,
            acknowledged,
            delivered_message_ids,
        )
        .await?;
        if admitted {
            self.ensure_execution(&attempt_id).await?;
        }
        Ok(())
    }

    async fn handle_cancel(
        &self,
        cancel: alloyport_proto::v1::CancelAttempt,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let already_terminal = {
            let state = self.state.lock().await;
            let attempt = state.attempt(&cancel.attempt_id)?.ok_or_else(|| {
                WorkerError::Protocol(format!(
                    "server cancelled unknown attempt {}",
                    cancel.attempt_id
                ))
            })?;
            let already_terminal = attempt.phase == LocalAttemptPhase::Finished;
            let assignment_id = attempt.assignment.assignment_id;
            state.enqueue_lifecycle(WorkerOutboxPayload::CancellationAcknowledged {
                assignment_id: assignment_id.clone(),
                attempt_id: cancel.attempt_id.clone(),
                already_terminal,
            })?;
            already_terminal
        };

        if already_terminal {
            self.send_pending_outbox(
                connection_id,
                outbound,
                next_worker_sequence,
                acknowledged,
                delivered_message_ids,
            )
            .await?;
            return Ok(());
        }

        if self.execution.is_none() {
            {
                let state = self.state.lock().await;
                state.mark_finished(
                    &cancel.attempt_id,
                    &StoredFinished {
                        outcome: AttemptOutcome::Cancelled.into(),
                        exit_code: None,
                        elapsed_ms: 0,
                        receipt: None,
                        stdout: None,
                        stderr: None,
                        detail: cancel.reason.clone(),
                    },
                )?;
                state
                    .attempt(&cancel.attempt_id)?
                    .and_then(|record| record.finished)
                    .ok_or_else(|| {
                        WorkerError::Protocol(
                            "cancelled attempt lacks terminal journal data".to_owned(),
                        )
                    })?;
            }
            return self
                .send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await;
        }

        let cancellation = self
            .ensure_execution(&cancel.attempt_id)
            .await?
            .ok_or_else(|| {
                WorkerError::Protocol(format!(
                    "non-terminal attempt {} did not start an executor",
                    cancel.attempt_id
                ))
            })?;

        // Put the durable acknowledgement on the wire before making cancellation visible to the
        // executor. Even an immediate fake completion therefore cannot overtake the ACK.
        self.send_pending_outbox(
            connection_id,
            outbound,
            next_worker_sequence,
            acknowledged,
            delivered_message_ids,
        )
        .await?;
        cancellation.cancel();

        Ok(())
    }

    async fn ensure_execution(
        &self,
        attempt_id: &str,
    ) -> Result<Option<CancellationToken>, WorkerError> {
        let Some(integration) = self.execution.as_ref() else {
            return Ok(None);
        };
        let attempt = self
            .state
            .lock()
            .await
            .attempt(attempt_id)?
            .ok_or_else(|| WorkerError::Protocol(format!("attempt {attempt_id} is missing")))?;
        if attempt.phase == LocalAttemptPhase::Finished {
            return Ok(None);
        }
        let executor = ExecutorKind::try_from(attempt.assignment.execution.executor_kind)
            .unwrap_or(ExecutorKind::Unspecified);
        if !integration.attached.supports(executor) {
            return Err(WorkerError::Execution(format!(
                "attached runtime does not support executor kind {}",
                executor.as_str_name()
            )));
        }

        let mut active = integration.active.lock().await;
        if let Some(cancellation) = active.get(attempt_id) {
            return Ok(Some(cancellation.clone()));
        }
        let cancellation = CancellationToken::new();
        active.insert(attempt_id.to_owned(), cancellation.clone());
        drop(active);

        let attempt_id = attempt_id.to_owned();
        let cancellation_for_task = cancellation.clone();
        let state = self.state.lock().await.clone();
        let integration = Arc::clone(integration);
        let artifact_publisher = self.artifact_publisher.clone();
        let updates = self.execution_updates.clone();
        tokio::spawn(async move {
            let result = run_registered_execution(
                &integration,
                &state,
                &attempt_id,
                &cancellation_for_task,
                artifact_publisher.as_deref(),
                &updates,
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string());
            integration.active.lock().await.remove(&attempt_id);
            let _ = updates.send(ExecutionUpdate::Completed { attempt_id, result });
        });
        Ok(Some(cancellation))
    }

    async fn handle_execution_update(
        &self,
        update: ExecutionUpdate,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        match update {
            ExecutionUpdate::Observation {
                attempt_id,
                observation: ExecutionObservation::Started,
            } => {
                let _ = attempt_id;
                self.send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
            ExecutionUpdate::Observation {
                attempt_id,
                observation: ExecutionObservation::Output(chunk),
            } => {
                Self::send_ephemeral(
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    worker_to_server::Message::OutputChunk(OutputChunk {
                        attempt_id,
                        stream: match chunk.stream {
                            ExecutionStream::Stdout => OutputStream::Stdout,
                            ExecutionStream::Stderr => OutputStream::Stderr,
                        }
                        .into(),
                        byte_offset: chunk.byte_offset,
                        display_sanitized: std::str::from_utf8(&chunk.bytes).is_err(),
                        payload: chunk.bytes,
                    }),
                )
                .await
            }
            ExecutionUpdate::Completed { attempt_id, result } => {
                result.map_err(|detail| {
                    WorkerError::Execution(format!("attempt {attempt_id}: {detail}"))
                })?;
                self.send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
        }
    }

    async fn handle_execution_receive(
        &self,
        update: Result<ExecutionUpdate, broadcast::error::RecvError>,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        match update {
            Ok(update) => {
                self.handle_execution_update(
                    update,
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // Output previews are explicitly best effort. Durable lifecycle rows are recovered
                // on the next observation, heartbeat, or reconnect.
                self.send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
            Err(broadcast::error::RecvError::Closed) => Err(WorkerError::Protocol(
                "execution update channel closed".to_owned(),
            )),
        }
    }

    async fn send_ephemeral(
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        message: worker_to_server::Message,
    ) -> Result<(), WorkerError> {
        let sequence = *next_worker_sequence;
        *next_worker_sequence += 1;
        outbound
            .send(WorkerToServer {
                sequence,
                acknowledges_server_through,
                message_id: String::new(),
                message: Some(message),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)
    }

    async fn publish_pending_terminal_artifacts(&self) -> Result<(), WorkerError> {
        let Some(publisher) = self.artifact_publisher.as_ref() else {
            return Ok(());
        };
        let pending = self.state.lock().await.pending_outbox()?;
        for entry in pending {
            let WorkerOutboxPayload::ExecutionFinished {
                attempt_id,
                finished,
                ..
            } = entry.payload
            else {
                continue;
            };
            publisher
                .publish(&terminal_reference_intents(&attempt_id, &finished))
                .await
                .map_err(WorkerError::Execution)?;
        }
        Ok(())
    }

    async fn send_pending_outbox(
        &self,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let pending = self.state.lock().await.pending_outbox()?;
        for entry in pending {
            if delivered_message_ids.contains(&entry.message_id) {
                continue;
            }
            let sequence = *next_worker_sequence;
            self.state
                .lock()
                .await
                .record_delivery(connection_id, sequence, &entry.message_id)?;
            *next_worker_sequence += 1;
            delivered_message_ids.insert(entry.message_id.clone());
            outbound
                .send(WorkerToServer {
                    sequence,
                    acknowledges_server_through,
                    message_id: entry.message_id,
                    message: Some(outbox_to_wire(entry.payload)),
                })
                .await
                .map_err(|_| WorkerError::StreamClosed)?;
        }
        Ok(())
    }

    async fn available_slots(&self) -> Result<u32, WorkerError> {
        let active = u32::try_from(self.state.lock().await.attempt_count()?).unwrap_or(u32::MAX);
        Ok(self.hello.capabilities.as_ref().map_or(0, |capabilities| {
            capabilities.max_concurrency.saturating_sub(active)
        }))
    }
}

async fn run_registered_execution(
    integration: &ExecutionIntegration,
    state: &WorkerState,
    attempt_id: &str,
    cancellation: &CancellationToken,
    publisher: Option<&dyn ArtifactPublisher>,
    updates: &broadcast::Sender<ExecutionUpdate>,
) -> Result<executor::ExecutionRun, executor::ExecutionRuntimeError> {
    let observed_attempt_id = attempt_id.to_owned();
    let observed_updates = updates.clone();
    let observer = move |observation| {
        let _ = observed_updates.send(ExecutionUpdate::Observation {
            attempt_id: observed_attempt_id.clone(),
            observation,
        });
    };
    match &integration.attached {
        AttachedRuntime::Fake { runtime, executor } => {
            if let Some(publisher) = publisher {
                runtime
                    .run_observed_and_publish(
                        state,
                        attempt_id,
                        executor,
                        cancellation,
                        publisher,
                        observer,
                    )
                    .await
            } else {
                runtime
                    .run_observed(state, attempt_id, executor, cancellation, observer)
                    .await
            }
        }
        AttachedRuntime::Cuda(runtime) => {
            if let Some(publisher) = publisher {
                runtime
                    .run_observed_and_publish(state, attempt_id, cancellation, publisher, observer)
                    .await
            } else {
                runtime
                    .run_observed(state, attempt_id, cancellation, observer)
                    .await
            }
        }
    }
}

fn assignment_to_stored(assignment: &Assignment) -> StoredAssignment {
    let execution = assignment
        .execution
        .as_ref()
        .expect("validated assignment contains execution");
    StoredAssignment {
        assignment_id: assignment.assignment_id.clone(),
        attempt_id: assignment.attempt_id.clone(),
        attempt_number: assignment.attempt_number,
        idempotency_key: assignment.idempotency_key.clone(),
        task_id: assignment.task_id.clone(),
        candidate_id: assignment.candidate_id.clone(),
        execution: StoredExecution {
            executor_kind: execution.executor_kind,
            argv: execution.argv.clone(),
            working_directory: execution.working_directory.clone(),
            environment: execution
                .environment
                .iter()
                .map(|entry| StoredEnvironment {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: execution.timeout_ms,
            bundle: artifact_to_stored(
                execution
                    .bundle
                    .as_ref()
                    .expect("validated assignment contains bundle"),
            ),
            image: artifact_to_stored(
                execution
                    .image
                    .as_ref()
                    .expect("validated assignment contains image"),
            ),
            limits: execution.limits.as_ref().map(|limits| StoredLimits {
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

fn artifact_to_stored(artifact: &ArtifactRef) -> StoredArtifact {
    StoredArtifact {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

fn stored_to_artifact(artifact: &StoredArtifact) -> ArtifactRef {
    ArtifactRef {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

fn stored_to_finished(
    assignment_id: &str,
    attempt_id: &str,
    finished: &StoredFinished,
) -> ExecutionFinished {
    ExecutionFinished {
        assignment_id: assignment_id.to_owned(),
        attempt_id: attempt_id.to_owned(),
        outcome: finished.outcome,
        exit_code: finished.exit_code,
        elapsed_ms: finished.elapsed_ms,
        receipt: finished.receipt.as_ref().map(stored_to_artifact),
        stdout: finished.stdout.as_ref().map(stored_to_artifact),
        stderr: finished.stderr.as_ref().map(stored_to_artifact),
        detail: finished.detail.clone(),
    }
}

fn lifecycle_identity(payload: &WorkerOutboxPayload) -> (String, String) {
    let (kind, attempt_id) = match payload {
        WorkerOutboxPayload::AssignmentAccepted { attempt_id, .. } => {
            ("assignment-accepted", attempt_id)
        }
        WorkerOutboxPayload::AssignmentRejected { attempt_id, .. } => {
            ("assignment-rejected", attempt_id)
        }
        WorkerOutboxPayload::ExecutionStarted { attempt_id, .. } => {
            ("execution-started", attempt_id)
        }
        WorkerOutboxPayload::ExecutionFinished { attempt_id, .. } => {
            ("execution-finished", attempt_id)
        }
        WorkerOutboxPayload::CancellationAcknowledged { attempt_id, .. } => {
            ("cancellation-acknowledged", attempt_id)
        }
    };
    (format!("{kind}:{attempt_id}"), attempt_id.clone())
}

fn expected_server_message_id(message: Option<&server_to_worker::Message>) -> Option<String> {
    match message? {
        server_to_worker::Message::Assignment(assignment) => {
            Some(format!("assignment:{}", assignment.attempt_id))
        }
        server_to_worker::Message::Cancel(cancel) => Some(format!("cancel:{}", cancel.attempt_id)),
        server_to_worker::Message::Welcome(_)
        | server_to_worker::Message::Drain(_)
        | server_to_worker::Message::Acknowledgement(_) => None,
    }
}

fn outbox_to_wire(payload: WorkerOutboxPayload) -> worker_to_server::Message {
    match payload {
        WorkerOutboxPayload::AssignmentAccepted {
            assignment_id,
            attempt_id,
            already_known,
        } => worker_to_server::Message::AssignmentAccepted(AssignmentAccepted {
            assignment_id,
            attempt_id,
            already_known,
        }),
        WorkerOutboxPayload::AssignmentRejected {
            assignment_id,
            attempt_id,
            reason,
            detail,
        } => worker_to_server::Message::AssignmentRejected(AssignmentRejected {
            assignment_id,
            attempt_id,
            reason,
            detail,
        }),
        WorkerOutboxPayload::ExecutionStarted {
            assignment_id,
            attempt_id,
        } => worker_to_server::Message::ExecutionStarted(alloyport_proto::v1::ExecutionStarted {
            assignment_id,
            attempt_id,
        }),
        WorkerOutboxPayload::ExecutionFinished {
            assignment_id,
            attempt_id,
            finished,
        } => worker_to_server::Message::ExecutionFinished(stored_to_finished(
            &assignment_id,
            &attempt_id,
            &finished,
        )),
        WorkerOutboxPayload::CancellationAcknowledged {
            assignment_id,
            attempt_id,
            already_terminal,
        } => worker_to_server::Message::CancellationAcknowledged(CancellationAcknowledged {
            assignment_id,
            attempt_id,
            already_terminal,
        }),
    }
}

fn now_unix_ms() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_proto::v1::{ArtifactRef, ExecutionSpec, ExecutorKind};

    fn artifact(byte: char) -> ArtifactRef {
        ArtifactRef {
            digest: format!("sha256:{}", byte.to_string().repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".to_owned(),
        }
    }

    fn assignment(argv: &str) -> Assignment {
        Assignment {
            assignment_id: "assignment-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            attempt_number: 1,
            idempotency_key: "task-1:build".to_owned(),
            task_id: "task-1".to_owned(),
            candidate_id: "candidate-1".to_owned(),
            execution: Some(ExecutionSpec {
                executor_kind: ExecutorKind::Container.into(),
                argv: vec![argv.to_owned()],
                working_directory: "source".to_owned(),
                environment: Vec::new(),
                timeout_ms: 30_000,
                bundle: Some(artifact('a')),
                image: Some(artifact('b')),
                limits: None,
            }),
            required_features: Vec::new(),
        }
    }

    #[test]
    fn replay_is_idempotent_but_conflicting_content_is_rejected() {
        let state = WorkerState::default();
        assert_eq!(
            state.admit(&assignment("true")).expect("first admission"),
            AdmissionOutcome::New
        );
        assert_eq!(
            state.admit(&assignment("true")).expect("same assignment"),
            AdmissionOutcome::Duplicate
        );
        assert!(matches!(
            state.admit(&assignment("false")),
            Err(WorkerError::ConflictingAttempt(attempt)) if attempt == "attempt-1"
        ));
    }

    #[test]
    fn shell_executor_requires_explicit_local_policy() {
        let mut shell = assignment("echo");
        shell
            .execution
            .as_mut()
            .expect("fixture has execution")
            .executor_kind = ExecutorKind::Shell.into();

        assert!(matches!(
            WorkerState::default().admit(&shell),
            Err(WorkerError::PolicyViolation(_))
        ));
        assert_eq!(
            WorkerState::with_policy(AdmissionPolicy::default().allowing_shell())
                .admit(&shell)
                .expect("explicit policy allows shell"),
            AdmissionOutcome::New
        );
    }

    #[test]
    fn cuda_fixture_executor_requires_explicit_local_policy() {
        let mut cuda = assignment("cuda-vectoradd-v1");
        cuda.execution
            .as_mut()
            .expect("fixture has execution")
            .executor_kind = ExecutorKind::CudaFixture.into();

        assert!(matches!(
            WorkerState::default().admit(&cuda),
            Err(WorkerError::PolicyViolation(_))
        ));
        assert_eq!(
            WorkerState::with_policy(AdmissionPolicy::default().allowing_cuda_fixture())
                .admit(&cuda)
                .expect("explicit policy allows the typed CUDA executor"),
            AdmissionOutcome::New
        );
    }

    #[test]
    fn sqlite_journal_restores_finished_attempt_and_rejects_conflict() -> Result<(), Box<dyn Error>>
    {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("worker.sqlite3");
        let finished = StoredFinished {
            outcome: alloyport_proto::v1::AttemptOutcome::Succeeded.into(),
            exit_code: Some(0),
            elapsed_ms: 25,
            receipt: Some(StoredArtifact {
                digest: format!("sha256:{}", "c".repeat(64)),
                size_bytes: 1,
                media_type: "application/vnd.alloyport.receipt+json".to_owned(),
            }),
            stdout: None,
            stderr: None,
            detail: "fixture complete".to_owned(),
        };
        {
            let state = WorkerState::open_sqlite(AdmissionPolicy::default(), &database)?;
            assert_eq!(state.admit(&assignment("true"))?, AdmissionOutcome::New);
            state.mark_running("attempt-1")?;
            state.mark_finished("attempt-1", &finished)?;
        }

        let restored = WorkerState::open_sqlite(AdmissionPolicy::default(), &database)?;
        let attempt = restored
            .attempt("attempt-1")?
            .expect("journal restores the attempt");
        assert_eq!(attempt.phase, LocalAttemptPhase::Finished);
        assert_eq!(attempt.finished, Some(finished));
        assert_eq!(
            restored.admit(&assignment("true"))?,
            AdmissionOutcome::Duplicate
        );
        assert!(matches!(
            restored.admit(&assignment("false")),
            Err(WorkerError::ConflictingAttempt(attempt)) if attempt == "attempt-1"
        ));
        Ok(())
    }

    #[test]
    fn server_acknowledgement_must_be_monotonic_and_not_future() {
        let valid = ServerToWorker {
            sequence: 2,
            acknowledges_worker_through: 3,
            message_id: String::new(),
            message: None,
        };
        assert!(OutboundWorker::validate_server_frame(&valid, 1, 2, 3, true).is_ok());

        let regressed = ServerToWorker {
            acknowledges_worker_through: 1,
            ..valid.clone()
        };
        assert!(matches!(
            OutboundWorker::validate_server_frame(&regressed, 1, 2, 3, true),
            Err(WorkerError::Protocol(detail)) if detail.contains("regressed")
        ));

        let future = ServerToWorker {
            acknowledges_worker_through: 4,
            ..valid
        };
        assert!(matches!(
            OutboundWorker::validate_server_frame(&future, 1, 2, 3, true),
            Err(WorkerError::Protocol(detail)) if detail.contains("beyond sent")
        ));
    }

    #[test]
    fn durable_journal_is_bound_to_one_logical_worker() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("worker.sqlite3");
        let endpoint = Endpoint::from_static("http://127.0.0.1:50051");
        let first = worker_hello("worker-1");
        OutboundWorker::open_sqlite(endpoint.clone(), first.clone(), &database)?;
        let mut changed = first;
        changed.worker_id = "worker-2".to_owned();
        assert!(matches!(
            OutboundWorker::open_sqlite(endpoint, changed, &database),
            Err(WorkerError::AttemptStore(
                AttemptStoreError::WorkerIdentityMismatch { .. }
            ))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn pending_legacy_terminal_is_published_before_control_replay()
    -> Result<(), Box<dyn Error>> {
        let state = WorkerState::default();
        state.admit(&assignment("true"))?;
        let artifact = StoredArtifact {
            digest: format!("sha256:{}", "c".repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".into(),
        };
        state.mark_finished(
            "attempt-1",
            &StoredFinished {
                outcome: AttemptOutcome::Succeeded.into(),
                exit_code: Some(0),
                elapsed_ms: 1,
                receipt: Some(artifact.clone()),
                stdout: Some(artifact.clone()),
                stderr: Some(artifact),
                detail: "legacy local terminal".into(),
            },
        )?;
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let worker = OutboundWorker::with_state(
            Endpoint::from_static("http://127.0.0.1:50051"),
            worker_hello("worker-1"),
            state,
        )?
        .with_artifact_publisher(Arc::new(RecordingTerminalPublisher(Arc::clone(&recorded))));

        worker.publish_pending_terminal_artifacts().await?;
        assert_eq!(
            *recorded.lock().expect("terminal publisher fixture lock"),
            vec![
                "output:attempt-1:stdout",
                "output:attempt-1:stderr",
                "receipt:attempt-1",
            ]
        );
        Ok(())
    }

    #[derive(Debug)]
    struct RecordingTerminalPublisher(Arc<std::sync::Mutex<Vec<String>>>);

    impl ArtifactPublisher for RecordingTerminalPublisher {
        fn publish<'a>(
            &'a self,
            references: &'a [executor::ArtifactReferenceIntent],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>
        {
            Box::pin(async move {
                self.0
                    .lock()
                    .map_err(|_| "terminal publisher fixture lock poisoned".to_owned())?
                    .extend(
                        references
                            .iter()
                            .map(|reference| reference.reference_key.clone()),
                    );
                Ok(())
            })
        }
    }

    fn worker_hello(worker_id: &str) -> WorkerHello {
        WorkerHello {
            protocol_major: alloyport_proto::PROTOCOL_MAJOR,
            protocol_minor: alloyport_proto::PROTOCOL_MINOR,
            worker_id: worker_id.to_owned(),
            instance_id: "instance-1".to_owned(),
            worker_version: "test".to_owned(),
            features: Vec::new(),
            capabilities: Some(alloyport_proto::v1::WorkerCapabilities {
                backend: alloyport_proto::v1::Backend::Cuda.into(),
                architecture: "test".to_owned(),
                device_count: 1,
                max_concurrency: 1,
                driver_version: "test".to_owned(),
                toolkit_version: "test".to_owned(),
                container_runtime: "test".to_owned(),
            }),
            active_attempts: Vec::new(),
        }
    }
}
