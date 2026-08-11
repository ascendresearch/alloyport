//! Outbound worker client and local assignment admission state.

pub mod adapters;
pub mod artifact_download;
pub mod artifact_input;
pub mod artifact_upload;
mod attempt_coordinator;
mod backend_error;
mod control_session;
pub mod cuda;
pub mod cuda_docker;
pub mod cuda_runtime;
pub mod cuda_supervisor;
pub mod execution_backend;
mod execution_coordination;
pub mod executor;
pub mod fake_executor;
pub mod journal;
mod wire_mapping;
mod worker_delivery;
mod worker_state;
use worker_state::WorkerPersistence;

use alloyport_proto::v1::{Backend, WorkerHello};
use alloyport_proto::{ValidationError, validate_worker_hello};
use artifact_download::RemoteArtifactDownloader;
use artifact_input::ArtifactInputProvider;
use cuda::CUDA_FIXTURE_FEATURE;
use cuda_runtime::CudaExecutionRuntime;
use execution_backend::{
    BackendError, CudaExecutionBackend, ExecutionBackend, ExecutionBackendRegistry,
    FakeExecutionBackend,
};
use executor::{
    ArtifactPublicationError, ArtifactPublisher, CancellationToken, ExecutionObservation,
    FakeExecutionRuntime, FakeExecutor,
};
use journal::{AttemptStore, AttemptStoreError};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
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
    PersistenceTask(tokio::task::JoinError),
    Transport(tonic::transport::Error),
    Rpc(tonic::Status),
    ArtifactPublication(ArtifactPublicationError),
    Backend(BackendError),
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
            Self::PersistenceTask(error) => write!(formatter, "persistence task failed: {error}"),
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Rpc(error) => Display::fmt(error, formatter),
            Self::ArtifactPublication(error) => {
                write!(formatter, "worker Artifact publication failed: {error}")
            }
            Self::Backend(error) => Display::fmt(error, formatter),
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
            Self::PersistenceTask(error) => Some(error),
            Self::ArtifactPublication(error) => Some(error),
            Self::Backend(error) => Some(error),
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

impl From<ArtifactPublicationError> for WorkerError {
    fn from(error: ArtifactPublicationError) -> Self {
        Self::ArtifactPublication(error)
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
    persistence: WorkerPersistence,
}

impl Default for WorkerState {
    fn default() -> Self {
        Self::with_policy(AdmissionPolicy::default())
    }
}

/// One outbound worker identity with attempt state that survives stream reconnects in-process.
#[derive(Clone, Debug)]
pub struct OutboundWorker {
    endpoint: Endpoint,
    hello: WorkerHello,
    state: Arc<WorkerState>,
    execution: Option<Arc<ExecutionIntegration>>,
    admission_only: bool,
    artifact_input: Option<Arc<dyn ArtifactInputProvider>>,
    artifact_publisher: Option<Arc<dyn ArtifactPublisher>>,
    execution_updates: broadcast::Sender<ExecutionUpdate>,
}

#[derive(Debug)]
struct ExecutionIntegration {
    backends: ExecutionBackendRegistry,
    active: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
}

impl ExecutionIntegration {
    fn with_backend(backend: Arc<dyn ExecutionBackend>) -> Result<Self, WorkerError> {
        let mut backends = ExecutionBackendRegistry::default();
        backends
            .register(backend)
            .map_err(|error| WorkerError::Execution(error.to_string()))?;
        Ok(Self {
            backends,
            active: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn register(&mut self, backend: Arc<dyn ExecutionBackend>) -> Result<(), WorkerError> {
        self.backends
            .register(backend)
            .map_err(|error| WorkerError::Execution(error.to_string()))
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
        result: Result<(), BackendError>,
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
            state: Arc::new(state),
            execution: None,
            admission_only: false,
            artifact_input: None,
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
        self,
        runtime: Arc<FakeExecutionRuntime>,
        executor: Arc<FakeExecutor>,
    ) -> Result<Self, WorkerError> {
        if runtime.worker_id() != self.hello.worker_id {
            return Err(WorkerError::Execution(format!(
                "fake runtime identity {} does not match worker {}",
                runtime.worker_id(),
                self.hello.worker_id
            )));
        }
        self.with_execution_backend(Arc::new(FakeExecutionBackend::new(runtime, executor)))
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
        state.policy = state.policy.cuda_fixture_only();
        self.with_execution_backend(Arc::new(CudaExecutionBackend::new(runtime)))
    }

    /// Registers an execution backend without changing the control-session state machine.
    ///
    /// # Errors
    ///
    /// Returns an error if another backend already owns one of the declared executor kinds or the
    /// worker was cloned before composition completed.
    pub fn with_execution_backend(
        mut self,
        backend: Arc<dyn ExecutionBackend>,
    ) -> Result<Self, WorkerError> {
        if let Some(integration) = self.execution.as_mut() {
            Arc::get_mut(integration)
                .ok_or_else(|| {
                    WorkerError::Execution(
                        "execution backends must be registered before sharing the worker".into(),
                    )
                })?
                .register(backend)?;
        } else {
            self.execution = Some(Arc::new(ExecutionIntegration::with_backend(backend)?));
        }
        Ok(self)
    }

    /// Requires execution artifacts to be published before terminal journal state is committed.
    #[must_use]
    pub fn with_artifact_publisher(mut self, publisher: Arc<dyn ArtifactPublisher>) -> Self {
        self.artifact_publisher = Some(publisher);
        self
    }

    /// Downloads assignment inputs into the verified worker-local CAS before CUDA execution.
    #[must_use]
    pub fn with_artifact_downloader(mut self, downloader: Arc<RemoteArtifactDownloader>) -> Self {
        self.artifact_input = Some(downloader);
        self
    }

    /// Supplies an implementation-independent assignment-input materializer to execution backends.
    #[must_use]
    pub fn with_artifact_input_provider(
        mut self,
        provider: Arc<dyn ArtifactInputProvider>,
    ) -> Self {
        self.artifact_input = Some(provider);
        self
    }

    /// Enables an explicit control-plane harness mode that admits work without executing it.
    ///
    /// Production workers must attach a matching execution backend instead. This mode exists for
    /// protocol, lease, replay, and restart tests whose subject is deliberately not execution.
    #[must_use]
    pub fn with_admission_only_mode(mut self) -> Self {
        self.admission_only = true;
        self
    }

    #[must_use]
    pub fn state(&self) -> Arc<WorkerState> {
        Arc::clone(&self.state)
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
#[cfg(test)]
mod worker_state_tests;
