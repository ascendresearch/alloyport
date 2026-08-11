//! Pluggable execution-backend port and built-in runtime adapters.

use crate::WorkerState;
use crate::artifact_input::ArtifactInputProvider;
pub use crate::backend_error::{BackendError, BackendFailureClass};
use crate::cuda_runtime::CudaExecutionRuntime;
use crate::executor::{
    ArtifactPublisher, CancellationToken, ExecutionObservation, ExecutionRun,
    ExecutionRuntimeError, FakeExecutionRuntime, FakeExecutor,
};
pub use alloyport_core::ExecutionKind;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Best-effort observation callback supplied by the control-session coordinator.
pub type ExecutionObserver = Arc<dyn Fn(ExecutionObservation) + Send + Sync>;

/// Future returned by an execution backend.
pub type BackendExecutionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ExecutionRun, BackendError>> + Send + 'a>>;

/// Future returned by a backend's restart cleanup hook.
pub type BackendCleanupFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>>;

/// Services and durable state supplied to one backend execution.
pub struct BackendExecutionRequest<'a> {
    pub state: &'a WorkerState,
    pub attempt_id: &'a str,
    pub cancellation: &'a CancellationToken,
    pub input_provider: Option<&'a dyn ArtifactInputProvider>,
    pub publisher: Option<&'a dyn ArtifactPublisher>,
    pub observer: ExecutionObserver,
}

/// Executor implementation selected through composition rather than the control state machine.
pub trait ExecutionBackend: Debug + Send + Sync {
    /// Executor kinds owned by this backend.
    fn executor_kinds(&self) -> &'static [ExecutionKind];

    /// Executes or recovers one durable attempt.
    fn execute<'a>(&'a self, request: BackendExecutionRequest<'a>) -> BackendExecutionFuture<'a>;

    /// Retries backend-owned cleanup for an already-terminal attempt after restart.
    fn retry_terminal_cleanup<'a>(
        &'a self,
        _state: &'a WorkerState,
        _attempt_id: &'a str,
    ) -> BackendCleanupFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

/// Immutable executor-kind lookup built during worker composition.
#[derive(Debug, Default)]
pub(crate) struct ExecutionBackendRegistry {
    backends: BTreeMap<ExecutionKind, Arc<dyn ExecutionBackend>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionBackendRegistrationError {
    EmptyCapabilitySet,
    Duplicate(ExecutionKind),
}

impl Display for ExecutionBackendRegistrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCapabilitySet => {
                write!(formatter, "execution backend declares no executor kinds")
            }
            Self::Duplicate(kind) => write!(
                formatter,
                "execution backend already registered for {}",
                kind.as_str_name()
            ),
        }
    }
}

impl ExecutionBackendRegistry {
    pub(crate) fn register(
        &mut self,
        backend: Arc<dyn ExecutionBackend>,
    ) -> Result<(), ExecutionBackendRegistrationError> {
        let executors = backend.executor_kinds();
        for executor in executors {
            if self.backends.contains_key(executor) {
                return Err(ExecutionBackendRegistrationError::Duplicate(*executor));
            }
        }
        let Some((last, preceding)) = executors.split_last() else {
            return Err(ExecutionBackendRegistrationError::EmptyCapabilitySet);
        };
        for executor in preceding {
            self.backends.insert(*executor, Arc::clone(&backend));
        }
        self.backends.insert(*last, backend);
        Ok(())
    }

    pub(crate) fn backend(&self, executor: ExecutionKind) -> Option<Arc<dyn ExecutionBackend>> {
        self.backends.get(&executor).cloned()
    }
}

#[derive(Debug)]
pub(crate) struct FakeExecutionBackend {
    runtime: Arc<FakeExecutionRuntime>,
    executor: Arc<FakeExecutor>,
}

impl FakeExecutionBackend {
    pub(crate) const fn new(
        runtime: Arc<FakeExecutionRuntime>,
        executor: Arc<FakeExecutor>,
    ) -> Self {
        Self { runtime, executor }
    }
}

impl ExecutionBackend for FakeExecutionBackend {
    fn executor_kinds(&self) -> &'static [ExecutionKind] {
        &[
            ExecutionKind::Process,
            ExecutionKind::Container,
            ExecutionKind::Shell,
        ]
    }

    fn execute<'a>(&'a self, request: BackendExecutionRequest<'a>) -> BackendExecutionFuture<'a> {
        Box::pin(async move {
            let observer = request.observer;
            let result = if let Some(publisher) = request.publisher {
                self.runtime
                    .run_observed_and_publish(
                        request.state,
                        request.attempt_id,
                        &self.executor,
                        request.cancellation,
                        publisher,
                        move |observation| observer(observation),
                    )
                    .await
            } else {
                self.runtime
                    .run_observed(
                        request.state,
                        request.attempt_id,
                        &self.executor,
                        request.cancellation,
                        move |observation| observer(observation),
                    )
                    .await
            };
            result.map_err(BackendError::from)
        })
    }
}

#[derive(Debug)]
pub(crate) struct CudaExecutionBackend {
    runtime: Arc<CudaExecutionRuntime>,
}

impl CudaExecutionBackend {
    pub(crate) const fn new(runtime: Arc<CudaExecutionRuntime>) -> Self {
        Self { runtime }
    }
}

impl ExecutionBackend for CudaExecutionBackend {
    fn executor_kinds(&self) -> &'static [ExecutionKind] {
        &[ExecutionKind::CudaFixture]
    }

    fn execute<'a>(&'a self, request: BackendExecutionRequest<'a>) -> BackendExecutionFuture<'a> {
        Box::pin(async move {
            if let Some(input_provider) = request.input_provider {
                let attempt = request
                    .state
                    .attempt_async(request.attempt_id.to_owned())
                    .await?
                    .ok_or_else(|| {
                        ExecutionRuntimeError::MissingAttempt(request.attempt_id.into())
                    })?;
                input_provider
                    .materialize(&attempt.assignment.execution.bundle)
                    .await?;
            }
            let observer = request.observer;
            let result = if let Some(publisher) = request.publisher {
                self.runtime
                    .run_observed_and_publish(
                        request.state,
                        request.attempt_id,
                        request.cancellation,
                        publisher,
                        move |observation| observer(observation),
                    )
                    .await
            } else {
                self.runtime
                    .run_observed(
                        request.state,
                        request.attempt_id,
                        request.cancellation,
                        move |observation| observer(observation),
                    )
                    .await
            };
            result.map_err(BackendError::from)
        })
    }

    fn retry_terminal_cleanup<'a>(
        &'a self,
        state: &'a WorkerState,
        attempt_id: &'a str,
    ) -> BackendCleanupFuture<'a> {
        Box::pin(async move {
            self.runtime
                .run(state, attempt_id, &CancellationToken::new())
                .await
                .map(|_| ())
                .map_err(BackendError::from)
        })
    }
}
