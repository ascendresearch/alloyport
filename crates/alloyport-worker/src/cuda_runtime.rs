//! Durable Artifact and journal boundary around supervised CUDA execution.

use crate::WorkerState;
use crate::backend_error::BackendError;
use crate::cuda_supervisor::{
    ContainerLogChunk, ContainerLogStream, CudaContainerEngine, CudaContainerSupervisor,
    CudaExecutionFacts,
};
use crate::device::{DeviceLifecycleManager, DeviceStatusError};
use crate::device_guard::{CleanupIntent, DeviceGuard, DeviceGuardError};
use crate::executor::{
    ArtifactPublisher, ExecutionChunk, ExecutionObservation, ExecutionRun, ExecutionRuntimeError,
    ExecutionStream, ExecutorInput, ExecutorResult, RECEIPT_MEDIA_TYPE, STDERR_MEDIA_TYPE,
    STDOUT_MEDIA_TYPE, event_artifact, output_event, producer_event, store_artifact,
    terminal_reference_intents,
};
use crate::journal::{LocalAttemptPhase, StoredArtifact, StoredFinished};
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{AttemptId, DeviceLease, DeviceObservation, Sha256Digest};
use alloyport_events::{Event, ProducerEvent};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CudaEnvironmentFacts {
    pub architecture: String,
    pub driver_version: String,
    pub toolkit_version: String,
}

impl CudaEnvironmentFacts {
    /// Creates the immutable worker facts copied into each CUDA receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when any required environment identity is empty.
    pub fn new(
        architecture: impl Into<String>,
        driver_version: impl Into<String>,
        toolkit_version: impl Into<String>,
    ) -> Result<Self, ExecutionRuntimeError> {
        let facts = Self {
            architecture: architecture.into(),
            driver_version: driver_version.into(),
            toolkit_version: toolkit_version.into(),
        };
        if facts.architecture.trim().is_empty()
            || facts.driver_version.trim().is_empty()
            || facts.toolkit_version.trim().is_empty()
        {
            return Err(ExecutionRuntimeError::InvalidConfiguration(
                "CUDA architecture, driver, and toolkit facts must be nonempty",
            ));
        }
        Ok(facts)
    }
}

pub struct CudaExecutionRuntime {
    worker_id: String,
    device_id: String,
    artifacts: Arc<dyn ArtifactStore>,
    supervisor: Arc<CudaContainerSupervisor>,
    engine: Arc<dyn CudaContainerEngine>,
    environment: CudaEnvironmentFacts,
    device_manager: Arc<dyn DeviceLifecycleManager>,
    active_attempts: Arc<Mutex<BTreeSet<String>>>,
}

impl Debug for CudaExecutionRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CudaExecutionRuntime")
            .field("worker_id", &self.worker_id)
            .field("device_id", &self.device_id)
            .field("supervisor", &self.supervisor)
            .field("engine", &self.engine)
            .field("environment", &self.environment)
            .field("device_manager", &self.device_manager)
            .finish_non_exhaustive()
    }
}

impl CudaExecutionRuntime {
    /// Creates a runtime whose receipt identity and execution policy are worker-local.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker identity is empty.
    pub fn new(
        worker_id: impl Into<String>,
        artifacts: Arc<dyn ArtifactStore>,
        supervisor: Arc<CudaContainerSupervisor>,
        engine: Arc<dyn CudaContainerEngine>,
        environment: CudaEnvironmentFacts,
        device_manager: Arc<dyn DeviceLifecycleManager>,
    ) -> Result<Self, ExecutionRuntimeError> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            return Err(ExecutionRuntimeError::InvalidConfiguration(
                "worker identity is empty",
            ));
        }
        let device_id = supervisor.device_id().to_owned();
        if device_id.trim().is_empty() {
            return Err(ExecutionRuntimeError::InvalidConfiguration(
                "CUDA device identity is empty",
            ));
        }
        Ok(Self {
            worker_id,
            device_id,
            artifacts,
            supervisor,
            engine,
            environment,
            device_manager,
            active_attempts: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    #[must_use]
    pub const fn environment(&self) -> &CudaEnvironmentFacts {
        &self.environment
    }

    #[must_use]
    pub fn executor_kind(&self) -> alloyport_core::ExecutionKind {
        self.supervisor.executor_kind()
    }

    /// Runs one CUDA attempt without a remote Artifact publisher.
    ///
    /// # Errors
    ///
    /// Returns an error for journal, supervisor, Artifact, serialization, or cleanup failures.
    pub async fn run(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        cancellation: &crate::executor::CancellationToken,
    ) -> Result<ExecutionRun, ExecutionRuntimeError> {
        self.run_inner(state, attempt_id, cancellation, None, |_| {})
            .await
    }

    /// Runs one CUDA attempt and publishes all terminal Artifacts before the journal commit.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`], including publisher failures.
    pub async fn run_observed_and_publish<F>(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        cancellation: &crate::executor::CancellationToken,
        publisher: &dyn ArtifactPublisher,
        observer: F,
    ) -> Result<ExecutionRun, ExecutionRuntimeError>
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        self.run_inner(state, attempt_id, cancellation, Some(publisher), observer)
            .await
    }

    /// Runs one CUDA attempt while forwarding best-effort terminal observations.
    ///
    /// Docker logs are followed internally for early budget enforcement, but observations remain
    /// terminal-only; complete terminal bytes are authoritative in the local CAS.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`].
    pub async fn run_observed<F>(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        cancellation: &crate::executor::CancellationToken,
        observer: F,
    ) -> Result<ExecutionRun, ExecutionRuntimeError>
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        self.run_inner(state, attempt_id, cancellation, None, observer)
            .await
    }

    async fn run_inner<F>(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        cancellation: &crate::executor::CancellationToken,
        publisher: Option<&dyn ArtifactPublisher>,
        mut observer: F,
    ) -> Result<ExecutionRun, ExecutionRuntimeError>
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        let _claim = CudaAttemptClaim::acquire(Arc::clone(&self.active_attempts), attempt_id)?;
        let attempt = state
            .attempt_async(attempt_id.to_owned())
            .await?
            .ok_or_else(|| ExecutionRuntimeError::MissingAttempt(attempt_id.into()))?;
        let typed_attempt_id = AttemptId::try_from(attempt_id).map_err(|error| {
            ExecutionRuntimeError::Backend(BackendError::integrity(error.to_string()))
        })?;
        if attempt.phase == LocalAttemptPhase::Finished {
            let finished = attempt
                .finished
                .ok_or_else(|| ExecutionRuntimeError::MissingTerminalData(attempt_id.into()))?;
            self.cleanup_after_commit(state, typed_attempt_id).await?;
            return Ok(ExecutionRun {
                reference_intents: terminal_reference_intents(attempt_id, &finished),
                finished,
                events: Vec::new(),
                replayed_terminal: true,
            });
        }

        let (lease, pre_observation) = self
            .prepare_attempt(state, attempt_id, typed_attempt_id.clone(), attempt.phase)
            .await?;
        observer(ExecutionObservation::Started);
        let input = ExecutorInput::from(&attempt.assignment);
        let mut events = vec![producer_event(
            &self.worker_id,
            &input,
            Event::CommandStarted {
                command: input.argv.join(" "),
                cwd: Some(input.working_directory.clone()),
                execution_site: self.worker_id.clone(),
                description: Some("policy-bound CUDA fixture".into()),
            },
        )];
        let execution = self
            .supervisor
            .run_with_facts_observed(
                &attempt.assignment,
                self.engine.as_ref(),
                cancellation,
                |chunk| {
                    let chunk = execution_chunk(chunk);
                    observer(ExecutionObservation::Output(chunk.clone()));
                    events.push(output_event(&self.worker_id, &input, &chunk));
                },
            )
            .await
            .map_err(|error| ExecutionRuntimeError::Backend(error.into()))?;
        if !execution.live_output_streaming {
            append_output(
                &self.worker_id,
                &input,
                &execution.result,
                &mut observer,
                &mut events,
            );
        }
        let post_observation = observe_exact(self.device_manager.as_ref(), &self.device_id).await?;
        let cleanup_intent = CleanupIntent::from_observation(&post_observation);
        let persisted = self
            .persist(
                &input,
                execution.result,
                &execution.facts,
                &pre_observation,
                &post_observation,
                &lease,
                cleanup_intent,
            )
            .await?;
        let references = terminal_reference_intents(attempt_id, &persisted.finished);
        if let Some(publisher) = publisher {
            publisher.publish(&references).await?;
        }
        state
            .mark_finished_async(attempt_id.to_owned(), persisted.finished.clone())
            .await?;
        append_terminal_events(&self.worker_id, &input, &persisted, &mut events);
        self.cleanup_after_commit(state, typed_attempt_id).await?;
        Ok(ExecutionRun {
            finished: persisted.finished,
            events,
            reference_intents: references,
            replayed_terminal: false,
        })
    }

    async fn prepare_attempt(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        typed_attempt_id: AttemptId,
        phase: LocalAttemptPhase,
    ) -> Result<(DeviceLease, DeviceObservation), ExecutionRuntimeError> {
        if phase == LocalAttemptPhase::Accepted {
            let preflight = DeviceGuard::acquire_and_preflight(
                state,
                typed_attempt_id.clone(),
                &self.device_id,
                self.device_manager.as_ref(),
            )
            .await
            .map_err(runtime_guard_error)?;
            let lease = active_lease(state, &typed_attempt_id).await?;
            state.mark_running_async(attempt_id.to_owned()).await?;
            return Ok((lease, preflight.observation));
        }
        debug_assert_eq!(phase, LocalAttemptPhase::Running);
        let lease = active_lease(state, &typed_attempt_id).await?;
        let observation = state
            .device_preflight_async(typed_attempt_id)
            .await?
            .ok_or_else(|| {
                ExecutionRuntimeError::Backend(BackendError::integrity(format!(
                    "running attempt {attempt_id} lacks durable CUDA preflight evidence"
                )))
            })?;
        if observation.device_id != self.device_id || lease.device_id != self.device_id {
            return Err(ExecutionRuntimeError::Backend(BackendError::integrity(
                format!("running attempt {attempt_id} has CUDA evidence for a different device"),
            )));
        }
        Ok((lease, observation))
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist(
        &self,
        input: &ExecutorInput,
        result: ExecutorResult,
        facts: &CudaExecutionFacts,
        pre_observation: &DeviceObservation,
        post_observation: &DeviceObservation,
        lease: &DeviceLease,
        cleanup_intent: CleanupIntent,
    ) -> Result<CudaPersistedExecution, ExecutionRuntimeError> {
        let outcome = result.outcome;
        let exit_code = result.exit_code;
        let elapsed_ms = result.elapsed_ms;
        let detail = result.detail;
        let stdout = store_artifact(
            Arc::clone(&self.artifacts),
            result.stdout,
            STDOUT_MEDIA_TYPE,
        )
        .await?;
        let stderr = store_artifact(
            Arc::clone(&self.artifacts),
            result.stderr,
            STDERR_MEDIA_TYPE,
        )
        .await?;
        let receipt = serde_json::to_vec(&CudaRunReceipt {
            schema_version: 3,
            worker_id: &self.worker_id,
            assignment_id: &input.assignment_id,
            attempt_id: &input.attempt_id,
            task_id: &input.task_id,
            candidate_id: &input.candidate_id,
            bundle_digest: &facts.bundle_digest,
            source_digest: &facts.source_digest,
            image_digest: &facts.image_digest,
            image_media_type: &facts.image_media_type,
            resolved_image_id: &facts.image_id,
            device_id: &facts.device_id,
            environment: &self.environment,
            lease,
            pre_observation,
            post_observation,
            post_commit_cleanup: cleanup_intent,
            outcome: outcome.as_str_name(),
            exit_code,
            elapsed_ms,
            stdout_digest: &stdout.digest,
            stderr_digest: &stderr.digest,
            detail: &detail,
        })?;
        let receipt =
            store_artifact(Arc::clone(&self.artifacts), receipt, RECEIPT_MEDIA_TYPE).await?;
        Ok(CudaPersistedExecution {
            finished: StoredFinished {
                outcome,
                exit_code,
                elapsed_ms,
                receipt: Some(receipt.clone()),
                stdout: Some(stdout.clone()),
                stderr: Some(stderr.clone()),
                detail,
            },
            stdout,
            stderr,
            receipt,
        })
    }

    async fn cleanup_after_commit(
        &self,
        state: &WorkerState,
        attempt_id: AttemptId,
    ) -> Result<(), ExecutionRuntimeError> {
        self.engine
            .remove(&format!("alloyport-{attempt_id}"))
            .await
            .map_err(|error| ExecutionRuntimeError::CleanupAfterCommit(error.to_string()))?;
        if !has_active_lease(state, &attempt_id).await? {
            return Ok(());
        }
        DeviceGuard::cleanup_after_terminal(
            state,
            attempt_id,
            &self.device_id,
            self.device_manager.as_ref(),
        )
        .await
        .map(|_| ())
        .map_err(|error| ExecutionRuntimeError::CleanupAfterCommit(error.to_string()))
    }
}

async fn observe_exact(
    manager: &dyn DeviceLifecycleManager,
    device_id: &str,
) -> Result<DeviceObservation, ExecutionRuntimeError> {
    let observation = manager
        .observe_device(device_id)
        .await
        .map_err(|error| ExecutionRuntimeError::Backend(error.into()))?;
    if observation.device_id != device_id {
        return Err(ExecutionRuntimeError::Backend(BackendError::from(
            DeviceStatusError::InvalidResponse(format!(
                "device probe returned {} while observing {device_id}",
                observation.device_id
            )),
        )));
    }
    Ok(observation)
}

async fn active_lease(
    state: &WorkerState,
    attempt_id: &AttemptId,
) -> Result<DeviceLease, ExecutionRuntimeError> {
    state
        .active_device_leases_async()
        .await?
        .into_iter()
        .find(|lease| lease.attempt_id == *attempt_id)
        .ok_or_else(|| {
            ExecutionRuntimeError::Backend(BackendError::integrity(format!(
                "attempt {attempt_id} lost its durable CUDA device lease"
            )))
        })
}

async fn has_active_lease(
    state: &WorkerState,
    attempt_id: &AttemptId,
) -> Result<bool, ExecutionRuntimeError> {
    Ok(state
        .active_device_leases_async()
        .await?
        .iter()
        .any(|lease| lease.attempt_id == *attempt_id))
}

fn runtime_guard_error(error: DeviceGuardError) -> ExecutionRuntimeError {
    ExecutionRuntimeError::Backend(BackendError::from(error))
}

fn execution_chunk(chunk: ContainerLogChunk) -> ExecutionChunk {
    ExecutionChunk {
        stream: match chunk.stream {
            ContainerLogStream::Stdout => ExecutionStream::Stdout,
            ContainerLogStream::Stderr => ExecutionStream::Stderr,
        },
        byte_offset: chunk.byte_offset,
        bytes: chunk.bytes,
    }
}

fn append_output<F>(
    worker_id: &str,
    input: &ExecutorInput,
    result: &ExecutorResult,
    observer: &mut F,
    events: &mut Vec<ProducerEvent>,
) where
    F: FnMut(ExecutionObservation),
{
    for (stream, bytes) in [
        (ExecutionStream::Stdout, &result.stdout),
        (ExecutionStream::Stderr, &result.stderr),
    ] {
        if bytes.is_empty() {
            continue;
        }
        let chunk = ExecutionChunk {
            stream,
            byte_offset: 0,
            bytes: bytes.clone(),
        };
        observer(ExecutionObservation::Output(chunk.clone()));
        events.push(output_event(worker_id, input, &chunk));
    }
}

fn append_terminal_events(
    worker_id: &str,
    input: &ExecutorInput,
    persisted: &CudaPersistedExecution,
    events: &mut Vec<ProducerEvent>,
) {
    for (artifact, reference) in [
        (&persisted.stdout, "stdout"),
        (&persisted.stderr, "stderr"),
        (&persisted.receipt, "receipt"),
    ] {
        events.push(producer_event(
            worker_id,
            input,
            Event::ArtifactProduced {
                artifact: event_artifact(artifact, reference),
            },
        ));
    }
    events.push(producer_event(
        worker_id,
        input,
        Event::CommandCompleted {
            exit_code: persisted.finished.exit_code.unwrap_or(-1),
            elapsed_ms: persisted.finished.elapsed_ms,
            timed_out: persisted.finished.outcome == alloyport_core::AttemptOutcome::TimedOut,
            output_artifact: Some(event_artifact(&persisted.stdout, "stdout")),
        },
    ));
}

struct CudaPersistedExecution {
    finished: StoredFinished,
    stdout: StoredArtifact,
    stderr: StoredArtifact,
    receipt: StoredArtifact,
}

#[derive(Serialize)]
struct CudaRunReceipt<'a> {
    schema_version: u16,
    worker_id: &'a str,
    assignment_id: &'a str,
    attempt_id: &'a str,
    task_id: &'a str,
    candidate_id: &'a str,
    bundle_digest: &'a str,
    source_digest: &'a str,
    image_digest: &'a str,
    image_media_type: &'a str,
    resolved_image_id: &'a str,
    device_id: &'a str,
    environment: &'a CudaEnvironmentFacts,
    lease: &'a DeviceLease,
    pre_observation: &'a DeviceObservation,
    post_observation: &'a DeviceObservation,
    post_commit_cleanup: CleanupIntent,
    outcome: &'a str,
    exit_code: Option<i32>,
    elapsed_ms: u64,
    stdout_digest: &'a Sha256Digest,
    stderr_digest: &'a Sha256Digest,
    detail: &'a str,
}

struct CudaAttemptClaim {
    attempts: Arc<Mutex<BTreeSet<String>>>,
    attempt_id: String,
}

impl CudaAttemptClaim {
    fn acquire(
        attempts: Arc<Mutex<BTreeSet<String>>>,
        attempt_id: &str,
    ) -> Result<Self, ExecutionRuntimeError> {
        let mut active = attempts
            .lock()
            .map_err(|_| ExecutionRuntimeError::AttemptAlreadyRunning(attempt_id.into()))?;
        if !active.insert(attempt_id.into()) {
            return Err(ExecutionRuntimeError::AttemptAlreadyRunning(
                attempt_id.into(),
            ));
        }
        drop(active);
        Ok(Self {
            attempts,
            attempt_id: attempt_id.into(),
        })
    }
}

impl Drop for CudaAttemptClaim {
    fn drop(&mut self) {
        if let Ok(mut attempts) = self.attempts.lock() {
            attempts.remove(&self.attempt_id);
        }
    }
}

#[cfg(test)]
#[path = "cuda_runtime_tests.rs"]
mod tests;
