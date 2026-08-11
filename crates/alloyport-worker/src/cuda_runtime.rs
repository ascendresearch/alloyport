//! Durable Artifact and journal boundary around supervised CUDA execution.

use crate::WorkerState;
use crate::cuda_supervisor::{CudaContainerEngine, CudaContainerSupervisor, CudaExecutionFacts};
use crate::executor::{
    ArtifactPublisher, ExecutionChunk, ExecutionObservation, ExecutionRun, ExecutionRuntimeError,
    ExecutionStream, ExecutorInput, ExecutorResult, RECEIPT_MEDIA_TYPE, STDERR_MEDIA_TYPE,
    STDOUT_MEDIA_TYPE, event_artifact, output_event, producer_event, store_artifact,
    terminal_reference_intents,
};
use crate::journal::{LocalAttemptPhase, StoredArtifact, StoredFinished};
use alloyport_artifacts::FilesystemArtifactStore;
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
    artifacts: Arc<FilesystemArtifactStore>,
    supervisor: Arc<CudaContainerSupervisor>,
    engine: Arc<dyn CudaContainerEngine>,
    environment: CudaEnvironmentFacts,
    active_attempts: Arc<Mutex<BTreeSet<String>>>,
}

impl Debug for CudaExecutionRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CudaExecutionRuntime")
            .field("worker_id", &self.worker_id)
            .field("supervisor", &self.supervisor)
            .field("engine", &self.engine)
            .field("environment", &self.environment)
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
        artifacts: Arc<FilesystemArtifactStore>,
        supervisor: Arc<CudaContainerSupervisor>,
        engine: Arc<dyn CudaContainerEngine>,
        environment: CudaEnvironmentFacts,
    ) -> Result<Self, ExecutionRuntimeError> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            return Err(ExecutionRuntimeError::InvalidConfiguration(
                "worker identity is empty",
            ));
        }
        Ok(Self {
            worker_id,
            artifacts,
            supervisor,
            engine,
            environment,
            active_attempts: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
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
    /// Live Docker following is intentionally a later boundary; complete terminal bytes remain
    /// authoritative in the local CAS.
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
            .attempt(attempt_id)?
            .ok_or_else(|| ExecutionRuntimeError::MissingAttempt(attempt_id.into()))?;
        if attempt.phase == LocalAttemptPhase::Finished {
            let finished = attempt
                .finished
                .ok_or_else(|| ExecutionRuntimeError::MissingTerminalData(attempt_id.into()))?;
            self.remove_after_commit(attempt_id).await?;
            return Ok(ExecutionRun {
                reference_intents: terminal_reference_intents(attempt_id, &finished),
                finished,
                events: Vec::new(),
                replayed_terminal: true,
            });
        }

        state.mark_running(attempt_id)?;
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
            .run_with_facts(&attempt.assignment, self.engine.as_ref(), cancellation)
            .await
            .map_err(|error| ExecutionRuntimeError::Executor(error.to_string()))?;
        append_output(
            &self.worker_id,
            &input,
            &execution.result,
            &mut observer,
            &mut events,
        );
        let persisted = self
            .persist(&input, execution.result, &execution.facts)
            .await?;
        let references = terminal_reference_intents(attempt_id, &persisted.finished);
        if let Some(publisher) = publisher {
            publisher
                .publish(&references)
                .await
                .map_err(ExecutionRuntimeError::ArtifactPublication)?;
        }
        state.mark_finished(attempt_id, &persisted.finished)?;
        append_terminal_events(&self.worker_id, &input, &persisted, &mut events);
        self.remove_after_commit(attempt_id).await?;
        Ok(ExecutionRun {
            finished: persisted.finished,
            events,
            reference_intents: references,
            replayed_terminal: false,
        })
    }

    async fn persist(
        &self,
        input: &ExecutorInput,
        result: ExecutorResult,
        facts: &CudaExecutionFacts,
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
            schema_version: 1,
            worker_id: &self.worker_id,
            assignment_id: &input.assignment_id,
            attempt_id: &input.attempt_id,
            task_id: &input.task_id,
            candidate_id: &input.candidate_id,
            bundle_digest: &facts.bundle_digest,
            source_digest: &facts.source_digest,
            image_manifest_digest: &facts.image_manifest_digest,
            resolved_image_id: &facts.image_id,
            device_id: &facts.device_id,
            environment: &self.environment,
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
                outcome: outcome.into(),
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

    async fn remove_after_commit(&self, attempt_id: &str) -> Result<(), ExecutionRuntimeError> {
        self.engine
            .remove(&format!("alloyport-{attempt_id}"))
            .await
            .map_err(ExecutionRuntimeError::CleanupAfterCommit)
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
            timed_out: persisted.finished.outcome
                == i32::from(alloyport_proto::v1::AttemptOutcome::TimedOut),
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
    image_manifest_digest: &'a str,
    resolved_image_id: &'a str,
    device_id: &'a str,
    environment: &'a CudaEnvironmentFacts,
    outcome: &'a str,
    exit_code: Option<i32>,
    elapsed_ms: u64,
    stdout_digest: &'a str,
    stderr_digest: &'a str,
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
mod tests {
    use super::*;
    use crate::cuda::{
        CUDA_FIXTURE_BUNDLE_MEDIA_TYPE, CUDA_FIXTURE_FEATURE, CudaFixtureBundle, CudaFixturePolicy,
        CudaResourceCeilings, OCI_IMAGE_MANIFEST_MEDIA_TYPE, VECTOR_ADD_FIXTURE_ID,
    };
    use crate::cuda_supervisor::{
        ContainerExit, ContainerIdentity, ContainerLogs, ContainerPhase, ContainerSnapshot,
        EngineFuture,
    };
    use crate::executor::CancellationToken;
    use crate::{AdmissionOutcome, AdmissionPolicy};
    use alloyport_artifacts::{ArtifactStore, IngestRequest, Sha256Digest};
    use alloyport_proto::v1::{
        ArtifactRef, Assignment, ExecutionSpec, ExecutorKind, NetworkPolicy, ResourceLimits,
    };
    use std::io::{Cursor, Read};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn publication_and_terminal_commit_precede_retryable_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("cas"),
            64 * 1024,
        )?);
        let bundle = CudaFixtureBundle::vector_add(include_str!(
            "../../../fixtures/cuda-vectoradd-v1/vector_add.cu"
        ));
        let bytes = serde_json::to_vec(&bundle)?;
        let stored = artifacts.ingest(&mut Cursor::new(bytes), IngestRequest::unverified())?;
        let image_manifest = Sha256Digest::digest_bytes(b"manifest");
        let image_id = Sha256Digest::digest_bytes(b"image-id");
        let policy = Arc::new(CudaFixturePolicy::new(
            VECTOR_ADD_FIXTURE_ID,
            stored.artifact.digest,
            image_manifest,
            format!("example.invalid/cuda@{image_manifest}"),
            image_id,
            "0",
            directory.path().join("sandboxes"),
            CudaResourceCeilings {
                cpu_millis: 2_000,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: 512 * 1024 * 1024,
                process_count: 64,
                output_bytes: 64 * 1024,
            },
        )?);
        let state = WorkerState::with_policy(AdmissionPolicy::default().allowing_cuda_fixture());
        assert_eq!(
            state.admit(&assignment(
                stored.artifact.digest,
                stored.artifact.size_bytes,
                image_manifest
            ))?,
            AdmissionOutcome::New
        );
        let engine = Arc::new(RecordingEngine::new(state.clone(), image_id.to_string()));
        let engine_trait: Arc<dyn CudaContainerEngine> = engine.clone();
        let supervisor = Arc::new(CudaContainerSupervisor::new(policy, Arc::clone(&artifacts)));
        let runtime = CudaExecutionRuntime::new(
            "cuda-worker-1",
            Arc::clone(&artifacts),
            supervisor,
            engine_trait,
            CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
        )?;
        let publisher = OrderingPublisher::new(state.clone());

        assert!(matches!(
            runtime
                .run_observed_and_publish(
                    &state,
                    "attempt-1",
                    &CancellationToken::new(),
                    &publisher,
                    |_| {}
                )
                .await,
            Err(ExecutionRuntimeError::CleanupAfterCommit(_))
        ));
        assert!(publisher.called.load(Ordering::SeqCst));
        let terminal = state.attempt("attempt-1")?.expect("attempt exists");
        assert_eq!(terminal.phase, LocalAttemptPhase::Finished);
        assert!(engine.has_container());

        let replay = runtime
            .run(&state, "attempt-1", &CancellationToken::new())
            .await?;
        assert!(replay.replayed_terminal);
        assert!(!engine.has_container());
        assert_eq!(engine.remove_attempts(), 2);

        let receipt = replay.finished.receipt.expect("receipt is persisted");
        let digest = Sha256Digest::from_str(&receipt.digest)?;
        let mut reader = artifacts.open(digest)?;
        let mut receipt_bytes = Vec::new();
        reader.read_to_end(&mut receipt_bytes)?;
        let receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes)?;
        assert_eq!(receipt["source_digest"], bundle.source_sha256);
        assert_eq!(receipt["resolved_image_id"], image_id.to_string());
        assert_eq!(receipt["device_id"], "0");
        assert_eq!(receipt["environment"]["driver_version"], "580.159.03");
        Ok(())
    }

    fn assignment(bundle: Sha256Digest, bundle_size: u64, image: Sha256Digest) -> Assignment {
        Assignment {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            attempt_number: 1,
            idempotency_key: VECTOR_ADD_FIXTURE_ID.into(),
            task_id: "task-1".into(),
            candidate_id: "candidate-1".into(),
            execution: Some(ExecutionSpec {
                executor_kind: ExecutorKind::CudaFixture.into(),
                argv: vec![VECTOR_ADD_FIXTURE_ID.into()],
                working_directory: ".".into(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: Some(ArtifactRef {
                    digest: bundle.to_string(),
                    size_bytes: bundle_size,
                    media_type: CUDA_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
                }),
                image: Some(ArtifactRef {
                    digest: image.to_string(),
                    size_bytes: 0,
                    media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.into(),
                }),
                limits: Some(ResourceLimits {
                    cpu_millis: 1_000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 256 * 1024 * 1024,
                    process_count: 32,
                    output_bytes: 64 * 1024,
                    device_count: 1,
                    network: NetworkPolicy::Disabled.into(),
                }),
            }),
            required_features: vec![CUDA_FIXTURE_FEATURE.into()],
        }
    }

    #[derive(Debug)]
    struct OrderingPublisher {
        state: WorkerState,
        called: AtomicBool,
    }

    impl OrderingPublisher {
        fn new(state: WorkerState) -> Self {
            Self {
                state,
                called: AtomicBool::new(false),
            }
        }
    }

    impl ArtifactPublisher for OrderingPublisher {
        fn publish<'a>(
            &'a self,
            _references: &'a [crate::executor::ArtifactReferenceIntent],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>
        {
            Box::pin(async move {
                let attempt = self
                    .state
                    .attempt("attempt-1")
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "attempt missing".to_owned())?;
                if attempt.phase != LocalAttemptPhase::Running {
                    return Err("publisher observed terminal state too early".into());
                }
                self.called.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[derive(Debug)]
    struct RecordingEngine {
        worker_state: WorkerState,
        image_id: String,
        state: Mutex<RecordingEngineState>,
    }

    #[derive(Debug, Default)]
    struct RecordingEngineState {
        container: Option<ContainerSnapshot>,
        remove_attempts: usize,
    }

    impl RecordingEngine {
        fn new(worker_state: WorkerState, image_id: String) -> Self {
            Self {
                worker_state,
                image_id,
                state: Mutex::new(RecordingEngineState::default()),
            }
        }

        fn has_container(&self) -> bool {
            self.state.lock().expect("engine lock").container.is_some()
        }

        fn remove_attempts(&self) -> usize {
            self.state.lock().expect("engine lock").remove_attempts
        }
    }

    impl CudaContainerEngine for RecordingEngine {
        fn resolve_image_id<'a>(
            &'a self,
            _plan: &'a crate::cuda::DockerCreatePlan,
        ) -> EngineFuture<'a, String> {
            Box::pin(async { Ok(self.image_id.clone()) })
        }

        fn inspect<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>> {
            Box::pin(async {
                Ok(self
                    .state
                    .lock()
                    .map_err(|_| "engine lock")?
                    .container
                    .clone())
            })
        }

        fn create<'a>(
            &'a self,
            _plan: &'a crate::cuda::DockerCreatePlan,
            identity: &'a ContainerIdentity,
        ) -> EngineFuture<'a, ()> {
            Box::pin(async move {
                self.state.lock().map_err(|_| "engine lock")?.container = Some(ContainerSnapshot {
                    identity: identity.clone(),
                    phase: ContainerPhase::Created,
                });
                Ok(())
            })
        }

        fn start<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ()> {
            Box::pin(async {
                self.state
                    .lock()
                    .map_err(|_| "engine lock")?
                    .container
                    .as_mut()
                    .ok_or("container missing")?
                    .phase = ContainerPhase::Running;
                Ok(())
            })
        }

        fn wait<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ContainerExit> {
            Box::pin(async {
                self.state
                    .lock()
                    .map_err(|_| "engine lock")?
                    .container
                    .as_mut()
                    .ok_or("container missing")?
                    .phase = ContainerPhase::Exited;
                Ok(ContainerExit {
                    exit_code: 0,
                    elapsed_ms: 7,
                })
            })
        }

        fn stop<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ()> {
            Box::pin(async { Err("unexpected stop".into()) })
        }

        fn logs<'a>(&'a self, _name: &'a str, _limit: u64) -> EngineFuture<'a, ContainerLogs> {
            Box::pin(async {
                Ok(ContainerLogs {
                    stdout: b"PASS fixture=cuda-vectoradd-v1 elements=1048576 checksum=670562424\n"
                        .to_vec(),
                    stderr: Vec::new(),
                    output_limit_exceeded: false,
                })
            })
        }

        fn remove<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ()> {
            Box::pin(async {
                let attempt = self
                    .worker_state
                    .attempt("attempt-1")
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "attempt missing".to_owned())?;
                if attempt.phase != LocalAttemptPhase::Finished {
                    return Err("remove happened before terminal commit".into());
                }
                let mut state = self.state.lock().map_err(|_| "engine lock")?;
                state.remove_attempts += 1;
                if state.remove_attempts == 1 {
                    return Err("simulated cleanup outage".into());
                }
                state.container = None;
                Ok(())
            })
        }
    }
}
