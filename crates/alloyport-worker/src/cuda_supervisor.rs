//! Durable reconciliation state machine for policy-bound CUDA containers.

use crate::cuda::{CudaContractError, CudaFixturePolicy, DockerCreatePlan, VECTOR_ADD_FIXTURE_ID};
use crate::executor::{CancellationToken, ExecutorResult};
use crate::journal::StoredAssignment;
use alloyport_artifacts::FilesystemArtifactStore;
use alloyport_proto::v1::AttemptOutcome;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerIdentity {
    pub name: String,
    pub attempt_id: String,
    pub bundle_digest: String,
    pub image_manifest_digest: String,
    pub image_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainerPhase {
    Created,
    Running,
    Exited,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerSnapshot {
    pub identity: ContainerIdentity,
    pub phase: ContainerPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerExit {
    pub exit_code: i32,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerLogs {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub output_limit_exceeded: bool,
}

/// Local container operations. Implementations must use argv, never a shell string.
pub trait CudaContainerEngine: Debug + Send + Sync {
    fn resolve_image_id<'a>(&'a self, plan: &'a DockerCreatePlan) -> EngineFuture<'a, String>;
    fn inspect<'a>(&'a self, name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>>;
    fn create<'a>(
        &'a self,
        plan: &'a DockerCreatePlan,
        identity: &'a ContainerIdentity,
    ) -> EngineFuture<'a, ()>;
    fn start<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    fn wait<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ContainerExit>;
    fn stop<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    /// Returns at most `limit` combined stdout/stderr bytes and reports whether more existed.
    fn logs<'a>(&'a self, name: &'a str, limit: u64) -> EngineFuture<'a, ContainerLogs>;
    /// Removes a terminal container after publication and the terminal journal commit.
    fn remove<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
}

#[derive(Clone, Debug)]
pub struct CudaContainerSupervisor {
    policy: Arc<CudaFixturePolicy>,
    artifacts: Arc<FilesystemArtifactStore>,
}

impl CudaContainerSupervisor {
    #[must_use]
    pub const fn new(
        policy: Arc<CudaFixturePolicy>,
        artifacts: Arc<FilesystemArtifactStore>,
    ) -> Self {
        Self { policy, artifacts }
    }

    /// Reconciles one admitted attempt with its stable container and returns bounded terminal data.
    ///
    /// # Errors
    ///
    /// Returns an error for a policy/bundle failure, image mismatch, conflicting container identity,
    /// or container-engine failure. Candidate exit, cancellation, timeout, and output exhaustion are
    /// returned as typed terminal outcomes rather than supervisor errors.
    pub async fn run(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn CudaContainerEngine,
        cancellation: &CancellationToken,
    ) -> Result<ExecutorResult, CudaSupervisorError> {
        let sandbox = self
            .policy
            .materialize_bundle(assignment, self.artifacts.as_ref())?;
        let plan = self.policy.docker_create_plan(assignment, &sandbox)?;
        let identity = ContainerIdentity {
            name: plan.container_name.clone(),
            attempt_id: assignment.attempt_id.clone(),
            bundle_digest: assignment.execution.bundle.digest.clone(),
            image_manifest_digest: assignment.execution.image.digest.clone(),
            image_id: plan.expected_image_id.to_string(),
        };
        let resolved_image = engine
            .resolve_image_id(&plan)
            .await
            .map_err(CudaSupervisorError::Engine)?;
        if resolved_image != identity.image_id {
            return Err(CudaSupervisorError::ImageMismatch {
                expected: identity.image_id,
                actual: resolved_image,
            });
        }

        let phase = reconcile_container(engine, &plan, &identity).await?;
        if phase == ContainerPhase::Created {
            engine
                .start(&identity.name)
                .await
                .map_err(CudaSupervisorError::Engine)?;
        }

        let termination = if phase == ContainerPhase::Exited {
            Termination::Exited(
                engine
                    .wait(&identity.name)
                    .await
                    .map_err(CudaSupervisorError::Engine)?,
            )
        } else {
            let mut cancelled = cancellation.subscribe();
            tokio::select! {
                biased;
                () = wait_for_cancellation(&mut cancelled) => {
                    engine.stop(&identity.name).await.map_err(CudaSupervisorError::Engine)?;
                    Termination::Cancelled(engine.wait(&identity.name).await.map_err(CudaSupervisorError::Engine)?)
                }
                result = tokio::time::timeout(
                    Duration::from_millis(assignment.execution.timeout_ms),
                    engine.wait(&identity.name),
                ) => if let Ok(exit) = result {
                    Termination::Exited(exit.map_err(CudaSupervisorError::Engine)?)
                } else {
                    engine.stop(&identity.name).await.map_err(CudaSupervisorError::Engine)?;
                    Termination::TimedOut(engine.wait(&identity.name).await.map_err(CudaSupervisorError::Engine)?)
                }
            }
        };
        let output_limit = assignment
            .execution
            .limits
            .as_ref()
            .map_or(0, |limits| limits.output_bytes);
        let logs = engine
            .logs(&identity.name, output_limit)
            .await
            .map_err(CudaSupervisorError::Engine)?;
        Ok(classify(
            termination,
            enforce_output_limit(logs, output_limit),
            assignment.execution.timeout_ms,
        ))
    }
}

async fn reconcile_container(
    engine: &dyn CudaContainerEngine,
    plan: &DockerCreatePlan,
    identity: &ContainerIdentity,
) -> Result<ContainerPhase, CudaSupervisorError> {
    if let Some(snapshot) = engine
        .inspect(&identity.name)
        .await
        .map_err(CudaSupervisorError::Engine)?
    {
        if snapshot.identity != *identity {
            return Err(CudaSupervisorError::IdentityConflict(identity.name.clone()));
        }
        return Ok(snapshot.phase);
    }

    engine
        .create(plan, identity)
        .await
        .map_err(CudaSupervisorError::Engine)?;
    let created = engine
        .inspect(&identity.name)
        .await
        .map_err(CudaSupervisorError::Engine)?
        .ok_or_else(|| {
            CudaSupervisorError::Engine(format!(
                "container {} is missing immediately after create",
                identity.name
            ))
        })?;
    if created.identity != *identity {
        return Err(CudaSupervisorError::IdentityConflict(identity.name.clone()));
    }
    if created.phase != ContainerPhase::Created {
        return Err(CudaSupervisorError::Engine(format!(
            "new container {} has unexpected phase {:?}",
            identity.name, created.phase
        )));
    }
    Ok(created.phase)
}

#[derive(Clone, Copy)]
enum Termination {
    Exited(ContainerExit),
    Cancelled(ContainerExit),
    TimedOut(ContainerExit),
}

fn enforce_output_limit(mut logs: ContainerLogs, limit: u64) -> ContainerLogs {
    let stdout_len = u64::try_from(logs.stdout.len()).unwrap_or(u64::MAX);
    let stderr_len = u64::try_from(logs.stderr.len()).unwrap_or(u64::MAX);
    if stdout_len.saturating_add(stderr_len) <= limit {
        return logs;
    }
    logs.output_limit_exceeded = true;
    let kept_stdout = usize::try_from(limit.min(stdout_len)).unwrap_or(usize::MAX);
    logs.stdout.truncate(kept_stdout);
    let remaining = limit.saturating_sub(u64::try_from(logs.stdout.len()).unwrap_or(u64::MAX));
    logs.stderr
        .truncate(usize::try_from(remaining).unwrap_or(usize::MAX));
    logs
}

fn classify(termination: Termination, logs: ContainerLogs, timeout_ms: u64) -> ExecutorResult {
    let (exit, forced_outcome, detail) = match termination {
        Termination::Exited(exit) => (exit, None, "CUDA fixture exited"),
        Termination::Cancelled(exit) => {
            (exit, Some(AttemptOutcome::Cancelled), "execution cancelled")
        }
        Termination::TimedOut(exit) => {
            (exit, Some(AttemptOutcome::TimedOut), "execution timed out")
        }
    };
    let (outcome, exit_code, elapsed_ms, detail) = if logs.output_limit_exceeded {
        (
            AttemptOutcome::InfraError,
            None,
            exit.elapsed_ms,
            "execution output limit exceeded",
        )
    } else if let Some(outcome) = forced_outcome {
        (
            outcome,
            None,
            if outcome == AttemptOutcome::TimedOut {
                timeout_ms
            } else {
                exit.elapsed_ms
            },
            detail,
        )
    } else if exit.exit_code != 0 {
        (
            AttemptOutcome::CandidateFailed,
            Some(exit.exit_code),
            exit.elapsed_ms,
            "CUDA fixture returned a nonzero exit code",
        )
    } else if !String::from_utf8_lossy(&logs.stdout)
        .lines()
        .any(|line| line.starts_with(&format!("PASS fixture={VECTOR_ADD_FIXTURE_ID} ")))
    {
        (
            AttemptOutcome::IntegrityViolation,
            Some(0),
            exit.elapsed_ms,
            "CUDA fixture exited zero without its verification marker",
        )
    } else {
        (AttemptOutcome::Succeeded, Some(0), exit.elapsed_ms, detail)
    };
    ExecutorResult {
        outcome,
        exit_code,
        elapsed_ms,
        stdout: logs.stdout,
        stderr: logs.stderr,
        detail: detail.into(),
    }
}

async fn wait_for_cancellation(cancellation: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *cancellation.borrow_and_update() {
            return;
        }
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}

#[derive(Debug)]
pub enum CudaSupervisorError {
    Contract(CudaContractError),
    Engine(String),
    ImageMismatch { expected: String, actual: String },
    IdentityConflict(String),
}

impl From<CudaContractError> for CudaSupervisorError {
    fn from(error: CudaContractError) -> Self {
        Self::Contract(error)
    }
}

impl std::fmt::Display for CudaSupervisorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(error) => std::fmt::Display::fmt(error, formatter),
            Self::Engine(detail) => write!(formatter, "CUDA container engine error: {detail}"),
            Self::ImageMismatch { expected, actual } => {
                write!(
                    formatter,
                    "CUDA image ID mismatch: expected {expected}, got {actual}"
                )
            }
            Self::IdentityConflict(name) => {
                write!(
                    formatter,
                    "container {name} has conflicting durable identity"
                )
            }
        }
    }
}

impl std::error::Error for CudaSupervisorError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cuda::{
        CUDA_FIXTURE_BUNDLE_MEDIA_TYPE, CUDA_FIXTURE_FEATURE, CudaFixtureBundle,
        CudaResourceCeilings, OCI_IMAGE_MANIFEST_MEDIA_TYPE,
    };
    use crate::journal::{StoredArtifact, StoredExecution, StoredLimits};
    use alloyport_artifacts::{ArtifactStore, IngestRequest, Sha256Digest};
    use alloyport_proto::v1::{ExecutorKind, NetworkPolicy};
    use std::io::Cursor;
    use std::sync::Mutex;

    #[tokio::test]
    async fn missing_container_is_created_while_exited_container_is_replayed()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture()?;
        let engine = FakeEngine::new(fixture.image_id.to_string());
        let first = fixture
            .supervisor
            .run(&fixture.assignment, &engine, &CancellationToken::new())
            .await?;
        assert_eq!(first.outcome, AttemptOutcome::Succeeded);
        assert_eq!(engine.counts(), (1, 1, 0));

        let replay = fixture
            .supervisor
            .run(&fixture.assignment, &engine, &CancellationToken::new())
            .await?;
        assert_eq!(replay.outcome, AttemptOutcome::Succeeded);
        assert_eq!(
            engine.counts(),
            (1, 1, 0),
            "exited recovery cannot create or start again"
        );
        Ok(())
    }

    #[tokio::test]
    async fn conflict_is_fail_closed_and_cancellation_stops_then_collects_logs()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture()?;
        let conflict = FakeEngine::new(fixture.image_id.to_string());
        conflict.set_snapshot(ContainerSnapshot {
            identity: ContainerIdentity {
                name: "alloyport-attempt-1".into(),
                attempt_id: "other".into(),
                bundle_digest: "other".into(),
                image_manifest_digest: "other".into(),
                image_id: fixture.image_id.to_string(),
            },
            phase: ContainerPhase::Running,
        });
        assert!(matches!(
            fixture
                .supervisor
                .run(&fixture.assignment, &conflict, &CancellationToken::new())
                .await,
            Err(CudaSupervisorError::IdentityConflict(_))
        ));
        assert_eq!(conflict.counts(), (0, 0, 0));

        let engine = FakeEngine::new(fixture.image_id.to_string());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled = fixture
            .supervisor
            .run(&fixture.assignment, &engine, &cancellation)
            .await?;
        assert_eq!(cancelled.outcome, AttemptOutcome::Cancelled);
        assert_eq!(engine.counts(), (1, 1, 1));
        Ok(())
    }

    #[tokio::test]
    async fn running_container_is_reattached_and_timeout_stops_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture()?;
        fixture.assignment.execution.timeout_ms = 1;
        let engine = FakeEngine::new(fixture.image_id.to_string());
        engine.set_snapshot(fixture.identity(ContainerPhase::Running));
        engine.block_first_wait();

        let timed_out = fixture
            .supervisor
            .run(&fixture.assignment, &engine, &CancellationToken::new())
            .await?;

        assert_eq!(timed_out.outcome, AttemptOutcome::TimedOut);
        assert_eq!(timed_out.elapsed_ms, 1);
        assert_eq!(engine.counts(), (0, 0, 1));
        Ok(())
    }

    #[tokio::test]
    async fn image_mismatch_and_terminal_classification_are_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture()?;
        let wrong_image = FakeEngine::new(Sha256Digest::digest_bytes(b"wrong").to_string());
        assert!(matches!(
            fixture
                .supervisor
                .run(&fixture.assignment, &wrong_image, &CancellationToken::new())
                .await,
            Err(CudaSupervisorError::ImageMismatch { .. })
        ));
        assert_eq!(wrong_image.counts(), (0, 0, 0));

        let exit = Termination::Exited(ContainerExit {
            exit_code: 0,
            elapsed_ms: 9,
        });
        let missing_marker = classify(
            exit,
            ContainerLogs {
                stdout: b"not a fixture result\n".to_vec(),
                stderr: Vec::new(),
                output_limit_exceeded: false,
            },
            100,
        );
        assert_eq!(missing_marker.outcome, AttemptOutcome::IntegrityViolation);

        let exhausted = classify(
            exit,
            enforce_output_limit(
                ContainerLogs {
                    stdout: b"1234".to_vec(),
                    stderr: b"5678".to_vec(),
                    output_limit_exceeded: false,
                },
                5,
            ),
            100,
        );
        assert_eq!(exhausted.outcome, AttemptOutcome::InfraError);
        assert_eq!(exhausted.stdout, b"1234");
        assert_eq!(exhausted.stderr, b"5");

        let failed = classify(
            Termination::Exited(ContainerExit {
                exit_code: 17,
                elapsed_ms: 9,
            }),
            ContainerLogs {
                stdout: Vec::new(),
                stderr: b"compiler failed\n".to_vec(),
                output_limit_exceeded: false,
            },
            100,
        );
        assert_eq!(failed.outcome, AttemptOutcome::CandidateFailed);
        assert_eq!(failed.exit_code, Some(17));
        Ok(())
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        supervisor: CudaContainerSupervisor,
        assignment: StoredAssignment,
        image_id: Sha256Digest,
    }

    impl Fixture {
        fn identity(&self, phase: ContainerPhase) -> ContainerSnapshot {
            ContainerSnapshot {
                identity: ContainerIdentity {
                    name: format!("alloyport-{}", self.assignment.attempt_id),
                    attempt_id: self.assignment.attempt_id.clone(),
                    bundle_digest: self.assignment.execution.bundle.digest.clone(),
                    image_manifest_digest: self.assignment.execution.image.digest.clone(),
                    image_id: self.image_id.to_string(),
                },
                phase,
            }
        }
    }

    fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
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
        let assignment = StoredAssignment {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            attempt_number: 1,
            idempotency_key: VECTOR_ADD_FIXTURE_ID.into(),
            task_id: "task-1".into(),
            candidate_id: "candidate-1".into(),
            execution: StoredExecution {
                executor_kind: ExecutorKind::CudaFixture.into(),
                argv: vec![VECTOR_ADD_FIXTURE_ID.into()],
                working_directory: ".".into(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: StoredArtifact {
                    digest: stored.artifact.digest.to_string(),
                    size_bytes: stored.artifact.size_bytes,
                    media_type: CUDA_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
                },
                image: StoredArtifact {
                    digest: image_manifest.to_string(),
                    size_bytes: 0,
                    media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.into(),
                },
                limits: Some(StoredLimits {
                    cpu_millis: 1_000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 256 * 1024 * 1024,
                    process_count: 32,
                    output_bytes: 64 * 1024,
                    device_count: 1,
                    network: NetworkPolicy::Disabled.into(),
                }),
            },
            required_features: vec![CUDA_FIXTURE_FEATURE.into()],
        };
        Ok(Fixture {
            _directory: directory,
            supervisor: CudaContainerSupervisor::new(policy, artifacts),
            assignment,
            image_id,
        })
    }

    #[derive(Debug)]
    struct FakeEngine {
        image_id: String,
        state: Mutex<FakeState>,
    }

    #[derive(Debug)]
    struct FakeState {
        snapshot: Option<ContainerSnapshot>,
        exit: ContainerExit,
        creates: usize,
        starts: usize,
        stops: usize,
        wait_calls: usize,
        block_first_wait: bool,
    }

    impl FakeEngine {
        fn new(image_id: String) -> Self {
            Self {
                image_id,
                state: Mutex::new(FakeState {
                    snapshot: None,
                    exit: ContainerExit {
                        exit_code: 0,
                        elapsed_ms: 7,
                    },
                    creates: 0,
                    starts: 0,
                    stops: 0,
                    wait_calls: 0,
                    block_first_wait: false,
                }),
            }
        }

        fn set_snapshot(&self, snapshot: ContainerSnapshot) {
            self.state.lock().expect("fake engine lock").snapshot = Some(snapshot);
        }

        fn counts(&self) -> (usize, usize, usize) {
            let state = self.state.lock().expect("fake engine lock");
            (state.creates, state.starts, state.stops)
        }

        fn block_first_wait(&self) {
            self.state
                .lock()
                .expect("fake engine lock")
                .block_first_wait = true;
        }
    }

    impl CudaContainerEngine for FakeEngine {
        fn resolve_image_id<'a>(&'a self, _plan: &'a DockerCreatePlan) -> EngineFuture<'a, String> {
            Box::pin(async { Ok(self.image_id.clone()) })
        }

        fn inspect<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>> {
            Box::pin(async { Ok(self.state.lock().map_err(|_| "lock")?.snapshot.clone()) })
        }

        fn create<'a>(
            &'a self,
            _plan: &'a DockerCreatePlan,
            identity: &'a ContainerIdentity,
        ) -> EngineFuture<'a, ()> {
            Box::pin(async move {
                let mut state = self.state.lock().map_err(|_| "lock")?;
                state.creates += 1;
                state.snapshot = Some(ContainerSnapshot {
                    identity: identity.clone(),
                    phase: ContainerPhase::Created,
                });
                Ok(())
            })
        }

        fn start<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ()> {
            Box::pin(async {
                let mut state = self.state.lock().map_err(|_| "lock")?;
                state.starts += 1;
                state.snapshot.as_mut().ok_or("missing")?.phase = ContainerPhase::Running;
                Ok(())
            })
        }

        fn wait<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ContainerExit> {
            Box::pin(async {
                let block = {
                    let mut state = self.state.lock().map_err(|_| "lock")?;
                    state.wait_calls += 1;
                    state.block_first_wait && state.wait_calls == 1
                };
                if block {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else {
                    tokio::task::yield_now().await;
                }
                let mut state = self.state.lock().map_err(|_| "lock")?;
                state.snapshot.as_mut().ok_or("missing")?.phase = ContainerPhase::Exited;
                Ok(state.exit)
            })
        }

        fn stop<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, ()> {
            Box::pin(async {
                let mut state = self.state.lock().map_err(|_| "lock")?;
                state.stops += 1;
                state.snapshot.as_mut().ok_or("missing")?.phase = ContainerPhase::Exited;
                Ok(())
            })
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
                self.state.lock().map_err(|_| "lock")?.snapshot = None;
                Ok(())
            })
        }
    }
}
