//! Behavioral tests for the CUDA supervisor module.

use super::*;
use crate::cuda::{
    CUDA_FIXTURE_BUNDLE_MEDIA_TYPE, CUDA_FIXTURE_FEATURE, CudaFixtureBundle, CudaResourceCeilings,
    OCI_IMAGE_MANIFEST_MEDIA_TYPE, VECTOR_ADD_FIXTURE_ID,
};
use crate::journal::{StoredArtifact, StoredExecution, StoredLimits};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, IngestRequest, Sha256Digest};
use alloyport_core::{AssignmentId, AttemptId, AttemptOutcome, ExecutionKind, NetworkPolicy};
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
async fn running_output_exhaustion_stops_the_container_before_it_exits()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    let engine = FakeEngine::new(fixture.image_id.to_string());
    engine.block_first_wait();
    engine.exceed_output_limit();

    let exhausted = fixture
        .supervisor
        .run(&fixture.assignment, &engine, &CancellationToken::new())
        .await?;

    assert_eq!(exhausted.outcome, AttemptOutcome::InfraError);
    assert!(exhausted.detail.contains("output limit exceeded"));
    assert_eq!(engine.counts(), (1, 1, 1));
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
                attempt_id: self.assignment.attempt_id.to_string(),
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
        assignment_id: AssignmentId::try_from("assignment-1").expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from("attempt-1")?,
        attempt_number: 1,
        idempotency_key: VECTOR_ADD_FIXTURE_ID.into(),
        task_id: "task-1".into(),
        candidate_id: "candidate-1".into(),
        execution: StoredExecution {
            executor_kind: ExecutionKind::CudaFixture,
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
                network: NetworkPolicy::Disabled,
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
    output_limit_exceeded: bool,
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
                output_limit_exceeded: false,
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

    fn exceed_output_limit(&self) {
        self.state
            .lock()
            .expect("fake engine lock")
            .output_limit_exceeded = true;
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
            let output_limit_exceeded =
                self.state.lock().map_err(|_| "lock")?.output_limit_exceeded;
            Ok(ContainerLogs {
                stdout: b"PASS fixture=cuda-vectoradd-v1 elements=1048576 checksum=670562424\n"
                    .to_vec(),
                stderr: Vec::new(),
                output_limit_exceeded,
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
