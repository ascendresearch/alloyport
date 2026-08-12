use super::*;
use crate::ascend::{
    ASCEND_ADD_FIXTURE_ID, ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE, ASCEND_FIXTURE_FEATURE,
    AscendEnvironmentFacts, AscendResourceCeilings,
};
use crate::journal::{StoredArtifact, StoredExecution, StoredLimits};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, IngestRequest, Sha256Digest};
use alloyport_core::{
    AcceleratorDevice, AssignmentId, AttemptId, AttemptOutcome, CandidateId, ExecutionKind,
    NetworkPolicy, TaskId,
};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Mutex;

#[tokio::test]
async fn missing_container_is_created_and_exited_recovery_never_restarts()
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
    assert_eq!(engine.counts(), (1, 1, 0));
    Ok(())
}

#[tokio::test]
async fn identity_conflict_and_image_mismatch_fail_before_start()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    let conflict = FakeEngine::new(fixture.image_id.to_string());
    conflict.set_snapshot(ContainerSnapshot {
        identity: ContainerIdentity {
            name: "alloyport-attempt-ascend-1".into(),
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
        Err(AscendSupervisorError::IdentityConflict(_))
    ));
    assert_eq!(conflict.counts(), (0, 0, 0));

    let wrong = FakeEngine::new(Sha256Digest::digest_bytes(b"wrong").to_string());
    assert!(matches!(
        fixture
            .supervisor
            .run(&fixture.assignment, &wrong, &CancellationToken::new())
            .await,
        Err(AscendSupervisorError::ImageMismatch { .. })
    ));
    assert_eq!(wrong.counts(), (0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn cancellation_timeout_and_output_exhaustion_stop_the_same_container()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation_fixture = fixture()?;
    let cancelled_engine = FakeEngine::new(cancellation_fixture.image_id.to_string());
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = cancellation_fixture
        .supervisor
        .run(
            &cancellation_fixture.assignment,
            &cancelled_engine,
            &cancellation,
        )
        .await?;
    assert_eq!(cancelled.outcome, AttemptOutcome::Cancelled);
    assert_eq!(cancelled_engine.counts(), (1, 1, 1));

    let mut timeout_fixture = fixture()?;
    timeout_fixture.assignment.execution.timeout_ms = 1;
    let timeout_engine = FakeEngine::new(timeout_fixture.image_id.to_string());
    timeout_engine.set_snapshot(timeout_fixture.identity(ContainerPhase::Running));
    timeout_engine.block_first_wait();
    let timed_out = timeout_fixture
        .supervisor
        .run(
            &timeout_fixture.assignment,
            &timeout_engine,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(timed_out.outcome, AttemptOutcome::TimedOut);
    assert_eq!(timed_out.elapsed_ms, 1);
    assert_eq!(timeout_engine.counts(), (0, 0, 1));

    let exhausted_fixture = fixture()?;
    let exhausted_engine = FakeEngine::new(exhausted_fixture.image_id.to_string());
    exhausted_engine.block_first_wait();
    exhausted_engine.exceed_output_limit();
    let exhausted = exhausted_fixture
        .supervisor
        .run(
            &exhausted_fixture.assignment,
            &exhausted_engine,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(exhausted.outcome, AttemptOutcome::InfraError);
    assert_eq!(exhausted_engine.counts(), (1, 1, 1));
    Ok(())
}

#[test]
fn zero_exit_without_marker_and_combined_output_overflow_fail_closed() {
    let exit = Termination::Exited(ContainerExit {
        exit_code: 0,
        elapsed_ms: 9,
    });
    let missing_marker = classify(
        exit,
        ContainerLogs {
            stdout: b"not verified\n".to_vec(),
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
}

struct Fixture {
    _directory: tempfile::TempDir,
    supervisor: AscendContainerSupervisor,
    assignment: StoredAssignment,
    image_id: Sha256Digest,
}

impl Fixture {
    fn identity(&self, phase: ContainerPhase) -> ContainerSnapshot {
        ContainerSnapshot {
            identity: ContainerIdentity {
                name: format!("alloyport-{}", self.assignment.attempt_id),
                attempt_id: self.assignment.attempt_id.to_string(),
                bundle_digest: self.assignment.execution.bundle.digest.to_string(),
                image_manifest_digest: self.assignment.execution.image.digest.to_string(),
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
    let bundle_bytes = serde_json::to_vec(&crate::ascend::AscendFixtureBundle::add(
        "extern \"C\" __global__ __aicore__ void add_custom() {}\n",
    ))?;
    let bundle = artifacts
        .ingest(&mut Cursor::new(bundle_bytes), IngestRequest::unverified())?
        .artifact;
    let image_manifest = Sha256Digest::digest_bytes(b"manifest");
    let image_id = Sha256Digest::digest_bytes(b"image-id");
    let device = AcceleratorDevice {
        device_id: "3".into(),
        product_name: "Ascend950PR".into(),
        serial_number: "serial-3".into(),
        firmware_version: "9.0.0.105.229".into(),
    };
    let environment =
        AscendEnvironmentFacts::new("Ascend950PR", "9.1.0-beta.1", "25.7.rc1.6", "9.0.0.105.229")?;
    let nodes = (0..7)
        .map(|index| PathBuf::from(format!("/dev/davinci{index}")))
        .chain([
            PathBuf::from("/dev/davinci_manager"),
            PathBuf::from("/dev/hisi_hdc"),
        ])
        .collect();
    let policy = Arc::new(AscendFixturePolicy::new(
        ASCEND_ADD_FIXTURE_ID,
        bundle.digest,
        image_manifest,
        format!("example.invalid/ascend@{image_manifest}"),
        image_id,
        device,
        nodes,
        "/usr/local/Ascend/driver",
        directory.path().join("sandboxes"),
        AscendResourceCeilings {
            timeout_ms: 60_000,
            cpu_millis: 2_000,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            disk_bytes: 512 * 1024 * 1024,
            process_count: 64,
            output_bytes: 64 * 1024,
        },
        environment,
    )?);
    let assignment = StoredAssignment {
        assignment_id: AssignmentId::try_from("assignment-ascend-1")?,
        attempt_id: AttemptId::try_from("attempt-ascend-1")?,
        attempt_number: 1,
        idempotency_key: ASCEND_ADD_FIXTURE_ID.into(),
        task_id: TaskId::try_from("task-ascend-1")?,
        candidate_id: CandidateId::try_from("candidate-ascend-1")?,
        execution: StoredExecution {
            executor_kind: ExecutionKind::AscendFixture,
            argv: vec![ASCEND_ADD_FIXTURE_ID.into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: StoredArtifact {
                digest: bundle.digest,
                size_bytes: bundle.size_bytes,
                media_type: ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
            },
            image: StoredArtifact {
                digest: image_manifest,
                size_bytes: 0,
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
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
        required_features: vec![ASCEND_FIXTURE_FEATURE.into()],
    };
    Ok(Fixture {
        _directory: directory,
        supervisor: AscendContainerSupervisor::new(policy, artifacts),
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

impl AscendContainerEngine for FakeEngine {
    fn resolve_image_id<'a>(
        &'a self,
        _plan: &'a AscendDockerCreatePlan,
    ) -> EngineFuture<'a, String> {
        Box::pin(async { Ok(self.image_id.clone()) })
    }

    fn inspect<'a>(&'a self, _name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>> {
        Box::pin(async { Ok(self.state.lock().map_err(|_| "lock")?.snapshot.clone()) })
    }

    fn create<'a>(
        &'a self,
        _plan: &'a AscendDockerCreatePlan,
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
                stdout: b"PASS fixture=ascend-add-v1 elements=16384 checksum=fixture\n".to_vec(),
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
