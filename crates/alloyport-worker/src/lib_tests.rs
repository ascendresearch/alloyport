//! Unit tests for worker admission, replay, and control semantics.

use super::*;
use crate::device::{DeviceSnapshot, DeviceSnapshotFuture, DeviceStatusProvider};
use crate::execution_backend::{
    BackendError, BackendExecutionFuture, BackendExecutionRequest, ExecutionBackend, ExecutionKind,
};
use crate::journal::{LocalAttemptPhase, StoredArtifact};
use alloyport_core::{
    AcceleratorDevice, AttemptId, AttemptOutcome, DeviceHealth, DeviceObservation,
};
use alloyport_proto::v1::{
    ArtifactRef, Assignment, ExecutionSpec, ExecutorKind as WireExecutorKind, ServerToWorker,
};

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
            executor_kind: WireExecutorKind::Container.into(),
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
fn worker_requires_an_attached_backend_unless_harness_mode_is_explicit() {
    let endpoint = Endpoint::from_static("http://127.0.0.1:50051");
    let worker = OutboundWorker::new(endpoint.clone(), worker_hello("worker-1"))
        .expect("worker fixture is valid");
    assert!(matches!(
        worker.validate_execution_support(&assignment("true")),
        Err(WorkerError::PolicyViolation(detail))
            if detail.contains("no execution backend is attached")
    ));

    let harness = OutboundWorker::new(endpoint, worker_hello("worker-1"))
        .expect("worker fixture is valid")
        .with_admission_only_mode();
    assert!(
        harness
            .validate_execution_support(&assignment("true"))
            .is_ok()
    );
}

#[test]
fn execution_backends_are_registered_without_control_state_machine_changes()
-> Result<(), Box<dyn Error>> {
    let endpoint = Endpoint::from_static("http://127.0.0.1:50051");
    let worker = OutboundWorker::new(endpoint, worker_hello("worker-1"))?
        .with_execution_backend(Arc::new(ProbeBackend(&[ExecutionKind::Container])))?
        .with_execution_backend(Arc::new(ProbeBackend(&[ExecutionKind::Process])))?;
    assert!(
        worker
            .validate_execution_support(&assignment("true"))
            .is_ok()
    );

    let error = worker
        .with_execution_backend(Arc::new(ProbeBackend(&[ExecutionKind::Container])))
        .expect_err("two backends cannot claim the same executor kind");
    assert!(matches!(
        error,
        WorkerError::Execution(detail) if detail.contains("CONTAINER")
    ));
    Ok(())
}

#[tokio::test]
async fn worker_concurrency_one_serializes_registered_backends() -> Result<(), Box<dyn Error>> {
    let integration = ExecutionIntegration::with_backend(
        Arc::new(ProbeBackend(&[ExecutionKind::AscendBuild])),
        1,
    )?;
    let first = Arc::clone(&integration.permits).acquire_owned().await?;
    let blocked = tokio::time::timeout(
        Duration::from_millis(10),
        Arc::clone(&integration.permits).acquire_owned(),
    )
    .await;
    assert!(blocked.is_err(), "a second backend entered the single slot");
    drop(first);
    let _second = tokio::time::timeout(
        Duration::from_millis(100),
        Arc::clone(&integration.permits).acquire_owned(),
    )
    .await??;
    Ok(())
}

#[derive(Debug)]
struct ProbeBackend(&'static [ExecutionKind]);

impl ExecutionBackend for ProbeBackend {
    fn executor_kinds(&self) -> &'static [ExecutionKind] {
        self.0
    }

    fn execute<'a>(&'a self, _request: BackendExecutionRequest<'a>) -> BackendExecutionFuture<'a> {
        Box::pin(async { Err(BackendError::terminal("probe backend must not execute")) })
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
        .executor_kind = WireExecutorKind::Shell.into();

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
        .executor_kind = WireExecutorKind::CudaFixture.into();

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
fn correctness_executors_require_role_specific_local_policy() {
    let mut cuda = assignment("reduction-correctness-v1");
    cuda.execution
        .as_mut()
        .expect("fixture has execution")
        .executor_kind = WireExecutorKind::CudaCorrectness.into();
    assert!(matches!(
        WorkerState::default().admit(&cuda),
        Err(WorkerError::PolicyViolation(_))
    ));
    assert_eq!(
        WorkerState::with_policy(AdmissionPolicy::default().cuda_correctness_only())
            .admit(&cuda)
            .expect("CUDA correctness policy"),
        AdmissionOutcome::New
    );

    let mut ascend = assignment("reduction-correctness-v1");
    ascend
        .execution
        .as_mut()
        .expect("fixture has execution")
        .executor_kind = WireExecutorKind::AscendCorrectness.into();
    assert!(matches!(
        WorkerState::default().admit(&ascend),
        Err(WorkerError::PolicyViolation(_))
    ));
    assert_eq!(
        WorkerState::with_policy(AdmissionPolicy::default().ascend_correctness_only())
            .admit(&ascend)
            .expect("Ascend correctness policy"),
        AdmissionOutcome::New
    );
}

#[test]
fn bound_device_is_registered_in_worker_capabilities() -> Result<(), Box<dyn Error>> {
    let worker = OutboundWorker::new(
        Endpoint::from_static("http://127.0.0.1:50051"),
        worker_hello("worker-1"),
    )?
    .with_bound_device(AcceleratorDevice {
        device_id: "3".into(),
        product_name: "accelerator".into(),
        serial_number: "serial-3".into(),
        firmware_version: "firmware".into(),
    })?;
    let devices = &worker
        .hello
        .capabilities
        .as_ref()
        .expect("capabilities")
        .devices;
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].device_id, "3");
    assert_eq!(devices[0].serial_number, "serial-3");
    Ok(())
}

#[tokio::test]
async fn heartbeat_reports_dynamic_health_occupancy_and_durable_device_lease()
-> Result<(), Box<dyn Error>> {
    let worker = OutboundWorker::new(
        Endpoint::from_static("http://127.0.0.1:50051"),
        worker_hello("worker-1"),
    )?
    .with_device_status_provider(Arc::new(FixedDeviceStatus));
    worker.state.admit(&assignment("true"))?;
    worker
        .state
        .acquire_device_lease(&AttemptId::try_from("attempt-1")?, "3")?;

    let heartbeat = worker.build_heartbeat().await?;
    alloyport_proto::validate_heartbeat(&heartbeat)?;
    assert_eq!(
        heartbeat.health,
        alloyport_proto::v1::WorkerHealth::Ready as i32
    );
    assert_eq!(heartbeat.available_slots, 0);
    assert_eq!(heartbeat.devices.len(), 1);
    assert_eq!(heartbeat.devices[0].process_count, 2);
    assert_eq!(heartbeat.device_leases.len(), 1);
    assert_eq!(heartbeat.device_leases[0].attempt_id, "attempt-1");
    assert_eq!(heartbeat.device_leases[0].device_id, "3");
    Ok(())
}

#[tokio::test]
/// A quarantined card reduces capacity, and only by one card.
///
/// The original form of this asserted the same zero by adding retained leases straight to the
/// occupied set, which charged the whole worker for one bad device: on a seven-NPU host a single
/// quarantine took the worker to zero slots while six cards were free. Capacity is now the lesser
/// of what may run at once and what is left to run it on, so this case still reaches zero — two
/// cards, one quarantined, one attempt already running — while a quarantine among many cards does
/// not.
async fn capacity_counts_distinct_running_attempts_and_terminal_quarantine_leases()
-> Result<(), Box<dyn Error>> {
    let mut hello = worker_hello("worker-1");
    let capabilities = hello.capabilities.as_mut().expect("fixture capabilities");
    capabilities.device_count = 2;
    capabilities.max_concurrency = 2;
    let worker = OutboundWorker::new(Endpoint::from_static("http://127.0.0.1:50051"), hello)?;

    let first = assignment("true");
    worker.state.admit(&first)?;
    let first_id = AttemptId::try_from("attempt-1")?;
    worker.state.acquire_device_lease(&first_id, "3")?;
    worker.state.mark_finished(
        first_id.as_str(),
        &StoredFinished {
            outcome: AttemptOutcome::InfraError,
            exit_code: None,
            elapsed_ms: 1,
            receipt: None,
            stdout: None,
            stderr: None,
            detail: "device remains quarantined".into(),
        },
    )?;

    let mut second = assignment("true");
    second.assignment_id = "assignment-2".into();
    second.attempt_id = "attempt-2".into();
    second.idempotency_key = "task-2:build".into();
    second.task_id = "task-2".into();
    second.candidate_id = "candidate-2".into();
    worker.state.admit(&second)?;

    // Two cards, one of them quarantined, leaves one usable.
    assert_eq!(worker.available_slots(1).await?, 0);

    // The same worker with six usable cards is not blocked by one quarantine.
    assert_eq!(worker.available_slots(6).await?, 1);
    Ok(())
}

#[derive(Debug)]
struct FixedDeviceStatus;

impl DeviceStatusProvider for FixedDeviceStatus {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async {
            Ok(DeviceSnapshot {
                devices: vec![DeviceObservation {
                    device_id: "3".to_owned(),
                    health: DeviceHealth::Ready,
                    process_count: 2,
                    utilization_percent: 0,
                    memory_used_bytes: 5_255,
                    memory_total_bytes: 131_072,
                    temperature_millicelsius: 56_000,
                    power_milliwatts: 191_300,
                    observed_at_ms: 1_000,
                    detail: "shared-host occupancy".to_owned(),
                }],
            })
        })
    }
}

#[test]
fn sqlite_journal_restores_finished_attempt_and_rejects_conflict() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("worker.sqlite3");
    let finished = StoredFinished {
        outcome: AttemptOutcome::Succeeded,
        exit_code: Some(0),
        elapsed_ms: 25,
        receipt: Some(StoredArtifact {
            digest: format!("sha256:{}", "c".repeat(64))
                .parse()
                .expect("valid fixture digest"),
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
async fn pending_legacy_terminal_is_published_before_control_replay() -> Result<(), Box<dyn Error>>
{
    let state = WorkerState::default();
    state.admit(&assignment("true"))?;
    let artifact = StoredArtifact {
        digest: format!("sha256:{}", "c".repeat(64))
            .parse()
            .expect("valid fixture digest"),
        size_bytes: 1,
        media_type: "application/octet-stream".into(),
    };
    state.mark_finished(
        "attempt-1",
        &StoredFinished {
            outcome: AttemptOutcome::Succeeded,
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
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), executor::ArtifactPublicationError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| {
                    executor::ArtifactPublicationError::Internal(
                        "terminal publisher fixture lock poisoned".to_owned(),
                    )
                })?
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
            devices: Vec::new(),
        }),
        active_attempts: Vec::new(),
    }
}
