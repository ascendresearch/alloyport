use super::*;
use crate::AdmissionPolicy;
use crate::ascend::{
    ASCEND_ADD_FIXTURE_ID, ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE, ASCEND_FIXTURE_FEATURE,
    AscendDockerCreatePlan, AscendEnvironmentFacts, AscendFixtureBundle, AscendFixturePolicy,
    AscendResourceCeilings,
};
use crate::ascend_smi::AscendDeviceFuture;
use crate::ascend_supervisor::{
    ContainerExit, ContainerIdentity, ContainerLogs, ContainerPhase, ContainerSnapshot,
    EngineFuture,
};
use crate::device::{DeviceSnapshot, DeviceSnapshotFuture, DeviceStatusProvider};
use crate::executor::{ArtifactPublicationError, CancellationToken};
use crate::{AdmissionOutcome, OutboundWorker, WorkerError};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, IngestRequest};
use alloyport_core::{AcceleratorDevice, DeviceHealth, DeviceObservation};
use alloyport_proto::v1::{
    AcceleratorDevice as WireDevice, ArtifactRef, Assignment, Backend, ExecutionSpec, ExecutorKind,
    NetworkPolicy, ResourceLimits, WorkerCapabilities, WorkerHello,
};
use std::collections::VecDeque;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

const ASCEND_STDOUT: &[u8] = b"PASS fixture=ascend-add-v1 elements=16384 checksum=fixture\n";

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn durable_preflight_survives_preterminal_recovery_and_terminal_cleanup_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("cas"),
        64 * 1024,
    )?);
    let source = "extern \"C\" __global__ __aicore__ void add_custom() {}\n";
    let bundle = AscendFixtureBundle::add(source);
    let stored = artifacts.ingest(
        &mut Cursor::new(serde_json::to_vec(&bundle)?),
        IngestRequest::unverified(),
    )?;
    let image_manifest = Sha256Digest::digest_bytes(b"manifest");
    let image_id = Sha256Digest::digest_bytes(b"image-id");
    let device = device();
    let policy = Arc::new(AscendFixturePolicy::new(
        ASCEND_ADD_FIXTURE_ID,
        stored.artifact.digest,
        image_manifest,
        format!("example.invalid/ascend@{image_manifest}"),
        image_id,
        device.clone(),
        device_nodes(),
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
        environment()?,
    )?);
    let state = WorkerState::with_policy(AdmissionPolicy::default().allowing_ascend_fixture());
    assert_eq!(
        state.admit(&assignment(
            stored.artifact.digest,
            stored.artifact.size_bytes,
            image_manifest,
        ))?,
        AdmissionOutcome::New
    );
    let engine = Arc::new(RecordingEngine::new(state.clone(), image_id.to_string()));
    let engine_trait: Arc<dyn AscendContainerEngine> = engine.clone();
    let manager = Arc::new(RecordingDeviceManager::new(vec![
        Ok(observation(DeviceHealth::Ready, 0, 1)),
        Ok(observation(DeviceHealth::Ready, 0, 2)),
        Ok(observation(DeviceHealth::Ready, 0, 3)),
        Ok(observation(DeviceHealth::Ready, 0, 4)),
    ]));
    let manager_trait: Arc<dyn AscendDeviceManager> = manager.clone();
    let supervisor = Arc::new(AscendContainerSupervisor::new(policy, artifacts.clone()));
    let runtime = Arc::new(AscendExecutionRuntime::new(
        "ascend-worker-1",
        artifacts.clone(),
        supervisor,
        engine_trait,
        manager_trait,
    )?);
    let publisher = FailOnceOrderingPublisher::new(state.clone());

    assert!(matches!(
        runtime
            .run_observed_and_publish(
                &state,
                "attempt-1",
                &CancellationToken::new(),
                &publisher,
                |_| {},
            )
            .await,
        Err(ExecutionRuntimeError::ArtifactPublication(_))
    ));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.attempt("attempt-1")?.expect("attempt").phase,
        LocalAttemptPhase::Running
    );
    assert_eq!(state.active_device_leases()?.len(), 1);
    assert!(engine.has_container());
    assert_eq!(manager.observe_calls(), 2);
    assert_eq!(
        state
            .device_preflight(&AttemptId::try_from("attempt-1")?)?
            .expect("durable preflight")
            .observed_at_ms,
        1
    );

    assert!(matches!(
        runtime
            .run_observed_and_publish(
                &state,
                "attempt-1",
                &CancellationToken::new(),
                &publisher,
                |_| {},
            )
            .await,
        Err(ExecutionRuntimeError::CleanupAfterCommit(_))
    ));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        state.attempt("attempt-1")?.expect("attempt").phase,
        LocalAttemptPhase::Finished
    );
    assert_eq!(manager.observe_calls(), 3);
    assert_eq!(engine.create_count(), 1);

    let replay = runtime
        .run(&state, "attempt-1", &CancellationToken::new())
        .await?;
    assert!(replay.replayed_terminal);
    assert!(!engine.has_container());
    assert!(state.active_device_leases()?.is_empty());
    assert_eq!(
        engine.create_count(),
        1,
        "terminal replay cannot re-execute"
    );
    assert_eq!(engine.remove_attempts(), 2);
    assert_eq!(manager.observe_calls(), 4);

    let receipt = replay.finished.receipt.expect("receipt");
    let mut reader = artifacts.open(receipt.digest)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let receipt: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(receipt["source_digest"], bundle.source_sha256);
    assert_eq!(receipt["resolved_image_id"], image_id.to_string());
    assert_eq!(receipt["device"]["device_id"], "3");
    assert_eq!(receipt["lease"]["attempt_id"], "attempt-1");
    assert_eq!(receipt["pre_observation"]["observed_at_ms"], 1);
    assert_eq!(receipt["post_observation"]["observed_at_ms"], 3);
    assert_eq!(receipt["post_commit_cleanup"], "release_after_commit");
    assert_eq!(receipt["environment"]["cann_version"], "9.1.0-beta.1");
    assert_outbound_ascend_only(
        runtime,
        stored.artifact.digest,
        stored.artifact.size_bytes,
        image_manifest,
        &device,
    )?;
    Ok(())
}

#[test]
fn ascend_fixture_admission_is_default_deny() {
    let assignment = assignment(
        Sha256Digest::digest_bytes(b"bundle"),
        1,
        Sha256Digest::digest_bytes(b"image"),
    );
    assert!(matches!(
        WorkerState::default().admit(&assignment),
        Err(WorkerError::PolicyViolation(detail)) if detail.contains("Ascend fixture")
    ));
}

fn assignment(bundle: Sha256Digest, bundle_size: u64, image: Sha256Digest) -> Assignment {
    Assignment {
        assignment_id: "assignment-1".into(),
        attempt_id: "attempt-1".into(),
        attempt_number: 1,
        idempotency_key: ASCEND_ADD_FIXTURE_ID.into(),
        task_id: "task-1".into(),
        candidate_id: "candidate-1".into(),
        execution: Some(ExecutionSpec {
            executor_kind: ExecutorKind::AscendFixture.into(),
            argv: vec![ASCEND_ADD_FIXTURE_ID.into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: Some(ArtifactRef {
                digest: bundle.to_string(),
                size_bytes: bundle_size,
                media_type: ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
            }),
            image: Some(ArtifactRef {
                digest: image.to_string(),
                size_bytes: 0,
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
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
        required_features: vec![ASCEND_FIXTURE_FEATURE.into()],
    }
}

fn device() -> AcceleratorDevice {
    AcceleratorDevice {
        device_id: "3".into(),
        product_name: "Ascend950PR".into(),
        serial_number: "serial-3".into(),
        firmware_version: "9.0.0.105.229".into(),
    }
}

fn environment() -> Result<AscendEnvironmentFacts, crate::ascend::AscendContractError> {
    AscendEnvironmentFacts::new("Ascend950PR", "9.1.0-beta.1", "25.7.rc1.6", "9.0.0.105.229")
}

fn device_nodes() -> Vec<PathBuf> {
    (0..7)
        .map(|index| PathBuf::from(format!("/dev/davinci{index}")))
        .chain([
            PathBuf::from("/dev/davinci_manager"),
            PathBuf::from("/dev/hisi_hdc"),
        ])
        .collect()
}

fn assert_outbound_ascend_only(
    runtime: Arc<AscendExecutionRuntime>,
    bundle: Sha256Digest,
    bundle_size: u64,
    image: Sha256Digest,
    device: &AcceleratorDevice,
) -> Result<(), WorkerError> {
    let worker = OutboundWorker::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:50051"),
        WorkerHello {
            protocol_major: alloyport_proto::PROTOCOL_MAJOR,
            protocol_minor: alloyport_proto::PROTOCOL_MINOR,
            worker_id: "ascend-worker-1".into(),
            instance_id: "ascend-worker-test".into(),
            worker_version: "test".into(),
            features: vec![ASCEND_FIXTURE_FEATURE.into()],
            capabilities: Some(WorkerCapabilities {
                backend: Backend::Ascend.into(),
                architecture: "Ascend950PR".into(),
                device_count: 1,
                max_concurrency: 1,
                driver_version: "25.7.rc1.6".into(),
                toolkit_version: "9.1.0-beta.1".into(),
                container_runtime: "docker".into(),
                devices: vec![WireDevice {
                    device_id: device.device_id.clone(),
                    product_name: device.product_name.clone(),
                    serial_number: device.serial_number.clone(),
                    firmware_version: device.firmware_version.clone(),
                }],
            }),
            active_attempts: Vec::new(),
        },
    )?
    .with_ascend_executor(runtime)?;
    let state = worker.state();
    assert_eq!(
        state.admit(&assignment(bundle, bundle_size, image))?,
        AdmissionOutcome::New
    );
    let mut generic = assignment(bundle, bundle_size, image);
    generic.assignment_id = "assignment-2".into();
    generic.attempt_id = "attempt-2".into();
    let execution = generic.execution.as_mut().expect("execution");
    execution.executor_kind = ExecutorKind::Container.into();
    execution.argv = vec!["true".into()];
    execution.working_directory = "source".into();
    generic.required_features.clear();
    assert!(matches!(
        state.admit(&generic),
        Err(WorkerError::PolicyViolation(_))
    ));
    Ok(())
}

fn observation(health: DeviceHealth, process_count: u32, observed_at_ms: u64) -> DeviceObservation {
    DeviceObservation {
        device_id: "3".into(),
        health,
        process_count,
        utilization_percent: 0,
        memory_used_bytes: 5_255 * 1024 * 1024,
        memory_total_bytes: 131_072 * 1024 * 1024,
        temperature_millicelsius: 56_000,
        power_milliwatts: 191_300,
        observed_at_ms,
        detail: String::new(),
    }
}

#[derive(Debug)]
struct FailOnceOrderingPublisher {
    state: WorkerState,
    calls: AtomicUsize,
}

impl FailOnceOrderingPublisher {
    fn new(state: WorkerState) -> Self {
        Self {
            state,
            calls: AtomicUsize::new(0),
        }
    }
}

impl ArtifactPublisher for FailOnceOrderingPublisher {
    fn publish<'a>(
        &'a self,
        _references: &'a [crate::executor::ArtifactReferenceIntent],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), ArtifactPublicationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let attempt = self
                .state
                .attempt("attempt-1")
                .map_err(|error| ArtifactPublicationError::Internal(error.to_string()))?
                .ok_or_else(|| ArtifactPublicationError::Internal("attempt missing".into()))?;
            if attempt.phase != LocalAttemptPhase::Running {
                return Err(ArtifactPublicationError::Internal(
                    "publisher observed terminal state too early".into(),
                ));
            }
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Err(ArtifactPublicationError::Unavailable(
                    "simulated publication outage".into(),
                ));
            }
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
    creates: usize,
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

    fn create_count(&self) -> usize {
        self.state.lock().expect("engine lock").creates
    }

    fn remove_attempts(&self) -> usize {
        self.state.lock().expect("engine lock").remove_attempts
    }
}

impl AscendContainerEngine for RecordingEngine {
    fn resolve_image_id<'a>(
        &'a self,
        _plan: &'a AscendDockerCreatePlan,
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
        _plan: &'a AscendDockerCreatePlan,
        identity: &'a ContainerIdentity,
    ) -> EngineFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().map_err(|_| "engine lock")?;
            state.creates += 1;
            state.container = Some(ContainerSnapshot {
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
                stdout: ASCEND_STDOUT.to_vec(),
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

#[derive(Debug)]
struct RecordingDeviceManager {
    observations: Mutex<VecDeque<Result<DeviceObservation, DeviceStatusError>>>,
    observe_calls: Mutex<usize>,
}

impl RecordingDeviceManager {
    fn new(observations: Vec<Result<DeviceObservation, DeviceStatusError>>) -> Self {
        Self {
            observations: Mutex::new(observations.into()),
            observe_calls: Mutex::new(0),
        }
    }

    fn observe_calls(&self) -> usize {
        *self.observe_calls.lock().expect("observe count lock")
    }
}

impl DeviceStatusProvider for RecordingDeviceManager {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async { Ok(DeviceSnapshot::default()) })
    }
}

impl AscendDeviceManager for RecordingDeviceManager {
    fn inventory(&self) -> AscendDeviceFuture<'_, Vec<AcceleratorDevice>> {
        Box::pin(async { Ok(vec![device()]) })
    }
}

impl crate::device::DeviceLifecycleManager for RecordingDeviceManager {
    fn observe_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> crate::device::DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            *self
                .observe_calls
                .lock()
                .map_err(|_| DeviceStatusError::Internal("observe count lock".into()))? += 1;
            self.observations
                .lock()
                .map_err(|_| DeviceStatusError::Internal("observations lock".into()))?
                .pop_front()
                .ok_or_else(|| DeviceStatusError::Internal("missing observation".into()))?
        })
    }

    fn recover_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> crate::device::DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async { Err(DeviceStatusError::RecoveryUnsupported("test".into())) })
    }
}
