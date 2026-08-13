//! Behavioral tests for the CUDA execution runtime module.

use super::*;
use crate::cuda::{
    CUDA_FIXTURE_BUNDLE_MEDIA_TYPE, CUDA_FIXTURE_FEATURE, CudaFixtureBundle, CudaFixturePolicy,
    CudaResourceCeilings, OCI_IMAGE_MANIFEST_MEDIA_TYPE, VECTOR_ADD_FIXTURE_ID,
};
use crate::cuda_supervisor::{
    ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream, ContainerLogs,
    ContainerPhase, ContainerSnapshot, EngineFuture,
};
use crate::device::{
    DeviceLifecycleFuture, DeviceLifecycleManager, DeviceSnapshot, DeviceSnapshotFuture,
    DeviceStatusError, DeviceStatusProvider,
};
use crate::executor::{ArtifactPublicationError, CancellationToken};
use crate::{AdmissionOutcome, AdmissionPolicy, OutboundWorker, WorkerError};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, IngestRequest, Sha256Digest};
use alloyport_core::{AcceleratorDevice, DeviceHealth, DeviceObservation};
use alloyport_proto::v1::{
    ArtifactRef, Assignment, Backend, ExecutionSpec, ExecutorKind, NetworkPolicy, ResourceLimits,
    WorkerCapabilities, WorkerHello,
};
use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicBool, Ordering};

const CUDA_RUNTIME_STDOUT: &[u8] =
    b"PASS fixture=cuda-vectoradd-v1 elements=1048576 checksum=670562424\n";

#[tokio::test]
async fn dynamic_runtime_selects_a_free_gpu_when_the_first_gpu_is_busy()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("cas"),
        64 * 1024,
    )?);
    let bundle = Sha256Digest::digest_bytes(b"dynamic-bundle");
    let image = Sha256Digest::digest_bytes(b"dynamic-image");
    let state = WorkerState::with_policy(AdmissionPolicy::default().allowing_cuda_fixture());
    assert_eq!(
        state.admit(&assignment(bundle, 1, image))?,
        AdmissionOutcome::New
    );
    let observations = vec![
        DeviceObservation {
            device_id: "0".into(),
            process_count: 1,
            ..cuda_observation()
        },
        DeviceObservation {
            device_id: "1".into(),
            ..cuda_observation()
        },
    ];
    let manager: Arc<dyn DeviceLifecycleManager> = Arc::new(MultiDeviceManager { observations });
    let inventory = vec![cuda_device("0"), cuda_device("1")];
    let factory_artifacts = artifacts.clone();
    let sandbox_root = directory.path().join("sandboxes");
    let factory = Arc::new(move |device_id: &str| {
        let policy = CudaFixturePolicy::new(
            VECTOR_ADD_FIXTURE_ID,
            bundle,
            image,
            format!("example.invalid/cuda@{image}"),
            image,
            device_id,
            &sandbox_root,
            CudaResourceCeilings {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 128 * 1024 * 1024,
                process_count: 1,
                output_bytes: 1,
            },
        )
        .map_err(|error| {
            ExecutionRuntimeError::Backend(BackendError::integrity(error.to_string()))
        })?;
        Ok(Arc::new(CudaContainerSupervisor::new(
            Arc::new(policy),
            factory_artifacts.clone(),
        )))
    });
    let engine: Arc<dyn CudaContainerEngine> =
        Arc::new(RecordingEngine::new(state.clone(), image.to_string()));
    let runtime = CudaExecutionRuntime::new_dynamic(
        "cuda-worker-1",
        artifacts,
        engine,
        CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
        manager,
        inventory,
        DeviceSelectionPolicy::default(),
        alloyport_core::ExecutionKind::CudaFixture,
        factory,
    )?;

    let selected = runtime.runtime_for_attempt(&state, "attempt-1").await?;
    assert_eq!(selected.device_id, "1");
    Ok(())
}

fn cuda_device(device_id: &str) -> AcceleratorDevice {
    AcceleratorDevice {
        device_id: device_id.into(),
        product_name: "NVIDIA GB10".into(),
        serial_number: format!("serial-{device_id}"),
        firmware_version: "580.159.03".into(),
    }
}

#[derive(Debug)]
struct MultiDeviceManager {
    observations: Vec<DeviceObservation>,
}

impl DeviceStatusProvider for MultiDeviceManager {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async {
            Ok(DeviceSnapshot {
                devices: self.observations.clone(),
            })
        })
    }
}

impl DeviceLifecycleManager for MultiDeviceManager {
    fn observe_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            self.observations
                .iter()
                .find(|observation| observation.device_id == device_id)
                .cloned()
                .ok_or_else(|| DeviceStatusError::Unavailable("missing test device".into()))
        })
    }

    fn recover_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async {
            Err(DeviceStatusError::RecoveryUnsupported(
                "test reset is disabled".into(),
            ))
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
    let supervisor = Arc::new(CudaContainerSupervisor::new(policy, artifacts.clone()));
    let device_manager = Arc::new(StaticDeviceManager::new(3));
    let runtime = Arc::new(CudaExecutionRuntime::new(
        "cuda-worker-1",
        artifacts.clone(),
        supervisor,
        engine_trait,
        CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
        device_manager,
    )?);
    let publisher = OrderingPublisher::new(state.clone());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let recorded_observations = Arc::clone(&observations);

    assert!(matches!(
        runtime
            .run_observed_and_publish(
                &state,
                "attempt-1",
                &CancellationToken::new(),
                &publisher,
                move |observation| recorded_observations
                    .lock()
                    .expect("observation lock")
                    .push(observation)
            )
            .await,
        Err(ExecutionRuntimeError::CleanupAfterCommit(_))
    ));
    assert!(publisher.called.load(Ordering::SeqCst));
    assert_live_observations(&observations);
    let terminal = state.attempt("attempt-1")?.expect("attempt exists");
    assert_eq!(terminal.phase, LocalAttemptPhase::Finished);
    assert!(engine.has_container());
    assert_eq!(state.active_device_leases()?.len(), 1);

    let replay = runtime
        .run(&state, "attempt-1", &CancellationToken::new())
        .await?;
    assert!(replay.replayed_terminal);
    assert!(!engine.has_container());
    assert_eq!(engine.remove_attempts(), 2);
    assert!(state.active_device_leases()?.is_empty());

    let receipt = replay.finished.receipt.expect("receipt is persisted");
    let digest = receipt.digest;
    let mut reader = artifacts.open(digest)?;
    let mut receipt_bytes = Vec::new();
    reader.read_to_end(&mut receipt_bytes)?;
    let receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes)?;
    assert_eq!(receipt["source_digest"], bundle.source_sha256);
    assert_eq!(receipt["resolved_image_id"], image_id.to_string());
    assert_eq!(receipt["device_id"], "0");
    assert_eq!(receipt["environment"]["driver_version"], "580.159.03");
    assert_eq!(receipt["lease"]["device_id"], "0");
    assert_eq!(receipt["pre_observation"]["health"], 1);
    assert_eq!(receipt["post_observation"]["health"], 1);
    assert_eq!(receipt["post_commit_cleanup"], "release_after_commit");

    assert_outbound_cuda_only(
        runtime,
        stored.artifact.digest,
        stored.artifact.size_bytes,
        image_manifest,
    )?;
    Ok(())
}

#[tokio::test]
async fn cuda_attempt_quarantines_a_device_that_became_unhealthy_before_preflight()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("cas"),
        64 * 1024,
    )?);
    let bundle = CudaFixtureBundle::vector_add("__global__ void vector_add() {}\n");
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
    state.admit(&assignment(
        stored.artifact.digest,
        stored.artifact.size_bytes,
        image_manifest,
    ))?;
    let engine = Arc::new(RecordingEngine::new(state.clone(), image_id.to_string()));
    let engine_trait: Arc<dyn CudaContainerEngine> = engine.clone();
    let supervisor = Arc::new(CudaContainerSupervisor::new(policy, artifacts.clone()));
    let device_manager: Arc<dyn DeviceLifecycleManager> = Arc::new(FixedObservationManager {
        observation: DeviceObservation {
            health: DeviceHealth::Unhealthy,
            detail: "gpu_recovery_action=Reset".into(),
            ..cuda_observation()
        },
    });
    let runtime = CudaExecutionRuntime::new(
        "cuda-worker-1",
        artifacts,
        supervisor,
        engine_trait,
        CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
        device_manager,
    )?;

    assert!(
        runtime
            .run(&state, "attempt-1", &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(
        state.attempt("attempt-1")?.expect("attempt exists").phase,
        LocalAttemptPhase::Accepted
    );
    assert_eq!(state.active_device_leases()?.len(), 1);
    assert!(!engine.has_container());
    Ok(())
}

#[derive(Debug)]
struct StaticDeviceManager {
    remaining_observations: Mutex<usize>,
}

#[derive(Debug)]
struct FixedObservationManager {
    observation: DeviceObservation,
}

impl DeviceStatusProvider for FixedObservationManager {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async move {
            Ok(DeviceSnapshot {
                devices: vec![self.observation.clone()],
            })
        })
    }
}

impl DeviceLifecycleManager for FixedObservationManager {
    fn observe_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move { Ok(self.observation.clone()) })
    }

    fn recover_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async {
            Err(DeviceStatusError::RecoveryUnsupported(
                "test reset is disabled".into(),
            ))
        })
    }
}

impl StaticDeviceManager {
    fn new(observations: usize) -> Self {
        Self {
            remaining_observations: Mutex::new(observations),
        }
    }
}

impl DeviceStatusProvider for StaticDeviceManager {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async {
            Ok(DeviceSnapshot {
                devices: vec![cuda_observation()],
            })
        })
    }
}

impl DeviceLifecycleManager for StaticDeviceManager {
    fn observe_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            let mut remaining = self
                .remaining_observations
                .lock()
                .map_err(|_| DeviceStatusError::Internal("device observation lock".into()))?;
            if *remaining == 0 {
                return Err(DeviceStatusError::Internal(
                    "unexpected device observation".into(),
                ));
            }
            *remaining -= 1;
            Ok(cuda_observation())
        })
    }

    fn recover_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async {
            Err(DeviceStatusError::RecoveryUnsupported(
                "test reset is disabled".into(),
            ))
        })
    }
}

fn cuda_observation() -> DeviceObservation {
    DeviceObservation {
        device_id: "0".into(),
        health: DeviceHealth::Ready,
        process_count: 0,
        utilization_percent: 0,
        memory_used_bytes: 0,
        memory_total_bytes: 24 * 1024 * 1024 * 1024,
        temperature_millicelsius: 40_000,
        power_milliwatts: 20_000,
        observed_at_ms: 1,
        detail: "gpu_recovery_action=None".into(),
    }
}

fn assert_live_observations(observations: &Mutex<Vec<ExecutionObservation>>) {
    let observations = observations.lock().expect("observation lock");
    let output = observations
        .iter()
        .filter_map(|observation| match observation {
            ExecutionObservation::Output(chunk) => Some(chunk),
            ExecutionObservation::Started => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        output.len(),
        2,
        "terminal output must not duplicate live chunks"
    );
    assert_eq!(output[0].byte_offset, 0);
    assert_eq!(output[1].byte_offset, 5);
    assert_eq!(
        output
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>(),
        CUDA_RUNTIME_STDOUT
    );
}

fn assert_outbound_cuda_only(
    runtime: Arc<CudaExecutionRuntime>,
    bundle: Sha256Digest,
    bundle_size: u64,
    image: Sha256Digest,
) -> Result<(), WorkerError> {
    let worker = OutboundWorker::new(
        tonic::transport::Endpoint::from_static("http://127.0.0.1:50051"),
        worker_hello(),
    )?
    .with_cuda_executor(runtime)?;
    let worker_state = worker.state();
    assert_eq!(
        worker_state.admit(&assignment(bundle, bundle_size, image))?,
        AdmissionOutcome::New
    );
    let mut generic = assignment(bundle, bundle_size, image);
    generic.assignment_id = "assignment-2".into();
    generic.attempt_id = "attempt-2".into();
    let execution = generic.execution.as_mut().expect("execution exists");
    execution.executor_kind = ExecutorKind::Container.into();
    execution.argv = vec!["true".into()];
    execution.working_directory = "source".into();
    generic.required_features.clear();
    assert!(matches!(
        worker_state.admit(&generic),
        Err(WorkerError::PolicyViolation(_))
    ));
    Ok(())
}

fn worker_hello() -> WorkerHello {
    WorkerHello {
        protocol_major: alloyport_proto::PROTOCOL_MAJOR,
        protocol_minor: alloyport_proto::PROTOCOL_MINOR,
        worker_id: "cuda-worker-1".into(),
        instance_id: "cuda-worker-1-test".into(),
        worker_version: "test".into(),
        features: Vec::new(),
        capabilities: Some(WorkerCapabilities {
            backend: Backend::Cuda.into(),
            architecture: "sm_121".into(),
            device_count: 1,
            max_concurrency: 1,
            driver_version: "580.159.03".into(),
            toolkit_version: "13.0".into(),
            container_runtime: "docker".into(),
            devices: Vec::new(),
        }),
        active_attempts: Vec::new(),
    }
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
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), ArtifactPublicationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let attempt = self
                .state
                .attempt("attempt-1")
                .map_err(|error| ArtifactPublicationError::Internal(error.to_string()))?
                .ok_or_else(|| ArtifactPublicationError::Internal("attempt missing".to_owned()))?;
            if attempt.phase != LocalAttemptPhase::Running {
                return Err(ArtifactPublicationError::Internal(
                    "publisher observed terminal state too early".into(),
                ));
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
                stdout: CUDA_RUNTIME_STDOUT.to_vec(),
                stderr: Vec::new(),
                output_limit_exceeded: false,
            })
        })
    }

    fn follow_logs_observed<'a>(
        &'a self,
        _name: &'a str,
        _limit: u64,
        observer: &'a mut (dyn FnMut(ContainerLogChunk) + Send),
    ) -> EngineFuture<'a, ContainerLogs> {
        Box::pin(async move {
            observer(ContainerLogChunk {
                stream: ContainerLogStream::Stdout,
                byte_offset: 0,
                bytes: CUDA_RUNTIME_STDOUT[..5].to_vec(),
            });
            observer(ContainerLogChunk {
                stream: ContainerLogStream::Stdout,
                byte_offset: 5,
                bytes: CUDA_RUNTIME_STDOUT[5..].to_vec(),
            });
            Ok(ContainerLogs {
                stdout: CUDA_RUNTIME_STDOUT.to_vec(),
                stderr: Vec::new(),
                output_limit_exceeded: false,
            })
        })
    }

    fn streams_live_log_observations(&self) -> bool {
        true
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
