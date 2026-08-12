use alloyport_artifacts::SqliteUploadStore;
use alloyport_artifacts::upload::{BeginUpload, UploadQuotas};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, Sha256Digest};
use alloyport_core::{DeviceHealth, DeviceObservation};
use alloyport_events::{Event, OutputStream as EventOutputStream};
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::v1::{
    AcceleratorDevice as WireDevice, ArtifactRef, Assignment, Backend, ExecutionSpec, ExecutorKind,
    NetworkPolicy, ResourceLimits, WorkerCapabilities, WorkerHello,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_server::artifact::{ArtifactAccessPolicy, ArtifactServiceImpl};
use alloyport_server::{AssignmentState, EnqueueOutcome, ManualClock, WorkerControlService};
use alloyport_worker::artifact_download::RemoteArtifactDownloader;
use alloyport_worker::artifact_upload::RemoteArtifactPublisher;
use alloyport_worker::ascend::{
    ASCEND_ADD_FIXTURE_ID, ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE, ASCEND_FIXTURE_FEATURE,
    AscendEnvironmentFacts, AscendFixtureBundle, AscendFixturePolicy, AscendResourceCeilings,
    OCI_IMAGE_CONFIG_MEDIA_TYPE,
};
use alloyport_worker::ascend_runtime::AscendExecutionRuntime;
use alloyport_worker::ascend_smi::{AscendDeviceManager, NpuSmi};
use alloyport_worker::ascend_supervisor::{AscendContainerEngine, AscendContainerSupervisor};
use alloyport_worker::cuda::{
    CUDA_FIXTURE_BUNDLE_MEDIA_TYPE, CUDA_FIXTURE_FEATURE, CudaFixtureBundle, CudaFixturePolicy,
    CudaResourceCeilings, OCI_IMAGE_MANIFEST_MEDIA_TYPE, VECTOR_ADD_FIXTURE_ID,
};
use alloyport_worker::cuda_docker::DockerCliEngine;
use alloyport_worker::cuda_runtime::{CudaEnvironmentFacts, CudaExecutionRuntime};
use alloyport_worker::cuda_supervisor::{
    ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream, ContainerLogs,
    ContainerPhase, ContainerSnapshot, CudaContainerEngine, CudaContainerSupervisor, EngineFuture,
};
use alloyport_worker::device::{
    BoundDeviceStatusProvider, DeviceLifecycleFuture, DeviceLifecycleManager,
    DeviceSelectionPolicy, DeviceSnapshot, DeviceSnapshotFuture, DeviceStatusError,
    DeviceStatusProvider, bind_worker_device,
};
use alloyport_worker::nvidia_smi::{CudaDeviceManager, NvidiaSmi};
use alloyport_worker::{OutboundWorker, StoredFinished};
use std::error::Error;
use std::fs;
use std::io::Read;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Extensions, Status};

const CUDA_STDOUT: &[u8] = b"PASS fixture=cuda-vectoradd-v1 elements=1048576 checksum=670562424\n";
const ASCEND_STDOUT: &[u8] =
    b"PASS fixture=ascend-add-v1 elements=16384 checksum=3d2cf971e11e0383\n";

#[tokio::test]
async fn cuda_runtime_completes_through_outbound_control_and_artifact_planes()
-> Result<(), Box<dyn Error>> {
    let fixture = CudaLoopbackFixture::start().await?;
    let worker_state = fixture.worker.state();
    let worker = fixture.worker.clone();
    let first_worker_task = tokio::spawn(async move { worker.run_session().await });

    wait_until(|| async {
        fixture
            .service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|worker| worker.connected)
    })
    .await?;
    assert_eq!(
        fixture
            .service
            .enqueue_assignment("cuda-1", fixture.assignment())
            .await?,
        EnqueueOutcome::Sent
    );
    let first_session = tokio::time::timeout(Duration::from_secs(5), first_worker_task).await??;
    assert!(
        first_session.is_err(),
        "the simulated cleanup failure ends the session"
    );
    assert!(fixture.engine.has_container());

    let worker = fixture.worker.clone();
    let second_worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        fixture.service.assignment_state("attempt-1").ok().flatten()
            == Some(AssignmentState::Finished)
    })
    .await?;

    let finished = worker_state
        .finished_attempt("attempt-1")?
        .expect("CUDA terminal state is durable");
    assert_terminal_artifacts(&fixture.uploads, &finished, "attempt-1", Some(CUDA_STDOUT))?;
    let output_chunks = assert_live_stdout(&fixture.service, CUDA_STDOUT)?;
    assert_eq!(
        output_chunks, 2,
        "live CUDA output must not be repeated at terminal"
    );
    assert_eq!(fixture.engine.remove_count(), 2);
    assert!(!fixture.engine.has_container());

    second_worker_task.abort();
    let _ = second_worker_task.await;
    fixture.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly configured CUDA host and Docker image"]
async fn cuda_runtime_completes_through_real_docker_outbound_loopback() -> Result<(), Box<dyn Error>>
{
    let image_manifest =
        Sha256Digest::from_str(&required_env("ALLOYPORT_CUDA_SMOKE_IMAGE_MANIFEST_DIGEST")?)?;
    let image_reference = required_env("ALLOYPORT_CUDA_SMOKE_IMAGE_REFERENCE")?;
    let image_id = Sha256Digest::from_str(&required_env("ALLOYPORT_CUDA_SMOKE_IMAGE_ID")?)?;
    let fixture = RealCudaLoopbackFixture::start(image_manifest, image_reference, image_id).await?;
    let worker_state = fixture.worker.state();
    let worker = fixture.worker.clone();
    let worker_task = tokio::spawn(async move { worker.run_session().await });

    wait_until(|| async {
        fixture
            .service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|worker| worker.connected)
    })
    .await?;
    let attempt_id = format!(
        "gb10-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
    );
    assert_eq!(
        fixture
            .service
            .enqueue_assignment(
                "cuda-1",
                cuda_assignment_for(
                    &attempt_id,
                    fixture.bundle_digest,
                    fixture.bundle_size,
                    image_manifest,
                    120_000,
                ),
            )
            .await?,
        EnqueueOutcome::Sent
    );
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if fixture.service.assignment_state(&attempt_id).ok().flatten()
                == Some(AssignmentState::Finished)
            {
                return;
            }
            assert!(
                !worker_task.is_finished(),
                "worker session ended before terminal commit"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;

    let finished = worker_state
        .finished_attempt(&attempt_id)?
        .expect("real CUDA terminal state is durable");
    assert_terminal_artifacts(&fixture.uploads, &finished, &attempt_id, Some(CUDA_STDOUT))?;
    let stdout = read_artifact(&fixture.local_artifacts, finished.stdout.as_ref())?;
    println!("GB10_OUTBOUND_STDOUT={}", String::from_utf8_lossy(&stdout));
    assert_eq!(stdout, CUDA_STDOUT);
    assert!(assert_live_stdout(&fixture.service, CUDA_STDOUT)? > 0);
    let receipt: serde_json::Value = serde_json::from_slice(&read_artifact(
        &fixture.local_artifacts,
        finished.receipt.as_ref(),
    )?)?;
    assert_eq!(receipt["bundle_digest"], fixture.bundle_digest.to_string());
    assert_eq!(receipt["image_digest"], image_manifest.to_string());
    assert_eq!(
        receipt["image_media_type"],
        "application/vnd.oci.image.manifest.v1+json"
    );
    assert_eq!(receipt["resolved_image_id"], image_id.to_string());
    assert_eq!(receipt["device_id"], "0");
    assert_eq!(receipt["lease"]["device_id"], "0");
    assert_eq!(receipt["pre_observation"]["health"], 1);
    assert_eq!(receipt["post_observation"]["health"], 1);
    assert_eq!(receipt["post_commit_cleanup"], "release_after_commit");
    assert_eq!(receipt["outcome"], "ATTEMPT_OUTCOME_SUCCEEDED");
    assert!(
        CudaContainerEngine::inspect(&*fixture.engine, &format!("alloyport-{attempt_id}"))
            .await?
            .is_none(),
        "terminal commit must be followed by container removal"
    );
    println!("GB10_OUTBOUND_RECEIPT={receipt}");

    worker_task.abort();
    let _ = worker_task.await;
    fixture.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly configured Ascend host and local fixture image"]
async fn ascend_runtime_completes_through_real_docker_outbound_loopback()
-> Result<(), Box<dyn Error>> {
    let fixture = RealAscendLoopbackFixture::start().await?;
    let worker_state = fixture.worker.state();
    let worker = fixture.worker.clone();
    let mut worker_task = tokio::spawn(async move { worker.run_session().await });

    wait_until(|| async {
        fixture
            .service
            .worker_snapshot("ascend-1")
            .await
            .is_some_and(|worker| worker.connected)
    })
    .await?;
    let attempt_id = format!(
        "ascend950-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
    );
    assert_eq!(
        fixture
            .service
            .enqueue_assignment(
                "ascend-1",
                ascend_assignment_for(
                    &attempt_id,
                    fixture.bundle_digest,
                    fixture.bundle_size,
                    fixture.image_id,
                ),
            )
            .await?,
        EnqueueOutcome::Sent
    );
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if fixture.service.assignment_state(&attempt_id).ok().flatten()
                == Some(AssignmentState::Finished)
            {
                return Ok::<(), Box<dyn Error>>(());
            }
            if worker_task.is_finished() {
                match (&mut worker_task).await {
                    Ok(Ok(())) => {
                        return Err::<(), Box<dyn Error>>(
                            "Ascend worker session ended before terminal commit".into(),
                        );
                    }
                    Ok(Err(error)) => return Err(error.into()),
                    Err(error) => return Err(error.into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;

    let finished = worker_state
        .finished_attempt(&attempt_id)?
        .expect("real Ascend terminal state is durable");
    assert_eq!(finished.outcome, alloyport_core::AttemptOutcome::Succeeded);
    let stdout = read_artifact(&fixture.local_artifacts, finished.stdout.as_ref())?;
    assert!(
        stdout.ends_with(ASCEND_STDOUT),
        "Ascend fixture stdout must end in the deterministic PASS record"
    );
    let receipt: serde_json::Value = serde_json::from_slice(&read_artifact(
        &fixture.local_artifacts,
        finished.receipt.as_ref(),
    )?)?;
    assert_eq!(receipt["bundle_digest"], fixture.bundle_digest.to_string());
    assert_eq!(receipt["image_digest"], fixture.image_id.to_string());
    assert_eq!(receipt["image_media_type"], OCI_IMAGE_CONFIG_MEDIA_TYPE);
    assert_eq!(receipt["resolved_image_id"], fixture.image_id.to_string());
    assert_eq!(receipt["device"]["device_id"], fixture.device_id);
    assert_eq!(receipt["lease"]["device_id"], fixture.device_id);
    assert_eq!(receipt["pre_observation"]["health"], 1);
    assert_eq!(receipt["pre_observation"]["process_count"], 0);
    assert_eq!(receipt["post_observation"]["health"], 1);
    assert_eq!(receipt["post_commit_cleanup"], "release_after_commit");
    assert!(worker_state.active_device_leases()?.is_empty());
    assert!(
        AscendContainerEngine::inspect(&*fixture.engine, &format!("alloyport-{attempt_id}"))
            .await?
            .is_none(),
        "terminal commit must be followed by container removal"
    );
    println!(
        "ASCEND_OUTBOUND_STDOUT={}",
        String::from_utf8_lossy(&stdout)
    );
    println!("ASCEND_OUTBOUND_RECEIPT={receipt}");

    worker_task.abort();
    let _ = worker_task.await;
    fixture.shutdown().await?;
    Ok(())
}

struct RealAscendLoopbackFixture {
    _directory: tempfile::TempDir,
    service: WorkerControlService,
    local_artifacts: Arc<FilesystemArtifactStore>,
    worker: OutboundWorker,
    engine: Arc<DockerCliEngine>,
    bundle_digest: Sha256Digest,
    bundle_size: u64,
    image_id: Sha256Digest,
    device_id: String,
    shutdown: oneshot::Sender<()>,
    server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RealAscendLoopbackFixture {
    #[allow(clippy::too_many_lines)]
    async fn start() -> Result<Self, Box<dyn Error>> {
        let image_reference = required_env("ALLOYPORT_ASCEND_SMOKE_IMAGE_REFERENCE")?;
        let image_id = Sha256Digest::from_str(&required_env("ALLOYPORT_ASCEND_SMOKE_IMAGE_ID")?)?;
        let firmware_version = required_env("ALLOYPORT_ASCEND_SMOKE_FIRMWARE_VERSION")?;
        let driver_version = required_env("ALLOYPORT_ASCEND_SMOKE_DRIVER_VERSION")?;
        let cann_version = required_env("ALLOYPORT_ASCEND_SMOKE_CANN_VERSION")?;
        let npu_smi_binary = required_env("ALLOYPORT_ASCEND_SMOKE_NPU_SMI")?;
        let directory = tempfile::tempdir()?;
        let local_artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("worker-cas"),
            16 * 1024 * 1024,
        )?);
        let remote_artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("server-cas"),
            16 * 1024 * 1024,
        )?);
        let uploads = Arc::new(SqliteUploadStore::open_with_quotas(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("upload-data"),
            16 * 1024 * 1024,
            1024 * 1024,
            UploadQuotas::unbounded(),
        )?);
        let bundle = AscendFixtureBundle::add(include_str!(
            "../../../fixtures/ascend-add-v1/add_custom.cpp"
        ));
        let bundle_bytes = serde_json::to_vec(&bundle)?;
        let bundle_digest = Sha256Digest::digest_bytes(&bundle_bytes);
        let bundle_size = u64::try_from(bundle_bytes.len())?;
        publish_fixture_bundle(
            &uploads,
            remote_artifacts.as_ref(),
            "fixture:ascend-add-v1",
            ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE,
            &bundle_bytes,
        )?;
        let service = WorkerControlService::new().with_artifact_metadata(uploads.clone());
        let (endpoint, shutdown, server_task) =
            start_loopback_services(service.clone(), uploads, remote_artifacts, "ascend-1").await?;

        let manager = Arc::new(NpuSmi::new(&npu_smi_binary, &firmware_version)?);
        let inventory = manager.inventory().await?;
        let snapshot = manager.snapshot().await?;
        let selected = bind_worker_device(
            &inventory,
            &snapshot,
            &[],
            &DeviceSelectionPolicy::default(),
        )?;
        let device = selected.identity;
        let bound_device = device.clone();
        let device_id = device.device_id.clone();
        let environment = AscendEnvironmentFacts::new(
            &device.product_name,
            &cann_version,
            &driver_version,
            firmware_version,
        )?;
        let policy = Arc::new(AscendFixturePolicy::new(
            ASCEND_ADD_FIXTURE_ID,
            bundle_digest,
            image_id,
            image_reference,
            image_id,
            device.clone(),
            discover_ascend_nodes(Path::new("/dev"))?,
            "/usr/local/Ascend/driver",
            directory.path().join("sandboxes"),
            AscendResourceCeilings {
                timeout_ms: 120_000,
                cpu_millis: 4_000,
                memory_bytes: 8 * 1024 * 1024 * 1024,
                disk_bytes: 1024 * 1024 * 1024,
                process_count: 128,
                output_bytes: 1024 * 1024,
            },
            environment,
        )?);
        let engine = Arc::new(DockerCliEngine::new("/usr/bin/docker")?);
        let engine_trait: Arc<dyn AscendContainerEngine> = engine.clone();
        let supervisor = Arc::new(AscendContainerSupervisor::new(
            policy,
            local_artifacts.clone(),
        ));
        let manager_trait: Arc<dyn AscendDeviceManager> = manager.clone();
        let runtime = Arc::new(AscendExecutionRuntime::new(
            "ascend-1",
            local_artifacts.clone(),
            supervisor,
            engine_trait,
            manager_trait,
        )?);
        let publisher = Arc::new(RemoteArtifactPublisher::new(
            endpoint.clone(),
            local_artifacts.clone(),
            1024 * 1024,
            Some(60_000),
        )?);
        let downloader = Arc::new(RemoteArtifactDownloader::new(
            endpoint.clone(),
            local_artifacts.clone(),
            16 * 1024 * 1024,
        )?);
        let worker =
            OutboundWorker::new(endpoint, ascend_hello(device, driver_version, cann_version))?
                .with_bound_device(bound_device)?
                .with_ascend_executor(runtime)?
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(Arc::new(BoundDeviceStatusProvider::new(
                    manager, &device_id,
                )?));
        Ok(Self {
            _directory: directory,
            service,
            local_artifacts,
            worker,
            engine,
            bundle_digest,
            bundle_size,
            image_id,
            device_id,
            shutdown,
            server_task,
        })
    }

    async fn shutdown(self) -> Result<(), Box<dyn Error>> {
        let _ = self.shutdown.send(());
        self.server_task.await??;
        Ok(())
    }
}

struct RealCudaLoopbackFixture {
    _directory: tempfile::TempDir,
    service: WorkerControlService,
    uploads: Arc<SqliteUploadStore>,
    local_artifacts: Arc<FilesystemArtifactStore>,
    worker: OutboundWorker,
    engine: Arc<DockerCliEngine>,
    bundle_digest: Sha256Digest,
    bundle_size: u64,
    shutdown: oneshot::Sender<()>,
    server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl RealCudaLoopbackFixture {
    async fn start(
        image_manifest: Sha256Digest,
        image_reference: String,
        image_id: Sha256Digest,
    ) -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let local_artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("worker-cas"),
            8 * 1024 * 1024,
        )?);
        let remote_artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("server-cas"),
            8 * 1024 * 1024,
        )?);
        let uploads = Arc::new(SqliteUploadStore::open_with_quotas(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("upload-data"),
            8 * 1024 * 1024,
            1024 * 1024,
            UploadQuotas::unbounded(),
        )?);
        let bundle = CudaFixtureBundle::vector_add(include_str!(
            "../../../fixtures/cuda-vectoradd-v1/vector_add.cu"
        ));
        let bundle_bytes = serde_json::to_vec(&bundle)?;
        let bundle_digest = Sha256Digest::digest_bytes(&bundle_bytes);
        let bundle_size = u64::try_from(bundle_bytes.len())?;
        publish_input_bundle(&uploads, remote_artifacts.as_ref(), &bundle_bytes)?;
        let service = WorkerControlService::new().with_artifact_metadata(uploads.clone());
        let (endpoint, shutdown, server_task) = start_loopback_services(
            service.clone(),
            Arc::clone(&uploads),
            remote_artifacts,
            "cuda-1",
        )
        .await?;

        let policy = Arc::new(CudaFixturePolicy::new(
            VECTOR_ADD_FIXTURE_ID,
            bundle_digest,
            image_manifest,
            image_reference,
            image_id,
            "0",
            directory.path().join("sandboxes"),
            ceilings(),
        )?);
        let engine = Arc::new(DockerCliEngine::new("/usr/bin/docker")?);
        let engine_trait: Arc<dyn CudaContainerEngine> = engine.clone();
        let supervisor = Arc::new(CudaContainerSupervisor::new(
            policy,
            local_artifacts.clone(),
        ));
        let device_manager = Arc::new(NvidiaSmi::new("/usr/bin/nvidia-smi")?);
        let device = device_manager
            .inventory()
            .await?
            .into_iter()
            .find(|device| device.device_id == "0")
            .ok_or("nvidia-smi omitted configured CUDA device 0")?;
        let runtime_device_manager: Arc<dyn DeviceLifecycleManager> = device_manager.clone();
        let runtime = Arc::new(CudaExecutionRuntime::new(
            "cuda-1",
            local_artifacts.clone(),
            supervisor,
            engine_trait,
            CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
            runtime_device_manager,
        )?);
        let publisher = Arc::new(RemoteArtifactPublisher::new(
            endpoint.clone(),
            local_artifacts.clone(),
            1024 * 1024,
            Some(60_000),
        )?);
        let downloader = Arc::new(RemoteArtifactDownloader::new(
            endpoint.clone(),
            local_artifacts.clone(),
            8 * 1024 * 1024,
        )?);
        let worker = OutboundWorker::new(endpoint, hello())?
            .with_bound_device(device)?
            .with_cuda_executor(runtime)?
            .with_artifact_downloader(downloader)
            .with_artifact_publisher(publisher)
            .with_device_status_provider(Arc::new(BoundDeviceStatusProvider::new(
                device_manager,
                "0",
            )?));
        Ok(Self {
            _directory: directory,
            service,
            uploads,
            local_artifacts,
            worker,
            engine,
            bundle_digest,
            bundle_size,
            shutdown,
            server_task,
        })
    }

    async fn shutdown(self) -> Result<(), Box<dyn Error>> {
        let _ = self.shutdown.send(());
        self.server_task.await??;
        Ok(())
    }
}

async fn start_loopback_services(
    service: WorkerControlService,
    uploads: Arc<SqliteUploadStore>,
    remote_artifacts: Arc<FilesystemArtifactStore>,
    owner_id: &str,
) -> Result<
    (
        Endpoint,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ),
    Box<dyn Error>,
> {
    let artifact_service = ArtifactServiceImpl::new(
        uploads,
        remote_artifacts,
        Arc::new(FixedArtifactOwner::new(owner_id)),
        Arc::new(ManualClock::new(2_000)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Endpoint::from_shared(format!("http://{}", listener.local_addr()?))?;
    let (shutdown, shutdown_receive) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(service))
            .add_service(ArtifactServiceServer::new(artifact_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });
    Ok((endpoint, shutdown, server_task))
}

struct CudaLoopbackFixture {
    _directory: tempfile::TempDir,
    service: WorkerControlService,
    uploads: Arc<SqliteUploadStore>,
    worker: OutboundWorker,
    engine: Arc<ImmediateCudaEngine>,
    bundle_digest: Sha256Digest,
    bundle_size: u64,
    image_manifest: Sha256Digest,
    shutdown: oneshot::Sender<()>,
    server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl CudaLoopbackFixture {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let local_artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("worker-cas"),
            8_192,
        )?);
        let remote_artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("server-cas"),
            8_192,
        )?);
        let uploads = Arc::new(SqliteUploadStore::open_with_quotas(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("upload-data"),
            8_192,
            8_192,
            UploadQuotas::unbounded(),
        )?);
        let bundle = CudaFixtureBundle::vector_add(include_str!(
            "../../../fixtures/cuda-vectoradd-v1/vector_add.cu"
        ));
        let bundle_bytes = serde_json::to_vec(&bundle)?;
        let bundle_digest = Sha256Digest::digest_bytes(&bundle_bytes);
        let bundle_size = u64::try_from(bundle_bytes.len())?;
        publish_input_bundle(&uploads, remote_artifacts.as_ref(), &bundle_bytes)?;

        let service = WorkerControlService::new().with_artifact_metadata(uploads.clone());
        let artifact_service = ArtifactServiceImpl::new(
            uploads.clone(),
            remote_artifacts,
            Arc::new(FixedArtifactOwner::new("cuda-1")),
            Arc::new(ManualClock::new(2_000)),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let endpoint = Endpoint::from_shared(format!("http://{address}"))?;
        let (shutdown, shutdown_receive) = oneshot::channel();
        let grpc_service = service.clone();
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerControlServer::new(grpc_service))
                .add_service(ArtifactServiceServer::new(artifact_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_receive.await;
                })
                .await
        });

        let image_manifest = Sha256Digest::digest_bytes(b"manifest");
        let image_id = Sha256Digest::digest_bytes(b"image-id");
        let policy = Arc::new(CudaFixturePolicy::new(
            VECTOR_ADD_FIXTURE_ID,
            bundle_digest,
            image_manifest,
            format!("example.invalid/cuda@{image_manifest}"),
            image_id,
            "0",
            directory.path().join("sandboxes"),
            ceilings(),
        )?);
        let engine = Arc::new(ImmediateCudaEngine::new(image_id.to_string()));
        let engine_trait: Arc<dyn CudaContainerEngine> = engine.clone();
        let supervisor = Arc::new(CudaContainerSupervisor::new(
            policy,
            local_artifacts.clone(),
        ));
        let device_manager: Arc<dyn DeviceLifecycleManager> = Arc::new(ReadyCudaDeviceManager);
        let runtime = Arc::new(CudaExecutionRuntime::new(
            "cuda-1",
            local_artifacts.clone(),
            supervisor,
            engine_trait,
            CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
            device_manager,
        )?);
        let publisher = Arc::new(RemoteArtifactPublisher::new(
            endpoint.clone(),
            local_artifacts.clone(),
            4_096,
            Some(60_000),
        )?);
        let downloader = Arc::new(RemoteArtifactDownloader::new(
            endpoint.clone(),
            local_artifacts,
            8_192,
        )?);
        let worker = OutboundWorker::new(endpoint, hello())?
            .with_cuda_executor(runtime)?
            .with_artifact_downloader(downloader)
            .with_artifact_publisher(publisher);
        Ok(Self {
            _directory: directory,
            service,
            uploads,
            worker,
            engine,
            bundle_digest,
            bundle_size,
            image_manifest,
            shutdown,
            server_task,
        })
    }

    fn assignment(&self) -> Assignment {
        cuda_assignment(self.bundle_digest, self.bundle_size, self.image_manifest)
    }

    async fn shutdown(self) -> Result<(), Box<dyn Error>> {
        let _ = self.shutdown.send(());
        self.server_task.await??;
        Ok(())
    }
}

#[derive(Debug)]
struct ReadyCudaDeviceManager;

impl DeviceStatusProvider for ReadyCudaDeviceManager {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async {
            Ok(DeviceSnapshot {
                devices: vec![ready_cuda_observation()],
            })
        })
    }
}

impl DeviceLifecycleManager for ReadyCudaDeviceManager {
    fn observe_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async { Ok(ready_cuda_observation()) })
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

fn ready_cuda_observation() -> DeviceObservation {
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

fn publish_input_bundle(
    uploads: &SqliteUploadStore,
    artifacts: &FilesystemArtifactStore,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    publish_fixture_bundle(
        uploads,
        artifacts,
        "fixture:cuda-vectoradd-v1",
        CUDA_FIXTURE_BUNDLE_MEDIA_TYPE,
        bytes,
    )
}

fn publish_fixture_bundle(
    uploads: &SqliteUploadStore,
    artifacts: &FilesystemArtifactStore,
    upload_key: &str,
    media_type: &str,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let digest = Sha256Digest::digest_bytes(bytes);
    let session = uploads.begin(&BeginUpload {
        owner_id: "controller".into(),
        upload_key: upload_key.into(),
        expected_digest: digest,
        expected_size_bytes: u64::try_from(bytes.len())?,
        media_type: media_type.into(),
        now_ms: 1,
        expires_at_ms: 60_001,
    })?;
    uploads.append("controller", &session.upload_id, 0, bytes, 2)?;
    uploads.finalize("controller", &session.upload_id, artifacts, 3)?;
    Ok(())
}

fn assert_terminal_artifacts(
    uploads: &SqliteUploadStore,
    finished: &StoredFinished,
    attempt_id: &str,
    expected_stdout: Option<&[u8]>,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(finished.outcome, alloyport_core::AttemptOutcome::Succeeded);
    if let Some(expected_stdout) = expected_stdout {
        let expected_stdout = Sha256Digest::digest_bytes(expected_stdout);
        assert_eq!(
            finished.stdout.as_ref().map(|artifact| artifact.digest),
            Some(expected_stdout)
        );
    }
    for key in [
        format!("output:{attempt_id}:stdout"),
        format!("output:{attempt_id}:stderr"),
        format!("receipt:{attempt_id}"),
    ] {
        assert!(uploads.completed_upload_by_key("cuda-1", &key)?.is_some());
        assert!(uploads.reference("cuda-1", &key).is_ok());
    }
    Ok(())
}

fn cuda_assignment(bundle: Sha256Digest, bundle_size: u64, image: Sha256Digest) -> Assignment {
    cuda_assignment_for("attempt-1", bundle, bundle_size, image, 1_000)
}

fn cuda_assignment_for(
    attempt_id: &str,
    bundle: Sha256Digest,
    bundle_size: u64,
    image: Sha256Digest,
    timeout_ms: u64,
) -> Assignment {
    Assignment {
        assignment_id: format!("assignment-{attempt_id}"),
        attempt_id: attempt_id.into(),
        attempt_number: 1,
        idempotency_key: VECTOR_ADD_FIXTURE_ID.into(),
        task_id: "task-1".into(),
        candidate_id: "candidate-1".into(),
        execution: Some(ExecutionSpec {
            executor_kind: ExecutorKind::CudaFixture.into(),
            argv: vec![VECTOR_ADD_FIXTURE_ID.into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms,
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

fn ascend_assignment_for(
    attempt_id: &str,
    bundle: Sha256Digest,
    bundle_size: u64,
    image: Sha256Digest,
) -> Assignment {
    Assignment {
        assignment_id: format!("assignment-{attempt_id}"),
        attempt_id: attempt_id.into(),
        attempt_number: 1,
        idempotency_key: ASCEND_ADD_FIXTURE_ID.into(),
        task_id: "task-ascend-1".into(),
        candidate_id: "candidate-ascend-1".into(),
        execution: Some(ExecutionSpec {
            executor_kind: ExecutorKind::AscendFixture.into(),
            argv: vec![ASCEND_ADD_FIXTURE_ID.into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 120_000,
            bundle: Some(ArtifactRef {
                digest: bundle.to_string(),
                size_bytes: bundle_size,
                media_type: ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
            }),
            image: Some(ArtifactRef {
                digest: image.to_string(),
                size_bytes: 0,
                media_type: OCI_IMAGE_CONFIG_MEDIA_TYPE.into(),
            }),
            limits: Some(ResourceLimits {
                cpu_millis: 4_000,
                memory_bytes: 8 * 1024 * 1024 * 1024,
                disk_bytes: 1024 * 1024 * 1024,
                process_count: 128,
                output_bytes: 1024 * 1024,
                device_count: 1,
                network: NetworkPolicy::Disabled.into(),
            }),
        }),
        required_features: vec![ASCEND_FIXTURE_FEATURE.into()],
    }
}

fn read_artifact(
    artifacts: &FilesystemArtifactStore,
    artifact: Option<&alloyport_worker::journal::StoredArtifact>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let artifact = artifact.ok_or("terminal Artifact is missing")?;
    let digest = artifact.digest;
    let mut bytes = Vec::new();
    artifacts.open(digest)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("ignored accelerator smoke requires {name}").into())
}

fn assert_live_stdout(
    service: &WorkerControlService,
    expected: &[u8],
) -> Result<usize, Box<dyn Error>> {
    let output = service
        .interaction_events("task-1")?
        .into_iter()
        .filter_map(|envelope| match envelope.event {
            Event::CommandOutput {
                stream: EventOutputStream::Stdout,
                byte_offset,
                text,
                ..
            } => Some((byte_offset, text)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut next_offset = 0;
    let mut bytes = Vec::new();
    for (offset, text) in &output {
        assert_eq!(
            *offset, next_offset,
            "live stdout offsets must be contiguous"
        );
        next_offset = next_offset.saturating_add(u64::try_from(text.len())?);
        bytes.extend_from_slice(text.as_bytes());
    }
    assert_eq!(bytes, expected);
    Ok(output.len())
}

fn hello() -> WorkerHello {
    WorkerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        worker_id: "cuda-1".into(),
        instance_id: "cuda-1-test-process".into(),
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

fn ascend_hello(
    device: alloyport_core::AcceleratorDevice,
    driver_version: String,
    cann_version: String,
) -> WorkerHello {
    WorkerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        worker_id: "ascend-1".into(),
        instance_id: "ascend-1-test-process".into(),
        worker_version: "test".into(),
        features: Vec::new(),
        capabilities: Some(WorkerCapabilities {
            backend: Backend::Ascend.into(),
            architecture: device.product_name.clone(),
            device_count: 1,
            max_concurrency: 1,
            driver_version,
            toolkit_version: cann_version,
            container_runtime: "docker".into(),
            devices: vec![WireDevice {
                device_id: device.device_id,
                product_name: device.product_name,
                serial_number: device.serial_number,
                firmware_version: device.firmware_version,
            }],
        }),
        active_attempts: Vec::new(),
    }
}

fn discover_ascend_nodes(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut nodes = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let is_device = name == "davinci_manager"
            || name == "hisi_hdc"
            || name.strip_prefix("davinci").is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
            });
        if !is_device {
            continue;
        }
        if !entry.file_type()?.is_char_device() {
            return Err(format!(
                "Ascend path {} is not a character device",
                entry.path().display()
            )
            .into());
        }
        nodes.push(entry.path());
    }
    nodes.sort();
    Ok(nodes)
}

const fn ceilings() -> CudaResourceCeilings {
    CudaResourceCeilings {
        cpu_millis: 2_000,
        memory_bytes: 2 * 1024 * 1024 * 1024,
        disk_bytes: 512 * 1024 * 1024,
        process_count: 64,
        output_bytes: 64 * 1024,
    }
}

#[derive(Debug)]
struct ImmediateCudaEngine {
    image_id: String,
    state: Mutex<ImmediateCudaState>,
}

#[derive(Debug, Default)]
struct ImmediateCudaState {
    container: Option<ContainerSnapshot>,
    removes: usize,
}

impl ImmediateCudaEngine {
    fn new(image_id: String) -> Self {
        Self {
            image_id,
            state: Mutex::new(ImmediateCudaState::default()),
        }
    }

    fn remove_count(&self) -> usize {
        self.state.lock().expect("engine lock").removes
    }

    fn has_container(&self) -> bool {
        self.state.lock().expect("engine lock").container.is_some()
    }
}

impl CudaContainerEngine for ImmediateCudaEngine {
    fn resolve_image_id<'a>(
        &'a self,
        _plan: &'a alloyport_worker::cuda::DockerCreatePlan,
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
        _plan: &'a alloyport_worker::cuda::DockerCreatePlan,
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
            tokio::task::yield_now().await;
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
                stdout: CUDA_STDOUT.to_vec(),
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
                bytes: CUDA_STDOUT[..5].to_vec(),
            });
            observer(ContainerLogChunk {
                stream: ContainerLogStream::Stdout,
                byte_offset: 5,
                bytes: CUDA_STDOUT[5..].to_vec(),
            });
            Ok(ContainerLogs {
                stdout: CUDA_STDOUT.to_vec(),
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
            let mut state = self.state.lock().map_err(|_| "engine lock")?;
            state.removes += 1;
            if state.removes == 1 {
                return Err("simulated first cleanup failure".into());
            }
            state.container = None;
            Ok(())
        })
    }
}

#[derive(Debug)]
struct FixedArtifactOwner {
    owner_id: String,
}

impl FixedArtifactOwner {
    fn new(owner_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
        }
    }
}

#[tonic::async_trait]
impl ArtifactAccessPolicy for FixedArtifactOwner {
    async fn resolve_owner(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
        _extensions: &Extensions,
    ) -> Result<String, Status> {
        Ok(self.owner_id.clone())
    }

    async fn authorize_download(
        &self,
        _owner_id: &str,
        _digest: Sha256Digest,
    ) -> Result<(), Status> {
        Ok(())
    }
}

async fn wait_until<F, Fut>(mut condition: F) -> Result<(), Box<dyn Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(Into::into)
}
