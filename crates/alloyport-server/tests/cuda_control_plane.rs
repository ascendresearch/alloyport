use alloyport_artifacts::upload::{BeginUpload, SqliteUploadStore, UploadQuotas};
use alloyport_artifacts::{FilesystemArtifactStore, Sha256Digest};
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, Backend, ExecutionSpec, ExecutorKind, NetworkPolicy, ResourceLimits,
    WorkerCapabilities, WorkerHello,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_server::artifact::{ArtifactAccessPolicy, ArtifactServiceImpl};
use alloyport_server::{AssignmentState, EnqueueOutcome, ManualClock, WorkerControlService};
use alloyport_worker::artifact_download::RemoteArtifactDownloader;
use alloyport_worker::artifact_upload::RemoteArtifactPublisher;
use alloyport_worker::cuda::{
    CUDA_FIXTURE_BUNDLE_MEDIA_TYPE, CUDA_FIXTURE_FEATURE, CudaFixtureBundle, CudaFixturePolicy,
    CudaResourceCeilings, OCI_IMAGE_MANIFEST_MEDIA_TYPE, VECTOR_ADD_FIXTURE_ID,
};
use alloyport_worker::cuda_runtime::{CudaEnvironmentFacts, CudaExecutionRuntime};
use alloyport_worker::cuda_supervisor::{
    ContainerExit, ContainerIdentity, ContainerLogs, ContainerPhase, ContainerSnapshot,
    CudaContainerEngine, CudaContainerSupervisor, EngineFuture,
};
use alloyport_worker::{OutboundWorker, StoredFinished};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Extensions, Status};

const CUDA_STDOUT: &[u8] = b"PASS fixture=cuda-vectoradd-v1 elements=1048576 checksum=670562424\n";

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
        .lock()
        .await
        .finished_attempt("attempt-1")?
        .expect("CUDA terminal state is durable");
    assert_terminal_artifacts(&fixture.uploads, &finished)?;
    assert_eq!(fixture.engine.remove_count(), 2);
    assert!(!fixture.engine.has_container());

    second_worker_task.abort();
    let _ = second_worker_task.await;
    fixture.shutdown().await?;
    Ok(())
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

        let service = WorkerControlService::new().with_artifact_metadata(Arc::clone(&uploads));
        let artifact_service = ArtifactServiceImpl::new(
            Arc::clone(&uploads),
            remote_artifacts,
            Arc::new(FixedArtifactOwner),
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
            Arc::clone(&local_artifacts),
        ));
        let runtime = Arc::new(CudaExecutionRuntime::new(
            "cuda-1",
            Arc::clone(&local_artifacts),
            supervisor,
            engine_trait,
            CudaEnvironmentFacts::new("sm_121", "580.159.03", "13.0")?,
        )?);
        let publisher = Arc::new(RemoteArtifactPublisher::new(
            endpoint.clone(),
            Arc::clone(&local_artifacts),
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

fn publish_input_bundle(
    uploads: &SqliteUploadStore,
    artifacts: &FilesystemArtifactStore,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let digest = Sha256Digest::digest_bytes(bytes);
    let session = uploads.begin(&BeginUpload {
        owner_id: "controller".into(),
        upload_key: "fixture:cuda-vectoradd-v1".into(),
        expected_digest: digest,
        expected_size_bytes: u64::try_from(bytes.len())?,
        media_type: CUDA_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
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
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        finished.outcome,
        i32::from(alloyport_proto::v1::AttemptOutcome::Succeeded)
    );
    let expected_stdout = Sha256Digest::digest_bytes(CUDA_STDOUT).to_string();
    assert_eq!(
        finished
            .stdout
            .as_ref()
            .map(|artifact| artifact.digest.as_str()),
        Some(expected_stdout.as_str())
    );
    for key in [
        "output:attempt-1:stdout",
        "output:attempt-1:stderr",
        "receipt:attempt-1",
    ] {
        assert!(uploads.completed_upload_by_key("cuda-1", key)?.is_some());
        assert!(uploads.reference("cuda-1", key).is_ok());
    }
    Ok(())
}

fn cuda_assignment(bundle: Sha256Digest, bundle_size: u64, image: Sha256Digest) -> Assignment {
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
        }),
        active_attempts: Vec::new(),
    }
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
struct FixedArtifactOwner;

impl ArtifactAccessPolicy for FixedArtifactOwner {
    fn resolve_owner(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
        _extensions: &Extensions,
    ) -> Result<String, Status> {
        Ok("cuda-1".into())
    }

    fn authorize_download(&self, _owner_id: &str, _digest: Sha256Digest) -> Result<(), Status> {
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
