use crate::artifact_download::RemoteArtifactDownloader;
use crate::artifact_upload::RemoteArtifactPublisher;
use crate::ascend_runtime::AscendExecutionRuntime;
use crate::ascend_smi::{AscendDeviceManager, NpuSmi};
use crate::ascend_supervisor::{AscendContainerEngine, AscendContainerSupervisor};
use crate::backend_error::BackendError;
use crate::cuda_runtime::{CudaEnvironmentFacts, CudaExecutionRuntime};
use crate::cuda_supervisor::{CudaContainerEngine, CudaContainerSupervisor};
use crate::device::{
    BoundDeviceStatusProvider, DeviceLifecycleManager, DeviceStatusProvider,
    bind_configured_device, bind_worker_device,
};
use crate::docker_cli::DockerCliEngine;
use crate::executor::ExecutionRuntimeError;
use crate::nvidia_smi::{CudaDeviceManager, NvidiaSmi};
use crate::{OutboundWorker, WorkerError};
use alloyport_artifacts::FilesystemArtifactStore;
use alloyport_core::ExecutionKind;
use alloyport_proto::v1::WorkerHello;
use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tonic::transport::Endpoint;

use super::ascend_candidate_config::AscendCandidateWorkerConfig;
use super::backend_config::{AscendWorkerConfig, CudaWorkerConfig};
use super::build_config::AscendBuildWorkerConfig;
use super::config::{BackendPolicy, LoadedWorkerConfig};
use super::correctness_config::{AscendCorrectnessWorkerConfig, CudaCorrectnessWorkerConfig};

pub(super) async fn assemble(loaded: LoadedWorkerConfig) -> Result<OutboundWorker, Box<dyn Error>> {
    let endpoint = loaded.endpoint;
    let hello = loaded.hello;
    let worker = OutboundWorker::open_sqlite(endpoint.clone(), hello.clone(), loaded.journal)?;
    match loaded.backend {
        BackendPolicy::Cuda(config) => attach_cuda(worker, endpoint, &hello, config).await,
        BackendPolicy::Ascend(config) => attach_ascend(worker, endpoint, &hello, config).await,
        BackendPolicy::AscendBuild(config) => {
            attach_ascend_build(worker, endpoint, &hello, config).await
        }
        BackendPolicy::CudaCorrectness(config) => {
            attach_cuda_correctness(worker, endpoint, &hello, config).await
        }
        BackendPolicy::AscendCorrectness(config) => {
            attach_ascend_correctness(worker, endpoint, &hello, config).await
        }
        BackendPolicy::AscendCandidate(config) => {
            attach_ascend_candidate(worker, endpoint, &hello, config).await
        }
    }
}

async fn attach_cuda(
    worker: OutboundWorker,
    endpoint: Endpoint,
    hello: &WorkerHello,
    config: CudaWorkerConfig,
) -> Result<OutboundWorker, Box<dyn Error>> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or("CUDA worker capabilities are missing")?;
    if capabilities.device_count != 1 || capabilities.max_concurrency != 1 {
        return Err("the fixed CUDA worker requires device_count=1 and max_concurrency=1".into());
    }
    if capabilities.container_runtime != "docker" {
        return Err("the fixed CUDA worker requires container_runtime=docker".into());
    }

    let manager = Arc::new(NvidiaSmi::new(
        &config.nvidia_smi_binary,
        config.probe_timeout()?,
    )?);
    let inventory = manager.inventory().await?;
    let snapshot = manager.snapshot().await?;
    let selected = bind_worker_device(
        &inventory,
        &snapshot,
        &worker.state().active_device_leases()?,
        &config.device_selection.policy()?,
    )?;
    let selected_identity = selected.identity.clone();
    let status_provider = Arc::new(BoundDeviceStatusProvider::new(
        manager.clone(),
        &selected.identity.device_id,
    )?);
    let policy = Arc::new(config.policy_for(&selected.identity.device_id)?);
    let engine: Arc<dyn CudaContainerEngine> = Arc::new(
        DockerCliEngine::new(config.docker_binary)?
            .with_stop_timeout_seconds(config.docker_stop_timeout_seconds),
    );
    let environment = CudaEnvironmentFacts::new(
        &capabilities.architecture,
        &capabilities.driver_version,
        &capabilities.toolkit_version,
    )?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        &config.local_artifact_root,
        config.local_artifact_max_bytes,
    )?);
    let supervisor = Arc::new(CudaContainerSupervisor::new(policy, artifacts.clone()));
    let device_manager: Arc<dyn DeviceLifecycleManager> = manager.clone();
    let runtime = Arc::new(CudaExecutionRuntime::new(
        &hello.worker_id,
        artifacts.clone(),
        supervisor,
        engine,
        environment,
        device_manager,
    )?);
    let downloader = Arc::new(RemoteArtifactDownloader::new(
        endpoint.clone(),
        artifacts.clone(),
        config.max_input_bytes,
    )?);
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint,
        artifacts,
        config.upload_chunk_bytes,
        Some(config.upload_ttl_ms),
    )?);

    worker
        .with_bound_device(selected_identity)?
        .with_cuda_executor(runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(status_provider)
        })
}

async fn attach_ascend(
    worker: OutboundWorker,
    endpoint: Endpoint,
    hello: &WorkerHello,
    config: AscendWorkerConfig,
) -> Result<OutboundWorker, Box<dyn Error>> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or("Ascend worker capabilities are missing")?;
    if capabilities.device_count != 1 || capabilities.max_concurrency != 1 {
        return Err("the fixed Ascend worker requires device_count=1 and max_concurrency=1".into());
    }
    if capabilities.container_runtime != "docker" {
        return Err("the fixed Ascend worker requires container_runtime=docker".into());
    }
    let expected_environment = config.environment()?;
    if capabilities.architecture != expected_environment.architecture
        || capabilities.driver_version != expected_environment.driver_version
        || capabilities.toolkit_version != expected_environment.cann_version
    {
        return Err("Ascend config environment does not match worker capability facts".into());
    }
    let configured_device = config.device();
    let discovered_nodes = discover_ascend_device_nodes(Path::new("/dev"))?;
    require_selected_ascend_device_nodes(
        &configured_device.device_id,
        &config.device_nodes,
        &discovered_nodes,
    )?;

    let manager = Arc::new(NpuSmi::new(
        &config.npu_smi_binary,
        &config.environment.firmware_version,
        config.probe_timeout()?,
    )?);
    let inventory = manager.inventory().await?;
    let snapshot = manager.snapshot().await?;
    let selected = bind_configured_device(
        &inventory,
        &snapshot,
        &worker.state().active_device_leases()?,
        &configured_device,
    )?;
    if selected.identity != configured_device {
        return Err(
            "selected npu-smi device identity does not match the configured Ascend identity".into(),
        );
    }
    let status_provider = Arc::new(BoundDeviceStatusProvider::new(
        manager.clone(),
        &selected.identity.device_id,
    )?);

    let policy = Arc::new(config.policy()?);
    let docker = Arc::new(
        DockerCliEngine::new(&config.docker_binary)?
            .with_stop_timeout_seconds(config.docker_stop_timeout_seconds),
    );
    let engine: Arc<dyn AscendContainerEngine> = docker;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        &config.local_artifact_root,
        config.local_artifact_max_bytes,
    )?);
    let supervisor = Arc::new(AscendContainerSupervisor::new(policy, artifacts.clone()));
    let device_manager: Arc<dyn AscendDeviceManager> = manager.clone();
    let runtime = Arc::new(AscendExecutionRuntime::new(
        &hello.worker_id,
        artifacts.clone(),
        supervisor,
        engine,
        device_manager,
    )?);
    let downloader = Arc::new(RemoteArtifactDownloader::new(
        endpoint.clone(),
        artifacts.clone(),
        config.max_input_bytes,
    )?);
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint,
        artifacts,
        config.upload_chunk_bytes,
        Some(config.upload_ttl_ms),
    )?);

    worker
        .with_bound_device(selected.identity)?
        .with_ascend_executor(runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(status_provider)
        })
}

async fn attach_cuda_correctness(
    worker: OutboundWorker,
    endpoint: Endpoint,
    hello: &WorkerHello,
    config: CudaCorrectnessWorkerConfig,
) -> Result<OutboundWorker, Box<dyn Error>> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or("CUDA correctness worker capabilities are missing")?;
    if capabilities.max_concurrency != 1 || capabilities.container_runtime != "docker" {
        return Err("the CUDA correctness worker requires concurrency one and Docker".into());
    }
    let manager = Arc::new(NvidiaSmi::new(
        &config.nvidia_smi_binary,
        config.probe_timeout()?,
    )?);
    let inventory = manager.inventory().await?;
    let selection = config.device_selection.policy()?;
    let status_provider: Arc<dyn DeviceStatusProvider> = manager.clone();
    let environment = CudaEnvironmentFacts::new(
        &capabilities.architecture,
        &capabilities.driver_version,
        &capabilities.toolkit_version,
    )?;
    let engine: Arc<dyn CudaContainerEngine> = Arc::new(
        DockerCliEngine::new(&config.docker_binary)?
            .with_stop_timeout_seconds(config.docker_stop_timeout_seconds),
    );
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        &config.local_artifact_root,
        config.local_artifact_max_bytes,
    )?);
    let config = Arc::new(config);
    let factory_config = Arc::clone(&config);
    let factory_artifacts = artifacts.clone();
    let factory_environment = environment.clone();
    let supervisor_factory = Arc::new(move |device_id: &str| {
        let policy = factory_config
            .policy_for(device_id, &factory_environment)
            .map_err(dynamic_runtime_configuration)?;
        Ok(Arc::new(CudaContainerSupervisor::new_correctness(
            Arc::new(policy),
            factory_artifacts.clone(),
        )))
    });
    let device_manager: Arc<dyn DeviceLifecycleManager> = manager.clone();
    let runtime = Arc::new(CudaExecutionRuntime::new_dynamic(
        &hello.worker_id,
        artifacts.clone(),
        engine,
        environment,
        device_manager,
        inventory.clone(),
        selection,
        ExecutionKind::CudaCorrectness,
        supervisor_factory,
    )?);
    let downloader = Arc::new(RemoteArtifactDownloader::new(
        endpoint.clone(),
        artifacts.clone(),
        config.max_input_bytes,
    )?);
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint,
        artifacts,
        config.upload_chunk_bytes,
        Some(config.upload_ttl_ms),
    )?);
    worker
        .with_discovered_devices(inventory)?
        .with_cuda_executor(runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(status_provider)
        })
}

async fn attach_ascend_build(
    worker: OutboundWorker,
    endpoint: Endpoint,
    hello: &WorkerHello,
    config: AscendBuildWorkerConfig,
) -> Result<OutboundWorker, Box<dyn Error>> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or("Ascend build worker capabilities are missing")?;
    if capabilities.device_count != 1
        || capabilities.max_concurrency != 1
        || capabilities.container_runtime != "docker"
    {
        return Err(
            "the Ascend build worker requires one device, concurrency one, and Docker".into(),
        );
    }
    let expected_environment = config.environment()?;
    if capabilities.architecture != expected_environment.architecture
        || capabilities.driver_version != expected_environment.driver_version
        || capabilities.toolkit_version != expected_environment.cann_version
    {
        return Err("Ascend build environment does not match worker capabilities".into());
    }
    let configured_device = config.device();
    let discovered_nodes = discover_ascend_device_nodes(Path::new("/dev"))?;
    require_selected_ascend_device_nodes(
        &configured_device.device_id,
        &config.device_nodes,
        &discovered_nodes,
    )?;
    let manager = Arc::new(NpuSmi::new(
        &config.npu_smi_binary,
        &config.environment.firmware_version,
        config.probe_timeout()?,
    )?);
    let inventory = manager.inventory().await?;
    let snapshot = manager.snapshot().await?;
    let selected = bind_configured_device(
        &inventory,
        &snapshot,
        &worker.state().active_device_leases()?,
        &configured_device,
    )?;
    if selected.identity != configured_device {
        return Err("selected NPU identity does not match Ascend build config".into());
    }
    let status_provider = Arc::new(BoundDeviceStatusProvider::new(
        manager.clone(),
        &selected.identity.device_id,
    )?);
    let policy = Arc::new(config.policy()?);
    let engine: Arc<dyn AscendContainerEngine> = Arc::new(
        DockerCliEngine::new(&config.docker_binary)?
            .with_stop_timeout_seconds(config.docker_stop_timeout_seconds),
    );
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        &config.local_artifact_root,
        config.local_artifact_max_bytes,
    )?);
    let supervisor = Arc::new(AscendContainerSupervisor::new_build(
        policy,
        artifacts.clone(),
    ));
    let device_manager: Arc<dyn AscendDeviceManager> = manager.clone();
    let runtime = Arc::new(AscendExecutionRuntime::new(
        &hello.worker_id,
        artifacts.clone(),
        supervisor,
        engine,
        device_manager,
    )?);
    let downloader = Arc::new(RemoteArtifactDownloader::new(
        endpoint.clone(),
        artifacts.clone(),
        config.max_input_bytes,
    )?);
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint,
        artifacts,
        config.upload_chunk_bytes,
        Some(config.upload_ttl_ms),
    )?);
    worker
        .with_bound_device(selected.identity)?
        .with_ascend_executor(runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(status_provider)
        })
}

async fn attach_ascend_correctness(
    worker: OutboundWorker,
    endpoint: Endpoint,
    hello: &WorkerHello,
    config: AscendCorrectnessWorkerConfig,
) -> Result<OutboundWorker, Box<dyn Error>> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or("Ascend correctness worker capabilities are missing")?;
    if capabilities.device_count != 1
        || capabilities.max_concurrency != 1
        || capabilities.container_runtime != "docker"
    {
        return Err(
            "the Ascend correctness worker requires one device, concurrency one, and Docker".into(),
        );
    }
    let expected_environment = config.environment()?;
    if capabilities.architecture != expected_environment.architecture
        || capabilities.driver_version != expected_environment.driver_version
        || capabilities.toolkit_version != expected_environment.cann_version
    {
        return Err("Ascend correctness environment does not match worker capabilities".into());
    }
    let configured_device = config.device();
    let discovered_nodes = discover_ascend_device_nodes(Path::new("/dev"))?;
    require_selected_ascend_device_nodes(
        &configured_device.device_id,
        &config.device_nodes,
        &discovered_nodes,
    )?;
    let manager = Arc::new(NpuSmi::new(
        &config.npu_smi_binary,
        &config.environment.firmware_version,
        config.probe_timeout()?,
    )?);
    let inventory = manager.inventory().await?;
    let snapshot = manager.snapshot().await?;
    let selected = bind_configured_device(
        &inventory,
        &snapshot,
        &worker.state().active_device_leases()?,
        &configured_device,
    )?;
    if selected.identity != configured_device {
        return Err("selected NPU identity does not match Ascend correctness config".into());
    }
    let status_provider = Arc::new(BoundDeviceStatusProvider::new(
        manager.clone(),
        &selected.identity.device_id,
    )?);
    let policy = Arc::new(config.policy()?);
    let engine: Arc<dyn AscendContainerEngine> = Arc::new(
        DockerCliEngine::new(&config.docker_binary)?
            .with_stop_timeout_seconds(config.docker_stop_timeout_seconds),
    );
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        &config.local_artifact_root,
        config.local_artifact_max_bytes,
    )?);
    let supervisor = Arc::new(AscendContainerSupervisor::new_correctness(
        policy,
        artifacts.clone(),
    )?);
    let device_manager: Arc<dyn AscendDeviceManager> = manager.clone();
    let runtime = Arc::new(AscendExecutionRuntime::new(
        &hello.worker_id,
        artifacts.clone(),
        supervisor,
        engine,
        device_manager,
    )?);
    let downloader = Arc::new(RemoteArtifactDownloader::new(
        endpoint.clone(),
        artifacts.clone(),
        config.max_input_bytes,
    )?);
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint,
        artifacts,
        config.upload_chunk_bytes,
        Some(config.upload_ttl_ms),
    )?);
    worker
        .with_bound_device(selected.identity)?
        .with_ascend_executor(runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(status_provider)
        })
}

#[allow(clippy::too_many_lines)]
async fn attach_ascend_candidate(
    worker: OutboundWorker,
    endpoint: Endpoint,
    hello: &WorkerHello,
    config: AscendCandidateWorkerConfig,
) -> Result<OutboundWorker, Box<dyn Error>> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or("Ascend candidate worker capabilities are missing")?;
    if capabilities.max_concurrency != 1 || capabilities.container_runtime != "docker" {
        return Err("the Ascend candidate worker requires concurrency one and Docker".into());
    }
    let expected_environment = config.environment()?;
    if capabilities.architecture != expected_environment.architecture
        || capabilities.driver_version != expected_environment.driver_version
        || capabilities.toolkit_version != expected_environment.cann_version
    {
        return Err("Ascend candidate environment does not match worker capabilities".into());
    }
    let discovered_nodes = discover_ascend_device_nodes(Path::new("/dev"))?;
    let manager = Arc::new(NpuSmi::new(
        &config.npu_smi_binary,
        &config.environment.firmware_version,
        config.probe_timeout()?,
    )?);
    let inventory = manager.inventory().await?;
    let selection = config.selection_policy()?;
    let status_provider: Arc<dyn DeviceStatusProvider> = manager.clone();
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        &config.local_artifact_root,
        config.local_artifact_max_bytes,
    )?);
    let docker = Arc::new(
        DockerCliEngine::new(&config.docker_binary)?
            .with_stop_timeout_seconds(config.docker_stop_timeout_seconds),
    );
    let config = Arc::new(config);
    let discovered_nodes = Arc::new(discovered_nodes.into_iter().collect::<BTreeSet<_>>());
    let build_config = Arc::clone(&config);
    let build_artifacts = artifacts.clone();
    let build_nodes = Arc::clone(&discovered_nodes);
    let build_factory = Arc::new(move |device: &alloyport_core::AcceleratorDevice| {
        let nodes = selected_ascend_device_nodes(&device.device_id, &build_nodes)
            .map_err(dynamic_runtime_configuration)?;
        let policy = build_config
            .build_policy_for(device.clone(), nodes)
            .map_err(dynamic_runtime_configuration)?;
        Ok(Arc::new(AscendContainerSupervisor::new_build(
            Arc::new(policy),
            build_artifacts.clone(),
        )))
    });
    let correctness_config = Arc::clone(&config);
    let correctness_artifacts = artifacts.clone();
    let correctness_nodes = Arc::clone(&discovered_nodes);
    let correctness_factory = Arc::new(move |device: &alloyport_core::AcceleratorDevice| {
        let nodes = selected_ascend_device_nodes(&device.device_id, &correctness_nodes)
            .map_err(dynamic_runtime_configuration)?;
        let policy = correctness_config
            .correctness_policy_for(device.clone(), nodes)
            .map_err(dynamic_runtime_configuration)?;
        let supervisor = AscendContainerSupervisor::new_correctness(
            Arc::new(policy),
            correctness_artifacts.clone(),
        )
        .map_err(dynamic_runtime_configuration)?;
        Ok(Arc::new(supervisor))
    });
    let build_engine: Arc<dyn AscendContainerEngine> = docker.clone();
    let correctness_engine: Arc<dyn AscendContainerEngine> = docker;
    let build_manager: Arc<dyn AscendDeviceManager> = manager.clone();
    let correctness_manager: Arc<dyn AscendDeviceManager> = manager;
    let environment = config.environment()?;
    let build_runtime = Arc::new(AscendExecutionRuntime::new_dynamic(
        &hello.worker_id,
        artifacts.clone(),
        build_engine,
        environment.clone(),
        build_manager,
        inventory.clone(),
        selection.clone(),
        ExecutionKind::AscendBuild,
        build_factory,
    )?);
    let correctness_runtime = Arc::new(AscendExecutionRuntime::new_dynamic(
        &hello.worker_id,
        artifacts.clone(),
        correctness_engine,
        environment,
        correctness_manager,
        inventory.clone(),
        selection,
        ExecutionKind::AscendCorrectness,
        correctness_factory,
    )?);
    let downloader = Arc::new(RemoteArtifactDownloader::new(
        endpoint.clone(),
        artifacts.clone(),
        config.max_input_bytes,
    )?);
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint,
        artifacts,
        config.upload_chunk_bytes,
        Some(config.upload_ttl_ms),
    )?);

    worker
        .with_discovered_devices(inventory)?
        .with_shared_ascend_executor(build_runtime)?
        .with_shared_ascend_executor(correctness_runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
                .with_device_status_provider(status_provider)
        })
}

fn discover_ascend_device_nodes(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut nodes = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_ascend_device_node_name(name) {
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

fn is_ascend_device_node_name(name: &str) -> bool {
    if name == "davinci_manager" || name == "hisi_hdc" {
        return true;
    }
    name.strip_prefix("davinci").is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn require_selected_ascend_device_nodes(
    device_id: &str,
    configured: &[PathBuf],
    discovered: &[PathBuf],
) -> Result<(), Box<dyn Error>> {
    let configured = configured.iter().cloned().collect::<BTreeSet<_>>();
    let discovered = discovered.iter().cloned().collect::<BTreeSet<_>>();
    let required = [
        PathBuf::from(format!("/dev/davinci{device_id}")),
        PathBuf::from("/dev/davinci_manager"),
        PathBuf::from("/dev/hisi_hdc"),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if configured != required {
        let missing = required
            .difference(&configured)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let extra = configured
            .difference(&required)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "Ascend device-node policy must expose only selected device {device_id}; missing [{missing}], extra [{extra}]"
        )
        .into());
    }
    let unavailable = required
        .difference(&discovered)
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    if !unavailable.is_empty() {
        return Err(format!(
            "selected Ascend device nodes are unavailable on the host: [{unavailable}]"
        )
        .into());
    }
    Ok(())
}

fn selected_ascend_device_nodes(
    device_id: &str,
    discovered: &BTreeSet<PathBuf>,
) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let required = vec![
        PathBuf::from(format!("/dev/davinci{device_id}")),
        PathBuf::from("/dev/davinci_manager"),
        PathBuf::from("/dev/hisi_hdc"),
    ];
    if let Some(missing) = required.iter().find(|node| !discovered.contains(*node)) {
        return Err(format!(
            "selected Ascend device {device_id} is missing required node {}",
            missing.display()
        )
        .into());
    }
    Ok(required)
}

fn dynamic_runtime_configuration(error: impl std::fmt::Display) -> ExecutionRuntimeError {
    ExecutionRuntimeError::Backend(BackendError::integrity(error.to_string()))
}

#[cfg(test)]
#[path = "assembly_tests.rs"]
mod tests;
