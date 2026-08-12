use alloyport_artifacts::{FilesystemArtifactStore, Sha256Digest};
use alloyport_core::AcceleratorDevice;
use alloyport_proto::v1::{AcceleratorDevice as WireDevice, WorkerHello};
use alloyport_worker::artifact_download::RemoteArtifactDownloader;
use alloyport_worker::artifact_upload::RemoteArtifactPublisher;
use alloyport_worker::ascend::{
    ASCEND_ADD_FIXTURE_ID, AscendEnvironmentFacts, AscendFixturePolicy, AscendResourceCeilings,
};
use alloyport_worker::ascend_runtime::AscendExecutionRuntime;
use alloyport_worker::ascend_smi::{AscendDeviceManager, NpuSmi};
use alloyport_worker::ascend_supervisor::{AscendContainerEngine, AscendContainerSupervisor};
use alloyport_worker::cuda::{CudaFixturePolicy, CudaResourceCeilings, VECTOR_ADD_FIXTURE_ID};
use alloyport_worker::cuda_runtime::{CudaEnvironmentFacts, CudaExecutionRuntime};
use alloyport_worker::cuda_supervisor::{CudaContainerEngine, CudaContainerSupervisor};
use alloyport_worker::device::{
    BoundDeviceStatusProvider, DeviceLifecycleManager, DeviceSelectionPolicy, DeviceStatusProvider,
    bind_worker_device,
};
use alloyport_worker::docker_cli::DockerCliEngine;
use alloyport_worker::nvidia_smi::{CudaDeviceManager, NvidiaSmi};
use alloyport_worker::{OutboundWorker, WorkerError};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Endpoint;

mod worker_config;
use worker_config::{BackendPolicy, WorkerFileConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let loaded = WorkerFileConfig::load_from_args()?.into_loaded()?;
    let endpoint = loaded.endpoint;
    let hello = loaded.hello;
    let mut worker = OutboundWorker::open_sqlite(endpoint.clone(), hello.clone(), loaded.journal)?;
    worker = match loaded.backend {
        BackendPolicy::Cuda(config) => attach_cuda(worker, endpoint, &hello, config).await?,
        BackendPolicy::Ascend(config) => attach_ascend(worker, endpoint, &hello, config).await?,
    };

    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::select! {
            result = worker.run_session() => {
                if let Err(error) = result {
                    eprintln!("worker session ended: {error}; reconnecting in {backoff:?}");
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CudaWorkerConfig {
    schema_version: u16,
    fixture_id: String,
    bundle_digest: String,
    #[serde(alias = "image_manifest_digest")]
    image_digest: String,
    image_reference: String,
    image_id: String,
    device_selection: DeviceSelectionConfig,
    sandbox_root: PathBuf,
    ceilings: CudaCeilingsConfig,
    local_artifact_root: PathBuf,
    local_artifact_max_bytes: u64,
    max_input_bytes: u64,
    upload_chunk_bytes: usize,
    upload_ttl_ms: u64,
    docker_binary: PathBuf,
    docker_stop_timeout_seconds: u32,
    nvidia_smi_binary: PathBuf,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceSelectionConfig {
    #[serde(default)]
    allowed_device_ids: Vec<String>,
    preferred_device_id: Option<String>,
}

impl DeviceSelectionConfig {
    fn policy(&self) -> Result<DeviceSelectionPolicy, Box<dyn Error>> {
        Ok(DeviceSelectionPolicy::new(
            self.allowed_device_ids.clone(),
            self.preferred_device_id.clone(),
        )?)
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CudaCeilingsConfig {
    cpu_millis: u64,
    memory_bytes: u64,
    disk_bytes: u64,
    process_count: u32,
    output_bytes: u64,
}

impl CudaWorkerConfig {
    #[cfg(test)]
    fn parse(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported CUDA worker config schema {}; expected 1",
                self.schema_version
            )
            .into());
        }
        if self.fixture_id != VECTOR_ADD_FIXTURE_ID {
            return Err(format!(
                "unsupported CUDA fixture {:?}; expected {VECTOR_ADD_FIXTURE_ID}",
                self.fixture_id
            )
            .into());
        }
        if !self.sandbox_root.is_absolute() || !self.local_artifact_root.is_absolute() {
            return Err("CUDA sandbox and local Artifact roots must be absolute".into());
        }
        if !self.docker_binary.is_absolute() || !self.nvidia_smi_binary.is_absolute() {
            return Err("CUDA Docker and nvidia-smi paths must be absolute".into());
        }
        if self.local_artifact_root.starts_with(&self.sandbox_root)
            || self.sandbox_root.starts_with(&self.local_artifact_root)
        {
            return Err("CUDA sandbox and local Artifact roots must not overlap".into());
        }
        if self.local_artifact_max_bytes == 0
            || self.max_input_bytes == 0
            || self.upload_chunk_bytes == 0
            || self.upload_ttl_ms == 0
            || self.docker_stop_timeout_seconds == 0
        {
            return Err("CUDA Artifact and Docker limits must all be nonzero".into());
        }
        if self.max_input_bytes > self.local_artifact_max_bytes {
            return Err("CUDA input limit exceeds the local Artifact object limit".into());
        }
        if self.ceilings.output_bytes > self.local_artifact_max_bytes {
            return Err("CUDA output ceiling exceeds the local Artifact object limit".into());
        }
        self.device_selection.policy()?;
        self.policy_for("validated-device")?;
        Ok(())
    }

    fn policy_for(&self, device_id: &str) -> Result<CudaFixturePolicy, Box<dyn Error>> {
        Ok(CudaFixturePolicy::new(
            self.fixture_id.as_str(),
            Sha256Digest::from_str(&self.bundle_digest)?,
            Sha256Digest::from_str(&self.image_digest)?,
            self.image_reference.as_str(),
            Sha256Digest::from_str(&self.image_id)?,
            device_id,
            &self.sandbox_root,
            self.ceilings(),
        )?)
    }

    const fn ceilings(&self) -> CudaResourceCeilings {
        CudaResourceCeilings {
            cpu_millis: self.ceilings.cpu_millis,
            memory_bytes: self.ceilings.memory_bytes,
            disk_bytes: self.ceilings.disk_bytes,
            process_count: self.ceilings.process_count,
            output_bytes: self.ceilings.output_bytes,
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

    let manager = Arc::new(NvidiaSmi::new(&config.nvidia_smi_binary)?);
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AscendWorkerConfig {
    schema_version: u16,
    fixture_id: String,
    bundle_digest: String,
    #[serde(alias = "image_manifest_digest")]
    image_digest: String,
    image_reference: String,
    image_id: String,
    device: AscendDeviceConfig,
    device_nodes: Vec<PathBuf>,
    driver_path: PathBuf,
    sandbox_root: PathBuf,
    environment: AscendEnvironmentConfig,
    ceilings: AscendCeilingsConfig,
    local_artifact_root: PathBuf,
    local_artifact_max_bytes: u64,
    max_input_bytes: u64,
    upload_chunk_bytes: usize,
    upload_ttl_ms: u64,
    docker_binary: PathBuf,
    docker_stop_timeout_seconds: u32,
    npu_smi_binary: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AscendDeviceConfig {
    device_id: String,
    product_name: String,
    serial_number: String,
    firmware_version: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AscendEnvironmentConfig {
    architecture: String,
    cann_version: String,
    driver_version: String,
    firmware_version: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AscendCeilingsConfig {
    timeout_ms: u64,
    cpu_millis: u64,
    memory_bytes: u64,
    disk_bytes: u64,
    process_count: u32,
    output_bytes: u64,
}

impl AscendWorkerConfig {
    #[cfg(test)]
    fn parse(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported Ascend worker config schema {}; expected 1",
                self.schema_version
            )
            .into());
        }
        if self.fixture_id != ASCEND_ADD_FIXTURE_ID {
            return Err(format!(
                "unsupported Ascend fixture {:?}; expected {ASCEND_ADD_FIXTURE_ID}",
                self.fixture_id
            )
            .into());
        }
        if !self.sandbox_root.is_absolute() || !self.local_artifact_root.is_absolute() {
            return Err("Ascend sandbox and local Artifact roots must be absolute".into());
        }
        if !self.docker_binary.is_absolute() || !self.npu_smi_binary.is_absolute() {
            return Err("Ascend Docker and npu-smi paths must be absolute".into());
        }
        if self.local_artifact_root.starts_with(&self.sandbox_root)
            || self.sandbox_root.starts_with(&self.local_artifact_root)
        {
            return Err("Ascend sandbox and local Artifact roots must not overlap".into());
        }
        if self.local_artifact_max_bytes == 0
            || self.max_input_bytes == 0
            || self.upload_chunk_bytes == 0
            || self.upload_ttl_ms == 0
            || self.docker_stop_timeout_seconds == 0
        {
            return Err("Ascend Artifact and command limits must all be nonzero".into());
        }
        if self.max_input_bytes > self.local_artifact_max_bytes
            || self.ceilings.output_bytes > self.local_artifact_max_bytes
        {
            return Err("Ascend input/output limits exceed the local Artifact object limit".into());
        }
        self.policy()?;
        Ok(())
    }

    fn device(&self) -> AcceleratorDevice {
        AcceleratorDevice {
            device_id: self.device.device_id.clone(),
            product_name: self.device.product_name.clone(),
            serial_number: self.device.serial_number.clone(),
            firmware_version: self.device.firmware_version.clone(),
        }
    }

    fn wire_device(&self) -> WireDevice {
        let device = self.device();
        WireDevice {
            device_id: device.device_id,
            product_name: device.product_name,
            serial_number: device.serial_number,
            firmware_version: device.firmware_version,
        }
    }

    fn environment(&self) -> Result<AscendEnvironmentFacts, Box<dyn Error>> {
        Ok(AscendEnvironmentFacts::new(
            &self.environment.architecture,
            &self.environment.cann_version,
            &self.environment.driver_version,
            &self.environment.firmware_version,
        )?)
    }

    const fn ceilings(&self) -> AscendResourceCeilings {
        AscendResourceCeilings {
            timeout_ms: self.ceilings.timeout_ms,
            cpu_millis: self.ceilings.cpu_millis,
            memory_bytes: self.ceilings.memory_bytes,
            disk_bytes: self.ceilings.disk_bytes,
            process_count: self.ceilings.process_count,
            output_bytes: self.ceilings.output_bytes,
        }
    }

    fn policy(&self) -> Result<AscendFixturePolicy, Box<dyn Error>> {
        Ok(AscendFixturePolicy::new(
            &self.fixture_id,
            Sha256Digest::from_str(&self.bundle_digest)?,
            Sha256Digest::from_str(&self.image_digest)?,
            &self.image_reference,
            Sha256Digest::from_str(&self.image_id)?,
            self.device(),
            self.device_nodes.clone(),
            &self.driver_path,
            &self.sandbox_root,
            self.ceilings(),
            self.environment()?,
        )?)
    }
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
    let discovered_nodes = discover_ascend_device_nodes(Path::new("/dev"))?;
    require_exact_ascend_device_nodes(&config.device_nodes, &discovered_nodes)?;

    let manager = Arc::new(NpuSmi::new(
        &config.npu_smi_binary,
        &config.environment.firmware_version,
    )?);
    let inventory = manager.inventory().await?;
    let configured_device = config.device();
    let snapshot = manager.snapshot().await?;
    let selected = bind_worker_device(
        &inventory,
        &snapshot,
        &worker.state().active_device_leases()?,
        &DeviceSelectionPolicy::new(
            vec![configured_device.device_id.clone()],
            Some(configured_device.device_id.clone()),
        )?,
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

fn require_exact_ascend_device_nodes(
    configured: &[PathBuf],
    discovered: &[PathBuf],
) -> Result<(), Box<dyn Error>> {
    let configured = configured.iter().cloned().collect::<BTreeSet<_>>();
    let discovered = discovered.iter().cloned().collect::<BTreeSet<_>>();
    if configured != discovered {
        let missing = discovered
            .difference(&configured)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let extra = configured
            .difference(&discovered)
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "Ascend device-node policy does not exactly match the host; missing [{missing}], extra [{extra}]"
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cuda_config_is_complete_pinned_and_rejects_unknown_fields() -> Result<(), Box<dyn Error>> {
        let manifest = Sha256Digest::digest_bytes(b"manifest");
        let image_id = Sha256Digest::digest_bytes(b"image");
        let bundle = Sha256Digest::digest_bytes(b"bundle");
        let config = format!(
            r#"{{
                "schema_version": 1,
                "fixture_id": "cuda-vectoradd-v1",
                "bundle_digest": "{bundle}",
                "image_manifest_digest": "{manifest}",
                "image_reference": "example.invalid/cuda@{manifest}",
                "image_id": "{image_id}",
                "device_selection": {{
                    "allowed_device_ids": ["0"],
                    "preferred_device_id": "0"
                }},
                "sandbox_root": "/var/lib/alloyport/cuda-sandboxes",
                "ceilings": {{
                    "cpu_millis": 2000,
                    "memory_bytes": 2147483648,
                    "disk_bytes": 536870912,
                    "process_count": 64,
                    "output_bytes": 65536
                }},
                "local_artifact_root": "/var/lib/alloyport/cuda-cas",
                "local_artifact_max_bytes": 8388608,
                "max_input_bytes": 8388608,
                "upload_chunk_bytes": 1048576,
                "upload_ttl_ms": 3600000,
                "docker_binary": "/usr/bin/docker",
                "docker_stop_timeout_seconds": 10,
                "nvidia_smi_binary": "/usr/bin/nvidia-smi"
            }}"#
        );

        let parsed = CudaWorkerConfig::parse(config.as_bytes())?;
        assert_eq!(parsed.fixture_id, VECTOR_ADD_FIXTURE_ID);

        let unknown = config.replacen(
            "\"schema_version\": 1,",
            "\"schema_version\": 1, \"allow_shell\": true,",
            1,
        );
        assert!(CudaWorkerConfig::parse(unknown.as_bytes()).is_err());

        let partial = config.replacen("\"upload_ttl_ms\": 3600000,", "", 1);
        assert!(CudaWorkerConfig::parse(partial.as_bytes()).is_err());

        let unpinned = config.replace(
            &format!("example.invalid/cuda@{manifest}"),
            "example.invalid/cuda:latest",
        );
        assert!(CudaWorkerConfig::parse(unpinned.as_bytes()).is_err());

        let overlapping = config.replace(
            "/var/lib/alloyport/cuda-cas",
            "/var/lib/alloyport/cuda-sandboxes/cas",
        );
        assert!(CudaWorkerConfig::parse(overlapping.as_bytes()).is_err());
        Ok(())
    }

    #[test]
    fn ascend_config_is_complete_pinned_and_default_deny() -> Result<(), Box<dyn Error>> {
        let manifest = Sha256Digest::digest_bytes(b"manifest");
        let image_id = Sha256Digest::digest_bytes(b"image");
        let bundle = Sha256Digest::digest_bytes(b"bundle");
        let config = format!(
            r#"{{
                "schema_version": 1,
                "fixture_id": "ascend-add-v1",
                "bundle_digest": "{bundle}",
                "image_manifest_digest": "{manifest}",
                "image_reference": "example.invalid/ascend@{manifest}",
                "image_id": "{image_id}",
                "device": {{
                    "device_id": "3",
                    "product_name": "Ascend950PR",
                    "serial_number": "serial-3",
                    "firmware_version": "9.0.0.105.229"
                }},
                "device_nodes": [
                    "/dev/davinci3", "/dev/davinci_manager", "/dev/hisi_hdc"
                ],
                "driver_path": "/usr/local/Ascend/driver",
                "sandbox_root": "/var/lib/alloyport/ascend-sandboxes",
                "environment": {{
                    "architecture": "Ascend950PR",
                    "cann_version": "9.1.0-beta.1",
                    "driver_version": "25.7.rc1.6",
                    "firmware_version": "9.0.0.105.229"
                }},
                "ceilings": {{
                    "timeout_ms": 60000,
                    "cpu_millis": 4000,
                    "memory_bytes": 8589934592,
                    "disk_bytes": 1073741824,
                    "process_count": 128,
                    "output_bytes": 1048576
                }},
                "local_artifact_root": "/var/lib/alloyport/ascend-cas",
                "local_artifact_max_bytes": 16777216,
                "max_input_bytes": 16777216,
                "upload_chunk_bytes": 1048576,
                "upload_ttl_ms": 3600000,
                "docker_binary": "/usr/bin/docker",
                "docker_stop_timeout_seconds": 10,
                "npu_smi_binary": "/usr/local/bin/npu-smi"
            }}"#
        );

        let parsed = AscendWorkerConfig::parse(config.as_bytes())?;
        assert_eq!(parsed.fixture_id, ASCEND_ADD_FIXTURE_ID);
        assert_eq!(parsed.wire_device().device_id, "3");

        let unknown = config.replacen(
            "\"schema_version\": 1,",
            "\"schema_version\": 1, \"allow_shell\": true,",
            1,
        );
        assert!(AscendWorkerConfig::parse(unknown.as_bytes()).is_err());
        let mutable_image = config.replace(
            &format!("example.invalid/ascend@{manifest}"),
            "example.invalid/ascend:latest",
        );
        assert!(AscendWorkerConfig::parse(mutable_image.as_bytes()).is_err());
        let relative_probe = config.replace("/usr/local/bin/npu-smi", "npu-smi");
        assert!(AscendWorkerConfig::parse(relative_probe.as_bytes()).is_err());
        let mismatched_firmware = config.replacen(
            "\"firmware_version\": \"9.0.0.105.229\"",
            "\"firmware_version\": \"other\"",
            1,
        );
        assert!(AscendWorkerConfig::parse(mismatched_firmware.as_bytes()).is_err());
        Ok(())
    }

    #[test]
    fn ascend_startup_requires_the_exact_enumerated_host_device_nodes() {
        let discovered = vec![
            PathBuf::from("/dev/davinci0"),
            PathBuf::from("/dev/davinci1"),
            PathBuf::from("/dev/davinci_manager"),
            PathBuf::from("/dev/hisi_hdc"),
        ];
        assert!(require_exact_ascend_device_nodes(&discovered, &discovered).is_ok());
        assert!(
            require_exact_ascend_device_nodes(&discovered[..3], &discovered)
                .expect_err("missing host node must fail")
                .to_string()
                .contains("hisi_hdc")
        );
        assert!(is_ascend_device_node_name("davinci12"));
        assert!(!is_ascend_device_node_name("davinci"));
        assert!(!is_ascend_device_node_name("davinci3.backup"));
    }
}
