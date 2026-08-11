use alloyport_artifacts::{FilesystemArtifactStore, Sha256Digest};
use alloyport_proto::v1::{Backend, WorkerCapabilities, WorkerHello};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_worker::artifact_download::RemoteArtifactDownloader;
use alloyport_worker::artifact_upload::RemoteArtifactPublisher;
use alloyport_worker::cuda::{CudaFixturePolicy, CudaResourceCeilings, VECTOR_ADD_FIXTURE_ID};
use alloyport_worker::cuda_docker::DockerCliEngine;
use alloyport_worker::cuda_runtime::{CudaEnvironmentFacts, CudaExecutionRuntime};
use alloyport_worker::cuda_supervisor::{CudaContainerEngine, CudaContainerSupervisor};
use alloyport_worker::{OutboundWorker, WorkerError};
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint_uri =
        env::var("ALLOYPORT_SERVER").unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
    let endpoint = endpoint(&endpoint_uri)?;
    let worker_id = env::var("ALLOYPORT_WORKER_ID")?;
    let backend = parse_backend(&env::var("ALLOYPORT_BACKEND")?)?;
    let instance_id = instance_id(&worker_id)?;
    let journal = env::var_os("ALLOYPORT_WORKER_DATABASE")
        .unwrap_or_else(|| "alloyport-worker.sqlite3".into());
    let hello = WorkerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        worker_id: worker_id.clone(),
        instance_id,
        worker_version: env!("CARGO_PKG_VERSION").to_owned(),
        features: Vec::new(),
        capabilities: Some(WorkerCapabilities {
            backend: backend.into(),
            architecture: env::var("ALLOYPORT_ARCH").unwrap_or_default(),
            device_count: parse_u32("ALLOYPORT_DEVICE_COUNT", 1)?,
            max_concurrency: parse_u32("ALLOYPORT_MAX_CONCURRENCY", 1)?,
            driver_version: env::var("ALLOYPORT_DRIVER_VERSION").unwrap_or_default(),
            toolkit_version: env::var("ALLOYPORT_TOOLKIT_VERSION").unwrap_or_default(),
            container_runtime: env::var("ALLOYPORT_CONTAINER_RUNTIME")
                .unwrap_or_else(|_| "docker".to_owned()),
        }),
        active_attempts: Vec::new(),
    };
    let mut worker = OutboundWorker::open_sqlite(endpoint.clone(), hello.clone(), journal)?;
    if let Some(config_path) = env::var_os("ALLOYPORT_CUDA_CONFIG") {
        if backend != Backend::Cuda {
            return Err("ALLOYPORT_CUDA_CONFIG requires ALLOYPORT_BACKEND=cuda".into());
        }
        let config = CudaWorkerConfig::load(&config_path)?;
        worker = attach_cuda(worker, endpoint, &hello, config)?;
    }

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
    image_manifest_digest: String,
    image_reference: String,
    image_id: String,
    device_id: String,
    sandbox_root: PathBuf,
    ceilings: CudaCeilingsConfig,
    local_artifact_root: PathBuf,
    local_artifact_max_bytes: u64,
    max_input_bytes: u64,
    upload_chunk_bytes: usize,
    upload_ttl_ms: u64,
    docker_binary: PathBuf,
    docker_stop_timeout_seconds: u32,
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
    fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();
        let config = Self::parse(&fs::read(path)?)?;
        Ok(config)
    }

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
        if !self.docker_binary.is_absolute() {
            return Err("CUDA Docker CLI path must be absolute".into());
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
        CudaFixturePolicy::new(
            self.fixture_id.as_str(),
            Sha256Digest::from_str(&self.bundle_digest)?,
            Sha256Digest::from_str(&self.image_manifest_digest)?,
            self.image_reference.as_str(),
            Sha256Digest::from_str(&self.image_id)?,
            self.device_id.as_str(),
            &self.sandbox_root,
            self.ceilings(),
        )?;
        Ok(())
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

fn attach_cuda(
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

    let ceilings = config.ceilings();
    let policy = Arc::new(CudaFixturePolicy::new(
        config.fixture_id,
        Sha256Digest::from_str(&config.bundle_digest)?,
        Sha256Digest::from_str(&config.image_manifest_digest)?,
        config.image_reference,
        Sha256Digest::from_str(&config.image_id)?,
        config.device_id,
        config.sandbox_root,
        ceilings,
    )?);
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
    let runtime = Arc::new(CudaExecutionRuntime::new(
        &hello.worker_id,
        artifacts.clone(),
        supervisor,
        engine,
        environment,
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
        .with_cuda_executor(runtime)
        .map_err(|error: WorkerError| -> Box<dyn Error> { Box::new(error) })
        .map(|worker| {
            worker
                .with_artifact_downloader(downloader)
                .with_artifact_publisher(publisher)
        })
}

fn endpoint(uri: &str) -> Result<Endpoint, Box<dyn Error>> {
    let mut endpoint = Endpoint::from_shared(uri.to_owned())?;
    let certificate = env::var_os("ALLOYPORT_TLS_CERT");
    let key = env::var_os("ALLOYPORT_TLS_KEY");
    let server_ca = env::var_os("ALLOYPORT_TLS_SERVER_CA");
    let server_name = env::var("ALLOYPORT_TLS_SERVER_NAME").ok();
    match (certificate, key, server_ca, server_name) {
        (None, None, None, None) if is_loopback_uri(uri) => {}
        (Some(certificate), Some(key), Some(server_ca), Some(server_name)) => {
            let identity = Identity::from_pem(fs::read(certificate)?, fs::read(key)?);
            let server_ca = Certificate::from_pem(fs::read(server_ca)?);
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(server_ca)
                    .domain_name(server_name),
            )?;
        }
        _ => {
            return Err("remote workers require ALLOYPORT_TLS_CERT, ALLOYPORT_TLS_KEY, ALLOYPORT_TLS_SERVER_CA and ALLOYPORT_TLS_SERVER_NAME"
                .into());
        }
    }
    Ok(endpoint)
}

fn is_loopback_uri(uri: &str) -> bool {
    uri.starts_with("http://127.0.0.1:")
        || uri.starts_with("http://localhost:")
        || uri.starts_with("http://[::1]:")
}

fn parse_backend(value: &str) -> Result<Backend, Box<dyn Error>> {
    match value.to_ascii_lowercase().as_str() {
        "cuda" => Ok(Backend::Cuda),
        "ascend" | "npu" => Ok(Backend::Ascend),
        _ => {
            Err(format!("unsupported ALLOYPORT_BACKEND {value:?}; expected cuda or ascend").into())
        }
    }
}

fn parse_u32(name: &str, default: u32) -> Result<u32, Box<dyn Error>> {
    Ok(env::var(name).map_or(Ok(default), |value| value.parse())?)
}

fn instance_id(worker_id: &str) -> Result<String, Box<dyn Error>> {
    let unix_nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{worker_id}-{}-{unix_nanos}", std::process::id()))
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
                "device_id": "0",
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
                "docker_stop_timeout_seconds": 10
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
}
