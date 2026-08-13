//! One schema-validated configuration file for an outbound accelerator worker.

use super::backend_config::{AscendWorkerConfig, CudaWorkerConfig};
use super::correctness_config::{AscendCorrectnessWorkerConfig, CudaCorrectnessWorkerConfig};
use alloyport_proto::v1::{Backend, WorkerCapabilities, WorkerHello};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerFileConfig {
    schema_version: u16,
    server: ServerConfig,
    worker: WorkerIdentityConfig,
    runtime: RuntimeConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerConfig {
    endpoint: String,
    tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    server_ca: PathBuf,
    server_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerIdentityConfig {
    id: String,
    journal: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase", deny_unknown_fields)]
enum RuntimeConfig {
    Cuda {
        environment: CudaEnvironmentConfig,
        policy: CudaWorkerConfig,
    },
    Ascend {
        policy: AscendWorkerConfig,
    },
    #[serde(rename = "cuda_correctness")]
    CudaCorrectness {
        environment: CudaEnvironmentConfig,
        policy: CudaCorrectnessWorkerConfig,
    },
    #[serde(rename = "ascend_correctness")]
    AscendCorrectness {
        policy: AscendCorrectnessWorkerConfig,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CudaEnvironmentConfig {
    architecture: String,
    driver_version: String,
    toolkit_version: String,
}

pub(super) enum BackendPolicy {
    Cuda(CudaWorkerConfig),
    Ascend(AscendWorkerConfig),
    CudaCorrectness(CudaCorrectnessWorkerConfig),
    AscendCorrectness(AscendCorrectnessWorkerConfig),
}

pub(super) struct LoadedWorkerConfig {
    pub endpoint: Endpoint,
    pub hello: WorkerHello,
    pub journal: PathBuf,
    pub backend: BackendPolicy,
}

impl WorkerFileConfig {
    pub fn load_from_args() -> Result<Self, Box<dyn Error>> {
        let mut arguments = env::args_os();
        let _program = arguments.next();
        let path = match (arguments.next(), arguments.next(), arguments.next()) {
            (Some(flag), Some(path), None) if flag == "--config" => PathBuf::from(path),
            (Some(path), None, None) => PathBuf::from(path),
            (None, None, None) => env::var_os("ALLOYPORT_WORKER_CONFIG")
                .map(PathBuf::from)
                .ok_or("usage: alloyport-worker --config PATH")?,
            _ => return Err("usage: alloyport-worker --config PATH".into()),
        };
        Self::load(path)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        Self::parse(&fs::read(path)?)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported worker config schema {}; expected 1",
                self.schema_version
            )
            .into());
        }
        if self.worker.id.trim().is_empty() {
            return Err("worker ID must be nonempty".into());
        }
        if self.worker.journal.as_os_str().is_empty() {
            return Err("worker journal path must be nonempty".into());
        }
        match &self.runtime {
            RuntimeConfig::Cuda {
                environment,
                policy,
            } => {
                validate_cuda_environment(environment)?;
                policy.validate()?;
            }
            RuntimeConfig::Ascend { policy } => policy.validate()?,
            RuntimeConfig::CudaCorrectness {
                environment,
                policy,
            } => {
                validate_cuda_environment(environment)?;
                policy.validate()?;
            }
            RuntimeConfig::AscendCorrectness { policy } => policy.validate()?,
        }
        self.server.endpoint()?;
        Ok(())
    }

    pub fn into_loaded(self) -> Result<LoadedWorkerConfig, Box<dyn Error>> {
        let endpoint = self.server.endpoint()?;
        let worker_id = self.worker.id;
        let (capabilities, backend) = match self.runtime {
            RuntimeConfig::Cuda {
                environment,
                policy,
            } => (
                WorkerCapabilities {
                    backend: Backend::Cuda.into(),
                    architecture: environment.architecture,
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: environment.driver_version,
                    toolkit_version: environment.toolkit_version,
                    container_runtime: "docker".into(),
                    devices: Vec::new(),
                },
                BackendPolicy::Cuda(policy),
            ),
            RuntimeConfig::Ascend { policy } => (
                WorkerCapabilities {
                    backend: Backend::Ascend.into(),
                    architecture: policy.environment.architecture.clone(),
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: policy.environment.driver_version.clone(),
                    toolkit_version: policy.environment.cann_version.clone(),
                    container_runtime: "docker".into(),
                    devices: vec![policy.wire_device()],
                },
                BackendPolicy::Ascend(policy),
            ),
            RuntimeConfig::CudaCorrectness {
                environment,
                policy,
            } => (
                WorkerCapabilities {
                    backend: Backend::Cuda.into(),
                    architecture: environment.architecture,
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: environment.driver_version,
                    toolkit_version: environment.toolkit_version,
                    container_runtime: "docker".into(),
                    devices: Vec::new(),
                },
                BackendPolicy::CudaCorrectness(policy),
            ),
            RuntimeConfig::AscendCorrectness { policy } => (
                WorkerCapabilities {
                    backend: Backend::Ascend.into(),
                    architecture: policy.environment.architecture.clone(),
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: policy.environment.driver_version.clone(),
                    toolkit_version: policy.environment.cann_version.clone(),
                    container_runtime: "docker".into(),
                    devices: vec![policy.wire_device()],
                },
                BackendPolicy::AscendCorrectness(policy),
            ),
        };
        Ok(LoadedWorkerConfig {
            endpoint,
            hello: WorkerHello {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
                worker_id: worker_id.clone(),
                instance_id: instance_id(&worker_id)?,
                worker_version: env!("CARGO_PKG_VERSION").into(),
                features: Vec::new(),
                capabilities: Some(capabilities),
                active_attempts: Vec::new(),
            },
            journal: self.worker.journal,
            backend,
        })
    }
}

fn validate_cuda_environment(environment: &CudaEnvironmentConfig) -> Result<(), Box<dyn Error>> {
    if environment.architecture.trim().is_empty()
        || environment.driver_version.trim().is_empty()
        || environment.toolkit_version.trim().is_empty()
    {
        return Err("CUDA environment facts must be nonempty".into());
    }
    Ok(())
}

impl ServerConfig {
    fn endpoint(&self) -> Result<Endpoint, Box<dyn Error>> {
        let mut endpoint = Endpoint::from_shared(self.endpoint.clone())?;
        match self.tls.as_ref() {
            None if is_loopback_uri(&self.endpoint) => {}
            Some(tls) => {
                if tls.server_name.trim().is_empty()
                    || !tls.certificate.is_absolute()
                    || !tls.private_key.is_absolute()
                    || !tls.server_ca.is_absolute()
                {
                    return Err("TLS paths must be absolute and server_name nonempty".into());
                }
                endpoint = endpoint.tls_config(
                    ClientTlsConfig::new()
                        .identity(Identity::from_pem(
                            fs::read(&tls.certificate)?,
                            fs::read(&tls.private_key)?,
                        ))
                        .ca_certificate(Certificate::from_pem(fs::read(&tls.server_ca)?))
                        .domain_name(&tls.server_name),
                )?;
            }
            None => return Err("remote worker config requires server.tls".into()),
        }
        Ok(endpoint)
    }
}

fn is_loopback_uri(uri: &str) -> bool {
    uri.starts_with("http://127.0.0.1:")
        || uri.starts_with("http://localhost:")
        || uri.starts_with("http://[::1]:")
}

fn instance_id(worker_id: &str) -> Result<String, Box<dyn Error>> {
    let unix_nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{worker_id}-{}-{unix_nanos}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_config_carries_connection_identity_and_backend() -> Result<(), Box<dyn Error>> {
        let digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        let config = format!(
            r#"{{
              "schema_version": 1,
              "server": {{"endpoint": "http://127.0.0.1:50051"}},
              "worker": {{"id": "cuda-demo", "journal": "worker.sqlite3"}},
              "runtime": {{
                "backend": "cuda",
                "environment": {{
                  "architecture": "sm_121", "driver_version": "580.0", "toolkit_version": "13.0"
                }},
                "policy": {{
                  "schema_version": 1, "fixture_id": "cuda-vectoradd-v1",
                  "bundle_digest": "{digest}", "image_digest": "{digest}",
                  "image_reference": "alloyport-cuda:local", "image_id": "{digest}",
                  "device_selection": {{"allowed_device_ids": [], "preferred_device_id": null}},
                  "sandbox_root": "/tmp/alloyport-cuda-sandboxes",
                  "ceilings": {{"cpu_millis": 1, "memory_bytes": 1, "disk_bytes": 134217728,
                    "process_count": 1, "output_bytes": 1}},
                  "local_artifact_root": "/tmp/alloyport-cuda-artifacts",
                  "local_artifact_max_bytes": 2, "max_input_bytes": 2,
                  "upload_chunk_bytes": 1, "upload_ttl_ms": 1,
                  "docker_binary": "/usr/bin/docker", "docker_stop_timeout_seconds": 1,
                  "nvidia_smi_binary": "/usr/bin/nvidia-smi"
                }}
              }}
            }}"#
        );
        let loaded = WorkerFileConfig::parse(config.as_bytes())?.into_loaded()?;
        assert_eq!(loaded.hello.worker_id, "cuda-demo");
        assert!(matches!(loaded.backend, BackendPolicy::Cuda(_)));
        Ok(())
    }

    #[test]
    fn correctness_backends_parse_without_fixture_bundle_authority() -> Result<(), Box<dyn Error>> {
        let digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        let cuda = format!(
            r#"{{
              "schema_version": 1,
              "server": {{"endpoint": "http://127.0.0.1:50051"}},
              "worker": {{"id": "cuda-correctness", "journal": "cuda.sqlite3"}},
              "runtime": {{
                "backend": "cuda_correctness",
                "environment": {{
                  "architecture": "sm_90", "driver_version": "580", "toolkit_version": "13.0"
                }},
                "policy": {{
                  "schema_version": 1, "image_digest": "{digest}",
                  "image_reference": "alloyport-cuda-correctness:local", "image_id": "{digest}",
                  "device_selection": {{"allowed_device_ids": ["0"], "preferred_device_id": "0"}},
                  "sandbox_root": "/tmp/cuda-correctness-sandboxes",
                  "ceilings": {{"timeout_ms": 60000, "cpu_millis": 2000,
                    "memory_bytes": 2147483648, "disk_bytes": 536870912,
                    "process_count": 64, "output_bytes": 1048576}},
                  "local_artifact_root": "/tmp/cuda-correctness-artifacts",
                  "local_artifact_max_bytes": 67108864, "max_input_bytes": 33554432,
                  "upload_chunk_bytes": 1048576, "upload_ttl_ms": 3600000,
                  "docker_binary": "/usr/bin/docker", "docker_stop_timeout_seconds": 10,
                  "nvidia_smi_binary": "/usr/bin/nvidia-smi"
                }}
              }}
            }}"#
        );
        let loaded = WorkerFileConfig::parse(cuda.as_bytes())?.into_loaded()?;
        assert!(matches!(loaded.backend, BackendPolicy::CudaCorrectness(_)));

        let ascend = format!(
            r#"{{
              "schema_version": 1,
              "server": {{"endpoint": "http://127.0.0.1:50051"}},
              "worker": {{"id": "ascend-correctness", "journal": "ascend.sqlite3"}},
              "runtime": {{
                "backend": "ascend_correctness",
                "policy": {{
                  "schema_version": 1, "image_digest": "{digest}",
                  "image_reference": "alloyport-ascend-correctness:local", "image_id": "{digest}",
                  "device": {{"device_id": "3", "product_name": "Ascend950PR",
                    "serial_number": "serial-3", "firmware_version": "9.0"}},
                  "device_nodes": ["/dev/davinci3", "/dev/davinci_manager", "/dev/hisi_hdc"],
                  "driver_path": "/usr/local/Ascend/driver",
                  "sandbox_root": "/tmp/ascend-correctness-sandboxes",
                  "environment": {{"architecture": "Ascend950PR", "cann_version": "9.1",
                    "driver_version": "25.7", "firmware_version": "9.0"}},
                  "ceilings": {{"timeout_ms": 60000, "cpu_millis": 4000,
                    "memory_bytes": 8589934592, "disk_bytes": 1073741824,
                    "process_count": 128, "output_bytes": 1048576}},
                  "local_artifact_root": "/tmp/ascend-correctness-artifacts",
                  "local_artifact_max_bytes": 67108864, "max_input_bytes": 33554432,
                  "upload_chunk_bytes": 1048576, "upload_ttl_ms": 3600000,
                  "docker_binary": "/usr/bin/docker", "docker_stop_timeout_seconds": 10,
                  "npu_smi_binary": "/usr/local/Ascend/driver/tools/npu-smi"
                }}
              }}
            }}"#
        );
        let loaded = WorkerFileConfig::parse(ascend.as_bytes())?.into_loaded()?;
        assert!(matches!(
            loaded.backend,
            BackendPolicy::AscendCorrectness(_)
        ));
        let crossed = ascend.replace(
            "\"backend\": \"ascend_correctness\"",
            "\"backend\": \"cuda_correctness\"",
        );
        assert!(WorkerFileConfig::parse(crossed.as_bytes()).is_err());
        Ok(())
    }

    #[test]
    fn checked_in_correctness_worker_examples_match_the_strict_schema() -> Result<(), Box<dyn Error>>
    {
        let cuda = WorkerFileConfig::parse(include_bytes!(
            "../../../../docs/cuda-correctness-worker-config.example.json"
        ))?
        .into_loaded()?;
        assert!(matches!(cuda.backend, BackendPolicy::CudaCorrectness(_)));
        let ascend = WorkerFileConfig::parse(include_bytes!(
            "../../../../docs/ascend-correctness-worker-config.example.json"
        ))?
        .into_loaded()?;
        assert!(matches!(
            ascend.backend,
            BackendPolicy::AscendCorrectness(_)
        ));
        Ok(())
    }
}
