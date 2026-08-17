//! One schema-validated configuration file for an outbound accelerator worker.

use super::ascend_candidate_config::AscendCandidateWorkerConfig;
use super::backend_config::{AscendWorkerConfig, CudaWorkerConfig};
use super::build_config::AscendBuildWorkerConfig;
use super::correctness_config::{AscendCorrectnessWorkerConfig, CudaCorrectnessWorkerConfig};
use alloyport_proto::v1::{Backend, WorkerCapabilities, WorkerHello};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use serde::Deserialize;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

const SIBLING_CONFIG_NAME: &str = "alloyport-worker.json";
const SYSTEM_CONFIG_PATH: &str = "/etc/alloyport-worker/worker.json";

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
    #[serde(rename = "ascend_build")]
    AscendBuild {
        policy: AscendBuildWorkerConfig,
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
    #[serde(rename = "ascend_candidate")]
    AscendCandidate {
        policy: AscendCandidateWorkerConfig,
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
    AscendBuild(AscendBuildWorkerConfig),
    CudaCorrectness(CudaCorrectnessWorkerConfig),
    AscendCorrectness(AscendCorrectnessWorkerConfig),
    AscendCandidate(AscendCandidateWorkerConfig),
}

pub(super) struct LoadedWorkerConfig {
    pub endpoint: Endpoint,
    pub hello: WorkerHello,
    pub journal: PathBuf,
    pub backend: BackendPolicy,
}

impl WorkerFileConfig {
    pub fn load_from_arguments(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<Self, Box<dyn Error>> {
        let executable = env::current_exe().ok();
        let path = locate_config_path(
            arguments,
            |name| env::var_os(name),
            executable.as_deref(),
            Path::is_file,
        )?;
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
            RuntimeConfig::AscendBuild { policy } => policy.validate()?,
            RuntimeConfig::CudaCorrectness {
                environment,
                policy,
            } => {
                validate_cuda_environment(environment)?;
                policy.validate()?;
            }
            RuntimeConfig::AscendCorrectness { policy } => policy.validate()?,
            RuntimeConfig::AscendCandidate { policy } => policy.validate()?,
        }
        self.server.endpoint()?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
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
            RuntimeConfig::AscendBuild { policy } => (
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
                BackendPolicy::AscendBuild(policy),
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
            RuntimeConfig::AscendCandidate { policy } => (
                WorkerCapabilities {
                    backend: Backend::Ascend.into(),
                    architecture: policy.environment.architecture.clone(),
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: policy.environment.driver_version.clone(),
                    toolkit_version: policy.environment.cann_version.clone(),
                    container_runtime: "docker".into(),
                    devices: Vec::new(),
                },
                BackendPolicy::AscendCandidate(policy),
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

fn locate_config_path(
    arguments: impl IntoIterator<Item = OsString>,
    environment: impl Fn(&str) -> Option<OsString>,
    executable: Option<&Path>,
    is_file: impl Fn(&Path) -> bool,
) -> Result<PathBuf, Box<dyn Error>> {
    let mut arguments = arguments.into_iter();
    let explicit = match (arguments.next(), arguments.next(), arguments.next()) {
        (Some(flag), Some(path), None) if flag == "--config" => Some(PathBuf::from(path)),
        (Some(path), None, None) => Some(PathBuf::from(path)),
        (None, None, None) => None,
        _ => return Err("usage: alloyport-worker [--config PATH]".into()),
    };
    explicit
        .or_else(|| environment("ALLOYPORT_WORKER_CONFIG").map(PathBuf::from))
        .or_else(|| {
            executable
                .and_then(Path::parent)
                .map(|directory| directory.join(SIBLING_CONFIG_NAME))
                .filter(|path| is_file(path))
        })
        .or_else(|| {
            let path = PathBuf::from(SYSTEM_CONFIG_PATH);
            is_file(&path).then_some(path)
        })
        .ok_or_else(|| {
            format!(
                "worker configuration not found; use --config PATH, set \
                 ALLOYPORT_WORKER_CONFIG, place {SIBLING_CONFIG_NAME} beside the executable, or \
                 install {SYSTEM_CONFIG_PATH}"
            )
            .into()
        })
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
    use alloyport_artifacts::Sha256Digest;
    use std::time::Duration;

    #[test]
    fn locator_precedence_is_explicit_environment_sibling_then_system() -> Result<(), Box<dyn Error>>
    {
        let executable = Path::new("/opt/alloyport-worker/alloyport-worker");
        let sibling = PathBuf::from("/opt/alloyport-worker/alloyport-worker.json");
        let system = PathBuf::from(SYSTEM_CONFIG_PATH);
        let environment = PathBuf::from("/run/secrets/worker.json");
        let explicit = PathBuf::from("/srv/alloyport/worker.json");
        let present = |path: &Path| path == sibling || path == system;

        assert_eq!(
            locate_config_path(
                [
                    OsString::from("--config"),
                    explicit.clone().into_os_string()
                ],
                |_| Some(environment.clone().into_os_string()),
                Some(executable),
                present,
            )?,
            explicit
        );
        assert_eq!(
            locate_config_path(
                [],
                |_| Some(environment.clone().into_os_string()),
                Some(executable),
                present,
            )?,
            environment
        );
        assert_eq!(
            locate_config_path([], |_| None, Some(executable), present)?,
            sibling.clone()
        );
        assert_eq!(
            locate_config_path([], |_| None, None, present)?,
            system.clone()
        );
        assert!(locate_config_path([], |_| None, None, |_| false).is_err());
        Ok(())
    }

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
                  "npu_smi_binary": "/usr/local/bin/npu-smi"
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

    #[test]
    fn checked_in_ascend_build_example_matches_the_strict_schema() -> Result<(), Box<dyn Error>> {
        let build = WorkerFileConfig::parse(include_bytes!(
            "../../../../docs/ascend-build-worker-config.example.json"
        ))?
        .into_loaded()?;
        assert!(matches!(build.backend, BackendPolicy::AscendBuild(_)));
        Ok(())
    }

    #[test]
    fn checked_in_ascend_candidate_example_exposes_the_shared_backend() -> Result<(), Box<dyn Error>>
    {
        let candidate = WorkerFileConfig::parse(include_bytes!(
            "../../../../docs/ascend-candidate-worker-config.example.json"
        ))?
        .into_loaded()?;
        assert!(matches!(
            candidate.backend,
            BackendPolicy::AscendCandidate(_)
        ));
        assert_eq!(candidate.hello.worker_id, "ascend-worker-1");
        assert_eq!(candidate.hello.capabilities.unwrap().max_concurrency, 1);
        Ok(())
    }

    #[test]
    fn device_probe_bound_is_configured_per_host_and_outlives_a_slow_probe()
    -> Result<(), Box<dyn Error>> {
        // A hard-coded 5s bound could not start a healthy Ascend host whose own `npu-smi info`
        // measured 2.17-7.16s; see docs/evidence/device-probe-timeout-20260816.md. The bound is a
        // deployment fact, so a configuration states it and the default must sit outside the
        // slowest probe that measurement observed.
        let slowest_probe_observed_ms: u64 = 7_160;
        assert!(
            crate::device::DEFAULT_DEVICE_PROBE_TIMEOUT_MS > slowest_probe_observed_ms,
            "the default probe bound is inside the measured spread of the probe it bounds"
        );

        // Deliberately not the default value: a configured bound equal to the default cannot tell
        // "the configuration was read" from "the configuration was ignored".
        let example = String::from_utf8(
            include_bytes!("../../../../docs/ascend-candidate-worker-config.example.json").to_vec(),
        )?;
        assert!(example.contains("\"device_probe_timeout_ms\": 30000"));
        let distinct = example.replace(
            "\"device_probe_timeout_ms\": 30000",
            "\"device_probe_timeout_ms\": 45000",
        );
        let candidate = WorkerFileConfig::parse(distinct.as_bytes())?.into_loaded()?;
        let BackendPolicy::AscendCandidate(policy) = candidate.backend else {
            return Err("expected the Ascend candidate backend".into());
        };
        assert_eq!(policy.probe_timeout()?, Duration::from_secs(45));
        assert_ne!(
            u64::try_from(policy.probe_timeout()?.as_millis())?,
            crate::device::DEFAULT_DEVICE_PROBE_TIMEOUT_MS
        );

        let defaulted = WorkerFileConfig::parse(include_bytes!(
            "../../../../docs/cuda-correctness-worker-config.example.json"
        ))?
        .into_loaded()?;
        let BackendPolicy::CudaCorrectness(policy) = defaulted.backend else {
            return Err("expected the CUDA correctness backend".into());
        };
        assert_eq!(
            policy.probe_timeout()?,
            Duration::from_millis(crate::device::DEFAULT_DEVICE_PROBE_TIMEOUT_MS)
        );

        let unbounded = example.replace(
            "\"device_probe_timeout_ms\": 30000",
            "\"device_probe_timeout_ms\": 0",
        );
        assert!(
            WorkerFileConfig::parse(unbounded.as_bytes())
                .and_then(WorkerFileConfig::into_loaded)
                .is_err(),
            "a zero probe bound must be refused rather than silently defaulted"
        );
        Ok(())
    }

    #[test]
    fn ascend_build_backend_is_exclusive_and_has_no_fixture_bundle() -> Result<(), Box<dyn Error>> {
        let image = Sha256Digest::digest_bytes(b"ascend-build-image");
        let config = format!(
            r#"{{
              "schema_version": 1,
              "server": {{"endpoint": "http://127.0.0.1:50051"}},
              "worker": {{"id": "ascend-build", "journal": "build.sqlite3"}},
              "runtime": {{
                "backend": "ascend_build",
                "policy": {{
                  "schema_version": 1,
                  "image_digest": "{image}",
                  "image_reference": "alloyport-ascend-build:local",
                  "image_id": "{image}",
                  "device": {{
                    "device_id": "0", "product_name": "Ascend950PR",
                    "serial_number": "serial", "firmware_version": "firmware"
                  }},
                  "device_nodes": [
                    "/dev/davinci0", "/dev/davinci_manager", "/dev/hisi_hdc"
                  ],
                  "driver_path": "/usr/local/Ascend/driver",
                  "sandbox_root": "/tmp/ascend-build-sandboxes",
                  "environment": {{
                    "architecture": "Ascend950PR", "cann_version": "9.1.0-beta.1",
                    "driver_version": "25.7.rc1.6", "firmware_version": "firmware"
                  }},
                  "ceilings": {{
                    "timeout_ms": 120000, "cpu_millis": 4000,
                    "memory_bytes": 8589934592, "disk_bytes": 1073741824,
                    "process_count": 128, "output_bytes": 8388608
                  }},
                  "local_artifact_root": "/tmp/ascend-build-artifacts",
                  "local_artifact_max_bytes": 67108864,
                  "max_input_bytes": 33554432,
                  "upload_chunk_bytes": 1048576,
                  "upload_ttl_ms": 3600000,
                  "docker_binary": "/usr/bin/docker",
                  "docker_stop_timeout_seconds": 10,
                  "npu_smi_binary": "/usr/local/bin/npu-smi"
                }}
              }}
            }}"#
        );
        let loaded = WorkerFileConfig::parse(config.as_bytes())?.into_loaded()?;
        assert!(matches!(loaded.backend, BackendPolicy::AscendBuild(_)));
        assert_eq!(loaded.hello.worker_id, "ascend-build");
        assert_eq!(
            loaded.hello.capabilities.unwrap().backend,
            Backend::Ascend as i32
        );
        assert!(!config.contains("fixture_id"));
        assert!(!config.contains("bundle_digest"));
        Ok(())
    }
}
