//! Strict standalone configuration for reduction correctness workers.

use super::backend_config::{
    AscendDeviceConfig, AscendEnvironmentConfig, DeviceSelectionConfig, resolve_probe_timeout,
};
use crate::ascend::AscendEnvironmentFacts;
use crate::cuda_runtime::CudaEnvironmentFacts;
use crate::reduction_correctness::{CorrectnessResourceCeilings, ReductionCorrectnessPolicy};
use alloyport_artifacts::Sha256Digest;
use alloyport_core::AcceleratorDevice;
use alloyport_proto::v1::AcceleratorDevice as WireDevice;
use serde::Deserialize;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CorrectnessCeilingsConfig {
    pub(super) timeout_ms: u64,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) disk_bytes: u64,
    pub(super) process_count: u32,
    pub(super) output_bytes: u64,
}

impl CorrectnessCeilingsConfig {
    const fn policy(&self) -> CorrectnessResourceCeilings {
        CorrectnessResourceCeilings {
            timeout_ms: self.timeout_ms,
            cpu_millis: self.cpu_millis,
            memory_bytes: self.memory_bytes,
            disk_bytes: self.disk_bytes,
            process_count: self.process_count,
            output_bytes: self.output_bytes,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CudaCorrectnessWorkerConfig {
    pub(super) schema_version: u16,
    #[serde(alias = "image_manifest_digest")]
    pub(super) image_digest: String,
    pub(super) image_reference: String,
    pub(super) image_id: String,
    pub(super) device_selection: DeviceSelectionConfig,
    pub(super) sandbox_root: PathBuf,
    pub(super) ceilings: CorrectnessCeilingsConfig,
    pub(super) local_artifact_root: PathBuf,
    pub(super) local_artifact_max_bytes: u64,
    pub(super) max_input_bytes: u64,
    pub(super) upload_chunk_bytes: usize,
    pub(super) upload_ttl_ms: u64,
    pub(super) docker_binary: PathBuf,
    pub(super) docker_stop_timeout_seconds: u32,
    pub(super) nvidia_smi_binary: PathBuf,
    #[serde(default)]
    pub(super) device_probe_timeout_ms: Option<u64>,
}

impl CudaCorrectnessWorkerConfig {
    pub(super) fn validate(&self) -> Result<(), Box<dyn Error>> {
        validate_common(
            self.schema_version,
            &self.sandbox_root,
            &self.local_artifact_root,
            self.local_artifact_max_bytes,
            self.max_input_bytes,
            self.upload_chunk_bytes,
            self.upload_ttl_ms,
            &self.docker_binary,
            self.docker_stop_timeout_seconds,
            self.ceilings,
        )?;
        if !self.nvidia_smi_binary.is_absolute() {
            return Err("CUDA correctness nvidia-smi path must be absolute".into());
        }
        self.device_selection.policy()?;
        self.probe_timeout()?;
        let environment = CudaEnvironmentFacts::new("validated", "validated", "validated")?;
        self.policy_for("validated-device", &environment)?;
        Ok(())
    }

    pub(super) fn probe_timeout(&self) -> Result<Duration, Box<dyn Error>> {
        resolve_probe_timeout(self.device_probe_timeout_ms)
    }

    pub(super) fn policy_for(
        &self,
        device_id: &str,
        environment: &CudaEnvironmentFacts,
    ) -> Result<ReductionCorrectnessPolicy, Box<dyn Error>> {
        Ok(ReductionCorrectnessPolicy::new_cuda(
            Sha256Digest::from_str(&self.image_digest)?,
            &self.image_reference,
            Sha256Digest::from_str(&self.image_id)?,
            device_id,
            &self.sandbox_root,
            self.ceilings.policy(),
            environment,
        )?)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AscendCorrectnessWorkerConfig {
    pub(super) schema_version: u16,
    #[serde(alias = "image_manifest_digest")]
    pub(super) image_digest: String,
    pub(super) image_reference: String,
    pub(super) image_id: String,
    pub(super) device: AscendDeviceConfig,
    pub(super) device_nodes: Vec<PathBuf>,
    pub(super) driver_path: PathBuf,
    pub(super) sandbox_root: PathBuf,
    pub(super) environment: AscendEnvironmentConfig,
    pub(super) ceilings: CorrectnessCeilingsConfig,
    pub(super) local_artifact_root: PathBuf,
    pub(super) local_artifact_max_bytes: u64,
    pub(super) max_input_bytes: u64,
    pub(super) upload_chunk_bytes: usize,
    pub(super) upload_ttl_ms: u64,
    pub(super) docker_binary: PathBuf,
    pub(super) docker_stop_timeout_seconds: u32,
    pub(super) npu_smi_binary: PathBuf,
    #[serde(default)]
    pub(super) device_probe_timeout_ms: Option<u64>,
}

impl AscendCorrectnessWorkerConfig {
    pub(super) fn validate(&self) -> Result<(), Box<dyn Error>> {
        validate_common(
            self.schema_version,
            &self.sandbox_root,
            &self.local_artifact_root,
            self.local_artifact_max_bytes,
            self.max_input_bytes,
            self.upload_chunk_bytes,
            self.upload_ttl_ms,
            &self.docker_binary,
            self.docker_stop_timeout_seconds,
            self.ceilings,
        )?;
        if !self.npu_smi_binary.is_absolute() {
            return Err("Ascend correctness npu-smi path must be absolute".into());
        }
        self.probe_timeout()?;
        self.policy()?;
        Ok(())
    }

    pub(super) fn probe_timeout(&self) -> Result<Duration, Box<dyn Error>> {
        resolve_probe_timeout(self.device_probe_timeout_ms)
    }

    pub(super) fn device(&self) -> AcceleratorDevice {
        AcceleratorDevice {
            device_id: self.device.device_id.clone(),
            product_name: self.device.product_name.clone(),
            serial_number: self.device.serial_number.clone(),
            firmware_version: self.device.firmware_version.clone(),
        }
    }

    pub(super) fn wire_device(&self) -> WireDevice {
        let device = self.device();
        WireDevice {
            device_id: device.device_id,
            product_name: device.product_name,
            serial_number: device.serial_number,
            firmware_version: device.firmware_version,
        }
    }

    pub(super) fn environment(&self) -> Result<AscendEnvironmentFacts, Box<dyn Error>> {
        Ok(AscendEnvironmentFacts::new(
            &self.environment.architecture,
            &self.environment.cann_version,
            &self.environment.driver_version,
            &self.environment.firmware_version,
        )?)
    }

    pub(super) fn policy(&self) -> Result<ReductionCorrectnessPolicy, Box<dyn Error>> {
        Ok(ReductionCorrectnessPolicy::new_ascend(
            Sha256Digest::from_str(&self.image_digest)?,
            &self.image_reference,
            Sha256Digest::from_str(&self.image_id)?,
            self.device(),
            self.device_nodes.clone(),
            &self.driver_path,
            &self.sandbox_root,
            self.ceilings.policy(),
            &self.environment()?,
        )?)
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_common(
    schema_version: u16,
    sandbox_root: &Path,
    artifact_root: &Path,
    artifact_max_bytes: u64,
    max_input_bytes: u64,
    upload_chunk_bytes: usize,
    upload_ttl_ms: u64,
    docker_binary: &Path,
    stop_timeout_seconds: u32,
    ceilings: CorrectnessCeilingsConfig,
) -> Result<(), Box<dyn Error>> {
    if schema_version != 1 {
        return Err(format!(
            "unsupported correctness worker config schema {schema_version}; expected 1"
        )
        .into());
    }
    if !sandbox_root.is_absolute() || !artifact_root.is_absolute() || !docker_binary.is_absolute() {
        return Err("correctness sandbox, Artifact, and Docker paths must be absolute".into());
    }
    if artifact_root.starts_with(sandbox_root) || sandbox_root.starts_with(artifact_root) {
        return Err("correctness sandbox and local Artifact roots must not overlap".into());
    }
    if artifact_max_bytes == 0
        || max_input_bytes == 0
        || upload_chunk_bytes == 0
        || upload_ttl_ms == 0
        || stop_timeout_seconds == 0
        || max_input_bytes > artifact_max_bytes
        || ceilings.output_bytes > artifact_max_bytes
    {
        return Err("correctness Artifact, upload, Docker, and output limits are invalid".into());
    }
    Ok(())
}
