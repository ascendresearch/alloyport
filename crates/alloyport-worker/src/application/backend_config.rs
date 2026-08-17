//! Schema and validation for backend-specific worker policy.

use crate::ascend::{
    ASCEND_ADD_FIXTURE_ID, AscendEnvironmentFacts, AscendFixturePolicy, AscendResourceCeilings,
};
use crate::cuda::{CudaFixturePolicy, CudaResourceCeilings, VECTOR_ADD_FIXTURE_ID};
use crate::device::{DEFAULT_DEVICE_PROBE_TIMEOUT_MS, DeviceSelectionPolicy};
use alloyport_artifacts::Sha256Digest;
use alloyport_core::AcceleratorDevice;
use alloyport_proto::v1::AcceleratorDevice as WireDevice;
use serde::Deserialize;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

/// Resolves the bound on one local accelerator probe.
///
/// The field is optional so an installed worker configuration keeps loading, and an absent value
/// means [`DEFAULT_DEVICE_PROBE_TIMEOUT_MS`]. A host whose probe is slower than the default must
/// measure it and say so here rather than have the worker refuse to start.
pub(super) fn resolve_probe_timeout(configured: Option<u64>) -> Result<Duration, Box<dyn Error>> {
    let millis = configured.unwrap_or(DEFAULT_DEVICE_PROBE_TIMEOUT_MS);
    if millis == 0 {
        return Err("device_probe_timeout_ms must be positive".into());
    }
    Ok(Duration::from_millis(millis))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CudaWorkerConfig {
    pub(super) schema_version: u16,
    pub(super) fixture_id: String,
    pub(super) bundle_digest: String,
    #[serde(alias = "image_manifest_digest")]
    pub(super) image_digest: String,
    pub(super) image_reference: String,
    pub(super) image_id: String,
    pub(super) device_selection: DeviceSelectionConfig,
    pub(super) sandbox_root: PathBuf,
    pub(super) ceilings: CudaCeilingsConfig,
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

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeviceSelectionConfig {
    #[serde(default)]
    pub(super) allowed_device_ids: Vec<String>,
    pub(super) preferred_device_id: Option<String>,
}

impl DeviceSelectionConfig {
    pub(super) fn policy(&self) -> Result<DeviceSelectionPolicy, Box<dyn Error>> {
        Ok(DeviceSelectionPolicy::new(
            self.allowed_device_ids.clone(),
            self.preferred_device_id.clone(),
        )?)
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CudaCeilingsConfig {
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) disk_bytes: u64,
    pub(super) process_count: u32,
    pub(super) output_bytes: u64,
}

impl CudaWorkerConfig {
    #[cfg(test)]
    pub(super) fn parse(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    pub(super) fn validate(&self) -> Result<(), Box<dyn Error>> {
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

    pub(super) fn policy_for(&self, device_id: &str) -> Result<CudaFixturePolicy, Box<dyn Error>> {
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

    pub(super) const fn ceilings(&self) -> CudaResourceCeilings {
        CudaResourceCeilings {
            cpu_millis: self.ceilings.cpu_millis,
            memory_bytes: self.ceilings.memory_bytes,
            disk_bytes: self.ceilings.disk_bytes,
            process_count: self.ceilings.process_count,
            output_bytes: self.ceilings.output_bytes,
        }
    }

    pub(super) fn probe_timeout(&self) -> Result<Duration, Box<dyn Error>> {
        resolve_probe_timeout(self.device_probe_timeout_ms)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AscendWorkerConfig {
    pub(super) schema_version: u16,
    pub(super) fixture_id: String,
    pub(super) bundle_digest: String,
    #[serde(alias = "image_manifest_digest")]
    pub(super) image_digest: String,
    pub(super) image_reference: String,
    pub(super) image_id: String,
    pub(super) device: AscendDeviceConfig,
    pub(super) device_nodes: Vec<PathBuf>,
    pub(super) driver_path: PathBuf,
    pub(super) sandbox_root: PathBuf,
    pub(super) environment: AscendEnvironmentConfig,
    pub(super) ceilings: AscendCeilingsConfig,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AscendDeviceConfig {
    pub(super) device_id: String,
    pub(super) product_name: String,
    pub(super) serial_number: String,
    pub(super) firmware_version: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AscendEnvironmentConfig {
    pub(super) architecture: String,
    pub(super) cann_version: String,
    pub(super) driver_version: String,
    pub(super) firmware_version: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AscendCeilingsConfig {
    pub(super) timeout_ms: u64,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) disk_bytes: u64,
    pub(super) process_count: u32,
    pub(super) output_bytes: u64,
}

impl AscendWorkerConfig {
    #[cfg(test)]
    pub(super) fn parse(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    pub(super) fn validate(&self) -> Result<(), Box<dyn Error>> {
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

    pub(super) fn probe_timeout(&self) -> Result<Duration, Box<dyn Error>> {
        resolve_probe_timeout(self.device_probe_timeout_ms)
    }

    pub(super) const fn ceilings(&self) -> AscendResourceCeilings {
        AscendResourceCeilings {
            timeout_ms: self.ceilings.timeout_ms,
            cpu_millis: self.ceilings.cpu_millis,
            memory_bytes: self.ceilings.memory_bytes,
            disk_bytes: self.ceilings.disk_bytes,
            process_count: self.ceilings.process_count,
            output_bytes: self.ceilings.output_bytes,
        }
    }

    pub(super) fn policy(&self) -> Result<AscendFixturePolicy, Box<dyn Error>> {
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
