//! Strict standalone configuration for the Ascend candidate Build Gate worker.

use super::backend_config::{
    AscendCeilingsConfig, AscendDeviceConfig, AscendEnvironmentConfig, resolve_probe_timeout,
};
use crate::ascend::{AscendEnvironmentFacts, AscendResourceCeilings};
use crate::ascend_build::AscendBuildPolicy;
use alloyport_artifacts::Sha256Digest;
use alloyport_core::AcceleratorDevice;
use alloyport_proto::v1::AcceleratorDevice as WireDevice;
use serde::Deserialize;
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AscendBuildWorkerConfig {
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

impl AscendBuildWorkerConfig {
    pub(super) fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported Ascend build worker config schema {}; expected 1",
                self.schema_version
            )
            .into());
        }
        if !self.sandbox_root.is_absolute()
            || !self.local_artifact_root.is_absolute()
            || !self.docker_binary.is_absolute()
            || !self.npu_smi_binary.is_absolute()
        {
            return Err(
                "Ascend build sandbox, Artifact, Docker, and npu-smi paths must be absolute".into(),
            );
        }
        if self.local_artifact_root.starts_with(&self.sandbox_root)
            || self.sandbox_root.starts_with(&self.local_artifact_root)
        {
            return Err("Ascend build sandbox and local Artifact roots must not overlap".into());
        }
        if self.local_artifact_max_bytes == 0
            || self.max_input_bytes == 0
            || self.upload_chunk_bytes == 0
            || self.upload_ttl_ms == 0
            || self.docker_stop_timeout_seconds == 0
            || self.max_input_bytes > self.local_artifact_max_bytes
            || self.ceilings.output_bytes > self.local_artifact_max_bytes
        {
            return Err(
                "Ascend build Artifact, upload, Docker, and output limits are invalid".into(),
            );
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

    pub(super) fn policy(&self) -> Result<AscendBuildPolicy, Box<dyn Error>> {
        Ok(AscendBuildPolicy::new(
            Sha256Digest::from_str(&self.image_digest)?,
            &self.image_reference,
            Sha256Digest::from_str(&self.image_id)?,
            self.device(),
            self.device_nodes.clone(),
            &self.driver_path,
            &self.sandbox_root,
            AscendResourceCeilings {
                timeout_ms: self.ceilings.timeout_ms,
                cpu_millis: self.ceilings.cpu_millis,
                memory_bytes: self.ceilings.memory_bytes,
                disk_bytes: self.ceilings.disk_bytes,
                process_count: self.ceilings.process_count,
                output_bytes: self.ceilings.output_bytes,
            },
            self.environment()?,
        )?)
    }
}
