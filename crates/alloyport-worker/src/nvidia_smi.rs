//! Bounded shell-free NVIDIA inventory and occupancy adapter.

use crate::device::{
    DeviceLifecycleFuture, DeviceLifecycleManager, DeviceSnapshot, DeviceSnapshotFuture,
    DeviceStatusError, DeviceStatusProvider,
};
use crate::device_command::{BoundedCommandOutput, run_bounded_command};
use alloyport_core::{AcceleratorDevice, DeviceHealth, DeviceObservation};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_OUTPUT_LIMIT: u64 = 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const GPU_QUERY: &[&str] = &[
    "--query-gpu=index,name,uuid,vbios_version,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,gpu_recovery_action",
    "--format=csv,noheader,nounits",
];
const PROCESS_QUERY: &[&str] = &[
    "--query-compute-apps=gpu_uuid,pid",
    "--format=csv,noheader,nounits",
];

pub type CudaDeviceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DeviceStatusError>> + Send + 'a>>;

pub trait CudaDeviceManager: DeviceLifecycleManager {
    fn inventory(&self) -> CudaDeviceFuture<'_, Vec<AcceleratorDevice>>;
}

/// System `nvidia-smi` adapter with fixed queries, timeout, and combined output ceiling.
#[derive(Clone)]
pub struct NvidiaSmi {
    runner: Arc<dyn NvidiaSmiCommandRunner>,
    output_limit: u64,
    timeout: Duration,
}

impl Debug for NvidiaSmi {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NvidiaSmi")
            .field("runner", &self.runner)
            .field("output_limit", &self.output_limit)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl NvidiaSmi {
    /// Creates an adapter around one absolute `nvidia-smi` binary.
    ///
    /// # Errors
    ///
    /// Returns an error when the binary path is not absolute.
    pub fn new(binary: impl Into<PathBuf>) -> Result<Self, DeviceStatusError> {
        Self::with_runner(
            Arc::new(SystemNvidiaSmiCommandRunner {
                binary: binary.into(),
            }),
            DEFAULT_OUTPUT_LIMIT,
            DEFAULT_TIMEOUT,
        )
    }

    fn with_runner(
        runner: Arc<dyn NvidiaSmiCommandRunner>,
        output_limit: u64,
        timeout: Duration,
    ) -> Result<Self, DeviceStatusError> {
        if !runner.binary().is_absolute() || output_limit == 0 || timeout.is_zero() {
            return Err(DeviceStatusError::InvalidConfiguration(
                "nvidia-smi path must be absolute and probe bounds must be nonzero".into(),
            ));
        }
        Ok(Self {
            runner,
            output_limit,
            timeout,
        })
    }

    async fn query(
        &self,
        arguments: &'static [&'static str],
    ) -> Result<Vec<u8>, DeviceStatusError> {
        let runner = Arc::clone(&self.runner);
        let output_limit = self.output_limit;
        let timeout = self.timeout;
        let output =
            tokio::task::spawn_blocking(move || runner.run(arguments, output_limit, timeout))
                .await
                .map_err(|error| DeviceStatusError::Internal(error.to_string()))??;
        if output.output_limit_exceeded {
            return Err(DeviceStatusError::InvalidResponse(
                "nvidia-smi output exceeded its configured bound".into(),
            ));
        }
        if !output.success {
            return Err(DeviceStatusError::Unavailable(format!(
                "nvidia-smi {:?} failed with status {:?}: {}",
                arguments,
                output.exit_code,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(output.stdout)
    }

    async fn discover(
        &self,
    ) -> Result<(Vec<AcceleratorDevice>, DeviceSnapshot), DeviceStatusError> {
        let gpu_rows = self.query(GPU_QUERY).await?;
        let process_rows = self.query(PROCESS_QUERY).await?;
        parse_discovery(&gpu_rows, &process_rows, crate::wire_mapping::now_unix_ms())
    }
}

impl DeviceStatusProvider for NvidiaSmi {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async move { self.discover().await.map(|(_, snapshot)| snapshot) })
    }
}

impl DeviceLifecycleManager for NvidiaSmi {
    fn observe_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            self.discover()
                .await?
                .1
                .devices
                .into_iter()
                .find(|device| device.device_id == device_id)
                .ok_or_else(|| {
                    DeviceStatusError::InvalidResponse(format!(
                        "nvidia-smi omitted configured device {device_id}"
                    ))
                })
        })
    }

    fn recover_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            Err(DeviceStatusError::RecoveryUnsupported(format!(
                "no GPU reset command is authorized for NVIDIA device {device_id}"
            )))
        })
    }
}

impl CudaDeviceManager for NvidiaSmi {
    fn inventory(&self) -> CudaDeviceFuture<'_, Vec<AcceleratorDevice>> {
        Box::pin(async move { self.discover().await.map(|(inventory, _)| inventory) })
    }
}

trait NvidiaSmiCommandRunner: Debug + Send + Sync {
    fn binary(&self) -> &Path;
    fn run(
        &self,
        arguments: &[&str],
        output_limit: u64,
        timeout: Duration,
    ) -> Result<BoundedCommandOutput, DeviceStatusError>;
}

#[derive(Debug)]
struct SystemNvidiaSmiCommandRunner {
    binary: PathBuf,
}

impl NvidiaSmiCommandRunner for SystemNvidiaSmiCommandRunner {
    fn binary(&self) -> &Path {
        &self.binary
    }

    fn run(
        &self,
        arguments: &[&str],
        output_limit: u64,
        timeout: Duration,
    ) -> Result<BoundedCommandOutput, DeviceStatusError> {
        run_bounded_command(&self.binary, arguments, output_limit, timeout)
    }
}

fn parse_discovery(
    gpu_rows: &[u8],
    process_rows: &[u8],
    observed_at_ms: u64,
) -> Result<(Vec<AcceleratorDevice>, DeviceSnapshot), DeviceStatusError> {
    let processes = parse_processes(process_rows)?;
    let text = std::str::from_utf8(gpu_rows).map_err(|error| {
        DeviceStatusError::InvalidResponse(format!("nvidia-smi GPU rows are not UTF-8: {error}"))
    })?;
    let mut inventory = Vec::new();
    let mut observations = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 10 || fields.iter().any(|value| value.is_empty()) {
            return Err(DeviceStatusError::InvalidResponse(
                "nvidia-smi GPU row is incomplete".into(),
            ));
        }
        let device_id = fields[0].to_owned();
        if !seen_ids.insert(device_id.clone()) {
            return Err(DeviceStatusError::InvalidResponse(
                "nvidia-smi returned a duplicate GPU index".into(),
            ));
        }
        let uuid = fields[2].to_owned();
        let utilization_percent = parse_u32(fields[4], "utilization")?;
        if utilization_percent > 100 {
            return Err(DeviceStatusError::InvalidResponse(
                "nvidia-smi utilization exceeds 100 percent".into(),
            ));
        }
        let health = parse_recovery_action(fields[9])?;
        inventory.push(AcceleratorDevice {
            device_id: device_id.clone(),
            product_name: fields[1].to_owned(),
            serial_number: uuid.clone(),
            firmware_version: fields[3].to_owned(),
        });
        observations.push(DeviceObservation {
            device_id,
            health,
            process_count: processes.get(&uuid).copied().unwrap_or(0),
            utilization_percent,
            memory_used_bytes: parse_u64(fields[5], "memory used")?.saturating_mul(1024 * 1024),
            memory_total_bytes: parse_u64(fields[6], "memory total")?.saturating_mul(1024 * 1024),
            temperature_millicelsius: parse_u32(fields[7], "temperature")?.saturating_mul(1000),
            power_milliwatts: parse_decimal_milli(fields[8], "power")?,
            observed_at_ms,
            detail: format!("gpu_recovery_action={}", fields[9]),
        });
    }
    if inventory.is_empty() {
        return Err(DeviceStatusError::InvalidResponse(
            "nvidia-smi returned no GPUs".into(),
        ));
    }
    if processes
        .keys()
        .any(|uuid| !inventory.iter().any(|device| &device.serial_number == uuid))
    {
        return Err(DeviceStatusError::InvalidResponse(
            "nvidia-smi reported a process for an unknown GPU UUID".into(),
        ));
    }
    Ok((
        inventory,
        DeviceSnapshot {
            devices: observations,
        },
    ))
}

fn parse_recovery_action(value: &str) -> Result<DeviceHealth, DeviceStatusError> {
    match value.trim() {
        "None" => Ok(DeviceHealth::Ready),
        "Reset" | "Reboot" | "Drain P2P" | "Drain and Reset" | "Recover IMEX Domain" => {
            Ok(DeviceHealth::Unhealthy)
        }
        "N/A" | "[N/A]" | "Not Supported" | "[Not Supported]" => Ok(DeviceHealth::Degraded),
        other => Err(DeviceStatusError::InvalidResponse(format!(
            "nvidia-smi returned unknown GPU recovery action {other:?}"
        ))),
    }
}

fn parse_processes(bytes: &[u8]) -> Result<BTreeMap<String, u32>, DeviceStatusError> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DeviceStatusError::InvalidResponse(format!(
            "nvidia-smi process rows are not UTF-8: {error}"
        ))
    })?;
    let mut processes = BTreeMap::<String, u32>::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 2 || fields[0].is_empty() || fields[1].parse::<u32>().is_err() {
            return Err(DeviceStatusError::InvalidResponse(
                "nvidia-smi process row is incomplete".into(),
            ));
        }
        *processes.entry(fields[0].to_owned()).or_default() += 1;
    }
    Ok(processes)
}

fn parse_u32(value: &str, field: &str) -> Result<u32, DeviceStatusError> {
    value.parse().map_err(|error| {
        DeviceStatusError::InvalidResponse(format!("invalid nvidia-smi {field}: {error}"))
    })
}

fn parse_u64(value: &str, field: &str) -> Result<u64, DeviceStatusError> {
    value.parse().map_err(|error| {
        DeviceStatusError::InvalidResponse(format!("invalid nvidia-smi {field}: {error}"))
    })
}

fn parse_decimal_milli(value: &str, field: &str) -> Result<u64, DeviceStatusError> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let whole = parse_u64(whole, field)?;
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DeviceStatusError::InvalidResponse(format!(
            "invalid nvidia-smi {field} decimal"
        )));
    }
    let mut fraction = fraction.to_owned();
    while fraction.len() < 3 {
        fraction.push('0');
    }
    Ok(whole
        .saturating_mul(1000)
        .saturating_add(if fraction.is_empty() {
            0
        } else {
            parse_u64(&fraction, field)?
        }))
}

#[cfg(test)]
#[path = "nvidia_smi_tests.rs"]
mod tests;
