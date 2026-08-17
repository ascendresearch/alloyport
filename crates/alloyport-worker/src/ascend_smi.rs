//! Bounded local `npu-smi` discovery and health-observation adapter.

use crate::device::{
    DeviceLifecycleFuture, DeviceLifecycleManager, DeviceSnapshot, DeviceSnapshotFuture,
    DeviceStatusError, DeviceStatusProvider,
};
use crate::device_command::run_bounded_command;
use alloyport_core::{AcceleratorDevice, DeviceHealth, DeviceObservation};
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_OUTPUT_LIMIT: u64 = 1024 * 1024;

pub type AscendDeviceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DeviceStatusError>> + Send + 'a>>;

/// Ascend-specific device operations used by discovery, runtime preflight, and cleanup.
pub trait AscendDeviceManager: DeviceLifecycleManager {
    fn inventory(&self) -> AscendDeviceFuture<'_, Vec<AcceleratorDevice>>;
}

/// System `npu-smi` adapter. It never invokes a shell and bounds time and retained output.
#[derive(Clone)]
pub struct NpuSmi {
    runner: Arc<dyn NpuSmiCommandRunner>,
    firmware_version: String,
    output_limit: u64,
    timeout: Duration,
}

impl Debug for NpuSmi {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NpuSmi")
            .field("runner", &self.runner)
            .field("firmware_version", &self.firmware_version)
            .field("output_limit", &self.output_limit)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl NpuSmi {
    /// Creates a bounded local adapter around one absolute `npu-smi` binary.
    ///
    /// `probe_timeout` bounds each individual `npu-smi` invocation. It is a deployment fact rather
    /// than a constant: see [`crate::device::DEFAULT_DEVICE_PROBE_TIMEOUT_MS`] for what was
    /// measured and why no single value fits every host.
    ///
    /// # Errors
    ///
    /// Returns an error for a relative binary, empty firmware identity, or zero bounds.
    pub fn new(
        binary: impl Into<PathBuf>,
        firmware_version: impl Into<String>,
        probe_timeout: Duration,
    ) -> Result<Self, DeviceStatusError> {
        Self::with_runner(
            Arc::new(SystemNpuSmiCommandRunner {
                binary: binary.into(),
            }),
            firmware_version,
            DEFAULT_OUTPUT_LIMIT,
            probe_timeout,
        )
    }

    fn with_runner(
        runner: Arc<dyn NpuSmiCommandRunner>,
        firmware_version: impl Into<String>,
        output_limit: u64,
        timeout: Duration,
    ) -> Result<Self, DeviceStatusError> {
        let firmware_version = firmware_version.into();
        if !runner.binary().is_absolute() {
            return Err(DeviceStatusError::InvalidConfiguration(
                "npu-smi binary must be absolute".to_owned(),
            ));
        }
        if firmware_version.trim().is_empty() || output_limit == 0 || timeout.is_zero() {
            return Err(DeviceStatusError::InvalidConfiguration(
                "firmware identity and npu-smi bounds must be nonzero".to_owned(),
            ));
        }
        Ok(Self {
            runner,
            firmware_version,
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
                "npu-smi output exceeded its configured bound".to_owned(),
            ));
        }
        if !output.success {
            let detail = String::from_utf8_lossy(&output.stderr);
            return Err(DeviceStatusError::Unavailable(format!(
                "npu-smi {:?} failed with status {:?}: {}",
                arguments,
                output.exit_code,
                detail.trim()
            )));
        }
        Ok(output.stdout)
    }

    async fn snapshot_inner(&self) -> Result<DeviceSnapshot, DeviceStatusError> {
        let output = self.query(&["info"]).await?;
        parse_snapshot(&output, crate::wire_mapping::now_unix_ms())
    }
}

impl DeviceStatusProvider for NpuSmi {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async move { self.snapshot_inner().await })
    }
}

impl AscendDeviceManager for NpuSmi {
    fn inventory(&self) -> AscendDeviceFuture<'_, Vec<AcceleratorDevice>> {
        Box::pin(async move {
            let listing = self.query(&["info", "-l"]).await?;
            let snapshot = self.query(&["info"]).await?;
            parse_inventory(&listing, &snapshot, &self.firmware_version)
        })
    }
}

impl DeviceLifecycleManager for NpuSmi {
    fn observe_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            self.snapshot_inner()
                .await?
                .devices
                .into_iter()
                .find(|device| device.device_id == device_id)
                .ok_or_else(|| {
                    DeviceStatusError::InvalidResponse(format!(
                        "npu-smi omitted configured device {device_id}"
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
                "no reset command is configured for Ascend device {device_id}"
            )))
        })
    }
}

trait NpuSmiCommandRunner: Debug + Send + Sync {
    fn binary(&self) -> &Path;
    fn run(
        &self,
        arguments: &[&str],
        output_limit: u64,
        timeout: Duration,
    ) -> Result<NpuSmiCommandOutput, DeviceStatusError>;
}

#[derive(Debug)]
struct SystemNpuSmiCommandRunner {
    binary: PathBuf,
}

impl NpuSmiCommandRunner for SystemNpuSmiCommandRunner {
    fn binary(&self) -> &Path {
        &self.binary
    }

    fn run(
        &self,
        arguments: &[&str],
        output_limit: u64,
        timeout: Duration,
    ) -> Result<NpuSmiCommandOutput, DeviceStatusError> {
        let output = run_bounded_command(&self.binary, arguments, output_limit, timeout)?;
        Ok(NpuSmiCommandOutput {
            success: output.success,
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
            output_limit_exceeded: output.output_limit_exceeded,
        })
    }
}

#[derive(Debug)]
struct NpuSmiCommandOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_limit_exceeded: bool,
}

#[derive(Clone)]
struct ParsedDevice {
    product_name: String,
    observation: DeviceObservation,
}

fn parse_snapshot(bytes: &[u8], observed_at_ms: u64) -> Result<DeviceSnapshot, DeviceStatusError> {
    let devices = parse_device_table(bytes, observed_at_ms)?;
    Ok(DeviceSnapshot {
        devices: devices
            .into_values()
            .map(|device| device.observation)
            .collect(),
    })
}

fn parse_inventory(
    listing: &[u8],
    snapshot: &[u8],
    firmware_version: &str,
) -> Result<Vec<AcceleratorDevice>, DeviceStatusError> {
    let text = std::str::from_utf8(listing).map_err(|error| {
        DeviceStatusError::InvalidResponse(format!("npu-smi inventory is not UTF-8: {error}"))
    })?;
    let mut serials = BTreeMap::new();
    let mut current_id = None;
    for line in text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        match name {
            "NPU ID" => current_id = Some(value.to_owned()),
            "Serial Number" => {
                let Some(device_id) = current_id.as_ref() else {
                    continue;
                };
                if value.is_empty()
                    || serials
                        .insert(device_id.clone(), value.to_owned())
                        .is_some()
                {
                    return Err(DeviceStatusError::InvalidResponse(
                        "npu-smi inventory contains an empty or duplicate serial".to_owned(),
                    ));
                }
            }
            _ => {}
        }
    }
    let dynamic = parse_device_table(snapshot, 0)?;
    if serials.is_empty() || serials.len() != dynamic.len() {
        return Err(DeviceStatusError::InvalidResponse(
            "npu-smi static and dynamic device inventories differ".to_owned(),
        ));
    }
    dynamic
        .into_iter()
        .map(|(device_id, device)| {
            Ok(AcceleratorDevice {
                serial_number: serials.get(&device_id).cloned().ok_or_else(|| {
                    DeviceStatusError::InvalidResponse(format!(
                        "npu-smi inventory lacks serial for device {device_id}"
                    ))
                })?,
                device_id,
                product_name: device.product_name,
                firmware_version: firmware_version.to_owned(),
            })
        })
        .collect()
}

fn parse_device_table(
    bytes: &[u8],
    observed_at_ms: u64,
) -> Result<BTreeMap<String, ParsedDevice>, DeviceStatusError> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        DeviceStatusError::InvalidResponse(format!("npu-smi output is not UTF-8: {error}"))
    })?;
    let mut devices: BTreeMap<String, ParsedDevice> = BTreeMap::new();
    let mut current_device = None;
    let mut process_table = false;
    for line in text.lines() {
        if line.contains("Process id") {
            process_table = true;
            current_device = None;
            continue;
        }
        let columns = table_columns(line);
        if columns.len() != 4 {
            continue;
        }
        if process_table {
            if columns[0].bytes().all(|byte| byte.is_ascii_digit())
                && !columns[0].is_empty()
                && let Some(device) = devices.get_mut(columns[0])
            {
                device.observation.process_count =
                    device.observation.process_count.saturating_add(1);
            }
            continue;
        }
        if !columns[0].is_empty()
            && columns[0].bytes().all(|byte| byte.is_ascii_digit())
            && !columns[1].is_empty()
        {
            let (power_milliwatts, temperature_millicelsius) = parse_power_temperature(columns[3])?;
            let health = parse_health(columns[2]);
            let device_id = columns[0].to_owned();
            if devices
                .insert(
                    device_id.clone(),
                    ParsedDevice {
                        product_name: columns[1].to_owned(),
                        observation: DeviceObservation {
                            device_id: device_id.clone(),
                            health,
                            process_count: 0,
                            utilization_percent: 0,
                            memory_used_bytes: 0,
                            memory_total_bytes: 0,
                            temperature_millicelsius,
                            power_milliwatts,
                            observed_at_ms,
                            detail: if health == DeviceHealth::Ready {
                                String::new()
                            } else {
                                format!("npu-smi health={}", columns[2])
                            },
                        },
                    },
                )
                .is_some()
            {
                return Err(DeviceStatusError::InvalidResponse(format!(
                    "npu-smi repeated device {device_id}"
                )));
            }
            current_device = Some(device_id);
        } else if let Some(device_id) = current_device.as_ref()
            && !columns[2].is_empty()
        {
            let device = devices.get_mut(device_id).ok_or_else(|| {
                DeviceStatusError::Internal("npu-smi parser lost current device".to_owned())
            })?;
            let (used_mib, total_mib) = last_slash_pair(columns[3])?;
            device.observation.utilization_percent = first_u32(columns[3])?;
            device.observation.memory_used_bytes = used_mib.saturating_mul(1024 * 1024);
            device.observation.memory_total_bytes = total_mib.saturating_mul(1024 * 1024);
            current_device = None;
        }
    }
    if devices.is_empty()
        || devices
            .values()
            .any(|device| device.observation.memory_total_bytes == 0)
    {
        return Err(DeviceStatusError::InvalidResponse(
            "npu-smi output contains no complete device observations".to_owned(),
        ));
    }
    Ok(devices)
}

fn table_columns(line: &str) -> Vec<&str> {
    let mut columns = line.split('|').map(str::trim).collect::<Vec<_>>();
    if columns.first() == Some(&"") {
        columns.remove(0);
    }
    if columns.last() == Some(&"") {
        columns.pop();
    }
    columns
}

fn parse_health(value: &str) -> DeviceHealth {
    match value.trim().to_ascii_uppercase().as_str() {
        "OK" => DeviceHealth::Ready,
        "ALARM" | "ERROR" | "FAULT" => DeviceHealth::Unhealthy,
        _ => DeviceHealth::Degraded,
    }
}

fn parse_power_temperature(value: &str) -> Result<(u64, u32), DeviceStatusError> {
    let mut tokens = value.split_whitespace();
    let power = tokens
        .next()
        .ok_or_else(|| DeviceStatusError::InvalidResponse("missing NPU power".to_owned()))?;
    let temperature = tokens
        .next()
        .ok_or_else(|| DeviceStatusError::InvalidResponse("missing NPU temperature".to_owned()))?
        .parse::<u32>()
        .map_err(|error| {
            DeviceStatusError::InvalidResponse(format!("invalid NPU temperature: {error}"))
        })?;
    Ok((decimal_milli(power)?, temperature.saturating_mul(1_000)))
}

fn decimal_milli(value: &str) -> Result<u64, DeviceStatusError> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 3
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(DeviceStatusError::InvalidResponse(format!(
            "invalid nonnegative decimal NPU power: {value}"
        )));
    }
    let whole = whole.parse::<u64>().map_err(|error| {
        DeviceStatusError::InvalidResponse(format!("invalid NPU power: {error}"))
    })?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u64>().map_err(|error| {
            DeviceStatusError::InvalidResponse(format!("invalid NPU power fraction: {error}"))
        })? * 10_u64.pow(u32::try_from(3 - fraction.len()).unwrap_or_default())
    };
    whole
        .checked_mul(1_000)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or_else(|| DeviceStatusError::InvalidResponse("NPU power overflow".to_owned()))
}

fn first_u32(value: &str) -> Result<u32, DeviceStatusError> {
    value
        .split_whitespace()
        .next()
        .ok_or_else(|| DeviceStatusError::InvalidResponse("missing NPU utilization".to_owned()))?
        .parse()
        .map_err(|error| {
            DeviceStatusError::InvalidResponse(format!("invalid NPU utilization: {error}"))
        })
}

fn last_slash_pair(value: &str) -> Result<(u64, u64), DeviceStatusError> {
    let tokens = value.split_whitespace().collect::<Vec<_>>();
    let mut pairs = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if *token == "/" && index > 0 && index + 1 < tokens.len() {
            let used = tokens[index - 1].parse::<u64>();
            let total = tokens[index + 1].parse::<u64>();
            if let (Ok(used), Ok(total)) = (used, total) {
                pairs.push((used, total));
            }
        }
    }
    pairs.last().copied().ok_or_else(|| {
        DeviceStatusError::InvalidResponse("missing NPU memory usage pair".to_owned())
    })
}

#[cfg(test)]
#[path = "ascend_smi_tests.rs"]
mod tests;
