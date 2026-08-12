//! Accelerator-device identity, observation, and worker-local lease vocabulary.

use crate::AttemptId;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Static identity advertised for one explicitly enumerated accelerator.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AcceleratorDevice {
    pub device_id: String,
    pub product_name: String,
    pub serial_number: String,
    pub firmware_version: String,
}

/// Dynamic health classification reported independently from occupancy and utilization.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i32)]
#[serde(try_from = "i32", into = "i32")]
pub enum DeviceHealth {
    Ready = 1,
    Degraded = 2,
    Unhealthy = 3,
    Recovering = 4,
}

impl From<DeviceHealth> for i32 {
    fn from(health: DeviceHealth) -> Self {
        health as Self
    }
}

impl TryFrom<i32> for DeviceHealth {
    type Error = DeviceHealthError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Ready),
            2 => Ok(Self::Degraded),
            3 => Ok(Self::Unhealthy),
            4 => Ok(Self::Recovering),
            _ => Err(DeviceHealthError(value)),
        }
    }
}

/// An unknown wire or persisted device-health value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeviceHealthError(pub i32);

impl Display for DeviceHealthError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid device health {}", self.0)
    }
}

impl Error for DeviceHealthError {}

/// One bounded point-in-time scheduling observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeviceObservation {
    pub device_id: String,
    pub health: DeviceHealth,
    pub process_count: u32,
    pub utilization_percent: u32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub temperature_millicelsius: u32,
    pub power_milliwatts: u64,
    pub observed_at_ms: u64,
    pub detail: String,
}

/// Crash-durable ownership of one worker-local device by one process attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeviceLease {
    pub attempt_id: AttemptId,
    pub device_id: String,
    pub acquired_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_health_keeps_numeric_compatibility() -> Result<(), Box<dyn Error>> {
        assert_eq!(serde_json::to_string(&DeviceHealth::Degraded)?, "2");
        assert_eq!(
            serde_json::from_str::<DeviceHealth>("4")?,
            DeviceHealth::Recovering
        );
        assert!(serde_json::from_str::<DeviceHealth>("0").is_err());
        Ok(())
    }
}
