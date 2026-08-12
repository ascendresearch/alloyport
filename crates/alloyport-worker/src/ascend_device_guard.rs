//! Backend-neutral durable device lease, preflight, quarantine, and cleanup policy.

use crate::WorkerState;
use crate::backend_error::BackendError;
use crate::device::{DeviceLifecycleManager, DeviceStatusError};
use crate::journal::{DeviceLeaseOutcome, DeviceReleaseOutcome};
use alloyport_core::{AttemptId, DeviceHealth, DeviceObservation};
use serde::Serialize;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevicePreflight {
    pub lease: DeviceLeaseOutcome,
    pub observation: DeviceObservation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceCleanup {
    pub before: DeviceObservation,
    pub after_recovery: Option<DeviceObservation>,
    pub release: DeviceReleaseOutcome,
}

/// Owns the fail-closed ordering around durable device leases and local health probes.
#[derive(Clone, Copy, Debug, Default)]
pub struct DeviceGuard;

impl DeviceGuard {
    /// Acquires the durable lease before observing whether an attempt may start.
    ///
    /// A healthy but occupied device is released because no candidate process has started. Unknown
    /// or unhealthy state retains the lease as a quarantine until an explicit cleanup succeeds.
    ///
    /// # Errors
    ///
    /// Returns a typed error for journal failure, failed probes, occupancy, or non-ready health.
    pub async fn acquire_and_preflight(
        state: &WorkerState,
        attempt_id: AttemptId,
        device_id: &str,
        manager: &dyn DeviceLifecycleManager,
    ) -> Result<DevicePreflight, DeviceGuardError> {
        let lease = state
            .acquire_device_lease_async(attempt_id.clone(), device_id.to_owned())
            .await
            .map_err(|error| DeviceGuardError::Journal(error.to_string()))?;
        let recorded = state
            .device_preflight_async(attempt_id.clone())
            .await
            .map_err(|error| DeviceGuardError::Journal(error.to_string()))?;
        if let Some(recorded) = recorded.as_ref() {
            ensure_device(device_id, recorded).map_err(DeviceGuardError::ProbeRetained)?;
        }
        let observation = manager
            .observe_device(device_id)
            .await
            .map_err(DeviceGuardError::ProbeRetained)?;
        ensure_device(device_id, &observation).map_err(DeviceGuardError::ProbeRetained)?;
        if observation.health == DeviceHealth::Ready && observation.process_count == 0 {
            if let Some(recorded) = recorded {
                return Ok(DevicePreflight {
                    lease,
                    observation: recorded,
                });
            }
            state
                .record_device_preflight_async(attempt_id, observation.clone())
                .await
                .map_err(|error| DeviceGuardError::Journal(error.to_string()))?;
            return Ok(DevicePreflight { lease, observation });
        }
        if observation.health == DeviceHealth::Ready {
            if recorded.is_some() {
                return Err(DeviceGuardError::OccupiedRetained(observation));
            }
            state
                .release_device_lease_async(attempt_id)
                .await
                .map_err(|error| DeviceGuardError::Journal(error.to_string()))?;
            return Err(DeviceGuardError::Occupied(observation));
        }
        Err(DeviceGuardError::NotReadyRetained(observation))
    }

    /// Releases a terminal attempt's lease only after a reusable observation or safe recovery.
    ///
    /// Recovery is never attempted while a process is visible because the probe cannot attribute
    /// that process to `AlloyPort`. Probe/recovery failure retains the durable lease as quarantine.
    ///
    /// # Errors
    ///
    /// Returns a quarantine error when the device cannot be proven reusable.
    pub async fn cleanup_after_terminal(
        state: &WorkerState,
        attempt_id: AttemptId,
        device_id: &str,
        manager: &dyn DeviceLifecycleManager,
    ) -> Result<DeviceCleanup, DeviceGuardError> {
        let before = manager
            .observe_device(device_id)
            .await
            .map_err(DeviceGuardError::ProbeRetained)?;
        ensure_device(device_id, &before).map_err(DeviceGuardError::ProbeRetained)?;
        if reusable(&before) {
            let release = state
                .release_device_lease_async(attempt_id)
                .await
                .map_err(|error| DeviceGuardError::Journal(error.to_string()))?;
            return Ok(DeviceCleanup {
                before,
                after_recovery: None,
                release,
            });
        }
        if before.process_count > 0 {
            return Err(DeviceGuardError::OccupiedRetained(before));
        }
        let after = manager
            .recover_device(device_id)
            .await
            .map_err(DeviceGuardError::RecoveryRetained)?;
        ensure_device(device_id, &after).map_err(DeviceGuardError::RecoveryRetained)?;
        if !reusable(&after) {
            return Err(DeviceGuardError::RecoveryIncompleteRetained { before, after });
        }
        let release = state
            .release_device_lease_async(attempt_id)
            .await
            .map_err(|error| DeviceGuardError::Journal(error.to_string()))?;
        Ok(DeviceCleanup {
            before,
            after_recovery: Some(after),
            release,
        })
    }
}

/// Receipt annotation describing what terminal cleanup must prove after durable commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupIntent {
    ReleaseAfterCommit,
    QuarantineVisibleProcesses,
    RecoverOrQuarantineAfterCommit,
}

impl CleanupIntent {
    #[must_use]
    pub fn from_observation(observation: &DeviceObservation) -> Self {
        if observation.process_count > 0 {
            Self::QuarantineVisibleProcesses
        } else if observation.health == DeviceHealth::Ready {
            Self::ReleaseAfterCommit
        } else {
            Self::RecoverOrQuarantineAfterCommit
        }
    }
}

fn reusable(observation: &DeviceObservation) -> bool {
    observation.health == DeviceHealth::Ready && observation.process_count == 0
}

fn ensure_device(expected: &str, observation: &DeviceObservation) -> Result<(), DeviceStatusError> {
    if observation.device_id == expected {
        Ok(())
    } else {
        Err(DeviceStatusError::InvalidResponse(format!(
            "device probe returned {} while observing {expected}",
            observation.device_id
        )))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceGuardError {
    Journal(String),
    ProbeRetained(DeviceStatusError),
    Occupied(DeviceObservation),
    NotReadyRetained(DeviceObservation),
    OccupiedRetained(DeviceObservation),
    RecoveryRetained(DeviceStatusError),
    RecoveryIncompleteRetained {
        before: DeviceObservation,
        after: DeviceObservation,
    },
}

impl Display for DeviceGuardError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(detail) => write!(formatter, "device journal failure: {detail}"),
            Self::ProbeRetained(error) => {
                write!(formatter, "device probe failed; lease retained: {error}")
            }
            Self::Occupied(observation) => write!(
                formatter,
                "device {} has {} visible processes before start",
                observation.device_id, observation.process_count
            ),
            Self::NotReadyRetained(observation) => write!(
                formatter,
                "device {} is {:?}; lease retained",
                observation.device_id, observation.health
            ),
            Self::OccupiedRetained(observation) => write!(
                formatter,
                "device {} has {} unattributed processes after execution; lease retained",
                observation.device_id, observation.process_count
            ),
            Self::RecoveryRetained(error) => {
                write!(formatter, "device recovery failed; lease retained: {error}")
            }
            Self::RecoveryIncompleteRetained { after, .. } => write!(
                formatter,
                "device {} remained {:?} after recovery; lease retained",
                after.device_id, after.health
            ),
        }
    }
}

impl std::error::Error for DeviceGuardError {}

impl From<DeviceGuardError> for BackendError {
    fn from(error: DeviceGuardError) -> Self {
        Self::retryable(error.to_string())
    }
}

#[cfg(test)]
#[path = "ascend_device_guard_tests.rs"]
mod tests;
