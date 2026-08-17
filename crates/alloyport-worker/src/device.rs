//! Dynamic accelerator observation port used by outbound heartbeat reporting.

use crate::backend_error::BackendError;
use alloyport_core::{AcceleratorDevice, DeviceHealth, DeviceLease, DeviceObservation};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Default bound on one local accelerator probe, in milliseconds.
///
/// This bound exists to catch a *hung* driver, not a slow one, and it was previously a hard-coded
/// `5s` in both `npu-smi` and `nvidia-smi` adapters that nobody had measured. Measured on
/// 2026-08-16 (see `docs/evidence/device-probe-timeout-20260816.md`), a single `npu-smi info` on a
/// healthy, shared `Ascend950PR` host took 2.17–7.16 s across twelve startup sequences: four of
/// twelve exceeded the 5 s bound, so a healthy host could not start its worker. The same constant
/// bounded `nvidia-smi` on GB10, which measured 0.02–0.03 s — 170× under it.
///
/// One constant therefore cannot be right for both, which is why this is a default and
/// `device_probe_timeout_ms` is a worker configuration field. This value is ~4× the slowest probe
/// observed on real hardware, so it sits outside that command's own spread while still bounding a
/// hang. It is a default, not a measurement of any host: measure the probe on a new host and set
/// the field.
pub const DEFAULT_DEVICE_PROBE_TIMEOUT_MS: u64 = 30_000;

/// Future returned by a device observation provider.
pub type DeviceSnapshotFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeviceSnapshot, DeviceStatusError>> + Send + 'a>>;

/// Future returned by one device-specific lifecycle operation.
pub type DeviceLifecycleFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DeviceStatusError>> + Send + 'a>>;

/// One bounded worker-local observation used for scheduling, not correctness authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeviceSnapshot {
    pub devices: Vec<DeviceObservation>,
}

/// Adapter boundary for `npu-smi`, NVML, or another locally authorized probe.
pub trait DeviceStatusProvider: Debug + Send + Sync {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_>;
}

/// Backend-neutral device operations required by durable attempt preflight and cleanup.
pub trait DeviceLifecycleManager: DeviceStatusProvider {
    fn observe_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation>;

    fn recover_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation>;
}

/// Heartbeat view restricted to the one device bound to this worker instance.
#[derive(Debug)]
pub struct BoundDeviceStatusProvider {
    manager: Arc<dyn DeviceLifecycleManager>,
    device_id: String,
}

impl BoundDeviceStatusProvider {
    /// Binds heartbeat telemetry to one nonempty device ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the device ID is empty.
    pub fn new(
        manager: Arc<dyn DeviceLifecycleManager>,
        device_id: impl Into<String>,
    ) -> Result<Self, DeviceStatusError> {
        let device_id = device_id.into();
        if device_id.trim().is_empty() {
            return Err(DeviceStatusError::InvalidConfiguration(
                "bound heartbeat device ID must be nonempty".into(),
            ));
        }
        Ok(Self { manager, device_id })
    }
}

impl DeviceStatusProvider for BoundDeviceStatusProvider {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async move {
            let observation = self.manager.observe_device(&self.device_id).await?;
            if observation.device_id != self.device_id {
                return Err(DeviceStatusError::InvalidResponse(format!(
                    "device probe returned {} for bound device {}",
                    observation.device_id, self.device_id
                )));
            }
            Ok(DeviceSnapshot {
                devices: vec![observation],
            })
        })
    }
}

/// Worker-local policy for selecting one accelerator from backend-specific discovery results.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeviceSelectionPolicy {
    allowed_device_ids: BTreeSet<String>,
    preferred_device_id: Option<String>,
}

impl DeviceSelectionPolicy {
    /// Creates an allowlist with an optional preferred device. An empty allowlist permits every
    /// discovered device; preference never overrides health, occupancy, or a durable lease.
    ///
    /// # Errors
    ///
    /// Returns an error for empty/duplicate IDs or a preferred device outside a nonempty allowlist.
    pub fn new(
        allowed_device_ids: impl IntoIterator<Item = String>,
        preferred_device_id: Option<String>,
    ) -> Result<Self, DeviceStatusError> {
        let mut allowed = BTreeSet::new();
        for device_id in allowed_device_ids {
            if device_id.trim().is_empty() || !allowed.insert(device_id.clone()) {
                return Err(DeviceStatusError::InvalidConfiguration(
                    "device-selection IDs must be nonempty and unique".into(),
                ));
            }
        }
        if preferred_device_id
            .as_ref()
            .is_some_and(|preferred| preferred.trim().is_empty())
        {
            return Err(DeviceStatusError::InvalidConfiguration(
                "preferred device ID must be nonempty".into(),
            ));
        }
        if preferred_device_id
            .as_ref()
            .is_some_and(|preferred| !allowed.is_empty() && !allowed.contains(preferred))
        {
            return Err(DeviceStatusError::InvalidConfiguration(
                "preferred device is outside the configured allowlist".into(),
            ));
        }
        Ok(Self {
            allowed_device_ids: allowed,
            preferred_device_id,
        })
    }
}

/// One static identity paired with the exact observation that made it selectable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedDevice {
    pub identity: AcceleratorDevice,
    pub observation: DeviceObservation,
}

type ValidatedDiscovery = (
    BTreeMap<String, AcceleratorDevice>,
    BTreeMap<String, DeviceObservation>,
);

/// Selects one ready, process-free, unleased accelerator using backend-neutral rules.
///
/// Backend adapters remain responsible for producing trustworthy inventory and observations. This
/// function deliberately does not infer idleness from utilization or memory use.
///
/// # Errors
///
/// Returns an error for inconsistent discovery data or when no allowed reusable device exists.
pub fn select_ready_device(
    inventory: &[AcceleratorDevice],
    snapshot: &DeviceSnapshot,
    leases: &[DeviceLease],
    policy: &DeviceSelectionPolicy,
) -> Result<SelectedDevice, DeviceStatusError> {
    let (identities, observations) = validated_discovery(inventory, snapshot)?;
    let leased_ids = leases
        .iter()
        .map(|lease| lease.device_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut candidates = identities
        .into_values()
        .filter(|device| {
            policy.allowed_device_ids.is_empty()
                || policy.allowed_device_ids.contains(&device.device_id)
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|device| {
        (
            policy.preferred_device_id.as_deref() != Some(device.device_id.as_str()),
            device.device_id.clone(),
        )
    });
    for identity in candidates {
        let observation = observations.get(&identity.device_id).ok_or_else(|| {
            DeviceStatusError::InvalidResponse(format!(
                "device probe omitted inventory device {}",
                identity.device_id
            ))
        })?;
        if observation.health == DeviceHealth::Ready
            && observation.process_count == 0
            && !leased_ids.contains(identity.device_id.as_str())
        {
            return Ok(SelectedDevice {
                identity,
                observation: observation.clone(),
            });
        }
    }
    Err(DeviceStatusError::Unavailable(
        "no allowed accelerator is Ready, process-free, and unleased".into(),
    ))
}

/// Binds one device while preserving crash recovery for an existing durable lease.
///
/// With no leases this is identical to [`select_ready_device`]. A single retained lease instead
/// binds its exact device even when it is occupied or unhealthy, so the runtime can reconcile or
/// quarantine the old attempt. Multiple leased device IDs are inconsistent with a single-device
/// worker and fail closed.
///
/// # Errors
///
/// Returns an error for inconsistent discovery, an out-of-policy retained lease, or no reusable
/// device when there is no recovery lease.
pub fn bind_worker_device(
    inventory: &[AcceleratorDevice],
    snapshot: &DeviceSnapshot,
    leases: &[DeviceLease],
    policy: &DeviceSelectionPolicy,
) -> Result<SelectedDevice, DeviceStatusError> {
    if leases.is_empty() {
        return select_ready_device(inventory, snapshot, leases, policy);
    }
    let leased_ids = leases
        .iter()
        .map(|lease| lease.device_id.as_str())
        .collect::<BTreeSet<_>>();
    if leased_ids.len() != 1 {
        return Err(DeviceStatusError::InvalidResponse(
            "single-device worker has durable leases for multiple devices".into(),
        ));
    }
    let leased_id = *leased_ids.iter().next().ok_or_else(|| {
        DeviceStatusError::Internal("retained device lease set became empty".into())
    })?;
    if !policy.allowed_device_ids.is_empty() && !policy.allowed_device_ids.contains(leased_id) {
        return Err(DeviceStatusError::InvalidConfiguration(
            "retained device lease is outside the configured allowlist".into(),
        ));
    }
    let (identities, observations) = validated_discovery(inventory, snapshot)?;
    let identity = identities.get(leased_id).cloned().ok_or_else(|| {
        DeviceStatusError::InvalidResponse(format!(
            "retained device lease names undiscovered device {leased_id}"
        ))
    })?;
    let observation = observations.get(leased_id).cloned().ok_or_else(|| {
        DeviceStatusError::InvalidResponse(format!(
            "device probe omitted retained lease device {leased_id}"
        ))
    })?;
    Ok(SelectedDevice {
        identity,
        observation,
    })
}

/// Binds a daemon to one configured accelerator without requiring it to be idle at startup.
///
/// The worker can therefore stay connected and report a busy or unhealthy device. Attempt
/// preflight remains responsible for requiring a ready, process-free device before execution.
/// A retained durable lease must still name this exact configured device.
///
/// # Errors
///
/// Returns an error when discovery omits or changes the configured identity, or when a retained
/// lease names another device.
pub fn bind_configured_device(
    inventory: &[AcceleratorDevice],
    snapshot: &DeviceSnapshot,
    leases: &[DeviceLease],
    configured: &AcceleratorDevice,
) -> Result<SelectedDevice, DeviceStatusError> {
    if leases
        .iter()
        .any(|lease| lease.device_id != configured.device_id)
    {
        return Err(DeviceStatusError::InvalidConfiguration(
            "retained device lease does not match the configured worker device".into(),
        ));
    }
    let (identities, observations) = validated_discovery(inventory, snapshot)?;
    let identity = identities.get(&configured.device_id).ok_or_else(|| {
        DeviceStatusError::InvalidResponse(format!(
            "configured device {} is absent from accelerator inventory",
            configured.device_id
        ))
    })?;
    if identity != configured {
        return Err(DeviceStatusError::InvalidResponse(
            "discovered accelerator identity does not match the configured worker device".into(),
        ));
    }
    let observation = observations
        .get(&configured.device_id)
        .cloned()
        .ok_or_else(|| {
            DeviceStatusError::InvalidResponse(format!(
                "device probe omitted configured device {}",
                configured.device_id
            ))
        })?;
    Ok(SelectedDevice {
        identity: identity.clone(),
        observation,
    })
}

fn validated_discovery(
    inventory: &[AcceleratorDevice],
    snapshot: &DeviceSnapshot,
) -> Result<ValidatedDiscovery, DeviceStatusError> {
    let mut identities = BTreeMap::new();
    for device in inventory {
        if device.device_id.trim().is_empty()
            || identities
                .insert(device.device_id.clone(), device.clone())
                .is_some()
        {
            return Err(DeviceStatusError::InvalidResponse(
                "accelerator inventory contains an empty or duplicate device ID".into(),
            ));
        }
    }
    let mut observations = BTreeMap::new();
    for observation in &snapshot.devices {
        if observations
            .insert(observation.device_id.clone(), observation.clone())
            .is_some()
            || !identities.contains_key(&observation.device_id)
        {
            return Err(DeviceStatusError::InvalidResponse(
                "device observations contain an unknown or duplicate device ID".into(),
            ));
        }
    }
    Ok((identities, observations))
}

/// Stable adapter failures for local accelerator discovery and health probes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceStatusError {
    InvalidConfiguration(String),
    Unavailable(String),
    InvalidResponse(String),
    RecoveryUnsupported(String),
    Internal(String),
}

impl Display for DeviceStatusError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let (kind, detail) = match self {
            Self::InvalidConfiguration(detail) => ("invalid device probe configuration", detail),
            Self::Unavailable(detail) => ("device probe unavailable", detail),
            Self::InvalidResponse(detail) => ("invalid device probe response", detail),
            Self::RecoveryUnsupported(detail) => ("device recovery unsupported", detail),
            Self::Internal(detail) => ("device probe internal failure", detail),
        };
        write!(formatter, "{kind}: {detail}")
    }
}

impl Error for DeviceStatusError {}

impl From<DeviceStatusError> for BackendError {
    fn from(error: DeviceStatusError) -> Self {
        let detail = error.to_string();
        match error {
            DeviceStatusError::InvalidConfiguration(_) => Self::policy(detail),
            DeviceStatusError::Unavailable(_) | DeviceStatusError::RecoveryUnsupported(_) => {
                Self::retryable(detail)
            }
            DeviceStatusError::InvalidResponse(_) => Self::integrity(detail),
            DeviceStatusError::Internal(_) => Self::terminal(detail),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_core::AttemptId;

    #[test]
    fn selection_prefers_but_never_chooses_busy_unhealthy_or_leased_devices()
    -> Result<(), Box<dyn Error>> {
        let inventory = (0..4).map(device).collect::<Vec<_>>();
        let snapshot = DeviceSnapshot {
            devices: vec![
                observation("0", DeviceHealth::Ready, 1),
                observation("1", DeviceHealth::Unhealthy, 0),
                observation("2", DeviceHealth::Ready, 0),
                observation("3", DeviceHealth::Ready, 0),
            ],
        };
        let leases = vec![DeviceLease {
            attempt_id: AttemptId::try_from("attempt-1")?,
            device_id: "2".into(),
            acquired_at_ms: 1,
        }];
        let policy = DeviceSelectionPolicy::new(
            vec!["0".into(), "1".into(), "2".into(), "3".into()],
            Some("0".into()),
        )?;
        assert_eq!(
            select_ready_device(&inventory, &snapshot, &leases, &policy)?
                .identity
                .device_id,
            "3"
        );
        Ok(())
    }

    #[test]
    fn selection_fails_closed_on_inconsistent_or_exhausted_discovery() {
        let inventory = vec![device(0)];
        let policy = DeviceSelectionPolicy::default();
        assert!(select_ready_device(&inventory, &DeviceSnapshot::default(), &[], &policy).is_err());
        let snapshot = DeviceSnapshot {
            devices: vec![observation("foreign", DeviceHealth::Ready, 0)],
        };
        assert!(select_ready_device(&inventory, &snapshot, &[], &policy).is_err());
    }

    #[test]
    fn configured_daemon_binds_and_reports_a_busy_device() -> Result<(), Box<dyn Error>> {
        let configured = device(1);
        let inventory = vec![device(0), configured.clone()];
        let snapshot = DeviceSnapshot {
            devices: vec![
                observation("0", DeviceHealth::Ready, 0),
                observation("1", DeviceHealth::Ready, 2),
            ],
        };
        let selected = bind_configured_device(&inventory, &snapshot, &[], &configured)?;
        assert_eq!(selected.identity, configured);
        assert_eq!(selected.observation.process_count, 2);
        Ok(())
    }

    #[test]
    fn startup_binds_one_quarantined_device_for_recovery_but_rejects_multiple()
    -> Result<(), Box<dyn Error>> {
        let inventory = vec![device(0), device(1)];
        let snapshot = DeviceSnapshot {
            devices: vec![
                observation("0", DeviceHealth::Unhealthy, 1),
                observation("1", DeviceHealth::Ready, 0),
            ],
        };
        let mut leases = vec![DeviceLease {
            attempt_id: AttemptId::try_from("attempt-0")?,
            device_id: "0".into(),
            acquired_at_ms: 1,
        }];
        let selected = bind_worker_device(
            &inventory,
            &snapshot,
            &leases,
            &DeviceSelectionPolicy::default(),
        )?;
        assert_eq!(selected.identity.device_id, "0");
        assert_eq!(selected.observation.health, DeviceHealth::Unhealthy);

        leases.push(DeviceLease {
            attempt_id: AttemptId::try_from("attempt-1")?,
            device_id: "1".into(),
            acquired_at_ms: 2,
        });
        assert!(
            bind_worker_device(
                &inventory,
                &snapshot,
                &leases,
                &DeviceSelectionPolicy::default()
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn bound_heartbeat_provider_reports_only_the_worker_device() -> Result<(), Box<dyn Error>>
    {
        let manager: Arc<dyn DeviceLifecycleManager> = Arc::new(TestLifecycleManager {
            observations: vec![
                observation("0", DeviceHealth::Ready, 0),
                observation("1", DeviceHealth::Ready, 2),
            ],
        });
        let provider = BoundDeviceStatusProvider::new(manager, "1")?;
        assert_eq!(
            provider.snapshot().await?.devices,
            vec![observation("1", DeviceHealth::Ready, 2)]
        );
        Ok(())
    }

    #[derive(Debug)]
    struct TestLifecycleManager {
        observations: Vec<DeviceObservation>,
    }

    impl DeviceStatusProvider for TestLifecycleManager {
        fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
            Box::pin(async move {
                Ok(DeviceSnapshot {
                    devices: self.observations.clone(),
                })
            })
        }
    }

    impl DeviceLifecycleManager for TestLifecycleManager {
        fn observe_device<'a>(
            &'a self,
            device_id: &'a str,
        ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
            Box::pin(async move {
                self.observations
                    .iter()
                    .find(|observation| observation.device_id == device_id)
                    .cloned()
                    .ok_or_else(|| {
                        DeviceStatusError::InvalidResponse(format!(
                            "test manager omitted device {device_id}"
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
                    "test recovery is disabled for {device_id}"
                )))
            })
        }
    }

    fn device(index: u32) -> AcceleratorDevice {
        AcceleratorDevice {
            device_id: index.to_string(),
            product_name: "accelerator".into(),
            serial_number: format!("serial-{index}"),
            firmware_version: "firmware".into(),
        }
    }

    fn observation(device_id: &str, health: DeviceHealth, process_count: u32) -> DeviceObservation {
        DeviceObservation {
            device_id: device_id.into(),
            health,
            process_count,
            utilization_percent: 0,
            memory_used_bytes: 0,
            memory_total_bytes: 1,
            temperature_millicelsius: 0,
            power_milliwatts: 0,
            observed_at_ms: 1,
            detail: String::new(),
        }
    }
}
