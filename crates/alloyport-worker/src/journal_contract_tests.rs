//! Reusable behavioral contracts for worker persistence ports.

use crate::adapters::sqlite::SqliteAttemptStore;
use crate::journal::{
    AttemptLifecycleStore, AttemptStoreError, DeviceLeaseOutcome, DeviceLeaseStore,
    DevicePreflightOutcome, DeviceReleaseOutcome, LocalAttemptPhase, StoredArtifact,
    StoredAssignment, StoredExecution, StoredFinished,
};
use alloyport_core::{
    AssignmentId, AttemptId, AttemptOutcome, CandidateId, DeviceHealth, DeviceLease,
    DeviceObservation, ExecutionKind, TaskId,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{Mutex, MutexGuard};

#[test]
fn sqlite_device_lease_store_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    let store = SqliteAttemptStore::in_memory()?;
    store.admit(&stored_assignment(1)?, 1_000)?;
    store.admit(&stored_assignment(2)?, 1_001)?;
    device_lease_port_contract(&store, |attempt_id| {
        store.mark_finished(attempt_id.as_str(), &finished(), 1_010)
    })
}

#[test]
fn memory_device_lease_store_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    let first_id = AttemptId::try_from("attempt-1")?;
    let second_id = AttemptId::try_from("attempt-2")?;
    let store = MemoryDeviceLeaseStore::new([first_id, second_id]);
    device_lease_port_contract(&store, |attempt_id| store.finish(attempt_id))
}

fn device_lease_port_contract(
    store: &impl DeviceLeaseStore,
    finish: impl FnOnce(&AttemptId) -> Result<(), AttemptStoreError>,
) -> Result<(), Box<dyn Error>> {
    let first_id = AttemptId::try_from("attempt-1")?;
    let second_id = AttemptId::try_from("attempt-2")?;
    let unknown_id = AttemptId::try_from("attempt-unknown")?;
    assert!(matches!(
        store.acquire_device_lease(&unknown_id, "3", 999),
        Err(AttemptStoreError::NotFound(attempt)) if attempt == "attempt-unknown"
    ));
    assert!(matches!(
        store.acquire_device_lease(&first_id, " ", 1_000),
        Err(AttemptStoreError::ConflictingDeviceLease { .. })
    ));
    assert_eq!(
        store.acquire_device_lease(&first_id, "3", 1_001)?,
        DeviceLeaseOutcome::Acquired
    );
    assert_eq!(
        store.acquire_device_lease(&first_id, "3", 1_002)?,
        DeviceLeaseOutcome::Duplicate
    );
    assert!(matches!(
        store.acquire_device_lease(&first_id, "4", 1_003),
        Err(AttemptStoreError::ConflictingDeviceLease { .. })
    ));
    assert!(matches!(
        store.acquire_device_lease(&second_id, "3", 1_004),
        Err(AttemptStoreError::DeviceAlreadyLeased {
            device_id,
            attempt_id,
        }) if device_id == "3" && attempt_id == "attempt-1"
    ));
    assert_eq!(
        store.active_device_leases()?,
        vec![DeviceLease {
            attempt_id: first_id.clone(),
            device_id: "3".to_owned(),
            acquired_at_ms: 1_001,
        }]
    );

    let observation = device_preflight(1_005);
    let mut wrong_device = observation.clone();
    wrong_device.device_id = "4".to_owned();
    assert!(matches!(
        store.record_device_preflight(&first_id, &wrong_device),
        Err(AttemptStoreError::ConflictingDeviceLease { .. })
    ));
    assert_eq!(
        store.record_device_preflight(&first_id, &observation)?,
        DevicePreflightOutcome::Recorded
    );
    assert_eq!(
        store.record_device_preflight(&first_id, &observation)?,
        DevicePreflightOutcome::Duplicate
    );
    assert_eq!(store.device_preflight(&first_id)?, Some(observation));
    assert!(matches!(
        store.record_device_preflight(&first_id, &device_preflight(1_006)),
        Err(AttemptStoreError::ConflictingDevicePreflight(attempt))
            if attempt == "attempt-1"
    ));

    finish(&first_id)?;
    assert_eq!(
        store.active_device_leases()?.len(),
        1,
        "terminal state retains quarantine until explicit cleanup releases it"
    );
    assert!(matches!(
        store.acquire_device_lease(&first_id, "3", 1_007),
        Err(AttemptStoreError::InvalidTransition {
            from: LocalAttemptPhase::Finished,
            to: LocalAttemptPhase::Running,
        })
    ));
    assert_eq!(
        store.release_device_lease(&first_id, 1_008)?,
        DeviceReleaseOutcome::Released
    );
    assert_eq!(
        store.release_device_lease(&first_id, 1_009)?,
        DeviceReleaseOutcome::AlreadyReleased
    );
    assert!(store.active_device_leases()?.is_empty());
    assert_eq!(
        store.acquire_device_lease(&second_id, "3", 1_010)?,
        DeviceLeaseOutcome::Acquired
    );
    Ok(())
}

#[derive(Debug)]
struct MemoryDeviceLeaseStore {
    state: Mutex<MemoryDeviceLeaseState>,
}

#[derive(Debug)]
struct MemoryDeviceLeaseState {
    phases: BTreeMap<String, LocalAttemptPhase>,
    leases: BTreeMap<String, MemoryLease>,
    preflights: BTreeMap<String, DeviceObservation>,
}

#[derive(Debug)]
struct MemoryLease {
    device_id: String,
    acquired_at_ms: u64,
    released: bool,
}

impl MemoryDeviceLeaseStore {
    fn new(attempt_ids: impl IntoIterator<Item = AttemptId>) -> Self {
        Self {
            state: Mutex::new(MemoryDeviceLeaseState {
                phases: attempt_ids
                    .into_iter()
                    .map(|attempt_id| (attempt_id.to_string(), LocalAttemptPhase::Accepted))
                    .collect(),
                leases: BTreeMap::new(),
                preflights: BTreeMap::new(),
            }),
        }
    }

    fn state(&self) -> Result<MutexGuard<'_, MemoryDeviceLeaseState>, AttemptStoreError> {
        self.state
            .lock()
            .map_err(|_| AttemptStoreError::LockPoisoned)
    }

    fn finish(&self, attempt_id: &AttemptId) -> Result<(), AttemptStoreError> {
        let mut state = self.state()?;
        let phase = state
            .phases
            .get_mut(attempt_id.as_str())
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        *phase = LocalAttemptPhase::Finished;
        Ok(())
    }
}

impl DeviceLeaseStore for MemoryDeviceLeaseStore {
    fn acquire_device_lease(
        &self,
        attempt_id: &AttemptId,
        device_id: &str,
        at_ms: u64,
    ) -> Result<DeviceLeaseOutcome, AttemptStoreError> {
        if device_id.trim().is_empty() {
            return Err(AttemptStoreError::ConflictingDeviceLease {
                attempt_id: attempt_id.to_string(),
                device_id: device_id.to_owned(),
            });
        }
        let mut state = self.state()?;
        let phase = state
            .phases
            .get(attempt_id.as_str())
            .copied()
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        if phase == LocalAttemptPhase::Finished {
            return Err(AttemptStoreError::InvalidTransition {
                from: phase,
                to: LocalAttemptPhase::Running,
            });
        }
        if let Some(existing) = state.leases.get(attempt_id.as_str()) {
            if existing.device_id == device_id && !existing.released {
                return Ok(DeviceLeaseOutcome::Duplicate);
            }
            if existing.device_id != device_id {
                return Err(AttemptStoreError::ConflictingDeviceLease {
                    attempt_id: attempt_id.to_string(),
                    device_id: existing.device_id.clone(),
                });
            }
        }
        if let Some((owner, _)) = state
            .leases
            .iter()
            .find(|(_, lease)| lease.device_id == device_id && !lease.released)
        {
            return Err(AttemptStoreError::DeviceAlreadyLeased {
                device_id: device_id.to_owned(),
                attempt_id: owner.clone(),
            });
        }
        state.leases.insert(
            attempt_id.to_string(),
            MemoryLease {
                device_id: device_id.to_owned(),
                acquired_at_ms: at_ms,
                released: false,
            },
        );
        Ok(DeviceLeaseOutcome::Acquired)
    }

    fn release_device_lease(
        &self,
        attempt_id: &AttemptId,
        _at_ms: u64,
    ) -> Result<DeviceReleaseOutcome, AttemptStoreError> {
        let mut state = self.state()?;
        let lease = state
            .leases
            .get_mut(attempt_id.as_str())
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        if lease.released {
            Ok(DeviceReleaseOutcome::AlreadyReleased)
        } else {
            lease.released = true;
            Ok(DeviceReleaseOutcome::Released)
        }
    }

    fn active_device_leases(&self) -> Result<Vec<DeviceLease>, AttemptStoreError> {
        let state = self.state()?;
        let mut leases = state
            .leases
            .iter()
            .filter(|(_, lease)| !lease.released)
            .map(|(attempt_id, lease)| {
                Ok(DeviceLease {
                    attempt_id: AttemptId::try_from(attempt_id.clone()).map_err(|error| {
                        AttemptStoreError::Corrupt(format!("invalid memory attempt ID: {error}"))
                    })?,
                    device_id: lease.device_id.clone(),
                    acquired_at_ms: lease.acquired_at_ms,
                })
            })
            .collect::<Result<Vec<_>, AttemptStoreError>>()?;
        leases.sort_by(|left, right| {
            (&left.device_id, &left.attempt_id).cmp(&(&right.device_id, &right.attempt_id))
        });
        Ok(leases)
    }

    fn record_device_preflight(
        &self,
        attempt_id: &AttemptId,
        observation: &DeviceObservation,
    ) -> Result<DevicePreflightOutcome, AttemptStoreError> {
        let mut state = self.state()?;
        if let Some(existing) = state.preflights.get(attempt_id.as_str()) {
            return if existing == observation {
                Ok(DevicePreflightOutcome::Duplicate)
            } else {
                Err(AttemptStoreError::ConflictingDevicePreflight(
                    attempt_id.to_string(),
                ))
            };
        }
        let phase = state
            .phases
            .get(attempt_id.as_str())
            .copied()
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        let lease = state
            .leases
            .get(attempt_id.as_str())
            .filter(|lease| !lease.released)
            .ok_or_else(|| AttemptStoreError::NotFound(attempt_id.to_string()))?;
        if phase != LocalAttemptPhase::Accepted {
            return Err(AttemptStoreError::InvalidTransition {
                from: phase,
                to: LocalAttemptPhase::Running,
            });
        }
        if lease.device_id != observation.device_id {
            return Err(AttemptStoreError::ConflictingDeviceLease {
                attempt_id: attempt_id.to_string(),
                device_id: lease.device_id.clone(),
            });
        }
        state
            .preflights
            .insert(attempt_id.to_string(), observation.clone());
        Ok(DevicePreflightOutcome::Recorded)
    }

    fn device_preflight(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Option<DeviceObservation>, AttemptStoreError> {
        Ok(self.state()?.preflights.get(attempt_id.as_str()).cloned())
    }
}

fn device_preflight(observed_at_ms: u64) -> DeviceObservation {
    DeviceObservation {
        device_id: "3".into(),
        health: DeviceHealth::Ready,
        process_count: 0,
        utilization_percent: 0,
        memory_used_bytes: 1024,
        memory_total_bytes: 1024 * 1024,
        temperature_millicelsius: 50_000,
        power_milliwatts: 100_000,
        observed_at_ms,
        detail: String::new(),
    }
}

fn stored_assignment(number: u8) -> Result<StoredAssignment, Box<dyn Error>> {
    Ok(StoredAssignment {
        assignment_id: AssignmentId::try_from(format!("assignment-{number}"))?,
        attempt_id: AttemptId::try_from(format!("attempt-{number}"))?,
        attempt_number: 1,
        idempotency_key: format!("task-{number}:build"),
        task_id: TaskId::try_from(format!("task-{number}"))?,
        candidate_id: CandidateId::try_from(format!("candidate-{number}"))?,
        execution: StoredExecution {
            executor_kind: ExecutionKind::Container,
            argv: vec!["true".to_owned()],
            working_directory: "source".to_owned(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: artifact('a'),
            image: artifact('b'),
            limits: None,
        },
        required_features: Vec::new(),
    })
}

fn artifact(byte: char) -> StoredArtifact {
    StoredArtifact {
        digest: format!("sha256:{}", byte.to_string().repeat(64))
            .parse()
            .expect("valid fixture digest"),
        size_bytes: 1,
        media_type: "application/octet-stream".to_owned(),
    }
}

fn finished() -> StoredFinished {
    StoredFinished {
        outcome: AttemptOutcome::InfraError,
        exit_code: None,
        elapsed_ms: 5,
        receipt: None,
        stdout: None,
        stderr: None,
        detail: "device requires post-crash health inspection".to_owned(),
    }
}
