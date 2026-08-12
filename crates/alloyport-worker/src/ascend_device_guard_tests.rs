use super::*;
use crate::AdmissionPolicy;
use crate::adapters::sqlite::SqliteAttemptStore;
use crate::device::{
    DeviceLifecycleFuture, DeviceLifecycleManager, DeviceSnapshot, DeviceSnapshotFuture,
    DeviceStatusProvider,
};
use crate::journal::{
    AttemptLifecycleStore, StoredArtifact, StoredAssignment, StoredExecution, StoredFinished,
};
use alloyport_core::{
    AssignmentId, AttemptOutcome, CandidateId, ExecutionKind, Sha256Digest, TaskId,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn ready_preflight_and_ready_terminal_probe_release_the_durable_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let (state, attempt_id) = state()?;
    let manager = FakeManager::new(vec![
        Ok(observation(DeviceHealth::Ready, 0)),
        Ok(observation(DeviceHealth::Ready, 0)),
    ]);

    let preflight =
        DeviceGuard::acquire_and_preflight(&state, attempt_id.clone(), "3", &manager).await?;
    assert_eq!(preflight.lease, DeviceLeaseOutcome::Acquired);
    assert_eq!(state.active_device_leases()?.len(), 1);

    state.mark_finished(attempt_id.as_str(), &finished())?;
    let cleanup = DeviceGuard::cleanup_after_terminal(&state, attempt_id, "3", &manager).await?;
    assert_eq!(cleanup.release, DeviceReleaseOutcome::Released);
    assert!(cleanup.after_recovery.is_none());
    assert!(state.active_device_leases()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn occupied_ready_preflight_releases_without_resetting_another_users_process()
-> Result<(), Box<dyn std::error::Error>> {
    let (state, attempt_id) = state()?;
    let manager = FakeManager::new(vec![
        Ok(observation(DeviceHealth::Ready, 2)),
        Ok(observation(DeviceHealth::Ready, 0)),
    ]);

    let error = DeviceGuard::acquire_and_preflight(&state, attempt_id.clone(), "3", &manager)
        .await
        .expect_err("occupied device must not start");
    assert!(matches!(error, DeviceGuardError::Occupied(_)));
    assert!(state.active_device_leases()?.is_empty());
    assert_eq!(manager.recovery_calls(), 0);

    let retry = DeviceGuard::acquire_and_preflight(&state, attempt_id, "3", &manager).await?;
    assert_eq!(retry.lease, DeviceLeaseOutcome::Acquired);
    assert_eq!(retry.observation.process_count, 0);
    assert_eq!(state.active_device_leases()?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn accepted_replay_returns_durable_preflight_instead_of_replacing_it()
-> Result<(), Box<dyn std::error::Error>> {
    let (state, attempt_id) = state()?;
    state.acquire_device_lease(&attempt_id, "3")?;
    let original = observation(DeviceHealth::Ready, 0);
    state.record_device_preflight(&attempt_id, &original)?;
    let mut later = original.clone();
    later.observed_at_ms = 2;
    let manager = FakeManager::new(vec![Ok(later)]);

    let replay =
        DeviceGuard::acquire_and_preflight(&state, attempt_id.clone(), "3", &manager).await?;
    assert_eq!(replay.lease, DeviceLeaseOutcome::Duplicate);
    assert_eq!(replay.observation, original);
    assert_eq!(
        state.device_preflight(&attempt_id)?,
        Some(replay.observation)
    );
    assert_eq!(state.active_device_leases()?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn unhealthy_or_unattributed_process_state_remains_quarantined_without_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let (state, attempt_id) = state()?;
    let manager = FakeManager::new(vec![
        Ok(observation(DeviceHealth::Unhealthy, 0)),
        Ok(observation(DeviceHealth::Unhealthy, 1)),
    ]);

    let preflight = DeviceGuard::acquire_and_preflight(&state, attempt_id.clone(), "3", &manager)
        .await
        .expect_err("unhealthy device must be quarantined");
    assert!(matches!(preflight, DeviceGuardError::NotReadyRetained(_)));
    assert_eq!(state.active_device_leases()?.len(), 1);

    state.mark_finished(attempt_id.as_str(), &finished())?;
    let cleanup = DeviceGuard::cleanup_after_terminal(&state, attempt_id, "3", &manager)
        .await
        .expect_err("an unattributed process blocks reset and release");
    assert!(matches!(cleanup, DeviceGuardError::OccupiedRetained(_)));
    assert_eq!(manager.recovery_calls(), 0);
    assert_eq!(state.active_device_leases()?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn idle_unhealthy_device_is_released_only_after_recovery_proves_ready()
-> Result<(), Box<dyn std::error::Error>> {
    let (state, attempt_id) = state()?;
    state.acquire_device_lease(&attempt_id, "3")?;
    state.mark_finished(attempt_id.as_str(), &finished())?;
    let manager = FakeManager::with_recovery(
        vec![Ok(observation(DeviceHealth::Unhealthy, 0))],
        vec![Ok(observation(DeviceHealth::Ready, 0))],
    );

    let cleanup = DeviceGuard::cleanup_after_terminal(&state, attempt_id, "3", &manager).await?;
    assert_eq!(manager.recovery_calls(), 1);
    assert_eq!(
        cleanup
            .after_recovery
            .as_ref()
            .map(|observation| observation.health),
        Some(DeviceHealth::Ready)
    );
    assert!(state.active_device_leases()?.is_empty());
    Ok(())
}

#[derive(Debug)]
struct FakeManager {
    observations: Mutex<VecDeque<Result<DeviceObservation, DeviceStatusError>>>,
    recoveries: Mutex<VecDeque<Result<DeviceObservation, DeviceStatusError>>>,
    recovery_calls: Mutex<usize>,
}

impl FakeManager {
    fn new(observations: Vec<Result<DeviceObservation, DeviceStatusError>>) -> Self {
        Self::with_recovery(observations, Vec::new())
    }

    fn with_recovery(
        observations: Vec<Result<DeviceObservation, DeviceStatusError>>,
        recoveries: Vec<Result<DeviceObservation, DeviceStatusError>>,
    ) -> Self {
        Self {
            observations: Mutex::new(observations.into()),
            recoveries: Mutex::new(recoveries.into()),
            recovery_calls: Mutex::new(0),
        }
    }

    fn recovery_calls(&self) -> usize {
        *self.recovery_calls.lock().expect("recovery count lock")
    }
}

impl DeviceStatusProvider for FakeManager {
    fn snapshot(&self) -> DeviceSnapshotFuture<'_> {
        Box::pin(async { Ok(DeviceSnapshot::default()) })
    }
}

impl crate::ascend_smi::AscendDeviceManager for FakeManager {
    fn inventory(
        &self,
    ) -> crate::ascend_smi::AscendDeviceFuture<'_, Vec<alloyport_core::AcceleratorDevice>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl DeviceLifecycleManager for FakeManager {
    fn observe_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            self.observations
                .lock()
                .map_err(|_| DeviceStatusError::Internal("observation lock".into()))?
                .pop_front()
                .ok_or_else(|| DeviceStatusError::Internal("missing observation".into()))?
        })
    }

    fn recover_device<'a>(
        &'a self,
        _device_id: &'a str,
    ) -> DeviceLifecycleFuture<'a, DeviceObservation> {
        Box::pin(async move {
            *self
                .recovery_calls
                .lock()
                .map_err(|_| DeviceStatusError::Internal("recovery count lock".into()))? += 1;
            self.recoveries
                .lock()
                .map_err(|_| DeviceStatusError::Internal("recovery lock".into()))?
                .pop_front()
                .ok_or_else(|| DeviceStatusError::RecoveryUnsupported("fixture".into()))?
        })
    }
}

fn state() -> Result<(WorkerState, AttemptId), Box<dyn std::error::Error>> {
    let store = Arc::new(SqliteAttemptStore::in_memory()?);
    let assignment = stored_assignment();
    store.admit(&assignment, 1)?;
    Ok((
        WorkerState::with_store(AdmissionPolicy::default(), store),
        assignment.attempt_id,
    ))
}

fn observation(health: DeviceHealth, process_count: u32) -> DeviceObservation {
    DeviceObservation {
        device_id: "3".into(),
        health,
        process_count,
        utilization_percent: 0,
        memory_used_bytes: 1024,
        memory_total_bytes: 1024 * 1024,
        temperature_millicelsius: 50_000,
        power_milliwatts: 100_000,
        observed_at_ms: 1,
        detail: String::new(),
    }
}

fn finished() -> StoredFinished {
    StoredFinished {
        outcome: AttemptOutcome::InfraError,
        exit_code: None,
        elapsed_ms: 1,
        receipt: None,
        stdout: None,
        stderr: None,
        detail: "terminal fixture".into(),
    }
}

fn stored_assignment() -> StoredAssignment {
    StoredAssignment {
        assignment_id: AssignmentId::try_from("assignment-1").expect("assignment ID"),
        attempt_id: AttemptId::try_from("attempt-1").expect("attempt ID"),
        attempt_number: 1,
        idempotency_key: "task-1:ascend".into(),
        task_id: TaskId::try_from("task-1").expect("task ID"),
        candidate_id: CandidateId::try_from("candidate-1").expect("candidate ID"),
        execution: StoredExecution {
            executor_kind: ExecutionKind::AscendFixture,
            argv: vec!["ascend-add-v1".into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: StoredArtifact {
                digest: Sha256Digest::digest_bytes(b"bundle"),
                size_bytes: 1,
                media_type: "application/octet-stream".into(),
            },
            image: StoredArtifact {
                digest: Sha256Digest::digest_bytes(b"image"),
                size_bytes: 1,
                media_type: "application/octet-stream".into(),
            },
            limits: None,
        },
        required_features: Vec::new(),
    }
}
