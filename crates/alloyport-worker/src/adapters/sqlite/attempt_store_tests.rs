//! Behavioral tests for the `SQLite` worker attempt journal.

use super::SqliteAttemptStore;
use crate::journal::{
    AttemptLifecycleStore, DeviceLeaseStore, DeviceReleaseOutcome, StoreAdmissionOutcome,
    StoredArtifact, StoredAssignment, StoredExecution, StoredFinished, WorkerOutboxMessage,
    WorkerOutboxPayload, WorkerOutboxStore,
};
use alloyport_core::{
    AssignmentId, AttemptId, AttemptOutcome, CandidateId, DeviceHealth, DeviceObservation,
    ExecutionKind, TaskId,
};
use std::error::Error;

#[test]
fn outbox_compacts_only_deliveries_within_the_cumulative_ack() -> Result<(), Box<dyn Error>> {
    let store = SqliteAttemptStore::in_memory()?;
    let first = accepted_message("attempt-1");
    let second = accepted_message("attempt-2");
    store.enqueue_outbox(&first, 1_000)?;
    store.enqueue_outbox(&second, 1_001)?;
    store.record_outbox_delivery("connection-1", 2, &first.message_id, 1_002)?;
    store.record_outbox_delivery("connection-1", 4, &second.message_id, 1_003)?;

    assert_eq!(store.acknowledge_outbox("connection-1", 3)?, 1);
    assert_eq!(store.pending_outbox()?, vec![second.clone()]);
    assert_eq!(store.acknowledge_outbox("connection-1", 4)?, 1);
    assert_eq!(store.outbox_len()?, 0);
    Ok(())
}

#[test]
fn pruning_orphaned_deliveries_never_discards_the_logical_message() -> Result<(), Box<dyn Error>> {
    let store = SqliteAttemptStore::in_memory()?;
    let message = accepted_message("attempt-1");
    store.enqueue_outbox(&message, 1_000)?;
    store.record_outbox_delivery("old-connection", 2, &message.message_id, 1_001)?;

    assert_eq!(store.prune_outbox_deliveries(1_002)?, 1);
    assert_eq!(store.pending_outbox()?, vec![message.clone()]);

    store.record_outbox_delivery("new-connection", 2, &message.message_id, 1_003)?;
    assert_eq!(store.acknowledge_outbox("new-connection", 2)?, 1);
    assert_eq!(store.outbox_len()?, 0);
    Ok(())
}

#[test]
fn admission_and_lifecycle_transitions_atomically_create_outbox_messages()
-> Result<(), Box<dyn Error>> {
    let store = SqliteAttemptStore::in_memory()?;
    let assignment = stored_assignment();
    assert_eq!(
        store.admit(&assignment, 1_000)?,
        StoreAdmissionOutcome::Inserted
    );
    let admission = store.pending_outbox()?;
    assert_eq!(admission.len(), 1);
    assert!(matches!(
        admission[0].payload,
        WorkerOutboxPayload::AssignmentAccepted {
            already_known: false,
            ..
        }
    ));
    store.record_outbox_delivery("connection-1", 2, &admission[0].message_id, 1_001)?;
    store.acknowledge_outbox("connection-1", 2)?;

    assert_eq!(
        store.admit(&assignment, 1_002)?,
        StoreAdmissionOutcome::Duplicate
    );
    assert!(matches!(
        store.pending_outbox()?[0].payload,
        WorkerOutboxPayload::AssignmentAccepted {
            already_known: true,
            ..
        }
    ));
    store.mark_running("attempt-1", 1_003)?;
    assert_eq!(store.outbox_len()?, 2);
    Ok(())
}

#[test]
fn device_lease_and_preflight_survive_reopen() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("worker.sqlite3");
    let first_id = AttemptId::try_from("attempt-1")?;
    {
        let store = SqliteAttemptStore::open(&database)?;
        let first = stored_assignment();
        store.admit(&first, 1_000)?;
        store.acquire_device_lease(&first_id, "3", 1_002)?;
        let preflight = device_preflight(1_004);
        store.record_device_preflight(&first_id, &preflight)?;
        store.mark_finished(first_id.as_str(), &finished(), 1_005)?;
    }

    let reopened = SqliteAttemptStore::open(&database)?;
    let leases = reopened.active_device_leases()?;
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].attempt_id, first_id);
    assert_eq!(leases[0].device_id, "3");
    assert_eq!(
        reopened.device_preflight(&leases[0].attempt_id)?,
        Some(device_preflight(1_004))
    );
    assert_eq!(
        reopened.release_device_lease(&leases[0].attempt_id, 1_006)?,
        DeviceReleaseOutcome::Released
    );
    assert_eq!(
        reopened.release_device_lease(&leases[0].attempt_id, 1_007)?,
        DeviceReleaseOutcome::AlreadyReleased
    );
    assert!(reopened.active_device_leases()?.is_empty());
    Ok(())
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

fn accepted_message(attempt_id: &str) -> WorkerOutboxMessage {
    WorkerOutboxMessage {
        message_id: format!("assignment-accepted:{attempt_id}"),
        attempt_id: attempt_id.to_owned(),
        payload: WorkerOutboxPayload::AssignmentAccepted {
            assignment_id: AssignmentId::try_from(format!("assignment-{attempt_id}"))
                .expect("valid fixture assignment ID"),
            attempt_id: AttemptId::try_from(attempt_id).expect("valid fixture attempt ID"),
            already_known: false,
        },
    }
}

fn stored_assignment() -> StoredAssignment {
    StoredAssignment {
        assignment_id: AssignmentId::try_from("assignment-1").expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from("attempt-1").expect("valid fixture attempt ID"),
        attempt_number: 1,
        idempotency_key: "task-1:build".to_owned(),
        task_id: TaskId::try_from("task-1").expect("valid fixture task ID"),
        candidate_id: CandidateId::try_from("candidate-1").expect("valid fixture candidate ID"),
        execution: StoredExecution {
            executor_kind: ExecutionKind::Container,
            argv: vec!["true".to_owned()],
            working_directory: "source".to_owned(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: StoredArtifact {
                digest: format!("sha256:{}", "a".repeat(64))
                    .parse()
                    .expect("valid fixture digest"),
                size_bytes: 1,
                media_type: "application/octet-stream".to_owned(),
            },
            image: StoredArtifact {
                digest: format!("sha256:{}", "b".repeat(64))
                    .parse()
                    .expect("valid fixture digest"),
                size_bytes: 1,
                media_type: "application/octet-stream".to_owned(),
            },
            limits: None,
        },
        required_features: Vec::new(),
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
