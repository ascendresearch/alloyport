//! Behavioral tests for the `SQLite` worker attempt journal.

use super::SqliteAttemptStore;
use crate::journal::{
    AttemptLifecycleStore, StoreAdmissionOutcome, StoredArtifact, StoredAssignment,
    StoredExecution, WorkerOutboxMessage, WorkerOutboxPayload, WorkerOutboxStore,
};
use alloyport_core::{AssignmentId, AttemptId, ExecutionKind, TaskId};
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
        candidate_id: "candidate-1".to_owned(),
        execution: StoredExecution {
            executor_kind: ExecutionKind::Container,
            argv: vec!["true".to_owned()],
            working_directory: "source".to_owned(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: StoredArtifact {
                digest: format!("sha256:{}", "a".repeat(64)),
                size_bytes: 1,
                media_type: "application/octet-stream".to_owned(),
            },
            image: StoredArtifact {
                digest: format!("sha256:{}", "b".repeat(64)),
                size_bytes: 1,
                media_type: "application/octet-stream".to_owned(),
            },
            limits: None,
        },
        required_features: Vec::new(),
    }
}
