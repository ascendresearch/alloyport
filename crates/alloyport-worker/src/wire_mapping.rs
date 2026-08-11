//! Protobuf and durable-journal record mappings at the worker boundary.

use crate::journal::{
    StoredArtifact, StoredAssignment, StoredEnvironment, StoredExecution, StoredFinished,
    StoredLimits, WorkerOutboxPayload,
};
use alloyport_core::{ExecutionKind, NetworkPolicy};
use alloyport_proto::v1::{
    ArtifactRef, Assignment, AssignmentAccepted, AssignmentRejected, CancellationAcknowledged,
    ExecutionFinished, server_to_worker, worker_to_server,
};

pub(super) fn assignment_to_stored(assignment: &Assignment) -> StoredAssignment {
    let execution = assignment
        .execution
        .as_ref()
        .expect("validated assignment contains execution");
    StoredAssignment {
        assignment_id: assignment.assignment_id.clone(),
        attempt_id: assignment.attempt_id.clone(),
        attempt_number: assignment.attempt_number,
        idempotency_key: assignment.idempotency_key.clone(),
        task_id: assignment.task_id.clone(),
        candidate_id: assignment.candidate_id.clone(),
        execution: StoredExecution {
            executor_kind: ExecutionKind::try_from(execution.executor_kind)
                .expect("validated assignment contains a known executor kind"),
            argv: execution.argv.clone(),
            working_directory: execution.working_directory.clone(),
            environment: execution
                .environment
                .iter()
                .map(|entry| StoredEnvironment {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: execution.timeout_ms,
            bundle: artifact_to_stored(
                execution
                    .bundle
                    .as_ref()
                    .expect("validated assignment contains bundle"),
            ),
            image: artifact_to_stored(
                execution
                    .image
                    .as_ref()
                    .expect("validated assignment contains image"),
            ),
            limits: execution.limits.as_ref().map(|limits| StoredLimits {
                cpu_millis: limits.cpu_millis,
                memory_bytes: limits.memory_bytes,
                disk_bytes: limits.disk_bytes,
                process_count: limits.process_count,
                output_bytes: limits.output_bytes,
                device_count: limits.device_count,
                network: NetworkPolicy::try_from(limits.network)
                    .expect("validated assignment contains a known network policy"),
            }),
        },
        required_features: assignment.required_features.clone(),
    }
}

fn artifact_to_stored(artifact: &ArtifactRef) -> StoredArtifact {
    StoredArtifact {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

fn stored_to_artifact(artifact: &StoredArtifact) -> ArtifactRef {
    ArtifactRef {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

fn stored_to_finished(
    assignment_id: &str,
    attempt_id: &str,
    finished: &StoredFinished,
) -> ExecutionFinished {
    ExecutionFinished {
        assignment_id: assignment_id.to_owned(),
        attempt_id: attempt_id.to_owned(),
        outcome: finished.outcome.into(),
        exit_code: finished.exit_code,
        elapsed_ms: finished.elapsed_ms,
        receipt: finished.receipt.as_ref().map(stored_to_artifact),
        stdout: finished.stdout.as_ref().map(stored_to_artifact),
        stderr: finished.stderr.as_ref().map(stored_to_artifact),
        detail: finished.detail.clone(),
    }
}

pub(super) fn lifecycle_identity(payload: &WorkerOutboxPayload) -> (String, String) {
    let (kind, attempt_id) = match payload {
        WorkerOutboxPayload::AssignmentAccepted { attempt_id, .. } => {
            ("assignment-accepted", attempt_id)
        }
        WorkerOutboxPayload::AssignmentRejected { attempt_id, .. } => {
            ("assignment-rejected", attempt_id)
        }
        WorkerOutboxPayload::ExecutionStarted { attempt_id, .. } => {
            ("execution-started", attempt_id)
        }
        WorkerOutboxPayload::ExecutionFinished { attempt_id, .. } => {
            ("execution-finished", attempt_id)
        }
        WorkerOutboxPayload::CancellationAcknowledged { attempt_id, .. } => {
            ("cancellation-acknowledged", attempt_id)
        }
    };
    (format!("{kind}:{attempt_id}"), attempt_id.clone())
}

pub(super) fn expected_server_message_id(
    message: Option<&server_to_worker::Message>,
) -> Option<String> {
    match message? {
        server_to_worker::Message::Assignment(assignment) => {
            Some(format!("assignment:{}", assignment.attempt_id))
        }
        server_to_worker::Message::Cancel(cancel) => Some(format!("cancel:{}", cancel.attempt_id)),
        server_to_worker::Message::Welcome(_)
        | server_to_worker::Message::Drain(_)
        | server_to_worker::Message::Acknowledgement(_) => None,
    }
}

pub(super) fn outbox_to_wire(payload: WorkerOutboxPayload) -> worker_to_server::Message {
    match payload {
        WorkerOutboxPayload::AssignmentAccepted {
            assignment_id,
            attempt_id,
            already_known,
        } => worker_to_server::Message::AssignmentAccepted(AssignmentAccepted {
            assignment_id,
            attempt_id,
            already_known,
        }),
        WorkerOutboxPayload::AssignmentRejected {
            assignment_id,
            attempt_id,
            reason,
            detail,
        } => worker_to_server::Message::AssignmentRejected(AssignmentRejected {
            assignment_id,
            attempt_id,
            reason: reason.into(),
            detail,
        }),
        WorkerOutboxPayload::ExecutionStarted {
            assignment_id,
            attempt_id,
        } => worker_to_server::Message::ExecutionStarted(alloyport_proto::v1::ExecutionStarted {
            assignment_id,
            attempt_id,
        }),
        WorkerOutboxPayload::ExecutionFinished {
            assignment_id,
            attempt_id,
            finished,
        } => worker_to_server::Message::ExecutionFinished(stored_to_finished(
            &assignment_id,
            &attempt_id,
            &finished,
        )),
        WorkerOutboxPayload::CancellationAcknowledged {
            assignment_id,
            attempt_id,
            already_terminal,
        } => worker_to_server::Message::CancellationAcknowledged(CancellationAcknowledged {
            assignment_id,
            attempt_id,
            already_terminal,
        }),
    }
}

pub(super) fn now_unix_ms() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}
