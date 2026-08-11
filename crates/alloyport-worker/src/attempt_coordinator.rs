//! Attempt admission and cancellation use cases.

use super::{OutboundWorker, WorkerError};
use crate::journal::{LocalAttemptPhase, StoredFinished, WorkerOutboxPayload};
use alloyport_core::{AttemptOutcome, ExecutionKind, RejectionReason};
use alloyport_proto::v1::{Assignment, WorkerToServer};
use alloyport_proto::validate_assignment;
use std::collections::BTreeSet;
use tokio::sync::mpsc;

impl OutboundWorker {
    pub(super) async fn handle_assignment(
        &self,
        assignment: Assignment,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let assignment_id = assignment.assignment_id.clone();
        let attempt_id = assignment.attempt_id.clone();
        let admission = match self.validate_execution_support(&assignment) {
            Ok(()) => self.state.admit_async(assignment.clone()).await,
            Err(error) => Err(error),
        };
        let admitted = match admission {
            Ok(_) => true,
            Err(WorkerError::InvalidAssignment(error)) => {
                self.state
                    .enqueue_lifecycle_async(WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Invalid,
                        detail: error.to_string(),
                    })
                    .await?;
                false
            }
            Err(WorkerError::ConflictingAttempt(_)) => {
                self.state
                    .enqueue_lifecycle_async(WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Conflict,
                        detail: "attempt ID conflicts with locally admitted content".to_owned(),
                    })
                    .await?;
                false
            }
            Err(WorkerError::PolicyViolation(detail)) => {
                self.state
                    .enqueue_lifecycle_async(WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Policy,
                        detail,
                    })
                    .await?;
                false
            }
            Err(error) => return Err(error),
        };
        if admitted {
            let attempt = self
                .state
                .attempt_async(attempt_id.clone())
                .await?
                .ok_or_else(|| {
                    WorkerError::Protocol(format!("admitted attempt {attempt_id} is missing"))
                })?;
            match (attempt.phase, attempt.finished) {
                (LocalAttemptPhase::Running, _) => {
                    self.state.mark_running_async(attempt_id.clone()).await?;
                }
                (LocalAttemptPhase::Finished, Some(finished)) => {
                    self.state
                        .mark_finished_async(attempt_id.clone(), finished)
                        .await?;
                }
                (LocalAttemptPhase::Finished, None) => {
                    return Err(WorkerError::Protocol(format!(
                        "finished attempt {attempt_id} lacks terminal journal data"
                    )));
                }
                (LocalAttemptPhase::Accepted, _) => {}
            }
        }
        self.send_pending_outbox(
            connection_id,
            outbound,
            next_worker_sequence,
            acknowledged,
            delivered_message_ids,
        )
        .await?;
        if admitted {
            self.ensure_execution(&attempt_id).await?;
        }
        Ok(())
    }

    pub(super) fn validate_execution_support(
        &self,
        assignment: &Assignment,
    ) -> Result<(), WorkerError> {
        validate_assignment(assignment).map_err(WorkerError::InvalidAssignment)?;
        if self.admission_only {
            return Ok(());
        }
        let execution = assignment.execution.as_ref().ok_or_else(|| {
            WorkerError::Protocol("validated assignment lacks execution".to_owned())
        })?;
        let executor = ExecutionKind::try_from(execution.executor_kind)
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        let integration = self.execution.as_ref().ok_or_else(|| {
            WorkerError::PolicyViolation(format!(
                "no execution backend is attached for {}",
                executor.as_str_name()
            ))
        })?;
        if integration.backends.backend(executor).is_some() {
            Ok(())
        } else {
            Err(WorkerError::PolicyViolation(format!(
                "attached execution backend does not support {}",
                executor.as_str_name()
            )))
        }
    }

    pub(super) async fn handle_cancel(
        &self,
        cancel: alloyport_proto::v1::CancelAttempt,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let already_terminal = {
            let attempt = self
                .state
                .attempt_async(cancel.attempt_id.clone())
                .await?
                .ok_or_else(|| {
                    WorkerError::Protocol(format!(
                        "server cancelled unknown attempt {}",
                        cancel.attempt_id
                    ))
                })?;
            let already_terminal = attempt.phase == LocalAttemptPhase::Finished;
            let assignment_id = attempt.assignment.assignment_id;
            let attempt_id = attempt.assignment.attempt_id;
            self.state
                .enqueue_lifecycle_async(WorkerOutboxPayload::CancellationAcknowledged {
                    assignment_id,
                    attempt_id,
                    already_terminal,
                })
                .await?;
            already_terminal
        };

        if already_terminal {
            self.send_pending_outbox(
                connection_id,
                outbound,
                next_worker_sequence,
                acknowledged,
                delivered_message_ids,
            )
            .await?;
            return Ok(());
        }

        if self.execution.is_none() {
            {
                self.state
                    .mark_finished_async(
                        cancel.attempt_id.clone(),
                        StoredFinished {
                            outcome: AttemptOutcome::Cancelled,
                            exit_code: None,
                            elapsed_ms: 0,
                            receipt: None,
                            stdout: None,
                            stderr: None,
                            detail: cancel.reason.clone(),
                        },
                    )
                    .await?;
                self.state
                    .attempt_async(cancel.attempt_id.clone())
                    .await?
                    .and_then(|record| record.finished)
                    .ok_or_else(|| {
                        WorkerError::Protocol(
                            "cancelled attempt lacks terminal journal data".to_owned(),
                        )
                    })?;
            }
            return self
                .send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await;
        }

        let cancellation = self
            .ensure_execution(&cancel.attempt_id)
            .await?
            .ok_or_else(|| {
                WorkerError::Protocol(format!(
                    "non-terminal attempt {} did not start an executor",
                    cancel.attempt_id
                ))
            })?;

        // Put the durable acknowledgement on the wire before making cancellation visible to the
        // executor. Even an immediate fake completion therefore cannot overtake the ACK.
        self.send_pending_outbox(
            connection_id,
            outbound,
            next_worker_sequence,
            acknowledged,
            delivered_message_ids,
        )
        .await?;
        cancellation.cancel();

        Ok(())
    }
}
