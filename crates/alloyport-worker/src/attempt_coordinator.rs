//! Attempt admission, cancellation, execution, and durable outbox coordination.

use super::{ExecutionUpdate, OutboundWorker, WorkerError, WorkerState};
use crate::execution_backend::{BackendExecutionRequest, ExecutionBackend, ExecutionObserver};
use crate::executor::{
    ArtifactPublisher, CancellationToken, ExecutionObservation, ExecutionStream,
    terminal_reference_intents,
};
use crate::journal::{LocalAttemptPhase, StoredFinished, WorkerOutboxPayload};
use crate::wire_mapping::outbox_to_wire;
use alloyport_proto::v1::{
    Assignment, AttemptOutcome, ExecutorKind, OutputChunk, OutputStream, RejectionReason,
    WorkerToServer, worker_to_server,
};
use alloyport_proto::validate_assignment;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

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
            Ok(()) => self.state.lock().await.admit(&assignment),
            Err(error) => Err(error),
        };
        let admitted = match admission {
            Ok(_) => true,
            Err(WorkerError::InvalidAssignment(error)) => {
                self.state.lock().await.enqueue_lifecycle(
                    WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Invalid.into(),
                        detail: error.to_string(),
                    },
                )?;
                false
            }
            Err(WorkerError::ConflictingAttempt(_)) => {
                self.state.lock().await.enqueue_lifecycle(
                    WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Conflict.into(),
                        detail: "attempt ID conflicts with locally admitted content".to_owned(),
                    },
                )?;
                false
            }
            Err(WorkerError::PolicyViolation(detail)) => {
                self.state.lock().await.enqueue_lifecycle(
                    WorkerOutboxPayload::AssignmentRejected {
                        assignment_id: assignment_id.clone(),
                        attempt_id: attempt_id.clone(),
                        reason: RejectionReason::Policy.into(),
                        detail,
                    },
                )?;
                false
            }
            Err(error) => return Err(error),
        };
        if admitted {
            let state = self.state.lock().await;
            let attempt = state.attempt(&attempt_id)?.ok_or_else(|| {
                WorkerError::Protocol(format!("admitted attempt {attempt_id} is missing"))
            })?;
            match (attempt.phase, attempt.finished) {
                (LocalAttemptPhase::Running, _) => state.mark_running(&attempt_id)?,
                (LocalAttemptPhase::Finished, Some(finished)) => {
                    state.mark_finished(&attempt_id, &finished)?;
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
        let executor =
            ExecutorKind::try_from(execution.executor_kind).unwrap_or(ExecutorKind::Unspecified);
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
            let state = self.state.lock().await;
            let attempt = state.attempt(&cancel.attempt_id)?.ok_or_else(|| {
                WorkerError::Protocol(format!(
                    "server cancelled unknown attempt {}",
                    cancel.attempt_id
                ))
            })?;
            let already_terminal = attempt.phase == LocalAttemptPhase::Finished;
            let assignment_id = attempt.assignment.assignment_id;
            state.enqueue_lifecycle(WorkerOutboxPayload::CancellationAcknowledged {
                assignment_id: assignment_id.clone(),
                attempt_id: cancel.attempt_id.clone(),
                already_terminal,
            })?;
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
                let state = self.state.lock().await;
                state.mark_finished(
                    &cancel.attempt_id,
                    &StoredFinished {
                        outcome: AttemptOutcome::Cancelled.into(),
                        exit_code: None,
                        elapsed_ms: 0,
                        receipt: None,
                        stdout: None,
                        stderr: None,
                        detail: cancel.reason.clone(),
                    },
                )?;
                state
                    .attempt(&cancel.attempt_id)?
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

    async fn ensure_execution(
        &self,
        attempt_id: &str,
    ) -> Result<Option<CancellationToken>, WorkerError> {
        let Some(integration) = self.execution.as_ref() else {
            return Ok(None);
        };
        let attempt = self
            .state
            .lock()
            .await
            .attempt(attempt_id)?
            .ok_or_else(|| WorkerError::Protocol(format!("attempt {attempt_id} is missing")))?;
        if attempt.phase == LocalAttemptPhase::Finished {
            return Ok(None);
        }
        let executor = ExecutorKind::try_from(attempt.assignment.execution.executor_kind)
            .unwrap_or(ExecutorKind::Unspecified);
        let backend = integration.backends.backend(executor).ok_or_else(|| {
            WorkerError::Execution(format!(
                "attached runtime does not support executor kind {}",
                executor.as_str_name()
            ))
        })?;

        let mut active = integration.active.lock().await;
        if let Some(cancellation) = active.get(attempt_id) {
            return Ok(Some(cancellation.clone()));
        }
        let cancellation = CancellationToken::new();
        active.insert(attempt_id.to_owned(), cancellation.clone());
        drop(active);

        let attempt_id = attempt_id.to_owned();
        let cancellation_for_task = cancellation.clone();
        let state = self.state.lock().await.clone();
        let integration = Arc::clone(integration);
        let artifact_downloader = self.artifact_downloader.clone();
        let artifact_publisher = self.artifact_publisher.clone();
        let updates = self.execution_updates.clone();
        tokio::spawn(async move {
            let result = run_registered_execution(
                backend.as_ref(),
                &state,
                &attempt_id,
                &cancellation_for_task,
                artifact_downloader.as_deref(),
                artifact_publisher.as_deref(),
                &updates,
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string());
            integration.active.lock().await.remove(&attempt_id);
            let _ = updates.send(ExecutionUpdate::Completed { attempt_id, result });
        });
        Ok(Some(cancellation))
    }

    async fn handle_execution_update(
        &self,
        update: ExecutionUpdate,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        match update {
            ExecutionUpdate::Observation {
                attempt_id,
                observation: ExecutionObservation::Started,
            } => {
                let _ = attempt_id;
                self.send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
            ExecutionUpdate::Observation {
                attempt_id,
                observation: ExecutionObservation::Output(chunk),
            } => {
                Self::send_ephemeral(
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    worker_to_server::Message::OutputChunk(OutputChunk {
                        attempt_id,
                        stream: match chunk.stream {
                            ExecutionStream::Stdout => OutputStream::Stdout,
                            ExecutionStream::Stderr => OutputStream::Stderr,
                        }
                        .into(),
                        byte_offset: chunk.byte_offset,
                        display_sanitized: std::str::from_utf8(&chunk.bytes).is_err(),
                        payload: chunk.bytes,
                    }),
                )
                .await
            }
            ExecutionUpdate::Completed { attempt_id, result } => {
                result.map_err(|detail| {
                    WorkerError::Execution(format!("attempt {attempt_id}: {detail}"))
                })?;
                self.send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
        }
    }

    pub(super) async fn handle_execution_receive(
        &self,
        update: Result<ExecutionUpdate, broadcast::error::RecvError>,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        match update {
            Ok(update) => {
                self.handle_execution_update(
                    update,
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // Output previews are explicitly best effort. Durable lifecycle rows are recovered
                // on the next observation, heartbeat, or reconnect.
                self.send_pending_outbox(
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await
            }
            Err(broadcast::error::RecvError::Closed) => Err(WorkerError::Protocol(
                "execution update channel closed".to_owned(),
            )),
        }
    }

    pub(super) async fn send_ephemeral(
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        message: worker_to_server::Message,
    ) -> Result<(), WorkerError> {
        let sequence = *next_worker_sequence;
        *next_worker_sequence += 1;
        outbound
            .send(WorkerToServer {
                sequence,
                acknowledges_server_through,
                message_id: String::new(),
                message: Some(message),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)
    }

    pub(super) async fn publish_pending_terminal_artifacts(&self) -> Result<(), WorkerError> {
        let Some(publisher) = self.artifact_publisher.as_ref() else {
            return Ok(());
        };
        let pending = self.state.lock().await.pending_outbox()?;
        for entry in pending {
            let WorkerOutboxPayload::ExecutionFinished {
                attempt_id,
                finished,
                ..
            } = entry.payload
            else {
                continue;
            };
            publisher
                .publish(&terminal_reference_intents(&attempt_id, &finished))
                .await
                .map_err(WorkerError::Execution)?;
        }
        Ok(())
    }

    pub(super) async fn send_pending_outbox(
        &self,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let pending = self.state.lock().await.pending_outbox()?;
        for entry in pending {
            if delivered_message_ids.contains(&entry.message_id) {
                continue;
            }
            let sequence = *next_worker_sequence;
            self.state
                .lock()
                .await
                .record_delivery(connection_id, sequence, &entry.message_id)?;
            *next_worker_sequence += 1;
            delivered_message_ids.insert(entry.message_id.clone());
            outbound
                .send(WorkerToServer {
                    sequence,
                    acknowledges_server_through,
                    message_id: entry.message_id,
                    message: Some(outbox_to_wire(entry.payload)),
                })
                .await
                .map_err(|_| WorkerError::StreamClosed)?;
        }
        Ok(())
    }

    pub(super) async fn available_slots(&self) -> Result<u32, WorkerError> {
        let active = u32::try_from(self.state.lock().await.attempt_count()?).unwrap_or(u32::MAX);
        Ok(self.hello.capabilities.as_ref().map_or(0, |capabilities| {
            capabilities.max_concurrency.saturating_sub(active)
        }))
    }
}

async fn run_registered_execution(
    backend: &dyn ExecutionBackend,
    state: &WorkerState,
    attempt_id: &str,
    cancellation: &CancellationToken,
    downloader: Option<&crate::artifact_download::RemoteArtifactDownloader>,
    publisher: Option<&dyn ArtifactPublisher>,
    updates: &broadcast::Sender<ExecutionUpdate>,
) -> Result<crate::executor::ExecutionRun, crate::executor::ExecutionRuntimeError> {
    let observed_attempt_id = attempt_id.to_owned();
    let observed_updates = updates.clone();
    let observer: ExecutionObserver = Arc::new(move |observation| {
        let _ = observed_updates.send(ExecutionUpdate::Observation {
            attempt_id: observed_attempt_id.clone(),
            observation,
        });
    });
    backend
        .execute(BackendExecutionRequest {
            state,
            attempt_id,
            cancellation,
            downloader,
            publisher,
            observer,
        })
        .await
}
