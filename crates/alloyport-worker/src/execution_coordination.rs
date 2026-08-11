//! Execution task registration and live update coordination for admitted attempts.

use super::{ExecutionUpdate, OutboundWorker, WorkerError, WorkerState};
use crate::execution_backend::{BackendExecutionRequest, ExecutionBackend, ExecutionObserver};
use crate::executor::{
    ArtifactPublisher, CancellationToken, ExecutionObservation, ExecutionStream,
};
use crate::journal::LocalAttemptPhase;
use alloyport_proto::v1::{OutputChunk, OutputStream, WorkerToServer, worker_to_server};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

impl OutboundWorker {
    pub(super) async fn ensure_execution(
        &self,
        attempt_id: &str,
    ) -> Result<Option<CancellationToken>, WorkerError> {
        let Some(integration) = self.execution.as_ref() else {
            return Ok(None);
        };
        let attempt = self
            .state
            .attempt_async(attempt_id.to_owned())
            .await?
            .ok_or_else(|| WorkerError::Protocol(format!("attempt {attempt_id} is missing")))?;
        if attempt.phase == LocalAttemptPhase::Finished {
            return Ok(None);
        }
        let executor = attempt.assignment.execution.executor_kind;
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
        let state = Arc::clone(&self.state);
        let integration = Arc::clone(integration);
        let artifact_input = self.artifact_input.clone();
        let artifact_publisher = self.artifact_publisher.clone();
        let updates = self.execution_updates.clone();
        tokio::spawn(async move {
            let result = run_registered_execution(
                backend.as_ref(),
                state.as_ref(),
                &attempt_id,
                &cancellation_for_task,
                artifact_input.as_deref(),
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
}

async fn run_registered_execution(
    backend: &dyn ExecutionBackend,
    state: &WorkerState,
    attempt_id: &str,
    cancellation: &CancellationToken,
    input_provider: Option<&dyn crate::artifact_input::ArtifactInputProvider>,
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
            input_provider,
            publisher,
            observer,
        })
        .await
}
