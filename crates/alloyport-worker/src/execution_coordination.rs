//! Execution task registration and live update coordination for admitted attempts.

use super::{ExecutionUpdate, OutboundWorker, WorkerError, WorkerState};
use crate::execution_backend::{
    BackendError, BackendExecutionRequest, ExecutionBackend, ExecutionObserver,
};
use crate::executor::{
    ArtifactPublisher, CancellationToken, ExecutionObservation, ExecutionStream,
};
use crate::journal::LocalAttemptPhase;
use alloyport_proto::MAX_OUTPUT_PREVIEW_CHUNK_BYTES;
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
        let permits = Arc::clone(&integration.permits);
        tokio::spawn(async move {
            let result = match permits.acquire_owned().await {
                Ok(_permit) => run_registered_execution(
                    backend.as_ref(),
                    state.as_ref(),
                    &attempt_id,
                    &cancellation_for_task,
                    artifact_input.as_deref(),
                    artifact_publisher.as_deref(),
                    &updates,
                )
                .await
                .map(|_| ()),
                Err(_) => Err(BackendError::retryable(
                    "worker execution concurrency gate closed",
                )),
            };
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
                for (relative_offset, payload) in bounded_preview_chunks(&chunk.bytes) {
                    Self::send_ephemeral(
                        outbound,
                        next_worker_sequence,
                        acknowledged,
                        worker_to_server::Message::OutputChunk(OutputChunk {
                            attempt_id: attempt_id.clone(),
                            stream: match chunk.stream {
                                ExecutionStream::Stdout => OutputStream::Stdout,
                                ExecutionStream::Stderr => OutputStream::Stderr,
                            }
                            .into(),
                            byte_offset: chunk.byte_offset.saturating_add(relative_offset),
                            display_sanitized: std::str::from_utf8(payload).is_err(),
                            payload: payload.to_vec(),
                        }),
                    )
                    .await?;
                }
                Ok(())
            }
            ExecutionUpdate::Completed { attempt_id, result } => {
                result.map_err(|error| {
                    WorkerError::Backend(error.with_context(format_args!("attempt {attempt_id}")))
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

fn bounded_preview_chunks(bytes: &[u8]) -> impl Iterator<Item = (u64, &[u8])> {
    let valid_text = std::str::from_utf8(bytes).ok();
    let mut offset = 0_usize;
    std::iter::from_fn(move || {
        if offset >= bytes.len() {
            return None;
        }
        let mut end = offset
            .saturating_add(MAX_OUTPUT_PREVIEW_CHUNK_BYTES)
            .min(bytes.len());
        if let Some(text) = valid_text {
            while end > offset && !text.is_char_boundary(end) {
                end -= 1;
            }
        }
        let relative_offset = u64::try_from(offset).unwrap_or(u64::MAX);
        let payload = &bytes[offset..end];
        offset = end;
        Some((relative_offset, payload))
    })
}

async fn run_registered_execution(
    backend: &dyn ExecutionBackend,
    state: &WorkerState,
    attempt_id: &str,
    cancellation: &CancellationToken,
    input_provider: Option<&dyn crate::artifact_input::ArtifactInputProvider>,
    publisher: Option<&dyn ArtifactPublisher>,
    updates: &broadcast::Sender<ExecutionUpdate>,
) -> Result<crate::executor::ExecutionRun, BackendError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_previews_are_split_without_gaps_at_the_wire_limit() {
        let bytes = vec![7; MAX_OUTPUT_PREVIEW_CHUNK_BYTES * 2 + 1];
        let chunks = bounded_preview_chunks(&bytes).collect::<Vec<_>>();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].0, 0);
        assert_eq!(chunks[0].1.len(), MAX_OUTPUT_PREVIEW_CHUNK_BYTES);
        assert_eq!(
            chunks[1].0,
            u64::try_from(MAX_OUTPUT_PREVIEW_CHUNK_BYTES).expect("wire limit fits u64")
        );
        assert_eq!(chunks[1].1.len(), MAX_OUTPUT_PREVIEW_CHUNK_BYTES);
        assert_eq!(
            chunks[2].0,
            u64::try_from(MAX_OUTPUT_PREVIEW_CHUNK_BYTES * 2).expect("wire limit fits u64")
        );
        assert_eq!(chunks[2].1, &[7]);
    }

    #[test]
    fn valid_utf8_preview_is_not_split_inside_a_character() {
        let mut bytes = vec![b'a'; MAX_OUTPUT_PREVIEW_CHUNK_BYTES - 1];
        bytes.extend_from_slice("€".as_bytes());

        let chunks = bounded_preview_chunks(&bytes).collect::<Vec<_>>();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].1.len(), MAX_OUTPUT_PREVIEW_CHUNK_BYTES - 1);
        assert_eq!(
            chunks[1].0,
            u64::try_from(MAX_OUTPUT_PREVIEW_CHUNK_BYTES - 1).expect("wire limit fits u64")
        );
        assert!(std::str::from_utf8(chunks[0].1).is_ok());
        assert_eq!(std::str::from_utf8(chunks[1].1), Ok("€"));
    }
}
