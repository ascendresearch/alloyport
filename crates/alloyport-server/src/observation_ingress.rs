//! Worker control-frame sequencing, durable acknowledgement, and observation dispatch.

use super::{
    ControlAcknowledgement, RepositoryError, ServerToWorker, Status, WorkerControlService,
    WorkerToServer, expected_worker_message_id, mpsc, repository_status, server_to_worker,
    validate_worker_acknowledgement,
};

impl WorkerControlService {
    pub(super) async fn ingest(
        &self,
        worker_id: &str,
        connection_id: &str,
        frame: WorkerToServer,
    ) -> Result<bool, Status> {
        let state = self.state.lock().await;
        let worker = state
            .workers
            .get(worker_id)
            .ok_or_else(|| Status::failed_precondition("worker is not registered"))?;
        if worker.connection_id != connection_id || !worker.connected {
            return Err(Status::aborted("worker connection was superseded"));
        }
        if frame.sequence != worker.last_worker_sequence + 1 {
            return Err(Status::invalid_argument(format!(
                "worker sequence gap: expected {}, got {}",
                worker.last_worker_sequence + 1,
                frame.sequence
            )));
        }
        let sent_server_through = worker.next_server_sequence.saturating_sub(1);
        validate_worker_acknowledgement(
            frame.acknowledges_server_through,
            worker.last_server_sequence_acknowledged,
            sent_server_through,
        )?;

        let durable_message_id = expected_worker_message_id(frame.message.as_ref());
        let supports_durable_message_ids = worker.hello.protocol_minor >= 2;
        if supports_durable_message_ids {
            if let Some(expected) = durable_message_id.as_ref()
                && frame.message_id != *expected
            {
                return Err(Status::invalid_argument(format!(
                    "worker message ID must be {expected}"
                )));
            }
            if durable_message_id.is_none() && !frame.message_id.is_empty() {
                return Err(Status::invalid_argument(
                    "ephemeral worker frame cannot carry a message ID",
                ));
            }
        }

        drop(state);

        let now_ms = self.clock.now_unix_ms();
        let service = self.clone();
        let observed_worker_id = worker_id.to_owned();
        let message = frame.message.clone();
        self.persistence
            .run(move || service.observe_message(&observed_worker_id, message, now_ms))
            .await
            .map_err(|error| Status::internal(error.to_string()))??;

        let mut state = self.state.lock().await;
        let worker = state
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| Status::aborted("worker connection was superseded"))?;
        if worker.connection_id != connection_id
            || !worker.connected
            || frame.sequence != worker.last_worker_sequence + 1
        {
            return Err(Status::aborted("worker connection was superseded"));
        }
        worker.last_worker_sequence = frame.sequence;
        worker.last_server_sequence_acknowledged = frame.acknowledges_server_through;
        let last_server_sequence = worker.next_server_sequence.saturating_sub(1);
        drop(state);

        let outbox = self.repositories.outbox.clone();
        let connections = self.repositories.connections.clone();
        let persisted_connection_id = connection_id.to_owned();
        self.persistence
            .run(move || {
                outbox.compact_server_frames(
                    &persisted_connection_id,
                    frame.acknowledges_server_through,
                    now_ms,
                )?;
                connections.update_connection_sequences(
                    &persisted_connection_id,
                    frame.sequence,
                    last_server_sequence,
                    frame.acknowledges_server_through,
                    now_ms,
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(repository_status)?;
        Ok(supports_durable_message_ids && durable_message_id.is_some())
    }

    pub(super) async fn prepare_transport_ack(
        &self,
        worker_id: &str,
        connection_id: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let _delivery = self.delivery.lock().await;
        let Some((sender, sequence, last_worker_sequence, last_server_acknowledged)) = ({
            let state = self.state.lock().await;
            state.workers.get(worker_id).and_then(|worker| {
                (worker.connected && worker.connection_id == connection_id).then(|| {
                    (
                        worker.sender.clone(),
                        worker.next_server_sequence,
                        worker.last_worker_sequence,
                        worker.last_server_sequence_acknowledged,
                    )
                })
            })
        }) else {
            return Ok(None);
        };
        let repository = self.repositories.connections.clone();
        let persisted_connection_id = connection_id.to_owned();
        let now_ms = self.clock.now_unix_ms();
        self.persistence
            .run(move || {
                repository.update_connection_sequences(
                    &persisted_connection_id,
                    last_worker_sequence,
                    sequence,
                    last_server_acknowledged,
                    now_ms,
                )
            })
            .await
            .map_err(|error| RepositoryError::Storage(Box::new(error)))??;
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if worker.connection_id != connection_id || worker.next_server_sequence != sequence {
            return Ok(None);
        }
        worker.next_server_sequence += 1;
        Ok(Some((
            sender,
            ServerToWorker {
                sequence,
                acknowledges_worker_through: last_worker_sequence,
                message_id: String::new(),
                message: Some(server_to_worker::Message::Acknowledgement(
                    ControlAcknowledgement {},
                )),
            },
        )))
    }
}
