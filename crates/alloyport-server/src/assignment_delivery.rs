//! Durable assignment/cancellation frame preparation at the connection delivery boundary.

use super::{
    ATTEMPT_LEASE_MS, AssignmentDeliveryPreparation, CancelAttempt, Ordering, RepositoryError,
    ServerFrameKind, ServerOutboxFrame, ServerToWorker, Status, WorkerControlService,
    contract_to_assignment, mpsc, server_to_worker,
};

impl WorkerControlService {
    pub(super) async fn prepare_assignment(
        &self,
        worker_id: &str,
        attempt_id: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let _delivery = self.delivery.lock().await;
        let Some((sender, connection_id, sequence, last_worker_sequence, last_server_acknowledged)) =
            ({
                let state = self.state.lock().await;
                state.workers.get(worker_id).and_then(|worker| {
                    worker.connected.then(|| {
                        (
                            worker.sender.clone(),
                            worker.connection_id.clone(),
                            worker.next_server_sequence,
                            worker.last_worker_sequence,
                            worker.last_server_sequence_acknowledged,
                        )
                    })
                })
            })
        else {
            return Ok(None);
        };
        let lease_number = self.lease_counter.fetch_add(1, Ordering::Relaxed);
        let lease_id = format!("lease-{lease_number}");
        let now_ms = self.clock.now_unix_ms();
        let message_id = format!("assignment:{attempt_id}");
        let repository = self.repositories.assignments.clone();
        let persisted_worker_id = worker_id.to_owned();
        let persisted_attempt_id = attempt_id.to_owned();
        let persisted_message_id = message_id.clone();
        let expected_connection_id = connection_id.clone();
        let contract = self
            .persistence
            .run(move || {
                repository.prepare_assignment_delivery(&AssignmentDeliveryPreparation {
                    frame: ServerOutboxFrame {
                        connection_id,
                        sequence,
                        message_id: persisted_message_id,
                        worker_id: persisted_worker_id,
                        kind: ServerFrameKind::Assignment,
                        attempt_id: Some(persisted_attempt_id),
                    },
                    lease_id,
                    last_worker_sequence,
                    last_server_acknowledged_by_worker: last_server_acknowledged,
                    now_ms,
                    lease_duration_ms: ATTEMPT_LEASE_MS,
                })
            })
            .await
            .map_err(|error| RepositoryError::Storage(Box::new(error)))??;
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if worker.connection_id != expected_connection_id || worker.next_server_sequence != sequence
        {
            return Ok(None);
        }
        worker.next_server_sequence += 1;
        Ok(Some((
            sender,
            ServerToWorker {
                sequence,
                acknowledges_worker_through: last_worker_sequence,
                message_id,
                message: Some(server_to_worker::Message::Assignment(
                    contract_to_assignment(&contract),
                )),
            },
        )))
    }

    pub(super) async fn mark_send_failed(&self, worker_id: &str) {
        let mut state = self.state.lock().await;
        if let Some(worker) = state.workers.get_mut(worker_id) {
            worker.connected = false;
        }
    }

    pub(super) async fn prepare_cancel(
        &self,
        worker_id: &str,
        attempt_id: &str,
        reason: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let _delivery = self.delivery.lock().await;
        let Some((sender, connection_id, sequence, last_worker_sequence, last_server_acknowledged)) =
            ({
                let state = self.state.lock().await;
                state.workers.get(worker_id).and_then(|worker| {
                    worker.connected.then(|| {
                        (
                            worker.sender.clone(),
                            worker.connection_id.clone(),
                            worker.next_server_sequence,
                            worker.last_worker_sequence,
                            worker.last_server_sequence_acknowledged,
                        )
                    })
                })
            })
        else {
            return Ok(None);
        };
        let now_ms = self.clock.now_unix_ms();
        let message_id = format!("cancel:{attempt_id}");
        let outbox = self.repositories.outbox.clone();
        let connections = self.repositories.connections.clone();
        let persisted_connection_id = connection_id.clone();
        let persisted_message_id = message_id.clone();
        let persisted_worker_id = worker_id.to_owned();
        let persisted_attempt_id = attempt_id.to_owned();
        self.persistence
            .run(move || {
                outbox.record_server_frame(
                    &ServerOutboxFrame {
                        connection_id: persisted_connection_id.clone(),
                        sequence,
                        message_id: persisted_message_id,
                        worker_id: persisted_worker_id,
                        kind: ServerFrameKind::Cancel,
                        attempt_id: Some(persisted_attempt_id),
                    },
                    now_ms,
                )?;
                connections.update_connection_sequences(
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
                message_id,
                message: Some(server_to_worker::Message::Cancel(CancelAttempt {
                    attempt_id: attempt_id.to_owned(),
                    reason: reason.to_owned(),
                })),
            },
        )))
    }
}
