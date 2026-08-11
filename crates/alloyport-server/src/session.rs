//! Worker connection registration, replay, stream consumption, and disconnect handling.

use super::{
    ATTEMPT_LEASE_MS, ConnectionRegistration, HEARTBEAT_INTERVAL_MS, OUTBOX_ORPHAN_RETENTION_MS,
    Ordering, PROTOCOL_MAJOR, PROTOCOL_MINOR, RepositoryError, ResolvedConnectionIdentity,
    ServerToWorker, ServerWelcome, Status, WorkerControlService, WorkerHello, WorkerRecord,
    WorkerToServer, hello_to_registration, mpsc, repository_status, server_to_worker,
};
use tokio_stream::StreamExt;
use tonic::Streaming;

impl WorkerControlService {
    /// Registers one validated hello and reconstructs replayable outbound state.
    ///
    /// # Errors
    ///
    /// Returns a repository error when durable connection or replay state cannot be committed.
    pub(super) async fn register(
        &self,
        hello: WorkerHello,
        sender: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) -> Result<(String, Vec<ServerToWorker>), RepositoryError> {
        let number = self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let connection_id = format!("connection-{number}");
        let worker_id = hello.worker_id.clone();
        let negotiated_protocol_minor = hello.protocol_minor.min(PROTOCOL_MINOR);
        let now_ms = self.clock.now_unix_ms();
        let attempts = self.repositories.attempts.clone();
        let outbox = self.repositories.outbox.clone();
        let connections = self.repositories.connections.clone();
        let assignments = self.repositories.assignments.clone();
        let registration = hello_to_registration(&hello);
        let connection = ConnectionRegistration {
            connection_id: connection_id.clone(),
            worker_id: worker_id.clone(),
            instance_id: hello.instance_id.clone(),
            connected_at_ms: now_ms,
        };
        let pending = self
            .persistence
            .run(move || {
                attempts.expire_leases(now_ms)?;
                outbox.prune_orphaned_server_frames(
                    now_ms.saturating_sub(OUTBOX_ORPHAN_RETENTION_MS),
                )?;
                connections.register_worker(&registration, &connection)?;
                assignments.replayable_assignments(&registration.worker_id)
            })
            .await
            .map_err(RepositoryError::from)??;

        {
            let mut state = self.state.lock().await;
            state.workers.insert(
                worker_id.clone(),
                WorkerRecord {
                    hello,
                    connection_id: connection_id.clone(),
                    connected: true,
                    last_worker_sequence: 1,
                    last_server_sequence_acknowledged: 0,
                    next_server_sequence: 2,
                    sender,
                },
            );
        }

        let mut messages = vec![ServerToWorker {
            sequence: 1,
            acknowledges_worker_through: 1,
            message_id: String::new(),
            message: Some(server_to_worker::Message::Welcome(ServerWelcome {
                connection_id: connection_id.clone(),
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: negotiated_protocol_minor,
                heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
                attempt_lease_ms: ATTEMPT_LEASE_MS,
            })),
        }];
        for assignment in pending {
            let cancellation_reason = assignment.cancellation_reason.clone();
            if let Some((_, message)) = self
                .prepare_assignment(&worker_id, &assignment.contract.attempt_id)
                .await?
            {
                messages.push(message);
            }
            if let Some(reason) = cancellation_reason
                && let Some((_, message)) = self
                    .prepare_cancel(&worker_id, &assignment.contract.attempt_id, &reason)
                    .await?
            {
                messages.push(message);
            }
        }
        Ok((connection_id, messages))
    }

    async fn disconnect(&self, worker_id: &str, connection_id: &str) {
        let repository = self.repositories.connections.clone();
        let persisted_connection_id = connection_id.to_owned();
        let now_ms = self.clock.now_unix_ms();
        let _ = self
            .persistence
            .run(move || repository.disconnect(&persisted_connection_id, now_ms))
            .await;
        let mut state = self.state.lock().await;
        if let Some(worker) = state.workers.get_mut(worker_id)
            && worker.connection_id == connection_id
        {
            worker.connected = false;
        }
    }

    pub(super) async fn consume_stream(
        self,
        worker_id: String,
        connection_id: String,
        authenticated_identity: Option<ResolvedConnectionIdentity>,
        mut inbound: Streaming<WorkerToServer>,
        outbound: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) {
        loop {
            match inbound.next().await {
                Some(Ok(frame)) => {
                    if let Some(identity) = authenticated_identity.as_ref()
                        && let Some(resolver) = self.identity_resolver.as_ref()
                        && let Err(status) = resolver.revalidate(identity).await
                    {
                        let _ = outbound.send(Err(status)).await;
                        break;
                    }
                    match self.ingest(&worker_id, &connection_id, frame).await {
                        Ok(true) => {
                            match self.prepare_transport_ack(&worker_id, &connection_id).await {
                                Ok(Some((sender, message))) => {
                                    if sender.send(Ok(message)).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(error) => {
                                    let _ = outbound.send(Err(repository_status(error))).await;
                                    break;
                                }
                            }
                        }
                        Ok(false) => {}
                        Err(status) => {
                            let _ = outbound.send(Err(status)).await;
                            break;
                        }
                    }
                }
                Some(Err(status)) => {
                    let _ = outbound.send(Err(status)).await;
                    break;
                }
                None => break,
            }
        }
        self.disconnect(&worker_id, &connection_id).await;
    }
}
