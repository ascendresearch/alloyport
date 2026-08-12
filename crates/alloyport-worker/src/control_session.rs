//! Outbound gRPC control-session lifecycle and frame validation.

use super::{DEFAULT_HEARTBEAT_INTERVAL, OutboundWorker, WorkerError};
use crate::journal::LocalAttemptPhase;
use crate::wire_mapping::expected_server_message_id;
use alloyport_core::DeviceHealth;
use alloyport_proto::v1::worker_control_client::WorkerControlClient;
use alloyport_proto::v1::{
    DeviceLease as WireDeviceLease, DeviceObservation as WireDeviceObservation, Heartbeat,
    ServerToWorker, WorkerHealth, WorkerToServer, server_to_worker, worker_to_server,
};
use std::collections::BTreeSet;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

impl OutboundWorker {
    /// Opens one gRPC session and processes messages until the stream closes.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] on transport, framing, validation or identity failures. A supervisor
    /// may reconnect this same value; its in-process attempt map is retained.
    pub async fn run_session(&self) -> Result<(), WorkerError> {
        self.publish_pending_terminal_artifacts().await?;
        self.retry_terminal_cuda_cleanup().await?;
        self.run_control_session().await
    }

    async fn retry_terminal_cuda_cleanup(&self) -> Result<(), WorkerError> {
        let Some(integration) = self.execution.as_ref() else {
            return Ok(());
        };
        let state = std::sync::Arc::clone(&self.state);
        let terminal_attempts = state
            .attempts_async()
            .await?
            .into_iter()
            .filter(|attempt| attempt.phase == LocalAttemptPhase::Finished)
            .map(|attempt| {
                (
                    attempt.assignment.execution.executor_kind,
                    attempt.assignment.attempt_id,
                )
            })
            .collect::<Vec<_>>();
        for (executor, attempt_id) in terminal_attempts {
            let Some(backend) = integration.backends.backend(executor) else {
                continue;
            };
            // Cleanup is deliberately best effort here: a stale container cannot prevent durable
            // terminal outbox delivery. A later session retries the same idempotent removal.
            let _ = backend.retry_terminal_cleanup(&state, &attempt_id).await;
        }
        Ok(())
    }

    async fn run_control_session(&self) -> Result<(), WorkerError> {
        let channel = self.endpoint.clone().connect().await?;
        let mut client = WorkerControlClient::new(channel);
        let (outbound, receiver) = mpsc::channel(64);
        let mut execution_updates = self.execution_updates.subscribe();

        let mut hello = self.hello.clone();
        hello.active_attempts = self.state.active_attempts_async().await?;
        outbound
            .send(WorkerToServer {
                sequence: 1,
                acknowledges_server_through: 0,
                message_id: String::new(),
                message: Some(worker_to_server::Message::Hello(hello)),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)?;

        let response = client
            .open_control_stream(Request::new(ReceiverStream::new(receiver)))
            .await?;
        let mut inbound = response.into_inner();
        let welcome_frame = inbound.message().await?.ok_or(WorkerError::StreamClosed)?;
        Self::validate_server_frame(&welcome_frame, 0, 0, 1, false)?;
        let (connection_id, negotiated_protocol_minor) = self.welcome_identity(&welcome_frame)?;
        let mut heartbeat = tokio::time::interval(DEFAULT_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut next_worker_sequence = 2;
        let mut last_server_sequence = welcome_frame.sequence;
        let mut last_worker_sequence_acknowledged = welcome_frame.acknowledges_worker_through;
        let mut delivered_message_ids = BTreeSet::new();
        let require_message_ids = negotiated_protocol_minor >= 2;
        self.resume_outbox_delivery(
            &connection_id,
            &outbound,
            &mut next_worker_sequence,
            last_server_sequence,
            last_worker_sequence_acknowledged,
            &mut delivered_message_ids,
        )
        .await?;

        loop {
            tokio::select! {
                incoming = inbound.message() => {
                    let message = incoming?.ok_or(WorkerError::StreamClosed)?;
                    Self::validate_server_frame(
                        &message,
                        last_server_sequence,
                        last_worker_sequence_acknowledged,
                        next_worker_sequence - 1,
                        require_message_ids,
                    )?;
                    let server_sequence = message.sequence;
                    let acknowledges_worker_through = message.acknowledges_worker_through;
                    if self.handle_server_message(
                        message,
                        &connection_id,
                        &outbound,
                        &mut next_worker_sequence,
                        server_sequence,
                        &mut delivered_message_ids,
                    ).await? {
                        return Ok(());
                    }
                    self.state.acknowledge_outbox_async(
                        connection_id.clone(),
                        acknowledges_worker_through,
                    ).await?;
                    last_server_sequence = server_sequence;
                    last_worker_sequence_acknowledged = acknowledges_worker_through;
                }
                _ = heartbeat.tick() => {
                    Self::send_ephemeral(
                        &outbound,
                        &mut next_worker_sequence,
                        last_server_sequence,
                        worker_to_server::Message::Heartbeat(self.build_heartbeat().await?),
                    ).await?;
                }
                update = execution_updates.recv(), if self.execution.is_some() => {
                    self.handle_execution_receive(
                        update,
                        &connection_id,
                        &outbound,
                        &mut next_worker_sequence,
                        last_server_sequence,
                        &mut delivered_message_ids,
                    ).await?;
                }
            }
        }
    }

    pub(super) async fn build_heartbeat(&self) -> Result<Heartbeat, WorkerError> {
        let active_attempts = self.state.active_attempts_async().await?;
        let (device_snapshot, device_probe_failed) = match self.device_status.as_ref() {
            Some(provider) => match provider.snapshot().await {
                Ok(snapshot) => (snapshot, false),
                Err(_) => (crate::device::DeviceSnapshot::default(), true),
            },
            None => (crate::device::DeviceSnapshot::default(), false),
        };
        let health = if !device_probe_failed
            && (self.device_status.is_none()
                || (!device_snapshot.devices.is_empty()
                    && device_snapshot
                        .devices
                        .iter()
                        .all(|device| device.health == DeviceHealth::Ready)))
        {
            WorkerHealth::Ready
        } else {
            WorkerHealth::Degraded
        };
        let devices = device_snapshot
            .devices
            .into_iter()
            .map(|device| WireDeviceObservation {
                device_id: device.device_id,
                health: i32::from(device.health),
                process_count: device.process_count,
                utilization_percent: device.utilization_percent,
                memory_used_bytes: device.memory_used_bytes,
                memory_total_bytes: device.memory_total_bytes,
                temperature_millicelsius: device.temperature_millicelsius,
                power_milliwatts: device.power_milliwatts,
                observed_at_ms: device.observed_at_ms,
                detail: device.detail,
            })
            .collect();
        let device_leases = self
            .state
            .active_device_leases_async()
            .await?
            .into_iter()
            .map(|lease| WireDeviceLease {
                attempt_id: lease.attempt_id.to_string(),
                device_id: lease.device_id,
                acquired_at_ms: lease.acquired_at_ms,
            })
            .collect();
        Ok(Heartbeat {
            active_attempts,
            available_slots: self.available_slots().await?,
            health: health.into(),
            devices,
            device_leases,
        })
    }

    async fn resume_outbox_delivery(
        &self,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        last_server_sequence: u64,
        last_worker_sequence_acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        self.state
            .acknowledge_outbox_async(connection_id.to_owned(), last_worker_sequence_acknowledged)
            .await?;
        self.state.prune_old_deliveries_async().await?;
        self.send_pending_outbox(
            connection_id,
            outbound,
            next_worker_sequence,
            last_server_sequence,
            delivered_message_ids,
        )
        .await
    }

    fn welcome_identity(&self, frame: &ServerToWorker) -> Result<(String, u32), WorkerError> {
        let Some(server_to_worker::Message::Welcome(welcome)) = frame.message.as_ref() else {
            return Err(WorkerError::Protocol(
                "first server frame must be welcome".to_owned(),
            ));
        };
        if welcome.protocol_major != self.hello.protocol_major {
            return Err(WorkerError::Protocol(format!(
                "server selected unsupported protocol major {}",
                welcome.protocol_major
            )));
        }
        Ok((welcome.connection_id.clone(), welcome.protocol_minor))
    }

    pub(super) fn validate_server_frame(
        message: &ServerToWorker,
        last_server_sequence: u64,
        last_worker_sequence_acknowledged: u64,
        sent_worker_through: u64,
        require_message_ids: bool,
    ) -> Result<(), WorkerError> {
        if message.sequence != last_server_sequence + 1 {
            return Err(WorkerError::Protocol(format!(
                "server sequence gap: expected {}, got {}",
                last_server_sequence + 1,
                message.sequence
            )));
        }
        if message.acknowledges_worker_through < last_worker_sequence_acknowledged {
            return Err(WorkerError::Protocol(format!(
                "server acknowledgement regressed from {last_worker_sequence_acknowledged} to {}",
                message.acknowledges_worker_through
            )));
        }
        if message.acknowledges_worker_through > sent_worker_through {
            return Err(WorkerError::Protocol(format!(
                "server acknowledged worker sequence {} beyond sent sequence {sent_worker_through}",
                message.acknowledges_worker_through
            )));
        }
        if require_message_ids {
            let expected_message_id = expected_server_message_id(message.message.as_ref());
            if let Some(expected) = expected_message_id {
                if message.message_id != expected {
                    return Err(WorkerError::Protocol(format!(
                        "server message ID must be {expected}"
                    )));
                }
            } else if !message.message_id.is_empty() {
                return Err(WorkerError::Protocol(
                    "ephemeral server frame cannot carry a message ID".to_owned(),
                ));
            }
        }
        Ok(())
    }

    async fn handle_server_message(
        &self,
        frame: ServerToWorker,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<bool, WorkerError> {
        match frame.message {
            Some(server_to_worker::Message::Welcome(welcome)) => {
                if welcome.protocol_major != self.hello.protocol_major {
                    return Err(WorkerError::Protocol(format!(
                        "server selected unsupported protocol major {}",
                        welcome.protocol_major
                    )));
                }
                Ok(false)
            }
            Some(server_to_worker::Message::Assignment(assignment)) => {
                self.handle_assignment(
                    assignment,
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await?;
                Ok(false)
            }
            Some(server_to_worker::Message::Drain(_)) => Ok(true),
            Some(server_to_worker::Message::Cancel(cancel)) => {
                self.handle_cancel(
                    cancel,
                    connection_id,
                    outbound,
                    next_worker_sequence,
                    acknowledged,
                    delivered_message_ids,
                )
                .await?;
                Ok(false)
            }
            Some(server_to_worker::Message::Acknowledgement(_)) => Ok(false),
            None => Err(WorkerError::Protocol(
                "server message payload is missing".to_owned(),
            )),
        }
    }
}
