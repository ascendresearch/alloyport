//! Worker control-frame sequencing, durable acknowledgement, and observation dispatch.

use super::{
    ControlAcknowledgement, RepositoryError, ServerToWorker, Status, WorkerControlService,
    WorkerToServer, expected_worker_message_id, mpsc, repository_status, server_to_worker,
    validate_worker_acknowledgement,
};
use alloyport_proto::v1::{Heartbeat, WorkerHealth, WorkerHello, worker_to_server};
use std::collections::BTreeSet;

impl WorkerControlService {
    pub(super) async fn ingest(
        &self,
        worker_id: &str,
        connection_id: &str,
        frame: WorkerToServer,
    ) -> Result<bool, Status> {
        alloyport_proto::validate_worker_frame(&frame)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if let Some(worker_to_server::Message::Heartbeat(heartbeat)) = frame.message.as_ref() {
            alloyport_proto::validate_heartbeat(heartbeat)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        }
        let state = self.state.lock().await;
        let worker = state
            .workers
            .get(worker_id)
            .ok_or_else(|| Status::failed_precondition("worker is not registered"))?;
        if worker.connection_id != connection_id || !worker.connected {
            return Err(Status::aborted("worker connection was superseded"));
        }
        if let Some(worker_to_server::Message::Heartbeat(heartbeat)) = frame.message.as_ref() {
            validate_heartbeat_against_hello(heartbeat, &worker.hello)?;
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

fn validate_heartbeat_against_hello(
    heartbeat: &Heartbeat,
    hello: &WorkerHello,
) -> Result<(), Status> {
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("registered worker capabilities are missing"))?;
    if heartbeat.available_slots > capabilities.max_concurrency {
        return Err(Status::invalid_argument(
            "heartbeat available slots exceed registered concurrency",
        ));
    }
    let known_devices = capabilities
        .devices
        .iter()
        .map(|device| device.device_id.as_str())
        .collect::<BTreeSet<_>>();
    if heartbeat
        .devices
        .iter()
        .any(|device| !known_devices.contains(device.device_id.as_str()))
        || heartbeat
            .device_leases
            .iter()
            .any(|lease| !known_devices.contains(lease.device_id.as_str()))
    {
        return Err(Status::invalid_argument(
            "heartbeat references a device absent from registered capabilities",
        ));
    }
    let fixed_ascend = hello
        .features
        .iter()
        .any(|feature| feature == "ascend-fixture-v1");
    if fixed_ascend
        && heartbeat.health == WorkerHealth::Ready as i32
        && heartbeat.devices.len() != known_devices.len()
    {
        return Err(Status::invalid_argument(
            "ready fixed Ascend heartbeat must observe every registered device",
        ));
    }
    let known_attempts = heartbeat
        .active_attempts
        .iter()
        .map(|attempt| attempt.attempt_id.as_str())
        .collect::<BTreeSet<_>>();
    if heartbeat
        .device_leases
        .iter()
        .any(|lease| !known_attempts.contains(lease.attempt_id.as_str()))
    {
        return Err(Status::invalid_argument(
            "heartbeat device lease does not name a durable local attempt",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_proto::v1::{
        AcceleratorDevice, Backend, DeviceHealth, DeviceLease, DeviceObservation,
        WorkerCapabilities,
    };

    fn hello() -> WorkerHello {
        WorkerHello {
            protocol_major: alloyport_proto::PROTOCOL_MAJOR,
            protocol_minor: alloyport_proto::PROTOCOL_MINOR,
            worker_id: "ascend-1".to_owned(),
            instance_id: "boot-1".to_owned(),
            worker_version: "test".to_owned(),
            features: vec!["ascend-fixture-v1".to_owned()],
            capabilities: Some(WorkerCapabilities {
                backend: Backend::Ascend.into(),
                architecture: "Ascend950PR".to_owned(),
                device_count: 1,
                max_concurrency: 1,
                driver_version: "25.7.rc1.6".to_owned(),
                toolkit_version: "9.1.0-beta.1".to_owned(),
                container_runtime: "docker".to_owned(),
                devices: vec![AcceleratorDevice {
                    device_id: "3".to_owned(),
                    product_name: "Ascend950PR".to_owned(),
                    serial_number: "serial-3".to_owned(),
                    firmware_version: "9.0.0.105.229".to_owned(),
                }],
            }),
            active_attempts: Vec::new(),
        }
    }

    #[test]
    fn heartbeat_devices_and_leases_must_match_registered_identity() {
        let heartbeat = Heartbeat {
            active_attempts: vec![alloyport_proto::v1::ActiveAttempt {
                assignment_id: "assignment-1".to_owned(),
                attempt_id: "attempt-1".to_owned(),
                phase: alloyport_proto::v1::AttemptPhase::Running.into(),
            }],
            available_slots: 0,
            health: WorkerHealth::Ready.into(),
            devices: vec![DeviceObservation {
                device_id: "3".to_owned(),
                health: DeviceHealth::Ready.into(),
                process_count: 0,
                utilization_percent: 0,
                memory_used_bytes: 0,
                memory_total_bytes: 1,
                temperature_millicelsius: 1,
                power_milliwatts: 1,
                observed_at_ms: 1,
                detail: String::new(),
            }],
            device_leases: vec![DeviceLease {
                attempt_id: "attempt-1".to_owned(),
                device_id: "3".to_owned(),
                acquired_at_ms: 1,
            }],
        };
        assert!(validate_heartbeat_against_hello(&heartbeat, &hello()).is_ok());

        let mut unknown = heartbeat.clone();
        unknown.device_leases[0].device_id = "6".to_owned();
        assert!(validate_heartbeat_against_hello(&unknown, &hello()).is_err());

        let mut missing_attempt = heartbeat;
        missing_attempt.device_leases[0].attempt_id = "attempt-missing".to_owned();
        assert!(validate_heartbeat_against_hello(&missing_attempt, &hello()).is_err());
    }
}
