//! Worker observation ingestion and canonical interaction projection.

use super::{
    ATTEMPT_LEASE_MS, AppendOutcome, AssignmentAccepted, AssignmentContract, AssignmentRejected,
    AssignmentState, AttemptObservation, Authority, CancellationAcknowledged,
    ControlAcknowledgement, Event, EventOutputStream, ExecutionFinished, ExecutionStarted,
    FinishedObservation, Heartbeat, InteractionError, ObservationDisposition, ObservedAttempt,
    OutputChunk, Producer, ProducerEvent, RepositoryError, ServerToWorker, Status, Visibility,
    WorkerControlService, WorkerOutputStream, WorkerStatus, WorkerToServer, artifact_to_identity,
    event_artifact, expected_worker_message_id, interaction_status, mpsc, repository_status,
    server_to_worker, validate_and_grant_finished_artifacts, validate_worker_acknowledgement,
    worker_event, worker_to_server,
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

        let repository = self.repository.clone();
        let persisted_connection_id = connection_id.to_owned();
        self.persistence
            .run(move || {
                repository.compact_server_frames(
                    &persisted_connection_id,
                    frame.acknowledges_server_through,
                    now_ms,
                )?;
                repository.update_connection_sequences(
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

    fn observe_message(
        &self,
        worker_id: &str,
        message: Option<worker_to_server::Message>,
        now_ms: u64,
    ) -> Result<(), Status> {
        match message {
            Some(worker_to_server::Message::Heartbeat(heartbeat)) => {
                self.observe_heartbeat(worker_id, &heartbeat, now_ms)?;
            }
            Some(worker_to_server::Message::Status(status)) => {
                Self::observe_status(status);
            }
            Some(worker_to_server::Message::AssignmentAccepted(accepted)) => {
                self.observe_accepted(worker_id, accepted, now_ms)?;
            }
            Some(worker_to_server::Message::AssignmentRejected(rejected)) => {
                self.observe_rejection(worker_id, rejected, now_ms)?;
            }
            Some(worker_to_server::Message::ExecutionStarted(started)) => {
                self.observe_started(worker_id, started, now_ms)?;
            }
            Some(worker_to_server::Message::ExecutionFinished(finished)) => {
                self.observe_finished(worker_id, &finished, now_ms)?;
            }
            Some(worker_to_server::Message::CancellationAcknowledged(acknowledged)) => {
                self.observe_cancellation_acknowledged(worker_id, acknowledged, now_ms)?;
            }
            Some(worker_to_server::Message::OutputChunk(output)) => {
                self.observe_output(worker_id, &output, now_ms)?;
            }
            Some(worker_to_server::Message::Hello(_)) => {
                return Err(Status::invalid_argument(
                    "hello is only valid as the first frame",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "worker message payload is missing",
                ));
            }
        }

        Ok(())
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
        let repository = self.repository.clone();
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

    pub(super) fn record_run_started(
        &self,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<AppendOutcome, InteractionError> {
        let mut frame = ProducerEvent::new(
            contract.task_id.clone(),
            Producer::new("alloyport-server", "controller"),
            Event::RunStarted {
                task: contract.task_id.clone(),
            },
        );
        frame.task_id = Some(contract.task_id.clone());
        frame.emitted_at_unix_ms = now_ms;
        frame.authority = Authority::Observed;
        frame.visibility = Visibility::User;
        self.interactions
            .append(&format!("task:{}:run-started", contract.task_id), &frame)
    }

    fn record_command_started(
        &self,
        worker_id: &str,
        attempt_id: &str,
        now_ms: u64,
    ) -> Result<AppendOutcome, Status> {
        let assignment = self
            .repository
            .assignment(attempt_id)
            .map_err(repository_status)?
            .ok_or_else(|| Status::failed_precondition("started attempt is unknown"))?;
        let frame = worker_event(
            &assignment.contract,
            worker_id,
            now_ms,
            Event::CommandStarted {
                command: assignment.contract.execution.argv.join(" "),
                cwd: Some(assignment.contract.execution.working_directory.clone()),
                execution_site: worker_id.to_owned(),
                description: Some("worker assignment execution".into()),
            },
        );
        self.interactions
            .append(&format!("attempt:{attempt_id}:command-started"), &frame)
            .map_err(|error| interaction_status(&error))
    }

    fn record_command_finished(
        &self,
        worker_id: &str,
        finished: &ExecutionFinished,
        now_ms: u64,
    ) -> Result<(), Status> {
        self.record_command_started(worker_id, &finished.attempt_id, now_ms)?;
        let assignment = self
            .repository
            .assignment(&finished.attempt_id)
            .map_err(repository_status)?
            .ok_or_else(|| Status::failed_precondition("finished attempt is unknown"))?;
        for (artifact, reference, suffix) in [
            (finished.stdout.as_ref(), "stdout", "stdout"),
            (finished.stderr.as_ref(), "stderr", "stderr"),
            (finished.receipt.as_ref(), "receipt", "receipt"),
        ] {
            let Some(artifact) = artifact else {
                continue;
            };
            let frame = worker_event(
                &assignment.contract,
                worker_id,
                now_ms,
                Event::ArtifactProduced {
                    artifact: event_artifact(artifact, reference),
                },
            );
            self.interactions
                .append(
                    &format!("attempt:{}:artifact:{suffix}", finished.attempt_id),
                    &frame,
                )
                .map_err(|error| interaction_status(&error))?;
        }
        let completion = worker_event(
            &assignment.contract,
            worker_id,
            now_ms,
            Event::CommandCompleted {
                exit_code: finished.exit_code.unwrap_or(-1),
                elapsed_ms: finished.elapsed_ms,
                timed_out: finished.outcome
                    == i32::from(alloyport_proto::v1::AttemptOutcome::TimedOut),
                output_artifact: finished
                    .stdout
                    .as_ref()
                    .map(|artifact| event_artifact(artifact, "stdout")),
            },
        );
        self.interactions
            .append(
                &format!("attempt:{}:command-completed", finished.attempt_id),
                &completion,
            )
            .map_err(|error| interaction_status(&error))?;
        Ok(())
    }

    fn observe_heartbeat(
        &self,
        worker_id: &str,
        heartbeat: &Heartbeat,
        now_ms: u64,
    ) -> Result<(), Status> {
        let active_attempts = heartbeat
            .active_attempts
            .iter()
            .map(|attempt| attempt.attempt_id.clone())
            .collect::<Vec<_>>();
        self.repository
            .renew_active_leases(worker_id, &active_attempts, now_ms, ATTEMPT_LEASE_MS)
            .map_err(repository_status)
    }

    fn observe_status(_status: WorkerStatus) {}

    fn observe_accepted(
        &self,
        worker_id: &str,
        accepted: AssignmentAccepted,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            accepted.assignment_id,
            accepted.attempt_id,
            now_ms,
            AttemptObservation::Accepted {
                already_known: accepted.already_known,
            },
        )
    }

    fn observe_rejection(
        &self,
        worker_id: &str,
        rejected: AssignmentRejected,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            rejected.assignment_id,
            rejected.attempt_id,
            now_ms,
            AttemptObservation::Rejected {
                reason: rejected.reason,
                detail: rejected.detail,
            },
        )
    }

    fn observe_started(
        &self,
        worker_id: &str,
        started: ExecutionStarted,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        let attempt_id = started.attempt_id.clone();
        let disposition = self.observe(
            worker_id,
            started.assignment_id,
            started.attempt_id,
            now_ms,
            AttemptObservation::Started,
        )?;
        self.record_command_started(worker_id, &attempt_id, now_ms)?;
        Ok(disposition)
    }

    fn observe_finished(
        &self,
        worker_id: &str,
        finished: &ExecutionFinished,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        if let Some(uploads) = self.artifact_metadata.as_ref() {
            validate_and_grant_finished_artifacts(
                uploads.as_ref(),
                worker_id,
                &finished.attempt_id,
                finished,
                now_ms,
            )?;
        }
        let observation = FinishedObservation {
            outcome: finished.outcome,
            exit_code: finished.exit_code,
            elapsed_ms: finished.elapsed_ms,
            receipt: finished.receipt.as_ref().map(artifact_to_identity),
            stdout: finished.stdout.as_ref().map(artifact_to_identity),
            stderr: finished.stderr.as_ref().map(artifact_to_identity),
            detail: finished.detail.clone(),
        };
        let disposition = self.observe(
            worker_id,
            finished.assignment_id.clone(),
            finished.attempt_id.clone(),
            now_ms,
            AttemptObservation::Finished(observation),
        )?;
        self.record_command_finished(worker_id, finished, now_ms)?;
        Ok(disposition)
    }

    fn observe_output(
        &self,
        worker_id: &str,
        output: &OutputChunk,
        now_ms: u64,
    ) -> Result<(), Status> {
        let assignment = self
            .repository
            .assignment(&output.attempt_id)
            .map_err(repository_status)?
            .ok_or_else(|| Status::failed_precondition("output attempt is unknown"))?;
        if assignment.worker_id != worker_id {
            return Err(Status::permission_denied(format!(
                "attempt {} belongs to another worker",
                output.attempt_id
            )));
        }
        if assignment.state != AssignmentState::Running {
            return Err(Status::failed_precondition(format!(
                "output attempt {} is not running",
                output.attempt_id
            )));
        }
        let stream =
            WorkerOutputStream::try_from(output.stream).unwrap_or(WorkerOutputStream::Unspecified);
        let event_stream = match stream {
            WorkerOutputStream::Stdout => EventOutputStream::Stdout,
            WorkerOutputStream::Stderr => EventOutputStream::Stderr,
            WorkerOutputStream::Unspecified => {
                return Err(Status::invalid_argument("output stream is unspecified"));
            }
        };
        let text = String::from_utf8_lossy(&output.payload);
        let display_sanitized =
            output.display_sanitized || matches!(text, std::borrow::Cow::Owned(_));
        let frame = worker_event(
            &assignment.contract,
            worker_id,
            now_ms,
            Event::CommandOutput {
                stream: event_stream,
                byte_offset: output.byte_offset,
                text: text.into_owned(),
                display_sanitized,
            },
        );
        let appended = self
            .interactions
            .append_output(
                &format!(
                    "attempt:{}:output:{}:{}",
                    output.attempt_id, output.stream, output.byte_offset
                ),
                &output.attempt_id,
                output.stream,
                output.byte_offset,
                &output.payload,
                &frame,
            )
            .map_err(|error| interaction_status(&error))?;
        if appended.missing_bytes_before != 0 {
            let expected = output
                .byte_offset
                .saturating_sub(appended.missing_bytes_before);
            let warning = worker_event(
                &assignment.contract,
                worker_id,
                now_ms,
                Event::Warning {
                    message: format!(
                        "live {stream:?} preview omitted bytes {expected}..{}; complete output remains in the terminal Artifact",
                        output.byte_offset
                    ),
                },
            );
            self.interactions
                .append(
                    &format!(
                        "attempt:{}:output-gap:{}:{expected}:{}",
                        output.attempt_id, output.stream, output.byte_offset
                    ),
                    &warning,
                )
                .map_err(|error| interaction_status(&error))?;
        }
        Ok(())
    }

    fn observe_cancellation_acknowledged(
        &self,
        worker_id: &str,
        acknowledged: CancellationAcknowledged,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            acknowledged.assignment_id,
            acknowledged.attempt_id,
            now_ms,
            AttemptObservation::CancellationAcknowledged {
                already_terminal: acknowledged.already_terminal,
            },
        )
    }

    fn observe(
        &self,
        worker_id: &str,
        assignment_id: String,
        attempt_id: String,
        observed_at_ms: u64,
        observation: AttemptObservation,
    ) -> Result<ObservationDisposition, Status> {
        self.repository
            .observe_attempt(&ObservedAttempt {
                assignment_id,
                attempt_id,
                worker_id: worker_id.to_owned(),
                observed_at_ms,
                observation,
            })
            .map_err(repository_status)
    }
}
