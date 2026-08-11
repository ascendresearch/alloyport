//! Canonical user-visible event projection for worker attempt observations.

use super::{
    AppendOutcome, AssignmentContract, AssignmentState, Authority, Event, EventOutputStream,
    ExecutionFinished, InteractionError, OutputChunk, Producer, ProducerEvent, Status, Visibility,
    WorkerControlService, WorkerOutputStream, event_artifact, interaction_status,
    repository_status, worker_event,
};

impl WorkerControlService {
    pub(super) fn record_run_started(
        &self,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<AppendOutcome, InteractionError> {
        let mut frame = ProducerEvent::new(
            contract.task_id.to_string(),
            Producer::new("alloyport-server", "controller"),
            Event::RunStarted {
                task: contract.task_id.to_string(),
            },
        );
        frame.task_id = Some(contract.task_id.to_string());
        frame.emitted_at_unix_ms = now_ms;
        frame.authority = Authority::Observed;
        frame.visibility = Visibility::User;
        self.interactions
            .append(&format!("task:{}:run-started", contract.task_id), &frame)
    }

    pub(super) fn record_command_started(
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

    pub(super) fn record_command_finished(
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

    pub(super) fn observe_output(
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
}
