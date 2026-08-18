//! Canonical user-visible event projection for worker attempt observations.

use super::{
    AppendOutcome, AssignmentContract, AssignmentState, Authority, Event, EventOutputStream,
    ExecutionFinished, InteractionError, OutputChunk, Producer, ProducerEvent, Status, Visibility,
    WorkerControlService, WorkerOutputStream, event_artifact, interaction_status,
    repository_status, worker_event,
};

impl WorkerControlService {
    /// Starts a run, unless something that owns it already did.
    ///
    /// An assignment used to be a run, so dispatching one published `run.started`. A migration run
    /// is now the umbrella: the management service starts it when the migration is captured, and
    /// many assignments happen inside it. Both producers published a start under different dedup
    /// keys, so every `alloyport-cli attach` on a migration died two events in with
    /// `run.started must be the first event` — the reducer was right and the stream was wrong.
    ///
    /// The condition is emptiness rather than a producer check, because the operator path that runs
    /// a candidate Episode directly still has no management-service start and would otherwise leave
    /// a run nothing can replay. Design 0017 already says this translator does not emit
    /// `run.completed` or verdicts because it does not own those decisions; a run's start is the
    /// same class, and this is the narrowest way to say so without silencing the only start some
    /// runs get.
    pub(super) fn record_run_started(
        &self,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<AppendOutcome, InteractionError> {
        if let Some(existing) = self
            .interactions
            .events_after(contract.task_id.as_str(), 0, 1)?
            .into_iter()
            .next()
        {
            return Ok(AppendOutcome::Duplicate(existing));
        }
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
            .repositories
            .assignment_reads
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
            .repositories
            .assignment_reads
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
            .repositories
            .assignment_reads
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_core::{
        ArtifactDescriptor, AssignmentId, AttemptId, CandidateId, ExecutionContract, ExecutionKind,
        TaskId,
    };
    use alloyport_events::{EventEnvelope, RunReducer};

    fn contract(task_id: &str) -> AssignmentContract {
        AssignmentContract {
            assignment_id: AssignmentId::try_from("assignment-1").expect("assignment"),
            attempt_id: AttemptId::try_from("attempt-1").expect("attempt"),
            attempt_number: 1,
            idempotency_key: "key-1".to_owned(),
            task_id: TaskId::try_from(task_id).expect("task"),
            candidate_id: CandidateId::try_from("candidate-1").expect("candidate"),
            execution: ExecutionContract {
                executor_kind: ExecutionKind::AscendBuild,
                argv: vec!["fixture".into()],
                working_directory: ".".into(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: ArtifactDescriptor {
                    digest: format!("sha256:{}", "a".repeat(64))
                        .parse()
                        .expect("digest"),
                    size_bytes: 1,
                    media_type: "application/octet-stream".into(),
                },
                image: ArtifactDescriptor {
                    digest: format!("sha256:{}", "b".repeat(64))
                        .parse()
                        .expect("digest"),
                    size_bytes: 0,
                    media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                },
                limits: None,
            },
            required_features: Vec::new(),
        }
    }

    /// Two producers must not both start one run, or nothing can replay it.
    ///
    /// The management service starts a migration run when it is captured; assignments then run
    /// inside it. Both used to publish `run.started`, so `alloyport-cli attach` failed two events
    /// in on every migration with `run.started must be the first event`.
    #[test]
    fn an_assignment_does_not_restart_a_run_its_owner_already_started() {
        let service = WorkerControlService::new();
        let task = "task-attach-1";

        // What the management service publishes when a migration is captured.
        let mut start = ProducerEvent::new(
            task.to_owned(),
            Producer::new("alloyport-server", "management"),
            Event::RunStarted {
                task: "migrate something".to_owned(),
            },
        );
        start.task_id = Some(task.to_owned());
        start.emitted_at_unix_ms = 1;
        start.authority = Authority::Observed;
        start.visibility = Visibility::User;
        service
            .interactions
            .append("migration-captured", &start)
            .expect("owner start");

        service
            .record_run_started(&contract(task), 2)
            .expect("assignment start");

        let events: Vec<EventEnvelope> = service.interactions.events(task).expect("events");
        assert_eq!(events.len(), 1, "the run must be started exactly once");
        let mut reducer = RunReducer::new();
        for event in &events {
            reducer.apply(event).expect("the stream must replay");
        }
    }

    /// A run nothing else started still gets one, or the operator path becomes unreplayable.
    #[test]
    fn an_assignment_still_starts_a_run_nobody_owns() {
        let service = WorkerControlService::new();
        let task = "task-attach-2";
        service
            .record_run_started(&contract(task), 1)
            .expect("assignment start");
        let events: Vec<EventEnvelope> = service.interactions.events(task).expect("events");
        assert_eq!(events.len(), 1);
        let mut reducer = RunReducer::new();
        for event in &events {
            reducer.apply(event).expect("the stream must replay");
        }
    }
}
