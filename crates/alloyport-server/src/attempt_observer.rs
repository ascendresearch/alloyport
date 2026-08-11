//! Worker observation routing and durable attempt-state transitions.

use super::{
    ATTEMPT_LEASE_MS, AssignmentAccepted, AssignmentRejected, AttemptObservation,
    CancellationAcknowledged, ExecutionFinished, ExecutionStarted, FinishedObservation, Heartbeat,
    ObservationDisposition, ObservedAttempt, Status, WorkerControlService, WorkerStatus,
    artifact_to_identity, repository_status, validate_and_grant_finished_artifacts,
    worker_to_server,
};
use alloyport_core::AttemptOutcome;

impl WorkerControlService {
    pub(super) fn observe_message(
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
            outcome: AttemptOutcome::try_from(finished.outcome)
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
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
