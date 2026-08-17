//! Worker-control adapter for the Agent runtime's Ascend build-attempt port.

use crate::control_transport::contract_to_assignment;
use crate::storage::{AssignmentRecord, AttemptState, FinishedObservation};
use crate::{EnqueueError, WorkerControlService};
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    ArtifactDescriptor, AscendBuildAttemptError, AscendBuildAttemptFuture,
    AscendBuildAttemptObservation, AscendBuildAttemptPort, AscendBuildEnvironment,
    AscendBuildTerminal, AssignmentContract, AttemptOutcome, Sha256Digest,
};
use serde::Deserialize;
use std::io::Read;
use std::str::FromStr;
use std::sync::Arc;

const MAX_WORKER_RECEIPT_BYTES: u64 = 1024 * 1024;

/// Production adapter that dispatches and reconciles one immutable build assignment.
#[derive(Clone, Debug)]
pub struct WorkerBuildAttemptAdapter {
    service: WorkerControlService,
    worker_id: String,
    artifacts: Arc<dyn ArtifactStore>,
}

impl WorkerBuildAttemptAdapter {
    /// Binds build attempts to one explicitly selected worker and controller CAS.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker identity is empty.
    pub fn new(
        service: WorkerControlService,
        worker_id: impl Into<String>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Result<Self, AscendBuildAttemptError> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            return Err(AscendBuildAttemptError::Rejected(
                "build worker identity is empty".to_owned(),
            ));
        }
        Ok(Self {
            service,
            worker_id,
            artifacts,
        })
    }

    /// Makes the controller-authored bundle downloadable before the assignment references it.
    ///
    /// The controller writes its build bundle straight into the CAS, which the Artifact ledger
    /// never hears about, so the assignment named a digest the coordinator could not grant:
    /// "input bundle ... is not published". The size comes from the stored object rather than from
    /// the assignment, because the assignment is the thing being checked.
    fn publish_bundle(
        &self,
        assignment: &AssignmentContract,
    ) -> Result<(), AscendBuildAttemptError> {
        let digest = assignment.execution.bundle.digest;
        let identity = self
            .artifacts
            .open(digest)
            .map_err(|error| {
                AscendBuildAttemptError::Rejected(format!(
                    "build bundle {digest} is not readable in the controller store: {error}"
                ))
            })?
            .identity();
        self.service
            .artifact_metadata()
            .ok_or_else(|| {
                AscendBuildAttemptError::Rejected(
                    "publishing a build bundle requires the Artifact metadata service".to_owned(),
                )
            })?
            .record_local_artifact(&self.worker_id, identity)
            .map_err(|error| {
                AscendBuildAttemptError::Rejected(format!("cannot publish build bundle: {error}"))
            })
    }

    async fn dispatch_or_observe(
        &self,
        assignment: &AssignmentContract,
    ) -> Result<AscendBuildAttemptObservation, AscendBuildAttemptError> {
        self.publish_bundle(assignment)?;
        self.service
            .enqueue_assignment(self.worker_id.clone(), contract_to_assignment(assignment))
            .await
            .map_err(|error| enqueue_error(&error))?;
        self.observe(assignment).await
    }

    async fn observe(
        &self,
        assignment: &AssignmentContract,
    ) -> Result<AscendBuildAttemptObservation, AscendBuildAttemptError> {
        let Some((record, finished)) = self.lookup(assignment).await? else {
            return Err(AscendBuildAttemptError::Integrity(
                "dispatched build assignment is missing".to_owned(),
            ));
        };
        self.observe_stored(assignment, record, finished).await
    }

    async fn lookup(
        &self,
        assignment: &AssignmentContract,
    ) -> Result<Option<(AssignmentRecord, Option<FinishedObservation>)>, AscendBuildAttemptError>
    {
        let reads = Arc::clone(&self.service.repositories.assignment_reads);
        let attempt_id = assignment.attempt_id.to_string();
        let (record, finished) = self
            .service
            .persistence
            .run(move || {
                Ok::<_, crate::storage::RepositoryError>((
                    reads.assignment(&attempt_id)?,
                    reads.finished_observation(&attempt_id)?,
                ))
            })
            .await
            .map_err(|error| AscendBuildAttemptError::Unavailable(error.to_string()))?
            .map_err(|error| AscendBuildAttemptError::Unavailable(error.to_string()))?;
        let Some(record) = record else {
            return Ok(None);
        };
        validate_record(&record, assignment, &self.worker_id).map(|()| Some((record, finished)))
    }

    async fn observe_stored(
        &self,
        assignment: &AssignmentContract,
        record: AssignmentRecord,
        finished: Option<FinishedObservation>,
    ) -> Result<AscendBuildAttemptObservation, AscendBuildAttemptError> {
        match (record.state, finished) {
            (AttemptState::Finished, Some(finished)) => self.terminal(assignment, finished).await,
            (AttemptState::Rejected | AttemptState::Cancelled, _) => {
                Err(AscendBuildAttemptError::Rejected(format!(
                    "build attempt became {:?}",
                    record.state
                )))
            }
            (AttemptState::LeaseExpired, _) => Err(AscendBuildAttemptError::Unavailable(
                "build attempt lease expired".to_owned(),
            )),
            (AttemptState::Finished, None) => Err(AscendBuildAttemptError::Integrity(
                "finished build attempt lacks terminal observation".to_owned(),
            )),
            _ => Ok(AscendBuildAttemptObservation::Pending {
                diagnostic_digest: pending_digest(&record),
            }),
        }
    }

    async fn terminal(
        &self,
        assignment: &AssignmentContract,
        finished: FinishedObservation,
    ) -> Result<AscendBuildAttemptObservation, AscendBuildAttemptError> {
        let descriptor = finished.receipt.clone().ok_or_else(|| {
            AscendBuildAttemptError::Integrity("build attempt lacks worker receipt".to_owned())
        })?;
        let artifacts = Arc::clone(&self.artifacts);
        let projection = tokio::task::spawn_blocking(move || {
            read_worker_receipt(artifacts.as_ref(), &descriptor)
        })
        .await
        .map_err(|error| AscendBuildAttemptError::Unavailable(error.to_string()))??;
        validate_worker_receipt(&projection, assignment, &finished)?;
        Ok(AscendBuildAttemptObservation::Finished(Box::new(
            AscendBuildTerminal {
                assignment_id: assignment.assignment_id.clone(),
                attempt_id: assignment.attempt_id.clone(),
                outcome: finished.outcome,
                exit_code: finished.exit_code,
                elapsed_ms: finished.elapsed_ms,
                detail: finished.detail,
                build_completed: finished.outcome == AttemptOutcome::Succeeded
                    && finished.exit_code == Some(0),
                environment: AscendBuildEnvironment {
                    architecture: projection.environment.architecture,
                    cann_version: projection.environment.cann_version,
                    driver_version: projection.environment.driver_version,
                    firmware_version: projection.environment.firmware_version,
                },
                worker_receipt: finished.receipt,
                stdout: finished.stdout,
                stderr: finished.stderr,
            },
        )))
    }
}

impl AscendBuildAttemptPort for WorkerBuildAttemptAdapter {
    fn dispatch<'a>(
        &'a mut self,
        assignment: &'a AssignmentContract,
    ) -> AscendBuildAttemptFuture<'a> {
        Box::pin(async move { self.dispatch_or_observe(assignment).await })
    }

    fn reconcile<'a>(
        &'a mut self,
        assignment: &'a AssignmentContract,
    ) -> AscendBuildAttemptFuture<'a> {
        Box::pin(async move {
            match self.lookup(assignment).await? {
                Some((record, finished)) => self.observe_stored(assignment, record, finished).await,
                None => self.dispatch_or_observe(assignment).await,
            }
        })
    }
}

fn validate_record(
    record: &AssignmentRecord,
    assignment: &AssignmentContract,
    worker_id: &str,
) -> Result<(), AscendBuildAttemptError> {
    if record.worker_id != worker_id || record.contract != *assignment {
        return Err(AscendBuildAttemptError::Integrity(
            "build assignment identity changed after dispatch".to_owned(),
        ));
    }
    Ok(())
}

fn pending_digest(record: &AssignmentRecord) -> Sha256Digest {
    let mut bytes = b"alloyport-build-pending-v1\0".to_vec();
    bytes.extend_from_slice(record.contract.attempt_id.as_str().as_bytes());
    bytes.extend_from_slice(&(record.state as i64).to_be_bytes());
    bytes.extend_from_slice(&record.updated_at_ms.to_be_bytes());
    Sha256Digest::digest_bytes(&bytes)
}

fn enqueue_error(error: &EnqueueError) -> AscendBuildAttemptError {
    AscendBuildAttemptError::Unavailable(error.to_string())
}

#[derive(Deserialize)]
struct WorkerReceiptProjection {
    schema_version: u16,
    assignment_id: String,
    attempt_id: String,
    task_id: String,
    candidate_id: String,
    bundle_digest: String,
    image_digest: String,
    environment: WorkerEnvironmentProjection,
    outcome: String,
    exit_code: Option<i32>,
    elapsed_ms: u64,
}

#[derive(Deserialize)]
struct WorkerEnvironmentProjection {
    architecture: String,
    cann_version: String,
    driver_version: String,
    firmware_version: String,
}

fn read_worker_receipt(
    artifacts: &dyn ArtifactStore,
    descriptor: &ArtifactDescriptor,
) -> Result<WorkerReceiptProjection, AscendBuildAttemptError> {
    let mut reader = artifacts
        .open(descriptor.digest)
        .map_err(|error| AscendBuildAttemptError::Unavailable(error.to_string()))?;
    if reader.identity().size_bytes != descriptor.size_bytes
        || descriptor.size_bytes > MAX_WORKER_RECEIPT_BYTES
    {
        return Err(AscendBuildAttemptError::Integrity(
            "worker receipt descriptor is invalid".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| AscendBuildAttemptError::Unavailable(error.to_string()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != descriptor.size_bytes
        || Sha256Digest::digest_bytes(&bytes) != descriptor.digest
    {
        return Err(AscendBuildAttemptError::Integrity(
            "worker receipt bytes changed".to_owned(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| AscendBuildAttemptError::Integrity(error.to_string()))
}

fn validate_worker_receipt(
    receipt: &WorkerReceiptProjection,
    assignment: &AssignmentContract,
    finished: &FinishedObservation,
) -> Result<(), AscendBuildAttemptError> {
    let bundle_digest = Sha256Digest::from_str(&receipt.bundle_digest)
        .map_err(|error| AscendBuildAttemptError::Integrity(error.to_string()))?;
    let image_digest = Sha256Digest::from_str(&receipt.image_digest)
        .map_err(|error| AscendBuildAttemptError::Integrity(error.to_string()))?;
    if receipt.schema_version != 2
        || receipt.assignment_id != assignment.assignment_id.as_str()
        || receipt.attempt_id != assignment.attempt_id.as_str()
        || receipt.task_id != assignment.task_id.as_str()
        || receipt.candidate_id != assignment.candidate_id.as_str()
        || bundle_digest != assignment.execution.bundle.digest
        || image_digest != assignment.execution.image.digest
        || receipt.outcome != finished.outcome.as_str_name()
        || receipt.exit_code != finished.exit_code
        || receipt.elapsed_ms != finished.elapsed_ms
    {
        return Err(AscendBuildAttemptError::Integrity(
            "worker receipt does not match the terminal build observation".to_owned(),
        ));
    }
    Ok(())
}
