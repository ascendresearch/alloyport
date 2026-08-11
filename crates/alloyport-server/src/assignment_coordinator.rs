//! Assignment admission, access grants, reassignment, and cancellation use cases.

use super::{
    ArtifactReferenceKind, Assignment, AssignmentContract, CancelOutcome, CancellationStoreOutcome,
    EnqueueError, EnqueueOutcome, GrantArtifactReference, InteractionError, RepositoryError,
    RunGrantOutcome, RunRevokeOutcome, StoreAssignmentOutcome, WorkerControlService,
    assignment_to_contract, contract_to_assignment, validate_assignment,
};
use alloyport_core::ExecutionKind;

#[allow(clippy::missing_errors_doc)]
impl WorkerControlService {
    /// Persists and, if connected, sends an assignment to a named worker.
    ///
    /// The immutable contract is committed before a send is prepared. Preparing the send commits
    /// its lease and `Sent` observation before the frame is placed on the network channel.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] for an invalid assignment, repository failure, or an attempt
    /// identifier reused with different content.
    pub async fn enqueue_assignment(
        &self,
        worker_id: impl Into<String>,
        assignment: Assignment,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        validate_assignment(&assignment)?;
        let worker_id = worker_id.into();
        let contract = assignment_to_contract(&assignment);
        let now_ms = self.clock.now_unix_ms();
        let service = self.clone();
        let prepared_worker_id = worker_id.clone();
        let prepared_contract = contract.clone();
        let (stored, became_dispatchable) = self
            .persistence
            .run(move || {
                let stored = service.repositories.assignments.store_assignment(
                    &prepared_worker_id,
                    &prepared_contract,
                    now_ms,
                )?;
                service.grant_cuda_assignment_input(
                    &prepared_worker_id,
                    &prepared_contract,
                    now_ms,
                )?;
                service.record_run_started(&prepared_contract, now_ms)?;
                let became_dispatchable = service
                    .repositories
                    .assignments
                    .mark_assignment_dispatchable(
                        &prepared_contract.attempt_id,
                        &prepared_worker_id,
                        service.clock.now_unix_ms(),
                    )?;
                Ok::<_, EnqueueError>((stored, became_dispatchable))
            })
            .await
            .map_err(RepositoryError::from)??;
        if stored == StoreAssignmentOutcome::Duplicate && !became_dispatchable {
            return Ok(EnqueueOutcome::Duplicate);
        }

        let outbound = self
            .prepare_assignment(&worker_id, &contract.attempt_id)
            .await?;
        let Some((sender, message)) = outbound else {
            return Ok(EnqueueOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&worker_id).await;
            return Ok(EnqueueOutcome::Pending);
        }
        Ok(EnqueueOutcome::Sent)
    }

    /// Grants one owner access to the run, then persists and dispatches its assignment.
    ///
    /// The explicit owner comes from the trusted controller call site, never from a worker frame.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] for invalid input, a terminally revoked grant, or enqueue failure.
    pub async fn enqueue_assignment_for_owner(
        &self,
        owner_id: &str,
        worker_id: impl Into<String>,
        assignment: Assignment,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        validate_assignment(&assignment)?;
        let interactions = self.interactions.clone();
        let run_id = assignment.task_id.clone();
        let owner_id = owner_id.to_owned();
        let now_ms = self.clock.now_unix_ms();
        self.persistence
            .run(move || interactions.grant_run_access(&run_id, &owner_id, now_ms))
            .await
            .map_err(RepositoryError::from)??;
        self.enqueue_assignment(worker_id, assignment).await
    }

    /// Adds an idempotent public-read grant for an existing or planned run.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, storage failure, or a terminally revoked grant.
    pub fn grant_interaction_access(
        &self,
        run_id: &str,
        owner_id: &str,
    ) -> Result<RunGrantOutcome, InteractionError> {
        self.interactions
            .grant_run_access(run_id, owner_id, self.clock.now_unix_ms())
    }

    /// Revokes an existing public-read grant idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error when the grant is unknown or cannot be durably updated.
    pub fn revoke_interaction_access(
        &self,
        run_id: &str,
        owner_id: &str,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        self.interactions
            .revoke_run_access(run_id, owner_id, self.clock.now_unix_ms())
    }

    /// Creates and dispatches a new process attempt for one durably expired attempt.
    ///
    /// The replacement copies the immutable assignment contract, increments its attempt number,
    /// and uses the caller-supplied fresh attempt ID. The expired record remains authoritative for
    /// classifying any late observations from the old worker.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] unless the source attempt is lease-expired, the replacement ID is
    /// fresh, and the copied assignment remains valid.
    pub async fn reassign_expired_attempt(
        &self,
        expired_attempt_id: &str,
        replacement_worker_id: impl Into<String>,
        replacement_attempt_id: impl Into<String>,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        let replacement_worker_id = replacement_worker_id.into();
        let replacement_attempt_id = replacement_attempt_id.into();
        let service = self.clone();
        let expired_attempt_id = expired_attempt_id.to_owned();
        let prepared_worker_id = replacement_worker_id.clone();
        let prepared_attempt_id = replacement_attempt_id.clone();
        let (reassignment, became_dispatchable) = self
            .persistence
            .run(move || {
                let reassignment = service.repositories.assignments.reassign_expired(
                    &expired_attempt_id,
                    &prepared_worker_id,
                    &prepared_attempt_id,
                    service.clock.now_unix_ms(),
                )?;
                validate_assignment(&contract_to_assignment(&reassignment.assignment.contract))?;
                service.grant_cuda_assignment_input(
                    &prepared_worker_id,
                    &reassignment.assignment.contract,
                    service.clock.now_unix_ms(),
                )?;
                service.record_run_started(
                    &reassignment.assignment.contract,
                    service.clock.now_unix_ms(),
                )?;
                let became_dispatchable = service
                    .repositories
                    .assignments
                    .mark_assignment_dispatchable(
                        &prepared_attempt_id,
                        &prepared_worker_id,
                        service.clock.now_unix_ms(),
                    )?;
                Ok::<_, EnqueueError>((reassignment, became_dispatchable))
            })
            .await
            .map_err(RepositoryError::from)??;
        if reassignment.outcome == StoreAssignmentOutcome::Duplicate && !became_dispatchable {
            return Ok(EnqueueOutcome::Duplicate);
        }
        let outbound = self
            .prepare_assignment(&replacement_worker_id, &replacement_attempt_id)
            .await?;
        let Some((sender, message)) = outbound else {
            return Ok(EnqueueOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&replacement_worker_id).await;
            return Ok(EnqueueOutcome::Pending);
        }
        Ok(EnqueueOutcome::Sent)
    }

    pub(super) fn grant_cuda_assignment_input(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<(), EnqueueError> {
        if contract.execution.executor_kind != ExecutionKind::CudaFixture {
            return Ok(());
        }
        let uploads = self.artifact_metadata.as_ref().ok_or_else(|| {
            EnqueueError::Artifact(
                "CUDA fixture assignments require the Artifact metadata service".into(),
            )
        })?;
        let digest = contract.execution.bundle.digest;
        let stored_size = uploads
            .artifact_size_bytes(digest)
            .map_err(|error| EnqueueError::Artifact(error.to_string()))?
            .ok_or_else(|| {
                EnqueueError::Artifact(format!("input bundle {digest} is not published"))
            })?;
        if stored_size != contract.execution.bundle.size_bytes {
            return Err(EnqueueError::Artifact(format!(
                "input bundle {digest} has size {stored_size}, assignment declares {}",
                contract.execution.bundle.size_bytes
            )));
        }
        uploads
            .grant_reference(&GrantArtifactReference {
                owner_id: worker_id.to_owned(),
                reference_key: format!("input:{}:bundle", contract.attempt_id),
                digest,
                kind: ArtifactReferenceKind::AssignmentInput,
                purpose: "CUDA fixture input bundle".into(),
                now_ms,
                retained_until_ms: None,
            })
            .map_err(|error| EnqueueError::Artifact(error.to_string()))?;
        Ok(())
    }

    /// Durably requests cancellation and sends it when the owning worker is connected.
    ///
    /// # Errors
    ///
    /// Returns a repository error when the attempt is unknown or the request cannot be committed.
    pub async fn cancel_attempt(
        &self,
        attempt_id: &str,
        reason: impl Into<String>,
    ) -> Result<CancelOutcome, RepositoryError> {
        let reason = reason.into();
        let repository = self.repositories.attempts.clone();
        let persisted_attempt_id = attempt_id.to_owned();
        let persisted_reason = reason.clone();
        let now_ms = self.clock.now_unix_ms();
        let cancellation = self
            .persistence
            .run(move || {
                repository.request_cancellation(&persisted_attempt_id, &persisted_reason, now_ms)
            })
            .await
            .map_err(RepositoryError::from)??;
        match cancellation.outcome {
            CancellationStoreOutcome::CancelledBeforeSend => {
                return Ok(CancelOutcome::CancelledBeforeSend);
            }
            CancellationStoreOutcome::AlreadyTerminal => {
                return Ok(CancelOutcome::AlreadyTerminal);
            }
            CancellationStoreOutcome::Requested | CancellationStoreOutcome::Duplicate => {}
        }
        let outbound = self
            .prepare_cancel(&cancellation.worker_id, attempt_id, &reason)
            .await?;
        let Some((sender, message)) = outbound else {
            return Ok(CancelOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&cancellation.worker_id).await;
            return Ok(CancelOutcome::Pending);
        }
        Ok(CancelOutcome::Sent)
    }
}
