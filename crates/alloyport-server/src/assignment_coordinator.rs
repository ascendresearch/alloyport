//! Assignment preparation, reconciliation, dispatch, reassignment, and cancellation.

use super::{
    ATTEMPT_LEASE_MS, ArtifactReferenceKind, Assignment, AssignmentContract,
    AssignmentDeliveryPreparation, CancelAttempt, CancelOutcome, CancellationStoreOutcome,
    EnqueueError, EnqueueOutcome, ExecutorKind, GrantArtifactReference, InteractionError, Ordering,
    PREPARATION_RECONCILE_BATCH_SIZE, PREPARATION_RECONCILE_INTERVAL_MS,
    PreparationReconciliationFailure, PreparationReconciliationReport, RepositoryError,
    RunGrantOutcome, RunRevokeOutcome, ServerFrameKind, ServerOutboxFrame, ServerToWorker,
    Sha256Digest, Status, StoreAssignmentOutcome, WorkerControlService, assignment_to_contract,
    contract_to_assignment, mpsc, server_to_worker, validate_assignment,
};
use std::str::FromStr;

#[allow(clippy::missing_errors_doc)]
impl WorkerControlService {
    pub async fn reconcile_preparing_assignments(
        &self,
    ) -> Result<PreparationReconciliationReport, RepositoryError> {
        let repository = self.repository.clone();
        let assignments = self
            .persistence
            .run(move || repository.preparing_assignments(PREPARATION_RECONCILE_BATCH_SIZE))
            .await
            .map_err(RepositoryError::from)??;
        let mut report = PreparationReconciliationReport {
            scanned: assignments.len(),
            ..PreparationReconciliationReport::default()
        };
        for assignment in assignments {
            let attempt_id = assignment.contract.attempt_id.clone();
            let now_ms = self.clock.now_unix_ms();
            let service = self.clone();
            let persisted_assignment = assignment.clone();
            let preparation = self
                .persistence
                .run(move || {
                    if let Err(error) = service.grant_cuda_assignment_input(
                        &persisted_assignment.worker_id,
                        &persisted_assignment.contract,
                        now_ms,
                    ) {
                        service.repository.defer_assignment_preparation(
                            &persisted_assignment.contract.attempt_id,
                            &persisted_assignment.worker_id,
                            now_ms,
                        )?;
                        return Ok::<_, RepositoryError>(Err(error.to_string()));
                    }
                    if let Err(error) =
                        service.record_run_started(&persisted_assignment.contract, now_ms)
                    {
                        service.repository.defer_assignment_preparation(
                            &persisted_assignment.contract.attempt_id,
                            &persisted_assignment.worker_id,
                            now_ms,
                        )?;
                        return Ok(Err(error.to_string()));
                    }
                    service
                        .repository
                        .mark_assignment_dispatchable(
                            &persisted_assignment.contract.attempt_id,
                            &persisted_assignment.worker_id,
                            now_ms,
                        )
                        .map(Ok)
                })
                .await
                .map_err(RepositoryError::from)??;
            let became_dispatchable = match preparation {
                Ok(became_dispatchable) => became_dispatchable,
                Err(detail) => {
                    report
                        .failures
                        .push(PreparationReconciliationFailure { attempt_id, detail });
                    continue;
                }
            };
            if !became_dispatchable {
                continue;
            }
            report.recovered += 1;
            match self
                .prepare_assignment(&assignment.worker_id, &assignment.contract.attempt_id)
                .await
            {
                Ok(Some((sender, message))) => {
                    if sender.send(Ok(message)).await.is_ok() {
                        report.sent += 1;
                    } else {
                        self.mark_send_failed(&assignment.worker_id).await;
                        report.pending_delivery += 1;
                    }
                }
                Ok(None) => report.pending_delivery += 1,
                Err(error) => report.failures.push(PreparationReconciliationFailure {
                    attempt_id,
                    detail: error.to_string(),
                }),
            }
        }
        Ok(report)
    }

    /// Reconciles every assignment that was preparing when startup began, using bounded queries.
    /// Rows deferred by one pass are rotated behind unseen work, preventing one unavailable
    /// Artifact from starving the rest of the startup recovery set.
    ///
    /// # Errors
    ///
    /// Returns a repository error if the recovery set cannot be counted, read, or updated.
    pub async fn reconcile_preparing_assignments_at_startup(
        &self,
    ) -> Result<PreparationReconciliationReport, RepositoryError> {
        let repository = self.repository.clone();
        let count = self
            .persistence
            .run(move || repository.preparing_assignment_count())
            .await
            .map_err(RepositoryError::from)??;
        let passes = count.div_ceil(PREPARATION_RECONCILE_BATCH_SIZE);
        let mut aggregate = PreparationReconciliationReport::default();
        for _ in 0..passes {
            let report = self.reconcile_preparing_assignments().await?;
            aggregate.scanned += report.scanned;
            aggregate.recovered += report.recovered;
            aggregate.sent += report.sent;
            aggregate.pending_delivery += report.pending_delivery;
            aggregate.failures.extend(report.failures);
        }
        Ok(aggregate)
    }

    /// Reconciles abandoned assignment preparation periodically until cancelled.
    ///
    /// # Errors
    ///
    /// Returns the first repository failure that prevents a trustworthy reconciliation pass.
    pub async fn run_preparation_reconciler(&self) -> Result<(), RepositoryError> {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            PREPARATION_RECONCILE_INTERVAL_MS,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let _report = self.reconcile_preparing_assignments().await?;
        }
    }

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
                let stored = service.repository.store_assignment(
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
                let became_dispatchable = service.repository.mark_assignment_dispatchable(
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
                let reassignment = service.repository.reassign_expired(
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
                let became_dispatchable = service.repository.mark_assignment_dispatchable(
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

    fn grant_cuda_assignment_input(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        now_ms: u64,
    ) -> Result<(), EnqueueError> {
        if ExecutorKind::try_from(contract.execution.executor_kind)
            .unwrap_or(ExecutorKind::Unspecified)
            != ExecutorKind::CudaFixture
        {
            return Ok(());
        }
        let uploads = self.artifact_metadata.as_ref().ok_or_else(|| {
            EnqueueError::Artifact(
                "CUDA fixture assignments require the Artifact metadata service".into(),
            )
        })?;
        let digest = Sha256Digest::from_str(&contract.execution.bundle.digest)
            .map_err(|error| EnqueueError::Artifact(error.to_string()))?;
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
        let repository = self.repository.clone();
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
        let repository = self.repository.clone();
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

    async fn mark_send_failed(&self, worker_id: &str) {
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
        let repository = self.repository.clone();
        let persisted_connection_id = connection_id.clone();
        let persisted_message_id = message_id.clone();
        let persisted_worker_id = worker_id.to_owned();
        let persisted_attempt_id = attempt_id.to_owned();
        self.persistence
            .run(move || {
                repository.record_server_frame(
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
                message_id,
                message: Some(server_to_worker::Message::Cancel(CancelAttempt {
                    attempt_id: attempt_id.to_owned(),
                    reason: reason.to_owned(),
                })),
            },
        )))
    }
}
