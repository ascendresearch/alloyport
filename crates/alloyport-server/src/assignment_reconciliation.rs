//! Recovery and periodic reconciliation of abandoned assignment preparation.

use super::{
    PREPARATION_RECONCILE_BATCH_SIZE, PREPARATION_RECONCILE_INTERVAL_MS,
    PreparationReconciliationFailure, PreparationReconciliationReport, RepositoryError,
    WorkerControlService,
};

#[allow(clippy::missing_errors_doc)]
impl WorkerControlService {
    pub async fn reconcile_preparing_assignments(
        &self,
    ) -> Result<PreparationReconciliationReport, RepositoryError> {
        let repository = self.repositories.assignment_reads.clone();
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
                    if let Err(error) = service.grant_fixed_fixture_assignment_input(
                        &persisted_assignment.worker_id,
                        &persisted_assignment.contract,
                        now_ms,
                    ) {
                        service
                            .repositories
                            .assignment_writes
                            .defer_assignment_preparation(
                                &persisted_assignment.contract.attempt_id,
                                &persisted_assignment.worker_id,
                                now_ms,
                            )?;
                        return Ok::<_, RepositoryError>(Err(error.to_string()));
                    }
                    if let Err(error) =
                        service.record_run_started(&persisted_assignment.contract, now_ms)
                    {
                        service
                            .repositories
                            .assignment_writes
                            .defer_assignment_preparation(
                                &persisted_assignment.contract.attempt_id,
                                &persisted_assignment.worker_id,
                                now_ms,
                            )?;
                        return Ok(Err(error.to_string()));
                    }
                    service
                        .repositories
                        .assignment_writes
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
                    report.failures.push(PreparationReconciliationFailure {
                        attempt_id: attempt_id.to_string(),
                        detail,
                    });
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
                    attempt_id: attempt_id.to_string(),
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
        let repository = self.repositories.assignment_reads.clone();
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
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let result = self.run_preparation_reconciler_until(receiver).await;
        drop(shutdown);
        result
    }

    /// Reconciles periodically until the process supervisor requests shutdown.
    ///
    /// # Errors
    ///
    /// Returns the first repository failure that prevents a trustworthy reconciliation pass.
    pub async fn run_preparation_reconciler_until(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), RepositoryError> {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(
            PREPARATION_RECONCILE_INTERVAL_MS,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    let _report = self.reconcile_preparing_assignments().await?;
                }
            }
        }
    }
}
