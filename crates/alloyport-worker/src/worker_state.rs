//! Worker-local admission policy and durable attempt-journal facade.

use super::{
    AdmissionOutcome, AdmissionPolicy, OUTBOX_DELIVERY_RETENTION, WorkerError, WorkerState,
};
use crate::adapters::sqlite::SqliteAttemptStore;
use crate::journal::{
    AttemptStore, LocalAttemptPhase, LocalAttemptRecord, StoreAdmissionOutcome, StoredFinished,
    WorkerOutboxMessage, WorkerOutboxPayload,
};
use crate::wire_mapping::{assignment_to_stored, lifecycle_identity, now_unix_ms};
use alloyport_proto::v1::{Assignment, AttemptPhase, ExecutorKind};
use alloyport_proto::{v1::ActiveAttempt, validate_assignment};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;

const MAX_BLOCKING_PERSISTENCE_OPERATIONS: usize = 4;

#[derive(Clone, Debug)]
pub(crate) struct WorkerPersistence {
    permits: Arc<Semaphore>,
}

impl Default for WorkerPersistence {
    fn default() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(MAX_BLOCKING_PERSISTENCE_OPERATIONS)),
        }
    }
}

impl WorkerPersistence {
    pub(crate) async fn run<T, F>(&self, operation: F) -> Result<T, WorkerError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, WorkerError> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| WorkerError::Execution("persistence executor closed".into()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(WorkerError::PersistenceTask)?
    }
}

impl WorkerState {
    /// Creates an ephemeral journal with the supplied policy.
    ///
    /// # Panics
    ///
    /// Panics only if the bundled `SQLite` library cannot create an in-memory journal.
    #[must_use]
    pub fn with_policy(policy: AdmissionPolicy) -> Self {
        let store = SqliteAttemptStore::in_memory()
            .expect("an in-memory worker attempt journal must initialize");
        Self::with_store(policy, Arc::new(store))
    }

    #[must_use]
    pub fn with_store(policy: AdmissionPolicy, store: Arc<dyn AttemptStore>) -> Self {
        Self {
            policy,
            store,
            persistence: WorkerPersistence::default(),
        }
    }

    /// Opens a crash-durable worker attempt journal.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` cannot open or migrate the journal.
    pub fn open_sqlite(
        policy: AdmissionPolicy,
        path: impl AsRef<Path>,
    ) -> Result<Self, WorkerError> {
        Ok(Self::with_store(
            policy,
            Arc::new(SqliteAttemptStore::open(path)?),
        ))
    }

    /// Validates and records an immutable attempt before acknowledging it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] if validation fails or the same attempt ID is reused for other bytes.
    pub fn admit(&self, assignment: &Assignment) -> Result<AdmissionOutcome, WorkerError> {
        validate_assignment(assignment).map_err(WorkerError::InvalidAssignment)?;
        if let Some(execution) = assignment.execution.as_ref() {
            let executor = ExecutorKind::try_from(execution.executor_kind)
                .unwrap_or(ExecutorKind::Unspecified);
            if self.policy.cuda_fixture_only && executor != ExecutorKind::CudaFixture {
                return Err(WorkerError::PolicyViolation(
                    "only the CUDA fixture executor is enabled".to_owned(),
                ));
            }
            if executor == ExecutorKind::Shell && !self.policy.allow_shell {
                return Err(WorkerError::PolicyViolation(
                    "shell executor is disabled".to_owned(),
                ));
            }
            if executor == ExecutorKind::CudaFixture && !self.policy.allow_cuda_fixture {
                return Err(WorkerError::PolicyViolation(
                    "CUDA fixture executor is disabled".to_owned(),
                ));
            }
        }
        let stored = assignment_to_stored(assignment);
        let outcome = self.store.admit(&stored, now_unix_ms())?;
        let admission = match outcome {
            StoreAdmissionOutcome::Inserted => AdmissionOutcome::New,
            StoreAdmissionOutcome::Duplicate => AdmissionOutcome::Duplicate,
        };
        Ok(admission)
    }

    /// Checks durable local attempt knowledge.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn contains_attempt(&self, attempt_id: &str) -> Result<bool, WorkerError> {
        self.store
            .attempt(attempt_id)
            .map(|attempt| attempt.is_some())
            .map_err(WorkerError::from)
    }

    /// Persists the transition that must precede starting an executor.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown attempt, invalid transition, or journal failure.
    pub fn mark_running(&self, attempt_id: &str) -> Result<(), WorkerError> {
        self.store
            .mark_running(attempt_id, now_unix_ms())
            .map_err(WorkerError::from)
    }

    /// Persists terminal result data before it can be reported to the server.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown attempt, conflicting terminal result, or journal failure.
    pub fn mark_finished(
        &self,
        attempt_id: &str,
        finished: &StoredFinished,
    ) -> Result<(), WorkerError> {
        self.store
            .mark_finished(attempt_id, finished, now_unix_ms())
            .map_err(WorkerError::from)
    }

    pub(super) fn enqueue_lifecycle(
        &self,
        payload: WorkerOutboxPayload,
    ) -> Result<String, WorkerError> {
        let (message_id, attempt_id) = lifecycle_identity(&payload);
        self.store.enqueue_outbox(
            &WorkerOutboxMessage {
                message_id: message_id.clone(),
                attempt_id,
                payload,
            },
            now_unix_ms(),
        )?;
        Ok(message_id)
    }

    pub(super) fn pending_outbox(&self) -> Result<Vec<WorkerOutboxMessage>, WorkerError> {
        self.store.pending_outbox().map_err(WorkerError::from)
    }

    pub(super) fn record_delivery(
        &self,
        connection_id: &str,
        sequence: u64,
        message_id: &str,
    ) -> Result<(), WorkerError> {
        self.store
            .record_outbox_delivery(connection_id, sequence, message_id, now_unix_ms())
            .map_err(WorkerError::from)
    }

    pub(super) fn acknowledge_outbox(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
    ) -> Result<usize, WorkerError> {
        self.store
            .acknowledge_outbox(connection_id, acknowledged_through)
            .map_err(WorkerError::from)
    }

    pub(super) fn prune_old_deliveries(&self) -> Result<usize, WorkerError> {
        let retention_ms = u64::try_from(OUTBOX_DELIVERY_RETENTION.as_millis()).unwrap_or(u64::MAX);
        self.store
            .prune_outbox_deliveries(now_unix_ms().saturating_sub(retention_ms))
            .map_err(WorkerError::from)
    }

    /// Returns the number of durable lifecycle messages awaiting acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn outbox_len(&self) -> Result<usize, WorkerError> {
        self.store.outbox_len().map_err(WorkerError::from)
    }

    /// Returns durable terminal data for a locally known attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be read.
    pub fn finished_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<StoredFinished>, WorkerError> {
        self.attempt(attempt_id)
            .map(|attempt| attempt.and_then(|record| record.finished))
    }

    pub(super) fn attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<LocalAttemptRecord>, WorkerError> {
        self.store.attempt(attempt_id).map_err(WorkerError::from)
    }

    pub(super) fn active_attempts(&self) -> Result<Vec<ActiveAttempt>, WorkerError> {
        self.store
            .attempts()?
            .into_iter()
            .map(|attempt| {
                Ok(ActiveAttempt {
                    assignment_id: attempt.assignment.assignment_id,
                    attempt_id: attempt.assignment.attempt_id,
                    phase: match attempt.phase {
                        LocalAttemptPhase::Accepted => AttemptPhase::Accepted,
                        LocalAttemptPhase::Running => AttemptPhase::Running,
                        LocalAttemptPhase::Finished => AttemptPhase::Finished,
                    }
                    .into(),
                })
            })
            .collect()
    }

    pub(super) fn attempt_count(&self) -> Result<usize, WorkerError> {
        self.store
            .attempts()
            .map(|attempts| {
                attempts
                    .iter()
                    .filter(|attempt| attempt.phase != LocalAttemptPhase::Finished)
                    .count()
            })
            .map_err(WorkerError::from)
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, WorkerError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, WorkerError> + Send + 'static,
    {
        let state = self.clone();
        self.persistence.run(move || operation(state)).await
    }

    pub(crate) async fn admit_async(
        &self,
        assignment: Assignment,
    ) -> Result<AdmissionOutcome, WorkerError> {
        self.blocking(move |state| state.admit(&assignment)).await
    }

    pub(crate) async fn mark_running_async(&self, attempt_id: String) -> Result<(), WorkerError> {
        self.blocking(move |state| state.mark_running(&attempt_id))
            .await
    }

    pub(crate) async fn mark_finished_async(
        &self,
        attempt_id: String,
        finished: StoredFinished,
    ) -> Result<(), WorkerError> {
        self.blocking(move |state| state.mark_finished(&attempt_id, &finished))
            .await
    }

    pub(crate) async fn enqueue_lifecycle_async(
        &self,
        payload: WorkerOutboxPayload,
    ) -> Result<String, WorkerError> {
        self.blocking(move |state| state.enqueue_lifecycle(payload))
            .await
    }

    pub(crate) async fn pending_outbox_async(
        &self,
    ) -> Result<Vec<WorkerOutboxMessage>, WorkerError> {
        self.blocking(|state| state.pending_outbox()).await
    }

    pub(crate) async fn record_delivery_async(
        &self,
        connection_id: String,
        sequence: u64,
        message_id: String,
    ) -> Result<(), WorkerError> {
        self.blocking(move |state| state.record_delivery(&connection_id, sequence, &message_id))
            .await
    }

    pub(crate) async fn acknowledge_outbox_async(
        &self,
        connection_id: String,
        acknowledged_through: u64,
    ) -> Result<usize, WorkerError> {
        self.blocking(move |state| state.acknowledge_outbox(&connection_id, acknowledged_through))
            .await
    }

    pub(crate) async fn prune_old_deliveries_async(&self) -> Result<usize, WorkerError> {
        self.blocking(|state| state.prune_old_deliveries()).await
    }

    pub(crate) async fn attempt_async(
        &self,
        attempt_id: String,
    ) -> Result<Option<LocalAttemptRecord>, WorkerError> {
        self.blocking(move |state| state.attempt(&attempt_id)).await
    }

    pub(crate) async fn active_attempts_async(&self) -> Result<Vec<ActiveAttempt>, WorkerError> {
        self.blocking(|state| state.active_attempts()).await
    }

    pub(crate) async fn attempt_count_async(&self) -> Result<usize, WorkerError> {
        self.blocking(|state| state.attempt_count()).await
    }

    pub(crate) async fn attempts_async(&self) -> Result<Vec<LocalAttemptRecord>, WorkerError> {
        self.blocking(|state| state.store.attempts().map_err(WorkerError::from))
            .await
    }
}
