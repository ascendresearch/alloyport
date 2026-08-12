//! Capability-segregated durable control-plane repository ports.

use super::{
    AssignmentContract, AssignmentDeliveryPreparation, AssignmentRecord, AttemptState,
    CancellationRecord, ConnectionRegistration, FinishedObservation, LeaseRecord,
    ObservationDisposition, ObservedAttempt, ReassignmentRecord, ServerOutboxFrame,
    StoreAssignmentOutcome, WorkerRegistration,
};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

/// Storage failures are kept distinct from RPC validation failures.
#[derive(Debug)]
pub enum RepositoryError {
    Storage(Box<dyn Error + Send + Sync>),
    Encoding(Box<dyn Error + Send + Sync>),
    LockPoisoned,
    NotFound(String),
    IdentityMismatch(String),
    InvalidIdentity(String),
    InvalidTransition {
        from: AttemptState,
        to: AttemptState,
    },
    ConflictingAttempt(String),
    Corrupt(String),
}

impl Display for RepositoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "control repository storage error: {error}"),
            Self::Encoding(error) => {
                write!(formatter, "control repository encoding error: {error}")
            }
            Self::LockPoisoned => write!(formatter, "control repository lock is poisoned"),
            Self::NotFound(attempt) => write!(formatter, "attempt {attempt} is not assigned"),
            Self::IdentityMismatch(attempt) => {
                write!(
                    formatter,
                    "attempt {attempt} identity does not match worker"
                )
            }
            Self::InvalidIdentity(detail) => write!(formatter, "invalid identity: {detail}"),
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid attempt transition from {from:?} to {to:?}"
                )
            }
            Self::ConflictingAttempt(attempt) => {
                write!(
                    formatter,
                    "attempt {attempt} was reused with different content"
                )
            }
            Self::Corrupt(detail) => write!(formatter, "corrupt control repository: {detail}"),
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) | Self::Encoding(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Durable worker registration and control-connection operations.
#[allow(clippy::missing_errors_doc)]
pub trait WorkerConnectionRepository: Debug + Send + Sync {
    fn register_worker(
        &self,
        registration: &WorkerRegistration,
        connection: &ConnectionRegistration,
    ) -> Result<(), RepositoryError>;

    fn update_connection_sequences(
        &self,
        connection_id: &str,
        last_worker_sequence: u64,
        last_server_sequence: u64,
        last_server_acknowledged_by_worker: u64,
        observed_at_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn disconnect(&self, connection_id: &str, at_ms: u64) -> Result<(), RepositoryError>;
}

/// Read-only access to durable assignment state and recovery queues.
#[allow(clippy::missing_errors_doc)]
pub trait AssignmentReadRepository: Debug + Send + Sync {
    fn assignment(&self, attempt_id: &str) -> Result<Option<AssignmentRecord>, RepositoryError>;

    fn finished_observation(
        &self,
        attempt_id: &str,
    ) -> Result<Option<FinishedObservation>, RepositoryError>;

    fn preparing_assignments(&self, limit: usize)
    -> Result<Vec<AssignmentRecord>, RepositoryError>;

    fn preparing_assignment_count(&self) -> Result<usize, RepositoryError>;

    fn replayable_assignments(
        &self,
        worker_id: &str,
    ) -> Result<Vec<AssignmentRecord>, RepositoryError>;
}

/// Durable assignment admission, dispatch, recovery, and reassignment commands.
///
/// Assignment content is immutable per attempt. `Preparing` records are invisible to replay until
/// explicitly made dispatchable. Delivery preparation is the single authorization transaction for
/// publishing an assignment frame: state transition, attempt lease, durable server outbox frame,
/// and active connection sequence must commit together or remain unchanged. Reassignment retains
/// the expired source and creates one idempotently linked fresh attempt.
#[allow(clippy::missing_errors_doc)]
pub trait AssignmentWriteRepository: Debug + Send + Sync {
    fn store_assignment(
        &self,
        worker_id: &str,
        contract: &AssignmentContract,
        at_ms: u64,
    ) -> Result<StoreAssignmentOutcome, RepositoryError>;

    /// Makes a fully prepared assignment eligible for dispatch and replay.
    ///
    /// Returns `true` only when this call performs the `Preparing -> Dispatchable` transition.
    fn mark_assignment_dispatchable(
        &self,
        attempt_id: &str,
        worker_id: &str,
        at_ms: u64,
    ) -> Result<bool, RepositoryError>;

    fn defer_assignment_preparation(
        &self,
        attempt_id: &str,
        worker_id: &str,
        retry_at_ms: u64,
    ) -> Result<bool, RepositoryError>;

    fn reassign_expired(
        &self,
        expired_attempt_id: &str,
        replacement_worker_id: &str,
        replacement_attempt_id: &str,
        at_ms: u64,
    ) -> Result<ReassignmentRecord, RepositoryError>;

    fn prepare_assignment_delivery(
        &self,
        preparation: &AssignmentDeliveryPreparation,
    ) -> Result<AssignmentContract, RepositoryError>;
}

/// Compatibility composition of assignment read and write capabilities.
pub trait AssignmentRepository: AssignmentReadRepository + AssignmentWriteRepository {}

impl<T> AssignmentRepository for T where T: AssignmentReadRepository + AssignmentWriteRepository {}

/// Durable attempt observations and lease lifecycle operations.
///
/// Implementations preserve immutable assignment/worker identity, monotonic attempt transitions,
/// idempotent observation classification, and auditable leases. Heartbeats may renew only active,
/// unexpired leases; an expired lease and its timestamp cannot be resurrected or rewritten. Late
/// terminal observations are retained as stale without replacing `LeaseExpired`. Cancellation
/// acknowledgement proves receipt of control intent but does not itself prove execution ended.
#[allow(clippy::missing_errors_doc)]
pub trait AttemptLifecycleRepository: Debug + Send + Sync {
    fn observe_attempt(
        &self,
        observation: &ObservedAttempt,
    ) -> Result<ObservationDisposition, RepositoryError>;

    fn renew_active_leases(
        &self,
        worker_id: &str,
        attempt_ids: &[String],
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn expire_leases(&self, now_ms: u64) -> Result<Vec<String>, RepositoryError>;

    fn request_cancellation(
        &self,
        attempt_id: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<CancellationRecord, RepositoryError>;

    fn lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError>;
}

/// Durable server-to-worker frame outbox operations.
#[allow(clippy::missing_errors_doc)]
pub trait ServerOutboxRepository: Debug + Send + Sync {
    fn record_server_frame(
        &self,
        frame: &ServerOutboxFrame,
        now_ms: u64,
    ) -> Result<(), RepositoryError>;

    fn compact_server_frames(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
        now_ms: u64,
    ) -> Result<usize, RepositoryError>;

    fn server_outbox_len(&self, connection_id: &str) -> Result<usize, RepositoryError>;

    fn prune_orphaned_server_frames(
        &self,
        disconnected_before_ms: u64,
    ) -> Result<usize, RepositoryError>;
}

/// Composite compatibility port used by the complete worker-control application service.
pub trait ControlRepository:
    WorkerConnectionRepository
    + AssignmentRepository
    + AttemptLifecycleRepository
    + ServerOutboxRepository
{
}

impl<T> ControlRepository for T where
    T: WorkerConnectionRepository
        + AssignmentRepository
        + AttemptLifecycleRepository
        + ServerOutboxRepository
{
}
