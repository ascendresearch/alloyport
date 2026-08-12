//! Worker-local attempt records and persistence port.
//!
//! Database drivers, transactions, schema migrations, and SQL belong to outer adapters.

use alloyport_core::{
    ArtifactDescriptor, AssignmentId, AttemptId, AttemptOutcome, DeviceLease, DeviceObservation,
    RejectionReason,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

/// Backward-compatible journal name for the shared immutable assignment contract.
pub type StoredAssignment = alloyport_core::AssignmentContract;

/// Backward-compatible journal name for the shared execution contract.
pub type StoredExecution = alloyport_core::ExecutionContract;

/// Backward-compatible journal name for the shared environment entry.
pub type StoredEnvironment = alloyport_core::EnvironmentEntry;

/// Backward-compatible journal name for the shared Artifact descriptor.
pub type StoredArtifact = ArtifactDescriptor;

/// Backward-compatible journal name for the shared resource contract.
pub type StoredLimits = alloyport_core::ResourceContract;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum LocalAttemptPhase {
    Accepted = 1,
    Running = 2,
    Finished = 3,
}

impl LocalAttemptPhase {
    pub(crate) fn from_i64(value: i64) -> Result<Self, AttemptStoreError> {
        match value {
            1 => Ok(Self::Accepted),
            2 => Ok(Self::Running),
            3 => Ok(Self::Finished),
            _ => Err(AttemptStoreError::Corrupt(format!(
                "unknown local attempt phase {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredFinished {
    pub outcome: AttemptOutcome,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub receipt: Option<StoredArtifact>,
    pub stdout: Option<StoredArtifact>,
    pub stderr: Option<StoredArtifact>,
    pub detail: String,
}

/// Storage-domain lifecycle payload retained until a server cumulatively acknowledges a delivery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkerOutboxPayload {
    AssignmentAccepted {
        assignment_id: AssignmentId,
        attempt_id: AttemptId,
        already_known: bool,
    },
    AssignmentRejected {
        assignment_id: String,
        attempt_id: String,
        reason: RejectionReason,
        detail: String,
    },
    ExecutionStarted {
        assignment_id: AssignmentId,
        attempt_id: AttemptId,
    },
    ExecutionFinished {
        assignment_id: AssignmentId,
        attempt_id: AttemptId,
        finished: Box<StoredFinished>,
    },
    CancellationAcknowledged {
        assignment_id: AssignmentId,
        attempt_id: AttemptId,
        already_terminal: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerOutboxMessage {
    pub message_id: String,
    pub attempt_id: String,
    pub payload: WorkerOutboxPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAttemptRecord {
    pub assignment: StoredAssignment,
    pub phase: LocalAttemptPhase,
    pub admitted_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished: Option<StoredFinished>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreAdmissionOutcome {
    Inserted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOutboxOutcome {
    Inserted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceLeaseOutcome {
    Acquired,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceReleaseOutcome {
    Released,
    AlreadyReleased,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DevicePreflightOutcome {
    Recorded,
    Duplicate,
}

#[derive(Debug)]
pub enum AttemptStoreError {
    Storage(Box<dyn Error + Send + Sync>),
    Encoding(Box<dyn Error + Send + Sync>),
    LockPoisoned,
    NotFound(String),
    ConflictingAttempt(String),
    InvalidTransition {
        from: LocalAttemptPhase,
        to: LocalAttemptPhase,
    },
    ConflictingFinished(String),
    ConflictingOutboxMessage(String),
    WorkerIdentityMismatch {
        stored: String,
        requested: String,
    },
    DeviceAlreadyLeased {
        device_id: String,
        attempt_id: String,
    },
    ConflictingDeviceLease {
        attempt_id: String,
        device_id: String,
    },
    ConflictingDevicePreflight(String),
    Corrupt(String),
}

impl Display for AttemptStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "worker journal storage error: {error}"),
            Self::Encoding(error) => write!(formatter, "worker journal encoding error: {error}"),
            Self::LockPoisoned => write!(formatter, "worker attempt journal lock is poisoned"),
            Self::NotFound(attempt) => write!(formatter, "local attempt {attempt} is unknown"),
            Self::ConflictingAttempt(attempt) => {
                write!(formatter, "local attempt {attempt} has conflicting content")
            }
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid local transition from {from:?} to {to:?}"
                )
            }
            Self::ConflictingFinished(attempt) => {
                write!(
                    formatter,
                    "local attempt {attempt} has conflicting terminal results"
                )
            }
            Self::ConflictingOutboxMessage(message_id) => {
                write!(
                    formatter,
                    "outbox message {message_id} has conflicting content"
                )
            }
            Self::WorkerIdentityMismatch { stored, requested } => write!(
                formatter,
                "worker journal belongs to {stored}, not requested worker {requested}"
            ),
            Self::DeviceAlreadyLeased {
                device_id,
                attempt_id,
            } => write!(
                formatter,
                "worker device {device_id} is already leased to attempt {attempt_id}"
            ),
            Self::ConflictingDeviceLease {
                attempt_id,
                device_id,
            } => write!(
                formatter,
                "attempt {attempt_id} has a conflicting device lease for {device_id}"
            ),
            Self::ConflictingDevicePreflight(attempt_id) => write!(
                formatter,
                "attempt {attempt_id} has conflicting durable device preflight evidence"
            ),
            Self::Corrupt(detail) => write!(formatter, "corrupt worker attempt journal: {detail}"),
        }
    }
}

impl Error for AttemptStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) | Self::Encoding(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub trait AttemptLifecycleStore: Debug + Send + Sync {
    fn bind_worker(&self, worker_id: &str) -> Result<(), AttemptStoreError>;

    fn admit(
        &self,
        assignment: &StoredAssignment,
        admitted_at_ms: u64,
    ) -> Result<StoreAdmissionOutcome, AttemptStoreError>;

    fn attempt(&self, attempt_id: &str) -> Result<Option<LocalAttemptRecord>, AttemptStoreError>;

    fn attempts(&self) -> Result<Vec<LocalAttemptRecord>, AttemptStoreError>;

    fn mark_running(&self, attempt_id: &str, at_ms: u64) -> Result<(), AttemptStoreError>;

    fn mark_finished(
        &self,
        attempt_id: &str,
        finished: &StoredFinished,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError>;
}

#[allow(clippy::missing_errors_doc)]
pub trait WorkerOutboxStore: Debug + Send + Sync {
    fn enqueue_outbox(
        &self,
        message: &WorkerOutboxMessage,
        at_ms: u64,
    ) -> Result<StoreOutboxOutcome, AttemptStoreError>;

    fn pending_outbox(&self) -> Result<Vec<WorkerOutboxMessage>, AttemptStoreError>;

    fn record_outbox_delivery(
        &self,
        connection_id: &str,
        sequence: u64,
        message_id: &str,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError>;

    fn acknowledge_outbox(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
    ) -> Result<usize, AttemptStoreError>;

    fn prune_outbox_deliveries(&self, older_than_ms: u64) -> Result<usize, AttemptStoreError>;

    fn outbox_len(&self) -> Result<usize, AttemptStoreError>;
}

#[allow(clippy::missing_errors_doc)]
/// Durable accelerator ownership and immutable preflight evidence.
///
/// Implementations must reject unknown or terminal attempts and blank device IDs. Acquisition is
/// exclusive by device and idempotent only for the same active attempt/device pair. Finishing an
/// attempt never releases its lease: terminal cleanup must explicitly release after proving the
/// device reusable, and repeated release is idempotent. Preflight evidence requires an active
/// matching lease in the accepted phase and is immutable once recorded.
pub trait DeviceLeaseStore: Debug + Send + Sync {
    fn acquire_device_lease(
        &self,
        attempt_id: &AttemptId,
        device_id: &str,
        at_ms: u64,
    ) -> Result<DeviceLeaseOutcome, AttemptStoreError>;

    fn release_device_lease(
        &self,
        attempt_id: &AttemptId,
        at_ms: u64,
    ) -> Result<DeviceReleaseOutcome, AttemptStoreError>;

    fn active_device_leases(&self) -> Result<Vec<DeviceLease>, AttemptStoreError>;

    fn record_device_preflight(
        &self,
        attempt_id: &AttemptId,
        observation: &DeviceObservation,
    ) -> Result<DevicePreflightOutcome, AttemptStoreError>;

    fn device_preflight(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Option<DeviceObservation>, AttemptStoreError>;
}

/// Compatibility composition for workers that need both attempt lifecycle and durable outbox.
pub trait AttemptStore: AttemptLifecycleStore + WorkerOutboxStore + DeviceLeaseStore {}

impl<T> AttemptStore for T where
    T: AttemptLifecycleStore + WorkerOutboxStore + DeviceLeaseStore + ?Sized
{
}
