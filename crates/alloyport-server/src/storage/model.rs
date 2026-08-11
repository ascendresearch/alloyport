//! Transport-independent control-plane records and lifecycle values.

use super::RepositoryError;
use alloyport_core::{AttemptOutcome, ExecutionKind, NetworkPolicy};
use serde::{Deserialize, Serialize};

/// Worker registration persisted independently of its current network session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub protocol_major: u32,
    pub protocol_minor: u32,
    pub worker_id: String,
    pub instance_id: String,
    pub worker_version: String,
    pub features: Vec<String>,
    pub capabilities: WorkerCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub backend: i32,
    pub architecture: String,
    pub device_count: u32,
    pub max_concurrency: u32,
    pub driver_version: String,
    pub toolkit_version: String,
    pub container_runtime: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionRegistration {
    pub connection_id: String,
    pub worker_id: String,
    pub instance_id: String,
    pub connected_at_ms: u64,
}

/// Storage-domain form of an immutable assignment contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AssignmentContract {
    pub assignment_id: String,
    pub attempt_id: String,
    pub attempt_number: u32,
    pub idempotency_key: String,
    pub task_id: String,
    pub candidate_id: String,
    pub execution: ExecutionContract,
    pub required_features: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionContract {
    pub executor_kind: ExecutionKind,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub environment: Vec<EnvironmentEntry>,
    pub timeout_ms: u64,
    pub bundle: ArtifactIdentity,
    pub image: ArtifactIdentity,
    pub limits: Option<ResourceContract>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentEntry {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactIdentity {
    pub digest: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceContract {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
    pub device_count: u32,
    pub network: NetworkPolicy,
}

/// Durable server-side lifecycle for a process attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i64)]
pub enum AttemptState {
    Preparing = 10,
    Dispatchable = 1,
    Sent = 2,
    Accepted = 3,
    Running = 4,
    Finished = 5,
    Rejected = 6,
    LeaseExpired = 7,
    CancelRequested = 8,
    Cancelled = 9,
}

impl AttemptState {
    pub(crate) fn from_i64(value: i64) -> Result<Self, RepositoryError> {
        match value {
            1 => Ok(Self::Dispatchable),
            2 => Ok(Self::Sent),
            3 => Ok(Self::Accepted),
            4 => Ok(Self::Running),
            5 => Ok(Self::Finished),
            6 => Ok(Self::Rejected),
            7 => Ok(Self::LeaseExpired),
            8 => Ok(Self::CancelRequested),
            9 => Ok(Self::Cancelled),
            10 => Ok(Self::Preparing),
            _ => Err(RepositoryError::Corrupt(format!(
                "unknown attempt state {value}"
            ))),
        }
    }

    pub(crate) const fn is_replayable(self) -> bool {
        matches!(
            self,
            Self::Dispatchable
                | Self::Sent
                | Self::Accepted
                | Self::Running
                | Self::CancelRequested
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentRecord {
    pub worker_id: String,
    pub contract: AssignmentContract,
    pub state: AttemptState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub cancellation_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseRecord {
    pub attempt_id: String,
    pub lease_id: String,
    pub worker_id: String,
    pub granted_at_ms: u64,
    pub renewed_at_ms: u64,
    pub expires_at_ms: u64,
    pub expired_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AttemptObservation {
    Accepted { already_known: bool },
    Rejected { reason: i32, detail: String },
    Started,
    Finished(FinishedObservation),
    CancellationAcknowledged { already_terminal: bool },
}

impl AttemptObservation {
    pub(crate) const fn target_state(&self) -> Option<AttemptState> {
        match self {
            Self::Accepted { .. } => Some(AttemptState::Accepted),
            Self::Rejected { .. } => Some(AttemptState::Rejected),
            Self::Started => Some(AttemptState::Running),
            Self::Finished(_) => Some(AttemptState::Finished),
            Self::CancellationAcknowledged { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FinishedObservation {
    pub outcome: AttemptOutcome,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub receipt: Option<ArtifactIdentity>,
    pub stdout: Option<ArtifactIdentity>,
    pub stderr: Option<ArtifactIdentity>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedAttempt {
    pub assignment_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub observed_at_ms: u64,
    pub observation: AttemptObservation,
}

/// Whether a worker observation advanced authoritative attempt state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum ObservationDisposition {
    Applied = 1,
    Duplicate = 2,
    Stale = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreAssignmentOutcome {
    Inserted,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReassignmentRecord {
    pub outcome: StoreAssignmentOutcome,
    pub assignment: AssignmentRecord,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationStoreOutcome {
    Requested,
    Duplicate,
    CancelledBeforeSend,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancellationRecord {
    pub worker_id: String,
    pub outcome: CancellationStoreOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum ServerFrameKind {
    Assignment = 1,
    Cancel = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerOutboxFrame {
    pub connection_id: String,
    pub sequence: u64,
    pub message_id: String,
    pub worker_id: String,
    pub kind: ServerFrameKind,
    pub attempt_id: Option<String>,
}

/// All durable inputs required before an assignment frame may be published.
///
/// Repository implementations must apply the attempt transition, lease grant, outbox insert, and
/// connection sequence update in one transaction. Returning successfully is the application's
/// permission to place the corresponding frame on the network.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentDeliveryPreparation {
    pub frame: ServerOutboxFrame,
    pub lease_id: String,
    pub last_worker_sequence: u64,
    pub last_server_acknowledged_by_worker: u64,
    pub now_ms: u64,
    pub lease_duration_ms: u64,
}
