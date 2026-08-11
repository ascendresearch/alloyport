//! Durable control-plane domain records and repository ports.
//!
//! This module is implementation-independent: generated transport messages, database drivers,
//! transactions, and SQL belong to outer adapters.

mod clock;
mod model;
mod repository;

pub use clock::{Clock, ManualClock, SystemClock};
pub use model::{
    ArtifactIdentity, AssignmentContract, AssignmentDeliveryPreparation, AssignmentRecord,
    AttemptObservation, AttemptState, CancellationRecord, CancellationStoreOutcome,
    ConnectionRegistration, EnvironmentEntry, ExecutionContract, FinishedObservation, LeaseRecord,
    ObservationDisposition, ObservedAttempt, ReassignmentRecord, ResourceContract, ServerFrameKind,
    ServerOutboxFrame, StoreAssignmentOutcome, WorkerCapabilities, WorkerRegistration,
};
pub use repository::{
    AssignmentReadRepository, AssignmentRepository, AssignmentWriteRepository,
    AttemptLifecycleRepository, ControlRepository, RepositoryError, ServerOutboxRepository,
    WorkerConnectionRepository,
};
