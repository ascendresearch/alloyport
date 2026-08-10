//! Server-side worker sessions backed by a crash-durable control repository.

pub mod artifact;
pub mod storage;

use alloyport_proto::v1::worker_control_server::WorkerControl;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, AssignmentAccepted, AssignmentRejected, CancelAttempt,
    CancellationAcknowledged, ControlAcknowledgement, EnvironmentVariable, ExecutionFinished,
    ExecutionSpec, ExecutionStarted, Heartbeat, ResourceLimits, ServerToWorker, ServerWelcome,
    WorkerHello, WorkerStatus, WorkerToServer, server_to_worker, worker_to_server,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR, ValidationError, validate_assignment};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use storage::{
    ArtifactIdentity, AssignmentContract, AttemptObservation, CancellationStoreOutcome, Clock,
    ConnectionRegistration, ControlRepository, EnvironmentEntry, ExecutionContract,
    FinishedObservation, ObservationDisposition, ObservedAttempt, RepositoryError,
    ResourceContract, ServerFrameKind, ServerOutboxFrame, SqliteControlRepository,
    StoreAssignmentOutcome, SystemClock, WorkerCapabilities, WorkerRegistration,
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

const HEARTBEAT_INTERVAL_MS: u64 = 5_000;
const ATTEMPT_LEASE_MS: u64 = 30_000;
const LEASE_REAPER_INTERVAL_MS: u64 = 1_000;
const OUTBOX_ORPHAN_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

pub use storage::{AttemptState as AssignmentState, LeaseRecord, ManualClock};

/// Read-only worker registry view for scheduling and diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerSnapshot {
    pub worker_id: String,
    pub instance_id: String,
    pub connection_id: String,
    pub connected: bool,
    pub last_worker_sequence: u64,
    pub backend: i32,
}

/// Result of submitting an immutable attempt to one worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    Sent,
    Pending,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    Sent,
    Pending,
    CancelledBeforeSend,
    AlreadyTerminal,
}

/// A server-side assignment cannot be admitted.
#[derive(Debug)]
pub enum EnqueueError {
    Invalid(ValidationError),
    ConflictingAttempt(String),
    Repository(RepositoryError),
}

impl Display for EnqueueError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => Display::fmt(error, formatter),
            Self::ConflictingAttempt(attempt_id) => {
                write!(
                    formatter,
                    "attempt {attempt_id} was reused with different content"
                )
            }
            Self::Repository(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for EnqueueError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::ConflictingAttempt(_) => None,
        }
    }
}

impl From<ValidationError> for EnqueueError {
    fn from(error: ValidationError) -> Self {
        Self::Invalid(error)
    }
}

impl From<RepositoryError> for EnqueueError {
    fn from(error: RepositoryError) -> Self {
        match error {
            RepositoryError::ConflictingAttempt(attempt_id) => Self::ConflictingAttempt(attempt_id),
            other => Self::Repository(other),
        }
    }
}

#[derive(Debug)]
struct WorkerRecord {
    hello: WorkerHello,
    connection_id: String,
    connected: bool,
    last_worker_sequence: u64,
    last_server_sequence_acknowledged: u64,
    next_server_sequence: u64,
    sender: mpsc::Sender<Result<ServerToWorker, Status>>,
}

#[derive(Debug, Default)]
struct ControlState {
    workers: BTreeMap<String, WorkerRecord>,
}

/// Cloneable implementation of the worker-facing gRPC service.
#[derive(Clone, Debug)]
pub struct WorkerControlService {
    state: Arc<Mutex<ControlState>>,
    repository: Arc<dyn ControlRepository>,
    clock: Arc<dyn Clock>,
    connection_counter: Arc<AtomicU64>,
    lease_counter: Arc<AtomicU64>,
}

impl Default for WorkerControlService {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerControlService {
    /// Creates an ephemeral service. The server binary uses [`Self::open_sqlite`] instead.
    ///
    /// # Panics
    ///
    /// Panics only if the bundled `SQLite` library cannot create an in-memory database.
    #[must_use]
    pub fn new() -> Self {
        let repository = SqliteControlRepository::in_memory()
            .expect("an in-memory SQLite control repository must initialize");
        Self::with_repository(Arc::new(repository), Arc::new(SystemClock))
    }

    /// Opens a service whose control state survives process restart.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` cannot open or migrate the database.
    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        Ok(Self::with_repository(
            Arc::new(SqliteControlRepository::open(path)?),
            Arc::new(SystemClock),
        ))
    }

    /// Builds a service around an injected repository and clock.
    #[must_use]
    pub fn with_repository(repository: Arc<dyn ControlRepository>, clock: Arc<dyn Clock>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ControlState::default())),
            repository,
            clock,
            connection_counter: Arc::new(AtomicU64::new(unique_seed())),
            lease_counter: Arc::new(AtomicU64::new(unique_seed())),
        }
    }

    /// Returns the latest in-process registry record for a logical worker.
    pub async fn worker_snapshot(&self, worker_id: &str) -> Option<WorkerSnapshot> {
        let state = self.state.lock().await;
        state.workers.get(worker_id).map(|worker| WorkerSnapshot {
            worker_id: worker.hello.worker_id.clone(),
            instance_id: worker.hello.instance_id.clone(),
            connection_id: worker.connection_id.clone(),
            connected: worker.connected,
            last_worker_sequence: worker.last_worker_sequence,
            backend: worker
                .hello
                .capabilities
                .as_ref()
                .map_or(0, |capabilities| capabilities.backend),
        })
    }

    /// Returns the durable lifecycle state for an attempt.
    ///
    /// # Errors
    ///
    /// Returns a repository error rather than treating a failed read as a missing attempt.
    pub fn assignment_state(
        &self,
        attempt_id: &str,
    ) -> Result<Option<AssignmentState>, RepositoryError> {
        self.repository
            .assignment(attempt_id)
            .map(|record| record.map(|record| record.state))
    }

    /// Returns the current durable lease for an attempt.
    ///
    /// # Errors
    ///
    /// Returns a repository error when the lease cannot be read.
    pub fn lease(&self, attempt_id: &str) -> Result<Option<LeaseRecord>, RepositoryError> {
        self.repository.lease(attempt_id)
    }

    /// Expires every due non-terminal lease according to the injected clock.
    ///
    /// # Errors
    ///
    /// Returns a repository error if expiry cannot be committed atomically.
    pub fn expire_leases(&self) -> Result<Vec<String>, RepositoryError> {
        self.repository.expire_leases(self.clock.now_unix_ms())
    }

    /// Runs the periodic durable lease-expiry loop until its task is cancelled.
    ///
    /// # Errors
    ///
    /// Returns the first repository error instead of silently stopping lease expiry.
    pub async fn run_lease_reaper(&self) -> Result<(), RepositoryError> {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(LEASE_REAPER_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            self.expire_leases()?;
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
        match self
            .repository
            .store_assignment(&worker_id, &contract, self.clock.now_unix_ms())?
        {
            StoreAssignmentOutcome::Duplicate => return Ok(EnqueueOutcome::Duplicate),
            StoreAssignmentOutcome::Inserted => {}
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
        let reassignment = self.repository.reassign_expired(
            expired_attempt_id,
            &replacement_worker_id,
            &replacement_attempt_id,
            self.clock.now_unix_ms(),
        )?;
        validate_assignment(&contract_to_assignment(&reassignment.assignment.contract))?;
        if reassignment.outcome == StoreAssignmentOutcome::Duplicate {
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
        let cancellation =
            self.repository
                .request_cancellation(attempt_id, &reason, self.clock.now_unix_ms())?;
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

    async fn prepare_assignment(
        &self,
        worker_id: &str,
        attempt_id: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if !worker.connected {
            return Ok(None);
        }
        let assignment = self
            .repository
            .assignment(attempt_id)?
            .ok_or_else(|| RepositoryError::NotFound(attempt_id.to_owned()))?;
        if assignment.worker_id != worker_id {
            return Err(RepositoryError::IdentityMismatch(attempt_id.to_owned()));
        }

        let sequence = worker.next_server_sequence;
        let lease_number = self.lease_counter.fetch_add(1, Ordering::Relaxed);
        let lease_id = format!("lease-{lease_number}");
        let now_ms = self.clock.now_unix_ms();
        self.repository.mark_sent_and_grant_lease(
            attempt_id,
            worker_id,
            &lease_id,
            now_ms,
            ATTEMPT_LEASE_MS,
        )?;
        self.repository.record_server_frame(
            &ServerOutboxFrame {
                connection_id: worker.connection_id.clone(),
                sequence,
                message_id: format!("assignment:{attempt_id}"),
                worker_id: worker_id.to_owned(),
                kind: ServerFrameKind::Assignment,
                attempt_id: Some(attempt_id.to_owned()),
            },
            now_ms,
        )?;
        worker.next_server_sequence += 1;
        self.repository.update_connection_sequences(
            &worker.connection_id,
            worker.last_worker_sequence,
            sequence,
            worker.last_server_sequence_acknowledged,
            now_ms,
        )?;
        Ok(Some((
            worker.sender.clone(),
            ServerToWorker {
                sequence,
                acknowledges_worker_through: worker.last_worker_sequence,
                message_id: format!("assignment:{attempt_id}"),
                message: Some(server_to_worker::Message::Assignment(
                    contract_to_assignment(&assignment.contract),
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

    async fn prepare_cancel(
        &self,
        worker_id: &str,
        attempt_id: &str,
        reason: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if !worker.connected {
            return Ok(None);
        }
        let sequence = worker.next_server_sequence;
        let now_ms = self.clock.now_unix_ms();
        self.repository.record_server_frame(
            &ServerOutboxFrame {
                connection_id: worker.connection_id.clone(),
                sequence,
                message_id: format!("cancel:{attempt_id}"),
                worker_id: worker_id.to_owned(),
                kind: ServerFrameKind::Cancel,
                attempt_id: Some(attempt_id.to_owned()),
            },
            now_ms,
        )?;
        worker.next_server_sequence += 1;
        self.repository.update_connection_sequences(
            &worker.connection_id,
            worker.last_worker_sequence,
            sequence,
            worker.last_server_sequence_acknowledged,
            now_ms,
        )?;
        Ok(Some((
            worker.sender.clone(),
            ServerToWorker {
                sequence,
                acknowledges_worker_through: worker.last_worker_sequence,
                message_id: format!("cancel:{attempt_id}"),
                message: Some(server_to_worker::Message::Cancel(CancelAttempt {
                    attempt_id: attempt_id.to_owned(),
                    reason: reason.to_owned(),
                })),
            },
        )))
    }

    async fn register(
        &self,
        hello: WorkerHello,
        sender: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) -> Result<(String, Vec<ServerToWorker>), RepositoryError> {
        let number = self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let connection_id = format!("connection-{number}");
        let worker_id = hello.worker_id.clone();
        let negotiated_protocol_minor = hello.protocol_minor.min(PROTOCOL_MINOR);
        let now_ms = self.clock.now_unix_ms();
        self.repository.expire_leases(now_ms)?;
        self.repository
            .prune_orphaned_server_frames(now_ms.saturating_sub(OUTBOX_ORPHAN_RETENTION_MS))?;
        self.repository.register_worker(
            &hello_to_registration(&hello),
            &ConnectionRegistration {
                connection_id: connection_id.clone(),
                worker_id: worker_id.clone(),
                instance_id: hello.instance_id.clone(),
                connected_at_ms: now_ms,
            },
        )?;

        {
            let mut state = self.state.lock().await;
            state.workers.insert(
                worker_id.clone(),
                WorkerRecord {
                    hello,
                    connection_id: connection_id.clone(),
                    connected: true,
                    last_worker_sequence: 1,
                    last_server_sequence_acknowledged: 0,
                    next_server_sequence: 2,
                    sender,
                },
            );
        }

        let mut messages = vec![ServerToWorker {
            sequence: 1,
            acknowledges_worker_through: 1,
            message_id: String::new(),
            message: Some(server_to_worker::Message::Welcome(ServerWelcome {
                connection_id: connection_id.clone(),
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: negotiated_protocol_minor,
                heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
                attempt_lease_ms: ATTEMPT_LEASE_MS,
            })),
        }];
        let pending = self.repository.replayable_assignments(&worker_id)?;
        for assignment in pending {
            let cancellation_reason = assignment.cancellation_reason.clone();
            if let Some((_, message)) = self
                .prepare_assignment(&worker_id, &assignment.contract.attempt_id)
                .await?
            {
                messages.push(message);
            }
            if let Some(reason) = cancellation_reason
                && let Some((_, message)) = self
                    .prepare_cancel(&worker_id, &assignment.contract.attempt_id, &reason)
                    .await?
            {
                messages.push(message);
            }
        }
        Ok((connection_id, messages))
    }

    async fn ingest(
        &self,
        worker_id: &str,
        connection_id: &str,
        frame: WorkerToServer,
    ) -> Result<bool, Status> {
        let mut state = self.state.lock().await;
        let worker = state
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| Status::failed_precondition("worker is not registered"))?;
        if worker.connection_id != connection_id || !worker.connected {
            return Err(Status::aborted("worker connection was superseded"));
        }
        if frame.sequence != worker.last_worker_sequence + 1 {
            return Err(Status::invalid_argument(format!(
                "worker sequence gap: expected {}, got {}",
                worker.last_worker_sequence + 1,
                frame.sequence
            )));
        }
        let sent_server_through = worker.next_server_sequence.saturating_sub(1);
        validate_worker_acknowledgement(
            frame.acknowledges_server_through,
            worker.last_server_sequence_acknowledged,
            sent_server_through,
        )?;

        let durable_message_id = expected_worker_message_id(frame.message.as_ref());
        let supports_durable_message_ids = worker.hello.protocol_minor >= 2;
        if supports_durable_message_ids {
            if let Some(expected) = durable_message_id.as_ref()
                && frame.message_id != *expected
            {
                return Err(Status::invalid_argument(format!(
                    "worker message ID must be {expected}"
                )));
            }
            if durable_message_id.is_none() && !frame.message_id.is_empty() {
                return Err(Status::invalid_argument(
                    "ephemeral worker frame cannot carry a message ID",
                ));
            }
        }

        let now_ms = self.clock.now_unix_ms();
        match frame.message {
            Some(worker_to_server::Message::Heartbeat(heartbeat)) => {
                self.observe_heartbeat(worker_id, &heartbeat, now_ms)?;
            }
            Some(worker_to_server::Message::Status(status)) => {
                Self::observe_status(worker, status);
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
                self.observe_finished(worker_id, finished, now_ms)?;
            }
            Some(worker_to_server::Message::CancellationAcknowledged(acknowledged)) => {
                self.observe_cancellation_acknowledged(worker_id, acknowledged, now_ms)?;
            }
            Some(worker_to_server::Message::OutputChunk(_)) => {}
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

        worker.last_worker_sequence = frame.sequence;
        worker.last_server_sequence_acknowledged = frame.acknowledges_server_through;
        self.repository
            .compact_server_frames(connection_id, frame.acknowledges_server_through, now_ms)
            .map_err(repository_status)?;
        self.repository
            .update_connection_sequences(
                connection_id,
                worker.last_worker_sequence,
                worker.next_server_sequence.saturating_sub(1),
                worker.last_server_sequence_acknowledged,
                now_ms,
            )
            .map_err(repository_status)?;
        Ok(supports_durable_message_ids && durable_message_id.is_some())
    }

    async fn prepare_transport_ack(
        &self,
        worker_id: &str,
        connection_id: &str,
    ) -> Result<
        Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)>,
        RepositoryError,
    > {
        let mut state = self.state.lock().await;
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return Ok(None);
        };
        if !worker.connected || worker.connection_id != connection_id {
            return Ok(None);
        }
        let sequence = worker.next_server_sequence;
        worker.next_server_sequence += 1;
        self.repository.update_connection_sequences(
            connection_id,
            worker.last_worker_sequence,
            sequence,
            worker.last_server_sequence_acknowledged,
            self.clock.now_unix_ms(),
        )?;
        Ok(Some((
            worker.sender.clone(),
            ServerToWorker {
                sequence,
                acknowledges_worker_through: worker.last_worker_sequence,
                message_id: String::new(),
                message: Some(server_to_worker::Message::Acknowledgement(
                    ControlAcknowledgement {},
                )),
            },
        )))
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

    fn observe_status(_worker: &mut WorkerRecord, _status: WorkerStatus) {}

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
        self.observe(
            worker_id,
            started.assignment_id,
            started.attempt_id,
            now_ms,
            AttemptObservation::Started,
        )
    }

    fn observe_finished(
        &self,
        worker_id: &str,
        finished: ExecutionFinished,
        now_ms: u64,
    ) -> Result<ObservationDisposition, Status> {
        self.observe(
            worker_id,
            finished.assignment_id,
            finished.attempt_id,
            now_ms,
            AttemptObservation::Finished(FinishedObservation {
                outcome: finished.outcome,
                exit_code: finished.exit_code,
                elapsed_ms: finished.elapsed_ms,
                receipt: finished.receipt.as_ref().map(artifact_to_identity),
                stdout: finished.stdout.as_ref().map(artifact_to_identity),
                stderr: finished.stderr.as_ref().map(artifact_to_identity),
                detail: finished.detail,
            }),
        )
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

    async fn disconnect(&self, worker_id: &str, connection_id: &str) {
        let _ = self
            .repository
            .disconnect(connection_id, self.clock.now_unix_ms());
        let mut state = self.state.lock().await;
        if let Some(worker) = state.workers.get_mut(worker_id)
            && worker.connection_id == connection_id
        {
            worker.connected = false;
        }
    }

    async fn consume_stream(
        self,
        worker_id: String,
        connection_id: String,
        mut inbound: Streaming<WorkerToServer>,
        outbound: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) {
        loop {
            match inbound.next().await {
                Some(Ok(frame)) => match self.ingest(&worker_id, &connection_id, frame).await {
                    Ok(true) => {
                        match self.prepare_transport_ack(&worker_id, &connection_id).await {
                            Ok(Some((sender, message))) => {
                                if sender.send(Ok(message)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(None) => break,
                            Err(error) => {
                                let _ = outbound.send(Err(repository_status(error))).await;
                                break;
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(status) => {
                        let _ = outbound.send(Err(status)).await;
                        break;
                    }
                },
                Some(Err(status)) => {
                    let _ = outbound.send(Err(status)).await;
                    break;
                }
                None => break,
            }
        }
        self.disconnect(&worker_id, &connection_id).await;
    }
}

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    type OpenControlStreamStream =
        Pin<Box<dyn Stream<Item = Result<ServerToWorker, Status>> + Send + 'static>>;

    async fn open_control_stream(
        &self,
        request: Request<Streaming<WorkerToServer>>,
    ) -> Result<Response<Self::OpenControlStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("worker stream ended before hello"))?;
        if first.sequence != 1 {
            return Err(Status::invalid_argument("hello must have sequence 1"));
        }
        if first.acknowledges_server_through != 0 {
            return Err(Status::invalid_argument(
                "hello cannot acknowledge a server connection that is not open",
            ));
        }
        if !first.message_id.is_empty() {
            return Err(Status::invalid_argument("hello cannot carry a message ID"));
        }
        let Some(worker_to_server::Message::Hello(hello)) = first.message else {
            return Err(Status::invalid_argument(
                "first worker message must be hello",
            ));
        };
        alloyport_proto::validate_worker_hello(&hello)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let worker_id = hello.worker_id.clone();
        let (outbound, receiver) = mpsc::channel(64);
        let (connection_id, initial_messages) = self
            .register(hello, outbound.clone())
            .await
            .map_err(repository_status)?;
        for message in initial_messages {
            outbound
                .send(Ok(message))
                .await
                .map_err(|_| Status::unavailable("worker response stream closed"))?;
        }

        tokio::spawn(
            self.clone()
                .consume_stream(worker_id, connection_id, inbound, outbound),
        );
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

fn repository_status(error: RepositoryError) -> Status {
    match error {
        RepositoryError::NotFound(detail) => Status::not_found(detail),
        RepositoryError::IdentityMismatch(detail) => Status::permission_denied(detail),
        RepositoryError::InvalidTransition { .. } => Status::failed_precondition(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

fn expected_worker_message_id(message: Option<&worker_to_server::Message>) -> Option<String> {
    let (kind, attempt_id) = match message? {
        worker_to_server::Message::AssignmentAccepted(accepted) => {
            ("assignment-accepted", accepted.attempt_id.as_str())
        }
        worker_to_server::Message::AssignmentRejected(rejected) => {
            ("assignment-rejected", rejected.attempt_id.as_str())
        }
        worker_to_server::Message::ExecutionStarted(started) => {
            ("execution-started", started.attempt_id.as_str())
        }
        worker_to_server::Message::ExecutionFinished(finished) => {
            ("execution-finished", finished.attempt_id.as_str())
        }
        worker_to_server::Message::CancellationAcknowledged(acknowledged) => (
            "cancellation-acknowledged",
            acknowledged.attempt_id.as_str(),
        ),
        worker_to_server::Message::Hello(_)
        | worker_to_server::Message::Heartbeat(_)
        | worker_to_server::Message::OutputChunk(_)
        | worker_to_server::Message::Status(_) => return None,
    };
    Some(format!("{kind}:{attempt_id}"))
}

fn hello_to_registration(hello: &WorkerHello) -> WorkerRegistration {
    let capabilities = hello
        .capabilities
        .as_ref()
        .expect("validated worker hello contains capabilities");
    WorkerRegistration {
        protocol_major: hello.protocol_major,
        protocol_minor: hello.protocol_minor,
        worker_id: hello.worker_id.clone(),
        instance_id: hello.instance_id.clone(),
        worker_version: hello.worker_version.clone(),
        features: hello.features.clone(),
        capabilities: WorkerCapabilities {
            backend: capabilities.backend,
            architecture: capabilities.architecture.clone(),
            device_count: capabilities.device_count,
            max_concurrency: capabilities.max_concurrency,
            driver_version: capabilities.driver_version.clone(),
            toolkit_version: capabilities.toolkit_version.clone(),
            container_runtime: capabilities.container_runtime.clone(),
        },
    }
}

fn assignment_to_contract(assignment: &Assignment) -> AssignmentContract {
    let execution = assignment
        .execution
        .as_ref()
        .expect("validated assignment contains execution");
    AssignmentContract {
        assignment_id: assignment.assignment_id.clone(),
        attempt_id: assignment.attempt_id.clone(),
        attempt_number: assignment.attempt_number,
        idempotency_key: assignment.idempotency_key.clone(),
        task_id: assignment.task_id.clone(),
        candidate_id: assignment.candidate_id.clone(),
        execution: ExecutionContract {
            executor_kind: execution.executor_kind,
            argv: execution.argv.clone(),
            working_directory: execution.working_directory.clone(),
            environment: execution
                .environment
                .iter()
                .map(|entry| EnvironmentEntry {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: execution.timeout_ms,
            bundle: artifact_to_identity(
                execution
                    .bundle
                    .as_ref()
                    .expect("validated assignment contains bundle"),
            ),
            image: artifact_to_identity(
                execution
                    .image
                    .as_ref()
                    .expect("validated assignment contains image"),
            ),
            limits: execution.limits.as_ref().map(|limits| ResourceContract {
                cpu_millis: limits.cpu_millis,
                memory_bytes: limits.memory_bytes,
                disk_bytes: limits.disk_bytes,
                process_count: limits.process_count,
                output_bytes: limits.output_bytes,
                device_count: limits.device_count,
                network: limits.network,
            }),
        },
        required_features: assignment.required_features.clone(),
    }
}

fn contract_to_assignment(contract: &AssignmentContract) -> Assignment {
    Assignment {
        assignment_id: contract.assignment_id.clone(),
        attempt_id: contract.attempt_id.clone(),
        attempt_number: contract.attempt_number,
        idempotency_key: contract.idempotency_key.clone(),
        task_id: contract.task_id.clone(),
        candidate_id: contract.candidate_id.clone(),
        execution: Some(ExecutionSpec {
            executor_kind: contract.execution.executor_kind,
            argv: contract.execution.argv.clone(),
            working_directory: contract.execution.working_directory.clone(),
            environment: contract
                .execution
                .environment
                .iter()
                .map(|entry| EnvironmentVariable {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: contract.execution.timeout_ms,
            bundle: Some(identity_to_artifact(&contract.execution.bundle)),
            image: Some(identity_to_artifact(&contract.execution.image)),
            limits: contract
                .execution
                .limits
                .as_ref()
                .map(|limits| ResourceLimits {
                    cpu_millis: limits.cpu_millis,
                    memory_bytes: limits.memory_bytes,
                    disk_bytes: limits.disk_bytes,
                    process_count: limits.process_count,
                    output_bytes: limits.output_bytes,
                    device_count: limits.device_count,
                    network: limits.network,
                }),
        }),
        required_features: contract.required_features.clone(),
    }
}

fn artifact_to_identity(artifact: &ArtifactRef) -> ArtifactIdentity {
    ArtifactIdentity {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

fn identity_to_artifact(identity: &ArtifactIdentity) -> ArtifactRef {
    ArtifactRef {
        digest: identity.digest.clone(),
        size_bytes: identity.size_bytes,
        media_type: identity.media_type.clone(),
    }
}

fn unique_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn validate_worker_acknowledgement(
    acknowledged: u64,
    last_acknowledged: u64,
    sent_server_through: u64,
) -> Result<(), Status> {
    if acknowledged < last_acknowledged {
        return Err(Status::invalid_argument(format!(
            "worker acknowledgement regressed from {last_acknowledged} to {acknowledged}"
        )));
    }
    if acknowledged > sent_server_through {
        return Err(Status::invalid_argument(format!(
            "worker acknowledged server sequence {acknowledged} beyond sent sequence {sent_server_through}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_acknowledgement_must_be_monotonic_and_not_future() {
        assert!(validate_worker_acknowledgement(3, 2, 3).is_ok());
        assert_eq!(
            validate_worker_acknowledgement(1, 2, 3)
                .expect_err("regression is rejected")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            validate_worker_acknowledgement(4, 2, 3)
                .expect_err("future acknowledgement is rejected")
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
