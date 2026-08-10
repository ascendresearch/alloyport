//! Server-side worker registry and the first durable-assignment state model.

use alloyport_proto::v1::worker_control_server::WorkerControl;
use alloyport_proto::v1::{
    Assignment, AssignmentRejected, ExecutionFinished, ExecutionStarted, Heartbeat, ServerToWorker,
    ServerWelcome, WorkerHello, WorkerStatus, WorkerToServer, server_to_worker, worker_to_server,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR, ValidationError, validate_assignment};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

const HEARTBEAT_INTERVAL_MS: u64 = 5_000;
const ATTEMPT_LEASE_MS: u64 = 30_000;

/// Server-side lifecycle observed for an attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignmentState {
    Queued,
    Sent,
    Accepted,
    Running,
    Finished,
    Rejected,
}

impl AssignmentState {
    const fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Rejected)
    }
}

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

/// A server-side assignment cannot be admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnqueueError {
    Invalid(ValidationError),
    ConflictingAttempt(String),
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
        }
    }
}

impl Error for EnqueueError {}

impl From<ValidationError> for EnqueueError {
    fn from(error: ValidationError) -> Self {
        Self::Invalid(error)
    }
}

#[derive(Debug)]
struct WorkerRecord {
    hello: WorkerHello,
    connection_id: String,
    connected: bool,
    last_worker_sequence: u64,
    next_server_sequence: u64,
    sender: mpsc::Sender<Result<ServerToWorker, Status>>,
}

#[derive(Clone, Debug)]
struct AssignmentRecord {
    worker_id: String,
    assignment: Assignment,
    state: AssignmentState,
}

#[derive(Debug, Default)]
struct ControlState {
    workers: BTreeMap<String, WorkerRecord>,
    assignments: BTreeMap<String, AssignmentRecord>,
}

/// Cloneable implementation of the worker-facing gRPC service.
#[derive(Clone, Debug)]
pub struct WorkerControlService {
    state: Arc<Mutex<ControlState>>,
    connection_counter: Arc<AtomicU64>,
}

impl Default for WorkerControlService {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerControlService {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ControlState::default())),
            connection_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns the latest registry record for a logical worker.
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

    /// Returns the server's current lifecycle state for an attempt.
    pub async fn assignment_state(&self, attempt_id: &str) -> Option<AssignmentState> {
        self.state
            .lock()
            .await
            .assignments
            .get(attempt_id)
            .map(|record| record.state)
    }

    /// Persists and, if connected, sends an assignment to a named worker.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError`] for an invalid assignment or an attempt identifier reused with
    /// different content.
    pub async fn enqueue_assignment(
        &self,
        worker_id: impl Into<String>,
        assignment: Assignment,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        validate_assignment(&assignment)?;
        let worker_id = worker_id.into();
        let attempt_id = assignment.attempt_id.clone();

        let outbound = {
            let mut state = self.state.lock().await;
            if let Some(existing) = state.assignments.get(&attempt_id) {
                if existing.worker_id == worker_id && existing.assignment == assignment {
                    return Ok(EnqueueOutcome::Duplicate);
                }
                return Err(EnqueueError::ConflictingAttempt(attempt_id));
            }

            state.assignments.insert(
                attempt_id.clone(),
                AssignmentRecord {
                    worker_id: worker_id.clone(),
                    assignment: assignment.clone(),
                    state: AssignmentState::Queued,
                },
            );
            Self::prepare_assignment(&mut state, &worker_id, &attempt_id)
        };

        let Some((sender, message)) = outbound else {
            return Ok(EnqueueOutcome::Pending);
        };
        if sender.send(Ok(message)).await.is_err() {
            self.mark_send_failed(&worker_id, &attempt_id).await;
            return Ok(EnqueueOutcome::Pending);
        }
        Ok(EnqueueOutcome::Sent)
    }

    fn prepare_assignment(
        state: &mut ControlState,
        worker_id: &str,
        attempt_id: &str,
    ) -> Option<(mpsc::Sender<Result<ServerToWorker, Status>>, ServerToWorker)> {
        let worker = state.workers.get_mut(worker_id)?;
        if !worker.connected {
            return None;
        }
        let sequence = worker.next_server_sequence;
        worker.next_server_sequence += 1;
        let sender = worker.sender.clone();
        let acknowledged = worker.last_worker_sequence;
        let assignment = state.assignments.get_mut(attempt_id)?;
        assignment.state = AssignmentState::Sent;
        Some((
            sender,
            ServerToWorker {
                sequence,
                acknowledges_worker_through: acknowledged,
                message: Some(server_to_worker::Message::Assignment(
                    assignment.assignment.clone(),
                )),
            },
        ))
    }

    async fn mark_send_failed(&self, worker_id: &str, attempt_id: &str) {
        let mut state = self.state.lock().await;
        if let Some(worker) = state.workers.get_mut(worker_id) {
            worker.connected = false;
        }
        if let Some(assignment) = state.assignments.get_mut(attempt_id) {
            assignment.state = AssignmentState::Queued;
        }
    }

    async fn register(
        &self,
        hello: WorkerHello,
        sender: mpsc::Sender<Result<ServerToWorker, Status>>,
    ) -> (String, Vec<ServerToWorker>) {
        let number = self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let connection_id = format!("connection-{number}");
        let worker_id = hello.worker_id.clone();
        let mut state = self.state.lock().await;
        state.workers.insert(
            worker_id.clone(),
            WorkerRecord {
                hello,
                connection_id: connection_id.clone(),
                connected: true,
                last_worker_sequence: 1,
                next_server_sequence: 2,
                sender,
            },
        );

        let mut messages = vec![ServerToWorker {
            sequence: 1,
            acknowledges_worker_through: 1,
            message: Some(server_to_worker::Message::Welcome(ServerWelcome {
                connection_id: connection_id.clone(),
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
                heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
                attempt_lease_ms: ATTEMPT_LEASE_MS,
            })),
        }];

        let pending: Vec<String> = state
            .assignments
            .iter()
            .filter(|(_, record)| record.worker_id == worker_id && !record.state.is_terminal())
            .map(|(attempt_id, _)| attempt_id.clone())
            .collect();
        for attempt_id in pending {
            if let Some((_, message)) =
                Self::prepare_assignment(&mut state, &worker_id, &attempt_id)
            {
                messages.push(message);
            }
        }
        (connection_id, messages)
    }

    async fn ingest(
        &self,
        worker_id: &str,
        connection_id: &str,
        frame: WorkerToServer,
    ) -> Result<(), Status> {
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
        worker.last_worker_sequence = frame.sequence;

        match frame.message {
            Some(worker_to_server::Message::Heartbeat(heartbeat)) => {
                Self::observe_heartbeat(worker, heartbeat);
            }
            Some(worker_to_server::Message::Status(status)) => {
                Self::observe_status(worker, status);
            }
            Some(worker_to_server::Message::AssignmentAccepted(accepted)) => {
                Self::transition_assignment(
                    &mut state,
                    worker_id,
                    &accepted.assignment_id,
                    &accepted.attempt_id,
                    AssignmentState::Accepted,
                )?;
            }
            Some(worker_to_server::Message::AssignmentRejected(rejected)) => {
                Self::observe_rejection(&mut state, worker_id, &rejected)?;
            }
            Some(worker_to_server::Message::ExecutionStarted(started)) => {
                Self::observe_started(&mut state, worker_id, &started)?;
            }
            Some(worker_to_server::Message::ExecutionFinished(finished)) => {
                Self::observe_finished(&mut state, worker_id, &finished)?;
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
        Ok(())
    }

    fn observe_heartbeat(_worker: &mut WorkerRecord, _heartbeat: Heartbeat) {}

    fn observe_status(_worker: &mut WorkerRecord, _status: WorkerStatus) {}

    fn observe_rejection(
        state: &mut ControlState,
        worker_id: &str,
        rejected: &AssignmentRejected,
    ) -> Result<(), Status> {
        Self::transition_assignment(
            state,
            worker_id,
            &rejected.assignment_id,
            &rejected.attempt_id,
            AssignmentState::Rejected,
        )
    }

    fn observe_started(
        state: &mut ControlState,
        worker_id: &str,
        started: &ExecutionStarted,
    ) -> Result<(), Status> {
        Self::transition_assignment(
            state,
            worker_id,
            &started.assignment_id,
            &started.attempt_id,
            AssignmentState::Running,
        )
    }

    fn observe_finished(
        state: &mut ControlState,
        worker_id: &str,
        finished: &ExecutionFinished,
    ) -> Result<(), Status> {
        Self::transition_assignment(
            state,
            worker_id,
            &finished.assignment_id,
            &finished.attempt_id,
            AssignmentState::Finished,
        )
    }

    fn transition_assignment(
        state: &mut ControlState,
        worker_id: &str,
        assignment_id: &str,
        attempt_id: &str,
        target: AssignmentState,
    ) -> Result<(), Status> {
        let assignment = state
            .assignments
            .get_mut(attempt_id)
            .ok_or_else(|| Status::not_found("attempt is not assigned"))?;
        if assignment.worker_id != worker_id || assignment.assignment.assignment_id != assignment_id
        {
            return Err(Status::permission_denied(
                "attempt identity does not match worker",
            ));
        }
        assignment.state = target;
        Ok(())
    }

    async fn disconnect(&self, worker_id: &str, connection_id: &str) {
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
                Some(Ok(frame)) => {
                    if let Err(status) = self.ingest(&worker_id, &connection_id, frame).await {
                        let _ = outbound.send(Err(status)).await;
                        break;
                    }
                }
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
        let Some(worker_to_server::Message::Hello(hello)) = first.message else {
            return Err(Status::invalid_argument(
                "first worker message must be hello",
            ));
        };
        alloyport_proto::validate_worker_hello(&hello)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let worker_id = hello.worker_id.clone();
        let (outbound, receiver) = mpsc::channel(64);
        let (connection_id, initial_messages) = self.register(hello, outbound.clone()).await;
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
