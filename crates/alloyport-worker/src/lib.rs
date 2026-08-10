//! Outbound worker client and local assignment admission state.

use alloyport_proto::v1::worker_control_client::WorkerControlClient;
use alloyport_proto::v1::{
    ActiveAttempt, Assignment, AssignmentAccepted, AssignmentRejected, AttemptPhase, ExecutorKind,
    Heartbeat, RejectionReason, ServerToWorker, WorkerHealth, WorkerHello, WorkerToServer,
    server_to_worker, worker_to_server,
};
use alloyport_proto::{ValidationError, validate_assignment, validate_worker_hello};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Endpoint;

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Whether an admitted attempt is new or an idempotent replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    New,
    Duplicate,
}

/// A worker cannot admit or communicate an assignment.
#[derive(Debug)]
pub enum WorkerError {
    InvalidHello(ValidationError),
    InvalidAssignment(ValidationError),
    ConflictingAttempt(String),
    PolicyViolation(String),
    Transport(tonic::transport::Error),
    Rpc(tonic::Status),
    Protocol(String),
    StreamClosed,
}

impl Display for WorkerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHello(error) | Self::InvalidAssignment(error) => {
                Display::fmt(error, formatter)
            }
            Self::ConflictingAttempt(attempt_id) => {
                write!(
                    formatter,
                    "attempt {attempt_id} was replayed with different content"
                )
            }
            Self::PolicyViolation(detail) => write!(formatter, "worker policy rejected: {detail}"),
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Rpc(error) => Display::fmt(error, formatter),
            Self::Protocol(detail) => write!(formatter, "worker protocol error: {detail}"),
            Self::StreamClosed => write!(formatter, "worker control stream closed"),
        }
    }
}

impl Error for WorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHello(error) | Self::InvalidAssignment(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Rpc(error) => Some(error),
            Self::ConflictingAttempt(_)
            | Self::PolicyViolation(_)
            | Self::Protocol(_)
            | Self::StreamClosed => None,
        }
    }
}

impl From<tonic::transport::Error> for WorkerError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<tonic::Status> for WorkerError {
    fn from(error: tonic::Status) -> Self {
        Self::Rpc(error)
    }
}

/// Local rules that remain authoritative even for an authenticated server.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdmissionPolicy {
    allow_shell: bool,
}

impl AdmissionPolicy {
    /// Returns a policy that permits the explicitly policy-gated shell executor.
    #[must_use]
    pub const fn allowing_shell(mut self) -> Self {
        self.allow_shell = true;
        self
    }
}

/// Worker-local attempt knowledge. This becomes disk-backed before real execution is enabled.
#[derive(Clone, Debug)]
pub struct WorkerState {
    policy: AdmissionPolicy,
    assignments: BTreeMap<String, Assignment>,
}

impl Default for WorkerState {
    fn default() -> Self {
        Self::with_policy(AdmissionPolicy::default())
    }
}

impl WorkerState {
    #[must_use]
    pub const fn with_policy(policy: AdmissionPolicy) -> Self {
        Self {
            policy,
            assignments: BTreeMap::new(),
        }
    }

    /// Validates and records an immutable attempt before acknowledging it.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] if validation fails or the same attempt ID is reused for other bytes.
    pub fn admit(&mut self, assignment: Assignment) -> Result<AdmissionOutcome, WorkerError> {
        validate_assignment(&assignment).map_err(WorkerError::InvalidAssignment)?;
        if assignment
            .execution
            .as_ref()
            .is_some_and(|execution| execution.executor_kind == i32::from(ExecutorKind::Shell))
            && !self.policy.allow_shell
        {
            return Err(WorkerError::PolicyViolation(
                "shell executor is disabled".to_owned(),
            ));
        }
        if let Some(existing) = self.assignments.get(&assignment.attempt_id) {
            return if existing == &assignment {
                Ok(AdmissionOutcome::Duplicate)
            } else {
                Err(WorkerError::ConflictingAttempt(
                    assignment.attempt_id.clone(),
                ))
            };
        }
        self.assignments
            .insert(assignment.attempt_id.clone(), assignment);
        Ok(AdmissionOutcome::New)
    }

    #[must_use]
    pub fn contains_attempt(&self, attempt_id: &str) -> bool {
        self.assignments.contains_key(attempt_id)
    }

    fn active_attempts(&self) -> Vec<ActiveAttempt> {
        self.assignments
            .values()
            .map(|assignment| ActiveAttempt {
                assignment_id: assignment.assignment_id.clone(),
                attempt_id: assignment.attempt_id.clone(),
                phase: AttemptPhase::Accepted.into(),
            })
            .collect()
    }
}

/// One outbound worker identity with attempt state that survives stream reconnects in-process.
#[derive(Clone, Debug)]
pub struct OutboundWorker {
    endpoint: Endpoint,
    hello: WorkerHello,
    state: Arc<Mutex<WorkerState>>,
}

impl OutboundWorker {
    /// Constructs a worker after validating its immutable hello contract.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] when the worker identity or capabilities are invalid.
    pub fn new(endpoint: Endpoint, hello: WorkerHello) -> Result<Self, WorkerError> {
        validate_worker_hello(&hello).map_err(WorkerError::InvalidHello)?;
        Ok(Self {
            endpoint,
            hello,
            state: Arc::new(Mutex::new(WorkerState::default())),
        })
    }

    #[must_use]
    pub fn state(&self) -> Arc<Mutex<WorkerState>> {
        Arc::clone(&self.state)
    }

    /// Opens one gRPC session and processes messages until the stream closes.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError`] on transport, framing, validation or identity failures. A supervisor
    /// may reconnect this same value; its in-process attempt map is retained.
    pub async fn run_session(&self) -> Result<(), WorkerError> {
        let channel = self.endpoint.clone().connect().await?;
        let mut client = WorkerControlClient::new(channel);
        let (outbound, receiver) = mpsc::channel(64);

        let mut hello = self.hello.clone();
        hello.active_attempts = self.state.lock().await.active_attempts();
        outbound
            .send(WorkerToServer {
                sequence: 1,
                acknowledges_server_through: 0,
                message: Some(worker_to_server::Message::Hello(hello)),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)?;

        let response = client
            .open_control_stream(Request::new(ReceiverStream::new(receiver)))
            .await?;
        let mut inbound = response.into_inner();
        let mut heartbeat = tokio::time::interval(DEFAULT_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut next_worker_sequence = 2;
        let mut last_server_sequence = 0;

        loop {
            tokio::select! {
                incoming = inbound.message() => {
                    let message = incoming?.ok_or(WorkerError::StreamClosed)?;
                    Self::validate_server_sequence(&message, last_server_sequence)?;
                    last_server_sequence = message.sequence;
                    if self.handle_server_message(
                        message,
                        &outbound,
                        &mut next_worker_sequence,
                        last_server_sequence,
                    ).await? {
                        return Ok(());
                    }
                }
                _ = heartbeat.tick() => {
                    let active_attempts = self.state.lock().await.active_attempts();
                    Self::send(
                        &outbound,
                        &mut next_worker_sequence,
                        last_server_sequence,
                        worker_to_server::Message::Heartbeat(Heartbeat {
                            active_attempts,
                            available_slots: self.available_slots().await,
                            health: WorkerHealth::Ready.into(),
                        }),
                    ).await?;
                }
            }
        }
    }

    fn validate_server_sequence(
        message: &ServerToWorker,
        last_server_sequence: u64,
    ) -> Result<(), WorkerError> {
        if message.sequence != last_server_sequence + 1 {
            return Err(WorkerError::Protocol(format!(
                "server sequence gap: expected {}, got {}",
                last_server_sequence + 1,
                message.sequence
            )));
        }
        Ok(())
    }

    async fn handle_server_message(
        &self,
        frame: ServerToWorker,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledged: u64,
    ) -> Result<bool, WorkerError> {
        match frame.message {
            Some(server_to_worker::Message::Welcome(welcome)) => {
                if welcome.protocol_major != self.hello.protocol_major {
                    return Err(WorkerError::Protocol(format!(
                        "server selected unsupported protocol major {}",
                        welcome.protocol_major
                    )));
                }
                Ok(false)
            }
            Some(server_to_worker::Message::Assignment(assignment)) => {
                let assignment_id = assignment.assignment_id.clone();
                let attempt_id = assignment.attempt_id.clone();
                let response = match self.state.lock().await.admit(assignment) {
                    Ok(outcome) => {
                        worker_to_server::Message::AssignmentAccepted(AssignmentAccepted {
                            assignment_id,
                            attempt_id,
                            already_known: outcome == AdmissionOutcome::Duplicate,
                        })
                    }
                    Err(WorkerError::InvalidAssignment(error)) => {
                        worker_to_server::Message::AssignmentRejected(AssignmentRejected {
                            assignment_id,
                            attempt_id,
                            reason: RejectionReason::Invalid.into(),
                            detail: error.to_string(),
                        })
                    }
                    Err(WorkerError::ConflictingAttempt(_)) => {
                        worker_to_server::Message::AssignmentRejected(AssignmentRejected {
                            assignment_id,
                            attempt_id,
                            reason: RejectionReason::Conflict.into(),
                            detail: "attempt ID conflicts with locally admitted content".to_owned(),
                        })
                    }
                    Err(WorkerError::PolicyViolation(detail)) => {
                        worker_to_server::Message::AssignmentRejected(AssignmentRejected {
                            assignment_id,
                            attempt_id,
                            reason: RejectionReason::Policy.into(),
                            detail,
                        })
                    }
                    Err(error) => return Err(error),
                };
                Self::send(outbound, next_worker_sequence, acknowledged, response).await?;
                Ok(false)
            }
            Some(server_to_worker::Message::Drain(_)) => Ok(true),
            Some(server_to_worker::Message::Cancel(_)) => Ok(false),
            None => Err(WorkerError::Protocol(
                "server message payload is missing".to_owned(),
            )),
        }
    }

    async fn send(
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        message: worker_to_server::Message,
    ) -> Result<(), WorkerError> {
        let sequence = *next_worker_sequence;
        *next_worker_sequence += 1;
        outbound
            .send(WorkerToServer {
                sequence,
                acknowledges_server_through,
                message: Some(message),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)
    }

    async fn available_slots(&self) -> u32 {
        let active = u32::try_from(self.state.lock().await.assignments.len()).unwrap_or(u32::MAX);
        self.hello.capabilities.as_ref().map_or(0, |capabilities| {
            capabilities.max_concurrency.saturating_sub(active)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_proto::v1::{ArtifactRef, ExecutionSpec, ExecutorKind};

    fn artifact(byte: char) -> ArtifactRef {
        ArtifactRef {
            digest: format!("sha256:{}", byte.to_string().repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".to_owned(),
        }
    }

    fn assignment(argv: &str) -> Assignment {
        Assignment {
            assignment_id: "assignment-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            attempt_number: 1,
            idempotency_key: "task-1:build".to_owned(),
            task_id: "task-1".to_owned(),
            candidate_id: "candidate-1".to_owned(),
            execution: Some(ExecutionSpec {
                executor_kind: ExecutorKind::Container.into(),
                argv: vec![argv.to_owned()],
                working_directory: "source".to_owned(),
                environment: Vec::new(),
                timeout_ms: 30_000,
                bundle: Some(artifact('a')),
                image: Some(artifact('b')),
                limits: None,
            }),
            required_features: Vec::new(),
        }
    }

    #[test]
    fn replay_is_idempotent_but_conflicting_content_is_rejected() {
        let mut state = WorkerState::default();
        assert_eq!(
            state.admit(assignment("true")).expect("first admission"),
            AdmissionOutcome::New
        );
        assert_eq!(
            state.admit(assignment("true")).expect("same assignment"),
            AdmissionOutcome::Duplicate
        );
        assert!(matches!(
            state.admit(assignment("false")),
            Err(WorkerError::ConflictingAttempt(attempt)) if attempt == "attempt-1"
        ));
    }

    #[test]
    fn shell_executor_requires_explicit_local_policy() {
        let mut shell = assignment("echo");
        shell
            .execution
            .as_mut()
            .expect("fixture has execution")
            .executor_kind = ExecutorKind::Shell.into();

        assert!(matches!(
            WorkerState::default().admit(shell.clone()),
            Err(WorkerError::PolicyViolation(_))
        ));
        assert_eq!(
            WorkerState::with_policy(AdmissionPolicy::default().allowing_shell())
                .admit(shell)
                .expect("explicit policy allows shell"),
            AdmissionOutcome::New
        );
    }
}
