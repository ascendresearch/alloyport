//! Typed executor boundary and deterministic fake runtime.

use crate::journal::{LocalAttemptPhase, StoredArtifact, StoredAssignment, StoredFinished};
use crate::{WorkerError, WorkerState};
use alloyport_artifacts::upload::ArtifactReferenceKind;
use alloyport_artifacts::{
    ArtifactStore, ArtifactStoreError, FilesystemArtifactStore, IngestRequest,
};
use alloyport_events::{
    ArtifactRef as EventArtifactRef, Authority, Event, OutputStream as EventOutputStream, Producer,
    ProducerEvent, Visibility,
};
use alloyport_proto::v1::AttemptOutcome;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};

pub(crate) const STDOUT_MEDIA_TYPE: &str = "application/vnd.alloyport.stdout";
pub(crate) const STDERR_MEDIA_TYPE: &str = "application/vnd.alloyport.stderr";
pub(crate) const RECEIPT_MEDIA_TYPE: &str = "application/vnd.alloyport.run-receipt+json";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorInput {
    pub assignment_id: String,
    pub attempt_id: String,
    pub task_id: String,
    pub candidate_id: String,
    pub argv: Vec<String>,
    pub working_directory: String,
    pub environment: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub output_limit_bytes: u64,
}

impl From<&StoredAssignment> for ExecutorInput {
    fn from(assignment: &StoredAssignment) -> Self {
        Self {
            assignment_id: assignment.assignment_id.clone(),
            attempt_id: assignment.attempt_id.clone(),
            task_id: assignment.task_id.clone(),
            candidate_id: assignment.candidate_id.clone(),
            argv: assignment.execution.argv.clone(),
            working_directory: assignment.execution.working_directory.clone(),
            environment: assignment
                .execution
                .environment
                .iter()
                .map(|entry| (entry.name.clone(), entry.value.clone()))
                .collect(),
            timeout_ms: assignment.execution.timeout_ms,
            output_limit_bytes: assignment
                .execution
                .limits
                .as_ref()
                .map_or(u64::MAX, |limits| limits.output_bytes),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FakeStep {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Delay(Duration),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FakeCompletion {
    Exit(i32),
    InfrastructureFailure(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FakeExecutionPlan {
    pub steps: Vec<FakeStep>,
    pub completion: FakeCompletion,
}

impl FakeExecutionPlan {
    #[must_use]
    pub fn successful(steps: Vec<FakeStep>) -> Self {
        Self {
            steps,
            completion: FakeCompletion::Exit(0),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionChunk {
    pub stream: ExecutionStream,
    pub byte_offset: u64,
    pub bytes: Vec<u8>,
}

/// A live, non-durable observation emitted while an execution is running.
///
/// Durable started/finished state remains authoritative in the worker journal. These observations
/// are only a low-latency bridge for the currently connected control session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionObservation {
    Started,
    Output(ExecutionChunk),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorResult {
    pub outcome: AttemptOutcome,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub detail: String,
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    sender: watch::Sender<bool>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.sender.subscribe()
    }
}

#[derive(Clone, Debug)]
pub struct FakeExecutor {
    plan: FakeExecutionPlan,
}

impl FakeExecutor {
    #[must_use]
    pub const fn new(plan: FakeExecutionPlan) -> Self {
        Self { plan }
    }

    pub async fn execute(
        &self,
        input: &ExecutorInput,
        cancellation: &CancellationToken,
        output: &mpsc::Sender<ExecutionChunk>,
    ) -> ExecutorResult {
        let started = Instant::now();
        let deadline = started + Duration::from_millis(input.timeout_ms);
        let mut cancellation = cancellation.subscribe();
        let mut accumulated = OutputAccumulator::new(input.output_limit_bytes);
        let mut logical_elapsed_ms = 0_u64;
        for step in &self.plan.steps {
            let step_result = match step {
                FakeStep::Stdout(bytes) => {
                    accumulated
                        .emit(
                            ExecutionStream::Stdout,
                            bytes,
                            output,
                            &mut cancellation,
                            deadline,
                        )
                        .await
                }
                FakeStep::Stderr(bytes) => {
                    accumulated
                        .emit(
                            ExecutionStream::Stderr,
                            bytes,
                            output,
                            &mut cancellation,
                            deadline,
                        )
                        .await
                }
                FakeStep::Delay(duration) => {
                    let result = wait_step(*duration, &mut cancellation, deadline).await;
                    if result.is_ok() {
                        logical_elapsed_ms = logical_elapsed_ms.saturating_add(
                            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                        );
                    }
                    result
                }
            };
            if let Err(termination) = step_result {
                return terminal_result(
                    termination,
                    if termination == ExecutionTermination::TimedOut {
                        input.timeout_ms
                    } else {
                        logical_elapsed_ms
                    },
                    accumulated.stdout,
                    accumulated.stderr,
                );
            }
        }
        let (outcome, exit_code, detail) = match &self.plan.completion {
            FakeCompletion::Exit(0) => (AttemptOutcome::Succeeded, Some(0), "completed".into()),
            FakeCompletion::Exit(code) => (
                AttemptOutcome::CandidateFailed,
                Some(*code),
                format!("fake executor exited with code {code}"),
            ),
            FakeCompletion::InfrastructureFailure(detail) => {
                (AttemptOutcome::InfraError, None, detail.clone())
            }
        };
        ExecutorResult {
            outcome,
            exit_code,
            elapsed_ms: logical_elapsed_ms,
            stdout: accumulated.stdout,
            stderr: accumulated.stderr,
            detail,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutionTermination {
    Cancelled,
    TimedOut,
    OutputLimitExceeded,
    OutputReceiverClosed,
}

struct OutputAccumulator {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    total_output: u64,
    limit: u64,
}

impl OutputAccumulator {
    const fn new(limit: u64) -> Self {
        Self {
            stdout: Vec::new(),
            stderr: Vec::new(),
            total_output: 0,
            limit,
        }
    }

    async fn emit(
        &mut self,
        stream: ExecutionStream,
        bytes: &[u8],
        output: &mpsc::Sender<ExecutionChunk>,
        cancellation: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> Result<(), ExecutionTermination> {
        let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let next_total = self.total_output.saturating_add(byte_count);
        if next_total > self.limit {
            return Err(ExecutionTermination::OutputLimitExceeded);
        }
        let accumulated = match stream {
            ExecutionStream::Stdout => &mut self.stdout,
            ExecutionStream::Stderr => &mut self.stderr,
        };
        let byte_offset = u64::try_from(accumulated.len()).unwrap_or(u64::MAX);
        accumulated.extend_from_slice(bytes);
        self.total_output = next_total;
        let chunk = ExecutionChunk {
            stream,
            byte_offset,
            bytes: bytes.to_vec(),
        };
        tokio::select! {
            biased;
            () = wait_for_cancellation(cancellation) => Err(ExecutionTermination::Cancelled),
            () = sleep_until(deadline) => Err(ExecutionTermination::TimedOut),
            result = output.send(chunk) => result.map_err(|_| ExecutionTermination::OutputReceiverClosed),
        }
    }
}

async fn wait_step(
    duration: Duration,
    cancellation: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), ExecutionTermination> {
    tokio::select! {
        biased;
        () = wait_for_cancellation(cancellation) => Err(ExecutionTermination::Cancelled),
        () = sleep_until(deadline) => Err(ExecutionTermination::TimedOut),
        () = tokio::time::sleep(duration) => Ok(()),
    }
}

async fn wait_for_cancellation(cancellation: &mut watch::Receiver<bool>) {
    loop {
        if *cancellation.borrow_and_update() {
            return;
        }
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}

fn terminal_result(
    termination: ExecutionTermination,
    elapsed_ms: u64,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> ExecutorResult {
    let (outcome, detail) = match termination {
        ExecutionTermination::Cancelled => (AttemptOutcome::Cancelled, "execution cancelled"),
        ExecutionTermination::TimedOut => (AttemptOutcome::TimedOut, "execution timed out"),
        ExecutionTermination::OutputLimitExceeded => (
            AttemptOutcome::InfraError,
            "execution output limit exceeded",
        ),
        ExecutionTermination::OutputReceiverClosed => (
            AttemptOutcome::InfraError,
            "execution output receiver closed",
        ),
    };
    ExecutorResult {
        outcome,
        exit_code: None,
        elapsed_ms,
        stdout,
        stderr,
        detail: detail.into(),
    }
}

#[derive(Debug)]
pub enum ExecutionRuntimeError {
    Worker(WorkerError),
    Artifact(ArtifactStoreError),
    Serialization(serde_json::Error),
    Executor(String),
    ArtifactPublication(String),
    CleanupAfterCommit(String),
    InvalidConfiguration(&'static str),
    AttemptAlreadyRunning(String),
    MissingAttempt(String),
    MissingTerminalData(String),
    TaskJoin(tokio::task::JoinError),
}

impl Display for ExecutionRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Worker(error) => Display::fmt(error, formatter),
            Self::Artifact(error) => Display::fmt(error, formatter),
            Self::Serialization(error) => Display::fmt(error, formatter),
            Self::Executor(detail) => write!(formatter, "executor failed: {detail}"),
            Self::ArtifactPublication(detail) => {
                write!(formatter, "execution Artifact publication failed: {detail}")
            }
            Self::CleanupAfterCommit(detail) => {
                write!(
                    formatter,
                    "terminal execution committed but cleanup failed: {detail}"
                )
            }
            Self::InvalidConfiguration(detail) => {
                write!(formatter, "invalid executor configuration: {detail}")
            }
            Self::AttemptAlreadyRunning(attempt) => {
                write!(formatter, "attempt {attempt} already has an executor")
            }
            Self::MissingAttempt(attempt) => write!(formatter, "attempt {attempt} is not admitted"),
            Self::MissingTerminalData(attempt) => {
                write!(formatter, "finished attempt {attempt} lacks terminal data")
            }
            Self::TaskJoin(error) => write!(formatter, "executor artifact task failed: {error}"),
        }
    }
}

impl Error for ExecutionRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Worker(error) => Some(error),
            Self::Artifact(error) => Some(error),
            Self::Serialization(error) => Some(error),
            Self::TaskJoin(error) => Some(error),
            Self::ArtifactPublication(_)
            | Self::CleanupAfterCommit(_)
            | Self::Executor(_)
            | Self::AttemptAlreadyRunning(_)
            | Self::InvalidConfiguration(_)
            | Self::MissingAttempt(_)
            | Self::MissingTerminalData(_) => None,
        }
    }
}

impl From<WorkerError> for ExecutionRuntimeError {
    fn from(error: WorkerError) -> Self {
        Self::Worker(error)
    }
}

impl From<ArtifactStoreError> for ExecutionRuntimeError {
    fn from(error: ArtifactStoreError) -> Self {
        Self::Artifact(error)
    }
}

impl From<serde_json::Error> for ExecutionRuntimeError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionRun {
    pub finished: StoredFinished,
    pub events: Vec<ProducerEvent>,
    pub reference_intents: Vec<ArtifactReferenceIntent>,
    pub replayed_terminal: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactReferenceIntent {
    pub reference_key: String,
    pub kind: ArtifactReferenceKind,
    pub purpose: String,
    pub artifact: StoredArtifact,
}

/// Publishes worker-local execution artifacts before terminal lifecycle state becomes reportable.
pub trait ArtifactPublisher: Debug + Send + Sync {
    /// Publishes every reference intent, idempotently resuming any prior partial publication.
    fn publish<'a>(
        &'a self,
        references: &'a [ArtifactReferenceIntent],
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

pub struct FakeExecutionRuntime {
    worker_id: String,
    artifacts: Arc<FilesystemArtifactStore>,
    output_channel_capacity: usize,
    active_attempts: Arc<Mutex<BTreeSet<String>>>,
}

impl Debug for FakeExecutionRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FakeExecutionRuntime")
            .field("worker_id", &self.worker_id)
            .field("output_channel_capacity", &self.output_channel_capacity)
            .finish_non_exhaustive()
    }
}

impl FakeExecutionRuntime {
    /// Creates one deterministic fake runtime over a worker-local Artifact spool.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker identity is empty or the output channel has zero capacity.
    pub fn new(
        worker_id: impl Into<String>,
        artifacts: Arc<FilesystemArtifactStore>,
        output_channel_capacity: usize,
    ) -> Result<Self, ExecutionRuntimeError> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            return Err(ExecutionRuntimeError::InvalidConfiguration(
                "worker identity is empty",
            ));
        }
        if output_channel_capacity == 0 {
            return Err(ExecutionRuntimeError::InvalidConfiguration(
                "output channel capacity is zero",
            ));
        }
        Ok(Self {
            worker_id,
            artifacts,
            output_channel_capacity,
            active_attempts: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    /// Returns the stable logical worker identity recorded in fake receipts and events.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Executes one admitted fake attempt and commits its artifacts before its terminal journal row.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown/concurrent attempts, journal failures, Artifact spool failures,
    /// or receipt serialization failures.
    pub async fn run(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        executor: &FakeExecutor,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionRun, ExecutionRuntimeError> {
        self.run_observed(state, attempt_id, executor, cancellation, |_| {})
            .await
    }

    /// Executes one attempt while forwarding best-effort live observations to a caller.
    ///
    /// The observer is deliberately synchronous: the runtime's own bounded executor channel
    /// remains the backpressure boundary, while a disconnected control session cannot terminate
    /// or stall durable execution.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`].
    pub async fn run_observed<F>(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        executor: &FakeExecutor,
        cancellation: &CancellationToken,
        observer: F,
    ) -> Result<ExecutionRun, ExecutionRuntimeError>
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        self.run_inner(state, attempt_id, executor, cancellation, None, observer)
            .await
    }

    /// Executes one attempt and publishes its artifacts before the terminal journal commit.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`], plus publication failures reported by the
    /// configured publisher.
    pub async fn run_observed_and_publish<F>(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        executor: &FakeExecutor,
        cancellation: &CancellationToken,
        publisher: &dyn ArtifactPublisher,
        observer: F,
    ) -> Result<ExecutionRun, ExecutionRuntimeError>
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        self.run_inner(
            state,
            attempt_id,
            executor,
            cancellation,
            Some(publisher),
            observer,
        )
        .await
    }

    async fn run_inner<F>(
        &self,
        state: &WorkerState,
        attempt_id: &str,
        executor: &FakeExecutor,
        cancellation: &CancellationToken,
        publisher: Option<&dyn ArtifactPublisher>,
        mut observer: F,
    ) -> Result<ExecutionRun, ExecutionRuntimeError>
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        let _claim = AttemptClaim::acquire(Arc::clone(&self.active_attempts), attempt_id)?;
        let attempt = state
            .attempt(attempt_id)?
            .ok_or_else(|| ExecutionRuntimeError::MissingAttempt(attempt_id.into()))?;
        if attempt.phase == LocalAttemptPhase::Finished {
            let finished = attempt
                .finished
                .ok_or_else(|| ExecutionRuntimeError::MissingTerminalData(attempt_id.into()))?;
            return Ok(ExecutionRun {
                reference_intents: terminal_reference_intents(attempt_id, &finished),
                finished,
                events: Vec::new(),
                replayed_terminal: true,
            });
        }
        state.mark_running(attempt_id)?;
        observer(ExecutionObservation::Started);
        let input = ExecutorInput::from(&attempt.assignment);
        let (result, mut events) = self
            .execute_with_events(&input, executor, cancellation, &mut observer)
            .await;
        let persisted = self.persist_result(&input, result).await?;
        let reference_intents = terminal_reference_intents(attempt_id, &persisted.finished);
        if let Some(publisher) = publisher {
            publisher
                .publish(&reference_intents)
                .await
                .map_err(ExecutionRuntimeError::ArtifactPublication)?;
        }
        state.mark_finished(attempt_id, &persisted.finished)?;
        self.append_terminal_events(&input, &persisted, &mut events);
        Ok(ExecutionRun {
            reference_intents,
            finished: persisted.finished,
            events,
            replayed_terminal: false,
        })
    }

    async fn execute_with_events<F>(
        &self,
        input: &ExecutorInput,
        executor: &FakeExecutor,
        cancellation: &CancellationToken,
        observer: &mut F,
    ) -> (ExecutorResult, Vec<ProducerEvent>)
    where
        F: FnMut(ExecutionObservation) + Send,
    {
        let mut events = vec![producer_event(
            &self.worker_id,
            input,
            Event::CommandStarted {
                command: input.argv.join(" "),
                cwd: Some(input.working_directory.clone()),
                execution_site: self.worker_id.clone(),
                description: Some("deterministic fake executor".into()),
            },
        )];
        let (sender, mut receiver) = mpsc::channel(self.output_channel_capacity);
        let result = {
            let execution = executor.execute(input, cancellation, &sender);
            tokio::pin!(execution);
            loop {
                tokio::select! {
                    result = &mut execution => break result,
                    chunk = receiver.recv() => {
                        if let Some(chunk) = chunk {
                            observer(ExecutionObservation::Output(chunk.clone()));
                            events.push(output_event(&self.worker_id, input, &chunk));
                        }
                    }
                }
            }
        };
        drop(sender);
        while let Some(chunk) = receiver.recv().await {
            observer(ExecutionObservation::Output(chunk.clone()));
            events.push(output_event(&self.worker_id, input, &chunk));
        }
        (result, events)
    }

    async fn persist_result(
        &self,
        input: &ExecutorInput,
        result: ExecutorResult,
    ) -> Result<PersistedExecution, ExecutionRuntimeError> {
        let stdout = store_artifact(
            Arc::clone(&self.artifacts),
            result.stdout,
            STDOUT_MEDIA_TYPE,
        )
        .await?;
        let stderr = store_artifact(
            Arc::clone(&self.artifacts),
            result.stderr,
            STDERR_MEDIA_TYPE,
        )
        .await?;
        let receipt_bytes = serde_json::to_vec(&FakeRunReceipt {
            schema_version: 1,
            worker_id: &self.worker_id,
            assignment_id: &input.assignment_id,
            attempt_id: &input.attempt_id,
            task_id: &input.task_id,
            candidate_id: &input.candidate_id,
            outcome: result.outcome.as_str_name(),
            exit_code: result.exit_code,
            elapsed_ms: result.elapsed_ms,
            stdout_digest: &stdout.digest,
            stderr_digest: &stderr.digest,
            detail: &result.detail,
        })?;
        let receipt = store_artifact(
            Arc::clone(&self.artifacts),
            receipt_bytes,
            RECEIPT_MEDIA_TYPE,
        )
        .await?;
        let finished = StoredFinished {
            outcome: result.outcome.into(),
            exit_code: result.exit_code,
            elapsed_ms: result.elapsed_ms,
            receipt: Some(receipt.clone()),
            stdout: Some(stdout.clone()),
            stderr: Some(stderr.clone()),
            detail: result.detail,
        };
        Ok(PersistedExecution {
            finished,
            stdout,
            stderr,
            receipt,
        })
    }

    fn append_terminal_events(
        &self,
        input: &ExecutorInput,
        persisted: &PersistedExecution,
        events: &mut Vec<ProducerEvent>,
    ) {
        for (artifact, reference) in [
            (&persisted.stdout, "stdout"),
            (&persisted.stderr, "stderr"),
            (&persisted.receipt, "receipt"),
        ] {
            events.push(producer_event(
                &self.worker_id,
                input,
                Event::ArtifactProduced {
                    artifact: event_artifact(artifact, reference),
                },
            ));
        }
        events.push(producer_event(
            &self.worker_id,
            input,
            Event::CommandCompleted {
                exit_code: persisted.finished.exit_code.unwrap_or(-1),
                elapsed_ms: persisted.finished.elapsed_ms,
                timed_out: persisted.finished.outcome == i32::from(AttemptOutcome::TimedOut),
                output_artifact: Some(event_artifact(&persisted.stdout, "stdout")),
            },
        ));
    }
}

struct PersistedExecution {
    finished: StoredFinished,
    stdout: StoredArtifact,
    stderr: StoredArtifact,
    receipt: StoredArtifact,
}

struct AttemptClaim {
    attempts: Arc<Mutex<BTreeSet<String>>>,
    attempt_id: String,
}

impl AttemptClaim {
    fn acquire(
        attempts: Arc<Mutex<BTreeSet<String>>>,
        attempt_id: &str,
    ) -> Result<Self, ExecutionRuntimeError> {
        let mut active = attempts
            .lock()
            .map_err(|_| ExecutionRuntimeError::AttemptAlreadyRunning(attempt_id.into()))?;
        if !active.insert(attempt_id.into()) {
            return Err(ExecutionRuntimeError::AttemptAlreadyRunning(
                attempt_id.into(),
            ));
        }
        drop(active);
        Ok(Self {
            attempts,
            attempt_id: attempt_id.into(),
        })
    }
}

impl Drop for AttemptClaim {
    fn drop(&mut self) {
        if let Ok(mut attempts) = self.attempts.lock() {
            attempts.remove(&self.attempt_id);
        }
    }
}

#[derive(Serialize)]
struct FakeRunReceipt<'a> {
    schema_version: u16,
    worker_id: &'a str,
    assignment_id: &'a str,
    attempt_id: &'a str,
    task_id: &'a str,
    candidate_id: &'a str,
    outcome: &'a str,
    exit_code: Option<i32>,
    elapsed_ms: u64,
    stdout_digest: &'a str,
    stderr_digest: &'a str,
    detail: &'a str,
}

pub(crate) async fn store_artifact(
    artifacts: Arc<FilesystemArtifactStore>,
    bytes: Vec<u8>,
    media_type: &'static str,
) -> Result<StoredArtifact, ExecutionRuntimeError> {
    let result = tokio::task::spawn_blocking(move || {
        let mut source = Cursor::new(bytes);
        artifacts.ingest(&mut source, IngestRequest::unverified())
    })
    .await
    .map_err(ExecutionRuntimeError::TaskJoin)??;
    Ok(StoredArtifact {
        digest: result.artifact.digest.to_string(),
        size_bytes: result.artifact.size_bytes,
        media_type: media_type.into(),
    })
}

pub(crate) fn producer_event(
    worker_id: &str,
    input: &ExecutorInput,
    event: Event,
) -> ProducerEvent {
    let mut frame = ProducerEvent::new(
        input.task_id.clone(),
        Producer::new("alloyport-worker", worker_id),
        event,
    );
    frame.task_id = Some(input.task_id.clone());
    frame.operation_id = Some(input.attempt_id.clone());
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame
}

pub(crate) fn output_event(
    worker_id: &str,
    input: &ExecutorInput,
    chunk: &ExecutionChunk,
) -> ProducerEvent {
    let text = String::from_utf8_lossy(&chunk.bytes);
    let display_sanitized = matches!(text, std::borrow::Cow::Owned(_));
    producer_event(
        worker_id,
        input,
        Event::CommandOutput {
            stream: match chunk.stream {
                ExecutionStream::Stdout => EventOutputStream::Stdout,
                ExecutionStream::Stderr => EventOutputStream::Stderr,
            },
            byte_offset: chunk.byte_offset,
            text: text.into_owned(),
            display_sanitized,
        },
    )
}

pub(crate) fn event_artifact(artifact: &StoredArtifact, reference: &str) -> EventArtifactRef {
    EventArtifactRef {
        digest: artifact.digest.clone(),
        media_type: artifact.media_type.clone(),
        size_bytes: artifact.size_bytes,
        reference: reference.into(),
    }
}

pub(crate) fn terminal_reference_intents(
    attempt_id: &str,
    finished: &StoredFinished,
) -> Vec<ArtifactReferenceIntent> {
    let mut references = Vec::new();
    for (artifact, suffix, purpose) in [
        (
            finished.stdout.as_ref(),
            "stdout",
            "complete attempt stdout",
        ),
        (
            finished.stderr.as_ref(),
            "stderr",
            "complete attempt stderr",
        ),
    ] {
        if let Some(artifact) = artifact {
            references.push(ArtifactReferenceIntent {
                reference_key: format!("output:{attempt_id}:{suffix}"),
                kind: ArtifactReferenceKind::AssignmentOutput,
                purpose: purpose.into(),
                artifact: artifact.clone(),
            });
        }
    }
    if let Some(receipt) = finished.receipt.as_ref() {
        references.push(ArtifactReferenceIntent {
            reference_key: format!("receipt:{attempt_id}"),
            kind: ArtifactReferenceKind::Receipt,
            purpose: "attempt run receipt".into(),
            artifact: receipt.clone(),
        });
    }
    references
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AdmissionPolicy;
    use alloyport_artifacts::Sha256Digest;
    use alloyport_events::Event;
    use alloyport_proto::v1::{ArtifactRef, ExecutionSpec, ExecutorKind, ResourceLimits};
    use std::str::FromStr;

    #[tokio::test]
    async fn fake_executor_preserves_offsets_and_obeys_bounded_backpressure() {
        let executor = FakeExecutor::new(FakeExecutionPlan::successful(vec![
            FakeStep::Stdout(b"a".to_vec()),
            FakeStep::Stdout(b"bc".to_vec()),
            FakeStep::Stderr(b"x".to_vec()),
        ]));
        let input = executor_input(1_000, 10);
        let cancellation = CancellationToken::new();
        let (sender, mut receiver) = mpsc::channel(1);
        let task =
            tokio::spawn(async move { executor.execute(&input, &cancellation, &sender).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !task.is_finished(),
            "bounded preview channel must apply backpressure"
        );
        let mut chunks = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            chunks.push(chunk);
        }
        let result = task.await.expect("fake executor task must not panic");
        assert_eq!(result.outcome, AttemptOutcome::Succeeded);
        assert_eq!(result.stdout, b"abc");
        assert_eq!(result.stderr, b"x");
        assert_eq!(
            chunks,
            vec![
                ExecutionChunk {
                    stream: ExecutionStream::Stdout,
                    byte_offset: 0,
                    bytes: b"a".to_vec(),
                },
                ExecutionChunk {
                    stream: ExecutionStream::Stdout,
                    byte_offset: 1,
                    bytes: b"bc".to_vec(),
                },
                ExecutionChunk {
                    stream: ExecutionStream::Stderr,
                    byte_offset: 0,
                    bytes: b"x".to_vec(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn fake_executor_classifies_timeout_cancellation_and_output_limit() {
        let timeout = execute_plan(
            FakeExecutionPlan::successful(vec![FakeStep::Delay(Duration::from_millis(20))]),
            executor_input(2, 10),
            None,
        )
        .await;
        assert_eq!(timeout.outcome, AttemptOutcome::TimedOut);
        assert_eq!(timeout.elapsed_ms, 2);

        let executor = FakeExecutor::new(FakeExecutionPlan::successful(vec![FakeStep::Delay(
            Duration::from_millis(50),
        )]));
        let input = executor_input(1_000, 10);
        let cancellation = CancellationToken::new();
        let cancel_from_test = cancellation.clone();
        let (sender, mut receiver) = mpsc::channel(1);
        let task =
            tokio::spawn(async move { executor.execute(&input, &cancellation, &sender).await });
        cancel_from_test.cancel();
        while receiver.recv().await.is_some() {}
        assert_eq!(
            task.await
                .expect("cancelled executor task must not panic")
                .outcome,
            AttemptOutcome::Cancelled
        );

        let limited = execute_plan(
            FakeExecutionPlan::successful(vec![FakeStep::Stdout(b"four".to_vec())]),
            executor_input(1_000, 3),
            None,
        )
        .await;
        assert_eq!(limited.outcome, AttemptOutcome::InfraError);
        assert!(limited.stdout.is_empty());
        assert!(limited.detail.contains("output limit"));
    }

    #[tokio::test]
    async fn runtime_spools_artifacts_events_and_exactly_one_terminal_result()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let journal = directory.path().join("worker.sqlite3");
        let state = WorkerState::open_sqlite(AdmissionPolicy::default(), &journal)?;
        state.admit(&assignment())?;
        let artifacts = Arc::new(FilesystemArtifactStore::open(
            directory.path().join("spool"),
            1_024,
        )?);
        let runtime = FakeExecutionRuntime::new("worker-1", Arc::clone(&artifacts), 1)?;
        let executor = FakeExecutor::new(FakeExecutionPlan::successful(vec![
            FakeStep::Stdout(b"hello ".to_vec()),
            FakeStep::Stdout(b"world".to_vec()),
            FakeStep::Stderr(b"warning".to_vec()),
        ]));
        let mut observations = Vec::new();
        let run = runtime
            .run_observed(
                &state,
                "attempt-1",
                &executor,
                &CancellationToken::new(),
                |observation| observations.push(observation),
            )
            .await?;
        assert!(!run.replayed_terminal);
        assert_eq!(run.finished.outcome, i32::from(AttemptOutcome::Succeeded));
        assert_live_observations(&observations);
        assert_eq!(state.outbox_len()?, 3);
        assert_eq!(run.reference_intents.len(), 3);
        assert_eq!(
            run.reference_intents
                .iter()
                .map(|reference| reference.kind)
                .collect::<Vec<_>>(),
            vec![
                ArtifactReferenceKind::AssignmentOutput,
                ArtifactReferenceKind::AssignmentOutput,
                ArtifactReferenceKind::Receipt,
            ]
        );
        for artifact in [
            run.finished.stdout.as_ref(),
            run.finished.stderr.as_ref(),
            run.finished.receipt.as_ref(),
        ] {
            let artifact = artifact.expect("runtime persists every terminal artifact");
            assert!(artifacts.contains(Sha256Digest::from_str(&artifact.digest)?)?);
        }
        let output_offsets = run
            .events
            .iter()
            .filter_map(|event| match &event.event {
                Event::CommandOutput {
                    stream,
                    byte_offset,
                    ..
                } => Some((*stream, *byte_offset)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            output_offsets,
            vec![
                (EventOutputStream::Stdout, 0),
                (EventOutputStream::Stdout, 6),
                (EventOutputStream::Stderr, 0),
            ]
        );
        assert!(matches!(
            run.events.first().map(|event| &event.event),
            Some(Event::CommandStarted { .. })
        ));
        assert!(matches!(
            run.events.last().map(|event| &event.event),
            Some(Event::CommandCompleted { .. })
        ));
        let mut sequencer = alloyport_events::EventSequencer::new("task-1");
        for (index, event) in run.events.iter().cloned().enumerate() {
            assert_eq!(sequencer.ingest(event)?.sequence, u64::try_from(index)? + 1);
        }

        let replay = runtime
            .run(&state, "attempt-1", &executor, &CancellationToken::new())
            .await?;
        assert!(replay.replayed_terminal);
        assert_eq!(replay.finished, run.finished);
        assert_eq!(replay.reference_intents, run.reference_intents);
        assert!(replay.events.is_empty());
        assert_eq!(state.outbox_len()?, 3);

        drop(state);
        let reopened = WorkerState::open_sqlite(AdmissionPolicy::default(), journal)?;
        let after_restart = runtime
            .run(&reopened, "attempt-1", &executor, &CancellationToken::new())
            .await?;
        assert!(after_restart.replayed_terminal);
        assert_eq!(after_restart.finished, run.finished);
        Ok(())
    }

    #[tokio::test]
    async fn running_fake_attempt_recovers_deterministically_after_restart()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let journal = directory.path().join("worker.sqlite3");
        {
            let state = WorkerState::open_sqlite(AdmissionPolicy::default(), &journal)?;
            state.admit(&assignment())?;
            state.mark_running("attempt-1")?;
        }
        let state = WorkerState::open_sqlite(AdmissionPolicy::default(), &journal)?;
        let runtime = FakeExecutionRuntime::new(
            "worker-1",
            Arc::new(FilesystemArtifactStore::open(
                directory.path().join("spool"),
                1_024,
            )?),
            1,
        )?;
        let executor = FakeExecutor::new(FakeExecutionPlan::successful(vec![FakeStep::Delay(
            Duration::from_millis(7),
        )]));
        let run = runtime
            .run(&state, "attempt-1", &executor, &CancellationToken::new())
            .await?;
        assert_eq!(run.finished.elapsed_ms, 7);
        assert_eq!(run.finished.outcome, i32::from(AttemptOutcome::Succeeded));
        assert_eq!(state.outbox_len()?, 3);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_rejects_two_executors_for_one_attempt() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let state = Arc::new(WorkerState::default());
        state.admit(&assignment())?;
        let runtime = Arc::new(FakeExecutionRuntime::new(
            "worker-1",
            Arc::new(FilesystemArtifactStore::open(
                directory.path().join("spool"),
                1_024,
            )?),
            1,
        )?);
        let executor = Arc::new(FakeExecutor::new(FakeExecutionPlan::successful(vec![
            FakeStep::Delay(Duration::from_millis(50)),
        ])));
        let cancellation = CancellationToken::new();
        let first = {
            let state = Arc::clone(&state);
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                runtime
                    .run(&state, "attempt-1", &executor, &cancellation)
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(
            runtime
                .run(&state, "attempt-1", &executor, &CancellationToken::new())
                .await,
            Err(ExecutionRuntimeError::AttemptAlreadyRunning(attempt)) if attempt == "attempt-1"
        ));
        cancellation.cancel();
        let finished = first.await??;
        assert_eq!(
            finished.finished.outcome,
            i32::from(AttemptOutcome::Cancelled)
        );
        Ok(())
    }

    #[tokio::test]
    async fn artifact_publication_gates_terminal_commit_and_retries_idempotently()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let state = WorkerState::open_sqlite(
            AdmissionPolicy::default(),
            directory.path().join("worker.sqlite3"),
        )?;
        state.admit(&assignment())?;
        let runtime = FakeExecutionRuntime::new(
            "worker-1",
            Arc::new(FilesystemArtifactStore::open(
                directory.path().join("spool"),
                4_096,
            )?),
            1,
        )?;
        let executor = FakeExecutor::new(FakeExecutionPlan::successful(vec![FakeStep::Stdout(
            b"publish me".to_vec(),
        )]));
        let failed = runtime
            .run_observed_and_publish(
                &state,
                "attempt-1",
                &executor,
                &CancellationToken::new(),
                &RejectingPublisher,
                |_| {},
            )
            .await;
        assert!(matches!(
            failed,
            Err(ExecutionRuntimeError::ArtifactPublication(detail)) if detail == "unavailable"
        ));
        assert!(state.finished_attempt("attempt-1")?.is_none());
        assert_eq!(state.outbox_len()?, 2);

        let published = Arc::new(Mutex::new(Vec::new()));
        let retry = runtime
            .run_observed_and_publish(
                &state,
                "attempt-1",
                &executor,
                &CancellationToken::new(),
                &RecordingPublisher(Arc::clone(&published)),
                |_| {},
            )
            .await?;
        assert_eq!(retry.finished.outcome, i32::from(AttemptOutcome::Succeeded));
        assert_eq!(state.outbox_len()?, 3);
        assert_eq!(
            *published.lock().expect("publication fixture lock"),
            vec![
                "output:attempt-1:stdout",
                "output:attempt-1:stderr",
                "receipt:attempt-1",
            ]
        );
        Ok(())
    }

    #[derive(Debug)]
    struct RejectingPublisher;

    impl ArtifactPublisher for RejectingPublisher {
        fn publish<'a>(
            &'a self,
            _references: &'a [ArtifactReferenceIntent],
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Err("unavailable".into()) })
        }
    }

    #[derive(Debug)]
    struct RecordingPublisher(Arc<Mutex<Vec<String>>>);

    impl ArtifactPublisher for RecordingPublisher {
        fn publish<'a>(
            &'a self,
            references: &'a [ArtifactReferenceIntent],
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .map_err(|_| "publication fixture lock poisoned".to_owned())?
                    .extend(
                        references
                            .iter()
                            .map(|reference| reference.reference_key.clone()),
                    );
                Ok(())
            })
        }
    }

    async fn execute_plan(
        plan: FakeExecutionPlan,
        input: ExecutorInput,
        cancellation: Option<CancellationToken>,
    ) -> ExecutorResult {
        let executor = FakeExecutor::new(plan);
        let cancellation = cancellation.unwrap_or_default();
        let (sender, mut receiver) = mpsc::channel(8);
        let execution = executor.execute(&input, &cancellation, &sender);
        tokio::pin!(execution);
        loop {
            tokio::select! {
                result = &mut execution => return result,
                chunk = receiver.recv() => {
                    assert!(chunk.is_some(), "preview channel remains open while executing");
                }
            }
        }
    }

    fn assert_live_observations(observations: &[ExecutionObservation]) {
        assert_eq!(
            observations,
            [
                ExecutionObservation::Started,
                ExecutionObservation::Output(ExecutionChunk {
                    stream: ExecutionStream::Stdout,
                    byte_offset: 0,
                    bytes: b"hello ".to_vec(),
                }),
                ExecutionObservation::Output(ExecutionChunk {
                    stream: ExecutionStream::Stdout,
                    byte_offset: 6,
                    bytes: b"world".to_vec(),
                }),
                ExecutionObservation::Output(ExecutionChunk {
                    stream: ExecutionStream::Stderr,
                    byte_offset: 0,
                    bytes: b"warning".to_vec(),
                }),
            ]
        );
    }

    fn executor_input(timeout_ms: u64, output_limit_bytes: u64) -> ExecutorInput {
        ExecutorInput {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            task_id: "task-1".into(),
            candidate_id: "candidate-1".into(),
            argv: vec!["fake".into()],
            working_directory: "source".into(),
            environment: BTreeMap::new(),
            timeout_ms,
            output_limit_bytes,
        }
    }

    fn assignment() -> alloyport_proto::v1::Assignment {
        alloyport_proto::v1::Assignment {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            attempt_number: 1,
            idempotency_key: "task-1:fake".into(),
            task_id: "task-1".into(),
            candidate_id: "candidate-1".into(),
            execution: Some(ExecutionSpec {
                executor_kind: ExecutorKind::Container.into(),
                argv: vec!["fake".into()],
                working_directory: "source".into(),
                environment: Vec::new(),
                timeout_ms: 1_000,
                bundle: Some(artifact('a')),
                image: Some(artifact('b')),
                limits: Some(ResourceLimits {
                    output_bytes: 1_024,
                    ..ResourceLimits::default()
                }),
            }),
            required_features: Vec::new(),
        }
    }

    fn artifact(byte: char) -> ArtifactRef {
        ArtifactRef {
            digest: format!("sha256:{}", byte.to_string().repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".into(),
        }
    }
}
