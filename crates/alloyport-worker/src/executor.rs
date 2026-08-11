//! Typed executor boundary and deterministic fake runtime.

use crate::artifact_input::ArtifactInputError;
use crate::journal::{LocalAttemptPhase, StoredArtifact, StoredFinished};
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
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub(crate) const STDOUT_MEDIA_TYPE: &str = "application/vnd.alloyport.stdout";
pub(crate) const STDERR_MEDIA_TYPE: &str = "application/vnd.alloyport.stderr";
pub(crate) const RECEIPT_MEDIA_TYPE: &str = "application/vnd.alloyport.run-receipt+json";

pub use crate::fake_executor::{
    CancellationToken, ExecutionChunk, ExecutionObservation, ExecutionStream, ExecutorInput,
    ExecutorResult, FakeCompletion, FakeExecutionPlan, FakeExecutor, FakeStep,
};

#[derive(Debug)]
pub enum ExecutionRuntimeError {
    Worker(WorkerError),
    Artifact(ArtifactStoreError),
    ArtifactInput(ArtifactInputError),
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
            Self::ArtifactInput(error) => Display::fmt(error, formatter),
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
            Self::ArtifactInput(error) => Some(error),
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

impl From<ArtifactInputError> for ExecutionRuntimeError {
    fn from(error: ArtifactInputError) -> Self {
        Self::ArtifactInput(error)
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
            .attempt_async(attempt_id.to_owned())
            .await?
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
        state.mark_running_async(attempt_id.to_owned()).await?;
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
        state
            .mark_finished_async(attempt_id.to_owned(), persisted.finished.clone())
            .await?;
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
#[path = "executor_tests.rs"]
mod tests;
