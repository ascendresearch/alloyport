//! Deterministic executor contract, cancellation, output limits, and fake process behavior.

use crate::journal::StoredAssignment;
use alloyport_proto::v1::AttemptOutcome;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until};

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
