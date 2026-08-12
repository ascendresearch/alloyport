//! Behavioral tests for fake execution and durable execution coordination.

use super::*;
use crate::AdmissionPolicy;
use alloyport_artifacts::upload::ArtifactReferenceKind;
use alloyport_artifacts::{FilesystemArtifactStore, InMemoryArtifactStore};
use alloyport_core::AttemptOutcome;
use alloyport_events::{Event, OutputStream as EventOutputStream};
use alloyport_proto::v1::{ArtifactRef, ExecutionSpec, ExecutorKind, ResourceLimits};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn execution_artifact_spool_accepts_an_in_memory_port() -> Result<(), ExecutionRuntimeError> {
    let artifacts = Arc::new(InMemoryArtifactStore::new(1024));
    let stored = store_artifact(artifacts.clone(), b"portable".to_vec(), STDOUT_MEDIA_TYPE).await?;
    let digest = stored.digest;
    assert!(artifacts.contains(digest).expect("memory store read"));
    Ok(())
}

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
    let task = tokio::spawn(async move { executor.execute(&input, &cancellation, &sender).await });
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
    let task = tokio::spawn(async move { executor.execute(&input, &cancellation, &sender).await });
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
    let runtime = FakeExecutionRuntime::new("worker-1", artifacts.clone(), 1)?;
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
    assert_eq!(run.finished.outcome, AttemptOutcome::Succeeded);
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
        assert!(artifacts.contains(artifact.digest)?);
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
    assert_eq!(run.finished.outcome, AttemptOutcome::Succeeded);
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
    assert_eq!(finished.finished.outcome, AttemptOutcome::Cancelled);
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
        Err(ExecutionRuntimeError::ArtifactPublication(
            ArtifactPublicationError::Unavailable(detail)
        )) if detail == "unavailable"
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
    assert_eq!(retry.finished.outcome, AttemptOutcome::Succeeded);
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
    ) -> Pin<Box<dyn Future<Output = Result<(), ArtifactPublicationError>> + Send + 'a>> {
        Box::pin(async { Err(ArtifactPublicationError::Unavailable("unavailable".into())) })
    }
}

#[derive(Debug)]
struct RecordingPublisher(Arc<Mutex<Vec<String>>>);

impl ArtifactPublisher for RecordingPublisher {
    fn publish<'a>(
        &'a self,
        references: &'a [ArtifactReferenceIntent],
    ) -> Pin<Box<dyn Future<Output = Result<(), ArtifactPublicationError>> + Send + 'a>> {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| {
                    ArtifactPublicationError::Internal(
                        "publication fixture lock poisoned".to_owned(),
                    )
                })?
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
