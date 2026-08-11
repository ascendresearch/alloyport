use alloyport_artifacts::upload::{ArtifactReferenceKind, BeginUpload, UploadQuotas};
use alloyport_artifacts::{FilesystemArtifactStore, Sha256Digest, SqliteUploadStore};
use alloyport_core::AttemptOutcome;
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, Backend, ExecutionSpec, ExecutorKind, WorkerCapabilities, WorkerHello,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_server::adapters::sqlite::SqliteControlRepository;
use alloyport_server::artifact::{ArtifactAccessPolicy, ArtifactServiceImpl};
use alloyport_server::interaction::InteractionRunAccessStore;
use alloyport_server::{
    AssignmentState, CancelOutcome, EnqueueOutcome, ManualClock, WorkerControlService,
};
use alloyport_worker::artifact_upload::RemoteArtifactPublisher;
use alloyport_worker::executor::{FakeExecutionPlan, FakeExecutionRuntime, FakeExecutor, FakeStep};
use alloyport_worker::{OutboundWorker, StoredFinished};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Extensions, Status};

#[tokio::test]
async fn owner_enqueue_publishes_through_shared_hub_and_grant_survives_restart()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("control.sqlite3");
    let (service, hub) = WorkerControlService::open_sqlite_with_interaction_hub(&database)?;
    let mut subscription = hub.subscribe("task-1", 0)?;

    assert_eq!(
        service
            .enqueue_assignment_for_owner("owner-a", "cuda-1", assignment())
            .await?,
        EnqueueOutcome::Pending
    );
    let started = tokio::time::timeout(Duration::from_secs(1), subscription.recv()).await??;
    assert_eq!(started.run_id, "task-1");
    assert_eq!(started.sequence, 1);
    assert!(hub.can_read_run("task-1", "owner-a")?);
    drop(service);
    drop(hub);

    let (_, reopened_hub) = WorkerControlService::open_sqlite_with_interaction_hub(database)?;
    assert!(reopened_hub.can_read_run("task-1", "owner-a")?);
    Ok(())
}

#[tokio::test]
async fn worker_without_execution_backend_rejects_before_local_admission()
-> Result<(), Box<dyn Error>> {
    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?;
    let worker_state = worker.state();
    let worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|worker| worker.connected)
    })
    .await?;

    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Rejected)
    })
    .await?;
    assert!(!worker_state.contains_attempt("attempt-1")?);

    worker_task.abort();
    let _ = worker_task.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn worker_handshake_assignment_and_duplicate_suppression() -> Result<(), Box<dyn Error>> {
    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_admission_only_mode();
    let worker_state = worker.state();
    let worker_task = tokio::spawn(async move { worker.run_session().await });

    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|worker| worker.connected)
    })
    .await?;

    let assignment = assignment();
    assert_eq!(
        service
            .enqueue_assignment("cuda-1", assignment.clone())
            .await?,
        EnqueueOutcome::Sent
    );
    assert!(
        service.lease("attempt-1")?.is_some(),
        "the lease must be durable before enqueue reports a send"
    );
    wait_until(|| async { worker_state.contains_attempt("attempt-1").unwrap_or(false) }).await?;
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;
    wait_until(|| async { worker_state.outbox_len().ok() == Some(0) }).await?;

    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment).await?,
        EnqueueOutcome::Duplicate
    );

    worker_task.abort();
    let _ = worker_task.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn queued_assignment_is_recovered_after_server_restart() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("control.sqlite3");
    {
        let first_server = WorkerControlService::open_sqlite(&database)?;
        assert_eq!(
            first_server
                .enqueue_assignment("cuda-1", assignment())
                .await?,
            EnqueueOutcome::Pending
        );
        assert_eq!(
            first_server.assignment_state("attempt-1")?,
            Some(AssignmentState::Dispatchable)
        );
    }

    let recovered_server = WorkerControlService::open_sqlite(&database)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let grpc_service = recovered_server.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_admission_only_mode();
    let worker_state = worker.state();
    let worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async { worker_state.contains_attempt("attempt-1").unwrap_or(false) }).await?;
    wait_until(|| async {
        recovered_server
            .assignment_state("attempt-1")
            .ok()
            .flatten()
            == Some(AssignmentState::Accepted)
    })
    .await?;
    assert!(recovered_server.lease("attempt-1")?.is_some());

    worker_task.abort();
    let _ = worker_task.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn accepted_assignment_replay_after_restart_does_not_regress_state()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("control.sqlite3");
    let first_server = WorkerControlService::open_sqlite(&database)?;
    let first_listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = first_listener.local_addr()?;
    let (first_shutdown_send, first_shutdown_receive) = oneshot::channel();
    let first_grpc_service = first_server.clone();
    let first_server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(first_grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(first_listener), async {
                let _ = first_shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_admission_only_mode();
    let first_session_worker = worker.clone();
    let first_session = tokio::spawn(async move { first_session_worker.run_session().await });
    wait_until(|| async {
        first_server
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        first_server
            .enqueue_assignment("cuda-1", assignment())
            .await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        first_server.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;
    let canonical_before_restart = first_server.interaction_events("task-1")?;
    assert_eq!(canonical_before_restart.len(), 1);
    first_session.abort();
    let _ = first_session.await;
    let _ = first_shutdown_send.send(());
    first_server_task.await??;
    drop(first_server);

    let recovered_server = WorkerControlService::open_sqlite(&database)?;
    let second_listener = TcpListener::bind(address).await?;
    let (second_shutdown_send, second_shutdown_receive) = oneshot::channel();
    let second_grpc_service = recovered_server.clone();
    let second_server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(second_grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(second_listener), async {
                let _ = second_shutdown_receive.await;
            })
            .await
    });
    let second_session = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        recovered_server
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected && snapshot.last_worker_sequence >= 2)
    })
    .await?;
    assert_eq!(
        recovered_server.assignment_state("attempt-1")?,
        Some(AssignmentState::Accepted),
        "an idempotent replay must not regress accepted state to sent"
    );
    assert_eq!(
        recovered_server.interaction_events("task-1")?,
        canonical_before_restart,
        "canonical identity and sequence must survive controller restart"
    );

    second_session.abort();
    let _ = second_session.await;
    let _ = second_shutdown_send.send(());
    second_server_task.await??;
    Ok(())
}

#[tokio::test]
async fn worker_restart_replays_durable_finished_result() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let worker_database = directory.path().join("worker.sqlite3");
    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let grpc_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let endpoint = Endpoint::from_shared(format!("http://{address}"))?;
    let first_worker = OutboundWorker::open_sqlite(endpoint.clone(), hello(), &worker_database)?
        .with_admission_only_mode();
    let first_state = first_worker.state();
    let first_session = tokio::spawn(async move { first_worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;
    wait_until(|| async { first_state.outbox_len().ok() == Some(0) }).await?;
    first_session.abort();
    let _ = first_session.await;
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| !snapshot.connected)
    })
    .await?;
    first_state.mark_finished(
        "attempt-1",
        &StoredFinished {
            outcome: AttemptOutcome::Succeeded,
            exit_code: Some(0),
            elapsed_ms: 10,
            receipt: None,
            stdout: None,
            stderr: None,
            detail: "durable fixture result".to_owned(),
        },
    )?;
    assert_eq!(first_state.outbox_len()?, 1);
    drop(first_state);

    let mut restarted_hello = hello();
    restarted_hello.instance_id = "cuda-1-restarted-process".to_owned();
    let restarted_worker =
        OutboundWorker::open_sqlite(endpoint, restarted_hello, &worker_database)?
            .with_admission_only_mode();
    let restarted_state = restarted_worker.state();
    let restarted_session = tokio::spawn(async move { restarted_worker.run_session().await });
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Finished)
    })
    .await?;
    wait_until(|| async { restarted_state.outbox_len().ok() == Some(0) }).await?;

    restarted_session.abort();
    let _ = restarted_session.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn cancellation_is_acknowledged_and_becomes_terminal() -> Result<(), Box<dyn Error>> {
    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let grpc_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });
    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_admission_only_mode();
    let worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    assert_eq!(
        service
            .cancel_attempt("attempt-1", "operator requested cancellation")
            .await?,
        CancelOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Finished)
    })
    .await?;
    assert_eq!(
        service.cancel_attempt("attempt-1", "duplicate").await?,
        CancelOutcome::AlreadyTerminal
    );

    worker_task.abort();
    let _ = worker_task.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn fake_execution_survives_stream_disconnect_and_replays_one_terminal_result()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("spool"),
        4_096,
    )?);
    let runtime = Arc::new(FakeExecutionRuntime::new("cuda-1", artifacts.clone(), 1)?);
    let executor = Arc::new(FakeExecutor::new(FakeExecutionPlan::successful(vec![
        FakeStep::Stdout(b"before disconnect".to_vec()),
        FakeStep::Delay(Duration::from_millis(250)),
        FakeStep::Stderr(b"after disconnect".to_vec()),
    ])));

    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let grpc_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_fake_executor(runtime, executor)?;
    let worker_state = worker.state();
    let first_worker = worker.clone();
    let first_session = tokio::spawn(async move { first_worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Running)
    })
    .await?;

    first_session.abort();
    let _ = first_session.await;
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| !snapshot.connected)
    })
    .await?;

    let second_session = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Finished)
    })
    .await?;
    let finished = worker_state
        .finished_attempt("attempt-1")?
        .expect("fake runtime must commit a terminal result while disconnected");
    assert_eq!(finished.outcome, AttemptOutcome::Succeeded);
    assert!(finished.receipt.is_some());
    assert!(finished.stdout.is_some());
    assert!(finished.stderr.is_some());

    wait_until(|| async { worker_state.outbox_len().ok() == Some(0) }).await?;
    assert_eq!(
        worker_state.finished_attempt("attempt-1")?,
        Some(finished),
        "reconnect must replay journal state without launching a second executor"
    );

    second_session.abort();
    let _ = second_session.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn fake_execution_cancellation_acknowledges_before_terminal_completion()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("spool"),
        4_096,
    )?);
    let runtime = Arc::new(FakeExecutionRuntime::new("cuda-1", artifacts, 1)?);
    let executor = Arc::new(FakeExecutor::new(FakeExecutionPlan::successful(vec![
        FakeStep::Delay(Duration::from_secs(30)),
    ])));
    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let grpc_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_fake_executor(runtime, executor)?;
    let worker_state = worker.state();
    let worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Running)
    })
    .await?;
    assert_eq!(
        service
            .cancel_attempt("attempt-1", "operator cancelled fake execution")
            .await?,
        CancelOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Finished)
    })
    .await?;
    let finished = worker_state
        .finished_attempt("attempt-1")?
        .expect("cancelled fake execution must persist its receipt and terminal result");
    assert_eq!(finished.outcome, AttemptOutcome::Cancelled);
    assert!(finished.receipt.is_some());
    assert_eq!(
        service.cancel_attempt("attempt-1", "duplicate").await?,
        CancelOutcome::AlreadyTerminal
    );

    worker_task.abort();
    let _ = worker_task.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn fake_execution_resumes_artifact_uploads_before_controller_accepts_terminal()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let local_artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("worker-spool"),
        8_192,
    )?);
    let remote_artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("server-cas"),
        8_192,
    )?);
    let uploads = Arc::new(SqliteUploadStore::open_with_quotas(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("upload-data"),
        8_192,
        4,
        UploadQuotas::unbounded(),
    )?);
    let stdout = b"resumable output";
    let stdout_digest = Sha256Digest::digest_bytes(stdout);
    let partial = uploads.begin(&BeginUpload {
        owner_id: "cuda-1".into(),
        upload_key: "output:attempt-1:stdout".into(),
        expected_digest: stdout_digest,
        expected_size_bytes: u64::try_from(stdout.len())?,
        media_type: "application/vnd.alloyport.stdout".into(),
        now_ms: 1_000,
        expires_at_ms: 61_000,
    })?;
    uploads.append("cuda-1", &partial.upload_id, 0, &stdout[..3], 1_001)?;

    let service = WorkerControlService::new().with_artifact_metadata(uploads.clone());
    let artifact_service = ArtifactServiceImpl::new(
        uploads.clone(),
        remote_artifacts,
        Arc::new(FixedArtifactOwner),
        Arc::new(ManualClock::new(2_000)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let grpc_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(grpc_service))
            .add_service(ArtifactServiceServer::new(artifact_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let runtime = Arc::new(FakeExecutionRuntime::new(
        "cuda-1",
        local_artifacts.clone(),
        1,
    )?);
    let executor = Arc::new(FakeExecutor::new(FakeExecutionPlan::successful(vec![
        FakeStep::Stdout(stdout.to_vec()),
    ])));
    let publisher = Arc::new(RemoteArtifactPublisher::new(
        endpoint.clone(),
        local_artifacts,
        4,
        Some(60_000),
    )?);
    let worker = OutboundWorker::new(endpoint, hello())?
        .with_fake_executor(runtime, executor)?
        .with_artifact_publisher(publisher);
    let worker_state = worker.state();
    let worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Finished)
    })
    .await?;
    wait_until(|| async {
        service
            .interaction_events("task-1")
            .is_ok_and(|events| events.len() == 7)
    })
    .await?;
    let finished = worker_state
        .finished_attempt("attempt-1")?
        .expect("terminal state is committed only after all remote finalizations");
    assert_uploaded_execution_artifacts(&uploads, &finished, stdout_digest)?;
    let events = service.interaction_events("task-1")?;
    assert_canonical_fake_events(&events)?;

    worker_task.abort();
    let _ = worker_task.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

fn assert_canonical_fake_events(
    events: &[alloyport_events::EventEnvelope],
) -> Result<(), Box<dyn Error>> {
    assert_eq!(events.len(), 7);
    assert!(matches!(
        events[0].event,
        alloyport_events::Event::RunStarted { .. }
    ));
    assert!(matches!(
        events[1].event,
        alloyport_events::Event::CommandStarted { .. }
    ));
    assert!(matches!(
        events[2].event,
        alloyport_events::Event::CommandOutput { .. }
    ));
    assert!(events[3..6].iter().all(|event| matches!(
        event.event,
        alloyport_events::Event::ArtifactProduced { .. }
    )));
    assert!(matches!(
        events[6].event,
        alloyport_events::Event::CommandCompleted { .. }
    ));
    let mut reducer = alloyport_events::RunReducer::new();
    for event in events {
        reducer.apply(event)?;
    }
    Ok(())
}

fn assert_uploaded_execution_artifacts(
    uploads: &SqliteUploadStore,
    finished: &StoredFinished,
    stdout_digest: Sha256Digest,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        finished.stdout.as_ref().map(|artifact| artifact.digest),
        Some(stdout_digest)
    );
    for (key, kind) in [
        (
            "output:attempt-1:stdout",
            ArtifactReferenceKind::AssignmentOutput,
        ),
        (
            "output:attempt-1:stderr",
            ArtifactReferenceKind::AssignmentOutput,
        ),
        ("receipt:attempt-1", ArtifactReferenceKind::Receipt),
    ] {
        assert!(uploads.completed_upload_by_key("cuda-1", key)?.is_some());
        assert_eq!(uploads.reference("cuda-1", key)?.kind, kind);
    }
    assert_eq!(
        uploads
            .completed_upload_by_key("cuda-1", "output:attempt-1:stdout")?
            .expect("partial upload must finalize")
            .digest,
        stdout_digest
    );
    Ok(())
}

#[derive(Debug)]
struct FixedArtifactOwner;

#[tonic::async_trait]
impl ArtifactAccessPolicy for FixedArtifactOwner {
    async fn resolve_owner(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
        _extensions: &Extensions,
    ) -> Result<String, Status> {
        Ok("cuda-1".into())
    }

    async fn authorize_download(
        &self,
        _owner_id: &str,
        _digest: Sha256Digest,
    ) -> Result<(), Status> {
        Err(Status::permission_denied(
            "download is outside this fixture",
        ))
    }
}

#[tokio::test]
async fn assignment_queued_while_disconnected_is_replayed_after_reconnect()
-> Result<(), Box<dyn Error>> {
    let service = WorkerControlService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_admission_only_mode();
    let worker_state = worker.state();
    let first_session_worker = worker.clone();
    let first_session = tokio::spawn(async move { first_session_worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|worker| worker.connected)
    })
    .await?;

    first_session.abort();
    let _ = first_session.await;
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|worker| !worker.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Pending
    );

    let second_session = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async { worker_state.contains_attempt("attempt-1").unwrap_or(false) }).await?;
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;

    second_session.abort();
    let _ = second_session.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn expired_attempt_is_reassigned_with_a_new_identity_and_old_result_stays_stale()
-> Result<(), Box<dyn Error>> {
    let repository = Arc::new(SqliteControlRepository::in_memory()?);
    let clock = Arc::new(ManualClock::new(1_000));
    let service = WorkerControlService::with_repository(repository, clock.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_service = service.clone();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerControlServer::new(server_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?
        .with_admission_only_mode();
    let worker_state = worker.state();
    let first_worker = worker.clone();
    let first_session = tokio::spawn(async move { first_worker.run_session().await });
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| snapshot.connected)
    })
    .await?;
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment()).await?,
        EnqueueOutcome::Sent
    );
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;

    first_session.abort();
    let _ = first_session.await;
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| !snapshot.connected)
    })
    .await?;
    clock.advance(30_000);
    assert_eq!(service.expire_leases()?, vec!["attempt-1"]);
    assert_eq!(
        service
            .reassign_expired_attempt("attempt-1", "cuda-1", "attempt-2")
            .await?,
        EnqueueOutcome::Pending
    );
    worker_state.mark_finished(
        "attempt-1",
        &StoredFinished {
            outcome: AttemptOutcome::Succeeded,
            exit_code: Some(0),
            elapsed_ms: 30_001,
            receipt: None,
            stdout: None,
            stderr: None,
            detail: "late result from expired process".to_owned(),
        },
    )?;

    let second_session = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        service.assignment_state("attempt-2").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;
    assert_eq!(
        service.assignment_state("attempt-1")?,
        Some(AssignmentState::LeaseExpired)
    );
    assert!(worker_state.contains_attempt("attempt-2")?);

    second_session.abort();
    let _ = second_session.await;
    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

async fn wait_until<F, Fut>(mut condition: F) -> Result<(), Box<dyn Error>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(Into::into)
}

fn hello() -> WorkerHello {
    WorkerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        worker_id: "cuda-1".to_owned(),
        instance_id: "cuda-1-test-process".to_owned(),
        worker_version: "0.1.0".to_owned(),
        features: Vec::new(),
        capabilities: Some(WorkerCapabilities {
            backend: Backend::Cuda.into(),
            architecture: "sm_80".to_owned(),
            device_count: 1,
            max_concurrency: 1,
            driver_version: "test".to_owned(),
            toolkit_version: "test".to_owned(),
            container_runtime: "test".to_owned(),
        }),
        active_attempts: Vec::new(),
    }
}

fn assignment() -> Assignment {
    Assignment {
        assignment_id: "assignment-1".to_owned(),
        attempt_id: "attempt-1".to_owned(),
        attempt_number: 1,
        idempotency_key: "task-1:build".to_owned(),
        task_id: "task-1".to_owned(),
        candidate_id: "candidate-1".to_owned(),
        execution: Some(ExecutionSpec {
            executor_kind: ExecutorKind::Container.into(),
            argv: vec!["cmake".to_owned(), "--build".to_owned(), "build".to_owned()],
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

fn artifact(byte: char) -> ArtifactRef {
    ArtifactRef {
        digest: format!("sha256:{}", byte.to_string().repeat(64)),
        size_bytes: 1,
        media_type: "application/octet-stream".to_owned(),
    }
}
