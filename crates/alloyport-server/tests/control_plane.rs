use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, AttemptOutcome, Backend, ExecutionSpec, ExecutorKind,
    WorkerCapabilities, WorkerHello,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_server::{AssignmentState, CancelOutcome, EnqueueOutcome, WorkerControlService};
use alloyport_worker::{OutboundWorker, StoredFinished};
use std::error::Error;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};

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
    wait_until(|| async {
        worker_state
            .lock()
            .await
            .contains_attempt("attempt-1")
            .unwrap_or(false)
    })
    .await?;
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Accepted)
    })
    .await?;

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
            Some(AssignmentState::Queued)
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

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?;
    let worker_state = worker.state();
    let worker_task = tokio::spawn(async move { worker.run_session().await });
    wait_until(|| async {
        worker_state
            .lock()
            .await
            .contains_attempt("attempt-1")
            .unwrap_or(false)
    })
    .await?;
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

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?;
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
    let first_worker = OutboundWorker::open_sqlite(endpoint.clone(), hello(), &worker_database)?;
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
    first_session.abort();
    let _ = first_session.await;
    wait_until(|| async {
        service
            .worker_snapshot("cuda-1")
            .await
            .is_some_and(|snapshot| !snapshot.connected)
    })
    .await?;
    first_state.lock().await.mark_finished(
        "attempt-1",
        &StoredFinished {
            outcome: AttemptOutcome::Succeeded.into(),
            exit_code: Some(0),
            elapsed_ms: 10,
            receipt: None,
            stdout: None,
            stderr: None,
            detail: "durable fixture result".to_owned(),
        },
    )?;
    drop(first_state);

    let mut restarted_hello = hello();
    restarted_hello.instance_id = "cuda-1-restarted-process".to_owned();
    let restarted_worker =
        OutboundWorker::open_sqlite(endpoint, restarted_hello, &worker_database)?;
    let restarted_session = tokio::spawn(async move { restarted_worker.run_session().await });
    wait_until(|| async {
        service.assignment_state("attempt-1").ok().flatten() == Some(AssignmentState::Finished)
    })
    .await?;

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
    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?;
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

    let worker = OutboundWorker::new(Endpoint::from_shared(format!("http://{address}"))?, hello())?;
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
    wait_until(|| async {
        worker_state
            .lock()
            .await
            .contains_attempt("attempt-1")
            .unwrap_or(false)
    })
    .await?;
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
