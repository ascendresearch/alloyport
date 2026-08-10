use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, Backend, ExecutionSpec, ExecutorKind, WorkerCapabilities, WorkerHello,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_server::{AssignmentState, EnqueueOutcome, WorkerControlService};
use alloyport_worker::OutboundWorker;
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
    wait_until(|| async { worker_state.lock().await.contains_attempt("attempt-1") }).await?;
    wait_until(|| async {
        service.assignment_state("attempt-1").await == Some(AssignmentState::Accepted)
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
    wait_until(|| async { worker_state.lock().await.contains_attempt("attempt-1") }).await?;
    wait_until(|| async {
        service.assignment_state("attempt-1").await == Some(AssignmentState::Accepted)
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
