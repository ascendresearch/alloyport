//! Server listener, background-task ownership, and bounded shutdown.

use super::assembly::ServerApplication;
use crate::storage::RepositoryError;
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::interaction_v1::interaction_service_server::InteractionServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tonic::transport::Server;

#[derive(Clone, Copy, Debug)]
enum TaskKind {
    GrpcServer,
    LeaseReaper,
    PreparationReconciler,
}

impl Display for TaskKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::GrpcServer => formatter.write_str("gRPC server"),
            Self::LeaseReaper => formatter.write_str("lease reaper"),
            Self::PreparationReconciler => formatter.write_str("assignment preparation reconciler"),
        }
    }
}

#[derive(Debug)]
enum TaskError {
    Transport(tonic::transport::Error),
    Repository(RepositoryError),
}

impl Display for TaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Repository(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Repository(error) => Some(error),
        }
    }
}

struct TaskExit {
    kind: TaskKind,
    result: Result<(), TaskError>,
}

pub(super) async fn run(mut application: ServerApplication) -> Result<(), Box<dyn Error>> {
    let address = application.address;
    let shutdown_timeout = application.shutdown_timeout;
    let ServerApplication { tls, .. } = &mut application;
    let tls = tls.take();
    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let tasks = spawn_tasks(application, server, shutdown_receiver);

    println!("AlloyPort worker control, artifact, and interaction services listening on {address}");
    supervise(tasks, shutdown, shutdown_timeout).await
}

fn spawn_tasks(
    application: ServerApplication,
    mut server: Server,
    shutdown_receiver: watch::Receiver<bool>,
) -> JoinSet<TaskExit> {
    let ServerApplication {
        address,
        control,
        artifact,
        artifact_max_decoding_message_bytes,
        interaction,
        ..
    } = application;
    let mut tasks = JoinSet::new();

    let grpc_control = control.clone();
    let server_shutdown = shutdown_receiver.clone();
    tasks.spawn(async move {
        let result = server
            .add_service(WorkerControlServer::new(grpc_control))
            .add_service(
                ArtifactServiceServer::new(artifact)
                    .max_decoding_message_size(artifact_max_decoding_message_bytes),
            )
            .add_service(InteractionServiceServer::new(interaction))
            .serve_with_shutdown(address, wait_for_shutdown(server_shutdown))
            .await
            .map_err(TaskError::Transport);
        TaskExit {
            kind: TaskKind::GrpcServer,
            result,
        }
    });
    let reaper = control.clone();
    let reaper_shutdown = shutdown_receiver.clone();
    tasks.spawn(async move {
        let result = reaper
            .run_lease_reaper_until(reaper_shutdown)
            .await
            .map_err(TaskError::Repository);
        TaskExit {
            kind: TaskKind::LeaseReaper,
            result,
        }
    });
    let reconciler_shutdown = shutdown_receiver;
    tasks.spawn(async move {
        let result = control
            .run_preparation_reconciler_until(reconciler_shutdown)
            .await
            .map_err(TaskError::Repository);
        TaskExit {
            kind: TaskKind::PreparationReconciler,
            result,
        }
    });

    tasks
}

async fn supervise(
    mut tasks: JoinSet<TaskExit>,
    shutdown: watch::Sender<bool>,
    shutdown_timeout: std::time::Duration,
) -> Result<(), Box<dyn Error>> {
    let first_exit = tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal?;
            None
        }
        joined = tasks.join_next() => Some(joined.ok_or("server task set became empty")?),
    };
    let _ = shutdown.send(true);

    let mut failure = first_exit.map(unexpected_exit).transpose()?;
    let drain = async {
        let mut drain_failure = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(exit) => {
                    if let Err(error) = exit.result {
                        drain_failure
                            .get_or_insert_with(|| format!("{} failed: {error}", exit.kind));
                    }
                }
                Err(error) => {
                    drain_failure.get_or_insert_with(|| join_error_detail(&error));
                }
            }
        }
        drain_failure
    };
    if let Ok(drain_failure) = tokio::time::timeout(shutdown_timeout, drain).await {
        if failure.is_none() {
            failure = drain_failure;
        }
    } else {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        return Err(format!("server tasks did not drain within {shutdown_timeout:?}").into());
    }
    if let Some(failure) = failure {
        return Err(failure.into());
    }
    Ok(())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn unexpected_exit(joined: Result<TaskExit, JoinError>) -> Result<String, Box<dyn Error>> {
    let exit = joined.map_err(|error| join_error_detail(&error))?;
    Ok(match exit.result {
        Ok(()) => format!("{} stopped unexpectedly", exit.kind),
        Err(error) => format!("{} failed: {error}", exit.kind),
    })
}

fn join_error_detail(error: &JoinError) -> String {
    format!("server task panicked or was cancelled: {error}")
}
