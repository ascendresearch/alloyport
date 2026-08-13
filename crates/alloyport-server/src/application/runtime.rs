//! Server listener, background-task ownership, and bounded shutdown.

use super::CandidateEpisodeApplication;
use super::assembly::ServerApplication;
use super::candidate_config::RequiredWorker;
use crate::WorkerControlService;
use crate::migration_task::SqliteMigrationTaskStore;
use crate::storage::RepositoryError;
use alloyport_core::{AgentLoopAdvance, EpisodeStatus};
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::interaction_v1::interaction_service_server::InteractionServiceServer;
use alloyport_proto::management_v1::management_service_server::ManagementServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::{
    MAX_ARTIFACT_DOWNLOAD_MESSAGE_BYTES, MAX_INTERACTION_EVENT_MESSAGE_BYTES,
    MAX_INTERACTION_REQUEST_MESSAGE_BYTES, MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES,
    MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES, MAX_SERVER_TO_WORKER_MESSAGE_BYTES,
    MAX_WORKER_TO_SERVER_MESSAGE_BYTES,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tonic::transport::Server;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskKind {
    GrpcServer,
    LeaseReaper,
    PreparationReconciler,
    CandidateEpisode,
    MigrationDispatcher,
}

impl Display for TaskKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::GrpcServer => formatter.write_str("gRPC server"),
            Self::LeaseReaper => formatter.write_str("lease reaper"),
            Self::PreparationReconciler => formatter.write_str("assignment preparation reconciler"),
            Self::CandidateEpisode => formatter.write_str("Candidate Episode"),
            Self::MigrationDispatcher => formatter.write_str("migration dispatcher"),
        }
    }
}

#[derive(Debug)]
enum TaskError {
    Transport(tonic::transport::Error),
    Repository(RepositoryError),
    Candidate(String),
    Migration(String),
}

impl Display for TaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Repository(error) => Display::fmt(error, formatter),
            Self::Candidate(error) | Self::Migration(error) => formatter.write_str(error),
        }
    }
}

impl Error for TaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::Candidate(_) | Self::Migration(_) => None,
        }
    }
}

struct TaskExit {
    kind: TaskKind,
    result: Result<(), TaskError>,
}

pub(super) async fn run(application: ServerApplication) -> Result<(), Box<dyn Error>> {
    run_inner(application, None).await
}

pub(super) struct CandidateRuntime {
    pub(super) application: CandidateEpisodeApplication,
    pub(super) control: WorkerControlService,
    pub(super) required_workers: Vec<RequiredWorker>,
    pub(super) poll_interval: std::time::Duration,
    pub(super) ready_timeout: std::time::Duration,
}

pub(super) async fn run_candidate(
    application: ServerApplication,
    candidate: CandidateRuntime,
) -> Result<(), Box<dyn Error>> {
    run_inner(application, Some(candidate)).await
}

async fn run_inner(
    mut application: ServerApplication,
    candidate: Option<CandidateRuntime>,
) -> Result<(), Box<dyn Error>> {
    let address = application.address;
    let shutdown_timeout = application.shutdown_timeout;
    let ServerApplication { tls, .. } = &mut application;
    let tls = tls.take();
    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let tasks = spawn_tasks(application, server, shutdown_receiver, candidate);

    println!(
        "AlloyPort worker control, artifact, interaction, and management services listening on {address}"
    );
    supervise(tasks, shutdown, shutdown_timeout).await
}

fn spawn_tasks(
    application: ServerApplication,
    mut server: Server,
    shutdown_receiver: watch::Receiver<bool>,
    candidate: Option<CandidateRuntime>,
) -> JoinSet<TaskExit> {
    let ServerApplication {
        address,
        control,
        artifact,
        artifact_max_decoding_message_bytes,
        interaction,
        management,
        migration_dispatcher,
        ..
    } = application;
    let mut tasks = JoinSet::new();

    let grpc_control = control.clone();
    let server_shutdown = shutdown_receiver.clone();
    tasks.spawn(async move {
        let result = server
            .add_service(
                WorkerControlServer::new(grpc_control)
                    .max_decoding_message_size(MAX_WORKER_TO_SERVER_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_SERVER_TO_WORKER_MESSAGE_BYTES),
            )
            .add_service(
                ArtifactServiceServer::new(artifact)
                    .max_decoding_message_size(artifact_max_decoding_message_bytes)
                    .max_encoding_message_size(MAX_ARTIFACT_DOWNLOAD_MESSAGE_BYTES),
            )
            .add_service(
                InteractionServiceServer::new(interaction)
                    .max_decoding_message_size(MAX_INTERACTION_REQUEST_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_INTERACTION_EVENT_MESSAGE_BYTES),
            )
            .add_service(
                ManagementServiceServer::new(management)
                    .max_decoding_message_size(MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES),
            )
            .serve_with_shutdown(address, wait_for_shutdown(server_shutdown))
            .await
            .map_err(TaskError::Transport);
        TaskExit {
            kind: TaskKind::GrpcServer,
            result,
        }
    });

    if let Some(dispatcher) = migration_dispatcher {
        let dispatcher_shutdown = shutdown_receiver.clone();
        tasks.spawn(async move {
            let result = dispatcher
                .run_until(dispatcher_shutdown)
                .await
                .map_err(TaskError::Migration);
            TaskExit {
                kind: TaskKind::MigrationDispatcher,
                result,
            }
        });
    }
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

    if let Some(candidate) = candidate {
        tasks.spawn(async move {
            let result = drive_candidate(candidate)
                .await
                .map_err(TaskError::Candidate);
            TaskExit {
                kind: TaskKind::CandidateEpisode,
                result,
            }
        });
    }

    tasks
}

async fn drive_candidate(mut runtime: CandidateRuntime) -> Result<(), String> {
    drive_candidate_inner(&mut runtime, None).await
}

pub(super) async fn drive_candidate_for_task(
    mut runtime: CandidateRuntime,
    tasks: Arc<SqliteMigrationTaskStore>,
    task_id: String,
) -> Result<(), String> {
    drive_candidate_inner(&mut runtime, Some((&tasks, task_id.as_str()))).await
}

async fn drive_candidate_inner(
    runtime: &mut CandidateRuntime,
    cancellation: Option<(&SqliteMigrationTaskStore, &str)>,
) -> Result<(), String> {
    // One-shot operator validation remains bounded, while a submitted migration is durable work:
    // it stays queued until its workers report capacity or the user cancels it.
    let deadline = cancellation
        .is_none()
        .then(|| tokio::time::Instant::now() + runtime.ready_timeout);
    loop {
        check_cancelled(cancellation)?;
        let mut missing = Vec::new();
        for required in &runtime.required_workers {
            let ready = runtime
                .control
                .worker_snapshot(&required.id)
                .await
                .is_some_and(|snapshot| {
                    snapshot.connected
                        && snapshot.heartbeat.as_ref().is_some_and(|heartbeat| {
                            heartbeat.health == alloyport_proto::v1::WorkerHealth::Ready as i32
                                && heartbeat.available_slots > 0
                        })
                        && snapshot.backend == i32::from(required.backend)
                        && snapshot
                            .features
                            .iter()
                            .any(|feature| feature == required.feature)
                });
            if !ready {
                missing.push(required.id.as_str());
            }
        }
        if missing.is_empty() {
            break;
        }
        if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Err(format!(
                "required workers were not ready before timeout: {}",
                missing.join(", ")
            ));
        }
        tokio::time::sleep(runtime.poll_interval).await;
    }
    println!("Candidate Episode workers are ready; authorized provider dispatch may begin");

    loop {
        check_cancelled(cancellation)?;
        match runtime
            .application
            .advance()
            .await
            .map_err(|error| error.to_string())?
        {
            AgentLoopAdvance::Terminal(EpisodeStatus::Succeeded) => {
                println!("Candidate Episode completed successfully");
                return Ok(());
            }
            AgentLoopAdvance::Terminal(status) => {
                return Err(format!(
                    "Candidate Episode ended in terminal state {status:?}"
                ));
            }
            AgentLoopAdvance::Suspended => {
                return Err("Candidate Episode suspended for operator reconciliation".to_owned());
            }
            AgentLoopAdvance::Progressed(EpisodeStatus::ToolWorkPending) => {
                tokio::time::sleep(runtime.poll_interval).await;
            }
            AgentLoopAdvance::Progressed(_) => tokio::task::yield_now().await,
        }
    }
}

fn check_cancelled(cancellation: Option<(&SqliteMigrationTaskStore, &str)>) -> Result<(), String> {
    if let Some((tasks, task_id)) = cancellation
        && tasks
            .is_cancelled(task_id)
            .map_err(|error| error.to_string())?
    {
        return Err("migration cancelled by user".to_owned());
    }
    Ok(())
}

async fn supervise(
    mut tasks: JoinSet<TaskExit>,
    shutdown: watch::Sender<bool>,
    shutdown_timeout: std::time::Duration,
) -> Result<(), Box<dyn Error>> {
    let failure = loop {
        let joined = tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break None;
            }
            joined = tasks.join_next() => joined.ok_or("server task set became empty")?,
        };
        let exit = joined.map_err(|error| join_error_detail(&error))?;
        if exit.kind == TaskKind::CandidateEpisode {
            report_candidate_exit(exit.result);
            continue;
        }
        break first_exit_detail(Ok(exit))?;
    };
    let _ = shutdown.send(true);

    let mut failure = failure;
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

fn report_candidate_exit(result: Result<(), TaskError>) {
    match result {
        Ok(()) => println!("Candidate Episode job exited; AlloyPort server remains available"),
        Err(error) => eprintln!(
            "Candidate Episode job failed: {error}; AlloyPort server remains available for inspection and new work"
        ),
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn first_exit_detail(
    joined: Result<TaskExit, JoinError>,
) -> Result<Option<String>, Box<dyn Error>> {
    let exit = joined.map_err(|error| join_error_detail(&error))?;
    match (exit.kind, exit.result) {
        (kind, Ok(())) => Ok(Some(format!("{kind} stopped unexpectedly"))),
        (kind, Err(error)) => Ok(Some(format!("{kind} failed: {error}"))),
    }
}

fn join_error_detail(error: &JoinError) -> String {
    format!("server task panicked or was cancelled: {error}")
}
