//! Shared cancellation, timeout, and output-budget supervision for running containers.

use crate::container_engine::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogs,
    ContainerPhase, ContainerSnapshot, EngineFuture,
};
use crate::container_outcome::ContainerTermination;
use crate::executor::CancellationToken;
use std::time::Duration;

pub(crate) trait RunningContainerEngine: Send + Sync {
    fn wait<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ContainerExit>;
    fn stop<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    fn follow_logs_observed<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
        observer: &'a mut (dyn FnMut(ContainerLogChunk) + Send),
    ) -> EngineFuture<'a, ContainerLogs>;
    fn streams_live_log_observations(&self) -> bool;
}

pub(crate) trait ContainerReconcileEngine<Plan>: Send + Sync {
    fn inspect<'a>(&'a self, name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>>;
    fn create<'a>(
        &'a self,
        plan: &'a Plan,
        identity: &'a ContainerIdentity,
    ) -> EngineFuture<'a, ()>;
}

#[derive(Debug)]
pub(crate) enum ContainerReconcileError {
    Engine(ContainerEngineError),
    MissingAfterCreate(String),
    IdentityConflict(String),
    UnexpectedCreatedPhase { name: String, phase: ContainerPhase },
}

pub(crate) async fn reconcile_container<E, Plan>(
    engine: &E,
    plan: &Plan,
    identity: &ContainerIdentity,
) -> Result<ContainerPhase, ContainerReconcileError>
where
    E: ContainerReconcileEngine<Plan> + ?Sized,
{
    if let Some(snapshot) = engine
        .inspect(&identity.name)
        .await
        .map_err(ContainerReconcileError::Engine)?
    {
        if snapshot.identity != *identity {
            return Err(ContainerReconcileError::IdentityConflict(
                identity.name.clone(),
            ));
        }
        return Ok(snapshot.phase);
    }
    engine
        .create(plan, identity)
        .await
        .map_err(ContainerReconcileError::Engine)?;
    let created = engine
        .inspect(&identity.name)
        .await
        .map_err(ContainerReconcileError::Engine)?
        .ok_or_else(|| ContainerReconcileError::MissingAfterCreate(identity.name.clone()))?;
    if created.identity != *identity {
        return Err(ContainerReconcileError::IdentityConflict(
            identity.name.clone(),
        ));
    }
    if created.phase != ContainerPhase::Created {
        return Err(ContainerReconcileError::UnexpectedCreatedPhase {
            name: identity.name.clone(),
            phase: created.phase,
        });
    }
    Ok(created.phase)
}

pub(crate) async fn supervise_running_container<E>(
    engine: &E,
    name: &str,
    timeout_ms: u64,
    output_limit: u64,
    cancellation: &CancellationToken,
    observer: &mut (dyn FnMut(ContainerLogChunk) + Send),
) -> Result<(ContainerTermination, ContainerLogs, bool), ContainerEngineError>
where
    E: RunningContainerEngine + ?Sized,
{
    let mut cancelled = cancellation.subscribe();
    let wait = engine.wait(name);
    let live_output_streaming = engine.streams_live_log_observations();
    let follow = engine.follow_logs_observed(name, output_limit, observer);
    let timeout = tokio::time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(wait, follow, timeout);
    let mut collected_logs = None;
    loop {
        let termination = tokio::select! {
            biased;
            () = wait_for_cancellation(&mut cancelled) => {
                stop_and_wait(engine, name, &mut wait)
                    .await
                    .map(ContainerTermination::Cancelled)?
            }
            () = &mut timeout => {
                stop_and_wait(engine, name, &mut wait)
                    .await
                    .map(ContainerTermination::TimedOut)?
            }
            exit = &mut wait => ContainerTermination::Exited(exit?),
            logs = &mut follow, if collected_logs.is_none() => {
                let logs = match logs {
                    Ok(logs) => logs,
                    Err(error) => {
                        let _ = engine.stop(name).await;
                        let _ = (&mut wait).await;
                        return Err(error);
                    }
                };
                if logs.output_limit_exceeded {
                    let exit = stop_and_wait(engine, name, &mut wait).await?;
                    return Ok((
                        ContainerTermination::OutputLimitExceeded(exit),
                        logs,
                        live_output_streaming,
                    ));
                }
                collected_logs = Some(logs);
                continue;
            }
        };
        let logs = if let Some(logs) = collected_logs {
            logs
        } else {
            follow.await?
        };
        return Ok((termination, logs, live_output_streaming));
    }
}

async fn stop_and_wait<E>(
    engine: &E,
    name: &str,
    wait: &mut EngineFuture<'_, ContainerExit>,
) -> Result<ContainerExit, ContainerEngineError>
where
    E: RunningContainerEngine + ?Sized,
{
    engine.stop(name).await?;
    wait.await
}

async fn wait_for_cancellation(cancellation: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *cancellation.borrow_and_update() {
            return;
        }
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}
