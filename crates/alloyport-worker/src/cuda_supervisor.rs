//! Durable reconciliation state machine for policy-bound CUDA containers.

mod engine;
mod outcome;

pub use engine::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream,
    ContainerLogs, ContainerPhase, ContainerSnapshot, CudaContainerEngine, CudaExecutionFacts,
    EngineFuture, SupervisedCudaExecution,
};
use outcome::{Termination, classify, enforce_output_limit};

use crate::cuda::{CudaContractError, CudaFixturePolicy, DockerCreatePlan};
use crate::executor::{CancellationToken, ExecutorResult};
use crate::journal::StoredAssignment;
use alloyport_artifacts::ArtifactStore;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CudaContainerSupervisor {
    policy: Arc<CudaFixturePolicy>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl CudaContainerSupervisor {
    #[must_use]
    pub const fn new(policy: Arc<CudaFixturePolicy>, artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self { policy, artifacts }
    }

    /// Reconciles one admitted attempt with its stable container and returns bounded terminal data.
    ///
    /// # Errors
    ///
    /// Returns an error for a policy/bundle failure, image mismatch, conflicting container identity,
    /// or container-engine failure. Candidate exit, cancellation, timeout, and output exhaustion are
    /// returned as typed terminal outcomes rather than supervisor errors.
    pub async fn run(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn CudaContainerEngine,
        cancellation: &CancellationToken,
    ) -> Result<ExecutorResult, CudaSupervisorError> {
        Ok(self
            .run_with_facts(assignment, engine, cancellation)
            .await?
            .result)
    }

    /// Runs the same reconciliation while retaining immutable receipt facts.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`].
    pub async fn run_with_facts(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn CudaContainerEngine,
        cancellation: &CancellationToken,
    ) -> Result<SupervisedCudaExecution, CudaSupervisorError> {
        self.run_with_facts_observed(assignment, engine, cancellation, |_| {})
            .await
    }

    /// Runs reconciliation while forwarding best-effort live container output chunks.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`].
    pub async fn run_with_facts_observed<F>(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn CudaContainerEngine,
        cancellation: &CancellationToken,
        mut observer: F,
    ) -> Result<SupervisedCudaExecution, CudaSupervisorError>
    where
        F: FnMut(ContainerLogChunk) + Send,
    {
        let sandbox = self
            .policy
            .materialize_bundle(assignment, self.artifacts.as_ref())?;
        let source_digest = sandbox.source_digest().to_owned();
        let plan = self.policy.docker_create_plan(assignment, &sandbox)?;
        let identity = ContainerIdentity {
            name: plan.container_name.clone(),
            attempt_id: assignment.attempt_id.to_string(),
            bundle_digest: assignment.execution.bundle.digest.clone(),
            image_manifest_digest: assignment.execution.image.digest.clone(),
            image_id: plan.expected_image_id.to_string(),
        };
        let resolved_image = engine
            .resolve_image_id(&plan)
            .await
            .map_err(CudaSupervisorError::Engine)?;
        if resolved_image != identity.image_id {
            return Err(CudaSupervisorError::ImageMismatch {
                expected: identity.image_id,
                actual: resolved_image,
            });
        }

        let phase = reconcile_container(engine, &plan, &identity).await?;
        if phase == ContainerPhase::Created {
            engine
                .start(&identity.name)
                .await
                .map_err(CudaSupervisorError::Engine)?;
        }

        let output_limit = assignment
            .execution
            .limits
            .as_ref()
            .map_or(0, |limits| limits.output_bytes);
        let (termination, logs, live_output_streaming) = if phase == ContainerPhase::Exited {
            let exit = engine
                .wait(&identity.name)
                .await
                .map_err(CudaSupervisorError::Engine)?;
            let logs = engine
                .logs(&identity.name, output_limit)
                .await
                .map_err(CudaSupervisorError::Engine)?;
            (Termination::Exited(exit), logs, false)
        } else {
            collect_running(
                engine,
                &identity.name,
                assignment.execution.timeout_ms,
                output_limit,
                cancellation,
                &mut observer,
            )
            .await?
        };
        Ok(SupervisedCudaExecution {
            result: classify(
                termination,
                enforce_output_limit(logs, output_limit),
                assignment.execution.timeout_ms,
            ),
            facts: CudaExecutionFacts {
                container_name: identity.name,
                bundle_digest: identity.bundle_digest,
                source_digest,
                image_manifest_digest: identity.image_manifest_digest,
                image_id: identity.image_id,
                device_id: plan.device_id,
            },
            live_output_streaming,
        })
    }
}

async fn collect_running(
    engine: &dyn CudaContainerEngine,
    name: &str,
    timeout_ms: u64,
    output_limit: u64,
    cancellation: &CancellationToken,
    observer: &mut (dyn FnMut(ContainerLogChunk) + Send),
) -> Result<(Termination, ContainerLogs, bool), CudaSupervisorError> {
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
                stop_and_wait(engine, name, &mut wait).await.map(Termination::Cancelled)?
            }
            () = &mut timeout => {
                stop_and_wait(engine, name, &mut wait).await.map(Termination::TimedOut)?
            }
            exit = &mut wait => Termination::Exited(exit.map_err(CudaSupervisorError::Engine)?),
            logs = &mut follow, if collected_logs.is_none() => {
                let logs = match logs {
                    Ok(logs) => logs,
                    Err(error) => {
                        let _ = engine.stop(name).await;
                        let _ = (&mut wait).await;
                        return Err(CudaSupervisorError::Engine(error));
                    }
                };
                if logs.output_limit_exceeded {
                    let exit = stop_and_wait(engine, name, &mut wait).await?;
                    return Ok((Termination::OutputLimitExceeded(exit), logs, live_output_streaming));
                }
                collected_logs = Some(logs);
                continue;
            }
        };
        let logs = if let Some(logs) = collected_logs {
            logs
        } else {
            follow.await.map_err(CudaSupervisorError::Engine)?
        };
        return Ok((termination, logs, live_output_streaming));
    }
}

async fn stop_and_wait(
    engine: &dyn CudaContainerEngine,
    name: &str,
    wait: &mut EngineFuture<'_, ContainerExit>,
) -> Result<ContainerExit, CudaSupervisorError> {
    engine
        .stop(name)
        .await
        .map_err(CudaSupervisorError::Engine)?;
    wait.await.map_err(CudaSupervisorError::Engine)
}

async fn reconcile_container(
    engine: &dyn CudaContainerEngine,
    plan: &DockerCreatePlan,
    identity: &ContainerIdentity,
) -> Result<ContainerPhase, CudaSupervisorError> {
    if let Some(snapshot) = engine
        .inspect(&identity.name)
        .await
        .map_err(CudaSupervisorError::Engine)?
    {
        if snapshot.identity != *identity {
            return Err(CudaSupervisorError::IdentityConflict(identity.name.clone()));
        }
        return Ok(snapshot.phase);
    }

    engine
        .create(plan, identity)
        .await
        .map_err(CudaSupervisorError::Engine)?;
    let created = engine
        .inspect(&identity.name)
        .await
        .map_err(CudaSupervisorError::Engine)?
        .ok_or_else(|| {
            CudaSupervisorError::Invariant(format!(
                "container {} is missing immediately after create",
                identity.name
            ))
        })?;
    if created.identity != *identity {
        return Err(CudaSupervisorError::IdentityConflict(identity.name.clone()));
    }
    if created.phase != ContainerPhase::Created {
        return Err(CudaSupervisorError::Invariant(format!(
            "new container {} has unexpected phase {:?}",
            identity.name, created.phase
        )));
    }
    Ok(created.phase)
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

#[derive(Debug)]
pub enum CudaSupervisorError {
    Contract(CudaContractError),
    Engine(ContainerEngineError),
    Invariant(String),
    ImageMismatch { expected: String, actual: String },
    IdentityConflict(String),
}

impl From<CudaContractError> for CudaSupervisorError {
    fn from(error: CudaContractError) -> Self {
        Self::Contract(error)
    }
}

impl std::fmt::Display for CudaSupervisorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(error) => std::fmt::Display::fmt(error, formatter),
            Self::Engine(error) => write!(formatter, "CUDA container engine error: {error}"),
            Self::Invariant(detail) => {
                write!(
                    formatter,
                    "CUDA container reconciliation invariant failed: {detail}"
                )
            }
            Self::ImageMismatch { expected, actual } => {
                write!(
                    formatter,
                    "CUDA image ID mismatch: expected {expected}, got {actual}"
                )
            }
            Self::IdentityConflict(name) => {
                write!(
                    formatter,
                    "container {name} has conflicting durable identity"
                )
            }
        }
    }
}

impl std::error::Error for CudaSupervisorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Invariant(_) | Self::ImageMismatch { .. } | Self::IdentityConflict(_) => None,
        }
    }
}

#[cfg(test)]
#[path = "cuda_supervisor_tests.rs"]
mod tests;
