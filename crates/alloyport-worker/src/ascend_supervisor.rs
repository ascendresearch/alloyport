//! Durable reconciliation state machine for policy-bound Ascend containers.

mod engine;
mod outcome;

pub use engine::{
    AscendContainerEngine, AscendExecutionFacts, ContainerEngineError, ContainerExit,
    ContainerIdentity, ContainerLogChunk, ContainerLogStream, ContainerLogs, ContainerPhase,
    ContainerSnapshot, EngineFuture, SupervisedAscendExecution,
};
use outcome::{Termination, classify, enforce_output_limit};

use crate::ascend::{AscendContractError, AscendDockerCreatePlan, AscendFixturePolicy};
use crate::backend_error::BackendError;
use crate::executor::{CancellationToken, ExecutorResult};
use crate::journal::StoredAssignment;
use alloyport_artifacts::ArtifactStore;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct AscendContainerSupervisor {
    policy: Arc<AscendFixturePolicy>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl AscendContainerSupervisor {
    #[must_use]
    pub const fn new(policy: Arc<AscendFixturePolicy>, artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self { policy, artifacts }
    }

    #[must_use]
    pub fn device(&self) -> &alloyport_core::AcceleratorDevice {
        self.policy.device()
    }

    #[must_use]
    pub fn environment(&self) -> &crate::ascend::AscendEnvironmentFacts {
        self.policy.environment()
    }

    /// Reconciles a stable attempt container and returns bounded terminal data.
    ///
    /// # Errors
    ///
    /// Returns an error for policy, identity, image, invariant, or engine failures.
    pub async fn run(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn AscendContainerEngine,
        cancellation: &CancellationToken,
    ) -> Result<ExecutorResult, AscendSupervisorError> {
        Ok(self
            .run_with_facts(assignment, engine, cancellation)
            .await?
            .result)
    }

    /// Runs reconciliation while retaining immutable receipt facts.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`].
    pub async fn run_with_facts(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn AscendContainerEngine,
        cancellation: &CancellationToken,
    ) -> Result<SupervisedAscendExecution, AscendSupervisorError> {
        self.run_with_facts_observed(assignment, engine, cancellation, |_| {})
            .await
    }

    /// Runs reconciliation and forwards best-effort bounded output observations.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::run`].
    pub async fn run_with_facts_observed<F>(
        &self,
        assignment: &StoredAssignment,
        engine: &dyn AscendContainerEngine,
        cancellation: &CancellationToken,
        mut observer: F,
    ) -> Result<SupervisedAscendExecution, AscendSupervisorError>
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
            bundle_digest: assignment.execution.bundle.digest.to_string(),
            image_manifest_digest: assignment.execution.image.digest.to_string(),
            image_id: plan.expected_image_id.to_string(),
        };
        let resolved_image = engine
            .resolve_image_id(&plan)
            .await
            .map_err(AscendSupervisorError::Engine)?;
        if resolved_image != identity.image_id {
            return Err(AscendSupervisorError::ImageMismatch {
                expected: identity.image_id,
                actual: resolved_image,
            });
        }

        let phase = reconcile_container(engine, &plan, &identity).await?;
        if phase == ContainerPhase::Created {
            engine
                .start(&identity.name)
                .await
                .map_err(AscendSupervisorError::Engine)?;
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
                .map_err(AscendSupervisorError::Engine)?;
            let logs = engine
                .logs(&identity.name, output_limit)
                .await
                .map_err(AscendSupervisorError::Engine)?;
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
        Ok(SupervisedAscendExecution {
            result: classify(
                termination,
                enforce_output_limit(logs, output_limit),
                assignment.execution.timeout_ms,
            ),
            facts: AscendExecutionFacts {
                container_name: identity.name,
                bundle_digest: identity.bundle_digest,
                source_digest,
                image_digest: identity.image_manifest_digest,
                image_media_type: assignment.execution.image.media_type.clone(),
                image_id: identity.image_id,
                device: plan.device,
                environment: plan.environment,
            },
            live_output_streaming,
        })
    }
}

async fn collect_running(
    engine: &dyn AscendContainerEngine,
    name: &str,
    timeout_ms: u64,
    output_limit: u64,
    cancellation: &CancellationToken,
    observer: &mut (dyn FnMut(ContainerLogChunk) + Send),
) -> Result<(Termination, ContainerLogs, bool), AscendSupervisorError> {
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
            exit = &mut wait => Termination::Exited(exit.map_err(AscendSupervisorError::Engine)?),
            logs = &mut follow, if collected_logs.is_none() => {
                let logs = match logs {
                    Ok(logs) => logs,
                    Err(error) => {
                        let _ = engine.stop(name).await;
                        let _ = (&mut wait).await;
                        return Err(AscendSupervisorError::Engine(error));
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
            follow.await.map_err(AscendSupervisorError::Engine)?
        };
        return Ok((termination, logs, live_output_streaming));
    }
}

async fn stop_and_wait(
    engine: &dyn AscendContainerEngine,
    name: &str,
    wait: &mut EngineFuture<'_, ContainerExit>,
) -> Result<ContainerExit, AscendSupervisorError> {
    engine
        .stop(name)
        .await
        .map_err(AscendSupervisorError::Engine)?;
    wait.await.map_err(AscendSupervisorError::Engine)
}

async fn reconcile_container(
    engine: &dyn AscendContainerEngine,
    plan: &AscendDockerCreatePlan,
    identity: &ContainerIdentity,
) -> Result<ContainerPhase, AscendSupervisorError> {
    if let Some(snapshot) = engine
        .inspect(&identity.name)
        .await
        .map_err(AscendSupervisorError::Engine)?
    {
        if snapshot.identity != *identity {
            return Err(AscendSupervisorError::IdentityConflict(
                identity.name.clone(),
            ));
        }
        return Ok(snapshot.phase);
    }
    engine
        .create(plan, identity)
        .await
        .map_err(AscendSupervisorError::Engine)?;
    let created = engine
        .inspect(&identity.name)
        .await
        .map_err(AscendSupervisorError::Engine)?
        .ok_or_else(|| {
            AscendSupervisorError::Invariant(format!(
                "container {} is missing immediately after create",
                identity.name
            ))
        })?;
    if created.identity != *identity {
        return Err(AscendSupervisorError::IdentityConflict(
            identity.name.clone(),
        ));
    }
    if created.phase != ContainerPhase::Created {
        return Err(AscendSupervisorError::Invariant(format!(
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
pub enum AscendSupervisorError {
    Contract(AscendContractError),
    Engine(ContainerEngineError),
    Invariant(String),
    ImageMismatch { expected: String, actual: String },
    IdentityConflict(String),
}

impl From<AscendContractError> for AscendSupervisorError {
    fn from(error: AscendContractError) -> Self {
        Self::Contract(error)
    }
}

impl std::fmt::Display for AscendSupervisorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(error) => std::fmt::Display::fmt(error, formatter),
            Self::Engine(error) => write!(formatter, "Ascend container engine error: {error}"),
            Self::Invariant(detail) => write!(
                formatter,
                "Ascend container reconciliation invariant failed: {detail}"
            ),
            Self::ImageMismatch { expected, actual } => {
                write!(
                    formatter,
                    "Ascend image ID mismatch: expected {expected}, got {actual}"
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

impl std::error::Error for AscendSupervisorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Invariant(_) | Self::ImageMismatch { .. } | Self::IdentityConflict(_) => None,
        }
    }
}

impl From<AscendSupervisorError> for BackendError {
    fn from(error: AscendSupervisorError) -> Self {
        let detail = error.to_string();
        match error {
            AscendSupervisorError::Contract(error) => match error {
                AscendContractError::InvalidPolicy(_) | AscendContractError::Assignment(_) => {
                    Self::policy(detail)
                }
                AscendContractError::Artifact(_)
                | AscendContractError::Bundle(_)
                | AscendContractError::Json(_) => Self::integrity(detail),
                AscendContractError::Io(_) => Self::retryable(detail),
            },
            AscendSupervisorError::Engine(error) => Self::from(error),
            AscendSupervisorError::Invariant(_) => Self::terminal(detail),
            AscendSupervisorError::ImageMismatch { .. }
            | AscendSupervisorError::IdentityConflict(_) => Self::integrity(detail),
        }
    }
}

#[cfg(test)]
#[path = "ascend_supervisor_tests.rs"]
mod tests;
