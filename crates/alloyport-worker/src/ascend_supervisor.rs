//! Durable reconciliation state machine for policy-bound Ascend containers.

mod engine;

use crate::ascend::{AscendContractError, AscendFixturePolicy};
use crate::backend_error::BackendError;
use crate::container_outcome::{
    ContainerTermination as Termination, FixtureOutcomePolicy, classify_fixture_outcome,
    enforce_output_limit,
};
use crate::container_supervision::{
    ContainerReconcileError, reconcile_container, supervise_running_container,
};
use crate::executor::{CancellationToken, ExecutorResult};
use crate::journal::StoredAssignment;
use alloyport_artifacts::ArtifactStore;
pub use engine::{
    AscendContainerEngine, AscendExecutionFacts, ContainerEngineError, ContainerExit,
    ContainerIdentity, ContainerLogChunk, ContainerLogStream, ContainerLogs, ContainerPhase,
    ContainerSnapshot, EngineFuture, SupervisedAscendExecution,
};
use std::sync::Arc;

const OUTCOME_POLICY: FixtureOutcomePolicy = FixtureOutcomePolicy {
    fixture_id: crate::ascend::ASCEND_ADD_FIXTURE_ID,
    exited_detail: "Ascend fixture exited",
    nonzero_detail: "Ascend fixture returned a nonzero exit code",
    missing_marker_detail: "Ascend fixture exited zero without its verification marker",
};

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
            supervise_running_container(
                engine,
                &identity.name,
                assignment.execution.timeout_ms,
                output_limit,
                cancellation,
                &mut observer,
            )
            .await
            .map_err(AscendSupervisorError::Engine)?
        };
        Ok(SupervisedAscendExecution {
            result: classify_fixture_outcome(
                termination,
                enforce_output_limit(logs, output_limit),
                assignment.execution.timeout_ms,
                OUTCOME_POLICY,
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

impl From<ContainerReconcileError> for AscendSupervisorError {
    fn from(error: ContainerReconcileError) -> Self {
        match error {
            ContainerReconcileError::Engine(error) => Self::Engine(error),
            ContainerReconcileError::MissingAfterCreate(name) => Self::Invariant(format!(
                "container {name} is missing immediately after create"
            )),
            ContainerReconcileError::IdentityConflict(name) => Self::IdentityConflict(name),
            ContainerReconcileError::UnexpectedCreatedPhase { name, phase } => Self::Invariant(
                format!("new container {name} has unexpected phase {phase:?}"),
            ),
        }
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
