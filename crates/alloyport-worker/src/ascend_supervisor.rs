//! Durable reconciliation state machine for policy-bound Ascend containers.

mod engine;

use crate::ascend::{AscendContractError, AscendFixturePolicy};
use crate::ascend_build::{AscendBuildContractError, AscendBuildPolicy};
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
const BUILD_OUTCOME_POLICY: FixtureOutcomePolicy = FixtureOutcomePolicy {
    fixture_id: alloyport_core::ASCEND_BUILD_FEATURE,
    exited_detail: "Ascend build exited",
    nonzero_detail: "Ascend compiler or linker returned a nonzero exit code",
    missing_marker_detail: "Ascend build exited zero without its trusted completion marker",
};

#[derive(Clone, Debug)]
enum AscendSupervisorPolicy {
    Fixture(Arc<AscendFixturePolicy>),
    Build(Arc<AscendBuildPolicy>),
}

#[derive(Clone, Debug)]
pub struct AscendContainerSupervisor {
    policy: AscendSupervisorPolicy,
    artifacts: Arc<dyn ArtifactStore>,
}

impl AscendContainerSupervisor {
    #[must_use]
    pub const fn new(policy: Arc<AscendFixturePolicy>, artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self {
            policy: AscendSupervisorPolicy::Fixture(policy),
            artifacts,
        }
    }

    #[must_use]
    pub const fn new_build(
        policy: Arc<AscendBuildPolicy>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            policy: AscendSupervisorPolicy::Build(policy),
            artifacts,
        }
    }

    #[must_use]
    pub fn device(&self) -> &alloyport_core::AcceleratorDevice {
        match &self.policy {
            AscendSupervisorPolicy::Fixture(policy) => policy.device(),
            AscendSupervisorPolicy::Build(policy) => policy.device(),
        }
    }

    #[must_use]
    pub fn environment(&self) -> &crate::ascend::AscendEnvironmentFacts {
        match &self.policy {
            AscendSupervisorPolicy::Fixture(policy) => policy.environment(),
            AscendSupervisorPolicy::Build(policy) => policy.environment(),
        }
    }

    #[must_use]
    pub const fn executor_kind(&self) -> alloyport_core::ExecutionKind {
        match self.policy {
            AscendSupervisorPolicy::Fixture(_) => alloyport_core::ExecutionKind::AscendFixture,
            AscendSupervisorPolicy::Build(_) => alloyport_core::ExecutionKind::AscendBuild,
        }
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
        let (source_digest, plan, outcome_policy) = match &self.policy {
            AscendSupervisorPolicy::Fixture(policy) => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.source_digest().to_owned(),
                    policy.docker_create_plan(assignment, &sandbox)?,
                    OUTCOME_POLICY,
                )
            }
            AscendSupervisorPolicy::Build(policy) => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.bundle_digest().to_string(),
                    policy.docker_create_plan(assignment, &sandbox)?,
                    BUILD_OUTCOME_POLICY,
                )
            }
        };
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
                outcome_policy,
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
    BuildContract(AscendBuildContractError),
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

impl From<AscendBuildContractError> for AscendSupervisorError {
    fn from(error: AscendBuildContractError) -> Self {
        Self::BuildContract(error)
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
            Self::BuildContract(error) => std::fmt::Display::fmt(error, formatter),
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
            Self::BuildContract(error) => Some(error),
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
            AscendSupervisorError::BuildContract(error) => match error {
                AscendBuildContractError::InvalidPolicy(_)
                | AscendBuildContractError::Assignment(_) => Self::policy(detail),
                AscendBuildContractError::Artifact(_)
                | AscendBuildContractError::Bundle(_)
                | AscendBuildContractError::UnsafePath
                | AscendBuildContractError::Json(_) => Self::integrity(detail),
                AscendBuildContractError::Io(_) => Self::retryable(detail),
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
