//! Durable reconciliation state machine for policy-bound Ascend containers.

mod engine;

use crate::ascend::{AscendContractError, AscendFixturePolicy};
use crate::ascend_build::{AscendBuildContractError, AscendBuildPolicy};
use crate::backend_error::BackendError;
use crate::container_outcome::{
    ContainerTermination as Termination, CorrectnessOutcomePolicy, FixtureOutcomePolicy,
    classify_correctness_outcome, classify_fixture_outcome, enforce_output_limit,
};
use crate::container_supervision::{
    ContainerReconcileError, reconcile_container, supervise_running_container,
};
use crate::executor::{CancellationToken, ExecutorResult};
use crate::journal::StoredAssignment;
use crate::reduction_correctness::{CorrectnessContractError, ReductionCorrectnessPolicy};
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
const CORRECTNESS_OUTCOME_POLICY: CorrectnessOutcomePolicy = CorrectnessOutcomePolicy {
    exited: "Ascend correctness runner exited",
    nonzero: "Ascend correctness runner returned a nonzero exit code",
    invalid_receipt: "Ascend correctness runner emitted an invalid structured receipt",
};

#[derive(Clone, Debug)]
enum AscendSupervisorPolicy {
    Fixture(Arc<AscendFixturePolicy>),
    Build(Arc<AscendBuildPolicy>),
    Correctness {
        policy: Arc<ReductionCorrectnessPolicy>,
        device: alloyport_core::AcceleratorDevice,
        environment: crate::ascend::AscendEnvironmentFacts,
    },
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

    /// Creates a supervisor only when the supplied correctness policy owns the Ascend role.
    ///
    /// # Errors
    ///
    /// Returns an error when a CUDA correctness policy is supplied.
    pub fn new_correctness(
        policy: Arc<ReductionCorrectnessPolicy>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Result<Self, CorrectnessContractError> {
        let device = policy
            .ascend_device()
            .ok_or(CorrectnessContractError::WrongBackend)?
            .clone();
        let environment = policy
            .ascend_environment()
            .ok_or(CorrectnessContractError::WrongBackend)?
            .clone();
        Ok(Self {
            policy: AscendSupervisorPolicy::Correctness {
                policy,
                device,
                environment,
            },
            artifacts,
        })
    }

    #[must_use]
    pub fn device(&self) -> &alloyport_core::AcceleratorDevice {
        match &self.policy {
            AscendSupervisorPolicy::Fixture(policy) => policy.device(),
            AscendSupervisorPolicy::Build(policy) => policy.device(),
            AscendSupervisorPolicy::Correctness { device, .. } => device,
        }
    }

    #[must_use]
    pub fn environment(&self) -> &crate::ascend::AscendEnvironmentFacts {
        match &self.policy {
            AscendSupervisorPolicy::Fixture(policy) => policy.environment(),
            AscendSupervisorPolicy::Build(policy) => policy.environment(),
            AscendSupervisorPolicy::Correctness { environment, .. } => environment,
        }
    }

    #[must_use]
    pub const fn executor_kind(&self) -> alloyport_core::ExecutionKind {
        match self.policy {
            AscendSupervisorPolicy::Fixture(_) => alloyport_core::ExecutionKind::AscendFixture,
            AscendSupervisorPolicy::Build(_) => alloyport_core::ExecutionKind::AscendBuild,
            AscendSupervisorPolicy::Correctness { .. } => {
                alloyport_core::ExecutionKind::AscendCorrectness
            }
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
        let (source_digest, plan) = match &self.policy {
            AscendSupervisorPolicy::Fixture(policy) => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.source_digest().to_owned(),
                    policy.docker_create_plan(assignment, &sandbox)?,
                )
            }
            AscendSupervisorPolicy::Build(policy) => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.bundle_digest().to_string(),
                    policy.docker_create_plan(assignment, &sandbox)?,
                )
            }
            AscendSupervisorPolicy::Correctness { policy, .. } => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.implementation_digest().to_string(),
                    policy.ascend_docker_create_plan(assignment, &sandbox)?,
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
        let result = classify_outcome(
            &self.policy,
            termination,
            enforce_output_limit(logs, output_limit),
            assignment.execution.timeout_ms,
        );
        Ok(SupervisedAscendExecution {
            result,
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

fn classify_outcome(
    policy: &AscendSupervisorPolicy,
    termination: Termination,
    logs: ContainerLogs,
    timeout_ms: u64,
) -> ExecutorResult {
    match policy {
        AscendSupervisorPolicy::Correctness { .. } => {
            classify_correctness_outcome(termination, logs, timeout_ms, CORRECTNESS_OUTCOME_POLICY)
        }
        AscendSupervisorPolicy::Fixture(_) => {
            classify_fixture_outcome(termination, logs, timeout_ms, OUTCOME_POLICY)
        }
        AscendSupervisorPolicy::Build(_) => {
            classify_fixture_outcome(termination, logs, timeout_ms, BUILD_OUTCOME_POLICY)
        }
    }
}

#[derive(Debug)]
pub enum AscendSupervisorError {
    Contract(AscendContractError),
    BuildContract(AscendBuildContractError),
    CorrectnessContract(CorrectnessContractError),
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

impl From<CorrectnessContractError> for AscendSupervisorError {
    fn from(error: CorrectnessContractError) -> Self {
        Self::CorrectnessContract(error)
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
            Self::CorrectnessContract(error) => std::fmt::Display::fmt(error, formatter),
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
            Self::CorrectnessContract(error) => Some(error),
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
            AscendSupervisorError::CorrectnessContract(error) => match error {
                CorrectnessContractError::InvalidPolicy(_)
                | CorrectnessContractError::Assignment(_)
                | CorrectnessContractError::WrongBackend => Self::policy(detail),
                CorrectnessContractError::Artifact(_)
                | CorrectnessContractError::Bundle(_)
                | CorrectnessContractError::UnsafePath
                | CorrectnessContractError::Json(_) => Self::integrity(detail),
                CorrectnessContractError::Io(_) => Self::retryable(detail),
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
