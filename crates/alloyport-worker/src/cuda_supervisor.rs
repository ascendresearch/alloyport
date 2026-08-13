//! Durable reconciliation state machine for policy-bound CUDA containers.

mod engine;

use crate::backend_error::BackendError;
use crate::container_outcome::{
    ContainerTermination as Termination, CorrectnessOutcomePolicy, FixtureOutcomePolicy,
    classify_correctness_outcome, classify_fixture_outcome, enforce_output_limit,
};
use crate::container_supervision::{
    ContainerReconcileError, reconcile_container, supervise_running_container,
};
use crate::cuda::{CudaContractError, CudaFixturePolicy};
use crate::executor::{CancellationToken, ExecutorResult};
use crate::journal::StoredAssignment;
use crate::reduction_correctness::{CorrectnessContractError, ReductionCorrectnessPolicy};
use alloyport_artifacts::ArtifactStore;
pub use engine::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream,
    ContainerLogs, ContainerPhase, ContainerSnapshot, CudaContainerEngine, CudaExecutionFacts,
    EngineFuture, SupervisedCudaExecution,
};
use std::sync::Arc;

const OUTCOME_POLICY: FixtureOutcomePolicy = FixtureOutcomePolicy {
    fixture_id: crate::cuda::VECTOR_ADD_FIXTURE_ID,
    exited_detail: "CUDA fixture exited",
    nonzero_detail: "CUDA fixture returned a nonzero exit code",
    missing_marker_detail: "CUDA fixture exited zero without its verification marker",
};
const CORRECTNESS_OUTCOME_POLICY: CorrectnessOutcomePolicy = CorrectnessOutcomePolicy {
    exited: "CUDA correctness runner exited",
    nonzero: "CUDA correctness runner returned a nonzero exit code",
    invalid_receipt: "CUDA correctness runner emitted an invalid structured receipt",
};

#[derive(Clone, Debug)]
enum CudaSupervisorPolicy {
    Fixture(Arc<CudaFixturePolicy>),
    Correctness(Arc<ReductionCorrectnessPolicy>),
}

#[derive(Clone, Debug)]
pub struct CudaContainerSupervisor {
    policy: CudaSupervisorPolicy,
    artifacts: Arc<dyn ArtifactStore>,
}

impl CudaContainerSupervisor {
    #[must_use]
    pub const fn new(policy: Arc<CudaFixturePolicy>, artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self {
            policy: CudaSupervisorPolicy::Fixture(policy),
            artifacts,
        }
    }

    #[must_use]
    pub const fn new_correctness(
        policy: Arc<ReductionCorrectnessPolicy>,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            policy: CudaSupervisorPolicy::Correctness(policy),
            artifacts,
        }
    }

    #[must_use]
    pub fn device_id(&self) -> &str {
        match &self.policy {
            CudaSupervisorPolicy::Fixture(policy) => policy.device_id(),
            CudaSupervisorPolicy::Correctness(policy) => policy.device_id(),
        }
    }

    #[must_use]
    pub const fn executor_kind(&self) -> alloyport_core::ExecutionKind {
        match self.policy {
            CudaSupervisorPolicy::Fixture(_) => alloyport_core::ExecutionKind::CudaFixture,
            CudaSupervisorPolicy::Correctness(_) => alloyport_core::ExecutionKind::CudaCorrectness,
        }
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
        let (source_digest, plan, correctness) = match &self.policy {
            CudaSupervisorPolicy::Fixture(policy) => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.source_digest().to_owned(),
                    policy.docker_create_plan(assignment, &sandbox)?,
                    false,
                )
            }
            CudaSupervisorPolicy::Correctness(policy) => {
                let sandbox = policy.materialize_bundle(assignment, self.artifacts.as_ref())?;
                (
                    sandbox.implementation_digest().to_string(),
                    policy.cuda_docker_create_plan(assignment, &sandbox)?,
                    true,
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
            supervise_running_container(
                engine,
                &identity.name,
                assignment.execution.timeout_ms,
                output_limit,
                cancellation,
                &mut observer,
            )
            .await
            .map_err(CudaSupervisorError::Engine)?
        };
        let logs = enforce_output_limit(logs, output_limit);
        let result = if correctness {
            classify_correctness_outcome(
                termination,
                logs,
                assignment.execution.timeout_ms,
                CORRECTNESS_OUTCOME_POLICY,
            )
        } else {
            classify_fixture_outcome(
                termination,
                logs,
                assignment.execution.timeout_ms,
                OUTCOME_POLICY,
            )
        };
        Ok(SupervisedCudaExecution {
            result,
            facts: CudaExecutionFacts {
                container_name: identity.name,
                bundle_digest: identity.bundle_digest,
                source_digest,
                image_digest: identity.image_manifest_digest,
                image_media_type: assignment.execution.image.media_type.clone(),
                image_id: identity.image_id,
                device_id: plan.device_id,
            },
            live_output_streaming,
        })
    }
}

#[derive(Debug)]
pub enum CudaSupervisorError {
    Contract(CudaContractError),
    CorrectnessContract(CorrectnessContractError),
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

impl From<CorrectnessContractError> for CudaSupervisorError {
    fn from(error: CorrectnessContractError) -> Self {
        Self::CorrectnessContract(error)
    }
}

impl From<ContainerReconcileError> for CudaSupervisorError {
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

impl std::fmt::Display for CudaSupervisorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(error) => std::fmt::Display::fmt(error, formatter),
            Self::CorrectnessContract(error) => std::fmt::Display::fmt(error, formatter),
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
            Self::CorrectnessContract(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Invariant(_) | Self::ImageMismatch { .. } | Self::IdentityConflict(_) => None,
        }
    }
}

impl From<CudaSupervisorError> for BackendError {
    fn from(error: CudaSupervisorError) -> Self {
        let detail = error.to_string();
        match error {
            CudaSupervisorError::Contract(error) => match error {
                CudaContractError::InvalidPolicy(_) | CudaContractError::Assignment(_) => {
                    Self::policy(detail)
                }
                CudaContractError::Digest(_)
                | CudaContractError::Artifact(_)
                | CudaContractError::Bundle(_)
                | CudaContractError::Json(_) => Self::integrity(detail),
                CudaContractError::Io(_) => Self::retryable(detail),
            },
            CudaSupervisorError::CorrectnessContract(error) => match error {
                CorrectnessContractError::InvalidPolicy(_)
                | CorrectnessContractError::Assignment(_)
                | CorrectnessContractError::WrongBackend => Self::policy(detail),
                CorrectnessContractError::Artifact(_)
                | CorrectnessContractError::Bundle(_)
                | CorrectnessContractError::UnsafePath
                | CorrectnessContractError::Json(_) => Self::integrity(detail),
                CorrectnessContractError::Io(_) => Self::retryable(detail),
            },
            CudaSupervisorError::Engine(error) => Self::from(error),
            CudaSupervisorError::Invariant(_) => Self::terminal(detail),
            CudaSupervisorError::ImageMismatch { .. }
            | CudaSupervisorError::IdentityConflict(_) => Self::integrity(detail),
        }
    }
}

#[cfg(test)]
#[path = "cuda_supervisor_tests.rs"]
mod tests;
