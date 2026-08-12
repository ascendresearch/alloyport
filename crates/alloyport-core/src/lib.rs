//! Domain primitives for `AlloyPort`'s verified delivery lifecycle.

mod agent;
mod agent_runtime;
mod agent_runtime_helpers;
mod agent_runtime_policy;
mod agent_runtime_support;
mod artifact;
mod assignment;
mod candidate_source;
mod device;
mod execution;
mod generation;
mod identity;
mod inspection;
mod migration;
mod model;
mod model_codec;
mod model_codec_anthropic;
mod model_codec_chat;
mod model_codec_responses;
mod model_gateway;
mod model_transport;
mod model_transport_policy;

#[cfg(test)]
mod model_codec_tests;

pub use agent::{
    AGENT_EPISODE_SCHEMA_V1, AgentEpisodeRecord, AgentRecordError, EpisodeSpec, EpisodeStatus,
    SEARCH_RUN_SCHEMA_V1, SearchPhase, SearchRunRecord, SearchRunSpec, SearchStatus,
    TOOL_OPERATION_SCHEMA_V1, ToolEffectClass, ToolOperationRecord, ToolOperationSpec,
    ToolOperationStatus, ToolResultAuthority,
};
pub use agent_runtime::{AgentLoopRunner, AgentLoopRuntimeSpec, DurableEpisodeState};
pub use agent_runtime_helpers::AgentLoopRuntimeError;
pub use agent_runtime_helpers::derive_model_continuation_input_digest;
pub use agent_runtime_policy::AgentLoopPolicy;
pub use agent_runtime_support::{
    AgentLoopAdvance, AgentRuntimeFaultInjector, AgentRuntimeFaultPoint, AgentToolGateway,
    EpisodeRepository, EpisodeRepositoryError, InMemoryEpisodeRepository, NoAgentRuntimeFault,
    OneShotAgentRuntimeFault, RuntimeToolDescriptor, ScriptedFakeToolGateway, ScriptedToolStep,
    ToolGatewayAction, ToolGatewayError, ToolGatewayOutcome, ToolInvocation, VersionedEpisodeState,
};
pub use artifact::{ArtifactDescriptor, DigestParseError, Sha256Digest};
pub use assignment::{AssignmentContract, EnvironmentEntry, ExecutionContract, ResourceContract};
pub use candidate_source::{
    CANDIDATE_SOURCE_MANIFEST_SCHEMA_V1, CandidateSourceError, CandidateSourceFile,
    CandidateSourceManifest, CandidateSourceManifestSpec, SOURCE_GATE_RECEIPT_SCHEMA_V1,
    SOURCE_GATE_REVISION_V1, SourceGateFailure, SourceGateFailureKind, SourceGateReceipt,
    evaluate_source_gate,
};
pub use device::{
    AcceleratorDevice, DeviceHealth, DeviceHealthError, DeviceLease, DeviceObservation,
};
pub use execution::{
    AttemptOutcome, AttemptOutcomeError, ExecutionKind, ExecutionKindError, NetworkPolicy,
    NetworkPolicyError, RejectionReason, RejectionReasonError,
};
pub use generation::{
    AUTHORING_REQUEST_SCHEMA_V1, CandidateAuthoringError, CandidateAuthoringRequest,
    CandidateProposal, GeneratedSourceBundle, GeneratedSourceError, GeneratedSourceFile,
    GeneratedSourceKind, ModelInvocation, SourceDocument,
};
pub use identity::{
    AssignmentId, AssignmentIdError, AttemptId, AttemptIdError, CandidateId, CandidateIdError,
    EpisodeId, EpisodeIdError, ModelAttemptId, ModelAttemptIdError, SearchRunId, SearchRunIdError,
    TaskId, TaskIdError, ToolOperationId, ToolOperationIdError, TurnId, TurnIdError,
};
pub use inspection::{
    InspectionEvidence, InspectionEvidenceKind, InspectionFailure, InspectionFailureKind,
    MigrationInspection, inspect_migration_source,
};
pub use migration::{
    AscendTarget, BundlePath, CudaSourceSet, MIGRATION_SPEC_SCHEMA_V1, MigrationSpec,
    MigrationSpecError, PublicEntryPoint, ReferenceWorkload,
};
pub use model::{
    MODEL_ATTEMPT_SCHEMA_V1, ModelAttemptError, ModelAttemptRecord, ModelAttemptSpec,
    ModelAttemptStatus, ModelAuthConfig, ModelCatalogError, ModelDataBoundary,
    ModelDeploymentConfig, ModelGenerationSettings, ModelProfileConfig, ModelUsage, ProtocolConfig,
    ProtocolKind, RUNTIME_MODEL_CATALOG_SCHEMA_V1, ReasoningEffort, ReasoningMode,
    ReasoningSettings, ResolvedRuntimeModel, RuntimeModelCatalog, RuntimeModelConfig,
    ToolSchemaDialect,
};
pub use model_codec::{
    CodecError, CodecLimits, CodecToolDefinition, DecodedModelTurn, ModelVisibleToolResult,
    NATIVE_CONTINUATION_SCHEMA_V1, NativeContinuation, NativeTurnInput, PreparedModelPayload,
    ProtocolCodec, RawModelResponseRef,
};
pub use model_codec_anthropic::AnthropicMessagesCodec;
pub use model_codec_chat::OpenAiChatCompletionsCodec;
pub use model_codec_responses::OpenAiResponsesCodec;
pub use model_gateway::{
    GatewayToolCall, GatewayTurn, GatewayTurnExchange, ModelGateway, ModelGatewayError,
    ModelGatewayFuture, ModelGatewayOutcome, ModelTurnRequest, NormalizedStopReason,
    ScriptedFakeModelGateway, ScriptedGatewayStep, TURN_RECORD_SCHEMA_V1, TurnRecord,
    TurnRecordError, TurnSpec,
};
pub use model_transport::{
    ModelTransport, ModelTransportFailure, ModelTransportFailureKind, ModelTransportFuture,
    ModelTransportOutcome, ModelTransportRetryHint, RawModelResponse, ScriptedFakeModelTransport,
    ScriptedModelTransportStep,
};
pub use model_transport_policy::{
    ModelProxyPolicy, ModelRedirectPolicy, ModelTlsMinimumVersion, ModelTransportPolicy,
};

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Strategy used to produce an Ascend C candidate.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GenerationStrategy {
    DirectAscendC,
    AscendSimtBootstrap,
    VerifiedTemplateAdaptation,
    MemoryGuidedSynthesis,
}

/// Durable task lifecycle. Terminal states have no outgoing transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TaskState {
    Captured,
    Specified,
    Generating,
    Building,
    Verifying,
    Optimizing,
    Integrating,
    Releasable,
    Released,
    Failed,
}

impl TaskState {
    /// Returns whether moving from this state to `next` preserves the lifecycle invariant.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Captured, Self::Specified | Self::Failed)
                | (Self::Specified, Self::Generating | Self::Failed)
                | (Self::Generating, Self::Building | Self::Failed)
                | (
                    Self::Building,
                    Self::Generating | Self::Verifying | Self::Failed
                )
                | (
                    Self::Verifying,
                    Self::Generating | Self::Optimizing | Self::Failed
                )
                | (
                    Self::Optimizing,
                    Self::Building | Self::Integrating | Self::Failed
                )
                | (
                    Self::Integrating,
                    Self::Generating | Self::Releasable | Self::Failed
                )
                | (Self::Releasable, Self::Released | Self::Failed)
        )
    }
}

/// A migration task with explicit lifecycle and selected generation strategy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Task {
    pub id: TaskId,
    pub source_revision: String,
    pub migration_spec_digest: Option<Sha256Digest>,
    pub state: TaskState,
    pub generation_strategy: Option<GenerationStrategy>,
}

impl Task {
    /// Moves the task to another valid lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] when the requested transition violates the state machine.
    pub fn transition(&mut self, next: TaskState) -> Result<(), TransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(TransitionError {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }
}

/// An immutable implementation candidate and its lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub id: CandidateId,
    pub task_id: TaskId,
    pub migration_spec_digest: Sha256Digest,
    pub generation_strategy: GenerationStrategy,
    pub parent_id: Option<CandidateId>,
    pub source_digest: Sha256Digest,
    pub artifact_digest: Option<Sha256Digest>,
}

/// Gate evaluated independently from candidate generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Gate {
    Contract,
    Source,
    Build,
    Correctness,
    Performance,
    Integration,
}

impl Gate {
    pub const ALL: [Self; 6] = [
        Self::Contract,
        Self::Source,
        Self::Build,
        Self::Correctness,
        Self::Performance,
        Self::Integration,
    ];
}

/// Independent decision for one candidate at one gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Verdict {
    pub candidate_id: CandidateId,
    pub gate: Gate,
    pub passed: bool,
    pub receipt_digests: Vec<Sha256Digest>,
}

/// Immutable release description presented to integration and deployment tooling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseManifest {
    pub candidate_id: CandidateId,
    pub supported_domain: String,
    pub dispatch_guard: String,
    pub fallback: String,
    pub source_artifact_digests: BTreeSet<Sha256Digest>,
    pub evidence_digests: BTreeSet<Sha256Digest>,
}

impl ReleaseManifest {
    /// Builds a manifest only when all release gates pass and every verdict has evidence.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError`] for missing, failed, duplicate, or evidence-free gate verdicts.
    pub fn from_verdicts(
        candidate_id: CandidateId,
        supported_domain: impl Into<String>,
        dispatch_guard: impl Into<String>,
        fallback: impl Into<String>,
        source_artifact_digests: BTreeSet<Sha256Digest>,
        verdicts: &[Verdict],
    ) -> Result<Self, ReleaseError> {
        if source_artifact_digests.is_empty() {
            return Err(ReleaseError::MissingSourceArtifacts);
        }
        let mut passed = BTreeSet::new();
        let mut evidence_digests = BTreeSet::new();

        for verdict in verdicts {
            if verdict.candidate_id != candidate_id {
                return Err(ReleaseError::CandidateMismatch);
            }
            if !passed.insert(verdict.gate) {
                return Err(ReleaseError::DuplicateGate(verdict.gate));
            }
            if !verdict.passed {
                return Err(ReleaseError::FailedGate(verdict.gate));
            }
            if verdict.receipt_digests.is_empty() {
                return Err(ReleaseError::MissingEvidence(verdict.gate));
            }
            evidence_digests.extend(verdict.receipt_digests.iter().copied());
        }

        for gate in Gate::ALL {
            if !passed.contains(&gate) {
                return Err(ReleaseError::MissingGate(gate));
            }
        }

        Ok(Self {
            candidate_id,
            supported_domain: supported_domain.into(),
            dispatch_guard: dispatch_guard.into(),
            fallback: fallback.into(),
            source_artifact_digests,
            evidence_digests,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionError {
    pub from: TaskState,
    pub to: TaskState,
}

impl Display for TransitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid task transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

impl Error for TransitionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseError {
    CandidateMismatch,
    DuplicateGate(Gate),
    FailedGate(Gate),
    MissingEvidence(Gate),
    MissingGate(Gate),
    MissingSourceArtifacts,
}

impl Display for ReleaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateMismatch => write!(formatter, "verdict belongs to another candidate"),
            Self::DuplicateGate(gate) => write!(formatter, "duplicate verdict for {gate:?}"),
            Self::FailedGate(gate) => write!(formatter, "gate {gate:?} did not pass"),
            Self::MissingEvidence(gate) => write!(formatter, "gate {gate:?} has no receipts"),
            Self::MissingGate(gate) => write!(formatter, "gate {gate:?} has no verdict"),
            Self::MissingSourceArtifacts => {
                write!(formatter, "release has no generated source artifacts")
            }
        }
    }
}

impl Error for ReleaseError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn passing_verdicts(candidate_id: &CandidateId) -> Vec<Verdict> {
        Gate::ALL
            .into_iter()
            .map(|gate| Verdict {
                candidate_id: candidate_id.clone(),
                gate,
                passed: true,
                receipt_digests: vec![Sha256Digest::digest_bytes(format!("{gate:?}").as_bytes())],
            })
            .collect()
    }

    fn source_artifacts() -> BTreeSet<Sha256Digest> {
        [Sha256Digest::digest_bytes(b"ascend-c-source")]
            .into_iter()
            .collect()
    }

    #[test]
    fn lifecycle_allows_rework_after_verification() {
        assert!(TaskState::Verifying.can_transition_to(TaskState::Generating));
        assert!(TaskState::Optimizing.can_transition_to(TaskState::Building));
        assert!(!TaskState::Released.can_transition_to(TaskState::Generating));
    }

    #[test]
    fn release_requires_every_gate() {
        let candidate_id = CandidateId::try_from("candidate-1").expect("valid candidate ID");
        let mut verdicts = passing_verdicts(&candidate_id);
        verdicts.retain(|verdict| verdict.gate != Gate::Performance);

        let error = ReleaseManifest::from_verdicts(
            candidate_id,
            "M,N,K divisible by 16",
            "shape_guard_v1",
            "torch_reference",
            source_artifacts(),
            &verdicts,
        )
        .expect_err("a release without performance evidence must fail");

        assert_eq!(error, ReleaseError::MissingGate(Gate::Performance));
    }

    #[test]
    fn release_collects_content_addressed_evidence() {
        let candidate_id = CandidateId::try_from("candidate-1").expect("valid candidate ID");
        let verdicts = passing_verdicts(&candidate_id);
        let manifest = ReleaseManifest::from_verdicts(
            candidate_id,
            "all tested shapes",
            "always",
            "torch_reference",
            source_artifacts(),
            &verdicts,
        )
        .expect("all gates have independent evidence");

        assert_eq!(manifest.evidence_digests.len(), Gate::ALL.len());
        assert_eq!(manifest.source_artifact_digests.len(), 1);
    }

    #[test]
    fn release_requires_generated_source_even_when_gates_pass() {
        let candidate_id = CandidateId::try_from("candidate-1").expect("valid candidate ID");
        let verdicts = passing_verdicts(&candidate_id);
        let error = ReleaseManifest::from_verdicts(
            candidate_id,
            "all tested shapes",
            "always",
            "return unsupported-domain status",
            BTreeSet::new(),
            &verdicts,
        )
        .expect_err("a source-less release is outside the product contract");

        assert_eq!(error, ReleaseError::MissingSourceArtifacts);
    }

    #[test]
    fn candidate_is_bound_to_spec_and_generation_strategy() {
        let spec_digest = Sha256Digest::digest_bytes(b"migration-spec-v1");
        let candidate = Candidate {
            id: CandidateId::try_from("candidate-1").expect("candidate ID"),
            task_id: TaskId::try_from("task-1").expect("task ID"),
            migration_spec_digest: spec_digest,
            generation_strategy: GenerationStrategy::DirectAscendC,
            parent_id: None,
            source_digest: Sha256Digest::digest_bytes(b"generated-source"),
            artifact_digest: None,
        };

        assert_eq!(
            candidate.generation_strategy,
            GenerationStrategy::DirectAscendC
        );
    }
}
