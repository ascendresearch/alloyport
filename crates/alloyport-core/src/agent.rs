//! Durable, provider-neutral records for agent episodes, tool operations, and candidate search.

use crate::{CandidateId, EpisodeId, SearchRunId, Sha256Digest, TaskId, ToolOperationId, TurnId};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

pub const AGENT_EPISODE_SCHEMA_V1: u16 = 1;
pub const SEARCH_RUN_SCHEMA_V1: u16 = 1;
pub const TOOL_OPERATION_SCHEMA_V1: u16 = 1;

/// Durable state of one bounded, model-pinned agent episode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeStatus {
    Created,
    ReadyForModel,
    ModelAttemptPending,
    TurnRecorded,
    ToolWorkPending,
    StopReview,
    SuspensionRequested,
    Suspended,
    CancellationPending,
    Succeeded,
    Incomplete,
    Cancelled,
    BudgetExhausted,
    Failed,
}

impl EpisodeStatus {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Created => matches!(next, Self::ReadyForModel | Self::CancellationPending),
            Self::ReadyForModel => matches!(
                next,
                Self::ModelAttemptPending
                    | Self::SuspensionRequested
                    | Self::CancellationPending
                    | Self::BudgetExhausted
                    | Self::Failed
            ),
            Self::ModelAttemptPending => matches!(
                next,
                Self::TurnRecorded
                    | Self::ReadyForModel
                    | Self::SuspensionRequested
                    | Self::CancellationPending
                    | Self::BudgetExhausted
                    | Self::Failed
            ),
            // `BudgetExhausted` belongs here because `plan_turn_tools` runs in this state and ends
            // the Episode when the total tool-operation budget is spent. Every other state that
            // can reach it listed it; this one did not, so that branch could only ever produce
            // `invalid episode transition: TurnRecorded -> BudgetExhausted`. It was dead on
            // arrival and a live migration found it.
            Self::TurnRecorded => matches!(
                next,
                Self::ToolWorkPending
                    | Self::StopReview
                    | Self::CancellationPending
                    | Self::BudgetExhausted
                    | Self::Failed
            ),
            Self::ToolWorkPending => matches!(
                next,
                Self::ReadyForModel
                    | Self::SuspensionRequested
                    | Self::CancellationPending
                    | Self::BudgetExhausted
                    | Self::Failed
            ),
            Self::StopReview => matches!(
                next,
                Self::ReadyForModel
                    | Self::Succeeded
                    | Self::Incomplete
                    | Self::SuspensionRequested
                    | Self::CancellationPending
                    | Self::BudgetExhausted
                    | Self::Failed
            ),
            Self::SuspensionRequested => {
                matches!(
                    next,
                    Self::Suspended | Self::CancellationPending | Self::Failed
                )
            }
            Self::Suspended => {
                matches!(
                    next,
                    Self::ReadyForModel
                        | Self::ToolWorkPending
                        | Self::CancellationPending
                        | Self::Failed
                )
            }
            Self::CancellationPending => matches!(next, Self::Cancelled | Self::Suspended),
            Self::Succeeded
            | Self::Incomplete
            | Self::Cancelled
            | Self::BudgetExhausted
            | Self::Failed => false,
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Incomplete
                | Self::Cancelled
                | Self::BudgetExhausted
                | Self::Failed
        )
    }
}

/// Immutable inputs captured when an episode is created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpisodeSpec {
    pub id: EpisodeId,
    pub task_id: TaskId,
    pub search_run_id: SearchRunId,
    pub parent_candidate_id: Option<CandidateId>,
    pub subtask_contract_digest: Sha256Digest,
    pub context_projection_digest: Sha256Digest,
    pub input_artifact_root_digest: Sha256Digest,
    pub runtime_model_alias: String,
    pub resolved_model_digest: Sha256Digest,
    pub prompt_revision: String,
    pub tool_catalog_digest: Sha256Digest,
    pub loop_policy_digest: Sha256Digest,
    pub data_boundary_policy_digest: Sha256Digest,
    pub budget_snapshot_digest: Sha256Digest,
}

/// Persistable episode aggregate. State changes only through the checked reducer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentEpisodeRecord {
    schema_version: u16,
    id: EpisodeId,
    task_id: TaskId,
    search_run_id: SearchRunId,
    parent_candidate_id: Option<CandidateId>,
    subtask_contract_digest: Sha256Digest,
    context_projection_digest: Sha256Digest,
    input_artifact_root_digest: Sha256Digest,
    runtime_model_alias: String,
    resolved_model_digest: Sha256Digest,
    prompt_revision: String,
    tool_catalog_digest: Sha256Digest,
    loop_policy_digest: Sha256Digest,
    data_boundary_policy_digest: Sha256Digest,
    budget_snapshot_digest: Sha256Digest,
    status: EpisodeStatus,
}

impl AgentEpisodeRecord {
    /// Creates an episode from one immutable resolved snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when a required snapshot label is empty.
    pub fn new(spec: EpisodeSpec) -> Result<Self, AgentRecordError> {
        require_text("runtime model alias", &spec.runtime_model_alias)?;
        require_text("prompt revision", &spec.prompt_revision)?;
        Ok(Self {
            schema_version: AGENT_EPISODE_SCHEMA_V1,
            id: spec.id,
            task_id: spec.task_id,
            search_run_id: spec.search_run_id,
            parent_candidate_id: spec.parent_candidate_id,
            subtask_contract_digest: spec.subtask_contract_digest,
            context_projection_digest: spec.context_projection_digest,
            input_artifact_root_digest: spec.input_artifact_root_digest,
            runtime_model_alias: spec.runtime_model_alias,
            resolved_model_digest: spec.resolved_model_digest,
            prompt_revision: spec.prompt_revision,
            tool_catalog_digest: spec.tool_catalog_digest,
            loop_policy_digest: spec.loop_policy_digest,
            data_boundary_policy_digest: spec.data_boundary_policy_digest,
            budget_snapshot_digest: spec.budget_snapshot_digest,
            status: EpisodeStatus::Created,
        })
    }

    /// Applies one legal episode-state transition.
    ///
    /// # Errors
    ///
    /// Returns an error when `next` is not reachable from the current state.
    pub fn transition(&mut self, next: EpisodeStatus) -> Result<(), AgentRecordError> {
        if !self.status.can_transition_to(next) {
            return Err(invalid_transition("episode", self.status, next));
        }
        self.status = next;
        Ok(())
    }

    /// Reopens a finished Episode so its accumulated work is not thrown away.
    ///
    /// Deliberately not a `can_transition_to` edge. Terminal means terminal for the loop; this is
    /// an operator decision taken from outside it, and keeping it off the transition table means
    /// the reducer still cannot resurrect an Episode by itself.
    ///
    /// `BudgetExhausted` is refused. The budget an Episode ran under is bound into its identity
    /// through `loop_policy_digest`, so continuing past a spent budget means running under a
    /// different one — a fork of this Episode rather than this Episode, and a decision this method
    /// has no business making silently.
    ///
    /// # Errors
    ///
    /// Returns an error unless the Episode finished in a state that still has budget to spend.
    pub fn resume(&mut self) -> Result<(), AgentRecordError> {
        if !matches!(
            self.status,
            EpisodeStatus::Failed | EpisodeStatus::Incomplete
        ) {
            return Err(invalid_transition(
                "episode",
                self.status,
                EpisodeStatus::ReadyForModel,
            ));
        }
        self.status = EpisodeStatus::ReadyForModel;
        Ok(())
    }

    #[must_use]
    pub const fn id(&self) -> &EpisodeId {
        &self.id
    }

    #[must_use]
    pub const fn status(&self) -> EpisodeStatus {
        self.status
    }

    #[must_use]
    pub fn runtime_model_alias(&self) -> &str {
        &self.runtime_model_alias
    }

    #[must_use]
    pub const fn resolved_model_digest(&self) -> Sha256Digest {
        self.resolved_model_digest
    }

    pub(crate) fn matches_immutable(&self, expected: &Self) -> bool {
        self.schema_version == expected.schema_version
            && self.id == expected.id
            && self.task_id == expected.task_id
            && self.search_run_id == expected.search_run_id
            && self.parent_candidate_id == expected.parent_candidate_id
            && self.subtask_contract_digest == expected.subtask_contract_digest
            && self.context_projection_digest == expected.context_projection_digest
            && self.input_artifact_root_digest == expected.input_artifact_root_digest
            && self.runtime_model_alias == expected.runtime_model_alias
            && self.resolved_model_digest == expected.resolved_model_digest
            && self.prompt_revision == expected.prompt_revision
            && self.tool_catalog_digest == expected.tool_catalog_digest
            && self.loop_policy_digest == expected.loop_policy_digest
            && self.data_boundary_policy_digest == expected.data_boundary_policy_digest
            && self.budget_snapshot_digest == expected.budget_snapshot_digest
    }
}

/// Effect boundary used to authorize and recover one model-visible tool.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffectClass {
    ReadOnly,
    CandidateWrite,
    RemoteExecution,
    AuthorityRequest,
}

/// Maximum authority a tool result can carry into the semantic transcript.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultAuthority {
    Narrative,
    Reported,
    Observed,
    VerifiedReference,
}

/// Durable tool-operation lifecycle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOperationStatus {
    Requested,
    AwaitingPermission,
    Authorized,
    Dispatching,
    Running,
    Ambiguous,
    Reconciling,
    RejectedAsInvalid,
    Denied,
    Succeeded,
    CandidateFailed,
    InfraFailed,
    TimedOut,
    Cancelled,
}

impl ToolOperationStatus {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Requested => matches!(
                next,
                Self::AwaitingPermission | Self::Authorized | Self::RejectedAsInvalid
            ),
            Self::AwaitingPermission => matches!(next, Self::Authorized | Self::Denied),
            Self::Authorized => matches!(next, Self::Dispatching | Self::Cancelled),
            Self::Dispatching => matches!(
                next,
                Self::Running
                    | Self::Ambiguous
                    | Self::Succeeded
                    | Self::CandidateFailed
                    | Self::InfraFailed
                    | Self::TimedOut
                    | Self::Cancelled
            ),
            Self::Running => matches!(
                next,
                Self::Ambiguous
                    | Self::Succeeded
                    | Self::CandidateFailed
                    | Self::InfraFailed
                    | Self::TimedOut
                    | Self::Cancelled
            ),
            Self::Ambiguous => matches!(next, Self::Reconciling),
            Self::Reconciling => matches!(
                next,
                Self::Running
                    | Self::Succeeded
                    | Self::CandidateFailed
                    | Self::InfraFailed
                    | Self::TimedOut
                    | Self::Cancelled
            ),
            Self::RejectedAsInvalid
            | Self::Denied
            | Self::Succeeded
            | Self::CandidateFailed
            | Self::InfraFailed
            | Self::TimedOut
            | Self::Cancelled => false,
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RejectedAsInvalid
                | Self::Denied
                | Self::Succeeded
                | Self::CandidateFailed
                | Self::InfraFailed
                | Self::TimedOut
                | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolOperationSpec {
    pub id: ToolOperationId,
    pub episode_id: EpisodeId,
    pub turn_id: TurnId,
    pub native_call_id: String,
    pub tool_name: String,
    pub tool_version: String,
    pub effect_class: ToolEffectClass,
    pub result_authority: ToolResultAuthority,
    pub arguments_digest: Sha256Digest,
    pub input_identity_digest: Sha256Digest,
}

/// Persistable logical tool operation with restart-safe terminal evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOperationRecord {
    schema_version: u16,
    id: ToolOperationId,
    episode_id: EpisodeId,
    turn_id: TurnId,
    native_call_id: String,
    tool_name: String,
    tool_version: String,
    effect_class: ToolEffectClass,
    result_authority: ToolResultAuthority,
    arguments_digest: Sha256Digest,
    input_identity_digest: Sha256Digest,
    status: ToolOperationStatus,
    result_digest: Option<Sha256Digest>,
    receipt_digests: Vec<Sha256Digest>,
}

impl ToolOperationRecord {
    /// Creates a requested tool operation with immutable call identity.
    ///
    /// # Errors
    ///
    /// Returns an error when a required identity or tool label is empty.
    pub fn new(spec: ToolOperationSpec) -> Result<Self, AgentRecordError> {
        require_text("native tool call ID", &spec.native_call_id)?;
        require_text("tool name", &spec.tool_name)?;
        require_text("tool version", &spec.tool_version)?;
        Ok(Self {
            schema_version: TOOL_OPERATION_SCHEMA_V1,
            id: spec.id,
            episode_id: spec.episode_id,
            turn_id: spec.turn_id,
            native_call_id: spec.native_call_id,
            tool_name: spec.tool_name,
            tool_version: spec.tool_version,
            effect_class: spec.effect_class,
            result_authority: spec.result_authority,
            arguments_digest: spec.arguments_digest,
            input_identity_digest: spec.input_identity_digest,
            status: ToolOperationStatus::Requested,
            result_digest: None,
            receipt_digests: Vec::new(),
        })
    }

    /// Advances a non-terminal operation state. Terminal states require [`Self::finish`].
    ///
    /// # Errors
    ///
    /// Returns an error for a terminal target or an illegal transition.
    pub fn transition(&mut self, next: ToolOperationStatus) -> Result<(), AgentRecordError> {
        if next.is_terminal() {
            return Err(AgentRecordError::TerminalResultRequired);
        }
        if !self.status.can_transition_to(next) {
            return Err(invalid_transition("tool operation", self.status, next));
        }
        self.status = next;
        Ok(())
    }

    /// Commits a terminal model-visible result before the episode can continue.
    ///
    /// # Errors
    ///
    /// Returns an error unless `terminal` is legally reachable from the current state.
    pub fn finish(
        &mut self,
        terminal: ToolOperationStatus,
        result_digest: Sha256Digest,
        receipt_digests: Vec<Sha256Digest>,
    ) -> Result<(), AgentRecordError> {
        if !terminal.is_terminal() || !self.status.can_transition_to(terminal) {
            return Err(invalid_transition("tool operation", self.status, terminal));
        }
        self.status = terminal;
        self.result_digest = Some(result_digest);
        self.receipt_digests = receipt_digests;
        Ok(())
    }

    #[must_use]
    pub const fn status(&self) -> ToolOperationStatus {
        self.status
    }

    #[must_use]
    pub const fn id(&self) -> &ToolOperationId {
        &self.id
    }

    #[must_use]
    pub const fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    #[must_use]
    pub const fn result_authority(&self) -> ToolResultAuthority {
        self.result_authority
    }

    #[must_use]
    pub const fn result_digest(&self) -> Option<Sha256Digest> {
        self.result_digest
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchPhase {
    Drafting,
    Refining,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchStatus {
    Created,
    Running,
    Suspended,
    Completed,
    Exhausted,
    Cancelled,
    Failed,
}

impl SearchStatus {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Created => matches!(next, Self::Running | Self::Cancelled | Self::Failed),
            Self::Running => matches!(
                next,
                Self::Suspended
                    | Self::Completed
                    | Self::Exhausted
                    | Self::Cancelled
                    | Self::Failed
            ),
            Self::Suspended => matches!(next, Self::Running | Self::Cancelled | Self::Failed),
            Self::Completed | Self::Exhausted | Self::Cancelled | Self::Failed => false,
        }
    }
}

/// Immutable policy and budget identities captured when candidate search begins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchRunSpec {
    pub id: SearchRunId,
    pub task_id: TaskId,
    pub migration_spec_digest: Sha256Digest,
    pub selection_policy_digest: Sha256Digest,
    pub budget_snapshot_digest: Sha256Digest,
}

/// Persistable candidate frontier. Candidate scoring remains controller-owned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SearchRunRecord {
    schema_version: u16,
    id: SearchRunId,
    task_id: TaskId,
    migration_spec_digest: Sha256Digest,
    selection_policy_digest: Sha256Digest,
    budget_snapshot_digest: Sha256Digest,
    status: SearchStatus,
    phase: SearchPhase,
    candidate_ids: Vec<CandidateId>,
    incumbent_candidate_id: Option<CandidateId>,
}

impl SearchRunRecord {
    #[must_use]
    pub fn new(spec: SearchRunSpec) -> Self {
        Self {
            schema_version: SEARCH_RUN_SCHEMA_V1,
            id: spec.id,
            task_id: spec.task_id,
            migration_spec_digest: spec.migration_spec_digest,
            selection_policy_digest: spec.selection_policy_digest,
            budget_snapshot_digest: spec.budget_snapshot_digest,
            status: SearchStatus::Created,
            phase: SearchPhase::Drafting,
            candidate_ids: Vec::new(),
            incumbent_candidate_id: None,
        }
    }

    /// Applies one legal search-state transition.
    ///
    /// # Errors
    ///
    /// Returns an error when `next` is not reachable from the current state.
    pub fn transition(&mut self, next: SearchStatus) -> Result<(), AgentRecordError> {
        if !self.status.can_transition_to(next) {
            return Err(invalid_transition("search run", self.status, next));
        }
        self.status = next;
        Ok(())
    }

    /// Adds a unique candidate to a running search frontier.
    ///
    /// # Errors
    ///
    /// Returns an error when the search is not running or the candidate already exists.
    pub fn record_candidate(&mut self, candidate_id: CandidateId) -> Result<(), AgentRecordError> {
        if self.status != SearchStatus::Running {
            return Err(AgentRecordError::SearchNotRunning);
        }
        if self.candidate_ids.contains(&candidate_id) {
            return Err(AgentRecordError::DuplicateCandidate(candidate_id));
        }
        self.candidate_ids.push(candidate_id);
        Ok(())
    }

    /// Moves a drafting search into refinement around a recorded incumbent.
    ///
    /// # Errors
    ///
    /// Returns an error when the search is not drafting or the incumbent is unknown.
    pub fn begin_refining(&mut self, incumbent: CandidateId) -> Result<(), AgentRecordError> {
        if self.status != SearchStatus::Running || self.phase != SearchPhase::Drafting {
            return Err(AgentRecordError::SearchCannotRefine);
        }
        if !self.candidate_ids.contains(&incumbent) {
            return Err(AgentRecordError::UnknownIncumbent(incumbent));
        }
        self.phase = SearchPhase::Refining;
        self.incumbent_candidate_id = Some(incumbent);
        Ok(())
    }

    #[must_use]
    pub const fn phase(&self) -> SearchPhase {
        self.phase
    }

    #[must_use]
    pub const fn status(&self) -> SearchStatus {
        self.status
    }

    #[must_use]
    pub fn candidates(&self) -> &[CandidateId] {
        &self.candidate_ids
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentRecordError {
    EmptyField(&'static str),
    InvalidTransition {
        aggregate: &'static str,
        from: String,
        to: String,
    },
    TerminalResultRequired,
    SearchNotRunning,
    DuplicateCandidate(CandidateId),
    SearchCannotRefine,
    UnknownIncumbent(CandidateId),
}

impl Display for AgentRecordError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField(field) => write!(formatter, "{field} must not be empty"),
            Self::InvalidTransition {
                aggregate,
                from,
                to,
            } => write!(formatter, "invalid {aggregate} transition: {from} -> {to}"),
            Self::TerminalResultRequired => {
                write!(formatter, "terminal tool state requires a durable result")
            }
            Self::SearchNotRunning => write!(formatter, "candidate search is not running"),
            Self::DuplicateCandidate(candidate_id) => {
                write!(
                    formatter,
                    "candidate {candidate_id} is already in the frontier"
                )
            }
            Self::SearchCannotRefine => write!(formatter, "search cannot enter refining now"),
            Self::UnknownIncumbent(candidate_id) => {
                write!(formatter, "candidate {candidate_id} is not in the frontier")
            }
        }
    }
}

impl Error for AgentRecordError {}

fn require_text(field: &'static str, value: &str) -> Result<(), AgentRecordError> {
    if value.trim().is_empty() {
        Err(AgentRecordError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn invalid_transition(
    aggregate: &'static str,
    from: impl Debug,
    to: impl Debug,
) -> AgentRecordError {
    AgentRecordError::InvalidTransition {
        aggregate,
        from: format!("{from:?}"),
        to: format!("{to:?}"),
    }
}

#[cfg(test)]
#[path = "agent_tests.rs"]
mod agent_tests;
