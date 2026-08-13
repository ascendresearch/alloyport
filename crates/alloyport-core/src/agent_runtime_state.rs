use super::{AgentLoopRuntimeSpec, DURABLE_EPISODE_STATE_SCHEMA_V2, DurableEpisodeState};
use crate::{AgentEpisodeRecord, AgentLoopRuntimeError, EpisodeId, ToolOperationStatus};

impl DurableEpisodeState {
    /// Creates a state snapshot before the first durable transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the loop policy is invalid.
    pub fn new(spec: AgentLoopRuntimeSpec) -> Result<Self, AgentLoopRuntimeError> {
        spec.policy.validate()?;
        Ok(Self {
            schema_version: DURABLE_EPISODE_STATE_SCHEMA_V2,
            episode: spec.episode,
            policy: spec.policy,
            initial_input_digest: spec.initial_input_digest,
            next_input_digest: spec.initial_input_digest,
            resolved_model_digest: spec.resolved_model_digest,
            deployment_digest: spec.deployment_digest,
            model_profile_digest: spec.model_profile_digest,
            request_budget_digest: spec.request_budget_digest,
            attempts: Vec::new(),
            turns: Vec::new(),
            tool_operations: Vec::new(),
            ambiguous_model_attempts: 0,
            stop_feedback_turns: 0,
            subtask_satisfied: false,
            cancellation_requested: false,
        })
    }

    #[must_use]
    pub const fn episode(&self) -> &AgentEpisodeRecord {
        &self.episode
    }

    /// Revalidates the persisted envelope before a reducer resumes it.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported schema, invalid policy, impossible record counts, or
    /// an invalid recovered semantic turn.
    pub fn validate_recovered(&self) -> Result<(), AgentLoopRuntimeError> {
        if self.schema_version != DURABLE_EPISODE_STATE_SCHEMA_V2 {
            return Err(AgentLoopRuntimeError::InvalidDurableState(
                "unsupported durable episode schema",
            ));
        }
        self.policy.validate()?;
        if self.turns.len() > self.attempts.len() {
            return Err(AgentLoopRuntimeError::InvalidDurableState(
                "episode has more turns than model attempts",
            ));
        }
        for turn in &self.turns {
            turn.semantic_turn.validate()?;
        }
        Ok(())
    }

    #[must_use]
    pub fn matches_runtime_spec(&self, spec: &AgentLoopRuntimeSpec) -> bool {
        self.episode.matches_immutable(&spec.episode)
            && self.policy == spec.policy
            && self.initial_input_digest == spec.initial_input_digest
            && self.resolved_model_digest == spec.resolved_model_digest
            && self.deployment_digest == spec.deployment_digest
            && self.model_profile_digest == spec.model_profile_digest
            && self.request_budget_digest == spec.request_budget_digest
    }

    #[must_use]
    pub fn model_attempt_count(&self) -> usize {
        self.attempts.len()
    }

    #[must_use]
    pub fn turn_count(&self) -> usize {
        self.turns.len()
    }

    #[must_use]
    pub fn tool_operation_count(&self) -> usize {
        self.tool_operations.len()
    }

    #[must_use]
    pub fn ambiguous_model_attempt_count(&self) -> u32 {
        self.ambiguous_model_attempts
    }

    #[must_use]
    pub fn tool_statuses(&self) -> Vec<ToolOperationStatus> {
        self.tool_operations
            .iter()
            .map(|operation| operation.record.status())
            .collect()
    }

    pub(crate) const fn episode_id(&self) -> &EpisodeId {
        self.episode.id()
    }
}
