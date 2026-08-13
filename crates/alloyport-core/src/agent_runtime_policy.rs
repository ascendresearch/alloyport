//! Bounded policy captured by one agent episode.

use crate::AgentLoopRuntimeError;
use serde::{Deserialize, Serialize};

/// Provider retry policy remains explicit and separate from these loop budgets.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLoopPolicy {
    pub max_model_turns: u32,
    pub max_model_attempts: u32,
    pub max_ambiguous_model_attempts: u32,
    pub max_tool_calls_per_turn: u32,
    pub max_total_tool_operations: u32,
    pub max_stop_feedback_turns: u32,
}

impl AgentLoopPolicy {
    /// Computes the identity of the exact Episode budget policy.
    ///
    /// # Errors
    ///
    /// Returns an error only if canonical JSON serialization fails.
    pub fn digest(self) -> Result<crate::Sha256Digest, serde_json::Error> {
        serde_json::to_vec(&self).map(|bytes| crate::Sha256Digest::digest_bytes(&bytes))
    }

    /// Validates positive hard limits and their ordering.
    ///
    /// # Errors
    ///
    /// Returns an error for zero hard limits or fewer attempts than semantic turns.
    pub fn validate(self) -> Result<(), AgentLoopRuntimeError> {
        if self.max_model_turns == 0
            || self.max_model_attempts == 0
            || self.max_tool_calls_per_turn == 0
            || self.max_total_tool_operations == 0
            || self.max_model_attempts < self.max_model_turns
        {
            return Err(AgentLoopRuntimeError::InvalidPolicy);
        }
        Ok(())
    }
}
