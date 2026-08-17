//! What an Episode is allowed to do, split from how much it is allowed to spend.
//!
//! These were one struct, and both halves were bound into Episode identity through
//! `loop_policy_digest`. That made a spending cap into a semantic rule: an Episode that exhausted
//! its turns could not be continued, because continuing meant running under a policy its own
//! identity did not describe. The result was inverted — `Failed`, which is a defect, could be
//! resumed, while `BudgetExhausted`, which is the operator's own cap working exactly as intended,
//! could not.
//!
//! The test that separates them is whether changing the value changes what the *recorded* turns
//! mean. Raising `max_model_turns` from 20 to 30 changes nothing about turn 7: same model, same
//! prompt, same tools, same gates. Raising `max_tool_calls_per_turn` from 4 to 6 does change it —
//! a six-call turn was illegal before and legal after, so the shape of a valid turn moved.
//!
//! The design already had two identity slots for this, `loop_policy_digest` and
//! `budget_snapshot_digest`, and fed both from the same object.

use crate::AgentLoopRuntimeError;
use serde::{Deserialize, Serialize};

/// Constraints that define what a legal turn is. Part of Episode identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EpisodeRules {
    /// How wide one turn may be. A turn that exceeds it is not a legal turn.
    pub max_tool_calls_per_turn: u32,
    /// How much unresolved external effect the Episode may accumulate before stopping. A rule
    /// rather than an allowance: raising it changes what a finished Episode means, because it may
    /// now conclude with more effects nobody could confirm.
    pub max_ambiguous_model_attempts: u32,
    /// How many times the loop may push back on a model that stopped without satisfying its
    /// subtask. It shapes the conversation, so it belongs to meaning.
    pub max_stop_feedback_turns: u32,
}

/// How much the Episode may spend. Deliberately **not** part of Episode identity.
///
/// An allowance says nothing about what the work is, only how much of it the operator is willing
/// to pay for, so it may be granted again on an explicit resumption without invalidating a single
/// recorded turn.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EpisodeAllowance {
    pub max_model_turns: u32,
    pub max_model_attempts: u32,
    pub max_total_tool_operations: u32,
}

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
    /// The half that defines what a legal turn is.
    #[must_use]
    pub const fn rules(self) -> EpisodeRules {
        EpisodeRules {
            max_tool_calls_per_turn: self.max_tool_calls_per_turn,
            max_ambiguous_model_attempts: self.max_ambiguous_model_attempts,
            max_stop_feedback_turns: self.max_stop_feedback_turns,
        }
    }

    /// The half that says how much may be spent.
    #[must_use]
    pub const fn allowance(self) -> EpisodeAllowance {
        EpisodeAllowance {
            max_model_turns: self.max_model_turns,
            max_model_attempts: self.max_model_attempts,
            max_total_tool_operations: self.max_total_tool_operations,
        }
    }

    /// Replaces the spending half, leaving every rule exactly as recorded.
    #[must_use]
    pub const fn with_allowance(self, allowance: EpisodeAllowance) -> Self {
        Self {
            max_model_turns: allowance.max_model_turns,
            max_model_attempts: allowance.max_model_attempts,
            max_total_tool_operations: allowance.max_total_tool_operations,
            ..self
        }
    }

    /// Computes the identity of the Episode's **rules**.
    ///
    /// Named `digest` still, and still what `loop_policy_digest` carries, but it now covers only
    /// the half that changes meaning. An Episode resumed under a larger allowance keeps this
    /// digest, because nothing it vouches for has moved.
    ///
    /// # Errors
    ///
    /// Returns an error only if canonical JSON serialization fails.
    pub fn digest(self) -> Result<crate::Sha256Digest, serde_json::Error> {
        serde_json::to_vec(&self.rules()).map(|bytes| crate::Sha256Digest::digest_bytes(&bytes))
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

/// One operator decision to keep paying, and what it cost to say so.
///
/// Recorded rather than inferred. Before this existed a raised budget was unrepresentable, so the
/// only way to continue was to abandon the Episode and start another — five separate records
/// instead of one audit trail, which is less honest and more expensive than the thing it refused.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AllowanceGrant {
    /// The terminal status the Episode was reopened from.
    pub resumed_from: crate::EpisodeStatus,
    /// What it had been allowed to spend up to that point.
    pub previous: EpisodeAllowance,
    /// What it is allowed to spend now.
    pub granted: EpisodeAllowance,
}
