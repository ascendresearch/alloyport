//! Durable, provider-neutral Agent Episode reducer and deterministic fake adapters.

use crate::agent_runtime_helpers::{
    AgentLoopRuntimeError, derived_model_attempt_id, progress, usize_from_u32,
};
use crate::agent_runtime_policy::{AllowanceGrant, EpisodeAllowance};
use crate::agent_runtime_support::{
    AgentLoopAdvance, AgentRuntimeFaultInjector, AgentToolGateway, EpisodeRepository,
    VersionedEpisodeState,
};
use crate::{
    AgentEpisodeRecord, AgentLoopPolicy, EpisodeId, EpisodeStatus, GatewayToolCall,
    ModelAttemptRecord, ModelAttemptSpec, ModelAttemptStatus, ModelGateway, Sha256Digest,
    ToolOperationRecord, ToolOperationStatus, TurnRecord,
};
use serde::{Deserialize, Serialize};

pub const DURABLE_EPISODE_STATE_SCHEMA_V2: u16 = 2;

/// Immutable runtime values not interpreted by a provider adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentLoopRuntimeSpec {
    pub episode: AgentEpisodeRecord,
    pub policy: AgentLoopPolicy,
    pub initial_input_digest: Sha256Digest,
    pub resolved_model_digest: Sha256Digest,
    pub deployment_digest: Sha256Digest,
    pub model_profile_digest: Sha256Digest,
    pub request_budget_digest: Sha256Digest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableTurn {
    record: TurnRecord,
    semantic_turn: crate::GatewayTurn,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableToolOperation {
    record: ToolOperationRecord,
    call: GatewayToolCall,
}

/// Complete authoritative state saved atomically after each reducer boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableEpisodeState {
    schema_version: u16,
    episode: AgentEpisodeRecord,
    policy: AgentLoopPolicy,
    initial_input_digest: Sha256Digest,
    next_input_digest: Sha256Digest,
    resolved_model_digest: Sha256Digest,
    deployment_digest: Sha256Digest,
    model_profile_digest: Sha256Digest,
    request_budget_digest: Sha256Digest,
    attempts: Vec<ModelAttemptRecord>,
    turns: Vec<DurableTurn>,
    tool_operations: Vec<DurableToolOperation>,
    ambiguous_model_attempts: u32,
    stop_feedback_turns: u32,
    subtask_satisfied: bool,
    cancellation_requested: bool,
    /// Every operator decision to keep paying, in order.
    ///
    /// Defaulted so states written before resumption existed still load. A run whose turns span
    /// two grants should say so, and say what each grant was.
    #[serde(default)]
    grants: Vec<AllowanceGrant>,
}

#[path = "agent_runtime_model.rs"]
mod model_turn;
#[path = "agent_runtime_state.rs"]
mod state;
#[path = "agent_runtime_tools.rs"]
mod tool_turn;

/// Stateless runner. Reconstructing it from the same repository exercises restart semantics.
#[derive(Clone, Debug)]
pub struct AgentLoopRunner {
    episode_id: EpisodeId,
}

impl AgentLoopRunner {
    #[must_use]
    pub const fn new(episode_id: EpisodeId) -> Self {
        Self { episode_id }
    }

    /// Advances at most one externally visible model/tool action and persists every boundary.
    ///
    /// # Errors
    ///
    /// Returns an error for persistence conflicts, invalid durable state, adapter failure, or an
    /// injected crash. Dispatching state remains durable on uncertain external failure.
    pub async fn advance<R, M, T, F>(
        &self,
        repository: &mut R,
        models: &mut M,
        tools: &mut T,
        faults: &mut F,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError>
    where
        R: EpisodeRepository,
        M: ModelGateway,
        T: AgentToolGateway,
        F: AgentRuntimeFaultInjector,
    {
        let versioned = repository.load(&self.episode_id)?;
        let status = versioned.state.episode.status();
        if status.is_terminal() {
            return Ok(AgentLoopAdvance::Terminal(status));
        }
        match status {
            EpisodeStatus::Created => {
                Self::transition_only(repository, versioned, EpisodeStatus::ReadyForModel)
            }
            EpisodeStatus::ReadyForModel => self.prepare_attempt(repository, versioned),
            EpisodeStatus::ModelAttemptPending => {
                self.drive_model(repository, versioned, models, faults)
                    .await
            }
            EpisodeStatus::TurnRecorded => {
                self.plan_turn_tools(repository, versioned, tools, faults)
            }
            EpisodeStatus::ToolWorkPending => {
                Self::drive_tools(repository, versioned, tools, faults).await
            }
            EpisodeStatus::StopReview => Self::review_stop(repository, versioned),
            EpisodeStatus::CancellationPending => {
                Self::transition_only(repository, versioned, EpisodeStatus::Cancelled)
            }
            EpisodeStatus::Suspended | EpisodeStatus::SuspensionRequested => {
                Ok(AgentLoopAdvance::Suspended)
            }
            EpisodeStatus::Succeeded
            | EpisodeStatus::Incomplete
            | EpisodeStatus::Cancelled
            | EpisodeStatus::BudgetExhausted
            | EpisodeStatus::Failed => unreachable!("terminal states returned above"),
        }
    }

    /// Reopens a finished Episode so its accumulated turns and tool results are not thrown away.
    ///
    /// Every retry before this started from nothing: four consecutive runs each spent their first
    /// four to eight turns re-reading the same reference documents before doing any work, because a
    /// new task minted a new Episode. Recovery already existed — the repository loads and
    /// revalidates an existing Episode — and only the terminal status stood in the way.
    ///
    /// Returns the status it resumed from, or the current status when the Episode is still running.
    ///
    /// # Errors
    ///
    /// Returns an error when the Episode finished in a state that cannot be continued, or for
    /// persistence failure.
    pub fn resume<R: EpisodeRepository>(
        &self,
        repository: &mut R,
        granted: EpisodeAllowance,
    ) -> Result<EpisodeStatus, AgentLoopRuntimeError> {
        let mut versioned = repository.load(&self.episode_id)?;
        versioned.state.validate_recovered()?;
        let resumed_from = versioned.state.episode.status();
        if !resumed_from.is_terminal() {
            return Ok(resumed_from);
        }
        versioned.state.episode.resume()?;
        let previous = versioned.state.policy.allowance();
        versioned.state.policy = versioned.state.policy.with_allowance(granted);
        versioned.state.policy.validate()?;
        versioned.state.grants.push(AllowanceGrant {
            resumed_from,
            previous,
            granted,
        });
        // A cancellation request belongs to the run that ended; carrying it forward would cancel
        // the resumption before it took a turn.
        versioned.state.cancellation_requested = false;
        repository.save(versioned.revision, versioned.state)?;
        Ok(resumed_from)
    }

    /// Durably requests cancellation without erasing an in-flight ambiguous effect.
    ///
    /// # Errors
    ///
    /// Returns an error for persistence or state-transition failure.
    pub fn request_cancellation<R: EpisodeRepository>(
        &self,
        repository: &mut R,
    ) -> Result<EpisodeStatus, AgentLoopRuntimeError> {
        let mut versioned = repository.load(&self.episode_id)?;
        if versioned.state.episode.status().is_terminal() {
            return Ok(versioned.state.episode.status());
        }
        versioned.state.cancellation_requested = true;
        let model_ambiguous = versioned
            .state
            .attempts
            .last_mut()
            .is_some_and(|attempt| attempt.status() == ModelAttemptStatus::Dispatching);
        let tool_ambiguous = versioned.state.tool_operations.iter_mut().any(|operation| {
            matches!(
                operation.record.status(),
                ToolOperationStatus::Dispatching
                    | ToolOperationStatus::Running
                    | ToolOperationStatus::Ambiguous
                    | ToolOperationStatus::Reconciling
            )
        });
        if model_ambiguous && let Some(attempt) = versioned.state.attempts.last_mut() {
            // No provider diagnostic exists for an attempt cancelled mid-dispatch. Hashing a
            // label produced a digest for an artifact nobody had stored; the status carries the
            // meaning, and `None` says plainly that there is nothing to read.
            attempt.finish_without_response(ModelAttemptStatus::CancelledAmbiguous, None)?;
        }
        if tool_ambiguous
            && let Some(operation) = versioned
                .state
                .tool_operations
                .iter_mut()
                .find(|operation| operation.record.status() == ToolOperationStatus::Dispatching)
        {
            operation
                .record
                .transition(ToolOperationStatus::Ambiguous)?;
        }
        if model_ambiguous || tool_ambiguous {
            if versioned.state.episode.status() != EpisodeStatus::Suspended {
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::SuspensionRequested)?;
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::Suspended)?;
            }
        } else if versioned.state.episode.status() != EpisodeStatus::CancellationPending {
            versioned
                .state
                .episode
                .transition(EpisodeStatus::CancellationPending)?;
        }
        let status = versioned.state.episode.status();
        repository.save(versioned.revision, versioned.state)?;
        Ok(status)
    }

    /// Resumes an explicitly suspended episode so stable tool operations can reconcile.
    ///
    /// # Errors
    ///
    /// Returns an error unless the episode is suspended or persistence fails.
    pub fn resume_reconciliation<R: EpisodeRepository>(
        &self,
        repository: &mut R,
    ) -> Result<EpisodeStatus, AgentLoopRuntimeError> {
        let mut versioned = repository.load(&self.episode_id)?;
        if versioned.state.episode.status() != EpisodeStatus::Suspended {
            return Err(AgentLoopRuntimeError::InvalidDurableState(
                "only a suspended episode can resume reconciliation",
            ));
        }
        let has_unresolved_tool = versioned
            .state
            .tool_operations
            .iter()
            .any(|operation| !operation.record.status().is_terminal());
        let next = if has_unresolved_tool {
            EpisodeStatus::ToolWorkPending
        } else if versioned.state.cancellation_requested {
            EpisodeStatus::CancellationPending
        } else {
            EpisodeStatus::ReadyForModel
        };
        versioned.state.episode.transition(next)?;
        repository.save(versioned.revision, versioned.state)?;
        Ok(next)
    }

    fn transition_only<R: EpisodeRepository>(
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
        next: EpisodeStatus,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError> {
        versioned.state.episode.transition(next)?;
        repository.save(versioned.revision, versioned.state)?;
        Ok(progress(next))
    }

    fn prepare_attempt<R: EpisodeRepository>(
        &self,
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError> {
        if versioned.state.cancellation_requested {
            return Self::transition_only(
                repository,
                versioned,
                EpisodeStatus::CancellationPending,
            );
        }
        if versioned.state.turns.len() >= usize_from_u32(versioned.state.policy.max_model_turns)
            || versioned.state.attempts.len()
                >= usize_from_u32(versioned.state.policy.max_model_attempts)
        {
            return Self::transition_only(repository, versioned, EpisodeStatus::BudgetExhausted);
        }
        let attempt_number = u32::try_from(versioned.state.attempts.len())
            .map_err(|_| AgentLoopRuntimeError::CounterExhausted)?
            .checked_add(1)
            .ok_or(AgentLoopRuntimeError::CounterExhausted)?;
        let predecessor_attempt_id = versioned
            .state
            .attempts
            .last()
            .map(|attempt| attempt.id().clone());
        let predecessor_continuation_digest = versioned
            .state
            .turns
            .last()
            .map(|turn| turn.record.native_continuation_digest());
        let attempt = ModelAttemptRecord::new(ModelAttemptSpec {
            id: derived_model_attempt_id(&self.episode_id, attempt_number)?,
            episode_id: self.episode_id.clone(),
            attempt_number,
            request_digest: versioned.state.next_input_digest,
            resolved_model_digest: versioned.state.resolved_model_digest,
            deployment_digest: versioned.state.deployment_digest,
            model_profile_digest: versioned.state.model_profile_digest,
            request_budget_digest: versioned.state.request_budget_digest,
            predecessor_attempt_id,
            predecessor_continuation_digest,
        })?;
        versioned.state.attempts.push(attempt);
        versioned
            .state
            .episode
            .transition(EpisodeStatus::ModelAttemptPending)?;
        repository.save(versioned.revision, versioned.state)?;
        Ok(AgentLoopAdvance::Progressed(
            EpisodeStatus::ModelAttemptPending,
        ))
    }

    fn review_stop<R: EpisodeRepository>(
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError> {
        let next = if versioned.state.subtask_satisfied {
            EpisodeStatus::Succeeded
        } else if versioned.state.stop_feedback_turns
            < versioned.state.policy.max_stop_feedback_turns
        {
            versioned.state.stop_feedback_turns += 1;
            EpisodeStatus::ReadyForModel
        } else {
            EpisodeStatus::Incomplete
        };
        versioned.state.episode.transition(next)?;
        repository.save(versioned.revision, versioned.state)?;
        Ok(progress(next))
    }
}
#[cfg(test)]
#[path = "agent_runtime_tests.rs"]
mod agent_runtime_tests;
