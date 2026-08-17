//! Durable, provider-neutral Agent Episode reducer and deterministic fake adapters.

use crate::ModelTransportRetryHint;
use crate::agent_runtime_helpers::{
    AgentLoopRuntimeError, crash_if, derive_model_continuation_input_digest,
    derived_model_attempt_id, derived_tool_operation_id, derived_turn_id, digest_label,
    digest_semantic_turn, progress, usize_from_u32,
};
use crate::agent_runtime_policy::{AllowanceGrant, EpisodeAllowance};
use crate::agent_runtime_support::{
    AgentLoopAdvance, AgentRuntimeFaultInjector, AgentRuntimeFaultPoint, AgentToolGateway,
    EpisodeRepository, RuntimeToolDescriptor, ToolGatewayOutcome, ToolInvocation,
    VersionedEpisodeState,
};
use crate::{
    AgentEpisodeRecord, AgentLoopPolicy, EpisodeId, EpisodeStatus, GatewayToolCall,
    ModelAttemptRecord, ModelAttemptSpec, ModelAttemptStatus, ModelGateway, ModelGatewayOutcome,
    ModelTurnRequest, Sha256Digest, ToolEffectClass, ToolOperationRecord, ToolOperationSpec,
    ToolOperationStatus, ToolResultAuthority, TurnRecord, TurnSpec,
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

#[path = "agent_runtime_state.rs"]
mod state;

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

    #[allow(clippy::too_many_lines)]
    async fn drive_model<R, M, F>(
        &self,
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
        models: &mut M,
        faults: &mut F,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError>
    where
        R: EpisodeRepository,
        M: ModelGateway,
        F: AgentRuntimeFaultInjector,
    {
        let status = versioned
            .state
            .attempts
            .last()
            .ok_or(AgentLoopRuntimeError::MissingModelAttempt)?
            .status();
        if status == ModelAttemptStatus::Dispatching {
            let attempt = versioned.state.attempts.last_mut().expect("checked above");
            let terminal = if versioned.state.cancellation_requested {
                ModelAttemptStatus::CancelledAmbiguous
            } else {
                ModelAttemptStatus::Ambiguous
            };
            // Likewise: recovery found a dispatch whose outcome is unknown, and there is no
            // diagnostic to name.
            attempt.finish_without_response(terminal, None)?;
            versioned.state.ambiguous_model_attempts = versioned
                .state
                .ambiguous_model_attempts
                .checked_add(1)
                .ok_or(AgentLoopRuntimeError::CounterExhausted)?;
            if versioned.state.cancellation_requested {
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::SuspensionRequested)?;
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::Suspended)?;
                repository.save(versioned.revision, versioned.state)?;
                return Ok(AgentLoopAdvance::Suspended);
            }
            let next = if versioned.state.ambiguous_model_attempts
                > versioned.state.policy.max_ambiguous_model_attempts
            {
                EpisodeStatus::BudgetExhausted
            } else {
                EpisodeStatus::ReadyForModel
            };
            versioned.state.episode.transition(next)?;
            repository.save(versioned.revision, versioned.state)?;
            return Ok(progress(next));
        }
        if status != ModelAttemptStatus::Prepared {
            return Err(AgentLoopRuntimeError::InvalidDurableState(
                "pending episode has no prepared/dispatching attempt",
            ));
        }

        let turn_index = u32::try_from(versioned.state.turns.len())
            .map_err(|_| AgentLoopRuntimeError::CounterExhausted)?
            .checked_add(1)
            .ok_or(AgentLoopRuntimeError::CounterExhausted)?;
        let request = {
            let attempt = versioned.state.attempts.last_mut().expect("checked above");
            attempt.mark_dispatching()?;
            ModelTurnRequest {
                attempt_id: attempt.id().clone(),
                episode_id: self.episode_id.clone(),
                turn_index,
                input_digest: versioned.state.next_input_digest,
            }
        };
        versioned.revision = repository.save(versioned.revision, versioned.state.clone())?;
        crash_if(faults, AgentRuntimeFaultPoint::AfterModelDispatchCommit)?;
        let outcome = models.invoke(&request).await?;
        crash_if(
            faults,
            AgentRuntimeFaultPoint::AfterModelOutcomeBeforeCommit,
        )?;

        match outcome {
            ModelGatewayOutcome::Turn(exchange) => {
                let decoded_digest = digest_semantic_turn(&exchange.turn);
                let attempt = versioned.state.attempts.last_mut().expect("checked above");
                attempt.record_response(exchange.raw_exchange_digest, None, exchange.turn.usage)?;
                attempt.mark_decoded(exchange.native_continuation_digest)?;
                let turn_id = derived_turn_id(&self.episode_id, turn_index)?;
                let record = TurnRecord::new(TurnSpec {
                    id: turn_id,
                    episode_id: self.episode_id.clone(),
                    model_attempt_id: request.attempt_id,
                    turn_index,
                    decoded_turn_digest: decoded_digest,
                    raw_exchange_digest: exchange.raw_exchange_digest,
                    native_continuation_digest: exchange.native_continuation_digest,
                    stop_reason: exchange.turn.stop_reason,
                    tool_call_count: u32::try_from(exchange.turn.tool_calls.len())
                        .map_err(|_| AgentLoopRuntimeError::CounterExhausted)?,
                    usage: exchange.turn.usage,
                })?;
                versioned.state.turns.push(DurableTurn {
                    record,
                    semantic_turn: exchange.turn,
                });
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::TurnRecorded)?;
                repository.save(versioned.revision, versioned.state)?;
                crash_if(faults, AgentRuntimeFaultPoint::AfterTurnCommit)?;
                Ok(AgentLoopAdvance::Progressed(EpisodeStatus::TurnRecorded))
            }
            ModelGatewayOutcome::ConfirmedNotSent {
                diagnostic_digest,
                retry,
                ..
            } => {
                let repeated = repeats_the_last_failure(&versioned.state, diagnostic_digest);
                let attempt = versioned.state.attempts.last_mut().expect("checked above");
                // The digest arrives already published. Hashing the string here named an artifact
                // nobody had stored, so a run that died on 21 identical dispatch failures threw
                // away its own explanation while recording it.
                attempt.finish_without_response(
                    ModelAttemptStatus::ConfirmedNotSent,
                    diagnostic_digest,
                )?;
                Self::after_failed_dispatch(repository, versioned, retry, repeated)
            }
            ModelGatewayOutcome::Rejected {
                response_digest,
                diagnostic_digest,
                retry,
                ..
            } => {
                let repeated = repeats_the_last_failure(&versioned.state, diagnostic_digest);
                let attempt = versioned.state.attempts.last_mut().expect("checked above");
                attempt.record_response(response_digest, None, None)?;
                attempt.mark_response_failed(diagnostic_digest)?;
                Self::after_failed_dispatch(repository, versioned, retry, repeated)
            }
            ModelGatewayOutcome::DecodeFailed {
                response_digest,
                diagnostic_digest,
                ..
            } => {
                let attempt = versioned.state.attempts.last_mut().expect("checked above");
                attempt.record_response(response_digest, None, None)?;
                attempt.mark_decode_failed(diagnostic_digest)?;
                versioned.state.episode.transition(EpisodeStatus::Failed)?;
                repository.save(versioned.revision, versioned.state)?;
                Ok(AgentLoopAdvance::Terminal(EpisodeStatus::Failed))
            }
            ModelGatewayOutcome::Ambiguous {
                diagnostic_digest, ..
            } => {
                let attempt = versioned.state.attempts.last_mut().expect("checked above");
                attempt
                    .finish_without_response(ModelAttemptStatus::Ambiguous, diagnostic_digest)?;
                versioned.state.ambiguous_model_attempts += 1;
                let next = if versioned.state.ambiguous_model_attempts
                    > versioned.state.policy.max_ambiguous_model_attempts
                {
                    EpisodeStatus::BudgetExhausted
                } else {
                    EpisodeStatus::ReadyForModel
                };
                versioned.state.episode.transition(next)?;
                repository.save(versioned.revision, versioned.state)?;
                Ok(progress(next))
            }
        }
    }

    fn plan_turn_tools<R, T, F>(
        &self,
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
        tools: &T,
        faults: &mut F,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError>
    where
        R: EpisodeRepository,
        T: AgentToolGateway,
        F: AgentRuntimeFaultInjector,
    {
        let turn = versioned
            .state
            .turns
            .last()
            .ok_or(AgentLoopRuntimeError::MissingTurn)?
            .clone();
        if turn.semantic_turn.tool_calls.is_empty() {
            versioned
                .state
                .episode
                .transition(EpisodeStatus::StopReview)?;
            repository.save(versioned.revision, versioned.state)?;
            return Ok(AgentLoopAdvance::Progressed(EpisodeStatus::StopReview));
        }
        let call_count = u32::try_from(turn.semantic_turn.tool_calls.len())
            .map_err(|_| AgentLoopRuntimeError::CounterExhausted)?;
        let total = versioned
            .state
            .tool_operations
            .len()
            .checked_add(turn.semantic_turn.tool_calls.len())
            .ok_or(AgentLoopRuntimeError::CounterExhausted)?;
        // Two different things were one branch, and calling both of them budget exhaustion put a
        // false verdict in the record: a live Episode ended here holding 52 of its 60 operations,
        // having simply asked for six tools in a turn that allows four.
        //
        // Spending the operation budget is terminal — there is nothing left to spend.
        if total > usize_from_u32(versioned.state.policy.max_total_tool_operations) {
            versioned
                .state
                .episode
                .transition(EpisodeStatus::BudgetExhausted)?;
            repository.save(versioned.revision, versioned.state)?;
            return Ok(AgentLoopAdvance::Terminal(EpisodeStatus::BudgetExhausted));
        }
        // A turn that is merely too wide is a turn, not an exhausted budget. It goes to stop
        // review, which already owns "that turn was not usable, give the model another within a
        // bounded feedback allowance and finish `Incomplete` when the allowance is gone".
        if call_count > versioned.state.policy.max_tool_calls_per_turn {
            versioned
                .state
                .episode
                .transition(EpisodeStatus::StopReview)?;
            repository.save(versioned.revision, versioned.state)?;
            return Ok(AgentLoopAdvance::Progressed(EpisodeStatus::StopReview));
        }
        for (index, call) in turn.semantic_turn.tool_calls.into_iter().enumerate() {
            let descriptor = tools
                .descriptor(&call.name)
                .unwrap_or(RuntimeToolDescriptor {
                    name: call.name.clone(),
                    version: "unknown".to_owned(),
                    effect_class: ToolEffectClass::ReadOnly,
                    result_authority: ToolResultAuthority::Narrative,
                });
            let arguments_digest = Sha256Digest::digest_bytes(&call.raw_arguments);
            let operation_id = derived_tool_operation_id(
                &self.episode_id,
                turn.record.id(),
                index,
                &descriptor,
                arguments_digest,
                versioned.state.next_input_digest,
            )?;
            let mut record = ToolOperationRecord::new(ToolOperationSpec {
                id: operation_id,
                episode_id: self.episode_id.clone(),
                turn_id: turn.record.id().clone(),
                native_call_id: call.native_call_id.clone(),
                tool_name: call.name.clone(),
                tool_version: descriptor.version,
                effect_class: descriptor.effect_class,
                result_authority: descriptor.result_authority,
                arguments_digest,
                input_identity_digest: versioned.state.next_input_digest,
            })?;
            if let Err(rejection) = tools.validate_call(&call) {
                // A defect in the model's own arguments is recoverable: the operation is terminal,
                // its published explanation becomes the next model input, and the episode continues.
                record.finish(
                    ToolOperationStatus::RejectedAsInvalid,
                    rejection.result_digest,
                    Vec::new(),
                )?;
            } else if tools.descriptor(&call.name).is_none() {
                // Defense in depth only: every protocol codec already fails closed on a tool name
                // that was not declared, and a real gateway rejects an unknown name through
                // `validate_call` with a readable artifact. This label names no artifact, so a
                // gateway that reaches it has not published the explanation the model needs.
                record.finish(
                    ToolOperationStatus::RejectedAsInvalid,
                    digest_label("unknown-tool"),
                    Vec::new(),
                )?;
            }
            versioned
                .state
                .tool_operations
                .push(DurableToolOperation { record, call });
        }
        versioned
            .state
            .episode
            .transition(EpisodeStatus::ToolWorkPending)?;
        repository.save(versioned.revision, versioned.state)?;
        crash_if(faults, AgentRuntimeFaultPoint::AfterTurnCommit)?;
        Ok(AgentLoopAdvance::Progressed(EpisodeStatus::ToolWorkPending))
    }

    #[allow(clippy::too_many_lines)]
    async fn drive_tools<R, T, F>(
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
        tools: &mut T,
        faults: &mut F,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError>
    where
        R: EpisodeRepository,
        T: AgentToolGateway,
        F: AgentRuntimeFaultInjector,
    {
        let pending_index = versioned
            .state
            .tool_operations
            .iter()
            .position(|operation| !operation.record.status().is_terminal());
        let Some(index) = pending_index else {
            let continuation = versioned
                .state
                .turns
                .last()
                .ok_or(AgentLoopRuntimeError::MissingTurn)?
                .record
                .native_continuation_digest();
            let turn_id = versioned
                .state
                .turns
                .last()
                .ok_or(AgentLoopRuntimeError::MissingTurn)?
                .record
                .id()
                .clone();
            let results = versioned
                .state
                .tool_operations
                .iter()
                .filter(|operation| operation.record.turn_id() == &turn_id)
                .filter_map(|operation| operation.record.result_digest())
                .collect::<Vec<_>>();
            versioned.state.next_input_digest =
                derive_model_continuation_input_digest(continuation, results.iter().copied());
            versioned
                .state
                .episode
                .transition(EpisodeStatus::ReadyForModel)?;
            repository.save(versioned.revision, versioned.state)?;
            return Ok(AgentLoopAdvance::Progressed(EpisodeStatus::ReadyForModel));
        };

        let status = versioned.state.tool_operations[index].record.status();
        if status == ToolOperationStatus::Requested {
            versioned.state.tool_operations[index]
                .record
                .transition(ToolOperationStatus::Authorized)?;
            versioned.state.tool_operations[index]
                .record
                .transition(ToolOperationStatus::Dispatching)?;
        } else if status == ToolOperationStatus::Dispatching {
            versioned.state.tool_operations[index]
                .record
                .transition(ToolOperationStatus::Ambiguous)?;
            versioned.state.tool_operations[index]
                .record
                .transition(ToolOperationStatus::Reconciling)?;
        } else if status == ToolOperationStatus::Ambiguous {
            versioned.state.tool_operations[index]
                .record
                .transition(ToolOperationStatus::Reconciling)?;
        } else if !matches!(
            status,
            ToolOperationStatus::Reconciling | ToolOperationStatus::Running
        ) {
            return Err(AgentLoopRuntimeError::InvalidDurableState(
                "tool work contains an unsupported nonterminal state",
            ));
        }
        let dispatch_status = versioned.state.tool_operations[index].record.status();
        let reconciling = matches!(
            dispatch_status,
            ToolOperationStatus::Reconciling | ToolOperationStatus::Running
        );
        let invocation = ToolInvocation {
            operation_id: versioned.state.tool_operations[index].record.id().clone(),
            call: versioned.state.tool_operations[index].call.clone(),
            input_identity_digest: versioned.state.next_input_digest,
        };
        versioned.revision = repository.save(versioned.revision, versioned.state.clone())?;
        crash_if(faults, AgentRuntimeFaultPoint::AfterToolDispatchCommit)?;
        let outcome = if reconciling {
            tools.reconcile(&invocation).await?
        } else {
            tools.execute(&invocation).await?
        };
        crash_if(faults, AgentRuntimeFaultPoint::AfterToolOutcomeBeforeCommit)?;
        match outcome {
            ToolGatewayOutcome::Completed {
                status,
                result_digest,
                receipt_digests,
                satisfies_subtask,
            } => {
                if !status.is_terminal() || status == ToolOperationStatus::RejectedAsInvalid {
                    return Err(AgentLoopRuntimeError::InvalidToolOutcome);
                }
                let authority = versioned.state.tool_operations[index]
                    .record
                    .result_authority();
                if satisfies_subtask && authority != ToolResultAuthority::VerifiedReference {
                    return Err(AgentLoopRuntimeError::UntrustedCompletion);
                }
                versioned.state.tool_operations[index].record.finish(
                    status,
                    result_digest,
                    receipt_digests,
                )?;
                versioned.state.subtask_satisfied |= satisfies_subtask;
                repository.save(versioned.revision, versioned.state)?;
                crash_if(faults, AgentRuntimeFaultPoint::AfterToolResultCommit)?;
                Ok(AgentLoopAdvance::Progressed(EpisodeStatus::ToolWorkPending))
            }
            ToolGatewayOutcome::Pending { .. } => {
                if dispatch_status != ToolOperationStatus::Running {
                    versioned.state.tool_operations[index]
                        .record
                        .transition(ToolOperationStatus::Running)?;
                }
                repository.save(versioned.revision, versioned.state)?;
                Ok(AgentLoopAdvance::Progressed(EpisodeStatus::ToolWorkPending))
            }
            ToolGatewayOutcome::Ambiguous { .. } => {
                if dispatch_status != ToolOperationStatus::Reconciling {
                    versioned.state.tool_operations[index]
                        .record
                        .transition(ToolOperationStatus::Ambiguous)?;
                }
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::SuspensionRequested)?;
                versioned
                    .state
                    .episode
                    .transition(EpisodeStatus::Suspended)?;
                repository.save(versioned.revision, versioned.state)?;
                Ok(AgentLoopAdvance::Suspended)
            }
        }
    }

    /// Decides what follows a dispatch that produced no usable turn.
    ///
    /// Three things the loop used to get wrong, all of them observed on real runs.
    ///
    /// A transport that says `Never` is describing its own request or configuration, so retrying
    /// re-sends the same doomed bytes. A transport that asks for a delay was answered immediately,
    /// which is the worst possible reply to a rate limit. And a failure that keeps returning
    /// byte-identical is not a flake: `task-ccd149dfc0f421d97ed7feb4` burned 21 attempts on one
    /// deterministic cause, learning nothing after the first.
    fn after_failed_dispatch<R: EpisodeRepository>(
        repository: &mut R,
        mut versioned: VersionedEpisodeState,
        retry: ModelTransportRetryHint,
        repeated: bool,
    ) -> Result<AgentLoopAdvance, AgentLoopRuntimeError> {
        if retry == ModelTransportRetryHint::Never || repeated {
            versioned.state.episode.transition(EpisodeStatus::Failed)?;
            repository.save(versioned.revision, versioned.state)?;
            return Ok(AgentLoopAdvance::Terminal(EpisodeStatus::Failed));
        }
        let delay_millis = match retry {
            ModelTransportRetryHint::AfterMillis(millis) => millis,
            // No stated delay still deserves one. The exponent counts this episode's failures so
            // far, so a run that is failing repeatedly slows down instead of hammering.
            _ => backoff_millis(failed_attempts(&versioned.state)),
        };
        versioned
            .state
            .episode
            .transition(EpisodeStatus::ReadyForModel)?;
        repository.save(versioned.revision, versioned.state)?;
        Ok(AgentLoopAdvance::ProgressedAfter {
            status: EpisodeStatus::ReadyForModel,
            delay_millis,
        })
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

/// Whether this failure is the same one the previous attempt already recorded.
///
/// Compared by published digest rather than by text, so it is an identity check on bytes that
/// exist rather than a guess. `None` never repeats: a failure nobody could store is not evidence
/// that the next one will match.
fn repeats_the_last_failure(state: &DurableEpisodeState, diagnostic: Option<Sha256Digest>) -> bool {
    let Some(diagnostic) = diagnostic else {
        return false;
    };
    state
        .attempts
        .iter()
        .rev()
        .find_map(crate::model::ModelAttemptRecord::diagnostic_digest)
        .is_some_and(|previous| previous == diagnostic)
}

/// Attempts in this episode that produced no usable turn.
fn failed_attempts(state: &DurableEpisodeState) -> u32 {
    u32::try_from(
        state
            .attempts
            .iter()
            .filter(|attempt| attempt.diagnostic_digest().is_some())
            .count(),
    )
    .unwrap_or(u32::MAX)
}

/// Bounded exponential backoff: 250ms doubling to a 30s ceiling.
const fn backoff_millis(failures: u32) -> u64 {
    const BASE_MILLIS: u64 = 250;
    const CEILING_MILLIS: u64 = 30_000;
    let shift = if failures > 7 { 7 } else { failures };
    let delay = BASE_MILLIS << shift;
    if delay > CEILING_MILLIS {
        CEILING_MILLIS
    } else {
        delay
    }
}
