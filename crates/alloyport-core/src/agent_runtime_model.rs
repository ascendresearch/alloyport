//! Driving one model request, and what to do when the transport refuses it.
//!
//! Split out of `agent_runtime.rs` for the module-size limit. It stays a child module so it can use
//! the runner's own private transitions rather than exposing them.

use crate::agent_runtime_helpers::{
    AgentLoopRuntimeError, crash_if, derived_turn_id, digest_semantic_turn, progress,
};
use crate::agent_runtime_support::{
    AgentLoopAdvance, AgentRuntimeFaultInjector, AgentRuntimeFaultPoint, EpisodeRepository,
    VersionedEpisodeState,
};
use crate::{
    EpisodeStatus, ModelAttemptStatus, ModelGateway, ModelGatewayOutcome, ModelTransportRetryHint,
    ModelTurnRequest, Sha256Digest, TurnRecord, TurnSpec,
};

use super::{AgentLoopRunner, DurableEpisodeState, DurableTurn};

impl AgentLoopRunner {
    #[allow(clippy::too_many_lines)]
    pub(super) async fn drive_model<R, M, F>(
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
}

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
