//! Planning a turn's tool calls, and driving them to a terminal result.
//!
//! Split out of `agent_runtime.rs` for the module-size limit, as a child module for the same reason
//! as its sibling: the runner's transitions stay private.

use crate::agent_runtime_helpers::{
    AgentLoopRuntimeError, crash_if, derive_model_continuation_input_digest,
    derived_tool_operation_id, digest_label, usize_from_u32,
};
use crate::agent_runtime_support::{
    AgentLoopAdvance, AgentRuntimeFaultInjector, AgentRuntimeFaultPoint, AgentToolGateway,
    EpisodeRepository, RuntimeToolDescriptor, ToolGatewayOutcome, ToolInvocation,
    VersionedEpisodeState,
};
use crate::{
    EpisodeStatus, Sha256Digest, ToolEffectClass, ToolOperationRecord, ToolOperationSpec,
    ToolOperationStatus, ToolResultAuthority,
};

use super::{AgentLoopRunner, DurableToolOperation};

impl AgentLoopRunner {
    pub(super) fn plan_turn_tools<R, T, F>(
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
    pub(super) async fn drive_tools<R, T, F>(
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
}
