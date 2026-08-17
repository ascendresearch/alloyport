use super::*;
use crate::{
    AgentLoopAdvance, AgentRuntimeFaultPoint, EpisodeRepository, EpisodeSpec, GatewayTurn,
    GatewayTurnExchange, InMemoryEpisodeRepository, ModelGatewayOutcome, NoAgentRuntimeFault,
    NormalizedStopReason, OneShotAgentRuntimeFault, RuntimeToolDescriptor,
    ScriptedFakeModelGateway, ScriptedFakeToolGateway, ScriptedGatewayStep, ScriptedToolStep,
    SearchRunId, TaskId, ToolEffectClass, ToolGatewayAction, ToolGatewayOutcome,
    ToolOperationStatus, ToolResultAuthority,
};
use std::error::Error;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn complete_immediate<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the core scripted gateway must complete without an async runtime"),
    }
}

macro_rules! immediate_async_test {
    ($test:ident, $case:ident) => {
        #[test]
        fn $test() -> Result<(), Box<dyn Error>> {
            complete_immediate($case())
        }
    };
}

immediate_async_test!(
    durable_loop_corrects_after_fake_source_gate_failure_across_restarts,
    durable_loop_corrects_after_fake_source_gate_failure_across_restarts_case
);
immediate_async_test!(
    model_dispatch_crash_is_charged_and_retry_creates_linked_history,
    model_dispatch_crash_is_charged_and_retry_creates_linked_history_case
);
immediate_async_test!(
    tool_outcome_crash_reconciles_same_logical_operation,
    tool_outcome_crash_reconciles_same_logical_operation_case
);
immediate_async_test!(
    cancellation_with_dispatched_tool_suspends_for_reconciliation,
    cancellation_with_dispatched_tool_suspends_for_reconciliation_case
);
immediate_async_test!(
    ambiguous_tool_outcome_resumes_through_explicit_reconciliation,
    ambiguous_tool_outcome_resumes_through_explicit_reconciliation_case
);
immediate_async_test!(
    pending_remote_tool_reconciles_without_becoming_ambiguous,
    pending_remote_tool_reconciles_without_becoming_ambiguous_case
);
immediate_async_test!(
    cancellation_without_ambiguous_effect_reaches_cancelled,
    cancellation_without_ambiguous_effect_reaches_cancelled_case
);
immediate_async_test!(
    observed_tool_cannot_satisfy_the_episode_contract,
    observed_tool_cannot_satisfy_the_episode_contract_case
);
immediate_async_test!(
    model_turn_budget_exhausts_instead_of_looping,
    model_turn_budget_exhausts_instead_of_looping_case
);
immediate_async_test!(
    narrative_stop_without_verified_result_is_incomplete,
    narrative_stop_without_verified_result_is_incomplete_case
);
immediate_async_test!(
    a_turn_wider_than_the_per_turn_cap_is_reviewed_not_charged_to_the_budget,
    a_turn_wider_than_the_per_turn_cap_is_reviewed_not_charged_to_the_budget_case
);
immediate_async_test!(
    spending_the_operation_budget_ends_the_episode_cleanly,
    spending_the_operation_budget_ends_the_episode_cleanly_case
);

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn policy() -> AgentLoopPolicy {
    AgentLoopPolicy {
        max_model_turns: 8,
        max_model_attempts: 10,
        max_ambiguous_model_attempts: 1,
        max_tool_calls_per_turn: 2,
        max_total_tool_operations: 8,
        max_stop_feedback_turns: 1,
    }
}

fn state_with_policy(policy: AgentLoopPolicy) -> Result<DurableEpisodeState, Box<dyn Error>> {
    let episode = AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from("episode-runtime-1")?,
        task_id: TaskId::try_from("task-1")?,
        search_run_id: SearchRunId::try_from("search-1")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: "configured-runtime-model".to_owned(),
        resolved_model_digest: digest("resolved-model"),
        prompt_revision: "migration-v1".to_owned(),
        tool_catalog_digest: digest("tool-catalog"),
        loop_policy_digest: digest("loop-policy"),
        data_boundary_policy_digest: digest("data-boundary"),
        budget_snapshot_digest: digest("budget"),
    })?;
    Ok(DurableEpisodeState::new(AgentLoopRuntimeSpec {
        episode,
        policy,
        initial_input_digest: digest("initial-input"),
        resolved_model_digest: digest("resolved-model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
    })?)
}

fn tool_turn(call_id: &str, name: &str, candidate: &str) -> GatewayTurn {
    GatewayTurn {
        narrative: Vec::new(),
        tool_calls: vec![GatewayToolCall {
            native_call_id: call_id.to_owned(),
            name: name.to_owned(),
            raw_arguments: format!(r#"{{"candidate":"{candidate}"}}"#).into_bytes(),
        }],
        stop_reason: NormalizedStopReason::ToolCalls,
        usage: None,
    }
}

fn final_turn() -> GatewayTurn {
    GatewayTurn {
        narrative: vec!["candidate corrected and independently source-checked".to_owned()],
        tool_calls: Vec::new(),
        stop_reason: NormalizedStopReason::Stop,
        usage: None,
    }
}

fn exchange(label: &str, turn: GatewayTurn) -> GatewayTurnExchange {
    GatewayTurnExchange {
        turn,
        raw_exchange_digest: digest(&format!("{label}-raw")),
        native_continuation_digest: digest(&format!("{label}-continuation")),
    }
}

fn descriptors() -> Vec<RuntimeToolDescriptor> {
    vec![
        RuntimeToolDescriptor {
            name: "submit_candidate_bundle".to_owned(),
            version: "1".to_owned(),
            effect_class: ToolEffectClass::CandidateWrite,
            result_authority: ToolResultAuthority::Observed,
        },
        RuntimeToolDescriptor {
            name: "request_source_gate".to_owned(),
            version: "1".to_owned(),
            effect_class: ToolEffectClass::ReadOnly,
            result_authority: ToolResultAuthority::VerifiedReference,
        },
    ]
}

fn completed(
    status: ToolOperationStatus,
    result: &str,
    satisfies_subtask: bool,
) -> ToolGatewayOutcome {
    ToolGatewayOutcome::Completed {
        status,
        result_digest: digest(result),
        receipt_digests: vec![digest(&format!("{result}-receipt"))],
        satisfies_subtask,
    }
}

fn scripted_model_for_correction() -> ScriptedFakeModelGateway {
    let result_1 = digest("candidate-1-created");
    let result_2 = digest("candidate-1-source-failed");
    let result_3 = digest("candidate-2-created");
    let result_4 = digest("candidate-2-source-passed");
    let continuation_1 = digest("submit-1-continuation");
    let continuation_2 = digest("gate-1-continuation");
    let continuation_3 = digest("submit-2-continuation");
    let continuation_4 = digest("gate-2-continuation");
    let input_2 = derive_model_continuation_input_digest(continuation_1, [result_1]);
    let input_3 = derive_model_continuation_input_digest(continuation_2, [result_2]);
    let input_4 = derive_model_continuation_input_digest(continuation_3, [result_3]);
    let input_5 = derive_model_continuation_input_digest(continuation_4, [result_4]);
    ScriptedFakeModelGateway::new([
        ScriptedGatewayStep {
            expected_turn_index: 1,
            expected_input_digest: digest("initial-input"),
            outcome: ModelGatewayOutcome::Turn(GatewayTurnExchange {
                turn: tool_turn("call-submit-1", "submit_candidate_bundle", "candidate-1"),
                raw_exchange_digest: digest("submit-1-raw"),
                native_continuation_digest: continuation_1,
            }),
        },
        ScriptedGatewayStep {
            expected_turn_index: 2,
            expected_input_digest: input_2,
            outcome: ModelGatewayOutcome::Turn(GatewayTurnExchange {
                turn: tool_turn("call-gate-1", "request_source_gate", "candidate-1"),
                raw_exchange_digest: digest("gate-1-raw"),
                native_continuation_digest: continuation_2,
            }),
        },
        ScriptedGatewayStep {
            expected_turn_index: 3,
            expected_input_digest: input_3,
            outcome: ModelGatewayOutcome::Turn(GatewayTurnExchange {
                turn: tool_turn("call-submit-2", "submit_candidate_bundle", "candidate-2"),
                raw_exchange_digest: digest("submit-2-raw"),
                native_continuation_digest: continuation_3,
            }),
        },
        ScriptedGatewayStep {
            expected_turn_index: 4,
            expected_input_digest: input_4,
            outcome: ModelGatewayOutcome::Turn(GatewayTurnExchange {
                turn: tool_turn("call-gate-2", "request_source_gate", "candidate-2"),
                raw_exchange_digest: digest("gate-2-raw"),
                native_continuation_digest: continuation_4,
            }),
        },
        ScriptedGatewayStep {
            expected_turn_index: 5,
            expected_input_digest: input_5,
            outcome: ModelGatewayOutcome::Turn(exchange("final", final_turn())),
        },
    ])
}

fn scripted_tools_for_correction() -> ScriptedFakeToolGateway {
    ScriptedFakeToolGateway::new(
        descriptors(),
        [
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: completed(ToolOperationStatus::Succeeded, "candidate-1-created", false),
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "request_source_gate".to_owned(),
                outcome: completed(
                    ToolOperationStatus::CandidateFailed,
                    "candidate-1-source-failed",
                    false,
                ),
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: completed(ToolOperationStatus::Succeeded, "candidate-2-created", false),
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "request_source_gate".to_owned(),
                outcome: completed(
                    ToolOperationStatus::Succeeded,
                    "candidate-2-source-passed",
                    true,
                ),
            },
        ],
    )
}

async fn durable_loop_corrects_after_fake_source_gate_failure_across_restarts_case()
-> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let mut models = scripted_model_for_correction();
    let mut tools = scripted_tools_for_correction();

    for _ in 0..64 {
        let runner = AgentLoopRunner::new(episode_id.clone());
        let outcome = runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
        if outcome == AgentLoopAdvance::Terminal(EpisodeStatus::Succeeded) {
            break;
        }
    }

    let state = repository.load(&episode_id)?.state;
    assert_eq!(state.episode().status(), EpisodeStatus::Succeeded);
    assert_eq!(state.turn_count(), 5);
    assert_eq!(state.tool_operation_count(), 4);
    assert_eq!(tools.invocation_count(), 4);
    assert_eq!(
        state.tool_statuses(),
        vec![
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::CandidateFailed,
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::Succeeded,
        ]
    );
    Ok(())
}

async fn model_dispatch_crash_is_charged_and_retry_creates_linked_history_case()
-> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange("retry", final_turn())),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(descriptors(), []);
    let runner = AgentLoopRunner::new(episode_id.clone());
    runner
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;
    runner
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;
    let error = runner
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut OneShotAgentRuntimeFault::new(AgentRuntimeFaultPoint::AfterModelDispatchCommit),
        )
        .await
        .expect_err("dispatch crash must be injected");
    assert!(matches!(error, AgentLoopRuntimeError::InjectedCrash(_)));

    AgentLoopRunner::new(episode_id.clone())
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;
    AgentLoopRunner::new(episode_id.clone())
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;
    AgentLoopRunner::new(episode_id.clone())
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;

    let state = repository.load(&episode_id)?.state;
    assert_eq!(state.model_attempt_count(), 2);
    assert_eq!(state.ambiguous_model_attempt_count(), 1);
    let json = serde_json::to_value(state)?;
    assert_eq!(
        json["attempts"][1]["predecessor_attempt_id"],
        "model-attempt-episode-runtime-1-1"
    );
    Ok(())
}

async fn tool_outcome_crash_reconciles_same_logical_operation_case() -> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let continuation = digest("tool-continuation");
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(GatewayTurnExchange {
            turn: tool_turn("call-1", "submit_candidate_bundle", "candidate-1"),
            raw_exchange_digest: digest("tool-raw"),
            native_continuation_digest: continuation,
        }),
    }]);
    let outcome = completed(ToolOperationStatus::Succeeded, "candidate-created", false);
    let mut tools = ScriptedFakeToolGateway::new(
        descriptors(),
        [
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: outcome.clone(),
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Reconcile,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome,
            },
        ],
    );
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..4 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    runner
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut OneShotAgentRuntimeFault::new(
                AgentRuntimeFaultPoint::AfterToolOutcomeBeforeCommit,
            ),
        )
        .await
        .expect_err("tool outcome crash must be injected");
    AgentLoopRunner::new(episode_id.clone())
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;

    assert_eq!(tools.invocation_count(), 2);
    assert_eq!(tools.invocation_ids()[0], tools.invocation_ids()[1]);
    assert_eq!(
        repository.load(&episode_id)?.state.tool_statuses(),
        vec![ToolOperationStatus::Succeeded]
    );
    Ok(())
}

async fn cancellation_with_dispatched_tool_suspends_for_reconciliation_case()
-> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange(
            "tool",
            tool_turn("call-1", "submit_candidate_bundle", "candidate-1"),
        )),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(
        descriptors(),
        [ScriptedToolStep {
            action: ToolGatewayAction::Execute,
            expected_tool_name: "submit_candidate_bundle".to_owned(),
            outcome: completed(ToolOperationStatus::Succeeded, "created", false),
        }],
    );
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..4 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    runner
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut OneShotAgentRuntimeFault::new(AgentRuntimeFaultPoint::AfterToolDispatchCommit),
        )
        .await
        .expect_err("dispatch crash must be injected");
    assert_eq!(
        AgentLoopRunner::new(episode_id.clone()).request_cancellation(&mut repository)?,
        EpisodeStatus::Suspended
    );
    let state = repository.load(&episode_id)?.state;
    assert_eq!(state.episode().status(), EpisodeStatus::Suspended);
    assert_eq!(state.tool_statuses(), vec![ToolOperationStatus::Ambiguous]);
    assert_eq!(tools.invocation_count(), 0);
    Ok(())
}

async fn ambiguous_tool_outcome_resumes_through_explicit_reconciliation_case()
-> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange(
            "ambiguous-tool",
            tool_turn("call-1", "submit_candidate_bundle", "candidate-1"),
        )),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(
        descriptors(),
        [
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: ToolGatewayOutcome::Ambiguous {
                    diagnostic_digest: digest("unknown-outcome"),
                },
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Reconcile,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: completed(ToolOperationStatus::Succeeded, "reconciled", false),
            },
        ],
    );
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..5 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    assert_eq!(
        repository.load(&episode_id)?.state.episode().status(),
        EpisodeStatus::Suspended
    );
    assert_eq!(
        AgentLoopRunner::new(episode_id.clone()).resume_reconciliation(&mut repository)?,
        EpisodeStatus::ToolWorkPending
    );
    AgentLoopRunner::new(episode_id.clone())
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await?;
    assert_eq!(tools.invocation_count(), 2);
    assert_eq!(tools.invocation_ids()[0], tools.invocation_ids()[1]);
    assert_eq!(
        repository.load(&episode_id)?.state.tool_statuses(),
        vec![ToolOperationStatus::Succeeded]
    );
    Ok(())
}

async fn pending_remote_tool_reconciles_without_becoming_ambiguous_case()
-> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange(
            "pending-tool",
            tool_turn("call-1", "submit_candidate_bundle", "candidate-1"),
        )),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(
        descriptors(),
        [
            ScriptedToolStep {
                action: ToolGatewayAction::Execute,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: ToolGatewayOutcome::Pending {
                    diagnostic_digest: digest("remote-attempt-dispatched"),
                },
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Reconcile,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: ToolGatewayOutcome::Pending {
                    diagnostic_digest: digest("remote-attempt-running"),
                },
            },
            ScriptedToolStep {
                action: ToolGatewayAction::Reconcile,
                expected_tool_name: "submit_candidate_bundle".to_owned(),
                outcome: completed(ToolOperationStatus::Succeeded, "remote-result", false),
            },
        ],
    );
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..5 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    assert_eq!(
        repository.load(&episode_id)?.state.tool_statuses(),
        vec![ToolOperationStatus::Running]
    );
    for _ in 0..2 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    assert_eq!(tools.invocation_count(), 3);
    assert!(
        tools
            .invocation_ids()
            .windows(2)
            .all(|ids| ids[0] == ids[1])
    );
    assert_eq!(
        repository.load(&episode_id)?.state.tool_statuses(),
        vec![ToolOperationStatus::Succeeded]
    );
    Ok(())
}

async fn cancellation_without_ambiguous_effect_reaches_cancelled_case() -> Result<(), Box<dyn Error>>
{
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let runner = AgentLoopRunner::new(episode_id.clone());
    assert_eq!(
        runner.request_cancellation(&mut repository)?,
        EpisodeStatus::CancellationPending
    );
    let mut models = ScriptedFakeModelGateway::new([]);
    let mut tools = ScriptedFakeToolGateway::new([], []);
    assert_eq!(
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?,
        AgentLoopAdvance::Terminal(EpisodeStatus::Cancelled)
    );
    Ok(())
}

async fn observed_tool_cannot_satisfy_the_episode_contract_case() -> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(policy())?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange(
            "untrusted-completion",
            tool_turn("call-1", "submit_candidate_bundle", "candidate-1"),
        )),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(
        descriptors(),
        [ScriptedToolStep {
            action: ToolGatewayAction::Execute,
            expected_tool_name: "submit_candidate_bundle".to_owned(),
            outcome: completed(ToolOperationStatus::Succeeded, "candidate-created", true),
        }],
    );
    let runner = AgentLoopRunner::new(episode_id);
    for _ in 0..4 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    let error = runner
        .advance(
            &mut repository,
            &mut models,
            &mut tools,
            &mut NoAgentRuntimeFault,
        )
        .await
        .expect_err("observed result must not claim verified completion");
    assert!(matches!(error, AgentLoopRuntimeError::UntrustedCompletion));
    Ok(())
}

async fn model_turn_budget_exhausts_instead_of_looping_case() -> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut limited = policy();
    limited.max_model_turns = 1;
    limited.max_model_attempts = 1;
    limited.max_stop_feedback_turns = 1;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(limited)?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange("early-stop", final_turn())),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(descriptors(), []);
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..8 {
        let outcome = runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
        if outcome == AgentLoopAdvance::Terminal(EpisodeStatus::BudgetExhausted) {
            break;
        }
    }
    assert_eq!(
        repository.load(&episode_id)?.state.episode().status(),
        EpisodeStatus::BudgetExhausted
    );
    Ok(())
}

async fn narrative_stop_without_verified_result_is_incomplete_case() -> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut no_feedback = policy();
    no_feedback.max_stop_feedback_turns = 0;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(no_feedback)?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange("unverified", final_turn())),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(descriptors(), []);
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..8 {
        runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
    }
    assert_eq!(
        repository.load(&episode_id)?.state.episode().status(),
        EpisodeStatus::Incomplete
    );
    Ok(())
}

fn wide_turn(calls: usize) -> GatewayTurn {
    GatewayTurn {
        narrative: Vec::new(),
        tool_calls: (0..calls)
            .map(|index| GatewayToolCall {
                native_call_id: format!("call-{index}"),
                name: "submit_candidate_bundle".to_owned(),
                raw_arguments: format!(r#"{{"candidate":"c{index}"}}"#).into_bytes(),
            })
            .collect(),
        stop_reason: NormalizedStopReason::ToolCalls,
        usage: None,
    }
}

/// A turn asking for more tools than one turn allows is a turn, not an exhausted budget.
///
/// `task-1cadf422a8fed170618c775a` died here: the model issued six `read_reference` calls where
/// four are allowed and the run failed with `invalid episode transition: TurnRecorded ->
/// BudgetExhausted`. `TurnRecorded` was the one state that never listed `BudgetExhausted`, so this
/// branch could only ever produce that error — it was dead on arrival. Recording it as budget
/// exhaustion would also have been false: the Episode still held 52 of its 60 operations.
async fn a_turn_wider_than_the_per_turn_cap_is_reviewed_not_charged_to_the_budget_case()
-> Result<(), Box<dyn Error>> {
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut narrow = policy();
    narrow.max_tool_calls_per_turn = 2;
    narrow.max_total_tool_operations = 8;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(narrow)?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange("too-wide", wide_turn(6))),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(descriptors(), []);
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..8 {
        let outcome = runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
        if matches!(
            outcome,
            AgentLoopAdvance::Progressed(EpisodeStatus::StopReview)
        ) {
            break;
        }
    }
    let state = repository.load(&episode_id)?.state;
    assert_eq!(state.episode().status(), EpisodeStatus::StopReview);
    assert_eq!(
        state.tool_operation_count(),
        0,
        "a discarded turn must not charge operations it never ran"
    );
    Ok(())
}

/// Spending the operation budget is terminal, and must be a verdict rather than an error.
async fn spending_the_operation_budget_ends_the_episode_cleanly_case() -> Result<(), Box<dyn Error>>
{
    let episode_id = EpisodeId::try_from("episode-runtime-1")?;
    let mut tiny = policy();
    tiny.max_tool_calls_per_turn = 4;
    tiny.max_total_tool_operations = 1;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(state_with_policy(tiny)?)?;
    let mut models = ScriptedFakeModelGateway::new([ScriptedGatewayStep {
        expected_turn_index: 1,
        expected_input_digest: digest("initial-input"),
        outcome: ModelGatewayOutcome::Turn(exchange("over-budget", wide_turn(2))),
    }]);
    let mut tools = ScriptedFakeToolGateway::new(descriptors(), []);
    let runner = AgentLoopRunner::new(episode_id.clone());
    let mut reached = false;
    for _ in 0..8 {
        let outcome = runner
            .advance(
                &mut repository,
                &mut models,
                &mut tools,
                &mut NoAgentRuntimeFault,
            )
            .await?;
        if outcome == AgentLoopAdvance::Terminal(EpisodeStatus::BudgetExhausted) {
            reached = true;
            break;
        }
    }
    assert!(
        reached,
        "an exhausted operation budget must be a terminal verdict, not an error"
    );
    assert_eq!(
        repository.load(&episode_id)?.state.episode().status(),
        EpisodeStatus::BudgetExhausted
    );
    Ok(())
}
