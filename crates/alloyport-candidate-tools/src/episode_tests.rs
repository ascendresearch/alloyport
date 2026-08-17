//! Durable Episode behaviour: a real Source Gate failure corrected inside one Episode, a malformed
//! call returned as a readable rejection, and the compiler's own words reaching the model.

use super::*;

#[test]
#[allow(clippy::too_many_lines)]
fn same_episode_consumes_real_source_failure_and_submits_a_correction() -> Result<(), Box<dyn Error>>
{
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut tools = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?;

    let bad_submit = invocation(
        SUBMIT_CANDIDATE_BUNDLE_TOOL,
        &bundle(false, None),
        "pre-bad",
    );
    let (_, bad_result) = execute(&mut tools, &bad_submit);
    let bad_json = read_json(artifacts.as_ref(), bad_result);
    let bad_candidate = bad_json["candidate_id"].as_str().expect("candidate ID");
    let bad_manifest: Sha256Digest =
        serde_json::from_value(bad_json["manifest"]["digest"].clone())?;
    let bad_gate = invocation(
        REQUEST_SOURCE_GATE_TOOL,
        &json!({"manifest_digest":bad_manifest}),
        "pre-gate-bad",
    );
    let (_, bad_receipt) = execute(&mut tools, &bad_gate);

    let good_bundle = bundle(true, Some(bad_candidate));
    let good_submit = invocation(SUBMIT_CANDIDATE_BUNDLE_TOOL, &good_bundle, "pre-good");
    let (_, good_result) = execute(&mut tools, &good_submit);
    let good_json = read_json(artifacts.as_ref(), good_result);
    let good_manifest: Sha256Digest =
        serde_json::from_value(good_json["manifest"]["digest"].clone())?;
    let good_gate = invocation(
        REQUEST_SOURCE_GATE_TOOL,
        &json!({"manifest_digest":good_manifest}),
        "pre-gate-good",
    );
    let (_, good_receipt) = execute(&mut tools, &good_gate);

    let continuations = [digest("c1"), digest("c2"), digest("c3"), digest("c4")];
    let input2 =
        alloyport_core::derive_model_continuation_input_digest(continuations[0], [bad_result]);
    let input3 =
        alloyport_core::derive_model_continuation_input_digest(continuations[1], [bad_receipt]);
    let input4 =
        alloyport_core::derive_model_continuation_input_digest(continuations[2], [good_result]);
    let input5 =
        alloyport_core::derive_model_continuation_input_digest(continuations[3], [good_receipt]);
    let mut models = ScriptedFakeModelGateway::new([
        ScriptedGatewayStep {
            expected_turn_index: 1,
            expected_input_digest: digest("initial-input"),
            outcome: ModelGatewayOutcome::Turn(exchange(
                "submit-bad",
                tool_turn(
                    "submit-bad",
                    SUBMIT_CANDIDATE_BUNDLE_TOOL,
                    &bundle(false, None),
                ),
                continuations[0],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 2,
            expected_input_digest: input2,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "gate-bad",
                tool_turn(
                    "gate-bad",
                    REQUEST_SOURCE_GATE_TOOL,
                    &json!({"manifest_digest":bad_manifest}),
                ),
                continuations[1],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 3,
            expected_input_digest: input3,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "submit-good",
                tool_turn("submit-good", SUBMIT_CANDIDATE_BUNDLE_TOOL, &good_bundle),
                continuations[2],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 4,
            expected_input_digest: input4,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "gate-good",
                tool_turn(
                    "gate-good",
                    REQUEST_SOURCE_GATE_TOOL,
                    &json!({"manifest_digest":good_manifest}),
                ),
                continuations[3],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 5,
            expected_input_digest: input5,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "final",
                GatewayTurn {
                    narrative: vec![
                        "corrected candidate passed independent Source Gate".to_owned(),
                    ],
                    tool_calls: Vec::new(),
                    stop_reason: NormalizedStopReason::Stop,
                    usage: None,
                },
                digest("c5"),
            )),
        },
    ]);
    let episode_id = EpisodeId::try_from("episode-real-source-gate")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(runtime_state()?)?;
    let runner = AgentLoopRunner::new(episode_id.clone());
    let outcome = complete_immediate(async {
        for _ in 0..64 {
            let outcome = runner
                .advance(
                    &mut repository,
                    &mut models,
                    &mut tools,
                    &mut NoAgentRuntimeFault,
                )
                .await?;
            if matches!(outcome, AgentLoopAdvance::Terminal(_)) {
                return Ok::<_, alloyport_core::AgentLoopRuntimeError>(outcome);
            }
        }
        Ok(AgentLoopAdvance::Progressed(EpisodeStatus::Created))
    })?;
    assert_eq!(
        outcome,
        AgentLoopAdvance::Terminal(EpisodeStatus::Succeeded)
    );
    let state = repository.load(&episode_id)?.state;
    assert_eq!(state.turn_count(), 5);
    assert_eq!(state.tool_operation_count(), 4);
    Ok(())
}

/// The exact defect that ended the 2026-08-13 live migration: one file object omits `path`.
fn malformed_bundle() -> Value {
    json!({
        "bundle": {
            "files": [
                {"kind":"ascend_c_device","contents":"#include <kernel_operator.h>\n"}
            ]
        }
    })
}

fn rejection_runtime_state() -> Result<DurableEpisodeState, Box<dyn Error>> {
    let episode = AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from("episode-invalid-arguments")?,
        task_id: TaskId::try_from("task-candidate-tools")?,
        search_run_id: SearchRunId::try_from("search-invalid-arguments")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: "configured-model".to_owned(),
        resolved_model_digest: digest("resolved-model"),
        prompt_revision: "fixture-v1".to_owned(),
        tool_catalog_digest: digest("tools"),
        loop_policy_digest: digest("policy"),
        data_boundary_policy_digest: digest("boundary"),
        budget_snapshot_digest: digest("budget"),
    })?;
    Ok(DurableEpisodeState::new(AgentLoopRuntimeSpec {
        episode,
        policy: AgentLoopPolicy {
            max_model_turns: 6,
            max_model_attempts: 6,
            max_ambiguous_model_attempts: 1,
            max_tool_calls_per_turn: 1,
            max_total_tool_operations: 4,
            max_stop_feedback_turns: 0,
        },
        initial_input_digest: digest("initial-input"),
        resolved_model_digest: digest("resolved-model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
    })?)
}

#[test]
fn malformed_arguments_publish_a_readable_rejection_instead_of_failing_the_migration()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let tools = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?;

    let call = GatewayToolCall {
        native_call_id: "call-malformed".to_owned(),
        name: SUBMIT_CANDIDATE_BUNDLE_TOOL.to_owned(),
        raw_arguments: serde_json::to_vec(&malformed_bundle())?,
    };
    let rejection = tools
        .validate_call(&call)
        .expect_err("a file object without a path cannot be decoded");
    let explanation = read_json(artifacts.as_ref(), rejection.result_digest);
    assert_eq!(explanation["rejected"], json!(true));
    assert_eq!(explanation["tool"], json!(SUBMIT_CANDIDATE_BUNDLE_TOOL));
    assert!(
        explanation["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("path")),
        "the model must be told which field was missing: {explanation}"
    );
    assert!(
        explanation["expected_arguments"]
            .as_str()
            .is_some_and(|contract| contract.contains("\"path\"")),
        "the rejection must restate the tool's own argument contract"
    );

    let unknown = GatewayToolCall {
        native_call_id: "call-unknown".to_owned(),
        name: "request_reduction_correctness".to_owned(),
        raw_arguments: b"{}".to_vec(),
    };
    let unknown_rejection = tools
        .validate_call(&unknown)
        .expect_err("a tool that is not enabled is not dispatchable");
    read_json(artifacts.as_ref(), unknown_rejection.result_digest);

    let good = GatewayToolCall {
        native_call_id: "call-good".to_owned(),
        name: SUBMIT_CANDIDATE_BUNDLE_TOOL.to_owned(),
        raw_arguments: serde_json::to_vec(&bundle(true, None))?,
    };
    assert!(
        tools.validate_call(&good).is_ok(),
        "validation must not change the good case"
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn an_invalid_tool_call_is_terminal_and_the_same_episode_continues() -> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut tools = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?;

    // Content-addressed publication is idempotent, so precomputing the rejection and result
    // digests scripts the model without changing what the run itself produces.
    let rejection_digest = tools
        .validate_call(&GatewayToolCall {
            native_call_id: "probe".to_owned(),
            name: SUBMIT_CANDIDATE_BUNDLE_TOOL.to_owned(),
            raw_arguments: serde_json::to_vec(&malformed_bundle())?,
        })
        .expect_err("malformed")
        .result_digest;
    let good_bundle = bundle(true, None);
    let (_, submit_result) = execute(
        &mut tools,
        &invocation(SUBMIT_CANDIDATE_BUNDLE_TOOL, &good_bundle, "pre-good"),
    );
    let manifest: Sha256Digest = serde_json::from_value(
        read_json(artifacts.as_ref(), submit_result)["manifest"]["digest"].clone(),
    )?;
    let gate_arguments = json!({ "manifest_digest": manifest });
    let (_, gate_receipt) = execute(
        &mut tools,
        &invocation(REQUEST_SOURCE_GATE_TOOL, &gate_arguments, "pre-gate"),
    );

    let continuations = [digest("r1"), digest("r2"), digest("r3")];
    let input2 = alloyport_core::derive_model_continuation_input_digest(
        continuations[0],
        [rejection_digest],
    );
    let input3 =
        alloyport_core::derive_model_continuation_input_digest(continuations[1], [submit_result]);
    let input4 =
        alloyport_core::derive_model_continuation_input_digest(continuations[2], [gate_receipt]);
    let mut models = ScriptedFakeModelGateway::new([
        ScriptedGatewayStep {
            expected_turn_index: 1,
            expected_input_digest: digest("initial-input"),
            outcome: ModelGatewayOutcome::Turn(exchange(
                "submit-malformed",
                tool_turn(
                    "submit-malformed",
                    SUBMIT_CANDIDATE_BUNDLE_TOOL,
                    &malformed_bundle(),
                ),
                continuations[0],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 2,
            expected_input_digest: input2,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "submit-corrected",
                tool_turn(
                    "submit-corrected",
                    SUBMIT_CANDIDATE_BUNDLE_TOOL,
                    &good_bundle,
                ),
                continuations[1],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 3,
            expected_input_digest: input3,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "gate",
                tool_turn("gate", REQUEST_SOURCE_GATE_TOOL, &gate_arguments),
                continuations[2],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 4,
            expected_input_digest: input4,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "final",
                GatewayTurn {
                    narrative: vec!["corrected the malformed call and passed".to_owned()],
                    tool_calls: Vec::new(),
                    stop_reason: NormalizedStopReason::Stop,
                    usage: None,
                },
                digest("r4"),
            )),
        },
    ]);
    let episode_id = EpisodeId::try_from("episode-invalid-arguments")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(rejection_runtime_state()?)?;
    let runner = AgentLoopRunner::new(episode_id.clone());
    let outcome = complete_immediate(async {
        for _ in 0..64 {
            let outcome = runner
                .advance(
                    &mut repository,
                    &mut models,
                    &mut tools,
                    &mut NoAgentRuntimeFault,
                )
                .await?;
            if matches!(outcome, AgentLoopAdvance::Terminal(_)) {
                return Ok::<_, alloyport_core::AgentLoopRuntimeError>(outcome);
            }
        }
        Ok(AgentLoopAdvance::Progressed(EpisodeStatus::Created))
    })?;
    assert_eq!(
        outcome,
        AgentLoopAdvance::Terminal(EpisodeStatus::Succeeded),
        "a defect in the model's own arguments must not end the migration"
    );
    let state = repository.load(&episode_id)?.state;
    assert_eq!(
        state.tool_statuses(),
        vec![
            ToolOperationStatus::RejectedAsInvalid,
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::Succeeded,
        ]
    );
    for result in state.tool_result_digests() {
        // Every result the controller will feed back must be a real artifact; a rejection whose
        // digest names nothing fails the next model turn instead of the malformed call.
        read_json(artifacts.as_ref(), result.expect("terminal result digest"));
    }
    Ok(())
}

#[test]
fn a_failed_build_hands_the_model_the_compiler_output_not_a_digest_it_cannot_open()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    // Long enough to exceed the returned bound, because a layer that truncates the ground truth
    // silently is how the important line disappears.
    let compiler_output = format!(
        "reduce_sum.cpp:12:5: error: use of undeclared identifier 'AscendC'\n{}",
        "note: expanded from macro\n".repeat(4_000)
    );
    let attempts = FakeBuildAttemptPort::new(
        [FakeBuildStep::Finished {
            outcome: AttemptOutcome::CandidateFailed,
            build_completed: false,
        }],
        Arc::new(Mutex::new(Vec::new())),
    )
    .publishing(artifacts.clone(), &compiler_output);
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut gateway = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?
        .with_ascend_build(build_config()?, Box::new(attempts));

    let bundle = bundle(true, None);
    let (_, submitted) = execute(
        &mut gateway,
        &invocation(SUBMIT_CANDIDATE_BUNDLE_TOOL, &bundle, "diag-submit"),
    );
    let manifest: Sha256Digest = serde_json::from_value(
        read_json(artifacts.as_ref(), submitted)["manifest"]["digest"].clone(),
    )?;
    let (_, source_gate_result) = execute(
        &mut gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({ "manifest_digest": manifest }),
            "diag-gate",
        ),
    );
    let source_receipt = cited_receipt_digest(artifacts.as_ref(), source_gate_result);
    let (status, build_result) = execute(
        &mut gateway,
        &invocation(
            REQUEST_ASCEND_BUILD_TOOL,
            &json!({
                "manifest_digest": manifest,
                "source_gate_receipt_digest": source_receipt
            }),
            "diag-build",
        ),
    );
    assert_eq!(status, ToolOperationStatus::CandidateFailed);
    // What the build receipt itself gives the model is a descriptor, which it has no way to open.
    let receipt = gate_payload(artifacts.as_ref(), build_result);
    assert!(receipt["stderr"]["digest"].is_string());
    assert!(receipt["stderr"]["text"].is_null());
    // The result names the receipt, so the instrument below is reachable at all.
    let build_receipt = cited_receipt_digest(artifacts.as_ref(), build_result);

    let (status, diagnostics) = execute(
        &mut gateway,
        &invocation(
            READ_BUILD_DIAGNOSTICS_TOOL,
            &json!({ "build_gate_receipt_digest": build_receipt }),
            "diag-read",
        ),
    );
    assert_eq!(status, ToolOperationStatus::Succeeded);
    let diagnostics = read_json(artifacts.as_ref(), diagnostics);
    let stderr = &diagnostics["stderr"];
    assert!(
        stderr["text"]
            .as_str()
            .is_some_and(|text| text.contains("use of undeclared identifier")),
        "the first compiler error must reach the model"
    );
    assert_eq!(stderr["truncated"], json!(true));
    assert!(
        stderr["total_bytes"].as_u64() > stderr["returned_bytes"].as_u64(),
        "truncation must state how much was withheld, not hide it"
    );
    Ok(())
}

#[test]
fn diagnostics_cannot_be_read_for_a_receipt_from_another_migration() -> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let attempts = FakeBuildAttemptPort::new(
        [FakeBuildStep::Finished {
            outcome: AttemptOutcome::CandidateFailed,
            build_completed: false,
        }],
        Arc::new(Mutex::new(Vec::new())),
    )
    .publishing(artifacts.clone(), "error: something");
    let mut gateway = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-candidate-tools")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        workspace.path(),
    )?
    .with_ascend_build(build_config()?, Box::new(attempts));
    let (_, submitted) = execute(
        &mut gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "foreign-submit",
        ),
    );
    let manifest: Sha256Digest = serde_json::from_value(
        read_json(artifacts.as_ref(), submitted)["manifest"]["digest"].clone(),
    )?;
    let (_, source_gate_result) = execute(
        &mut gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({ "manifest_digest": manifest }),
            "foreign-gate",
        ),
    );
    let source_receipt = cited_receipt_digest(artifacts.as_ref(), source_gate_result);
    let (_, build_result) = execute(
        &mut gateway,
        &invocation(
            REQUEST_ASCEND_BUILD_TOOL,
            &json!({
                "manifest_digest": manifest,
                "source_gate_receipt_digest": source_receipt
            }),
            "foreign-build",
        ),
    );
    let build_receipt = cited_receipt_digest(artifacts.as_ref(), build_result);

    // A second migration shares the store. Authority must come from the receipt belonging to this
    // context, never from the model naming a digest.
    let other_workspace = tempfile::tempdir()?;
    let mut other = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-somebody-else")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        other_workspace.path(),
    )?
    .with_ascend_build(
        build_config()?,
        Box::new(FakeBuildAttemptPort::new(
            [],
            Arc::new(Mutex::new(Vec::new())),
        )),
    );
    let (status, refusal) = execute(
        &mut other,
        &invocation(
            READ_BUILD_DIAGNOSTICS_TOOL,
            &json!({ "build_gate_receipt_digest": build_receipt }),
            "foreign-read",
        ),
    );
    // Two properties, and the second must not have cost the first. The refusal is recoverable,
    // because an instrument granting no authority must not end a migration; and it still discloses
    // nothing, because recoverable is not the same as permitted.
    assert_eq!(status, ToolOperationStatus::CandidateFailed);
    let explanation = read_json(artifacts.as_ref(), refusal);
    assert_eq!(explanation["recoverable"], json!(true));
    assert!(
        explanation["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("does not belong to this migration context"))
    );
    let disclosed = serde_json::to_string(&explanation)?;
    assert!(
        !disclosed.contains("something") && !disclosed.contains("stderr"),
        "a refusal must not carry the other migration's diagnostics: {disclosed}"
    );
    Ok(())
}
