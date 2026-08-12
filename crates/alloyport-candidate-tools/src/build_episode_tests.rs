use super::*;

#[test]
#[allow(clippy::too_many_lines)]
fn same_episode_corrects_a_failed_build_and_retries_with_a_child_candidate()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );

    // Precompute only deterministic candidate and Source Gate identities for the scripted turns.
    let mut preflight =
        CandidateToolGateway::new(config.clone(), artifacts.clone(), workspace.path())?;
    let (_, first_submit_result) = execute(
        &mut preflight,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "pre-build-first",
        ),
    );
    let first_submit_json = read_json(artifacts.as_ref(), first_submit_result);
    let first_candidate = first_submit_json["candidate_id"]
        .as_str()
        .expect("first candidate ID")
        .to_owned();
    let first_manifest: Sha256Digest =
        serde_json::from_value(first_submit_json["manifest"]["digest"].clone())?;
    let (_, first_source_receipt) = execute(
        &mut preflight,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":first_manifest}),
            "pre-build-first-source",
        ),
    );

    let child_bundle = bundle(true, Some(&first_candidate));
    let (_, child_submit_result) = execute(
        &mut preflight,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &child_bundle,
            "pre-build-child",
        ),
    );
    let child_submit_json = read_json(artifacts.as_ref(), child_submit_result);
    let child_candidate = child_submit_json["candidate_id"]
        .as_str()
        .expect("child candidate ID")
        .to_owned();
    let child_manifest: Sha256Digest =
        serde_json::from_value(child_submit_json["manifest"]["digest"].clone())?;
    let (_, child_source_receipt) = execute(
        &mut preflight,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":child_manifest}),
            "pre-build-child-source",
        ),
    );

    let assignments = Arc::new(Mutex::new(Vec::new()));
    let attempts = FakeBuildAttemptPort::new(
        [
            FakeBuildStep::Pending,
            FakeBuildStep::Finished {
                outcome: AttemptOutcome::CandidateFailed,
                build_completed: false,
            },
            FakeBuildStep::Pending,
            FakeBuildStep::Finished {
                outcome: AttemptOutcome::Succeeded,
                build_completed: true,
            },
        ],
        Arc::clone(&assignments),
    );
    let mut tools = CandidateToolGateway::new(config, artifacts, workspace.path())?
        .with_ascend_build(build_config()?, Box::new(attempts));
    let continuations = [
        digest("build-c1"),
        digest("build-c2"),
        digest("build-c3"),
        digest("build-c4"),
        digest("build-c5"),
        digest("build-c6"),
        digest("build-c7"),
    ];
    let mut models = OrderedModelGateway::new([
        exchange(
            "build-submit-first",
            tool_turn(
                "build-submit-first",
                SUBMIT_CANDIDATE_BUNDLE_TOOL,
                &bundle(true, None),
            ),
            continuations[0],
        ),
        exchange(
            "build-source-first",
            tool_turn(
                "build-source-first",
                REQUEST_SOURCE_GATE_TOOL,
                &json!({"manifest_digest":first_manifest}),
            ),
            continuations[1],
        ),
        exchange(
            "build-attempt-first",
            tool_turn(
                "build-attempt-first",
                REQUEST_ASCEND_BUILD_TOOL,
                &json!({
                    "manifest_digest":first_manifest,
                    "source_gate_receipt_digest":first_source_receipt
                }),
            ),
            continuations[2],
        ),
        exchange(
            "build-submit-child",
            tool_turn(
                "build-submit-child",
                SUBMIT_CANDIDATE_BUNDLE_TOOL,
                &child_bundle,
            ),
            continuations[3],
        ),
        exchange(
            "build-source-child",
            tool_turn(
                "build-source-child",
                REQUEST_SOURCE_GATE_TOOL,
                &json!({"manifest_digest":child_manifest}),
            ),
            continuations[4],
        ),
        exchange(
            "build-attempt-child",
            tool_turn(
                "build-attempt-child",
                REQUEST_ASCEND_BUILD_TOOL,
                &json!({
                    "manifest_digest":child_manifest,
                    "source_gate_receipt_digest":child_source_receipt
                }),
            ),
            continuations[5],
        ),
        exchange(
            "build-final",
            GatewayTurn {
                narrative: vec![
                    "child candidate passed the independent Ascend Build Gate".to_owned(),
                ],
                tool_calls: Vec::new(),
                stop_reason: NormalizedStopReason::Stop,
                usage: None,
            },
            continuations[6],
        ),
    ]);
    let episode_id = EpisodeId::try_from("episode-real-build-gate")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(build_runtime_state()?)?;
    let runner = AgentLoopRunner::new(episode_id.clone());
    let outcome = complete_immediate(async {
        for _ in 0..96 {
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
    assert_eq!(state.turn_count(), 7);
    assert_eq!(state.tool_operation_count(), 6);
    assert_eq!(
        state.tool_statuses(),
        [
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::CandidateFailed,
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::Succeeded,
            ToolOperationStatus::Succeeded,
        ]
    );
    let assignments = assignments.lock().expect("assignment log");
    assert_eq!(assignments.len(), 4);
    assert_eq!(assignments[0], assignments[1]);
    assert_eq!(assignments[2], assignments[3]);
    assert_eq!(assignments[0].candidate_id.as_ref(), first_candidate);
    assert_eq!(assignments[2].candidate_id.as_ref(), child_candidate);
    assert_ne!(assignments[0].candidate_id, assignments[2].candidate_id);
    Ok(())
}
