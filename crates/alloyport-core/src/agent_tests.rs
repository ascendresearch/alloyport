use super::*;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn episode() -> Result<AgentEpisodeRecord, Box<dyn Error>> {
    Ok(AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from("episode-1")?,
        task_id: TaskId::try_from("task-1")?,
        search_run_id: SearchRunId::try_from("search-1")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("inputs"),
        runtime_model_alias: "deepseek-v4-pro-default".to_owned(),
        resolved_model_digest: digest("model"),
        prompt_revision: "migration-agent-v1".to_owned(),
        tool_catalog_digest: digest("tools"),
        loop_policy_digest: digest("policy"),
        data_boundary_policy_digest: digest("data-boundary"),
        budget_snapshot_digest: digest("episode-budget"),
    })?)
}

#[test]
fn episode_reducer_preserves_iterative_tool_loop() -> Result<(), Box<dyn Error>> {
    let mut episode = episode()?;
    for state in [
        EpisodeStatus::ReadyForModel,
        EpisodeStatus::ModelAttemptPending,
        EpisodeStatus::TurnRecorded,
        EpisodeStatus::ToolWorkPending,
        EpisodeStatus::ReadyForModel,
        EpisodeStatus::ModelAttemptPending,
        EpisodeStatus::TurnRecorded,
        EpisodeStatus::StopReview,
        EpisodeStatus::Succeeded,
    ] {
        episode.transition(state)?;
    }
    assert_eq!(episode.status(), EpisodeStatus::Succeeded);
    assert!(episode.status().is_terminal());
    assert!(episode.transition(EpisodeStatus::ReadyForModel).is_err());
    Ok(())
}

#[test]
fn episode_snapshot_serializes_every_recovery_identity() -> Result<(), Box<dyn Error>> {
    let value = serde_json::to_value(episode()?)?;
    for field in [
        "subtask_contract_digest",
        "context_projection_digest",
        "input_artifact_root_digest",
        "resolved_model_digest",
        "tool_catalog_digest",
        "loop_policy_digest",
        "data_boundary_policy_digest",
        "budget_snapshot_digest",
    ] {
        assert!(
            value.get(field).is_some(),
            "missing recovery identity {field}"
        );
    }
    Ok(())
}

#[test]
fn final_text_state_cannot_skip_stop_review() -> Result<(), Box<dyn Error>> {
    let mut episode = episode()?;
    episode.transition(EpisodeStatus::ReadyForModel)?;
    episode.transition(EpisodeStatus::ModelAttemptPending)?;
    episode.transition(EpisodeStatus::TurnRecorded)?;
    assert!(episode.transition(EpisodeStatus::Succeeded).is_err());
    episode.transition(EpisodeStatus::StopReview)?;
    episode.transition(EpisodeStatus::Incomplete)?;
    assert_eq!(episode.status(), EpisodeStatus::Incomplete);
    Ok(())
}

fn tool_operation() -> Result<ToolOperationRecord, Box<dyn Error>> {
    Ok(ToolOperationRecord::new(ToolOperationSpec {
        id: ToolOperationId::try_from("tool-operation-1")?,
        episode_id: EpisodeId::try_from("episode-1")?,
        turn_id: TurnId::try_from("turn-1")?,
        native_call_id: "call-1".to_owned(),
        tool_name: "submit_candidate_bundle".to_owned(),
        tool_version: "1".to_owned(),
        effect_class: ToolEffectClass::CandidateWrite,
        result_authority: ToolResultAuthority::Observed,
        arguments_digest: digest("args"),
        input_identity_digest: digest("inputs"),
    })?)
}

#[test]
fn terminal_tool_state_requires_a_durable_result() -> Result<(), Box<dyn Error>> {
    let mut operation = tool_operation()?;
    operation.transition(ToolOperationStatus::Authorized)?;
    operation.transition(ToolOperationStatus::Dispatching)?;
    assert_eq!(
        operation.transition(ToolOperationStatus::Succeeded),
        Err(AgentRecordError::TerminalResultRequired)
    );
    operation.finish(
        ToolOperationStatus::Succeeded,
        digest("result"),
        vec![digest("receipt")],
    )?;
    assert_eq!(operation.result_digest(), Some(digest("result")));
    Ok(())
}

#[test]
fn ambiguous_tool_operation_must_reconcile() -> Result<(), Box<dyn Error>> {
    let mut operation = tool_operation()?;
    operation.transition(ToolOperationStatus::Authorized)?;
    operation.transition(ToolOperationStatus::Dispatching)?;
    operation.transition(ToolOperationStatus::Ambiguous)?;
    assert!(
        operation
            .finish(ToolOperationStatus::Succeeded, digest("result"), Vec::new())
            .is_err()
    );
    operation.transition(ToolOperationStatus::Reconciling)?;
    operation.finish(
        ToolOperationStatus::Succeeded,
        digest("reconciled-result"),
        Vec::new(),
    )?;
    assert_eq!(operation.status(), ToolOperationStatus::Succeeded);
    Ok(())
}

#[test]
fn search_frontier_enters_refining_only_from_recorded_candidate() -> Result<(), Box<dyn Error>> {
    let candidate = CandidateId::try_from("candidate-1")?;
    let mut search = SearchRunRecord::new(SearchRunSpec {
        id: SearchRunId::try_from("search-1")?,
        task_id: TaskId::try_from("task-1")?,
        migration_spec_digest: digest("spec"),
        selection_policy_digest: digest("selection-policy"),
        budget_snapshot_digest: digest("search-budget"),
    });
    search.transition(SearchStatus::Running)?;
    assert!(search.begin_refining(candidate.clone()).is_err());
    search.record_candidate(candidate.clone())?;
    assert!(search.record_candidate(candidate.clone()).is_err());
    search.begin_refining(candidate)?;
    assert_eq!(search.phase(), SearchPhase::Refining);
    Ok(())
}
