use super::*;
use alloyport_core::{
    AgentEpisodeRecord, AgentLoopPolicy, AgentLoopRuntimeSpec, EpisodeRepository, EpisodeSpec,
    InMemoryEpisodeRepository, SearchRunId, Sha256Digest, TaskId,
};

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn state(id: &str) -> Result<DurableEpisodeState, Box<dyn std::error::Error>> {
    let episode = AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from(id)?,
        task_id: TaskId::try_from("task-sqlite-episode")?,
        search_run_id: SearchRunId::try_from("search-sqlite-episode")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: "configured-model".to_owned(),
        resolved_model_digest: digest("resolved-model"),
        prompt_revision: "candidate-v1".to_owned(),
        tool_catalog_digest: digest("tools"),
        loop_policy_digest: digest("loop-policy"),
        data_boundary_policy_digest: digest("boundary"),
        budget_snapshot_digest: digest("budget"),
    })?;
    Ok(DurableEpisodeState::new(AgentLoopRuntimeSpec {
        episode,
        policy: AgentLoopPolicy {
            max_model_turns: 8,
            max_model_attempts: 10,
            max_ambiguous_model_attempts: 1,
            max_tool_calls_per_turn: 2,
            max_total_tool_operations: 16,
            max_stop_feedback_turns: 1,
        },
        initial_input_digest: digest("initial-input"),
        resolved_model_digest: digest("resolved-model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
    })?)
}

#[test]
fn sqlite_episode_repository_reopens_exact_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("episodes.sqlite3");
    let expected = state("episode-sqlite-reopen")?;
    let id = expected.episode().id().clone();
    {
        let mut repository = SqliteEpisodeRepository::open(&path)?;
        repository.create(expected.clone())?;
    }
    let repository = SqliteEpisodeRepository::open(&path)?;
    let loaded = repository.load(&id)?;
    assert_eq!(loaded.revision, 0);
    assert_eq!(loaded.state, expected);
    Ok(())
}

fn episode_repository_contract(
    repository: &mut dyn EpisodeRepository,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = state("episode-sqlite-cas")?;
    let id = expected.episode().id().clone();
    assert_eq!(
        repository.load(&id),
        Err(EpisodeRepositoryError::NotFound(id.clone()))
    );
    repository.create(expected.clone())?;
    assert_eq!(
        repository.create(expected.clone()),
        Err(EpisodeRepositoryError::AlreadyExists(id.clone()))
    );
    assert_eq!(repository.save(0, expected.clone())?, 1);
    assert_eq!(
        repository.save(0, expected),
        Err(EpisodeRepositoryError::Conflict {
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(repository.load(&id)?.revision, 1);
    Ok(())
}

#[test]
fn sqlite_episode_repository_matches_reference_contract() -> Result<(), Box<dyn std::error::Error>>
{
    episode_repository_contract(&mut InMemoryEpisodeRepository::default())?;
    episode_repository_contract(&mut SqliteEpisodeRepository::in_memory()?)
}

#[test]
fn sqlite_episode_repository_rejects_malformed_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut repository = SqliteEpisodeRepository::in_memory()?;
    let expected = state("episode-sqlite-corrupt")?;
    let id = expected.episode().id().clone();
    repository.create(expected)?;
    repository.connection()?.execute(
        "UPDATE agent_episodes SET state_json = ?1 WHERE episode_id = ?2",
        params![b"{}".as_slice(), id.to_string()],
    )?;
    assert!(matches!(
        repository.load(&id),
        Err(EpisodeRepositoryError::Adapter(_))
    ));
    Ok(())
}
