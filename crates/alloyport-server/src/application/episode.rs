//! Controller-owned composition for one durable, model-pinned Agent Episode.

use crate::adapters::sqlite::{
    SharedSqliteModelContextStore, SqliteEpisodeRepository, SqliteModelContextStore,
};
use crate::model_context::ContextRecordingToolGateway;
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    AgentEpisodeRecord, AgentLoopAdvance, AgentLoopPolicy, AgentLoopRunner, AgentLoopRuntimeSpec,
    AgentToolGateway, CandidateId, CodecLimits, CodecToolDefinition, DurableEpisodeState,
    EpisodeId, EpisodeRepository, EpisodeRepositoryError, EpisodeSpec, EpisodeStatus,
    ModelTransport, NoAgentRuntimeFault, RuntimeModelCatalog, SearchRunId, Sha256Digest, TaskId,
};
use alloyport_llm_provider::{LlmProviderSdk, ProviderModelGateway, ReqwestModelTransport};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::Arc;

/// Immutable controller facts needed to create or recover one Episode.
#[derive(Clone, Debug)]
pub struct ControllerEpisodeSpec {
    pub episode_id: EpisodeId,
    pub task_id: TaskId,
    pub search_run_id: SearchRunId,
    pub parent_candidate_id: Option<CandidateId>,
    pub subtask_contract_digest: Sha256Digest,
    pub context_projection_digest: Sha256Digest,
    pub input_artifact_root_digest: Sha256Digest,
    pub runtime_model_alias: Option<String>,
    pub prompt_revision: String,
    pub tools: Vec<CodecToolDefinition>,
    pub loop_policy: AgentLoopPolicy,
    pub data_boundary_policy_digest: Sha256Digest,
    pub budget_snapshot_digest: Sha256Digest,
    pub request_budget_digest: Sha256Digest,
    pub system_prompt: String,
    pub initial_user_text: String,
}

/// Production composition whose adapters can be replaced with deterministic transports in tests.
pub struct ControllerEpisodeApplication<T, G> {
    runner: AgentLoopRunner,
    episode_id: EpisodeId,
    episodes: SqliteEpisodeRepository,
    models: ProviderModelGateway<SharedSqliteModelContextStore, T>,
    tools: ContextRecordingToolGateway<G>,
    faults: NoAgentRuntimeFault,
}

impl<T: std::fmt::Debug, G: std::fmt::Debug> std::fmt::Debug
    for ControllerEpisodeApplication<T, G>
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControllerEpisodeApplication")
            .field("episode_id", &self.episode_id)
            .finish_non_exhaustive()
    }
}

impl<T, G> ControllerEpisodeApplication<T, G>
where
    T: ModelTransport,
    G: AgentToolGateway,
{
    /// Creates a new Episode or recovers an exactly matching persisted Episode.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid model configuration, conflicting persistent identity, invalid
    /// context/tool configuration, or adapter initialization failure.
    pub fn open(
        spec: ControllerEpisodeSpec,
        catalog: RuntimeModelCatalog,
        transport: T,
        codec_limits: CodecLimits,
        artifacts: Arc<dyn ArtifactStore>,
        database_path: impl AsRef<Path>,
        tools: G,
    ) -> Result<Self, ControllerEpisodeError> {
        let sdk = LlmProviderSdk::new(catalog, transport, codec_limits).map_err(runtime_error)?;
        let deployment = sdk
            .resolve(spec.runtime_model_alias.as_deref())
            .map_err(runtime_error)?;
        let resolved_model_digest = deployment.digest().map_err(runtime_error)?;
        let deployment_digest = deployment.deployment_digest().map_err(runtime_error)?;
        let model_profile_digest = deployment.profile_digest().map_err(runtime_error)?;
        let tool_catalog_digest = serde_json::to_vec(&spec.tools)
            .map(|bytes| Sha256Digest::digest_bytes(&bytes))
            .map_err(runtime_error)?;
        let loop_policy_digest = spec.loop_policy.digest().map_err(runtime_error)?;
        let contexts = Arc::new(
            SqliteModelContextStore::open(database_path.as_ref(), artifacts, codec_limits)
                .map_err(ControllerEpisodeError)?,
        );
        let initial_input_digest = contexts
            .create_episode(
                &spec.episode_id,
                &spec.system_prompt,
                &spec.initial_user_text,
                &spec.tools,
            )
            .map_err(ControllerEpisodeError)?;
        let episode = AgentEpisodeRecord::new(EpisodeSpec {
            id: spec.episode_id.clone(),
            task_id: spec.task_id,
            search_run_id: spec.search_run_id,
            parent_candidate_id: spec.parent_candidate_id,
            subtask_contract_digest: spec.subtask_contract_digest,
            context_projection_digest: spec.context_projection_digest,
            input_artifact_root_digest: spec.input_artifact_root_digest,
            runtime_model_alias: deployment.alias().to_owned(),
            resolved_model_digest,
            prompt_revision: spec.prompt_revision,
            tool_catalog_digest,
            loop_policy_digest,
            data_boundary_policy_digest: spec.data_boundary_policy_digest,
            budget_snapshot_digest: spec.budget_snapshot_digest,
        })
        .map_err(runtime_error)?;
        let runtime_spec = AgentLoopRuntimeSpec {
            episode,
            policy: spec.loop_policy,
            initial_input_digest,
            resolved_model_digest,
            deployment_digest,
            model_profile_digest,
            request_budget_digest: spec.request_budget_digest,
        };
        let state = DurableEpisodeState::new(runtime_spec.clone()).map_err(runtime_error)?;
        let mut repository = SqliteEpisodeRepository::open(database_path).map_err(runtime_error)?;
        match repository.create(state) {
            Ok(()) => {}
            Err(EpisodeRepositoryError::AlreadyExists(_)) => {
                let recovered = repository.load(&spec.episode_id).map_err(runtime_error)?;
                if !recovered.state.matches_runtime_spec(&runtime_spec) {
                    return Err(ControllerEpisodeError(
                        "persisted Episode conflicts with requested runtime identity".to_owned(),
                    ));
                }
            }
            Err(error) => return Err(runtime_error(error)),
        }
        let models = ProviderModelGateway::new(
            sdk,
            spec.runtime_model_alias.as_deref(),
            SharedSqliteModelContextStore::new(contexts.clone()),
        )
        .map_err(runtime_error)?;
        Ok(Self {
            runner: AgentLoopRunner::new(spec.episode_id.clone()),
            episode_id: spec.episode_id.clone(),
            episodes: repository,
            models,
            tools: ContextRecordingToolGateway::new(tools, spec.episode_id, contexts),
            faults: NoAgentRuntimeFault,
        })
    }

    /// Advances at most one external model or tool action and durably commits each boundary.
    ///
    /// # Errors
    ///
    /// Returns an error from the reducer, provider, tool, or persistence adapter.
    pub async fn advance(&mut self) -> Result<AgentLoopAdvance, ControllerEpisodeError> {
        self.runner
            .advance(
                &mut self.episodes,
                &mut self.models,
                &mut self.tools,
                &mut self.faults,
            )
            .await
            .map_err(runtime_error)
    }

    /// Reads the latest durable Episode status.
    ///
    /// # Errors
    ///
    /// Returns an error if the Episode snapshot cannot be loaded.
    pub fn status(&self) -> Result<EpisodeStatus, ControllerEpisodeError> {
        self.episodes
            .load(&self.episode_id)
            .map(|versioned| versioned.state.episode().status())
            .map_err(runtime_error)
    }
}

impl<G> ControllerEpisodeApplication<ReqwestModelTransport, G>
where
    G: AgentToolGateway,
{
    /// Creates the production HTTPS composition with redirects, proxies, and retries disabled by
    /// the bounded model transport.
    ///
    /// # Errors
    ///
    /// Returns the same fail-closed configuration and recovery errors as [`Self::open`].
    pub fn open_https(
        spec: ControllerEpisodeSpec,
        catalog: RuntimeModelCatalog,
        codec_limits: CodecLimits,
        artifacts: Arc<dyn ArtifactStore>,
        database_path: impl AsRef<Path>,
        tools: G,
    ) -> Result<Self, ControllerEpisodeError> {
        Self::open(
            spec,
            catalog,
            ReqwestModelTransport::default(),
            codec_limits,
            artifacts,
            database_path,
            tools,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerEpisodeError(String);

impl ControllerEpisodeError {
    pub(super) fn adapter(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

impl Display for ControllerEpisodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ControllerEpisodeError {}

fn runtime_error(error: impl Display) -> ControllerEpisodeError {
    ControllerEpisodeError(error.to_string())
}

#[cfg(test)]
#[path = "episode_tests.rs"]
mod tests;
