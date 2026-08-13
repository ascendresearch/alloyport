//! Explicit operator bootstrap for one production Candidate Episode.

use super::candidate_config::CandidateEpisodeConfig;
use super::command::CandidateEpisodeAction;
use super::{assembly, config, open_candidate_episode_https, runtime};
use std::error::Error;
use std::path::PathBuf;

pub(super) async fn run(
    server_config: config::ServerConfig,
    candidate_config_path: PathBuf,
    action: CandidateEpisodeAction,
) -> Result<(), Box<dyn Error>> {
    let candidate = CandidateEpisodeConfig::load(candidate_config_path)?;
    candidate.preflight_provider().await?;
    if action == CandidateEpisodeAction::Validate {
        println!(
            "Candidate Episode configuration, model credential, migration inputs, and worker policies are valid"
        );
        return Ok(());
    }

    let server = assembly::assemble(server_config).await?;
    let episode = open_candidate_episode_https(
        candidate.episode,
        candidate.catalog,
        candidate.codec_limits,
        server.artifacts.clone(),
        candidate.database,
        server.control.clone(),
        candidate.tools,
    )?;
    let runtime = runtime::CandidateRuntime {
        application: episode,
        control: server.control.clone(),
        required_workers: candidate.required_workers,
        poll_interval: candidate.worker_poll_interval,
        ready_timeout: candidate.worker_ready_timeout,
    };
    runtime::run_candidate(server, runtime).await
}
