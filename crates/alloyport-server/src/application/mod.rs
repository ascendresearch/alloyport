//! Server process composition root.

mod assembly;
mod bootstrap;
mod candidate_bootstrap;
mod candidate_config;
mod candidate_episode;
mod candidate_record;
mod command;
mod config;
mod episode;
mod identity_admin;
mod migration_dispatcher;
mod runtime;

use std::error::Error;

pub use candidate_episode::{
    CandidateEpisodeApplication, CandidateEpisodeToolSpec, candidate_episode_tool_definitions,
    open_candidate_episode_https,
};
pub use episode::{ControllerEpisodeApplication, ControllerEpisodeError, ControllerEpisodeSpec};

/// Runs an offline identity command or starts the configured server process.
///
/// # Errors
///
/// Returns command, configuration, assembly, service, or shutdown failures to
/// the thin binary entry point.
pub async fn run_from_args() -> Result<(), Box<dyn Error>> {
    match command::ServerCommand::from_process_args()? {
        command::ServerCommand::Bootstrap { directory } => bootstrap::run(&directory),
        command::ServerCommand::Serve { config_path } => {
            let config = config::ServerConfig::load(config_path)?;
            let application = assembly::assemble(config).await?;
            runtime::run(application).await
        }
        command::ServerCommand::Identity {
            config_path,
            action,
        } => {
            let config = config::ServerConfig::load(config_path)?;
            identity_admin::run(action, &config.identity_database)
        }
        command::ServerCommand::CandidateEpisode {
            config_path,
            candidate_config_path,
            action,
        } => {
            let config = config::ServerConfig::load(config_path)?;
            candidate_bootstrap::run(config, candidate_config_path, action).await
        }
        command::ServerCommand::CandidateRecord {
            config_path,
            task_id,
            into,
        } => {
            let config = config::ServerConfig::load(config_path)?;
            candidate_record::run(&config, &task_id, &into)
        }
    }
}
