//! Server process composition root.

mod assembly;
mod config;
mod identity_admin;
mod runtime;

use std::error::Error;

/// Runs an offline identity command or starts the configured server process.
///
/// # Errors
///
/// Returns command, configuration, assembly, service, or shutdown failures to
/// the thin binary entry point.
pub async fn run_from_args() -> Result<(), Box<dyn Error>> {
    if identity_admin::try_run_from_args()? {
        return Ok(());
    }
    let config = config::ServerConfig::from_environment()?;
    let application = assembly::assemble(config).await?;
    runtime::run(application).await
}
