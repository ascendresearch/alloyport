//! Worker process composition root.
//!
//! Configuration parsing, dependency assembly, and long-running process
//! lifecycle are deliberately separate. Execution and control-plane modules
//! remain topology-agnostic and do not read process arguments or environment.

mod assembly;
mod backend_config;
mod bootstrap;
mod build_config;
mod config;
mod correctness_config;
mod runtime;

use std::error::Error;

/// Loads the worker configuration, assembles its local adapters, and runs it
/// until shutdown.
///
/// # Errors
///
/// Returns configuration, startup preflight, persistence, transport, or
/// shutdown errors to the thin binary entry point.
pub async fn run_from_args() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "bootstrap")
    {
        return bootstrap::run(&arguments[1..]).await;
    }
    let config = config::WorkerFileConfig::load_from_arguments(arguments)?.into_loaded()?;
    let worker = assembly::assemble(config).await?;
    runtime::run(worker).await
}
