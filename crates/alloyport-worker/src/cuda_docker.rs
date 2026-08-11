//! Argv-only Docker CLI adapter for the durable CUDA container supervisor.

use crate::cuda::DockerCreatePlan;
use crate::cuda_supervisor::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogs,
    ContainerSnapshot, CudaContainerEngine,
};
use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;
use std::sync::Arc;

mod command;
use command::{
    DockerCommandOutput, DockerCommandRunner, SystemDockerCommandRunner, command_failure,
    require_exit_success, require_success,
};
#[cfg(test)]
use command::{command_io_error, read_bounded, read_follow_bounded};

#[path = "cuda_docker_protocol.rs"]
mod protocol;
use protocol::{
    DockerContainerDetail, parse_container_inspect, parse_image_id, parse_wait_exit_code,
};

const METADATA_OUTPUT_LIMIT: u64 = 1024 * 1024;
const LOG_PREVIEW_CHANNEL_CAPACITY: usize = 16;

/// Docker CLI implementation used by the CUDA supervisor.
#[derive(Clone)]
pub struct DockerCliEngine {
    runner: Arc<dyn DockerCommandRunner>,
    stop_timeout_seconds: u32,
}

impl DockerCliEngine {
    /// Creates an engine using a trusted worker-local Docker CLI path.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured path is empty.
    pub fn new(binary: impl Into<PathBuf>) -> Result<Self, ContainerEngineError> {
        let binary = binary.into();
        if binary.as_os_str().is_empty() {
            return Err(ContainerEngineError::InvalidConfiguration(
                "Docker CLI path is empty".into(),
            ));
        }
        Ok(Self {
            runner: Arc::new(SystemDockerCommandRunner { binary }),
            stop_timeout_seconds: 10,
        })
    }

    /// Sets the grace period passed to `docker container stop`.
    #[must_use]
    pub const fn with_stop_timeout_seconds(mut self, seconds: u32) -> Self {
        self.stop_timeout_seconds = seconds;
        self
    }

    async fn command(
        &self,
        arguments: Vec<String>,
        output_limit: u64,
    ) -> Result<DockerCommandOutput, ContainerEngineError> {
        let runner = Arc::clone(&self.runner);
        tokio::task::spawn_blocking(move || runner.run(&arguments, output_limit))
            .await
            .map_err(|error| {
                ContainerEngineError::Internal(format!("Docker CLI task failed: {error}"))
            })?
    }

    async fn follow_command(
        &self,
        arguments: Vec<String>,
        output_limit: u64,
        preview: Option<tokio::sync::mpsc::Sender<ContainerLogChunk>>,
    ) -> Result<DockerCommandOutput, ContainerEngineError> {
        let runner = Arc::clone(&self.runner);
        tokio::task::spawn_blocking(move || runner.follow(&arguments, output_limit, preview))
            .await
            .map_err(|error| {
                ContainerEngineError::Internal(format!("Docker CLI follow task failed: {error}"))
            })?
    }

    async fn inspect_detail(
        &self,
        name: &str,
    ) -> Result<Option<DockerContainerDetail>, ContainerEngineError> {
        let inspect = self
            .command(
                vec!["container".into(), "inspect".into(), name.into()],
                METADATA_OUTPUT_LIMIT,
            )
            .await?;
        if inspect.success {
            require_success("inspect container", &inspect)?;
            return parse_container_inspect(&inspect.stdout).map(Some);
        }

        let list = self
            .command(
                vec![
                    "container".into(),
                    "list".into(),
                    "--all".into(),
                    "--filter".into(),
                    format!("name=^/{name}$"),
                    "--format".into(),
                    "{{.Names}}".into(),
                ],
                METADATA_OUTPUT_LIMIT,
            )
            .await?;
        require_success("probe container existence", &list)?;
        if String::from_utf8_lossy(&list.stdout).trim().is_empty() {
            Ok(None)
        } else {
            Err(command_failure("inspect container", &inspect))
        }
    }
}

impl Default for DockerCliEngine {
    fn default() -> Self {
        Self::new("docker").expect("the default Docker CLI path is nonempty")
    }
}

impl Debug for DockerCliEngine {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DockerCliEngine")
            .field("runner", &self.runner)
            .field("stop_timeout_seconds", &self.stop_timeout_seconds)
            .finish()
    }
}

impl CudaContainerEngine for DockerCliEngine {
    fn resolve_image_id<'a>(
        &'a self,
        plan: &'a DockerCreatePlan,
    ) -> crate::cuda_supervisor::EngineFuture<'a, String> {
        Box::pin(async move {
            let output = self
                .command(
                    vec![
                        "image".into(),
                        "inspect".into(),
                        plan.image_reference.clone(),
                    ],
                    METADATA_OUTPUT_LIMIT,
                )
                .await?;
            require_success("inspect image", &output)?;
            parse_image_id(&output.stdout)
        })
    }

    fn inspect<'a>(
        &'a self,
        name: &'a str,
    ) -> crate::cuda_supervisor::EngineFuture<'a, Option<ContainerSnapshot>> {
        Box::pin(async move {
            Ok(self
                .inspect_detail(name)
                .await?
                .map(|detail| detail.snapshot))
        })
    }

    fn create<'a>(
        &'a self,
        plan: &'a DockerCreatePlan,
        _identity: &'a ContainerIdentity,
    ) -> crate::cuda_supervisor::EngineFuture<'a, ()> {
        Box::pin(async move {
            let output = self
                .command(plan.argv.clone(), METADATA_OUTPUT_LIMIT)
                .await?;
            require_success("create container", &output)
        })
    }

    fn start<'a>(&'a self, name: &'a str) -> crate::cuda_supervisor::EngineFuture<'a, ()> {
        Box::pin(async move {
            let output = self
                .command(
                    vec!["container".into(), "start".into(), name.into()],
                    METADATA_OUTPUT_LIMIT,
                )
                .await?;
            require_success("start container", &output)
        })
    }

    fn wait<'a>(
        &'a self,
        name: &'a str,
    ) -> crate::cuda_supervisor::EngineFuture<'a, ContainerExit> {
        Box::pin(async move {
            let output = self
                .command(
                    vec!["container".into(), "wait".into(), name.into()],
                    METADATA_OUTPUT_LIMIT,
                )
                .await?;
            require_success("wait for container", &output)?;
            let waited_exit = parse_wait_exit_code(&output.stdout)?;
            let detail = self.inspect_detail(name).await?.ok_or_else(|| {
                ContainerEngineError::InvalidResponse(format!(
                    "container {name} disappeared after wait"
                ))
            })?;
            let exit = detail.exit.ok_or_else(|| {
                ContainerEngineError::InvalidResponse(format!(
                    "container {name} is not exited after wait"
                ))
            })?;
            if exit.exit_code != waited_exit {
                return Err(ContainerEngineError::InvalidResponse(format!(
                    "container {name} wait returned {waited_exit}, but inspect returned {}",
                    exit.exit_code
                )));
            }
            Ok(exit)
        })
    }

    fn stop<'a>(&'a self, name: &'a str) -> crate::cuda_supervisor::EngineFuture<'a, ()> {
        Box::pin(async move {
            let output = self
                .command(
                    vec![
                        "container".into(),
                        "stop".into(),
                        "--time".into(),
                        self.stop_timeout_seconds.to_string(),
                        name.into(),
                    ],
                    METADATA_OUTPUT_LIMIT,
                )
                .await?;
            require_success("stop container", &output)
        })
    }

    fn logs<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
    ) -> crate::cuda_supervisor::EngineFuture<'a, ContainerLogs> {
        Box::pin(async move {
            let output = self
                .command(vec!["container".into(), "logs".into(), name.into()], limit)
                .await?;
            require_exit_success("read container logs", &output)?;
            Ok(ContainerLogs {
                stdout: output.stdout,
                stderr: output.stderr,
                output_limit_exceeded: output.output_limit_exceeded,
            })
        })
    }

    fn follow_logs<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
    ) -> crate::cuda_supervisor::EngineFuture<'a, ContainerLogs> {
        Box::pin(async move {
            let output = self
                .follow_command(
                    vec![
                        "container".into(),
                        "logs".into(),
                        "--follow".into(),
                        name.into(),
                    ],
                    limit,
                    None,
                )
                .await?;
            require_exit_success("follow container logs", &output)?;
            Ok(ContainerLogs {
                stdout: output.stdout,
                stderr: output.stderr,
                output_limit_exceeded: output.output_limit_exceeded,
            })
        })
    }

    fn follow_logs_observed<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
        observer: &'a mut (dyn FnMut(ContainerLogChunk) + Send),
    ) -> crate::cuda_supervisor::EngineFuture<'a, ContainerLogs> {
        Box::pin(async move {
            let (preview_sender, mut preview_receiver) =
                tokio::sync::mpsc::channel(LOG_PREVIEW_CHANNEL_CAPACITY);
            let command = self.follow_command(
                vec![
                    "container".into(),
                    "logs".into(),
                    "--follow".into(),
                    name.into(),
                ],
                limit,
                Some(preview_sender),
            );
            tokio::pin!(command);
            let output = loop {
                tokio::select! {
                    output = &mut command => break output?,
                    preview = preview_receiver.recv() => {
                        if let Some(preview) = preview {
                            observer(preview);
                        } else {
                            break command.await?;
                        }
                    }
                }
            };
            while let Ok(preview) = preview_receiver.try_recv() {
                observer(preview);
            }
            require_exit_success("follow container logs", &output)?;
            Ok(ContainerLogs {
                stdout: output.stdout,
                stderr: output.stderr,
                output_limit_exceeded: output.output_limit_exceeded,
            })
        })
    }

    fn streams_live_log_observations(&self) -> bool {
        true
    }

    fn remove<'a>(&'a self, name: &'a str) -> crate::cuda_supervisor::EngineFuture<'a, ()> {
        Box::pin(async move {
            let output = self
                .command(
                    vec!["container".into(), "rm".into(), name.into()],
                    METADATA_OUTPUT_LIMIT,
                )
                .await?;
            if output.success {
                return require_success("remove container", &output);
            }
            if self.inspect_detail(name).await?.is_none() {
                Ok(())
            } else {
                Err(command_failure("remove container", &output))
            }
        })
    }
}

#[cfg(test)]
#[path = "cuda_docker_tests.rs"]
mod tests;
