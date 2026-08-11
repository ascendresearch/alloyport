//! Argv-only Docker CLI adapter for the durable CUDA container supervisor.

use crate::cuda::DockerCreatePlan;
use crate::cuda_supervisor::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream,
    ContainerLogs, ContainerSnapshot, CudaContainerEngine,
};
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

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

#[derive(Debug)]
struct SystemDockerCommandRunner {
    binary: PathBuf,
}

trait DockerCommandRunner: Debug + Send + Sync {
    fn run(
        &self,
        arguments: &[String],
        output_limit: u64,
    ) -> Result<DockerCommandOutput, ContainerEngineError>;

    fn follow(
        &self,
        arguments: &[String],
        output_limit: u64,
        _preview: Option<tokio::sync::mpsc::Sender<ContainerLogChunk>>,
    ) -> Result<DockerCommandOutput, ContainerEngineError> {
        self.run(arguments, output_limit)
    }
}

impl DockerCommandRunner for SystemDockerCommandRunner {
    fn run(
        &self,
        arguments: &[String],
        output_limit: u64,
    ) -> Result<DockerCommandOutput, ContainerEngineError> {
        let mut child = Command::new(&self.binary)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| command_io_error(&self.binary, &error))?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ContainerEngineError::Internal("Docker CLI stdout pipe is missing".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            ContainerEngineError::Internal("Docker CLI stderr pipe is missing".into())
        })?;
        let stdout_task = thread::spawn(move || read_bounded(stdout, output_limit));
        let stderr_task = thread::spawn(move || read_bounded(stderr, output_limit));
        let status = child.wait().map_err(|error| {
            ContainerEngineError::Unavailable(format!("failed waiting for Docker CLI: {error}"))
        })?;
        let (stdout, stdout_exceeded) = join_reader(stdout_task, "stdout")?;
        let (stderr, stderr_exceeded) = join_reader(stderr_task, "stderr")?;
        let combined_exceeded = u64::try_from(stdout.len())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(stderr.len()).unwrap_or(u64::MAX))
            > output_limit;
        Ok(DockerCommandOutput {
            success: status.success(),
            exit_code: status.code(),
            stdout,
            stderr,
            output_limit_exceeded: stdout_exceeded || stderr_exceeded || combined_exceeded,
        })
    }

    fn follow(
        &self,
        arguments: &[String],
        output_limit: u64,
        preview: Option<tokio::sync::mpsc::Sender<ContainerLogChunk>>,
    ) -> Result<DockerCommandOutput, ContainerEngineError> {
        follow_command(&self.binary, arguments, output_limit, preview)
    }
}

fn follow_command(
    binary: &Path,
    arguments: &[String],
    output_limit: u64,
    preview: Option<tokio::sync::mpsc::Sender<ContainerLogChunk>>,
) -> Result<DockerCommandOutput, ContainerEngineError> {
    let mut child = Command::new(binary)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| command_io_error(binary, &error))?;
    let stdout = child.stdout.take().ok_or_else(|| {
        ContainerEngineError::Internal("Docker CLI stdout pipe is missing".into())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        ContainerEngineError::Internal("Docker CLI stderr pipe is missing".into())
    })?;
    let total = Arc::new(AtomicU64::new(0));
    let (limit_sender, limit_receiver) = mpsc::sync_channel(1);
    let stdout_total = Arc::clone(&total);
    let stdout_limit_sender = limit_sender.clone();
    let stdout_preview = preview.clone();
    let stdout_task = thread::spawn(move || {
        read_follow_bounded(
            stdout,
            output_limit,
            &stdout_total,
            &stdout_limit_sender,
            ContainerLogStream::Stdout,
            stdout_preview.as_ref(),
        )
    });
    let stderr_total = Arc::clone(&total);
    let stderr_task = thread::spawn(move || {
        read_follow_bounded(
            stderr,
            output_limit,
            &stderr_total,
            &limit_sender,
            ContainerLogStream::Stderr,
            preview.as_ref(),
        )
    });
    let mut killed_for_limit = false;
    let status = loop {
        if limit_receiver.try_recv().is_ok() {
            killed_for_limit = true;
            let _ = child.kill();
            break child.wait().map_err(|error| {
                ContainerEngineError::Unavailable(format!(
                    "failed waiting for Docker log follower: {error}"
                ))
            })?;
        }
        if let Some(status) = child.try_wait().map_err(|error| {
            ContainerEngineError::Unavailable(format!(
                "failed polling Docker log follower: {error}"
            ))
        })? {
            break status;
        }
        thread::sleep(Duration::from_millis(2));
    };
    let (stdout, stdout_exceeded) = join_reader(stdout_task, "stdout")?;
    let (stderr, stderr_exceeded) = join_reader(stderr_task, "stderr")?;
    let combined_exceeded = total.load(Ordering::Relaxed) > output_limit;
    Ok(DockerCommandOutput {
        success: status.success() || killed_for_limit,
        exit_code: status.code(),
        stdout,
        stderr,
        output_limit_exceeded: killed_for_limit
            || stdout_exceeded
            || stderr_exceeded
            || combined_exceeded,
    })
}

#[derive(Debug)]
struct DockerCommandOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_limit_exceeded: bool,
}

fn read_bounded(mut reader: impl Read, limit: u64) -> io::Result<(Vec<u8>, bool)> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        let keep = remaining.min(read);
        bytes.extend_from_slice(&buffer[..keep]);
        exceeded |= keep < read;
    }
    Ok((bytes, exceeded))
}

fn read_follow_bounded(
    mut reader: impl Read,
    limit: u64,
    total: &AtomicU64,
    limit_sender: &mpsc::SyncSender<()>,
    stream: ContainerLogStream,
    preview: Option<&tokio::sync::mpsc::Sender<ContainerLogChunk>>,
) -> io::Result<(Vec<u8>, bool)> {
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).unwrap_or(u64::MAX);
        let previous = total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(read_u64))
            })
            .unwrap_or_else(|value| value);
        if previous.saturating_add(read_u64) > limit {
            exceeded = true;
            let _ = limit_sender.try_send(());
        }
        let globally_remaining = limit.saturating_sub(previous);
        let keep = usize::try_from(globally_remaining)
            .unwrap_or(usize::MAX)
            .min(read);
        let byte_offset = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        bytes.extend_from_slice(&buffer[..keep]);
        if keep > 0
            && let Some(preview) = preview
        {
            let _ = preview.try_send(ContainerLogChunk {
                stream,
                byte_offset,
                bytes: buffer[..keep].to_vec(),
            });
        }
        exceeded |= keep < read;
    }
    Ok((bytes, exceeded))
}

fn join_reader(
    task: thread::JoinHandle<io::Result<(Vec<u8>, bool)>>,
    stream: &str,
) -> Result<(Vec<u8>, bool), ContainerEngineError> {
    task.join()
        .map_err(|_| {
            ContainerEngineError::Internal(format!("Docker CLI {stream} reader panicked"))
        })?
        .map_err(|error| {
            ContainerEngineError::Unavailable(format!(
                "failed reading Docker CLI {stream}: {error}"
            ))
        })
}

fn command_io_error(binary: &Path, error: &io::Error) -> ContainerEngineError {
    ContainerEngineError::Unavailable(format!(
        "failed to start Docker CLI {}: {error}",
        binary.display()
    ))
}

fn require_success(action: &str, output: &DockerCommandOutput) -> Result<(), ContainerEngineError> {
    require_exit_success(action, output)?;
    if output.output_limit_exceeded {
        return Err(ContainerEngineError::InvalidResponse(format!(
            "Docker CLI output exceeded its limit while trying to {action}"
        )));
    }
    Ok(())
}

fn require_exit_success(
    action: &str,
    output: &DockerCommandOutput,
) -> Result<(), ContainerEngineError> {
    if output.success {
        Ok(())
    } else {
        Err(command_failure(action, output))
    }
}

fn command_failure(action: &str, output: &DockerCommandOutput) -> ContainerEngineError {
    let detail = String::from_utf8_lossy(&output.stderr);
    let detail = detail.trim();
    if detail.is_empty() {
        ContainerEngineError::CommandFailed(format!(
            "Docker CLI failed to {action} with exit status {:?}",
            output.exit_code
        ))
    } else {
        ContainerEngineError::CommandFailed(format!("Docker CLI failed to {action}: {detail}"))
    }
}

#[cfg(test)]
#[path = "cuda_docker_tests.rs"]
mod tests;
