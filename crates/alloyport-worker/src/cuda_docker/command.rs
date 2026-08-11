//! Bounded Docker CLI process execution and log following.

use crate::cuda_supervisor::{ContainerEngineError, ContainerLogChunk, ContainerLogStream};
use std::fmt::Debug;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
pub(super) struct SystemDockerCommandRunner {
    pub(super) binary: PathBuf,
}

pub(super) trait DockerCommandRunner: Debug + Send + Sync {
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
pub(super) struct DockerCommandOutput {
    pub(super) success: bool,
    pub(super) exit_code: Option<i32>,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: Vec<u8>,
    pub(super) output_limit_exceeded: bool,
}

pub(super) fn read_bounded(mut reader: impl Read, limit: u64) -> io::Result<(Vec<u8>, bool)> {
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

pub(super) fn read_follow_bounded(
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

pub(super) fn command_io_error(binary: &Path, error: &io::Error) -> ContainerEngineError {
    ContainerEngineError::Unavailable(format!(
        "failed to start Docker CLI {}: {error}",
        binary.display()
    ))
}

pub(super) fn require_success(
    action: &str,
    output: &DockerCommandOutput,
) -> Result<(), ContainerEngineError> {
    require_exit_success(action, output)?;
    if output.output_limit_exceeded {
        return Err(ContainerEngineError::InvalidResponse(format!(
            "Docker CLI output exceeded its limit while trying to {action}"
        )));
    }
    Ok(())
}

pub(super) fn require_exit_success(
    action: &str,
    output: &DockerCommandOutput,
) -> Result<(), ContainerEngineError> {
    if output.success {
        Ok(())
    } else {
        Err(command_failure(action, output))
    }
}

pub(super) fn command_failure(action: &str, output: &DockerCommandOutput) -> ContainerEngineError {
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
