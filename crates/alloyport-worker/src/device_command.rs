//! Bounded shell-free command execution shared by local accelerator probes.

use crate::device::DeviceStatusError;
use std::io::{self, Read};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(crate) struct BoundedCommandOutput {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub output_limit_exceeded: bool,
}

pub(crate) fn run_bounded_command(
    binary: &Path,
    arguments: &[&str],
    output_limit: u64,
    timeout: Duration,
) -> Result<BoundedCommandOutput, DeviceStatusError> {
    let mut child = Command::new(binary)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            DeviceStatusError::Unavailable(format!("failed to start {}: {error}", binary.display()))
        })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| DeviceStatusError::Internal("probe stdout pipe is missing".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| DeviceStatusError::Internal("probe stderr pipe is missing".into()))?;
    let stdout_task = thread::spawn(move || read_bounded(stdout, output_limit));
    let stderr_task = thread::spawn(move || read_bounded(stderr, output_limit));
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|error| {
            DeviceStatusError::Unavailable(format!("failed polling accelerator probe: {error}"))
        })? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = join_reader(stdout_task, "stdout");
            let _ = join_reader(stderr_task, "stderr");
            return Err(DeviceStatusError::Unavailable(format!(
                "accelerator probe exceeded its {timeout:?} timeout"
            )));
        }
        thread::sleep(Duration::from_millis(2));
    };
    let (stdout, stdout_exceeded) = join_reader(stdout_task, "stdout")?;
    let (stderr, stderr_exceeded) = join_reader(stderr_task, "stderr")?;
    let combined_exceeded = u64::try_from(stdout.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(stderr.len()).unwrap_or(u64::MAX))
        > output_limit;
    Ok(BoundedCommandOutput {
        success: status.success(),
        exit_code: status.code(),
        stdout,
        stderr,
        output_limit_exceeded: stdout_exceeded || stderr_exceeded || combined_exceeded,
    })
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

fn join_reader(
    task: thread::JoinHandle<io::Result<(Vec<u8>, bool)>>,
    stream: &str,
) -> Result<(Vec<u8>, bool), DeviceStatusError> {
    task.join()
        .map_err(|_| DeviceStatusError::Internal(format!("probe {stream} reader panicked")))?
        .map_err(|error| {
            DeviceStatusError::Unavailable(format!("failed reading probe {stream}: {error}"))
        })
}
