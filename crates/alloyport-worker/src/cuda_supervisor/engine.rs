//! Pluggable CUDA container-engine port and transport-neutral value objects.

use crate::backend_error::BackendError;
use crate::cuda::DockerCreatePlan;
use crate::executor::ExecutorResult;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub type EngineFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ContainerEngineError>> + Send + 'a>>;

/// Stable failure categories exposed by pluggable CUDA container engines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContainerEngineError {
    InvalidConfiguration(String),
    Unavailable(String),
    CommandFailed(String),
    InvalidResponse(String),
    Internal(String),
}

impl std::fmt::Display for ContainerEngineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration(detail) => {
                write!(
                    formatter,
                    "invalid container engine configuration: {detail}"
                )
            }
            Self::Unavailable(detail) => {
                write!(formatter, "container engine unavailable: {detail}")
            }
            Self::CommandFailed(detail) => write!(formatter, "container command failed: {detail}"),
            Self::InvalidResponse(detail) => {
                write!(formatter, "invalid container engine response: {detail}")
            }
            Self::Internal(detail) => {
                write!(formatter, "container engine internal failure: {detail}")
            }
        }
    }
}

impl std::error::Error for ContainerEngineError {}

impl From<ContainerEngineError> for BackendError {
    fn from(error: ContainerEngineError) -> Self {
        let detail = error.to_string();
        match error {
            ContainerEngineError::InvalidConfiguration(_) => Self::policy(detail),
            ContainerEngineError::Unavailable(_) => Self::retryable(detail),
            ContainerEngineError::CommandFailed(_) | ContainerEngineError::Internal(_) => {
                Self::terminal(detail)
            }
            ContainerEngineError::InvalidResponse(_) => Self::integrity(detail),
        }
    }
}

impl From<String> for ContainerEngineError {
    fn from(detail: String) -> Self {
        Self::Internal(detail)
    }
}

impl From<&str> for ContainerEngineError {
    fn from(detail: &str) -> Self {
        Self::Internal(detail.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerIdentity {
    pub name: String,
    pub attempt_id: String,
    pub bundle_digest: String,
    pub image_manifest_digest: String,
    pub image_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainerPhase {
    Created,
    Running,
    Exited,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerSnapshot {
    pub identity: ContainerIdentity,
    pub phase: ContainerPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerExit {
    pub exit_code: i32,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerLogs {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub output_limit_exceeded: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainerLogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerLogChunk {
    pub stream: ContainerLogStream,
    pub byte_offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaExecutionFacts {
    pub container_name: String,
    pub bundle_digest: String,
    pub source_digest: String,
    pub image_manifest_digest: String,
    pub image_id: String,
    pub device_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisedCudaExecution {
    pub result: ExecutorResult,
    pub facts: CudaExecutionFacts,
    pub live_output_streaming: bool,
}

/// Local container operations. Implementations must use argv, never a shell string.
pub trait CudaContainerEngine: Debug + Send + Sync {
    fn resolve_image_id<'a>(&'a self, plan: &'a DockerCreatePlan) -> EngineFuture<'a, String>;
    fn inspect<'a>(&'a self, name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>>;
    fn create<'a>(
        &'a self,
        plan: &'a DockerCreatePlan,
        identity: &'a ContainerIdentity,
    ) -> EngineFuture<'a, ()>;
    fn start<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    fn wait<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ContainerExit>;
    fn stop<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    /// Returns at most `limit` combined stdout/stderr bytes and reports whether more existed.
    fn logs<'a>(&'a self, name: &'a str, limit: u64) -> EngineFuture<'a, ContainerLogs>;
    /// Follows a running container and returns early when the combined output limit is exceeded.
    fn follow_logs<'a>(&'a self, name: &'a str, limit: u64) -> EngineFuture<'a, ContainerLogs> {
        self.logs(name, limit)
    }
    /// Follows logs while forwarding best-effort bounded chunks with per-stream offsets.
    fn follow_logs_observed<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
        _observer: &'a mut (dyn FnMut(ContainerLogChunk) + Send),
    ) -> EngineFuture<'a, ContainerLogs> {
        self.follow_logs(name, limit)
    }
    /// Reports that observed following owns preview emission, including intentional omissions.
    fn streams_live_log_observations(&self) -> bool {
        false
    }
    /// Removes a terminal container after publication and the terminal journal commit.
    fn remove<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
}
