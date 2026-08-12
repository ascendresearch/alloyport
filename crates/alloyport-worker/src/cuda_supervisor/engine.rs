//! Pluggable CUDA container-engine port and CUDA receipt facts.

pub use crate::container_engine::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream,
    ContainerLogs, ContainerPhase, ContainerSnapshot, EngineFuture,
};
use crate::cuda::DockerCreatePlan;
use crate::executor::ExecutorResult;
use std::fmt::Debug;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaExecutionFacts {
    pub container_name: String,
    pub bundle_digest: String,
    pub source_digest: String,
    pub image_digest: String,
    pub image_media_type: String,
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

impl crate::container_supervision::RunningContainerEngine for dyn CudaContainerEngine + '_ {
    fn wait<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ContainerExit> {
        CudaContainerEngine::wait(self, name)
    }

    fn stop<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()> {
        CudaContainerEngine::stop(self, name)
    }

    fn follow_logs_observed<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
        observer: &'a mut (dyn FnMut(ContainerLogChunk) + Send),
    ) -> EngineFuture<'a, ContainerLogs> {
        CudaContainerEngine::follow_logs_observed(self, name, limit, observer)
    }

    fn streams_live_log_observations(&self) -> bool {
        CudaContainerEngine::streams_live_log_observations(self)
    }
}

impl crate::container_supervision::ContainerReconcileEngine<DockerCreatePlan>
    for dyn CudaContainerEngine + '_
{
    fn inspect<'a>(&'a self, name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>> {
        CudaContainerEngine::inspect(self, name)
    }

    fn create<'a>(
        &'a self,
        plan: &'a DockerCreatePlan,
        identity: &'a ContainerIdentity,
    ) -> EngineFuture<'a, ()> {
        CudaContainerEngine::create(self, plan, identity)
    }
}
