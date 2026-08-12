//! Ascend-specific container-engine port over shared transport-neutral values.

use crate::ascend::{AscendDockerCreatePlan, AscendEnvironmentFacts};
pub use crate::container_engine::{
    ContainerEngineError, ContainerExit, ContainerIdentity, ContainerLogChunk, ContainerLogStream,
    ContainerLogs, ContainerPhase, ContainerSnapshot, EngineFuture,
};
use crate::executor::ExecutorResult;
use alloyport_core::AcceleratorDevice;
use std::fmt::Debug;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AscendExecutionFacts {
    pub container_name: String,
    pub bundle_digest: String,
    pub source_digest: String,
    pub image_digest: String,
    pub image_media_type: String,
    pub image_id: String,
    pub device: AcceleratorDevice,
    pub environment: AscendEnvironmentFacts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisedAscendExecution {
    pub result: ExecutorResult,
    pub facts: AscendExecutionFacts,
    pub live_output_streaming: bool,
}

/// Local Ascend container operations. Implementations use argv and never a shell string.
pub trait AscendContainerEngine: Debug + Send + Sync {
    fn resolve_image_id<'a>(&'a self, plan: &'a AscendDockerCreatePlan)
    -> EngineFuture<'a, String>;
    fn inspect<'a>(&'a self, name: &'a str) -> EngineFuture<'a, Option<ContainerSnapshot>>;
    fn create<'a>(
        &'a self,
        plan: &'a AscendDockerCreatePlan,
        identity: &'a ContainerIdentity,
    ) -> EngineFuture<'a, ()>;
    fn start<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    fn wait<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ContainerExit>;
    fn stop<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
    fn logs<'a>(&'a self, name: &'a str, limit: u64) -> EngineFuture<'a, ContainerLogs>;
    fn follow_logs<'a>(&'a self, name: &'a str, limit: u64) -> EngineFuture<'a, ContainerLogs> {
        self.logs(name, limit)
    }
    fn follow_logs_observed<'a>(
        &'a self,
        name: &'a str,
        limit: u64,
        _observer: &'a mut (dyn FnMut(ContainerLogChunk) + Send),
    ) -> EngineFuture<'a, ContainerLogs> {
        self.follow_logs(name, limit)
    }
    fn streams_live_log_observations(&self) -> bool {
        false
    }
    fn remove<'a>(&'a self, name: &'a str) -> EngineFuture<'a, ()>;
}
