//! Bridge from terminal tool outcomes into the next provider turn's durable context.

use alloyport_core::{
    AgentToolGateway, EpisodeId, Sha256Digest, ToolGatewayError, ToolGatewayFuture,
    ToolGatewayOutcome, ToolInvocation,
};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Durable sink for one model-visible terminal tool result.
pub trait ModelToolResultSink: Debug + Send + Sync {
    /// Binds a result Artifact to the native call that produced it.
    ///
    /// # Errors
    ///
    /// Returns a sanitized diagnostic when the call is not pending or persistence fails.
    fn record_tool_result(
        &self,
        episode_id: &EpisodeId,
        native_call_id: &str,
        result_digest: Sha256Digest,
    ) -> Result<(), String>;
}

/// Tool gateway decorator that records a completed result before the reducer observes success.
pub struct ContextRecordingToolGateway<T> {
    inner: T,
    episode_id: EpisodeId,
    results: Arc<dyn ModelToolResultSink>,
}

impl<T> ContextRecordingToolGateway<T> {
    #[must_use]
    pub fn new(inner: T, episode_id: EpisodeId, results: Arc<dyn ModelToolResultSink>) -> Self {
        Self {
            inner,
            episode_id,
            results,
        }
    }

    #[must_use]
    pub const fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: Debug> Debug for ContextRecordingToolGateway<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextRecordingToolGateway")
            .field("inner", &self.inner)
            .field("episode_id", &self.episode_id)
            .finish_non_exhaustive()
    }
}

impl<T: AgentToolGateway> AgentToolGateway for ContextRecordingToolGateway<T> {
    fn descriptor(&self, name: &str) -> Option<alloyport_core::RuntimeToolDescriptor> {
        self.inner.descriptor(name)
    }

    fn execute<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
        Box::pin(async move {
            let outcome = self.inner.execute(request).await?;
            self.record(request, outcome)
        })
    }

    fn reconcile<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
        Box::pin(async move {
            let outcome = self.inner.reconcile(request).await?;
            self.record(request, outcome)
        })
    }
}

impl<T> ContextRecordingToolGateway<T> {
    fn record(
        &self,
        request: &ToolInvocation,
        outcome: ToolGatewayOutcome,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        if let ToolGatewayOutcome::Completed { result_digest, .. } = &outcome {
            self.results
                .record_tool_result(
                    &self.episode_id,
                    &request.call.native_call_id,
                    *result_digest,
                )
                .map_err(ToolGatewayError::Adapter)?;
        }
        Ok(outcome)
    }
}
