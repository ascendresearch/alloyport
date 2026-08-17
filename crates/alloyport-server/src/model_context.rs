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

    /// Forwards validation, which this decorator silently swallowed for its whole existence.
    ///
    /// Nothing is recorded here: a rejected call never becomes a tool operation, so there is no
    /// result context to keep. The recording belongs to calls that actually ran.
    fn validate_call(
        &self,
        call: &alloyport_core::GatewayToolCall,
    ) -> Result<(), alloyport_core::ToolInputRejection> {
        self.inner.validate_call(call)
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_core::{GatewayToolCall, RuntimeToolDescriptor, ToolInputRejection};

    #[derive(Debug)]
    struct RejectingGateway;

    impl AgentToolGateway for RejectingGateway {
        fn descriptor(&self, _name: &str) -> Option<RuntimeToolDescriptor> {
            None
        }

        fn validate_call(&self, _call: &GatewayToolCall) -> Result<(), ToolInputRejection> {
            Err(ToolInputRejection {
                result_digest: Sha256Digest::digest_bytes(b"published-explanation"),
                diagnostic: "the inner gateway refused this call".to_owned(),
            })
        }

        fn execute<'a>(&'a mut self, _request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
            Box::pin(async { Err(ToolGatewayError::UnexpectedRequest) })
        }

        fn reconcile<'a>(&'a mut self, _request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
            Box::pin(async { Err(ToolGatewayError::UnexpectedRequest) })
        }
    }

    #[derive(Debug)]
    struct DiscardingSink;

    impl ModelToolResultSink for DiscardingSink {
        fn record_tool_result(
            &self,
            _episode_id: &EpisodeId,
            _native_call_id: &str,
            _result_digest: Sha256Digest,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    /// The decorator must not swallow validation.
    ///
    /// It forwarded `descriptor`, `execute`, and `reconcile` and inherited a defaulted
    /// `validate_call` returning `Ok(())`. Production wraps the real gateway in this type, so
    /// Design 0040's correction path — written after a malformed argument ended a paid migration —
    /// never ran on any real run, while its own tests passed against the unwrapped gateway.
    #[test]
    fn the_recording_decorator_forwards_validation_instead_of_swallowing_it() {
        let gateway = ContextRecordingToolGateway::new(
            RejectingGateway,
            EpisodeId::try_from("episode-decorator").expect("episode"),
            Arc::new(DiscardingSink),
        );
        let call = GatewayToolCall {
            native_call_id: "call-1".to_owned(),
            name: "submit_candidate_bundle".to_owned(),
            raw_arguments: b"{}".to_vec(),
        };
        let rejection = gateway
            .validate_call(&call)
            .expect_err("a decorator must not turn a refusal into permission");
        assert_eq!(rejection.diagnostic, "the inner gateway refused this call");
    }
}
