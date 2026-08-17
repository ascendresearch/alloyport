//! Provider-neutral SDK that composes model catalogs, protocol codecs, and one-shot transports.

use alloyport_core::{
    AnthropicMessagesCodec, CodecError, CodecLimits, CodecToolDefinition, GatewayTurnExchange,
    ModelCatalogError, ModelGateway, ModelGatewayError, ModelGatewayFuture, ModelGatewayOutcome,
    ModelTransport, ModelTransportOutcome, ModelTransportRetryHint, ModelTurnRequest,
    ModelVisibleToolResult, NativeContinuation, NativeTurnInput, OpenAiChatCompletionsCodec,
    OpenAiResponsesCodec, PreparedModelPayload, ProtocolCodec, RawModelResponse,
    RawModelResponseRef, ResolvedRuntimeModel, RuntimeModelCatalog, Sha256Digest,
};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

pub use alloyport_model_http::ReqwestModelTransport;

/// Owned provider-neutral input loaded for one durable model attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct ProviderTurnInput {
    pub system_prompt: String,
    pub initial_user_text: Option<String>,
    pub continuation: Option<NativeContinuation>,
    pub tool_results: Vec<OwnedToolResult>,
    pub tools: Vec<CodecToolDefinition>,
}

/// Owned tool result correlated to an ID emitted by the selected protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedToolResult {
    pub native_call_id: String,
    pub output: String,
}

/// Exact native exchange and continuation produced by one successful SDK invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct ProviderTurnExchange {
    pub gateway_exchange: GatewayTurnExchange,
    pub request_body: Vec<u8>,
    pub response_body: Vec<u8>,
    pub native_continuation: NativeContinuation,
    pub provider_request_id: Option<String>,
    pub actual_model: Option<String>,
}

/// Complete SDK outcome. Dispatch certainty is preserved for the durable agent reducer.
#[derive(Clone, Debug, PartialEq)]
pub enum ProviderSdkOutcome {
    Turn(Box<ProviderTurnExchange>),
    ConfirmedNotSent {
        diagnostic: String,
    },
    Rejected {
        response_digest: Sha256Digest,
        diagnostic: String,
        retryable: bool,
    },
    DecodeFailed {
        response_digest: Sha256Digest,
        diagnostic: String,
    },
    Ambiguous {
        diagnostic: String,
    },
}

/// Pluggable provider SDK. It performs exactly one transport dispatch and never executes tools.
#[derive(Debug)]
pub struct LlmProviderSdk<T> {
    catalog: RuntimeModelCatalog,
    transport: T,
    codec_limits: CodecLimits,
}

impl<T> LlmProviderSdk<T>
where
    T: ModelTransport,
{
    /// Creates an SDK only after the complete catalog and codec limits validate.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid catalog or disabled codec bound.
    pub fn new(
        catalog: RuntimeModelCatalog,
        transport: T,
        codec_limits: CodecLimits,
    ) -> Result<Self, ProviderSdkError> {
        catalog.validate().map_err(ProviderSdkError::Catalog)?;
        codec_limits.validate().map_err(ProviderSdkError::Codec)?;
        Ok(Self {
            catalog,
            transport,
            codec_limits,
        })
    }

    /// Resolves an alias through configuration without inspecting its vendor or wire-model string.
    ///
    /// # Errors
    ///
    /// Returns an error when the alias does not resolve through the validated catalog.
    pub fn resolve(
        &self,
        runtime_model_alias: Option<&str>,
    ) -> Result<ResolvedRuntimeModel, ProviderSdkError> {
        self.catalog
            .resolve(runtime_model_alias)
            .map_err(ProviderSdkError::Catalog)
    }

    /// Executes one provider turn using the resolved protocol codec and bounded transport.
    #[must_use]
    pub async fn invoke(
        &self,
        deployment: &ResolvedRuntimeModel,
        input: &ProviderTurnInput,
    ) -> ProviderSdkOutcome {
        let codec = match codec_for(deployment.protocol(), self.codec_limits) {
            Ok(codec) => codec,
            Err(error) => return confirmed_not_sent(error.to_string()),
        };
        let continuation = match continuation_with_results(codec.as_ref(), input) {
            Ok(continuation) => continuation,
            Err(error) => return confirmed_not_sent(error.to_string()),
        };
        let prepared = match codec.prepare(NativeTurnInput {
            wire_model: deployment.wire_model(),
            system_prompt: &input.system_prompt,
            initial_user_text: input.initial_user_text.as_deref(),
            continuation: continuation.as_ref(),
            tools: &input.tools,
            max_output_tokens: deployment.max_output_tokens(),
            reasoning_effort: deployment.reasoning_effort(),
            reasoning_mode: deployment.reasoning_mode(),
        }) {
            Ok(prepared) => prepared,
            Err(error) => return confirmed_not_sent(error.to_string()),
        };
        match self.transport.dispatch(deployment, &prepared).await {
            ModelTransportOutcome::Response(response) => {
                decode(codec.as_ref(), &prepared, response)
            }
            ModelTransportOutcome::ConfirmedNotSent(failure) => {
                ProviderSdkOutcome::ConfirmedNotSent {
                    diagnostic: failure.diagnostic,
                }
            }
            ModelTransportOutcome::ProviderRejected { response, failure } => {
                ProviderSdkOutcome::Rejected {
                    response_digest: Sha256Digest::digest_bytes(&response.body),
                    diagnostic: failure.diagnostic,
                    retryable: failure.retry_hint != ModelTransportRetryHint::Never,
                }
            }
            ModelTransportOutcome::Ambiguous { failure, .. } => ProviderSdkOutcome::Ambiguous {
                diagnostic: failure.diagnostic,
            },
        }
    }
}

fn codec_for(
    protocol: &alloyport_core::ProtocolConfig,
    limits: CodecLimits,
) -> Result<Box<dyn ProtocolCodec>, CodecError> {
    match protocol {
        alloyport_core::ProtocolConfig::OpenAiResponses { .. } => {
            Ok(Box::new(OpenAiResponsesCodec::new(limits)?))
        }
        alloyport_core::ProtocolConfig::OpenAiChatCompletions { thinking_parameter } => {
            Ok(Box::new(
                OpenAiChatCompletionsCodec::new(limits)?
                    .with_thinking_parameter(*thinking_parameter),
            ))
        }
        alloyport_core::ProtocolConfig::AnthropicMessages { .. } => {
            Ok(Box::new(AnthropicMessagesCodec::new(limits)?))
        }
    }
}

fn continuation_with_results(
    codec: &dyn ProtocolCodec,
    input: &ProviderTurnInput,
) -> Result<Option<NativeContinuation>, CodecError> {
    let Some(continuation) = &input.continuation else {
        if input.tool_results.is_empty() {
            return Ok(None);
        }
        return Err(CodecError::ToolResultMismatch);
    };
    if input.tool_results.is_empty() {
        return Ok(Some(continuation.clone()));
    }
    let results: Vec<_> = input
        .tool_results
        .iter()
        .map(|result| ModelVisibleToolResult {
            native_call_id: &result.native_call_id,
            output: &result.output,
        })
        .collect();
    codec.append_tool_results(continuation, &results).map(Some)
}

fn decode(
    codec: &dyn ProtocolCodec,
    prepared: &PreparedModelPayload,
    response: RawModelResponse,
) -> ProviderSdkOutcome {
    let response_digest = Sha256Digest::digest_bytes(&response.body);
    let decoded = match codec.decode(
        prepared,
        RawModelResponseRef {
            body: &response.body,
        },
    ) {
        Ok(decoded) => decoded,
        Err(error) => {
            return ProviderSdkOutcome::DecodeFailed {
                response_digest,
                diagnostic: error.to_string(),
            };
        }
    };
    let continuation_digest = match decoded.native_continuation.digest() {
        Ok(digest) => digest,
        Err(error) => {
            return ProviderSdkOutcome::DecodeFailed {
                response_digest,
                diagnostic: error.to_string(),
            };
        }
    };
    ProviderSdkOutcome::Turn(Box::new(ProviderTurnExchange {
        gateway_exchange: GatewayTurnExchange {
            turn: decoded.turn,
            raw_exchange_digest: response_digest,
            native_continuation_digest: continuation_digest,
        },
        request_body: prepared.body().to_vec(),
        response_body: response.body,
        native_continuation: decoded.native_continuation,
        provider_request_id: response.provider_request_id,
        actual_model: decoded.actual_model,
    }))
}

fn confirmed_not_sent(diagnostic: String) -> ProviderSdkOutcome {
    ProviderSdkOutcome::ConfirmedNotSent { diagnostic }
}

/// Supplies exact turn inputs and durably commits successful native exchanges for the agent loop.
pub trait ModelTurnContextStore: Debug + Send {
    /// Loads the input whose digest is present in the durable model-attempt request.
    ///
    /// # Errors
    ///
    /// Returns a sanitized diagnostic when the exact input cannot be loaded.
    fn load(&mut self, request: &ModelTurnRequest) -> Result<ProviderTurnInput, String>;

    /// Commits raw bytes and native continuation before the semantic turn is returned to the loop.
    ///
    /// # Errors
    ///
    /// Returns a sanitized diagnostic when the exact exchange cannot be committed durably.
    fn commit(
        &mut self,
        request: &ModelTurnRequest,
        exchange: &ProviderTurnExchange,
    ) -> Result<(), String>;

    /// Publishes a failure diagnostic so the durable record names bytes that exist.
    ///
    /// A successful exchange has its request and response bodies ingested by `commit`; a failed one
    /// has no exchange, so its explanation was previously only hashed by the reducer and never
    /// stored. A run then died on 21 identical dispatch failures with nothing left to read.
    ///
    /// # Errors
    ///
    /// Returns a sanitized diagnostic when the bytes cannot be stored.
    fn publish_diagnostic(&mut self, text: &str) -> Result<Sha256Digest, String>;
}

/// Concrete `ModelGateway` used by the agent loop; all provider behavior remains inside the SDK.
#[derive(Debug)]
pub struct ProviderModelGateway<S, T> {
    sdk: LlmProviderSdk<T>,
    deployment: ResolvedRuntimeModel,
    contexts: S,
}

impl<S, T> ProviderModelGateway<S, T>
where
    S: ModelTurnContextStore,
    T: ModelTransport,
{
    /// Pins one resolved runtime model for the lifetime of this gateway/episode composition.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested alias is absent from the SDK catalog.
    pub fn new(
        sdk: LlmProviderSdk<T>,
        runtime_model_alias: Option<&str>,
        contexts: S,
    ) -> Result<Self, ProviderSdkError> {
        let deployment = sdk.resolve(runtime_model_alias)?;
        Ok(Self {
            sdk,
            deployment,
            contexts,
        })
    }

    #[must_use]
    pub const fn deployment(&self) -> &ResolvedRuntimeModel {
        &self.deployment
    }

    #[must_use]
    pub const fn contexts(&self) -> &S {
        &self.contexts
    }
}

impl<S, T> ModelGateway for ProviderModelGateway<S, T>
where
    S: ModelTurnContextStore,
    T: ModelTransport,
{
    fn invoke<'a>(&'a mut self, request: &'a ModelTurnRequest) -> ModelGatewayFuture<'a> {
        Box::pin(async move {
            let input = match self.contexts.load(request) {
                Ok(input) => input,
                Err(diagnostic) => {
                    let diagnostic_digest = self.contexts.publish_diagnostic(&diagnostic).ok();
                    return Ok(ModelGatewayOutcome::ConfirmedNotSent {
                        diagnostic,
                        diagnostic_digest,
                    });
                }
            };
            let outcome = self.sdk.invoke(&self.deployment, &input).await;
            Ok(match outcome {
                ProviderSdkOutcome::Turn(exchange) => {
                    self.contexts
                        .commit(request, &exchange)
                        .map_err(ModelGatewayError::Adapter)?;
                    ModelGatewayOutcome::Turn(exchange.gateway_exchange)
                }
                // Every failure below publishes its explanation before the reducer records it.
                // `ok()` rather than `?`: a store that cannot take the diagnostic is not a reason
                // to lose the outcome, and `None` states honestly that nothing was stored instead
                // of naming an artifact that is not there.
                ProviderSdkOutcome::ConfirmedNotSent { diagnostic } => {
                    let diagnostic_digest = self.contexts.publish_diagnostic(&diagnostic).ok();
                    ModelGatewayOutcome::ConfirmedNotSent {
                        diagnostic,
                        diagnostic_digest,
                    }
                }
                ProviderSdkOutcome::Rejected {
                    response_digest,
                    diagnostic,
                    retryable,
                } => {
                    let diagnostic_digest = self.contexts.publish_diagnostic(&diagnostic).ok();
                    ModelGatewayOutcome::Rejected {
                        response_digest,
                        diagnostic,
                        diagnostic_digest,
                        retryable,
                    }
                }
                ProviderSdkOutcome::DecodeFailed {
                    response_digest,
                    diagnostic,
                } => {
                    let diagnostic_digest = self.contexts.publish_diagnostic(&diagnostic).ok();
                    ModelGatewayOutcome::DecodeFailed {
                        response_digest,
                        diagnostic,
                        diagnostic_digest,
                    }
                }
                ProviderSdkOutcome::Ambiguous { diagnostic } => {
                    let diagnostic_digest = self.contexts.publish_diagnostic(&diagnostic).ok();
                    ModelGatewayOutcome::Ambiguous {
                        diagnostic,
                        diagnostic_digest,
                    }
                }
            })
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSdkError {
    Catalog(ModelCatalogError),
    Codec(CodecError),
}

impl Display for ProviderSdkError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => Display::fmt(error, formatter),
            Self::Codec(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ProviderSdkError {}

#[cfg(test)]
mod tests;
