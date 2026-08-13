//! Provider-protocol codec boundary and shared bounded native-continuation values.

use crate::Sha256Digest;
use crate::model::{ModelUsage, ProtocolKind};
use crate::model_gateway::{GatewayToolCall, GatewayTurn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub const NATIVE_CONTINUATION_SCHEMA_V1: u16 = 1;

/// Codec-owned size and cardinality bounds, applied before native data is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecLimits {
    pub max_response_bytes: usize,
    pub max_request_bytes: usize,
    pub max_continuation_bytes: usize,
    pub max_tool_argument_bytes: usize,
    pub max_tool_result_bytes: usize,
    pub max_narrative_bytes: usize,
    pub max_tool_calls: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: 8 * 1024 * 1024,
            max_request_bytes: 8 * 1024 * 1024,
            max_continuation_bytes: 8 * 1024 * 1024,
            max_tool_argument_bytes: 1024 * 1024,
            max_tool_result_bytes: 1024 * 1024,
            max_narrative_bytes: 1024 * 1024,
            max_tool_calls: 64,
        }
    }
}

impl CodecLimits {
    /// Rejects a limit set that would silently disable a required boundary.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidLimits`] when any limit is zero.
    pub fn validate(self) -> Result<Self, CodecError> {
        if self.max_response_bytes == 0
            || self.max_request_bytes == 0
            || self.max_continuation_bytes == 0
            || self.max_tool_argument_bytes == 0
            || self.max_tool_result_bytes == 0
            || self.max_narrative_bytes == 0
            || self.max_tool_calls == 0
        {
            return Err(CodecError::InvalidLimits);
        }
        Ok(self)
    }
}

/// One client-executed tool exposed to a protocol codec.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodecToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub strict: bool,
}

/// Provider-neutral inputs required to construct one native request.
#[derive(Clone, Copy, Debug)]
pub struct NativeTurnInput<'a> {
    pub wire_model: &'a str,
    pub system_prompt: &'a str,
    pub initial_user_text: Option<&'a str>,
    pub continuation: Option<&'a NativeContinuation>,
    pub tools: &'a [CodecToolDefinition],
    pub max_output_tokens: u32,
    pub reasoning_effort: Option<crate::model::ReasoningEffort>,
}

/// Exact native state replayed locally on a later request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeContinuation {
    schema_version: u16,
    protocol: ProtocolKind,
    native_history: Value,
    pending_call_ids: Vec<String>,
}

impl NativeContinuation {
    #[must_use]
    pub const fn protocol(&self) -> ProtocolKind {
        self.protocol
    }

    #[must_use]
    pub fn native_history(&self) -> &Value {
        &self.native_history
    }

    #[must_use]
    pub fn pending_call_ids(&self) -> &[String] {
        &self.pending_call_ids
    }

    /// Serializes the native state used for content addressing.
    ///
    /// # Errors
    ///
    /// Returns an error if the retained JSON value cannot be serialized.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(self).map_err(|_| CodecError::Serialization)
    }

    /// Rehydrates one versioned continuation Artifact under the current bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON, unsupported schema versions, invalid pending call
    /// identities, or exceeded bounds.
    pub fn from_canonical_bytes(bytes: &[u8], limits: CodecLimits) -> Result<Self, CodecError> {
        limits.validate()?;
        ensure_bound(
            "native continuation",
            bytes.len(),
            limits.max_continuation_bytes,
        )?;
        let continuation: Self =
            serde_json::from_slice(bytes).map_err(|_| CodecError::InvalidJson)?;
        if continuation.schema_version != NATIVE_CONTINUATION_SCHEMA_V1 {
            return Err(CodecError::UnsupportedContinuationSchema(
                continuation.schema_version,
            ));
        }
        let mut seen = BTreeSet::new();
        for call_id in &continuation.pending_call_ids {
            if call_id.trim().is_empty() {
                return Err(CodecError::EmptyCallId);
            }
            if !seen.insert(call_id) {
                return Err(CodecError::DuplicateCallId(call_id.clone()));
            }
        }
        Ok(continuation)
    }

    /// Computes the identity of the exact locally replayed native state.
    ///
    /// # Errors
    ///
    /// Returns an error if the retained JSON value cannot be serialized.
    pub fn digest(&self) -> Result<Sha256Digest, CodecError> {
        Ok(Sha256Digest::digest_bytes(&self.canonical_bytes()?))
    }

    pub(crate) fn new(
        protocol: ProtocolKind,
        native_history: Value,
        pending_call_ids: Vec<String>,
        limits: CodecLimits,
    ) -> Result<Self, CodecError> {
        let continuation = Self {
            schema_version: NATIVE_CONTINUATION_SCHEMA_V1,
            protocol,
            native_history,
            pending_call_ids,
        };
        ensure_bound(
            "native continuation",
            continuation.canonical_bytes()?.len(),
            limits.max_continuation_bytes,
        )?;
        Ok(continuation)
    }

    pub(crate) fn with_history(
        &self,
        native_history: Value,
        pending_call_ids: Vec<String>,
        limits: CodecLimits,
    ) -> Result<Self, CodecError> {
        Self::new(self.protocol, native_history, pending_call_ids, limits)
    }
}

/// Serialized request plus the exact history from which a response continues.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedModelPayload {
    body: Vec<u8>,
    base_continuation: NativeContinuation,
    offered_tool_names: BTreeSet<String>,
}

impl PreparedModelPayload {
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    #[must_use]
    pub const fn base_continuation(&self) -> &NativeContinuation {
        &self.base_continuation
    }

    pub(crate) fn new(
        body: &Value,
        base_continuation: NativeContinuation,
        tools: &[CodecToolDefinition],
        limits: CodecLimits,
    ) -> Result<Self, CodecError> {
        let body = serde_json::to_vec(body).map_err(|_| CodecError::Serialization)?;
        ensure_bound("model request", body.len(), limits.max_request_bytes)?;
        Ok(Self {
            body,
            base_continuation,
            offered_tool_names: tools.iter().map(|tool| tool.name.clone()).collect(),
        })
    }
}

/// Bounded raw provider response passed into a codec.
#[derive(Clone, Copy, Debug)]
pub struct RawModelResponseRef<'a> {
    pub body: &'a [u8],
}

/// Model-visible result correlated to one native client-tool call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelVisibleToolResult<'a> {
    pub native_call_id: &'a str,
    pub output: &'a str,
}

/// Provider-neutral turn plus the exact native continuation it produced.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedModelTurn {
    pub turn: GatewayTurn,
    pub native_continuation: NativeContinuation,
    pub raw_response_digest: Sha256Digest,
    pub provider_response_id: Option<String>,
    pub actual_model: Option<String>,
}

/// A protocol codec owns wire shape and continuation semantics only.
pub trait ProtocolCodec: Send + Sync {
    #[must_use]
    fn kind(&self) -> ProtocolKind;

    /// Builds one bounded native protocol request.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid initial/continuation state or exceeded bounds.
    fn prepare(&self, input: NativeTurnInput<'_>) -> Result<PreparedModelPayload, CodecError>;

    /// Decodes one bounded native response and extends the prepared request history.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, unsupported, ambiguous, or oversized responses.
    fn decode(
        &self,
        request: &PreparedModelPayload,
        response: RawModelResponseRef<'_>,
    ) -> Result<DecodedModelTurn, CodecError>;

    /// Correlates all pending client-tool results into protocol-native continuation state.
    ///
    /// # Errors
    ///
    /// Returns an error when results are missing, duplicated, empty, or mismatched.
    fn append_tool_results(
        &self,
        continuation: &NativeContinuation,
        results: &[ModelVisibleToolResult<'_>],
    ) -> Result<NativeContinuation, CodecError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    InvalidLimits,
    InvalidJson,
    UnsupportedContinuationSchema(u16),
    InvalidShape(String),
    ProtocolMismatch {
        expected: ProtocolKind,
        actual: ProtocolKind,
    },
    MissingInitialInput,
    ConflictingInput,
    PendingToolResults,
    EmptyCallId,
    DuplicateCallId(String),
    DuplicateToolName(String),
    EmptyToolName,
    UnknownTool(String),
    EmptyToolResult(String),
    ToolResultMismatch,
    UnsupportedNativeItem(String),
    LimitExceeded {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    Serialization,
}

impl Display for CodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => write!(formatter, "codec limits must all be positive"),
            Self::InvalidJson => write!(formatter, "provider response is not valid JSON"),
            Self::UnsupportedContinuationSchema(version) => {
                write!(
                    formatter,
                    "unsupported native continuation schema {version}"
                )
            }
            Self::InvalidShape(message) => {
                write!(formatter, "invalid provider response: {message}")
            }
            Self::ProtocolMismatch { expected, actual } => {
                write!(
                    formatter,
                    "expected protocol {expected:?}, received {actual:?}"
                )
            }
            Self::MissingInitialInput => write!(formatter, "initial user input is required"),
            Self::ConflictingInput => {
                write!(formatter, "initial input and continuation are exclusive")
            }
            Self::PendingToolResults => {
                write!(formatter, "pending tool results must be appended first")
            }
            Self::EmptyCallId => write!(formatter, "native tool call ID must not be empty"),
            Self::DuplicateCallId(id) => write!(formatter, "duplicate native tool call ID {id}"),
            Self::DuplicateToolName(name) => {
                write!(formatter, "duplicate offered tool name {name}")
            }
            Self::EmptyToolName => write!(formatter, "native tool name must not be empty"),
            Self::UnknownTool(name) => write!(formatter, "model called unknown tool {name}"),
            Self::EmptyToolResult(id) => {
                write!(formatter, "tool result for {id} must not be empty")
            }
            Self::ToolResultMismatch => {
                write!(formatter, "tool results do not exactly match pending calls")
            }
            Self::UnsupportedNativeItem(kind) => {
                write!(formatter, "unsupported native item {kind}")
            }
            Self::LimitExceeded {
                field,
                actual,
                maximum,
            } => write!(
                formatter,
                "{field} uses {actual} bytes/items; maximum is {maximum}"
            ),
            Self::Serialization => {
                write!(formatter, "native protocol JSON could not be serialized")
            }
        }
    }
}

impl Error for CodecError {}

pub(crate) fn parse_response(
    response: RawModelResponseRef<'_>,
    limits: CodecLimits,
) -> Result<Value, CodecError> {
    ensure_bound(
        "model response",
        response.body.len(),
        limits.max_response_bytes,
    )?;
    serde_json::from_slice(response.body).map_err(|_| CodecError::InvalidJson)
}

pub(crate) fn validate_prepare_input(
    kind: ProtocolKind,
    input: NativeTurnInput<'_>,
    limits: CodecLimits,
) -> Result<(), CodecError> {
    limits.validate()?;
    if input.wire_model.trim().is_empty() || input.max_output_tokens == 0 {
        return Err(CodecError::InvalidShape(
            "wire model and max output tokens must be non-empty".to_owned(),
        ));
    }
    match (input.initial_user_text, input.continuation) {
        (Some(text), None) if !text.trim().is_empty() => {}
        (None, Some(continuation)) => {
            require_protocol(kind, continuation)?;
            if !continuation.pending_call_ids.is_empty() {
                return Err(CodecError::PendingToolResults);
            }
        }
        (None, None) => return Err(CodecError::MissingInitialInput),
        _ => return Err(CodecError::ConflictingInput),
    }
    let mut tool_names = BTreeSet::new();
    for tool in input.tools {
        if tool.name.trim().is_empty() || tool.description.trim().is_empty() {
            return Err(CodecError::EmptyToolName);
        }
        if !tool.input_schema.is_object() {
            return Err(CodecError::InvalidShape(
                "tool input schema must be a JSON object".to_owned(),
            ));
        }
        if !tool_names.insert(&tool.name) {
            return Err(CodecError::DuplicateToolName(tool.name.clone()));
        }
    }
    Ok(())
}

pub(crate) fn require_protocol(
    expected: ProtocolKind,
    continuation: &NativeContinuation,
) -> Result<(), CodecError> {
    if continuation.protocol != expected {
        return Err(CodecError::ProtocolMismatch {
            expected,
            actual: continuation.protocol,
        });
    }
    Ok(())
}

pub(crate) fn validate_calls(
    calls: &[GatewayToolCall],
    request: &PreparedModelPayload,
    limits: CodecLimits,
) -> Result<Vec<String>, CodecError> {
    ensure_bound("tool calls", calls.len(), limits.max_tool_calls)?;
    let mut seen = BTreeSet::new();
    let mut ids = Vec::with_capacity(calls.len());
    for call in calls {
        if call.native_call_id.trim().is_empty() {
            return Err(CodecError::EmptyCallId);
        }
        if call.name.trim().is_empty() {
            return Err(CodecError::EmptyToolName);
        }
        if !request.offered_tool_names.contains(&call.name) {
            return Err(CodecError::UnknownTool(call.name.clone()));
        }
        ensure_bound(
            "tool arguments",
            call.raw_arguments.len(),
            limits.max_tool_argument_bytes,
        )?;
        if !seen.insert(call.native_call_id.clone()) {
            return Err(CodecError::DuplicateCallId(call.native_call_id.clone()));
        }
        ids.push(call.native_call_id.clone());
    }
    Ok(ids)
}

pub(crate) fn correlate_results<'a>(
    continuation: &NativeContinuation,
    results: &'a [ModelVisibleToolResult<'a>],
    limits: CodecLimits,
) -> Result<BTreeMap<&'a str, &'a str>, CodecError> {
    if continuation.pending_call_ids.len() != results.len() {
        return Err(CodecError::ToolResultMismatch);
    }
    let pending: BTreeSet<&str> = continuation
        .pending_call_ids
        .iter()
        .map(String::as_str)
        .collect();
    let mut correlated = BTreeMap::new();
    for result in results {
        if result.output.is_empty() {
            return Err(CodecError::EmptyToolResult(
                result.native_call_id.to_owned(),
            ));
        }
        ensure_bound(
            "tool result",
            result.output.len(),
            limits.max_tool_result_bytes,
        )?;
        if !pending.contains(result.native_call_id)
            || correlated
                .insert(result.native_call_id, result.output)
                .is_some()
        {
            return Err(CodecError::ToolResultMismatch);
        }
    }
    Ok(correlated)
}

pub(crate) fn validate_narrative(
    narrative: &[String],
    limits: CodecLimits,
) -> Result<(), CodecError> {
    let bytes = narrative.iter().map(String::len).sum();
    ensure_bound("narrative", bytes, limits.max_narrative_bytes)
}

pub(crate) fn finish_decoded(
    response: RawModelResponseRef<'_>,
    turn: GatewayTurn,
    native_continuation: NativeContinuation,
    provider_response_id: Option<String>,
    actual_model: Option<String>,
) -> Result<DecodedModelTurn, CodecError> {
    turn.validate()
        .map_err(|error| CodecError::InvalidShape(error.to_string()))?;
    Ok(DecodedModelTurn {
        turn,
        native_continuation,
        raw_response_digest: Sha256Digest::digest_bytes(response.body),
        provider_response_id,
        actual_model,
    })
}

pub(crate) fn optional_u64(value: &Value, path: &[&str]) -> Option<u64> {
    let mut current = value;
    for field in path {
        current = current.get(*field)?;
    }
    current.as_u64()
}

pub(crate) fn usage(
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
) -> Option<ModelUsage> {
    match (input_tokens, output_tokens) {
        (Some(input_tokens), Some(output_tokens)) => Some(ModelUsage {
            input_tokens,
            output_tokens,
            cached_input_tokens,
            cost_micros: None,
        }),
        _ => None,
    }
}

pub(crate) fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

pub(crate) fn ensure_bound(
    field: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), CodecError> {
    if actual > maximum {
        return Err(CodecError::LimitExceeded {
            field,
            actual,
            maximum,
        });
    }
    Ok(())
}
