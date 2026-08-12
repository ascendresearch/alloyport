//! Anthropic Messages wire codec with ordered content-block replay.

use crate::model::ProtocolKind;
use crate::model_codec::{
    CodecError, CodecLimits, CodecToolDefinition, DecodedModelTurn, ModelVisibleToolResult,
    NativeContinuation, NativeTurnInput, PreparedModelPayload, ProtocolCodec, RawModelResponseRef,
    correlate_results, finish_decoded, optional_u64, parse_response, require_protocol,
    string_field, usage, validate_calls, validate_narrative, validate_prepare_input,
};
use crate::model_gateway::{GatewayToolCall, GatewayTurn, NormalizedStopReason};
use serde_json::{Map, Value, json};

/// Stateless Anthropic Messages protocol codec using local content-block replay.
#[derive(Clone, Copy, Debug, Default)]
pub struct AnthropicMessagesCodec {
    limits: CodecLimits,
}

impl AnthropicMessagesCodec {
    /// Creates a codec with explicit bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when any bound is zero.
    pub fn new(limits: CodecLimits) -> Result<Self, CodecError> {
        Ok(Self {
            limits: limits.validate()?,
        })
    }

    fn initial_history(input: NativeTurnInput<'_>) -> Value {
        json!([{
            "role": "user",
            "content": [{
                "type": "text",
                "text": input.initial_user_text.unwrap_or_default()
            }]
        }])
    }

    fn tools(tools: &[CodecToolDefinition]) -> Value {
        Value::Array(
            tools
                .iter()
                .map(|tool| {
                    let mut native = Map::new();
                    native.insert("name".to_owned(), Value::String(tool.name.clone()));
                    native.insert(
                        "description".to_owned(),
                        Value::String(tool.description.clone()),
                    );
                    native.insert("input_schema".to_owned(), tool.input_schema.clone());
                    if tool.strict {
                        native.insert("strict".to_owned(), Value::Bool(true));
                    }
                    Value::Object(native)
                })
                .collect(),
        )
    }

    fn decode_content(
        content: &[Value],
    ) -> Result<(Vec<String>, Vec<GatewayToolCall>), CodecError> {
        let mut narrative = Vec::new();
        let mut calls = Vec::new();
        for block in content {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let text = required_string(block, "text")?;
                    if !text.is_empty() {
                        narrative.push(text);
                    }
                }
                Some("thinking" | "redacted_thinking") => {}
                Some("tool_use") => {
                    let input = block.get("input").ok_or_else(|| {
                        CodecError::InvalidShape("tool_use.input is required".to_owned())
                    })?;
                    if !input.is_object() {
                        return Err(CodecError::InvalidShape(
                            "tool_use.input must be an object".to_owned(),
                        ));
                    }
                    calls.push(GatewayToolCall {
                        native_call_id: required_string(block, "id")?,
                        name: required_string(block, "name")?,
                        raw_arguments: serde_json::to_vec(input)
                            .map_err(|_| CodecError::Serialization)?,
                    });
                }
                kind => {
                    return Err(CodecError::UnsupportedNativeItem(
                        kind.unwrap_or("missing Anthropic content type").to_owned(),
                    ));
                }
            }
        }
        Ok((narrative, calls))
    }
}

impl ProtocolCodec for AnthropicMessagesCodec {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::AnthropicMessages
    }

    fn prepare(&self, input: NativeTurnInput<'_>) -> Result<PreparedModelPayload, CodecError> {
        validate_prepare_input(self.kind(), input, self.limits)?;
        let history = match input.continuation {
            Some(continuation) => continuation.native_history().clone(),
            None => Self::initial_history(input),
        };
        if !history.is_array() {
            return Err(CodecError::InvalidShape(
                "Anthropic continuation history must be an array".to_owned(),
            ));
        }
        let continuation =
            NativeContinuation::new(self.kind(), history.clone(), vec![], self.limits)?;
        PreparedModelPayload::new(
            &json!({
                "model": input.wire_model,
                "max_tokens": input.max_output_tokens,
                "system": input.system_prompt,
                "messages": history,
                "tools": Self::tools(input.tools)
            }),
            continuation,
            input.tools,
            self.limits,
        )
    }

    fn decode(
        &self,
        request: &PreparedModelPayload,
        response: RawModelResponseRef<'_>,
    ) -> Result<DecodedModelTurn, CodecError> {
        require_protocol(self.kind(), request.base_continuation())?;
        let value = parse_response(response, self.limits)?;
        if value.get("type").and_then(Value::as_str) != Some("message")
            || value.get("role").and_then(Value::as_str) != Some("assistant")
        {
            return Err(CodecError::InvalidShape(
                "Anthropic response must be an assistant message".to_owned(),
            ));
        }
        let content = value
            .get("content")
            .and_then(Value::as_array)
            .ok_or_else(|| CodecError::InvalidShape("content must be an array".to_owned()))?;
        let (narrative, calls) = Self::decode_content(content)?;
        validate_narrative(&narrative, self.limits)?;
        let pending_call_ids = validate_calls(&calls, request, self.limits)?;
        let turn = GatewayTurn {
            narrative,
            stop_reason: anthropic_stop_reason(&value, !calls.is_empty()),
            tool_calls: calls,
            usage: usage(
                optional_u64(&value, &["usage", "input_tokens"]),
                optional_u64(&value, &["usage", "output_tokens"]),
                optional_u64(&value, &["usage", "cache_read_input_tokens"]),
            ),
        };
        let mut history = request
            .base_continuation()
            .native_history()
            .as_array()
            .ok_or_else(|| {
                CodecError::InvalidShape("Anthropic history must be an array".to_owned())
            })?
            .clone();
        history.push(json!({"role": "assistant", "content": content}));
        let continuation = request.base_continuation().with_history(
            Value::Array(history),
            pending_call_ids,
            self.limits,
        )?;
        finish_decoded(
            response,
            turn,
            continuation,
            string_field(&value, "id"),
            string_field(&value, "model"),
        )
    }

    fn append_tool_results(
        &self,
        continuation: &NativeContinuation,
        results: &[ModelVisibleToolResult<'_>],
    ) -> Result<NativeContinuation, CodecError> {
        require_protocol(self.kind(), continuation)?;
        let correlated = correlate_results(continuation, results, self.limits)?;
        let content: Vec<Value> = continuation
            .pending_call_ids()
            .iter()
            .map(|call_id| {
                json!({
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": correlated[call_id.as_str()]
                })
            })
            .collect();
        let mut history = continuation
            .native_history()
            .as_array()
            .ok_or_else(|| {
                CodecError::InvalidShape("Anthropic history must be an array".to_owned())
            })?
            .clone();
        history.push(json!({"role": "user", "content": content}));
        continuation.with_history(Value::Array(history), vec![], self.limits)
    }
}

fn required_string(value: &Value, field: &str) -> Result<String, CodecError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| CodecError::InvalidShape(format!("{field} must be a string")))
}

fn anthropic_stop_reason(response: &Value, has_calls: bool) -> NormalizedStopReason {
    match response.get("stop_reason").and_then(Value::as_str) {
        Some("tool_use") => NormalizedStopReason::ToolCalls,
        Some("end_turn" | "stop_sequence") => NormalizedStopReason::Stop,
        Some("max_tokens" | "model_context_window_exceeded") => NormalizedStopReason::Length,
        Some("refusal") => NormalizedStopReason::Refusal,
        Some("pause_turn") => NormalizedStopReason::ProviderResource,
        _ if has_calls => NormalizedStopReason::ToolCalls,
        _ => NormalizedStopReason::Unknown,
    }
}
