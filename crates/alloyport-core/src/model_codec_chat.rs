//! `OpenAI` Chat Completions wire codec with exact assistant-message replay.

use crate::model::ProtocolKind;
use crate::model_codec::{
    CodecError, CodecLimits, CodecToolDefinition, DecodedModelTurn, ModelVisibleToolResult,
    NativeContinuation, NativeTurnInput, PreparedModelPayload, ProtocolCodec, RawModelResponseRef,
    correlate_results, finish_decoded, optional_u64, parse_response, require_protocol,
    string_field, usage, validate_calls, validate_narrative, validate_prepare_input,
};
use crate::model_gateway::{GatewayToolCall, GatewayTurn, NormalizedStopReason};
use serde_json::{Map, Value, json};

/// Stateless `OpenAI` Chat Completions protocol codec.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenAiChatCompletionsCodec {
    limits: CodecLimits,
}

impl OpenAiChatCompletionsCodec {
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
        let mut messages = Vec::new();
        if !input.system_prompt.trim().is_empty() {
            messages.push(json!({"role": "system", "content": input.system_prompt}));
        }
        messages.push(json!({
            "role": "user",
            "content": input.initial_user_text.unwrap_or_default()
        }));
        Value::Array(messages)
    }

    fn tools(tools: &[CodecToolDefinition]) -> Value {
        Value::Array(
            tools
                .iter()
                .map(|tool| {
                    let mut function = Map::new();
                    function.insert("name".to_owned(), Value::String(tool.name.clone()));
                    function.insert(
                        "description".to_owned(),
                        Value::String(tool.description.clone()),
                    );
                    function.insert("parameters".to_owned(), tool.input_schema.clone());
                    if tool.strict {
                        function.insert("strict".to_owned(), Value::Bool(true));
                    }
                    json!({"type": "function", "function": function})
                })
                .collect(),
        )
    }

    fn decode_calls(message: &Value) -> Result<Vec<GatewayToolCall>, CodecError> {
        let Some(native_calls) = message.get("tool_calls") else {
            return Ok(Vec::new());
        };
        if native_calls.is_null() {
            return Ok(Vec::new());
        }
        let native_calls = native_calls.as_array().ok_or_else(|| {
            CodecError::InvalidShape("message.tool_calls must be an array".to_owned())
        })?;
        native_calls
            .iter()
            .map(|call| {
                if call.get("type").and_then(Value::as_str) != Some("function") {
                    return Err(CodecError::UnsupportedNativeItem(
                        call.get("type")
                            .and_then(Value::as_str)
                            .unwrap_or("missing chat tool-call type")
                            .to_owned(),
                    ));
                }
                let function = call.get("function").ok_or_else(|| {
                    CodecError::InvalidShape("tool call is missing function".to_owned())
                })?;
                Ok(GatewayToolCall {
                    native_call_id: required_string(call, "id")?,
                    name: required_string(function, "name")?,
                    raw_arguments: required_string(function, "arguments")?.into_bytes(),
                })
            })
            .collect()
    }
}

impl ProtocolCodec for OpenAiChatCompletionsCodec {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::OpenAiChatCompletions
    }

    fn prepare(&self, input: NativeTurnInput<'_>) -> Result<PreparedModelPayload, CodecError> {
        validate_prepare_input(self.kind(), input, self.limits)?;
        let history = match input.continuation {
            Some(continuation) => continuation.native_history().clone(),
            None => Self::initial_history(input),
        };
        if !history.is_array() {
            return Err(CodecError::InvalidShape(
                "chat continuation history must be an array".to_owned(),
            ));
        }
        let continuation =
            NativeContinuation::new(self.kind(), history.clone(), vec![], self.limits)?;
        PreparedModelPayload::new(
            &json!({
                "model": input.wire_model,
                "messages": history,
                "tools": Self::tools(input.tools),
                "max_tokens": input.max_output_tokens
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
        let choices = value
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| CodecError::InvalidShape("choices must be an array".to_owned()))?;
        if choices.len() != 1 {
            return Err(CodecError::InvalidShape(
                "exactly one chat choice is required".to_owned(),
            ));
        }
        let choice = &choices[0];
        let message = choice
            .get("message")
            .ok_or_else(|| CodecError::InvalidShape("choice.message is required".to_owned()))?;
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            return Err(CodecError::InvalidShape(
                "chat response message must have assistant role".to_owned(),
            ));
        }

        let calls = Self::decode_calls(message)?;
        let pending_call_ids = validate_calls(&calls, request, self.limits)?;
        let narrative = chat_narrative(message)?;
        validate_narrative(&narrative, self.limits)?;
        let stop_reason = chat_stop_reason(choice, message, !calls.is_empty());
        let turn = GatewayTurn {
            narrative,
            tool_calls: calls,
            stop_reason,
            usage: usage(
                optional_u64(&value, &["usage", "prompt_tokens"]),
                optional_u64(&value, &["usage", "completion_tokens"]),
                optional_u64(&value, &["usage", "prompt_tokens_details", "cached_tokens"]),
            ),
        };

        let mut history = request
            .base_continuation()
            .native_history()
            .as_array()
            .ok_or_else(|| CodecError::InvalidShape("chat history must be an array".to_owned()))?
            .clone();
        history.push(message.clone());
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
        let mut history = continuation
            .native_history()
            .as_array()
            .ok_or_else(|| CodecError::InvalidShape("chat history must be an array".to_owned()))?
            .clone();
        for call_id in continuation.pending_call_ids() {
            history.push(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": correlated[call_id.as_str()]
            }));
        }
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

fn chat_narrative(message: &Value) -> Result<Vec<String>, CodecError> {
    let mut narrative = Vec::new();
    if let Some(content) = message.get("content") {
        match content {
            Value::String(text) if !text.is_empty() => narrative.push(text.clone()),
            Value::Null | Value::String(_) => {}
            _ => {
                return Err(CodecError::InvalidShape(
                    "chat assistant content must be a string or null".to_owned(),
                ));
            }
        }
    }
    if let Some(refusal) = message.get("refusal") {
        match refusal {
            Value::String(text) if !text.is_empty() => narrative.push(text.clone()),
            Value::Null | Value::String(_) => {}
            _ => {
                return Err(CodecError::InvalidShape(
                    "chat refusal must be a string or null".to_owned(),
                ));
            }
        }
    }
    Ok(narrative)
}

fn chat_stop_reason(choice: &Value, message: &Value, has_calls: bool) -> NormalizedStopReason {
    if message
        .get("refusal")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.is_empty())
    {
        return NormalizedStopReason::Refusal;
    }
    match choice.get("finish_reason").and_then(Value::as_str) {
        Some("stop") => NormalizedStopReason::Stop,
        Some("tool_calls") => NormalizedStopReason::ToolCalls,
        Some("length") => NormalizedStopReason::Length,
        Some("content_filter") => NormalizedStopReason::ContentFilter,
        _ if has_calls => NormalizedStopReason::ToolCalls,
        _ => NormalizedStopReason::Unknown,
    }
}
