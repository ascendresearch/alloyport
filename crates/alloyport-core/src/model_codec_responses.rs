//! `OpenAI` Responses wire codec with stateless typed-item replay.

use crate::model::ProtocolKind;
use crate::model_codec::{
    CodecError, CodecLimits, CodecToolDefinition, DecodedModelTurn, ModelVisibleToolResult,
    NativeContinuation, NativeTurnInput, PreparedModelPayload, ProtocolCodec, RawModelResponseRef,
    correlate_results, finish_decoded, optional_u64, parse_response, require_protocol,
    string_field, usage, validate_calls, validate_narrative, validate_prepare_input,
};
use crate::model_gateway::{GatewayToolCall, GatewayTurn, NormalizedStopReason};
use serde_json::{Map, Value, json};

/// Stateless `OpenAI` Responses protocol codec using local item replay and `store: false`.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenAiResponsesCodec {
    limits: CodecLimits,
}

impl OpenAiResponsesCodec {
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
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
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
                    native.insert("type".to_owned(), Value::String("function".to_owned()));
                    native.insert("name".to_owned(), Value::String(tool.name.clone()));
                    native.insert(
                        "description".to_owned(),
                        Value::String(tool.description.clone()),
                    );
                    native.insert("parameters".to_owned(), tool.input_schema.clone());
                    if tool.strict {
                        native.insert("strict".to_owned(), Value::Bool(true));
                    }
                    Value::Object(native)
                })
                .collect(),
        )
    }

    fn decode_output(
        output: &[Value],
    ) -> Result<(Vec<String>, Vec<GatewayToolCall>, bool), CodecError> {
        let mut narrative = Vec::new();
        let mut calls = Vec::new();
        let mut refusal = false;
        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("reasoning") => {}
                Some("function_call") => calls.push(GatewayToolCall {
                    native_call_id: required_string(item, "call_id")?,
                    name: required_string(item, "name")?,
                    raw_arguments: required_string(item, "arguments")?.into_bytes(),
                }),
                Some("message") => {
                    if item.get("role").and_then(Value::as_str) != Some("assistant") {
                        return Err(CodecError::InvalidShape(
                            "Responses output message must have assistant role".to_owned(),
                        ));
                    }
                    let content =
                        item.get("content")
                            .and_then(Value::as_array)
                            .ok_or_else(|| {
                                CodecError::InvalidShape(
                                    "Responses message content must be an array".to_owned(),
                                )
                            })?;
                    for block in content {
                        match block.get("type").and_then(Value::as_str) {
                            Some("output_text") => {
                                let text = required_string(block, "text")?;
                                if !text.is_empty() {
                                    narrative.push(text);
                                }
                            }
                            Some("refusal") => {
                                refusal = true;
                                let text = required_string(block, "refusal")?;
                                if !text.is_empty() {
                                    narrative.push(text);
                                }
                            }
                            kind => {
                                return Err(CodecError::UnsupportedNativeItem(
                                    kind.unwrap_or("missing Responses content type").to_owned(),
                                ));
                            }
                        }
                    }
                }
                kind => {
                    return Err(CodecError::UnsupportedNativeItem(
                        kind.unwrap_or("missing Responses output type").to_owned(),
                    ));
                }
            }
        }
        Ok((narrative, calls, refusal))
    }
}

impl ProtocolCodec for OpenAiResponsesCodec {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::OpenAiResponses
    }

    fn prepare(&self, input: NativeTurnInput<'_>) -> Result<PreparedModelPayload, CodecError> {
        validate_prepare_input(self.kind(), input, self.limits)?;
        let history = match input.continuation {
            Some(continuation) => continuation.native_history().clone(),
            None => Self::initial_history(input),
        };
        if !history.is_array() {
            return Err(CodecError::InvalidShape(
                "Responses continuation history must be an array".to_owned(),
            ));
        }
        let continuation =
            NativeContinuation::new(self.kind(), history.clone(), vec![], self.limits)?;
        PreparedModelPayload::new(
            &json!({
                "model": input.wire_model,
                "instructions": input.system_prompt,
                "input": history,
                "tools": Self::tools(input.tools),
                "max_output_tokens": input.max_output_tokens,
                "store": false
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
        let output = value
            .get("output")
            .and_then(Value::as_array)
            .ok_or_else(|| CodecError::InvalidShape("output must be an array".to_owned()))?;
        let (narrative, calls, refusal) = Self::decode_output(output)?;
        validate_narrative(&narrative, self.limits)?;
        let pending_call_ids = validate_calls(&calls, request, self.limits)?;
        let turn = GatewayTurn {
            narrative,
            stop_reason: responses_stop_reason(&value, refusal, !calls.is_empty()),
            tool_calls: calls,
            usage: usage(
                optional_u64(&value, &["usage", "input_tokens"]),
                optional_u64(&value, &["usage", "output_tokens"]),
                optional_u64(&value, &["usage", "input_tokens_details", "cached_tokens"]),
            ),
        };
        let mut history = request
            .base_continuation()
            .native_history()
            .as_array()
            .ok_or_else(|| {
                CodecError::InvalidShape("Responses history must be an array".to_owned())
            })?
            .clone();
        history.extend(output.iter().cloned());
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
            .ok_or_else(|| {
                CodecError::InvalidShape("Responses history must be an array".to_owned())
            })?
            .clone();
        for call_id in continuation.pending_call_ids() {
            history.push(json!({
                "type": "function_call_output",
                "call_id": call_id,
                "output": correlated[call_id.as_str()]
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

fn responses_stop_reason(response: &Value, refusal: bool, has_calls: bool) -> NormalizedStopReason {
    if refusal {
        return NormalizedStopReason::Refusal;
    }
    if has_calls {
        return NormalizedStopReason::ToolCalls;
    }
    match response.get("status").and_then(Value::as_str) {
        Some("completed") => NormalizedStopReason::Stop,
        Some("incomplete") => match response
            .get("incomplete_details")
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str)
        {
            Some("max_output_tokens") => NormalizedStopReason::Length,
            Some("content_filter") => NormalizedStopReason::ContentFilter,
            _ => NormalizedStopReason::ProviderResource,
        },
        _ => NormalizedStopReason::Unknown,
    }
}
