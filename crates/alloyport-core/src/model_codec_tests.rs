use crate::{
    AnthropicMessagesCodec, CodecError, CodecLimits, CodecToolDefinition, ModelUsage,
    ModelVisibleToolResult, NativeContinuation, NativeTurnInput, NormalizedStopReason,
    OpenAiChatCompletionsCodec, OpenAiResponsesCodec, ProtocolCodec, RawModelResponseRef,
    ReasoningEffort, ReasoningMode, Sha256Digest,
};
use serde_json::{Value, json};

const CHAT_RESPONSE: &[u8] =
    include_bytes!("../fixtures/model-codecs/openai_chat_tool_response.json");
const CHAT_FOLLOWUP: &str =
    include_str!("../fixtures/model-codecs/openai_chat_followup_request.json");
const CHAT_SECOND_RESPONSE: &[u8] =
    include_bytes!("../fixtures/model-codecs/openai_chat_second_tool_response.json");
const RESPONSES_RESPONSE: &[u8] =
    include_bytes!("../fixtures/model-codecs/openai_responses_tool_response.json");
const RESPONSES_FOLLOWUP: &str =
    include_str!("../fixtures/model-codecs/openai_responses_followup_request.json");
const RESPONSES_SECOND_RESPONSE: &[u8] =
    include_bytes!("../fixtures/model-codecs/openai_responses_second_tool_response.json");
const ANTHROPIC_RESPONSE: &[u8] =
    include_bytes!("../fixtures/model-codecs/anthropic_tool_response.json");
const ANTHROPIC_FOLLOWUP: &str =
    include_str!("../fixtures/model-codecs/anthropic_followup_request.json");
const ANTHROPIC_SECOND_RESPONSE: &[u8] =
    include_bytes!("../fixtures/model-codecs/anthropic_second_tool_response.json");

fn tools() -> Vec<CodecToolDefinition> {
    vec![
        CodecToolDefinition {
            name: "inspect_candidate".to_owned(),
            description: "Inspect a candidate.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {"candidate_id": {"type": "string"}}
            }),
            strict: true,
        },
        CodecToolDefinition {
            name: "run_gate".to_owned(),
            description: "Run one candidate gate.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {"gate": {"type": "string"}}
            }),
            strict: false,
        },
    ]
}

fn turn_input<'a>(
    tools: &'a [CodecToolDefinition],
    continuation: Option<&'a NativeContinuation>,
) -> NativeTurnInput<'a> {
    NativeTurnInput {
        wire_model: "fixture-model",
        system_prompt: "You are AlloyPort.",
        initial_user_text: continuation.is_none().then_some("Port vector_add."),
        continuation,
        tools,
        max_output_tokens: 4096,
        reasoning_effort: None,
        reasoning_mode: ReasoningMode::Disabled,
    }
}

fn results<'a>(first: &'a str, second: &'a str) -> [ModelVisibleToolResult<'a>; 2] {
    [
        ModelVisibleToolResult {
            native_call_id: second,
            output: "source-gate-failed",
        },
        ModelVisibleToolResult {
            native_call_id: first,
            output: "inspection-ok",
        },
    ]
}

fn json_body(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("fixture request must be JSON")
}

#[test]
fn chat_fixture_replays_exact_assistant_message_and_correlated_tool_messages() {
    let codec = OpenAiChatCompletionsCodec::default();
    let tools = tools();
    let prepared = codec
        .prepare(turn_input(&tools, None))
        .expect("initial Chat request must prepare");
    let decoded = codec
        .decode(
            &prepared,
            RawModelResponseRef {
                body: CHAT_RESPONSE,
            },
        )
        .expect("Chat fixture must decode");

    assert_eq!(decoded.turn.stop_reason, NormalizedStopReason::ToolCalls);
    assert_eq!(decoded.turn.narrative, ["I will inspect both candidates."]);
    assert_eq!(decoded.turn.tool_calls[0].raw_arguments, b"{broken");
    assert_eq!(
        decoded.provider_response_id.as_deref(),
        Some("chatcmpl_fixture_1")
    );
    assert_eq!(
        decoded.actual_model.as_deref(),
        Some("fixture-model-actual")
    );
    assert_eq!(
        decoded.turn.usage,
        Some(ModelUsage {
            input_tokens: 17,
            output_tokens: 9,
            cached_input_tokens: Some(4),
            cost_micros: None,
        })
    );
    assert_eq!(
        decoded.raw_response_digest,
        Sha256Digest::digest_bytes(CHAT_RESPONSE)
    );
    assert_eq!(
        decoded.native_continuation.pending_call_ids(),
        ["chat_call_a", "chat_call_b"]
    );

    let continued = codec
        .append_tool_results(
            &decoded.native_continuation,
            &results("chat_call_a", "chat_call_b"),
        )
        .expect("Chat results must correlate regardless of input order");
    let followup = codec
        .prepare(turn_input(&tools, Some(&continued)))
        .expect("Chat follow-up must prepare");
    assert_eq!(
        json_body(followup.body()),
        serde_json::from_str::<Value>(CHAT_FOLLOWUP).expect("golden fixture must parse")
    );

    let second = codec
        .decode(
            &followup,
            RawModelResponseRef {
                body: CHAT_SECOND_RESPONSE,
            },
        )
        .expect("second Chat tool turn must decode");
    assert_eq!(second.turn.tool_calls[0].native_call_id, "chat_call_c");
    assert_eq!(
        second.native_continuation.native_history()[2]["reasoning_content"],
        "opaque-compatible-profile-state"
    );
    let completed = codec
        .append_tool_results(
            &second.native_continuation,
            &[ModelVisibleToolResult {
                native_call_id: "chat_call_c",
                output: "second-inspection-ok",
            }],
        )
        .expect("second Chat result must correlate");
    let second_followup = codec
        .prepare(turn_input(&tools, Some(&completed)))
        .expect("second Chat follow-up must prepare");
    assert_eq!(
        json_body(second_followup.body())["messages"][6]["tool_call_id"],
        "chat_call_c"
    );
}

#[test]
fn chat_request_carries_the_configured_reasoning_effort() {
    let codec = OpenAiChatCompletionsCodec::default();
    let tools = tools();
    let mut input = turn_input(&tools, None);
    input.reasoning_effort = Some(ReasoningEffort::High);
    input.reasoning_mode = ReasoningMode::Enabled;
    let prepared = codec
        .with_thinking_parameter(true)
        .prepare(input)
        .expect("Chat request must prepare");
    assert_eq!(json_body(prepared.body())["reasoning_effort"], "high");
    assert_eq!(json_body(prepared.body())["thinking"]["type"], "enabled");
}

#[test]
fn responses_fixture_replays_every_output_item_and_encrypted_reasoning() {
    let codec = OpenAiResponsesCodec::default();
    let tools = tools();
    let prepared = codec
        .prepare(turn_input(&tools, None))
        .expect("initial Responses request must prepare");
    assert_eq!(json_body(prepared.body())["store"], false);
    let decoded = codec
        .decode(
            &prepared,
            RawModelResponseRef {
                body: RESPONSES_RESPONSE,
            },
        )
        .expect("Responses fixture must decode");

    assert_eq!(decoded.turn.stop_reason, NormalizedStopReason::ToolCalls);
    assert_eq!(decoded.turn.tool_calls[0].raw_arguments, b"{broken");
    assert_eq!(
        decoded.native_continuation.pending_call_ids(),
        ["resp_call_a", "resp_call_b"]
    );
    assert_eq!(
        decoded.native_continuation.native_history()[1]["encrypted_content"],
        "encrypted-reasoning-state"
    );
    assert_eq!(
        decoded
            .native_continuation
            .digest()
            .expect("continuation must serialize"),
        Sha256Digest::digest_bytes(
            &decoded
                .native_continuation
                .canonical_bytes()
                .expect("continuation must serialize")
        )
    );
    assert_eq!(
        NativeContinuation::from_canonical_bytes(
            &decoded
                .native_continuation
                .canonical_bytes()
                .expect("continuation must serialize"),
            CodecLimits::default(),
        )
        .expect("continuation Artifact must rehydrate"),
        decoded.native_continuation
    );

    let continued = codec
        .append_tool_results(
            &decoded.native_continuation,
            &results("resp_call_a", "resp_call_b"),
        )
        .expect("Responses results must correlate by call_id");
    let followup = codec
        .prepare(turn_input(&tools, Some(&continued)))
        .expect("Responses follow-up must prepare");
    assert_eq!(
        json_body(followup.body()),
        serde_json::from_str::<Value>(RESPONSES_FOLLOWUP).expect("golden fixture must parse")
    );

    let second = codec
        .decode(
            &followup,
            RawModelResponseRef {
                body: RESPONSES_SECOND_RESPONSE,
            },
        )
        .expect("second Responses tool turn must decode");
    assert_eq!(
        second.native_continuation.native_history()[7]["encrypted_content"],
        "second-encrypted-reasoning-state"
    );
    let completed = codec
        .append_tool_results(
            &second.native_continuation,
            &[ModelVisibleToolResult {
                native_call_id: "resp_call_c",
                output: "second-inspection-ok",
            }],
        )
        .expect("second Responses result must correlate");
    let second_followup = codec
        .prepare(turn_input(&tools, Some(&completed)))
        .expect("second Responses follow-up must prepare");
    assert_eq!(
        json_body(second_followup.body())["input"][9]["call_id"],
        "resp_call_c"
    );
}

#[test]
fn anthropic_fixture_preserves_thinking_signature_and_block_order() {
    let codec = AnthropicMessagesCodec::default();
    let tools = tools();
    let prepared = codec
        .prepare(turn_input(&tools, None))
        .expect("initial Anthropic request must prepare");
    let decoded = codec
        .decode(
            &prepared,
            RawModelResponseRef {
                body: ANTHROPIC_RESPONSE,
            },
        )
        .expect("Anthropic fixture must decode");

    assert_eq!(decoded.turn.stop_reason, NormalizedStopReason::ToolCalls);
    assert_eq!(
        decoded.native_continuation.pending_call_ids(),
        ["anthropic_call_a", "anthropic_call_b"]
    );
    let blocks = decoded.native_continuation.native_history()[1]["content"]
        .as_array()
        .expect("assistant content must remain an array");
    assert_eq!(blocks[0]["signature"], "signed-thinking-state");
    assert_eq!(blocks[3]["type"], "redacted_thinking");

    let continued = codec
        .append_tool_results(
            &decoded.native_continuation,
            &results("anthropic_call_a", "anthropic_call_b"),
        )
        .expect("Anthropic results must correlate by tool_use_id");
    let followup = codec
        .prepare(turn_input(&tools, Some(&continued)))
        .expect("Anthropic follow-up must prepare");
    assert_eq!(
        json_body(followup.body()),
        serde_json::from_str::<Value>(ANTHROPIC_FOLLOWUP).expect("golden fixture must parse")
    );

    let second = codec
        .decode(
            &followup,
            RawModelResponseRef {
                body: ANTHROPIC_SECOND_RESPONSE,
            },
        )
        .expect("second Anthropic tool turn must decode");
    assert_eq!(
        second.native_continuation.native_history()[3]["content"][0]["signature"],
        "second-signed-thinking-state"
    );
    let completed = codec
        .append_tool_results(
            &second.native_continuation,
            &[ModelVisibleToolResult {
                native_call_id: "anthropic_call_c",
                output: "second-inspection-ok",
            }],
        )
        .expect("second Anthropic result must correlate");
    let second_followup = codec
        .prepare(turn_input(&tools, Some(&completed)))
        .expect("second Anthropic follow-up must prepare");
    assert_eq!(
        json_body(second_followup.body())["messages"][4]["content"][0]["tool_use_id"],
        "anthropic_call_c"
    );
}

#[test]
fn native_call_ids_and_result_sets_fail_closed() {
    let codec = OpenAiChatCompletionsCodec::default();
    let tools = tools();
    let prepared = codec
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    let duplicate = br#"{
        "choices":[{"message":{"role":"assistant","tool_calls":[
          {"id":"same","type":"function","function":{"name":"inspect_candidate","arguments":"{}"}},
          {"id":"same","type":"function","function":{"name":"run_gate","arguments":"{}"}}
        ]},"finish_reason":"tool_calls"}]
    }"#;
    assert_eq!(
        codec.decode(&prepared, RawModelResponseRef { body: duplicate }),
        Err(CodecError::DuplicateCallId("same".to_owned()))
    );

    let decoded = codec
        .decode(
            &prepared,
            RawModelResponseRef {
                body: CHAT_RESPONSE,
            },
        )
        .expect("fixture must decode");
    assert_eq!(
        codec.append_tool_results(
            &decoded.native_continuation,
            &[ModelVisibleToolResult {
                native_call_id: "chat_call_a",
                output: "only-one",
            }],
        ),
        Err(CodecError::ToolResultMismatch)
    );
    assert_eq!(
        codec.append_tool_results(
            &decoded.native_continuation,
            &[
                ModelVisibleToolResult {
                    native_call_id: "chat_call_a",
                    output: "",
                },
                ModelVisibleToolResult {
                    native_call_id: "chat_call_b",
                    output: "ok",
                },
            ],
        ),
        Err(CodecError::EmptyToolResult("chat_call_a".to_owned()))
    );
    assert_eq!(
        codec.prepare(turn_input(&tools, Some(&decoded.native_continuation))),
        Err(CodecError::PendingToolResults)
    );
    assert!(matches!(
        OpenAiResponsesCodec::default().append_tool_results(&decoded.native_continuation, &[]),
        Err(CodecError::ProtocolMismatch { .. })
    ));
}

#[test]
fn unsupported_native_items_and_missing_ids_are_rejected() {
    let tools = tools();
    let responses = OpenAiResponsesCodec::default();
    let responses_request = responses
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    let missing_call_id = br#"{
      "status":"completed",
      "output":[{"type":"function_call","name":"inspect_candidate","arguments":"{}"}]
    }"#;
    assert!(matches!(
        responses.decode(
            &responses_request,
            RawModelResponseRef {
                body: missing_call_id
            }
        ),
        Err(CodecError::InvalidShape(_))
    ));

    let anthropic = AnthropicMessagesCodec::default();
    let anthropic_request = anthropic
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    let server_tool = br#"{
      "type":"message","role":"assistant","stop_reason":"tool_use",
      "content":[{"type":"server_tool_use","id":"srv","name":"web_search","input":{}}]
    }"#;
    assert_eq!(
        anthropic.decode(
            &anthropic_request,
            RawModelResponseRef { body: server_tool }
        ),
        Err(CodecError::UnsupportedNativeItem(
            "server_tool_use".to_owned()
        ))
    );
}

#[test]
fn malformed_schema_unknown_tools_and_empty_turns_fail_closed() {
    let tools = tools();
    let chat = OpenAiChatCompletionsCodec::default();
    let request = chat
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    assert_eq!(
        chat.decode(&request, RawModelResponseRef { body: b"not-json" }),
        Err(CodecError::InvalidJson)
    );
    assert!(matches!(
        chat.decode(
            &request,
            RawModelResponseRef {
                body: br#"{"choices":{}}"#
            }
        ),
        Err(CodecError::InvalidShape(_))
    ));
    let unknown_tool = br#"{
      "choices":[{"message":{"role":"assistant","tool_calls":[
        {"id":"call","type":"function","function":{"name":"not_offered","arguments":"{}"}}
      ]},"finish_reason":"tool_calls"}]
    }"#;
    assert_eq!(
        chat.decode(&request, RawModelResponseRef { body: unknown_tool }),
        Err(CodecError::UnknownTool("not_offered".to_owned()))
    );
    let empty_turn = br#"{
      "choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}]
    }"#;
    assert!(matches!(
        chat.decode(&request, RawModelResponseRef { body: empty_turn }),
        Err(CodecError::InvalidShape(_))
    ));
    assert_eq!(
        NativeContinuation::from_canonical_bytes(
            br#"{
              "schema_version":2,
              "protocol":"openai_responses",
              "native_history":[],
              "pending_call_ids":[]
            }"#,
            CodecLimits::default(),
        ),
        Err(CodecError::UnsupportedContinuationSchema(2))
    );
}

#[test]
fn protocol_stop_reasons_are_normalized_without_vendor_branches() {
    let tools = tools();
    let chat = OpenAiChatCompletionsCodec::default();
    let chat_request = chat
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    let refusal = br#"{
      "choices":[{"message":{"role":"assistant","content":null,"refusal":"not allowed"},
      "finish_reason":"stop"}]
    }"#;
    assert_eq!(
        chat.decode(&chat_request, RawModelResponseRef { body: refusal })
            .expect("refusal must decode")
            .turn
            .stop_reason,
        NormalizedStopReason::Refusal
    );
    let filtered = br#"{
      "choices":[{"message":{"role":"assistant","content":"partial"},
      "finish_reason":"content_filter"}]
    }"#;
    assert_eq!(
        chat.decode(&chat_request, RawModelResponseRef { body: filtered })
            .expect("filtered response must decode")
            .turn
            .stop_reason,
        NormalizedStopReason::ContentFilter
    );
    let unknown = br#"{
      "choices":[{"message":{"role":"assistant","content":"provider-specific stop"},
      "finish_reason":"new_reason"}]
    }"#;
    assert_eq!(
        chat.decode(&chat_request, RawModelResponseRef { body: unknown })
            .expect("unknown stop response must decode")
            .turn
            .stop_reason,
        NormalizedStopReason::Unknown
    );

    let responses = OpenAiResponsesCodec::default();
    let responses_request = responses
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    let incomplete = br#"{
      "status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},
      "output":[{"type":"message","role":"assistant","content":[
        {"type":"output_text","text":"partial"}]}]
    }"#;
    assert_eq!(
        responses
            .decode(&responses_request, RawModelResponseRef { body: incomplete })
            .expect("incomplete response must decode")
            .turn
            .stop_reason,
        NormalizedStopReason::Length
    );

    let anthropic = AnthropicMessagesCodec::default();
    let anthropic_request = anthropic
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    let max_tokens = br#"{
      "type":"message","role":"assistant","stop_reason":"max_tokens",
      "content":[{"type":"text","text":"partial"}]
    }"#;
    assert_eq!(
        anthropic
            .decode(&anthropic_request, RawModelResponseRef { body: max_tokens })
            .expect("truncated message must decode")
            .turn
            .stop_reason,
        NormalizedStopReason::Length
    );
}

#[test]
fn codec_limits_apply_before_native_data_is_retained() {
    let limits = CodecLimits {
        max_response_bytes: 8,
        ..CodecLimits::default()
    };
    let codec = OpenAiChatCompletionsCodec::new(limits).expect("limits must be valid");
    let tools = tools();
    let prepared = codec
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    assert!(matches!(
        codec.decode(
            &prepared,
            RawModelResponseRef {
                body: CHAT_RESPONSE
            }
        ),
        Err(CodecError::LimitExceeded {
            field: "model response",
            ..
        })
    ));
    let argument_codec = OpenAiChatCompletionsCodec::new(CodecLimits {
        max_tool_argument_bytes: 2,
        ..CodecLimits::default()
    })
    .expect("limits must be valid");
    let argument_request = argument_codec
        .prepare(turn_input(&tools, None))
        .expect("request must prepare");
    assert!(matches!(
        argument_codec.decode(
            &argument_request,
            RawModelResponseRef {
                body: CHAT_RESPONSE
            }
        ),
        Err(CodecError::LimitExceeded {
            field: "tool arguments",
            ..
        })
    ));
    let continuation_codec = OpenAiResponsesCodec::new(CodecLimits {
        max_continuation_bytes: 1,
        ..CodecLimits::default()
    })
    .expect("limits must be valid");
    assert!(matches!(
        continuation_codec.prepare(turn_input(&tools, None)),
        Err(CodecError::LimitExceeded {
            field: "native continuation",
            ..
        })
    ));
    assert!(matches!(
        OpenAiResponsesCodec::new(CodecLimits {
            max_tool_calls: 0,
            ..CodecLimits::default()
        }),
        Err(CodecError::InvalidLimits)
    ));
}
