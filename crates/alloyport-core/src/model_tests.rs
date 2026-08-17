use super::*;
use crate::{
    EpisodeId, GatewayToolCall, GatewayTurn, GatewayTurnExchange, ModelAttemptId, ModelGateway,
    ModelGatewayOutcome, ModelTurnRequest, NormalizedStopReason, ScriptedFakeModelGateway,
    ScriptedGatewayStep, TurnId, TurnRecord, TurnRecordError, TurnSpec,
};
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn complete_immediate<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the core scripted gateway must complete without an async runtime"),
    }
}

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

#[test]
fn checked_runtime_model_catalog_example_resolves_the_configured_default()
-> Result<(), Box<dyn Error>> {
    let catalog: RuntimeModelCatalog = serde_json::from_slice(include_bytes!(
        "../../../docs/runtime-model-catalog.example.json"
    ))?;
    let resolved = catalog.resolve(None)?;
    assert_eq!(resolved.alias(), "deepseek-v4-pro-default");
    assert_eq!(
        resolved.protocol_kind(),
        ProtocolKind::OpenAiChatCompletions
    );
    Ok(())
}

fn catalog_json(protocol: &str, auth: &str) -> String {
    let conformance = digest("conformance");
    format!(
        r#"{{
          "schema_version": 1,
          "default_runtime_model": "deepseek-v4-pro-default",
          "runtime_models": {{
            "deepseek-v4-pro-default": {{
              "wire_model": "deepseek-v4-pro",
              "deployment": "deepseek-official",
              "profile": "deepseek-v4",
              "settings": {{
                "max_output_tokens": 16384,
                "temperature_millis": 200,
                "reasoning": {{"mode": "enabled", "effort": "high"}}
              }}
            }}
          }},
          "deployments": {{
            "deepseek-official": {{
              "vendor": "deepseek",
              "protocol": {protocol},
              "endpoint": "https://api.deepseek.com/model-endpoint",
              "auth": {auth},
              "transport": {{
                "connect_timeout_millis": 5000,
                "request_timeout_millis": 30000,
                "max_request_bytes": 8388608,
                "max_response_bytes": 8388608,
                "max_response_header_bytes": 65536,
                "max_diagnostic_bytes": 65536,
                "tls_minimum_version": "tls12",
                "redirects": "disabled",
                "proxy": "disabled"
              }},
              "data_boundary": "external_provider",
              "conformance_receipt_digest": "{conformance}"
            }}
          }},
          "profiles": {{
            "deepseek-v4": {{
              "supported_protocols": ["openai_chat_completions", "anthropic_messages"],
              "supports_tools": true,
              "supports_parallel_tool_calls": true,
              "supports_reasoning": true,
              "tool_schema_dialect": "json_schema",
              "max_context_tokens": 1000000,
              "max_output_tokens": 384000
            }}
          }}
        }}"#
    )
}

#[test]
fn same_deepseek_model_resolves_over_two_protocols_without_vendor_logic()
-> Result<(), Box<dyn Error>> {
    let chat: RuntimeModelCatalog = serde_json::from_str(&catalog_json(
        r#"{"kind":"openai_chat_completions"}"#,
        r#"{"kind":"bearer_file","path":"/run/secrets/deepseek"}"#,
    ))?;
    let anthropic: RuntimeModelCatalog = serde_json::from_str(&catalog_json(
        r#"{"kind":"anthropic_messages","api_version":"2023-06-01"}"#,
        r#"{"kind":"x_api_key_file","path":"/run/secrets/deepseek"}"#,
    ))?;

    let chat = chat.resolve(None)?;
    let anthropic = anthropic.resolve(None)?;
    assert_eq!(chat.wire_model(), "deepseek-v4-pro");
    assert_eq!(anthropic.wire_model(), "deepseek-v4-pro");
    assert_eq!(chat.protocol_kind(), ProtocolKind::OpenAiChatCompletions);
    assert_eq!(anthropic.protocol_kind(), ProtocolKind::AnthropicMessages);
    Ok(())
}

#[test]
fn catalog_rejects_unknown_fields_and_hosted_responses_state() -> Result<(), Box<dyn Error>> {
    let with_unknown = catalog_json(
        r#"{"kind":"openai_chat_completions","surprise":true}"#,
        r#"{"kind":"bearer_file","path":"/run/secrets/deepseek"}"#,
    );
    assert!(serde_json::from_str::<RuntimeModelCatalog>(&with_unknown).is_err());

    let responses = catalog_json(
        r#"{"kind":"openai_responses","store":true}"#,
        r#"{"kind":"bearer_file","path":"/run/secrets/deepseek"}"#,
    )
    .replace(
        r#"["openai_chat_completions", "anthropic_messages"]"#,
        r#"["openai_responses"]"#,
    );
    let responses: RuntimeModelCatalog = serde_json::from_str(&responses)?;
    assert_eq!(
        responses.validate(),
        Err(ModelCatalogError::HostedStateUnsupported(
            "deepseek-official".to_owned()
        ))
    );
    Ok(())
}

#[test]
fn catalog_rejects_unsafe_transport_inputs_without_coupling_auth_to_protocol()
-> Result<(), Box<dyn Error>> {
    let insecure = catalog_json(
        r#"{"kind":"openai_chat_completions"}"#,
        r#"{"kind":"bearer_file","path":"/run/secrets/deepseek"}"#,
    )
    .replace("https://api.deepseek.com", "http://api.deepseek.com");
    let insecure: RuntimeModelCatalog = serde_json::from_str(&insecure)?;
    assert!(matches!(
        insecure.validate(),
        Err(ModelCatalogError::UnsafeEndpoint(_))
    ));

    let independent_auth = catalog_json(
        r#"{"kind":"anthropic_messages","api_version":"2023-06-01"}"#,
        r#"{"kind":"bearer_file","path":"/run/secrets/deepseek"}"#,
    );
    let independent_auth: RuntimeModelCatalog = serde_json::from_str(&independent_auth)?;
    independent_auth.validate()?;

    let relative_secret = catalog_json(
        r#"{"kind":"openai_chat_completions"}"#,
        r#"{"kind":"bearer_file","path":"relative-secret"}"#,
    );
    let relative_secret: RuntimeModelCatalog = serde_json::from_str(&relative_secret)?;
    assert!(matches!(
        relative_secret.validate(),
        Err(ModelCatalogError::UnsafeSecretPath(_))
    ));
    Ok(())
}

#[test]
fn model_attempt_preserves_ambiguous_dispatch_as_terminal_history() -> Result<(), Box<dyn Error>> {
    let mut attempt = ModelAttemptRecord::new(ModelAttemptSpec {
        id: ModelAttemptId::try_from("attempt-1")?,
        episode_id: EpisodeId::try_from("episode-1")?,
        attempt_number: 1,
        request_digest: digest("request"),
        resolved_model_digest: digest("model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
        predecessor_attempt_id: None,
        predecessor_continuation_digest: None,
    })?;
    attempt.mark_dispatching()?;
    attempt.finish_without_response(ModelAttemptStatus::Ambiguous, Some(digest("diagnostic")))?;
    assert_eq!(attempt.status(), ModelAttemptStatus::Ambiguous);
    assert!(attempt.status().is_terminal());
    assert!(attempt.mark_dispatching().is_err());

    let value = serde_json::to_value(&attempt)?;
    for field in [
        "request_digest",
        "resolved_model_digest",
        "deployment_digest",
        "model_profile_digest",
        "request_budget_digest",
        "predecessor_attempt_id",
        "predecessor_continuation_digest",
    ] {
        assert!(
            value.get(field).is_some(),
            "missing attempt identity {field}"
        );
    }
    Ok(())
}

fn tool_turn(call_id: &str, name: &str) -> GatewayTurn {
    GatewayTurn {
        narrative: Vec::new(),
        tool_calls: vec![GatewayToolCall {
            native_call_id: call_id.to_owned(),
            name: name.to_owned(),
            raw_arguments: br#"{"candidate":"candidate-1"}"#.to_vec(),
        }],
        stop_reason: NormalizedStopReason::ToolCalls,
        usage: None,
    }
}

fn exchange(label: &str, turn: GatewayTurn) -> GatewayTurnExchange {
    GatewayTurnExchange {
        turn,
        raw_exchange_digest: digest(&format!("{label}-raw")),
        native_continuation_digest: digest(&format!("{label}-continuation")),
    }
}

#[test]
fn scripted_gateway_proves_two_tool_turns_without_network() -> Result<(), Box<dyn Error>> {
    complete_immediate(scripted_gateway_two_turn_case())
}

async fn scripted_gateway_two_turn_case() -> Result<(), Box<dyn Error>> {
    let first_input = digest("initial-context");
    let second_input = digest("source-gate-result");
    let mut gateway = ScriptedFakeModelGateway::new([
        ScriptedGatewayStep {
            expected_turn_index: 1,
            expected_input_digest: first_input,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "first",
                tool_turn("call-1", "submit_candidate_bundle"),
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 2,
            expected_input_digest: second_input,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "second",
                tool_turn("call-2", "request_source_gate"),
            )),
        },
    ]);

    for (index, input) in [(1, first_input), (2, second_input)] {
        let outcome = gateway
            .invoke(&ModelTurnRequest {
                attempt_id: ModelAttemptId::try_from(format!("attempt-{index}"))?,
                episode_id: EpisodeId::try_from("episode-1")?,
                turn_index: index,
                input_digest: input,
            })
            .await?;
        assert!(matches!(outcome, ModelGatewayOutcome::Turn(_)));
    }
    assert_eq!(gateway.remaining_steps(), 0);
    assert!(
        gateway
            .invoke(&ModelTurnRequest {
                attempt_id: ModelAttemptId::try_from("attempt-3")?,
                episode_id: EpisodeId::try_from("episode-1")?,
                turn_index: 3,
                input_digest: digest("unused"),
            })
            .await
            .is_err()
    );
    Ok(())
}

#[test]
fn durable_turn_binds_attempt_exchange_and_native_continuation() -> Result<(), Box<dyn Error>> {
    let continuation = digest("native-continuation");
    let turn = TurnRecord::new(TurnSpec {
        id: TurnId::try_from("turn-1")?,
        episode_id: EpisodeId::try_from("episode-1")?,
        model_attempt_id: ModelAttemptId::try_from("attempt-1")?,
        turn_index: 1,
        decoded_turn_digest: digest("decoded-turn"),
        raw_exchange_digest: digest("raw-exchange"),
        native_continuation_digest: continuation,
        stop_reason: NormalizedStopReason::ToolCalls,
        tool_call_count: 2,
        usage: None,
    })?;
    assert_eq!(turn.native_continuation_digest(), continuation);

    let zero_index = TurnRecord::new(TurnSpec {
        id: TurnId::try_from("turn-zero")?,
        episode_id: EpisodeId::try_from("episode-1")?,
        model_attempt_id: ModelAttemptId::try_from("attempt-1")?,
        turn_index: 0,
        decoded_turn_digest: digest("decoded-turn"),
        raw_exchange_digest: digest("raw-exchange"),
        native_continuation_digest: continuation,
        stop_reason: NormalizedStopReason::Stop,
        tool_call_count: 0,
        usage: None,
    });
    assert_eq!(zero_index, Err(TurnRecordError::ZeroTurnIndex));
    Ok(())
}

#[test]
fn gateway_rejects_duplicate_native_tool_call_ids() {
    let mut turn = tool_turn("duplicate", "first");
    turn.tool_calls.push(GatewayToolCall {
        native_call_id: "duplicate".to_owned(),
        name: "second".to_owned(),
        raw_arguments: b"{}".to_vec(),
    });
    assert!(turn.validate().is_err());
}
