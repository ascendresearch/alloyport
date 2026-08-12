use super::*;
use alloyport_core::{
    AgentEpisodeRecord, AgentLoopAdvance, AgentLoopPolicy, AgentLoopRunner, AgentLoopRuntimeSpec,
    DurableEpisodeState, EpisodeId, EpisodeRepository, EpisodeSpec, EpisodeStatus,
    InMemoryEpisodeRepository, ModelGatewayOutcome, ModelTransportOutcome, NoAgentRuntimeFault,
    RawModelResponse, RuntimeToolDescriptor, ScriptedFakeModelTransport, ScriptedFakeToolGateway,
    ScriptedModelTransportStep, SearchRunId, TaskId,
};
use serde_json::json;
use std::sync::Mutex;

const CHAT_FINAL_RESPONSE: &[u8] = br#"{
  "id":"chatcmpl-sdk-fixture",
  "model":"configured-model-actual",
  "choices":[{"message":{"role":"assistant","content":"ready","tool_calls":[]},"finish_reason":"stop"}],
  "usage":{"prompt_tokens":11,"completion_tokens":2,"total_tokens":13}
}"#;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn catalog() -> RuntimeModelCatalog {
    serde_json::from_value(json!({
        "schema_version": 1,
        "default_runtime_model": "deepseek-v4-pro-default",
        "runtime_models": {
            "deepseek-v4-pro-default": {
                "wire_model": "deepseek-v4-pro",
                "deployment": "configured-chat",
                "profile": "configured-profile",
                "settings": {
                    "max_output_tokens": 4096,
                    "temperature_millis": 200,
                    "reasoning": {"mode": "enabled", "effort": "high"}
                }
            }
        },
        "deployments": {
            "configured-chat": {
                "vendor": "replaceable-label",
                "protocol": {"kind": "openai_chat_completions"},
                "endpoint": "https://api.example.test/v1/chat/completions",
                "auth": {"kind": "bearer_file", "path": "/run/secrets/model-key"},
                "transport": {
                    "connect_timeout_millis": 5000,
                    "request_timeout_millis": 30000,
                    "max_request_bytes": 8_388_608,
                    "max_response_bytes": 8_388_608,
                    "max_response_header_bytes": 65536,
                    "max_diagnostic_bytes": 65536,
                    "tls_minimum_version": "tls12",
                    "redirects": "disabled",
                    "proxy": "disabled"
                },
                "data_boundary": "external_provider",
                "conformance_receipt_digest": digest("conformance")
            }
        },
        "profiles": {
            "configured-profile": {
                "supported_protocols": ["openai_chat_completions"],
                "supports_tools": true,
                "supports_parallel_tool_calls": true,
                "supports_reasoning": true,
                "tool_schema_dialect": "json_schema",
                "max_context_tokens": 100_000,
                "max_output_tokens": 8192
            }
        }
    }))
    .expect("catalog fixture")
}

fn tools() -> Vec<CodecToolDefinition> {
    vec![CodecToolDefinition {
        name: "inspect_candidate".to_owned(),
        description: "Inspect a candidate.".to_owned(),
        input_schema: json!({"type": "object"}),
        strict: false,
    }]
}

fn provider_input() -> ProviderTurnInput {
    ProviderTurnInput {
        system_prompt: "You are AlloyPort.".to_owned(),
        initial_user_text: Some("Inspect the candidate.".to_owned()),
        continuation: None,
        tool_results: Vec::new(),
        tools: tools(),
    }
}

#[derive(Debug)]
struct OneTurnContextStore {
    input: Mutex<Option<ProviderTurnInput>>,
    committed: Mutex<Vec<ProviderTurnExchange>>,
}

impl ModelTurnContextStore for OneTurnContextStore {
    fn load(&mut self, _request: &ModelTurnRequest) -> Result<ProviderTurnInput, String> {
        self.input
            .lock()
            .map_err(|_| "input lock poisoned".to_owned())?
            .take()
            .ok_or_else(|| "input is absent".to_owned())
    }

    fn commit(
        &mut self,
        _request: &ModelTurnRequest,
        exchange: &ProviderTurnExchange,
    ) -> Result<(), String> {
        self.committed
            .lock()
            .map_err(|_| "commit lock poisoned".to_owned())?
            .push(exchange.clone());
        Ok(())
    }
}

fn scripted_transport(
    deployment: &ResolvedRuntimeModel,
    input: &ProviderTurnInput,
) -> ScriptedFakeModelTransport {
    let codec = OpenAiChatCompletionsCodec::default();
    let prepared = codec
        .prepare(NativeTurnInput {
            wire_model: deployment.wire_model(),
            system_prompt: &input.system_prompt,
            initial_user_text: input.initial_user_text.as_deref(),
            continuation: None,
            tools: &input.tools,
            max_output_tokens: deployment.max_output_tokens(),
        })
        .expect("prepared request fixture");
    ScriptedFakeModelTransport::new([ScriptedModelTransportStep {
        expected_deployment_name: deployment.deployment_name().to_owned(),
        expected_protocol: ProtocolKind::OpenAiChatCompletions,
        expected_request_digest: Sha256Digest::digest_bytes(prepared.body()),
        outcome: ModelTransportOutcome::Response(RawModelResponse {
            status_code: 200,
            body: CHAT_FINAL_RESPONSE.to_vec(),
            response_headers_digest: digest("response-headers"),
            provider_request_id: Some("provider-request-fixture".to_owned()),
            retry_after_millis: None,
        }),
    }])
}

#[tokio::test]
async fn sdk_gateway_resolves_configured_default_and_commits_exact_exchange() {
    let catalog = catalog();
    let deployment = catalog.resolve(None).expect("default deployment");
    let input = provider_input();
    let transport = scripted_transport(&deployment, &input);
    let sdk = LlmProviderSdk::new(catalog, transport, CodecLimits::default()).expect("SDK");
    let contexts = OneTurnContextStore {
        input: Mutex::new(Some(input)),
        committed: Mutex::new(Vec::new()),
    };
    let mut gateway = ProviderModelGateway::new(sdk, None, contexts).expect("gateway");
    let outcome = gateway
        .invoke(&ModelTurnRequest {
            attempt_id: "attempt-sdk-1".try_into().expect("attempt ID"),
            episode_id: "episode-sdk-1".try_into().expect("episode ID"),
            turn_index: 1,
            input_digest: digest("initial-input"),
        })
        .await
        .expect("gateway outcome");
    let ModelGatewayOutcome::Turn(exchange) = outcome else {
        panic!("expected decoded turn");
    };
    assert_eq!(gateway.deployment().alias(), "deepseek-v4-pro-default");
    assert_eq!(exchange.turn.narrative, ["ready"]);
    let committed = gateway.contexts().committed.lock().expect("commit lock");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].response_body, CHAT_FINAL_RESPONSE);
    assert_eq!(
        committed[0].provider_request_id.as_deref(),
        Some("provider-request-fixture")
    );
}

fn runtime_state(deployment: &ResolvedRuntimeModel) -> DurableEpisodeState {
    let episode = AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from("episode-sdk-loop-1").expect("episode ID"),
        task_id: TaskId::try_from("task-sdk-1").expect("task ID"),
        search_run_id: SearchRunId::try_from("search-sdk-1").expect("search ID"),
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: deployment.alias().to_owned(),
        resolved_model_digest: digest("resolved-model"),
        prompt_revision: "fixture-v1".to_owned(),
        tool_catalog_digest: digest("tools"),
        loop_policy_digest: digest("loop-policy"),
        data_boundary_policy_digest: digest("boundary"),
        budget_snapshot_digest: digest("budget"),
    })
    .expect("episode");
    DurableEpisodeState::new(AgentLoopRuntimeSpec {
        episode,
        policy: AgentLoopPolicy {
            max_model_turns: 2,
            max_model_attempts: 2,
            max_ambiguous_model_attempts: 1,
            max_tool_calls_per_turn: 2,
            max_total_tool_operations: 2,
            max_stop_feedback_turns: 0,
        },
        initial_input_digest: digest("initial-input"),
        resolved_model_digest: digest("resolved-model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
    })
    .expect("runtime state")
}

#[tokio::test]
async fn agent_loop_invokes_the_provider_sdk_through_model_gateway() {
    let catalog = catalog();
    let deployment = catalog.resolve(None).expect("default deployment");
    let input = provider_input();
    let transport = scripted_transport(&deployment, &input);
    let sdk = LlmProviderSdk::new(catalog, transport, CodecLimits::default()).expect("SDK");
    let contexts = OneTurnContextStore {
        input: Mutex::new(Some(input)),
        committed: Mutex::new(Vec::new()),
    };
    let mut gateway = ProviderModelGateway::new(sdk, None, contexts).expect("gateway");
    let episode_id = EpisodeId::try_from("episode-sdk-loop-1").expect("episode ID");
    let mut repository = InMemoryEpisodeRepository::default();
    repository
        .create(runtime_state(&deployment))
        .expect("create episode");
    let mut tool_gateway =
        ScriptedFakeToolGateway::new(Vec::<RuntimeToolDescriptor>::new(), std::iter::empty());
    let runner = AgentLoopRunner::new(episode_id.clone());
    for _ in 0..3 {
        runner
            .advance(
                &mut repository,
                &mut gateway,
                &mut tool_gateway,
                &mut NoAgentRuntimeFault,
            )
            .await
            .expect("agent-loop advance");
    }
    let state = repository.load(&episode_id).expect("load state").state;
    assert_eq!(state.episode().status(), EpisodeStatus::TurnRecorded);
    assert_eq!(state.turn_count(), 1);
    assert_eq!(
        runner
            .advance(
                &mut repository,
                &mut gateway,
                &mut tool_gateway,
                &mut NoAgentRuntimeFault,
            )
            .await
            .expect("stop review"),
        AgentLoopAdvance::Progressed(EpisodeStatus::StopReview)
    );
}
