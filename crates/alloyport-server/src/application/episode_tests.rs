use super::*;
use alloyport_artifacts::InMemoryArtifactStore;
use alloyport_core::{
    ModelTransportOutcome, NativeTurnInput, OpenAiChatCompletionsCodec, ProtocolCodec,
    ProtocolKind, RawModelResponse, ScriptedFakeModelTransport, ScriptedFakeToolGateway,
    ScriptedModelTransportStep,
};
use serde_json::json;

const FINAL_RESPONSE: &[u8] = br#"{
  "id":"chatcmpl-controller-episode",
  "model":"configured-model-actual",
  "choices":[{"message":{"role":"assistant","content":"candidate ready","tool_calls":[]},"finish_reason":"stop"}],
  "usage":{"prompt_tokens":11,"completion_tokens":2,"total_tokens":13}
}"#;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn catalog() -> RuntimeModelCatalog {
    serde_json::from_value(json!({
        "schema_version": 1,
        "default_runtime_model": "configured-default",
        "runtime_models": {
            "configured-default": {
                "wire_model": "configured-model",
                "deployment": "configured-chat",
                "profile": "configured-profile",
                "settings": {
                    "max_output_tokens": 4096,
                    "temperature_millis": 200,
                    "reasoning": {"mode": "disabled"}
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
                "supports_reasoning": false,
                "tool_schema_dialect": "json_schema",
                "max_context_tokens": 100_000,
                "max_output_tokens": 8192
            }
        }
    }))
    .expect("catalog")
}

fn spec() -> Result<ControllerEpisodeSpec, Box<dyn Error>> {
    Ok(ControllerEpisodeSpec {
        episode_id: EpisodeId::try_from("episode-controller-composition")?,
        task_id: TaskId::try_from("task-controller-composition")?,
        search_run_id: SearchRunId::try_from("search-controller-composition")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: None,
        prompt_revision: "candidate-authoring-v1".to_owned(),
        tools: Vec::new(),
        loop_policy: AgentLoopPolicy {
            max_model_turns: 1,
            max_model_attempts: 1,
            max_ambiguous_model_attempts: 0,
            max_tool_calls_per_turn: 1,
            max_total_tool_operations: 1,
            max_stop_feedback_turns: 0,
        },
        data_boundary_policy_digest: digest("data-boundary"),
        budget_snapshot_digest: digest("budget"),
        request_budget_digest: digest("request-budget"),
        system_prompt: "You are AlloyPort.".to_owned(),
        initial_user_text: "Author one candidate.".to_owned(),
    })
}

fn transport(spec: &ControllerEpisodeSpec) -> ScriptedFakeModelTransport {
    let catalog = catalog();
    let deployment = catalog.resolve(None).expect("deployment");
    let codec = OpenAiChatCompletionsCodec::default();
    let prepared = codec
        .prepare(NativeTurnInput {
            wire_model: deployment.wire_model(),
            system_prompt: &spec.system_prompt,
            initial_user_text: Some(&spec.initial_user_text),
            continuation: None,
            tools: &spec.tools,
            max_output_tokens: deployment.max_output_tokens(),
        })
        .expect("prepared request");
    ScriptedFakeModelTransport::new([ScriptedModelTransportStep {
        expected_deployment_name: deployment.deployment_name().to_owned(),
        expected_protocol: ProtocolKind::OpenAiChatCompletions,
        expected_request_digest: Sha256Digest::digest_bytes(prepared.body()),
        outcome: ModelTransportOutcome::Response(RawModelResponse {
            status_code: 200,
            body: FINAL_RESPONSE.to_vec(),
            response_headers_digest: digest("headers"),
            provider_request_id: Some("provider-request-controller".to_owned()),
            retry_after_millis: None,
        }),
    }])
}

#[tokio::test]
async fn controller_episode_composition_runs_and_recovers_terminal_state()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("controller-episode.sqlite3");
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(8 * 1024 * 1024));
    let spec = spec()?;
    let mut application = ControllerEpisodeApplication::open(
        spec.clone(),
        catalog(),
        transport(&spec),
        CodecLimits::default(),
        artifacts.clone(),
        &database,
        ScriptedFakeToolGateway::new([], []),
    )?;
    for _ in 0..5 {
        application.advance().await?;
    }
    assert_eq!(application.status()?, EpisodeStatus::Incomplete);
    drop(application);

    let mut recovered = ControllerEpisodeApplication::open(
        spec,
        catalog(),
        ScriptedFakeModelTransport::new([]),
        CodecLimits::default(),
        artifacts,
        &database,
        ScriptedFakeToolGateway::new([], []),
    )?;
    assert_eq!(recovered.status()?, EpisodeStatus::Incomplete);
    assert_eq!(
        recovered.advance().await?,
        AgentLoopAdvance::Terminal(EpisodeStatus::Incomplete)
    );
    Ok(())
}

#[test]
fn controller_episode_rejects_conflicting_recovery_context() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("controller-episode-conflict.sqlite3");
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(8 * 1024 * 1024));
    let spec = spec()?;
    let _application = ControllerEpisodeApplication::open(
        spec.clone(),
        catalog(),
        ScriptedFakeModelTransport::new([]),
        CodecLimits::default(),
        artifacts.clone(),
        &database,
        ScriptedFakeToolGateway::new([], []),
    )?;
    let mut conflicting = spec;
    conflicting.system_prompt = "changed prompt".to_owned();
    assert!(
        ControllerEpisodeApplication::open(
            conflicting,
            catalog(),
            ScriptedFakeModelTransport::new([]),
            CodecLimits::default(),
            artifacts,
            &database,
            ScriptedFakeToolGateway::new([], []),
        )
        .is_err()
    );
    Ok(())
}
