use super::*;
use crate::model_context::ContextRecordingToolGateway;
use alloyport_artifacts::{ArtifactStore, InMemoryArtifactStore, IngestRequest};
use alloyport_core::{
    AgentToolGateway, GatewayToolCall, GatewayTurn, GatewayTurnExchange, ModelAttemptId,
    NormalizedStopReason, RuntimeToolDescriptor, ScriptedFakeToolGateway, ScriptedToolStep,
    ToolEffectClass, ToolGatewayAction, ToolGatewayOutcome, ToolInvocation, ToolOperationId,
    ToolOperationStatus, ToolResultAuthority,
};
use alloyport_llm_provider::ModelTurnContextStore;
use serde_json::json;
use std::io::Cursor;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn tools() -> Vec<CodecToolDefinition> {
    vec![CodecToolDefinition {
        name: "submit_candidate_bundle".to_owned(),
        description: "Submit one generated source bundle.".to_owned(),
        input_schema: json!({"type":"object"}),
        strict: true,
    }]
}

fn request(
    episode_id: &EpisodeId,
    turn_index: u32,
    input_digest: Sha256Digest,
) -> Result<ModelTurnRequest, Box<dyn std::error::Error>> {
    Ok(ModelTurnRequest {
        attempt_id: ModelAttemptId::try_from(format!("model-attempt-context-{turn_index}"))?,
        episode_id: episode_id.clone(),
        turn_index,
        input_digest,
    })
}

fn continuation() -> Result<NativeContinuation, Box<dyn std::error::Error>> {
    Ok(NativeContinuation::from_canonical_bytes(
        &serde_json::to_vec(&json!({
            "schema_version": 1,
            "protocol": "openai_chat_completions",
            "native_history": {"messages": []},
            "pending_call_ids": ["call-a", "call-b"]
        }))?,
        CodecLimits::default(),
    )?)
}

fn exchange(continuation: NativeContinuation) -> Result<ProviderTurnExchange, String> {
    let continuation_digest = continuation.digest().map_err(adapter_error)?;
    Ok(ProviderTurnExchange {
        gateway_exchange: GatewayTurnExchange {
            turn: GatewayTurn {
                narrative: vec!["submitting candidates".to_owned()],
                tool_calls: vec![
                    GatewayToolCall {
                        native_call_id: "call-a".to_owned(),
                        name: "submit_candidate_bundle".to_owned(),
                        raw_arguments: br#"{"candidate":"a"}"#.to_vec(),
                    },
                    GatewayToolCall {
                        native_call_id: "call-b".to_owned(),
                        name: "submit_candidate_bundle".to_owned(),
                        raw_arguments: br#"{"candidate":"b"}"#.to_vec(),
                    },
                ],
                stop_reason: NormalizedStopReason::ToolCalls,
                usage: None,
            },
            raw_exchange_digest: Sha256Digest::digest_bytes(b"response"),
            native_continuation_digest: continuation_digest,
        },
        request_body: b"request".to_vec(),
        response_body: b"response".to_vec(),
        native_continuation: continuation,
        provider_request_id: Some("provider-request-context".to_owned()),
        actual_model: Some("configured-model".to_owned()),
    })
}

fn ingest_result(
    artifacts: &dyn ArtifactStore,
    bytes: &[u8],
) -> Result<Sha256Digest, Box<dyn std::error::Error>> {
    let digest = Sha256Digest::digest_bytes(bytes);
    artifacts.ingest(
        &mut Cursor::new(bytes),
        IngestRequest {
            expected_digest: Some(digest),
            expected_size_bytes: Some(u64::try_from(bytes.len())?),
        },
    )?;
    Ok(digest)
}

fn terminal(result_digest: Sha256Digest) -> ToolGatewayOutcome {
    ToolGatewayOutcome::Completed {
        status: ToolOperationStatus::Succeeded,
        result_digest,
        receipt_digests: Vec::new(),
        satisfies_subtask: false,
    }
}

#[tokio::test]
async fn context_store_reopens_exact_continuation_and_tool_results()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("model-context.sqlite3");
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(1024 * 1024));
    let episode_id = EpisodeId::try_from("episode-model-context")?;
    let store = Arc::new(SqliteModelContextStore::open(
        &database,
        artifacts.clone(),
        CodecLimits::default(),
    )?);
    let initial = store.create_episode(
        &episode_id,
        "You author candidates.",
        "Port the reduction fixture.",
        &tools(),
    )?;
    let first = request(&episode_id, 1, initial)?;
    let mut contexts = SharedSqliteModelContextStore::new(store.clone());
    let loaded = contexts.load(&first)?;
    assert_eq!(
        loaded.initial_user_text.as_deref(),
        Some("Port the reduction fixture.")
    );
    let exchange = exchange(continuation()?)?;
    contexts.commit(&first, &exchange)?;
    assert!(artifacts.contains(Sha256Digest::digest_bytes(b"request"))?);
    assert!(artifacts.contains(Sha256Digest::digest_bytes(b"response"))?);

    let result_a = ingest_result(artifacts.as_ref(), br#"{"candidate":"a"}"#)?;
    let result_b = ingest_result(artifacts.as_ref(), br#"{"candidate":"b"}"#)?;
    let descriptors = [RuntimeToolDescriptor {
        name: "submit_candidate_bundle".to_owned(),
        version: "1".to_owned(),
        effect_class: ToolEffectClass::CandidateWrite,
        result_authority: ToolResultAuthority::Observed,
    }];
    let steps = [result_a, result_b].map(|result_digest| ScriptedToolStep {
        action: ToolGatewayAction::Execute,
        expected_tool_name: "submit_candidate_bundle".to_owned(),
        outcome: terminal(result_digest),
    });
    let inner = ScriptedFakeToolGateway::new(descriptors, steps);
    let mut gateway = ContextRecordingToolGateway::new(inner, episode_id.clone(), store.clone());
    for (index, (native_call_id, result_digest)) in [("call-a", result_a), ("call-b", result_b)]
        .into_iter()
        .enumerate()
    {
        let invocation = ToolInvocation {
            operation_id: ToolOperationId::try_from(format!("tool-context-{}", index + 1))?,
            call: GatewayToolCall {
                native_call_id: native_call_id.to_owned(),
                name: "submit_candidate_bundle".to_owned(),
                raw_arguments: Vec::new(),
            },
            input_identity_digest: initial,
        };
        assert_eq!(gateway.execute(&invocation).await?, terminal(result_digest));
    }
    assert_eq!(gateway.inner().invocation_count(), 2);
    contexts.commit(&first, &exchange)?;

    let latest: String = store.connection()?.query_row(
        "SELECT latest_input_digest FROM model_episode_contexts WHERE episode_id = ?1",
        [episode_id.to_string()],
        |row| row.get(0),
    )?;
    assert_eq!(
        latest.parse::<Sha256Digest>()?,
        derive_model_continuation_input_digest(continuation()?.digest()?, [result_a, result_b],)
    );
    drop(contexts);
    drop(gateway);
    drop(store);

    let reopened = Arc::new(SqliteModelContextStore::open(
        &database,
        artifacts,
        CodecLimits::default(),
    )?);
    let mut contexts = SharedSqliteModelContextStore::new(reopened);
    let loaded = contexts.load(&request(&episode_id, 2, latest.parse()?)?)?;
    assert!(loaded.initial_user_text.is_none());
    assert_eq!(loaded.continuation, Some(continuation()?));
    assert_eq!(
        loaded
            .tool_results
            .iter()
            .map(|result| (result.native_call_id.as_str(), result.output.as_str()))
            .collect::<Vec<_>>(),
        [
            ("call-a", r#"{"candidate":"a"}"#),
            ("call-b", r#"{"candidate":"b"}"#)
        ]
    );
    Ok(())
}

#[test]
fn context_store_rejects_wrong_input_and_unrelated_result() -> Result<(), Box<dyn std::error::Error>>
{
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(1024 * 1024));
    let episode_id = EpisodeId::try_from("episode-model-context-invalid")?;
    let store = Arc::new(SqliteModelContextStore::in_memory(
        artifacts.clone(),
        CodecLimits::default(),
    )?);
    store.create_episode(&episode_id, "system", "user", &tools())?;
    let mut contexts = SharedSqliteModelContextStore::new(store.clone());
    assert!(
        contexts
            .load(&request(&episode_id, 1, digest("wrong"))?)
            .is_err()
    );
    let result = ingest_result(artifacts.as_ref(), b"result")?;
    assert!(
        store
            .record_tool_result(&episode_id, "unrelated-call", result)
            .is_err()
    );
    Ok(())
}
