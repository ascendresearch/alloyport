use super::*;
use alloyport_core::{
    AnthropicMessagesCodec, CodecToolDefinition, NativeTurnInput, OpenAiChatCompletionsCodec,
    OpenAiResponsesCodec, ProtocolCodec, ProtocolKind, RuntimeModelCatalog,
};
use serde_json::{Value, json};
use std::fs;
use std::sync::Mutex;

struct FakeDispatcher {
    next: Mutex<Option<Result<HttpResponse, HttpDispatchError>>>,
    captured: Mutex<Option<CapturedRequest>>,
}

impl Debug for FakeDispatcher {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FakeDispatcher")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct CapturedRequest {
    endpoint: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl HttpDispatcher for FakeDispatcher {
    fn send(&self, request: HttpRequest) -> HttpFuture<'_> {
        *self.captured.lock().expect("capture lock") = Some(CapturedRequest {
            endpoint: request.endpoint,
            headers: request.headers,
            body: request.body,
        });
        let result = self
            .next
            .lock()
            .expect("response lock")
            .take()
            .expect("one scripted response");
        Box::pin(async move { result })
    }
}

fn write_secret(directory: &Path, value: &str) -> String {
    let path = directory.join("model.key");
    fs::write(&path, value).expect("secret fixture");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("secret mode");
    path.to_string_lossy().into_owned()
}

fn resolved(
    secret_path: &str,
    protocol: &Value,
    auth_kind: &str,
    max_request_bytes: u64,
    max_response_bytes: u64,
    max_header_bytes: u64,
) -> ResolvedRuntimeModel {
    let protocol_name = protocol["kind"].as_str().expect("protocol kind");
    let conformance = Sha256Digest::digest_bytes(b"conformance");
    let catalog: RuntimeModelCatalog = serde_json::from_value(json!({
        "schema_version": 1,
        "default_runtime_model": "deepseek-v4-pro-default",
        "runtime_models": {
            "deepseek-v4-pro-default": {
                "wire_model": "deepseek-v4-pro",
                "deployment": "selected-deployment",
                "profile": "selected-profile",
                "settings": {
                    "max_output_tokens": 4096,
                    "temperature_millis": 200,
                    "reasoning": {"mode": "enabled", "effort": "high"}
                }
            }
        },
        "deployments": {
            "selected-deployment": {
                "vendor": "replaceable-vendor-label",
                "protocol": protocol,
                "endpoint": "https://api.example.test/v1/model-endpoint",
                "auth": {"kind": auth_kind, "path": secret_path},
                "transport": {
                    "connect_timeout_millis": 5000,
                    "request_timeout_millis": 30000,
                    "max_request_bytes": max_request_bytes,
                    "max_response_bytes": max_response_bytes,
                    "max_response_header_bytes": max_header_bytes,
                    "max_diagnostic_bytes": 128,
                    "tls_minimum_version": "tls12",
                    "redirects": "disabled",
                    "proxy": "disabled"
                },
                "data_boundary": "external_provider",
                "conformance_receipt_digest": conformance
            }
        },
        "profiles": {
            "selected-profile": {
                "supported_protocols": [protocol_name],
                "supports_tools": true,
                "supports_parallel_tool_calls": true,
                "supports_reasoning": true,
                "tool_schema_dialect": "json_schema",
                "max_context_tokens": 100_000,
                "max_output_tokens": 8192
            }
        }
    }))
    .expect("catalog schema");
    catalog.resolve(None).expect("resolved default model")
}

fn prepared(kind: ProtocolKind) -> PreparedModelPayload {
    let tools = [CodecToolDefinition {
        name: "inspect_candidate".to_owned(),
        description: "Inspect a candidate.".to_owned(),
        input_schema: json!({"type": "object"}),
        strict: false,
    }];
    let input = NativeTurnInput {
        wire_model: "deepseek-v4-pro",
        system_prompt: "You are AlloyPort.",
        initial_user_text: Some("Inspect the candidate."),
        continuation: None,
        tools: &tools,
        max_output_tokens: 4096,
        reasoning_effort: None,
    };
    match kind {
        ProtocolKind::OpenAiResponses => OpenAiResponsesCodec::default()
            .prepare(input)
            .expect("Responses request"),
        ProtocolKind::OpenAiChatCompletions => OpenAiChatCompletionsCodec::default()
            .prepare(input)
            .expect("Chat request"),
        ProtocolKind::AnthropicMessages => AnthropicMessagesCodec::default()
            .prepare(input)
            .expect("Anthropic request"),
    }
}

fn response(status: u16, body: &[u8], headers: &[(&str, &str)]) -> HttpResponse {
    let mut native_headers = HeaderMap::new();
    for (name, value) in headers {
        native_headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    HttpResponse {
        status,
        headers: native_headers,
        body: body.to_vec(),
    }
}

fn transport_with(
    result: Result<HttpResponse, HttpDispatchError>,
) -> (ReqwestModelTransport, Arc<FakeDispatcher>) {
    let dispatcher = Arc::new(FakeDispatcher {
        next: Mutex::new(Some(result)),
        captured: Mutex::new(None),
    });
    (
        ReqwestModelTransport::with_dispatcher(dispatcher.clone()),
        dispatcher,
    )
}

#[tokio::test]
async fn configured_default_uses_bearer_auth_without_vendor_or_model_branches() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let secret = write_secret(directory.path(), "sk-test-secret\n");
    let deployment = resolved(
        &secret,
        &json!({"kind": "openai_chat_completions"}),
        "bearer_file",
        8192,
        8192,
        1024,
    );
    let request = prepared(ProtocolKind::OpenAiChatCompletions);
    let (transport, dispatcher) = transport_with(Ok(response(
        200,
        br#"{"choices":[]}"#,
        &[("x-request-id", "request-fixture")],
    )));

    let outcome = transport.dispatch(&deployment, &request).await;
    let ModelTransportOutcome::Response(response) = outcome else {
        panic!("expected successful response");
    };
    assert_eq!(deployment.alias(), "deepseek-v4-pro-default");
    assert_eq!(deployment.wire_model(), "deepseek-v4-pro");
    assert_eq!(
        response.provider_request_id.as_deref(),
        Some("request-fixture")
    );

    let captured = dispatcher.captured.lock().expect("capture lock");
    let captured = captured.as_ref().expect("captured request");
    assert_eq!(
        captured.endpoint,
        "https://api.example.test/v1/model-endpoint"
    );
    assert_eq!(captured.body, request.body());
    assert_eq!(
        captured.headers[AUTHORIZATION]
            .to_str()
            .expect("auth header"),
        "Bearer sk-test-secret"
    );
    assert!(captured.headers[AUTHORIZATION].is_sensitive());
    assert!(!format!("{:?}", captured.headers).contains("sk-test-secret"));
    assert!(!String::from_utf8_lossy(&captured.body).contains("sk-test-secret"));
    assert!(!captured.headers.contains_key(ANTHROPIC_VERSION));
}

#[tokio::test]
async fn anthropic_version_and_auth_kind_are_independent_configuration_axes() {
    for (auth_kind, expected_header) in [
        ("x_api_key_file", X_API_KEY),
        ("bearer_file", AUTHORIZATION),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let secret = write_secret(directory.path(), "key-fixture");
        let deployment = resolved(
            &secret,
            &json!({"kind": "anthropic_messages", "api_version": "2023-06-01"}),
            auth_kind,
            8192,
            8192,
            1024,
        );
        let request = prepared(ProtocolKind::AnthropicMessages);
        let (transport, dispatcher) = transport_with(Ok(response(200, b"{}", &[])));
        assert!(matches!(
            transport.dispatch(&deployment, &request).await,
            ModelTransportOutcome::Response(_)
        ));
        let captured = dispatcher.captured.lock().expect("capture lock");
        let headers = &captured.as_ref().expect("captured request").headers;
        assert!(headers.contains_key(expected_header));
        assert_eq!(headers[ANTHROPIC_VERSION], "2023-06-01");
    }
}

#[tokio::test]
async fn pre_send_validation_never_reaches_the_http_dispatcher() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let secret = write_secret(directory.path(), "key-fixture");
    let deployment = resolved(
        &secret,
        &json!({"kind": "openai_responses", "store": false}),
        "bearer_file",
        1,
        8192,
        1024,
    );
    let request = prepared(ProtocolKind::OpenAiResponses);
    let (transport, dispatcher) = transport_with(Ok(response(200, b"{}", &[])));
    let outcome = transport.dispatch(&deployment, &request).await;
    assert!(matches!(
        outcome,
        ModelTransportOutcome::ConfirmedNotSent(ModelTransportFailure {
            kind: ModelTransportFailureKind::RequestTooLarge,
            ..
        })
    ));
    assert!(dispatcher.captured.lock().expect("capture lock").is_none());

    let wrong_protocol = prepared(ProtocolKind::OpenAiChatCompletions);
    let outcome = transport.dispatch(&deployment, &wrong_protocol).await;
    assert!(matches!(
        outcome,
        ModelTransportOutcome::ConfirmedNotSent(ModelTransportFailure {
            kind: ModelTransportFailureKind::Configuration,
            ..
        })
    ));
}

#[tokio::test]
async fn insecure_secret_is_confirmed_not_sent_and_never_rendered() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let secret = write_secret(directory.path(), "secret-fixture");
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o640)).expect("insecure mode");
    let deployment = resolved(
        &secret,
        &json!({"kind": "openai_chat_completions"}),
        "bearer_file",
        8192,
        8192,
        1024,
    );
    let (transport, dispatcher) = transport_with(Ok(response(200, b"{}", &[])));
    let outcome = transport
        .dispatch(&deployment, &prepared(ProtocolKind::OpenAiChatCompletions))
        .await;
    let ModelTransportOutcome::ConfirmedNotSent(failure) = outcome else {
        panic!("expected pre-send secret failure");
    };
    assert_eq!(failure.kind, ModelTransportFailureKind::SecretUnavailable);
    assert!(!failure.diagnostic.contains("secret-fixture"));
    assert!(dispatcher.captured.lock().expect("capture lock").is_none());
}

#[tokio::test]
async fn rate_limit_and_server_error_have_distinct_retry_and_ambiguity_semantics() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let secret = write_secret(directory.path(), "key-fixture");
    let deployment = resolved(
        &secret,
        &json!({"kind": "openai_chat_completions"}),
        "bearer_file",
        8192,
        8192,
        1024,
    );
    let request = prepared(ProtocolKind::OpenAiChatCompletions);
    let (rate_transport, _) =
        transport_with(Ok(response(429, b"slow down", &[("retry-after", "3")])));
    let ModelTransportOutcome::ProviderRejected {
        response: rate_response,
        failure,
    } = rate_transport.dispatch(&deployment, &request).await
    else {
        panic!("429 must be an explicit provider rejection");
    };
    assert_eq!(rate_response.status_code, 429);
    assert_eq!(failure.kind, ModelTransportFailureKind::RateLimited);
    assert_eq!(
        failure.retry_hint,
        ModelTransportRetryHint::AfterMillis(3000)
    );

    let (server_transport, _) = transport_with(Ok(response(503, b"provider unavailable", &[])));
    let ModelTransportOutcome::Ambiguous {
        response: Some(response),
        failure,
    } = server_transport.dispatch(&deployment, &request).await
    else {
        panic!("5xx must preserve response and dispatch ambiguity");
    };
    assert_eq!(response.status_code, 503);
    assert_eq!(failure.kind, ModelTransportFailureKind::ProviderServerError);
    assert_eq!(failure.retry_hint, ModelTransportRetryHint::NewAttempt);
}

#[tokio::test]
async fn transport_errors_preserve_before_send_certainty() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let secret = write_secret(directory.path(), "key-fixture");
    let deployment = resolved(
        &secret,
        &json!({"kind": "openai_chat_completions"}),
        "bearer_file",
        8192,
        8192,
        1024,
    );
    let request = prepared(ProtocolKind::OpenAiChatCompletions);
    let (connect_transport, _) = transport_with(Err(HttpDispatchError {
        before_send: true,
        kind: ModelTransportFailureKind::Connection,
        diagnostic: "connect failed".to_owned(),
    }));
    assert!(matches!(
        connect_transport.dispatch(&deployment, &request).await,
        ModelTransportOutcome::ConfirmedNotSent(_)
    ));

    let (timeout_transport, _) = transport_with(Err(HttpDispatchError {
        before_send: false,
        kind: ModelTransportFailureKind::Timeout,
        diagnostic: "request timed out".to_owned(),
    }));
    assert!(matches!(
        timeout_transport.dispatch(&deployment, &request).await,
        ModelTransportOutcome::Ambiguous { response: None, .. }
    ));
}

#[tokio::test]
async fn response_bounds_and_diagnostic_redaction_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let secret = write_secret(directory.path(), "secret-fixture");
    let deployment = resolved(
        &secret,
        &json!({"kind": "openai_chat_completions"}),
        "bearer_file",
        8192,
        4,
        1024,
    );
    let request = prepared(ProtocolKind::OpenAiChatCompletions);
    let (oversized, _) = transport_with(Ok(response(200, b"12345", &[])));
    assert!(matches!(
        oversized.dispatch(&deployment, &request).await,
        ModelTransportOutcome::Ambiguous {
            failure: ModelTransportFailure {
                kind: ModelTransportFailureKind::ResponseTooLarge,
                ..
            },
            ..
        }
    ));

    let redaction_deployment = resolved(
        &secret,
        &json!({"kind": "openai_chat_completions"}),
        "bearer_file",
        8192,
        8192,
        1024,
    );
    let (rejected, _) = transport_with(Ok(response(401, b"bad key secret-fixture\x1b[31m", &[])));
    let ModelTransportOutcome::ProviderRejected { failure, .. } =
        rejected.dispatch(&redaction_deployment, &request).await
    else {
        panic!("401 must be a provider rejection");
    };
    assert_eq!(
        failure.kind,
        ModelTransportFailureKind::AuthenticationRejected
    );
    assert!(!failure.diagnostic.contains("secret-fixture"));
    assert!(!failure.diagnostic.contains('\u{1b}'));
}
