//! Protocol-agnostic model transport port and deterministic dispatch double.

use crate::{PreparedModelPayload, ProtocolKind, ResolvedRuntimeModel, Sha256Digest};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

pub type ModelTransportFuture<'a> =
    Pin<Box<dyn Future<Output = ModelTransportOutcome> + Send + 'a>>;

/// Stable transport failure category; diagnostics never control retry semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelTransportFailureKind {
    Configuration,
    RequestTooLarge,
    SecretUnavailable,
    NameResolution,
    Connection,
    Tls,
    Timeout,
    ResponseTooLarge,
    ResponseHeadersTooLarge,
    DiagnosticTooLarge,
    InvalidHttpResponse,
    AuthenticationRejected,
    PermissionRejected,
    RateLimited,
    ProviderClientError,
    ProviderServerError,
    ProcessIo,
}

/// Retry guidance derived from typed transport evidence, never diagnostic text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelTransportRetryHint {
    Never,
    NewAttempt,
    AfterMillis(u64),
}

/// Bounded, sanitized failure metadata with no request body, auth header, or secret value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelTransportFailure {
    pub kind: ModelTransportFailureKind,
    pub diagnostic: String,
    pub http_status: Option<u16>,
    pub retry_hint: ModelTransportRetryHint,
}

impl ModelTransportFailure {
    #[must_use]
    pub fn new(
        kind: ModelTransportFailureKind,
        diagnostic: impl Into<String>,
        http_status: Option<u16>,
        retry_hint: ModelTransportRetryHint,
    ) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
            http_status,
            retry_hint,
        }
    }
}

/// Successful HTTP response bytes plus bounded provider metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawModelResponse {
    pub status_code: u16,
    pub body: Vec<u8>,
    pub response_headers_digest: Sha256Digest,
    pub provider_request_id: Option<String>,
    pub retry_after_millis: Option<u64>,
}

/// Dispatch certainty determines how the durable model attempt may advance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelTransportOutcome {
    Response(RawModelResponse),
    ConfirmedNotSent(ModelTransportFailure),
    ProviderRejected {
        response: RawModelResponse,
        failure: ModelTransportFailure,
    },
    Ambiguous {
        response: Option<RawModelResponse>,
        failure: ModelTransportFailure,
    },
}

/// Transport dispatches exactly once. It never decodes a model turn or retries invisibly.
pub trait ModelTransport: Debug + Send + Sync {
    #[must_use]
    fn dispatch<'a>(
        &'a self,
        deployment: &'a ResolvedRuntimeModel,
        request: &'a PreparedModelPayload,
    ) -> ModelTransportFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptedModelTransportStep {
    pub expected_deployment_name: String,
    pub expected_protocol: ProtocolKind,
    pub expected_request_digest: Sha256Digest,
    pub outcome: ModelTransportOutcome,
}

/// Thread-safe non-network transport used to prove composition before a real endpoint is enabled.
#[derive(Debug)]
pub struct ScriptedFakeModelTransport {
    steps: Mutex<VecDeque<ScriptedModelTransportStep>>,
}

impl ScriptedFakeModelTransport {
    #[must_use]
    pub fn new(steps: impl IntoIterator<Item = ScriptedModelTransportStep>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
        }
    }

    #[must_use]
    pub fn remaining_steps(&self) -> usize {
        self.steps.lock().map_or(usize::MAX, |steps| steps.len())
    }
}

impl ModelTransport for ScriptedFakeModelTransport {
    fn dispatch<'a>(
        &'a self,
        deployment: &'a ResolvedRuntimeModel,
        request: &'a PreparedModelPayload,
    ) -> ModelTransportFuture<'a> {
        Box::pin(async move {
            let Ok(mut steps) = self.steps.lock() else {
                return fake_mismatch("scripted transport lock is poisoned");
            };
            let Some(step) = steps.pop_front() else {
                return fake_mismatch("scripted transport is exhausted");
            };
            let request_digest = Sha256Digest::digest_bytes(request.body());
            if step.expected_deployment_name != deployment.deployment_name()
                || step.expected_protocol != deployment.protocol_kind()
                || step.expected_request_digest != request_digest
            {
                return fake_mismatch("scripted transport request does not match the next step");
            }
            step.outcome
        })
    }
}

fn fake_mismatch(diagnostic: &str) -> ModelTransportOutcome {
    ModelTransportOutcome::ConfirmedNotSent(ModelTransportFailure::new(
        ModelTransportFailureKind::Configuration,
        diagnostic,
        None,
        ModelTransportRetryHint::Never,
    ))
}
