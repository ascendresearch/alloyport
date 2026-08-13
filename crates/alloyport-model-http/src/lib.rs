//! Tokio-native bounded HTTPS adapter for `AlloyPort`'s model transport port.

use alloyport_core::{
    ModelAuthConfig, ModelTlsMinimumVersion, ModelTransport, ModelTransportFailure,
    ModelTransportFailureKind, ModelTransportFuture, ModelTransportOutcome, ModelTransportPolicy,
    ModelTransportRetryHint, PreparedModelPayload, ProtocolConfig, RawModelResponse,
    ResolvedRuntimeModel, Sha256Digest,
};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const MAX_SECRET_BYTES: u64 = 512;
const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
const ANTHROPIC_VERSION: HeaderName = HeaderName::from_static("anthropic-version");

/// Async HTTPS adapter with no redirect, proxy, decompression, or internal retry behavior.
#[derive(Clone)]
pub struct ReqwestModelTransport {
    dispatcher: Arc<dyn HttpDispatcher>,
}

impl Debug for ReqwestModelTransport {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestModelTransport")
            .finish_non_exhaustive()
    }
}

impl Default for ReqwestModelTransport {
    fn default() -> Self {
        Self {
            dispatcher: Arc::new(SystemHttpDispatcher),
        }
    }
}

impl ReqwestModelTransport {
    #[cfg(test)]
    fn with_dispatcher(dispatcher: Arc<dyn HttpDispatcher>) -> Self {
        Self { dispatcher }
    }

    /// Verifies the deployment credential file and protocol headers without making a request.
    ///
    /// # Errors
    ///
    /// Returns a sanitized diagnostic when the secret file or derived headers violate policy.
    pub async fn preflight(&self, deployment: &ResolvedRuntimeModel) -> Result<(), String> {
        let secret = read_secret(secret_path(deployment.auth())).await?;
        protocol_headers(deployment, &secret).map(|_| ())
    }

    async fn dispatch_once(
        &self,
        deployment: &ResolvedRuntimeModel,
        request: &PreparedModelPayload,
    ) -> ModelTransportOutcome {
        let policy = deployment.transport_policy();
        if request.base_continuation().protocol() != deployment.protocol_kind() {
            return confirmed_not_sent(
                ModelTransportFailureKind::Configuration,
                "prepared payload protocol does not match deployment",
            );
        }
        let request_size = u64::try_from(request.body().len()).unwrap_or(u64::MAX);
        if request_size > policy.max_request_bytes() {
            return confirmed_not_sent(
                ModelTransportFailureKind::RequestTooLarge,
                format!(
                    "model request uses {request_size} bytes; maximum is {}",
                    policy.max_request_bytes()
                ),
            );
        }
        let secret = match read_secret(secret_path(deployment.auth())).await {
            Ok(secret) => secret,
            Err(diagnostic) => {
                return confirmed_not_sent(
                    ModelTransportFailureKind::SecretUnavailable,
                    diagnostic,
                );
            }
        };
        let headers = match protocol_headers(deployment, &secret) {
            Ok(headers) => headers,
            Err(diagnostic) => {
                return confirmed_not_sent(ModelTransportFailureKind::Configuration, diagnostic);
            }
        };
        let http_request = HttpRequest {
            endpoint: deployment.endpoint().to_owned(),
            headers,
            body: request.body().to_vec(),
            policy: policy.clone(),
        };
        match self.dispatcher.send(http_request).await {
            Ok(response) => classify_response(response, policy, &secret),
            Err(error) if error.before_send => {
                confirmed_not_sent(error.kind, sanitize_text(&error.diagnostic, &secret))
            }
            Err(error) => ambiguous(error.kind, sanitize_text(&error.diagnostic, &secret), None),
        }
    }
}

impl ModelTransport for ReqwestModelTransport {
    fn dispatch<'a>(
        &'a self,
        deployment: &'a ResolvedRuntimeModel,
        request: &'a PreparedModelPayload,
    ) -> ModelTransportFuture<'a> {
        Box::pin(self.dispatch_once(deployment, request))
    }
}

fn secret_path(auth: &ModelAuthConfig) -> &Path {
    match auth {
        ModelAuthConfig::BearerFile { path } | ModelAuthConfig::XApiKeyFile { path } => {
            Path::new(path)
        }
    }
}

async fn read_secret(path: &Path) -> Result<String, String> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|error| format!("cannot inspect secret file: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("secret path must be a regular non-symlink file".to_owned());
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err("secret file must not grant group or other permissions".to_owned());
    }
    if metadata.len() == 0 || metadata.len() > MAX_SECRET_BYTES {
        return Err(format!(
            "secret file must contain 1..={MAX_SECRET_BYTES} bytes"
        ));
    }
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| format!("cannot read secret file: {error}"))?;
    let value = String::from_utf8(bytes).map_err(|_| "secret must be UTF-8".to_owned())?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    {
        return Err("secret contains unsupported characters".to_owned());
    }
    Ok(value.to_owned())
}

fn protocol_headers(deployment: &ResolvedRuntimeModel, secret: &str) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    let mut secret_value = match deployment.auth() {
        ModelAuthConfig::BearerFile { .. } => HeaderValue::from_str(&format!("Bearer {secret}")),
        ModelAuthConfig::XApiKeyFile { .. } => HeaderValue::from_str(secret),
    }
    .map_err(|_| "secret cannot be represented as an HTTP header".to_owned())?;
    secret_value.set_sensitive(true);
    match deployment.auth() {
        ModelAuthConfig::BearerFile { .. } => {
            headers.insert(AUTHORIZATION, secret_value);
        }
        ModelAuthConfig::XApiKeyFile { .. } => {
            headers.insert(X_API_KEY, secret_value);
        }
    }
    if let ProtocolConfig::AnthropicMessages { api_version } = deployment.protocol() {
        headers.insert(
            ANTHROPIC_VERSION,
            HeaderValue::from_str(api_version)
                .map_err(|_| "Anthropic API version is not a valid header value".to_owned())?,
        );
    }
    Ok(headers)
}

struct HttpRequest {
    endpoint: String,
    headers: HeaderMap,
    body: Vec<u8>,
    policy: ModelTransportPolicy,
}

struct HttpResponse {
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct HttpDispatchError {
    before_send: bool,
    kind: ModelTransportFailureKind,
    diagnostic: String,
}

type HttpFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HttpResponse, HttpDispatchError>> + Send + 'a>>;

trait HttpDispatcher: Debug + Send + Sync {
    fn send(&self, request: HttpRequest) -> HttpFuture<'_>;
}

#[derive(Debug)]
struct SystemHttpDispatcher;

impl HttpDispatcher for SystemHttpDispatcher {
    fn send(&self, request: HttpRequest) -> HttpFuture<'_> {
        Box::pin(async move { send_with_reqwest(request).await })
    }
}

async fn send_with_reqwest(request: HttpRequest) -> Result<HttpResponse, HttpDispatchError> {
    let tls_version = match request.policy.tls_minimum_version() {
        ModelTlsMinimumVersion::Tls12 => reqwest::tls::Version::TLS_1_2,
        ModelTlsMinimumVersion::Tls13 => reqwest::tls::Version::TLS_1_3,
    };
    let client = reqwest::Client::builder()
        .https_only(true)
        .tls_backend_rustls()
        .tls_version_min(tls_version)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .retry(reqwest::retry::never())
        .referer(false)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .connect_timeout(Duration::from_millis(
            request.policy.connect_timeout_millis(),
        ))
        .timeout(Duration::from_millis(
            request.policy.request_timeout_millis(),
        ))
        .build()
        .map_err(|error| HttpDispatchError {
            before_send: true,
            kind: ModelTransportFailureKind::Configuration,
            diagnostic: format!("cannot build HTTPS client: {error}"),
        })?;
    let response = client
        .post(request.endpoint)
        .headers(request.headers)
        .body(request.body)
        .send()
        .await
        .map_err(|error| classify_reqwest_error(&error))?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    ensure_header_bound(&headers, request.policy.max_response_header_bytes()).map_err(
        |diagnostic| HttpDispatchError {
            before_send: false,
            kind: ModelTransportFailureKind::ResponseHeadersTooLarge,
            diagnostic,
        },
    )?;
    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| classify_reqwest_error(&error))?
    {
        let new_length = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| HttpDispatchError {
                before_send: false,
                kind: ModelTransportFailureKind::ResponseTooLarge,
                diagnostic: "HTTP response length overflowed".to_owned(),
            })?;
        if u64::try_from(new_length).unwrap_or(u64::MAX) > request.policy.max_response_bytes() {
            return Err(HttpDispatchError {
                before_send: false,
                kind: ModelTransportFailureKind::ResponseTooLarge,
                diagnostic: "HTTP response body exceeded its configured bound".to_owned(),
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn classify_reqwest_error(error: &reqwest::Error) -> HttpDispatchError {
    let before_send = error.is_builder() || error.is_connect();
    let kind = if error.is_timeout() {
        ModelTransportFailureKind::Timeout
    } else if error.is_connect() {
        ModelTransportFailureKind::Connection
    } else {
        ModelTransportFailureKind::ProcessIo
    };
    HttpDispatchError {
        before_send,
        kind,
        diagnostic: format!("HTTPS request failed: {error}"),
    }
}

fn ensure_header_bound(headers: &HeaderMap, maximum: u64) -> Result<(), String> {
    let size = canonical_headers(headers).len();
    if u64::try_from(size).unwrap_or(u64::MAX) > maximum {
        Err("HTTP response headers exceeded their configured bound".to_owned())
    } else {
        Ok(())
    }
}

fn classify_response(
    response: HttpResponse,
    policy: &ModelTransportPolicy,
    secret: &str,
) -> ModelTransportOutcome {
    if let Err(diagnostic) =
        ensure_header_bound(&response.headers, policy.max_response_header_bytes())
    {
        return ambiguous(
            ModelTransportFailureKind::ResponseHeadersTooLarge,
            diagnostic,
            None,
        );
    }
    if u64::try_from(response.body.len()).unwrap_or(u64::MAX) > policy.max_response_bytes() {
        return ambiguous(
            ModelTransportFailureKind::ResponseTooLarge,
            "HTTP response body exceeded its configured bound",
            None,
        );
    }
    let retry_after_millis = header_text(&response.headers, "retry-after")
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|seconds| seconds.checked_mul(1_000));
    let raw = RawModelResponse {
        status_code: response.status,
        body: response.body,
        response_headers_digest: Sha256Digest::digest_bytes(&canonical_headers(&response.headers)),
        provider_request_id: header_text(&response.headers, "x-request-id")
            .or_else(|| header_text(&response.headers, "request-id")),
        retry_after_millis,
    };
    classify_http_status(raw, policy.max_diagnostic_bytes(), secret)
}

fn classify_http_status(
    response: RawModelResponse,
    diagnostic_limit: u64,
    secret: &str,
) -> ModelTransportOutcome {
    let status = response.status_code;
    if (200..300).contains(&status) {
        return ModelTransportOutcome::Response(response);
    }
    let diagnostic = bounded_response_diagnostic(&response.body, diagnostic_limit, secret);
    let (kind, retry_hint) = match status {
        401 => (
            ModelTransportFailureKind::AuthenticationRejected,
            ModelTransportRetryHint::Never,
        ),
        403 => (
            ModelTransportFailureKind::PermissionRejected,
            ModelTransportRetryHint::Never,
        ),
        429 => (
            ModelTransportFailureKind::RateLimited,
            response.retry_after_millis.map_or(
                ModelTransportRetryHint::NewAttempt,
                ModelTransportRetryHint::AfterMillis,
            ),
        ),
        300..=499 => (
            ModelTransportFailureKind::ProviderClientError,
            ModelTransportRetryHint::Never,
        ),
        _ => (
            ModelTransportFailureKind::ProviderServerError,
            ModelTransportRetryHint::NewAttempt,
        ),
    };
    let failure = ModelTransportFailure::new(kind, diagnostic, Some(status), retry_hint);
    if status >= 500 {
        ModelTransportOutcome::Ambiguous {
            response: Some(response),
            failure,
        }
    } else {
        ModelTransportOutcome::ProviderRejected { response, failure }
    }
}

fn canonical_headers(headers: &HeaderMap) -> Vec<u8> {
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_ascii_lowercase().into_bytes(),
                value.as_bytes().to_vec(),
            )
        })
        .collect();
    fields.sort();
    let mut canonical = Vec::new();
    for (name, value) in fields {
        canonical.extend_from_slice(name.len().to_string().as_bytes());
        canonical.push(b':');
        canonical.extend_from_slice(&name);
        canonical.push(b'=');
        canonical.extend_from_slice(value.len().to_string().as_bytes());
        canonical.push(b':');
        canonical.extend_from_slice(&value);
        canonical.push(b'\n');
    }
    canonical
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)?
        .to_str()
        .ok()
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_owned)
}

fn bounded_response_diagnostic(body: &[u8], limit: u64, secret: &str) -> String {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let retained = &body[..body.len().min(limit)];
    let mut diagnostic = sanitize_text(&String::from_utf8_lossy(retained), secret);
    if body.len() > limit {
        diagnostic.push_str(" [truncated]");
    }
    if diagnostic.is_empty() {
        "provider returned an empty error body".to_owned()
    } else {
        diagnostic
    }
}

fn sanitize_text(value: &str, secret: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .collect::<String>()
        .replace(secret, "[REDACTED]")
        .trim()
        .to_owned()
}

fn confirmed_not_sent(
    kind: ModelTransportFailureKind,
    diagnostic: impl Into<String>,
) -> ModelTransportOutcome {
    ModelTransportOutcome::ConfirmedNotSent(ModelTransportFailure::new(
        kind,
        diagnostic,
        None,
        ModelTransportRetryHint::Never,
    ))
}

fn ambiguous(
    kind: ModelTransportFailureKind,
    diagnostic: impl Into<String>,
    response: Option<RawModelResponse>,
) -> ModelTransportOutcome {
    ModelTransportOutcome::Ambiguous {
        response,
        failure: ModelTransportFailure::new(
            kind,
            diagnostic,
            None,
            ModelTransportRetryHint::NewAttempt,
        ),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
