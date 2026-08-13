//! Runtime-model catalog, model-attempt records, and a deterministic gateway port.

use crate::model_transport_policy::{ModelTransportPolicy, validate_transport_policy};
use crate::{EpisodeId, ModelAttemptId, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;

pub const RUNTIME_MODEL_CATALOG_SCHEMA_V1: u16 = 1;
pub const MODEL_ATTEMPT_SCHEMA_V1: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ProtocolKind {
    #[serde(rename = "openai_responses")]
    OpenAiResponses,
    #[serde(rename = "openai_chat_completions")]
    OpenAiChatCompletions,
    #[serde(rename = "anthropic_messages")]
    AnthropicMessages,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ProtocolConfig {
    #[serde(rename = "openai_responses")]
    OpenAiResponses {
        #[serde(default)]
        store: bool,
    },
    #[serde(rename = "openai_chat_completions")]
    OpenAiChatCompletions {},
    #[serde(rename = "anthropic_messages")]
    AnthropicMessages { api_version: String },
}

impl ProtocolConfig {
    #[must_use]
    pub const fn kind(&self) -> ProtocolKind {
        match self {
            Self::OpenAiResponses { .. } => ProtocolKind::OpenAiResponses,
            Self::OpenAiChatCompletions {} => ProtocolKind::OpenAiChatCompletions,
            Self::AnthropicMessages { .. } => ProtocolKind::AnthropicMessages,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelAuthConfig {
    BearerFile { path: String },
    XApiKeyFile { path: String },
}

impl ModelAuthConfig {
    fn path(&self) -> &str {
        match self {
            Self::BearerFile { path } | Self::XApiKeyFile { path } => path,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelDataBoundary {
    ExternalProvider,
    PrivateDeployment,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSchemaDialect {
    JsonSchema,
    StrictSubset,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningMode {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Max,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningSettings {
    mode: ReasoningMode,
    #[serde(default)]
    effort: Option<ReasoningEffort>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelGenerationSettings {
    max_output_tokens: u32,
    temperature_millis: u16,
    reasoning: ReasoningSettings,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeModelConfig {
    wire_model: String,
    deployment: String,
    profile: String,
    settings: ModelGenerationSettings,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDeploymentConfig {
    vendor: String,
    protocol: ProtocolConfig,
    endpoint: String,
    auth: ModelAuthConfig,
    transport: ModelTransportPolicy,
    data_boundary: ModelDataBoundary,
    conformance_receipt_digest: Sha256Digest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfileConfig {
    supported_protocols: BTreeSet<ProtocolKind>,
    supports_tools: bool,
    supports_parallel_tool_calls: bool,
    supports_reasoning: bool,
    tool_schema_dialect: ToolSchemaDialect,
    max_context_tokens: u32,
    max_output_tokens: u32,
}

/// Strict, versioned runtime-model catalog. It performs no file or network I/O.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeModelCatalog {
    schema_version: u16,
    default_runtime_model: String,
    runtime_models: BTreeMap<String, RuntimeModelConfig>,
    deployments: BTreeMap<String, ModelDeploymentConfig>,
    profiles: BTreeMap<String, ModelProfileConfig>,
}

impl RuntimeModelCatalog {
    /// Validates every reference and fail-closed schema-1 capability rule.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported schemas, unsafe secrets or endpoints, dangling
    /// references, protocol/auth mismatches, or unsupported capabilities.
    pub fn validate(&self) -> Result<(), ModelCatalogError> {
        if self.schema_version != RUNTIME_MODEL_CATALOG_SCHEMA_V1 {
            return Err(ModelCatalogError::UnsupportedSchema(self.schema_version));
        }
        require_catalog_text("default runtime model", &self.default_runtime_model)?;
        if !self
            .runtime_models
            .contains_key(&self.default_runtime_model)
        {
            return Err(ModelCatalogError::UnknownDefault(
                self.default_runtime_model.clone(),
            ));
        }

        for (name, deployment) in &self.deployments {
            require_catalog_text("deployment name", name)?;
            require_catalog_text("vendor", &deployment.vendor)?;
            validate_endpoint(name, &deployment.endpoint)?;
            validate_secret_path(name, deployment.auth.path())?;
            validate_transport_policy(name, &deployment.transport)?;
            if matches!(
                &deployment.protocol,
                ProtocolConfig::OpenAiResponses { store: true }
            ) {
                return Err(ModelCatalogError::HostedStateUnsupported(name.clone()));
            }
            if let ProtocolConfig::AnthropicMessages { api_version } = &deployment.protocol {
                require_catalog_text("Anthropic API version", api_version)?;
            }
        }

        for (name, profile) in &self.profiles {
            require_catalog_text("profile name", name)?;
            if profile.supported_protocols.is_empty() {
                return Err(ModelCatalogError::EmptyProtocolSet(name.clone()));
            }
            if !profile.supports_tools {
                return Err(ModelCatalogError::ToolsRequired(name.clone()));
            }
            if profile.max_context_tokens == 0
                || profile.max_output_tokens == 0
                || profile.max_output_tokens > profile.max_context_tokens
            {
                return Err(ModelCatalogError::InvalidProfileBounds(name.clone()));
            }
        }

        for (alias, runtime_model) in &self.runtime_models {
            require_catalog_text("runtime model alias", alias)?;
            require_catalog_text("wire model", &runtime_model.wire_model)?;
            let deployment = self
                .deployments
                .get(&runtime_model.deployment)
                .ok_or_else(|| ModelCatalogError::UnknownDeployment {
                    alias: alias.clone(),
                    deployment: runtime_model.deployment.clone(),
                })?;
            let profile = self.profiles.get(&runtime_model.profile).ok_or_else(|| {
                ModelCatalogError::UnknownProfile {
                    alias: alias.clone(),
                    profile: runtime_model.profile.clone(),
                }
            })?;
            if !profile
                .supported_protocols
                .contains(&deployment.protocol.kind())
            {
                return Err(ModelCatalogError::UnsupportedProtocol {
                    alias: alias.clone(),
                    protocol: deployment.protocol.kind(),
                });
            }
            validate_settings(alias, &runtime_model.settings, profile)?;
        }
        Ok(())
    }

    /// Resolves one alias into the immutable snapshot captured by an episode.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog is invalid or the requested alias is unknown.
    pub fn resolve(&self, alias: Option<&str>) -> Result<ResolvedRuntimeModel, ModelCatalogError> {
        self.validate()?;
        let alias = alias.unwrap_or(&self.default_runtime_model);
        let runtime_model = self
            .runtime_models
            .get(alias)
            .ok_or_else(|| ModelCatalogError::UnknownRuntimeModel(alias.to_owned()))?;
        let deployment = self
            .deployments
            .get(&runtime_model.deployment)
            .ok_or_else(|| ModelCatalogError::UnknownDeployment {
                alias: alias.to_owned(),
                deployment: runtime_model.deployment.clone(),
            })?;
        let profile = self.profiles.get(&runtime_model.profile).ok_or_else(|| {
            ModelCatalogError::UnknownProfile {
                alias: alias.to_owned(),
                profile: runtime_model.profile.clone(),
            }
        })?;
        Ok(ResolvedRuntimeModel {
            alias: alias.to_owned(),
            wire_model: runtime_model.wire_model.clone(),
            deployment_name: runtime_model.deployment.clone(),
            profile_name: runtime_model.profile.clone(),
            vendor: deployment.vendor.clone(),
            protocol: deployment.protocol.clone(),
            endpoint: deployment.endpoint.clone(),
            auth: deployment.auth.clone(),
            transport: deployment.transport.clone(),
            data_boundary: deployment.data_boundary,
            conformance_receipt_digest: deployment.conformance_receipt_digest,
            settings: runtime_model.settings.clone(),
            profile: profile.clone(),
        })
    }
}

/// Fully resolved model/deployment/protocol snapshot pinned to one episode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResolvedRuntimeModel {
    alias: String,
    wire_model: String,
    deployment_name: String,
    profile_name: String,
    vendor: String,
    protocol: ProtocolConfig,
    endpoint: String,
    auth: ModelAuthConfig,
    transport: ModelTransportPolicy,
    data_boundary: ModelDataBoundary,
    conformance_receipt_digest: Sha256Digest,
    settings: ModelGenerationSettings,
    profile: ModelProfileConfig,
}

impl ResolvedRuntimeModel {
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    #[must_use]
    pub fn wire_model(&self) -> &str {
        &self.wire_model
    }

    #[must_use]
    pub const fn protocol_kind(&self) -> ProtocolKind {
        self.protocol.kind()
    }

    #[must_use]
    pub fn deployment_name(&self) -> &str {
        &self.deployment_name
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub const fn protocol(&self) -> &ProtocolConfig {
        &self.protocol
    }

    #[must_use]
    pub const fn auth(&self) -> &ModelAuthConfig {
        &self.auth
    }

    #[must_use]
    pub const fn transport_policy(&self) -> &ModelTransportPolicy {
        &self.transport
    }

    #[must_use]
    pub const fn max_output_tokens(&self) -> u32 {
        self.settings.max_output_tokens
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelCatalogError {
    UnsupportedSchema(u16),
    EmptyField(&'static str),
    UnknownDefault(String),
    UnknownRuntimeModel(String),
    UnknownDeployment {
        alias: String,
        deployment: String,
    },
    UnknownProfile {
        alias: String,
        profile: String,
    },
    UnsafeEndpoint(String),
    UnsafeSecretPath(String),
    InvalidTransportPolicy(String),
    HostedStateUnsupported(String),
    EmptyProtocolSet(String),
    ToolsRequired(String),
    InvalidProfileBounds(String),
    UnsupportedProtocol {
        alias: String,
        protocol: ProtocolKind,
    },
    InvalidSettings(String),
}

impl Display for ModelCatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported runtime-model catalog schema {version}"
                )
            }
            Self::EmptyField(field) => write!(formatter, "{field} must not be empty"),
            Self::UnknownDefault(alias) => write!(formatter, "unknown default model {alias}"),
            Self::UnknownRuntimeModel(alias) => write!(formatter, "unknown runtime model {alias}"),
            Self::UnknownDeployment { alias, deployment } => {
                write!(
                    formatter,
                    "model {alias} references unknown deployment {deployment}"
                )
            }
            Self::UnknownProfile { alias, profile } => {
                write!(
                    formatter,
                    "model {alias} references unknown profile {profile}"
                )
            }
            Self::UnsafeEndpoint(name) => {
                write!(formatter, "deployment {name} has unsafe endpoint")
            }
            Self::UnsafeSecretPath(name) => {
                write!(formatter, "deployment {name} has unsafe secret path")
            }
            Self::InvalidTransportPolicy(name) => {
                write!(formatter, "deployment {name} has invalid transport policy")
            }
            Self::HostedStateUnsupported(name) => {
                write!(
                    formatter,
                    "deployment {name} enables deferred provider-hosted state"
                )
            }
            Self::EmptyProtocolSet(name) => {
                write!(formatter, "profile {name} supports no protocol")
            }
            Self::ToolsRequired(name) => {
                write!(formatter, "profile {name} lacks required tool use")
            }
            Self::InvalidProfileBounds(name) => {
                write!(formatter, "profile {name} has invalid token bounds")
            }
            Self::UnsupportedProtocol { alias, protocol } => {
                write!(formatter, "model {alias} does not support {protocol:?}")
            }
            Self::InvalidSettings(alias) => write!(formatter, "model {alias} has invalid settings"),
        }
    }
}

impl Error for ModelCatalogError {}

fn require_catalog_text(field: &'static str, value: &str) -> Result<(), ModelCatalogError> {
    if value.trim().is_empty() {
        Err(ModelCatalogError::EmptyField(field))
    } else {
        Ok(())
    }
}

fn validate_endpoint(name: &str, endpoint: &str) -> Result<(), ModelCatalogError> {
    let Ok(uri) = endpoint.parse::<http::Uri>() else {
        return Err(ModelCatalogError::UnsafeEndpoint(name.to_owned()));
    };
    let authority = uri.authority();
    if uri.scheme_str() != Some("https")
        || authority.is_none_or(|authority| authority.host().is_empty())
        || uri.path().is_empty()
        || uri.path() == "/"
        || uri.query().is_some()
        || endpoint.chars().any(char::is_whitespace)
        || endpoint.contains('@')
        || endpoint.contains('#')
    {
        return Err(ModelCatalogError::UnsafeEndpoint(name.to_owned()));
    }
    Ok(())
}

fn validate_secret_path(name: &str, path: &str) -> Result<(), ModelCatalogError> {
    if path.trim() != path || !Path::new(path).is_absolute() {
        return Err(ModelCatalogError::UnsafeSecretPath(name.to_owned()));
    }
    Ok(())
}

fn validate_settings(
    alias: &str,
    settings: &ModelGenerationSettings,
    profile: &ModelProfileConfig,
) -> Result<(), ModelCatalogError> {
    let reasoning_valid = match settings.reasoning.mode {
        ReasoningMode::Disabled => settings.reasoning.effort.is_none(),
        ReasoningMode::Enabled => profile.supports_reasoning,
    };
    if settings.max_output_tokens == 0
        || settings.max_output_tokens > profile.max_output_tokens
        || settings.temperature_millis > 2_000
        || !reasoning_valid
    {
        return Err(ModelCatalogError::InvalidSettings(alias.to_owned()));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAttemptStatus {
    Prepared,
    Dispatching,
    Responded,
    Decoded,
    DecodeFailed,
    ConfirmedNotSent,
    Failed,
    Ambiguous,
    CancelledAmbiguous,
}

impl ModelAttemptStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Decoded
                | Self::DecodeFailed
                | Self::ConfirmedNotSent
                | Self::Failed
                | Self::Ambiguous
                | Self::CancelledAmbiguous
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: Option<u64>,
    pub cost_micros: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelAttemptSpec {
    pub id: ModelAttemptId,
    pub episode_id: EpisodeId,
    pub attempt_number: u32,
    pub request_digest: Sha256Digest,
    pub resolved_model_digest: Sha256Digest,
    pub deployment_digest: Sha256Digest,
    pub model_profile_digest: Sha256Digest,
    pub request_budget_digest: Sha256Digest,
    pub predecessor_attempt_id: Option<ModelAttemptId>,
    pub predecessor_continuation_digest: Option<Sha256Digest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAttemptRecord {
    schema_version: u16,
    id: ModelAttemptId,
    episode_id: EpisodeId,
    attempt_number: u32,
    request_digest: Sha256Digest,
    resolved_model_digest: Sha256Digest,
    deployment_digest: Sha256Digest,
    model_profile_digest: Sha256Digest,
    request_budget_digest: Sha256Digest,
    predecessor_attempt_id: Option<ModelAttemptId>,
    predecessor_continuation_digest: Option<Sha256Digest>,
    status: ModelAttemptStatus,
    response_digest: Option<Sha256Digest>,
    continuation_digest: Option<Sha256Digest>,
    diagnostic_digest: Option<Sha256Digest>,
    actual_model: Option<String>,
    usage: Option<ModelUsage>,
}

impl ModelAttemptRecord {
    /// Creates a prepared attempt record.
    ///
    /// # Errors
    ///
    /// Returns an error when the attempt number is zero.
    pub fn new(spec: ModelAttemptSpec) -> Result<Self, ModelAttemptError> {
        if spec.attempt_number == 0 {
            return Err(ModelAttemptError::ZeroAttemptNumber);
        }
        Ok(Self {
            schema_version: MODEL_ATTEMPT_SCHEMA_V1,
            id: spec.id,
            episode_id: spec.episode_id,
            attempt_number: spec.attempt_number,
            request_digest: spec.request_digest,
            resolved_model_digest: spec.resolved_model_digest,
            deployment_digest: spec.deployment_digest,
            model_profile_digest: spec.model_profile_digest,
            request_budget_digest: spec.request_budget_digest,
            predecessor_attempt_id: spec.predecessor_attempt_id,
            predecessor_continuation_digest: spec.predecessor_continuation_digest,
            status: ModelAttemptStatus::Prepared,
            response_digest: None,
            continuation_digest: None,
            diagnostic_digest: None,
            actual_model: None,
            usage: None,
        })
    }

    /// Marks the point after which dispatch ambiguity must be preserved.
    ///
    /// # Errors
    ///
    /// Returns an error unless the attempt is prepared.
    pub fn mark_dispatching(&mut self) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Prepared)?;
        self.status = ModelAttemptStatus::Dispatching;
        Ok(())
    }

    /// Records the exact provider response identity and optional usage.
    ///
    /// # Errors
    ///
    /// Returns an error unless dispatch started or when `actual_model` is empty.
    pub fn record_response(
        &mut self,
        response_digest: Sha256Digest,
        actual_model: Option<String>,
        usage: Option<ModelUsage>,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Dispatching)?;
        if actual_model
            .as_ref()
            .is_some_and(|model| model.trim().is_empty())
        {
            return Err(ModelAttemptError::EmptyActualModel);
        }
        self.response_digest = Some(response_digest);
        self.actual_model = actual_model;
        self.usage = usage;
        self.status = ModelAttemptStatus::Responded;
        Ok(())
    }

    /// Commits the normalized continuation identity.
    ///
    /// # Errors
    ///
    /// Returns an error unless a provider response was recorded.
    pub fn mark_decoded(
        &mut self,
        continuation_digest: Sha256Digest,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Responded)?;
        self.continuation_digest = Some(continuation_digest);
        self.status = ModelAttemptStatus::Decoded;
        Ok(())
    }

    /// Records a terminal decode failure without discarding the provider response.
    ///
    /// # Errors
    ///
    /// Returns an error unless a provider response was recorded.
    pub fn mark_decode_failed(
        &mut self,
        diagnostic_digest: Sha256Digest,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Responded)?;
        self.diagnostic_digest = Some(diagnostic_digest);
        self.status = ModelAttemptStatus::DecodeFailed;
        Ok(())
    }

    /// Records a provider response that explicitly rejected the request.
    ///
    /// # Errors
    ///
    /// Returns an error unless a provider response was recorded.
    pub fn mark_response_failed(
        &mut self,
        diagnostic_digest: Sha256Digest,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Responded)?;
        self.diagnostic_digest = Some(diagnostic_digest);
        self.status = ModelAttemptStatus::Failed;
        Ok(())
    }

    /// Finishes a dispatch that produced no authoritative response bytes.
    ///
    /// # Errors
    ///
    /// Returns an error unless dispatch started or `terminal` is invalid for this path.
    pub fn finish_without_response(
        &mut self,
        terminal: ModelAttemptStatus,
        diagnostic_digest: Sha256Digest,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Dispatching)?;
        if !matches!(
            terminal,
            ModelAttemptStatus::ConfirmedNotSent
                | ModelAttemptStatus::Failed
                | ModelAttemptStatus::Ambiguous
                | ModelAttemptStatus::CancelledAmbiguous
        ) {
            return Err(ModelAttemptError::InvalidTerminal(terminal));
        }
        self.diagnostic_digest = Some(diagnostic_digest);
        self.status = terminal;
        Ok(())
    }

    #[must_use]
    pub const fn status(&self) -> ModelAttemptStatus {
        self.status
    }

    #[must_use]
    pub const fn id(&self) -> &ModelAttemptId {
        &self.id
    }

    fn require_status(&self, expected: ModelAttemptStatus) -> Result<(), ModelAttemptError> {
        if self.status == expected {
            Ok(())
        } else {
            Err(ModelAttemptError::InvalidTransition {
                from: self.status,
                expected,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelAttemptError {
    ZeroAttemptNumber,
    EmptyActualModel,
    InvalidTransition {
        from: ModelAttemptStatus,
        expected: ModelAttemptStatus,
    },
    InvalidTerminal(ModelAttemptStatus),
}

impl Display for ModelAttemptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAttemptNumber => write!(formatter, "model attempt number must be positive"),
            Self::EmptyActualModel => write!(formatter, "actual model must not be empty"),
            Self::InvalidTransition { from, expected } => {
                write!(
                    formatter,
                    "model attempt is {from:?}, expected {expected:?}"
                )
            }
            Self::InvalidTerminal(status) => write!(formatter, "invalid model terminal {status:?}"),
        }
    }
}

impl Error for ModelAttemptError {}

#[cfg(test)]
#[path = "model_tests.rs"]
mod model_tests;
