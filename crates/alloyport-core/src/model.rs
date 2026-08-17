//! Runtime-model catalog, model-attempt records, and a deterministic gateway port.

use crate::Sha256Digest;
use crate::model_transport_policy::{ModelTransportPolicy, validate_transport_policy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::Path;

#[path = "model_attempt.rs"]
mod attempt;

pub use attempt::{
    MODEL_ATTEMPT_SCHEMA_V1, ModelAttemptError, ModelAttemptRecord, ModelAttemptSpec,
    ModelAttemptStatus, ModelUsage,
};

pub const RUNTIME_MODEL_CATALOG_SCHEMA_V1: u16 = 1;

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
    OpenAiChatCompletions {
        /// Emit the OpenAI-compatible `thinking.type` request extension when the deployment
        /// explicitly supports it.
        #[serde(default)]
        thinking_parameter: bool,
    },
    #[serde(rename = "anthropic_messages")]
    AnthropicMessages { api_version: String },
}

impl ProtocolConfig {
    #[must_use]
    pub const fn kind(&self) -> ProtocolKind {
        match self {
            Self::OpenAiResponses { .. } => ProtocolKind::OpenAiResponses,
            Self::OpenAiChatCompletions { .. } => ProtocolKind::OpenAiChatCompletions,
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
    /// Computes the identity of the complete resolved model snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error only if canonical JSON serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }

    /// Computes the independently captured deployment identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if canonical JSON serialization fails.
    pub fn deployment_digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(&(
            &self.deployment_name,
            &self.vendor,
            &self.protocol,
            &self.endpoint,
            &self.auth,
            &self.transport,
            self.data_boundary,
            self.conformance_receipt_digest,
        ))
        .map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }

    /// Computes the configured model-profile identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if canonical JSON serialization fails.
    pub fn profile_digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(&(&self.profile_name, &self.profile))
            .map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }

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
    pub const fn data_boundary(&self) -> ModelDataBoundary {
        self.data_boundary
    }

    #[must_use]
    pub const fn max_output_tokens(&self) -> u32 {
        self.settings.max_output_tokens
    }

    #[must_use]
    pub const fn reasoning_effort(&self) -> Option<ReasoningEffort> {
        self.settings.reasoning.effort
    }

    #[must_use]
    pub const fn reasoning_mode(&self) -> ReasoningMode {
        self.settings.reasoning.mode
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

#[cfg(test)]
#[path = "model_tests.rs"]
mod model_tests;
