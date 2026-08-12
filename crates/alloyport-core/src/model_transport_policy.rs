//! Bounded deployment policy for model HTTP transports.

use crate::model::ModelCatalogError;
use serde::{Deserialize, Serialize};

const MAX_TRANSPORT_BODY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_TRANSPORT_METADATA_BYTES: u64 = 256 * 1024;
const MAX_TRANSPORT_TIMEOUT_MILLIS: u64 = 600_000;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelTlsMinimumVersion {
    Tls12,
    Tls13,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRedirectPolicy {
    Disabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProxyPolicy {
    Disabled,
}

/// Strict transport policy pinned to one deployment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTransportPolicy {
    connect_timeout_millis: u64,
    request_timeout_millis: u64,
    max_request_bytes: u64,
    max_response_bytes: u64,
    max_response_header_bytes: u64,
    max_diagnostic_bytes: u64,
    tls_minimum_version: ModelTlsMinimumVersion,
    redirects: ModelRedirectPolicy,
    proxy: ModelProxyPolicy,
}

impl ModelTransportPolicy {
    #[must_use]
    pub const fn connect_timeout_millis(&self) -> u64 {
        self.connect_timeout_millis
    }

    #[must_use]
    pub const fn request_timeout_millis(&self) -> u64 {
        self.request_timeout_millis
    }

    #[must_use]
    pub const fn max_request_bytes(&self) -> u64 {
        self.max_request_bytes
    }

    #[must_use]
    pub const fn max_response_bytes(&self) -> u64 {
        self.max_response_bytes
    }

    #[must_use]
    pub const fn max_response_header_bytes(&self) -> u64 {
        self.max_response_header_bytes
    }

    #[must_use]
    pub const fn max_diagnostic_bytes(&self) -> u64 {
        self.max_diagnostic_bytes
    }

    #[must_use]
    pub const fn tls_minimum_version(&self) -> ModelTlsMinimumVersion {
        self.tls_minimum_version
    }
}

pub(crate) fn validate_transport_policy(
    name: &str,
    policy: &ModelTransportPolicy,
) -> Result<(), ModelCatalogError> {
    if policy.connect_timeout_millis == 0
        || policy.request_timeout_millis == 0
        || policy.connect_timeout_millis > policy.request_timeout_millis
        || policy.request_timeout_millis > MAX_TRANSPORT_TIMEOUT_MILLIS
        || policy.max_request_bytes == 0
        || policy.max_request_bytes > MAX_TRANSPORT_BODY_BYTES
        || policy.max_response_bytes == 0
        || policy.max_response_bytes > MAX_TRANSPORT_BODY_BYTES
        || policy.max_response_header_bytes == 0
        || policy.max_response_header_bytes > MAX_TRANSPORT_METADATA_BYTES
        || policy.max_diagnostic_bytes == 0
        || policy.max_diagnostic_bytes > MAX_TRANSPORT_METADATA_BYTES
    {
        return Err(ModelCatalogError::InvalidTransportPolicy(name.to_owned()));
    }
    Ok(())
}
