//! Transport-neutral container-engine values shared by fixed accelerator backends.

use crate::backend_error::BackendError;
use alloyport_artifacts::Sha256Digest;
use std::future::Future;
use std::pin::Pin;

pub const OCI_IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
pub const OCI_IMAGE_CONFIG_MEDIA_TYPE: &str = "application/vnd.oci.image.config.v1+json";

/// Chooses the assignment image media type while preserving an exact local image-ID check.
///
/// Registry-backed installations may use `repository@manifest-digest`. Standalone workers may use
/// any nonempty local Docker reference when the assignment digest is the expected image config ID.
///
/// # Errors
///
/// Returns an error when a mutable/local reference is not bound to its exact local image ID.
pub fn image_artifact_media_type(
    image_reference: &str,
    assignment_digest: Sha256Digest,
    image_id: Sha256Digest,
) -> Result<&'static str, &'static str> {
    if image_reference.trim().is_empty()
        || image_reference.chars().any(char::is_whitespace)
        || image_reference.contains(',')
    {
        return Err("image reference is empty or contains an unsafe separator");
    }
    if image_reference.ends_with(&format!("@{assignment_digest}")) {
        return Ok(OCI_IMAGE_MANIFEST_MEDIA_TYPE);
    }
    if assignment_digest == image_id {
        return Ok(OCI_IMAGE_CONFIG_MEDIA_TYPE);
    }
    Err("local image references must bind the assignment digest to the exact image ID")
}

pub type EngineFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ContainerEngineError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContainerEngineError {
    InvalidConfiguration(String),
    Unavailable(String),
    CommandFailed(String),
    InvalidResponse(String),
    Internal(String),
}

impl std::fmt::Display for ContainerEngineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration(detail) => {
                write!(
                    formatter,
                    "invalid container engine configuration: {detail}"
                )
            }
            Self::Unavailable(detail) => {
                write!(formatter, "container engine unavailable: {detail}")
            }
            Self::CommandFailed(detail) => write!(formatter, "container command failed: {detail}"),
            Self::InvalidResponse(detail) => {
                write!(formatter, "invalid container engine response: {detail}")
            }
            Self::Internal(detail) => {
                write!(formatter, "container engine internal failure: {detail}")
            }
        }
    }
}

impl std::error::Error for ContainerEngineError {}

impl From<ContainerEngineError> for BackendError {
    fn from(error: ContainerEngineError) -> Self {
        let detail = error.to_string();
        match error {
            ContainerEngineError::InvalidConfiguration(_) => Self::policy(detail),
            ContainerEngineError::Unavailable(_) => Self::retryable(detail),
            ContainerEngineError::CommandFailed(_) | ContainerEngineError::Internal(_) => {
                Self::terminal(detail)
            }
            ContainerEngineError::InvalidResponse(_) => Self::integrity(detail),
        }
    }
}

impl From<String> for ContainerEngineError {
    fn from(detail: String) -> Self {
        Self::Internal(detail)
    }
}

impl From<&str> for ContainerEngineError {
    fn from(detail: &str) -> Self {
        Self::Internal(detail.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerIdentity {
    pub name: String,
    pub attempt_id: String,
    pub bundle_digest: String,
    pub image_manifest_digest: String,
    pub image_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainerPhase {
    Created,
    Running,
    Exited,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerSnapshot {
    pub identity: ContainerIdentity,
    pub phase: ContainerPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerExit {
    pub exit_code: i32,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerLogs {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub output_limit_exceeded: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainerLogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerLogChunk {
    pub stream: ContainerLogStream,
    pub byte_offset: u64,
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_manifest_and_standalone_image_id_are_both_immutable() {
        let manifest = Sha256Digest::digest_bytes(b"manifest");
        let image_id = Sha256Digest::digest_bytes(b"image config");
        assert_eq!(
            image_artifact_media_type(
                &format!("example.invalid/alloyport@{manifest}"),
                manifest,
                image_id,
            ),
            Ok(OCI_IMAGE_MANIFEST_MEDIA_TYPE)
        );
        assert_eq!(
            image_artifact_media_type("alloyport-fixture:local", image_id, image_id),
            Ok(OCI_IMAGE_CONFIG_MEDIA_TYPE)
        );
        assert!(image_artifact_media_type("alloyport-fixture:local", manifest, image_id).is_err());
    }
}
