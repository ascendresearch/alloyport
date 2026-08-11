//! Bounded, digest-verified assignment input download from the Artifact service.

use crate::artifact_input::{ArtifactInputError, ArtifactInputFuture, ArtifactInputProvider};
use crate::journal::StoredArtifact;
use alloyport_artifacts::{ArtifactStore, IngestRequest, Sha256Digest};
use alloyport_proto::artifact_v1::DownloadRequest;
use alloyport_proto::artifact_v1::artifact_service_client::ArtifactServiceClient;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;
use tonic::Request;
use tonic::transport::Endpoint;

#[derive(Clone, Debug)]
pub struct RemoteArtifactDownloader {
    endpoint: Endpoint,
    artifacts: Arc<dyn ArtifactStore>,
    max_input_bytes: u64,
}

impl RemoteArtifactDownloader {
    /// Creates a downloader whose memory bound is explicit and no larger than the local CAS policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the input bound is zero.
    pub fn new(
        endpoint: Endpoint,
        artifacts: Arc<dyn ArtifactStore>,
        max_input_bytes: u64,
    ) -> Result<Self, ArtifactDownloadError> {
        if max_input_bytes == 0 {
            return Err(ArtifactDownloadError::InvalidConfiguration(
                "maximum input size is zero",
            ));
        }
        Ok(Self {
            endpoint,
            artifacts,
            max_input_bytes,
        })
    }

    /// Ensures the exact declared Artifact exists in the verified worker-local CAS.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid contract, authorization/transport failure, offset gap,
    /// oversized response, or digest/size mismatch.
    pub async fn download(
        &self,
        artifact: &StoredArtifact,
    ) -> Result<Sha256Digest, ArtifactDownloadError> {
        let digest = artifact.digest;
        if artifact.size_bytes > self.max_input_bytes {
            return Err(ArtifactDownloadError::SizeLimitExceeded {
                limit: self.max_input_bytes,
                declared: artifact.size_bytes,
            });
        }
        if self
            .artifacts
            .contains(digest)
            .map_err(|error| ArtifactDownloadError::Local(error.to_string()))?
        {
            let reader = self
                .artifacts
                .open(digest)
                .map_err(|error| ArtifactDownloadError::Local(error.to_string()))?;
            if reader.identity().size_bytes != artifact.size_bytes {
                return Err(ArtifactDownloadError::Protocol(
                    "local Artifact size differs from assignment".into(),
                ));
            }
            return Ok(digest);
        }

        let channel = self.endpoint.clone().connect().await?;
        let mut client = ArtifactServiceClient::new(channel);
        let mut stream = client
            .download(Request::new(DownloadRequest {
                digest: artifact.digest.to_string(),
                offset: 0,
                max_bytes: artifact.size_bytes,
            }))
            .await?
            .into_inner();
        let capacity = usize::try_from(artifact.size_bytes).map_err(|_| {
            ArtifactDownloadError::Protocol("input size exceeds this platform".into())
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        while let Some(chunk) = stream.message().await? {
            let expected = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if chunk.offset != expected {
                return Err(ArtifactDownloadError::Protocol(format!(
                    "download offset gap: expected {expected}, got {}",
                    chunk.offset
                )));
            }
            let next = expected.saturating_add(u64::try_from(chunk.data.len()).unwrap_or(u64::MAX));
            if next > artifact.size_bytes || next > self.max_input_bytes {
                return Err(ArtifactDownloadError::SizeLimitExceeded {
                    limit: artifact.size_bytes.min(self.max_input_bytes),
                    declared: next,
                });
            }
            bytes.extend_from_slice(&chunk.data);
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != artifact.size_bytes {
            return Err(ArtifactDownloadError::Protocol(format!(
                "download ended at {} bytes, expected {}",
                bytes.len(),
                artifact.size_bytes
            )));
        }
        let artifacts = Arc::clone(&self.artifacts);
        let expected_size_bytes = artifact.size_bytes;
        tokio::task::spawn_blocking(move || {
            artifacts.ingest(
                &mut Cursor::new(bytes),
                IngestRequest {
                    expected_digest: Some(digest),
                    expected_size_bytes: Some(expected_size_bytes),
                },
            )
        })
        .await
        .map_err(ArtifactDownloadError::Join)?
        .map_err(|error| ArtifactDownloadError::Local(error.to_string()))?;
        Ok(digest)
    }
}

impl ArtifactInputProvider for RemoteArtifactDownloader {
    fn materialize<'a>(&'a self, artifact: &'a StoredArtifact) -> ArtifactInputFuture<'a> {
        Box::pin(async move {
            self.download(artifact)
                .await
                .map(|_| ())
                .map_err(ArtifactInputError::from)
        })
    }
}

#[derive(Debug)]
pub enum ArtifactDownloadError {
    InvalidConfiguration(&'static str),
    SizeLimitExceeded { limit: u64, declared: u64 },
    Local(String),
    Protocol(String),
    Transport(tonic::transport::Error),
    Rpc(tonic::Status),
    Join(tokio::task::JoinError),
}

impl Display for ArtifactDownloadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(detail) => {
                write!(formatter, "invalid Artifact downloader: {detail}")
            }
            Self::SizeLimitExceeded { limit, declared } => write!(
                formatter,
                "input Artifact size {declared} exceeds download limit {limit}"
            ),
            Self::Local(detail) => write!(formatter, "local Artifact error: {detail}"),
            Self::Protocol(detail) => {
                write!(formatter, "Artifact download protocol error: {detail}")
            }
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Rpc(error) => Display::fmt(error, formatter),
            Self::Join(error) => write!(formatter, "Artifact ingest task failed: {error}"),
        }
    }
}

impl Error for ArtifactDownloadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Rpc(error) => Some(error),
            Self::Join(error) => Some(error),
            Self::InvalidConfiguration(_)
            | Self::SizeLimitExceeded { .. }
            | Self::Local(_)
            | Self::Protocol(_) => None,
        }
    }
}

impl From<tonic::transport::Error> for ArtifactDownloadError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<tonic::Status> for ArtifactDownloadError {
    fn from(error: tonic::Status) -> Self {
        Self::Rpc(error)
    }
}

impl From<ArtifactDownloadError> for ArtifactInputError {
    fn from(error: ArtifactDownloadError) -> Self {
        let detail = error.to_string();
        match error {
            ArtifactDownloadError::InvalidConfiguration(_) => Self::Invalid(detail),
            ArtifactDownloadError::SizeLimitExceeded { .. } => Self::Policy(detail),
            ArtifactDownloadError::Protocol(_) => Self::Integrity(detail),
            ArtifactDownloadError::Transport(_) | ArtifactDownloadError::Rpc(_) => {
                Self::Unavailable(detail)
            }
            ArtifactDownloadError::Local(_) | ArtifactDownloadError::Join(_) => {
                Self::Internal(detail)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_errors_map_to_stable_input_categories() {
        assert!(matches!(
            ArtifactInputError::from(ArtifactDownloadError::InvalidConfiguration("bad")),
            ArtifactInputError::Invalid(_)
        ));
        assert!(matches!(
            ArtifactInputError::from(ArtifactDownloadError::SizeLimitExceeded {
                limit: 1,
                declared: 2,
            }),
            ArtifactInputError::Policy(_)
        ));
        assert!(matches!(
            ArtifactInputError::from(ArtifactDownloadError::Protocol("gap".into())),
            ArtifactInputError::Integrity(_)
        ));
        assert!(matches!(
            ArtifactInputError::from(ArtifactDownloadError::Local("disk".into())),
            ArtifactInputError::Internal(_)
        ));
    }
}
