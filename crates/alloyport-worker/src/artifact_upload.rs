//! Resumable publication of worker-local execution artifacts through the Artifact RPC.

use crate::executor::{ArtifactPublicationError, ArtifactPublisher, ArtifactReferenceIntent};
use alloyport_artifacts::{ArtifactStore, ArtifactStoreError, Sha256Digest};
use alloyport_proto::PROTOBUF_MESSAGE_OVERHEAD_BYTES;
use alloyport_proto::artifact_v1::artifact_service_client::ArtifactServiceClient;
use alloyport_proto::artifact_v1::{
    ArtifactIdentity, BeginUploadRequest, FinalizeUploadRequest, UploadChunk, UploadSession,
    UploadState,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::Read;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Endpoint;

const DEFAULT_UPLOAD_TTL_MS: u64 = 60 * 60 * 1_000;

/// A local execution artifact could not be durably published to the controller.
#[derive(Debug)]
pub enum RemoteArtifactUploadError {
    InvalidConfiguration(&'static str),
    LocalArtifact(ArtifactStoreError),
    LocalRead(std::io::Error),
    Transport(tonic::transport::Error),
    Rpc(tonic::Status),
    ProducerJoin(tokio::task::JoinError),
    StreamClosed,
    Protocol(String),
}

impl Display for RemoteArtifactUploadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(detail) => {
                write!(
                    formatter,
                    "invalid remote Artifact uploader configuration: {detail}"
                )
            }
            Self::LocalArtifact(error) => Display::fmt(error, formatter),
            Self::LocalRead(error) => write!(formatter, "read local Artifact: {error}"),
            Self::Transport(error) => Display::fmt(error, formatter),
            Self::Rpc(error) => Display::fmt(error, formatter),
            Self::ProducerJoin(error) => {
                write!(formatter, "Artifact upload producer failed: {error}")
            }
            Self::StreamClosed => write!(formatter, "Artifact upload stream closed"),
            Self::Protocol(detail) => write!(formatter, "Artifact upload protocol error: {detail}"),
        }
    }
}

impl Error for RemoteArtifactUploadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalArtifact(error) => Some(error),
            Self::LocalRead(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Rpc(error) => Some(error),
            Self::ProducerJoin(error) => Some(error),
            Self::InvalidConfiguration(_) | Self::StreamClosed | Self::Protocol(_) => None,
        }
    }
}

/// Publishes a local filesystem CAS through resumable, owner-bound Artifact sessions.
#[derive(Clone, Debug)]
pub struct RemoteArtifactPublisher {
    endpoint: Endpoint,
    artifacts: Arc<dyn ArtifactStore>,
    chunk_bytes: usize,
    upload_ttl_ms: u64,
}

impl RemoteArtifactPublisher {
    /// Creates a publisher that reuses each reference key as its stable upload idempotency key.
    ///
    /// # Errors
    ///
    /// Returns an error when chunk size or session TTL is zero.
    pub fn new(
        endpoint: Endpoint,
        artifacts: Arc<dyn ArtifactStore>,
        chunk_bytes: usize,
        upload_ttl_ms: Option<u64>,
    ) -> Result<Self, RemoteArtifactUploadError> {
        if chunk_bytes == 0 {
            return Err(RemoteArtifactUploadError::InvalidConfiguration(
                "chunk size is zero",
            ));
        }
        let upload_ttl_ms = upload_ttl_ms.unwrap_or(DEFAULT_UPLOAD_TTL_MS);
        if upload_ttl_ms == 0 {
            return Err(RemoteArtifactUploadError::InvalidConfiguration(
                "upload TTL is zero",
            ));
        }
        Ok(Self {
            endpoint,
            artifacts,
            chunk_bytes,
            upload_ttl_ms,
        })
    }

    /// Publishes all references over one authenticated Artifact client connection.
    ///
    /// # Errors
    ///
    /// Returns on the first local integrity, transport, RPC, or server-contract failure. Retrying
    /// the same references resumes from the server's committed offsets.
    pub async fn publish_references(
        &self,
        references: &[ArtifactReferenceIntent],
    ) -> Result<(), RemoteArtifactUploadError> {
        if references.is_empty() {
            return Ok(());
        }
        let channel = self.endpoint.clone().connect().await?;
        let mut client = ArtifactServiceClient::new(channel).max_encoding_message_size(
            self.chunk_bytes
                .saturating_add(PROTOBUF_MESSAGE_OVERHEAD_BYTES),
        );
        for reference in references {
            self.publish_one(&mut client, reference).await?;
        }
        Ok(())
    }

    async fn publish_one(
        &self,
        client: &mut ArtifactServiceClient<tonic::transport::Channel>,
        reference: &ArtifactReferenceIntent,
    ) -> Result<(), RemoteArtifactUploadError> {
        let expected_digest = reference.artifact.digest;
        let session = client
            .begin_upload(Request::new(BeginUploadRequest {
                upload_key: reference.reference_key.clone(),
                expected_digest: reference.artifact.digest.to_string(),
                expected_size_bytes: reference.artifact.size_bytes,
                media_type: reference.artifact.media_type.clone(),
                ttl_ms: self.upload_ttl_ms,
            }))
            .await?
            .into_inner();
        validate_session(&session, reference)?;
        let state = UploadState::try_from(session.state).unwrap_or(UploadState::Unspecified);
        if state == UploadState::Completed {
            return validate_completed(session.artifact.as_ref(), reference);
        }
        if state == UploadState::Failed {
            return Err(RemoteArtifactUploadError::Protocol(format!(
                "upload {} is terminally failed",
                session.upload_id
            )));
        }
        if session.committed_offset < reference.artifact.size_bytes {
            let resumed = self
                .stream_remaining(client, expected_digest, &session)
                .await?;
            validate_session(&resumed, reference)?;
            if resumed.committed_offset != reference.artifact.size_bytes {
                return Err(RemoteArtifactUploadError::Protocol(format!(
                    "upload {} committed {} bytes, expected {}",
                    resumed.upload_id, resumed.committed_offset, reference.artifact.size_bytes
                )));
            }
        }
        let artifact = client
            .finalize_upload(Request::new(FinalizeUploadRequest {
                upload_id: session.upload_id,
            }))
            .await?
            .into_inner();
        validate_identity(&artifact, reference)
    }

    async fn stream_remaining(
        &self,
        client: &mut ArtifactServiceClient<tonic::transport::Channel>,
        digest: Sha256Digest,
        session: &UploadSession,
    ) -> Result<UploadSession, RemoteArtifactUploadError> {
        let (sender, receiver) = mpsc::channel(4);
        let artifacts = Arc::clone(&self.artifacts);
        let upload_id = session.upload_id.clone();
        let offset = session.committed_offset;
        let chunk_bytes = self.chunk_bytes;
        let producer = tokio::task::spawn_blocking(move || {
            stream_local_artifact(
                artifacts.as_ref(),
                digest,
                &upload_id,
                offset,
                chunk_bytes,
                &sender,
            )
        });
        let response = client
            .upload(Request::new(ReceiverStream::new(receiver)))
            .await;
        producer
            .await
            .map_err(RemoteArtifactUploadError::ProducerJoin)??;
        Ok(response?.into_inner())
    }
}

impl ArtifactPublisher for RemoteArtifactPublisher {
    fn publish<'a>(
        &'a self,
        references: &'a [ArtifactReferenceIntent],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), ArtifactPublicationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.publish_references(references)
                .await
                .map_err(Into::into)
        })
    }
}

fn stream_local_artifact(
    artifacts: &dyn ArtifactStore,
    digest: Sha256Digest,
    upload_id: &str,
    mut offset: u64,
    chunk_bytes: usize,
    sender: &mpsc::Sender<UploadChunk>,
) -> Result<(), RemoteArtifactUploadError> {
    let mut reader = artifacts.open(digest)?;
    let skipped = std::io::copy(&mut reader.by_ref().take(offset), &mut std::io::sink())?;
    if skipped != offset {
        return Err(RemoteArtifactUploadError::Protocol(format!(
            "local Artifact ended at {skipped}, before resume offset {offset}"
        )));
    }
    let mut buffer = vec![0_u8; chunk_bytes];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        sender
            .blocking_send(UploadChunk {
                upload_id: upload_id.to_owned(),
                offset,
                data: buffer[..read].to_vec(),
            })
            .map_err(|_| RemoteArtifactUploadError::StreamClosed)?;
        offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
}

fn validate_session(
    session: &UploadSession,
    reference: &ArtifactReferenceIntent,
) -> Result<(), RemoteArtifactUploadError> {
    if session.upload_key != reference.reference_key
        || session.expected_digest != reference.artifact.digest.to_string()
        || session.expected_size_bytes != reference.artifact.size_bytes
        || session.media_type != reference.artifact.media_type
    {
        return Err(RemoteArtifactUploadError::Protocol(format!(
            "upload {} does not match reference {}",
            session.upload_id, reference.reference_key
        )));
    }
    if session.committed_offset > session.expected_size_bytes {
        return Err(RemoteArtifactUploadError::Protocol(format!(
            "upload {} committed beyond its declared size",
            session.upload_id
        )));
    }
    Ok(())
}

fn validate_completed(
    artifact: Option<&ArtifactIdentity>,
    reference: &ArtifactReferenceIntent,
) -> Result<(), RemoteArtifactUploadError> {
    let artifact = artifact.ok_or_else(|| {
        RemoteArtifactUploadError::Protocol("completed upload lacks Artifact identity".into())
    })?;
    validate_identity(artifact, reference)
}

fn validate_identity(
    artifact: &ArtifactIdentity,
    reference: &ArtifactReferenceIntent,
) -> Result<(), RemoteArtifactUploadError> {
    if artifact.digest != reference.artifact.digest.to_string()
        || artifact.size_bytes != reference.artifact.size_bytes
    {
        return Err(RemoteArtifactUploadError::Protocol(format!(
            "finalized Artifact does not match reference {}",
            reference.reference_key
        )));
    }
    Ok(())
}

impl From<ArtifactStoreError> for RemoteArtifactUploadError {
    fn from(error: ArtifactStoreError) -> Self {
        Self::LocalArtifact(error)
    }
}

impl From<std::io::Error> for RemoteArtifactUploadError {
    fn from(error: std::io::Error) -> Self {
        Self::LocalRead(error)
    }
}

impl From<tonic::transport::Error> for RemoteArtifactUploadError {
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<tonic::Status> for RemoteArtifactUploadError {
    fn from(error: tonic::Status) -> Self {
        Self::Rpc(error)
    }
}

impl From<RemoteArtifactUploadError> for ArtifactPublicationError {
    fn from(error: RemoteArtifactUploadError) -> Self {
        let detail = error.to_string();
        match error {
            RemoteArtifactUploadError::LocalArtifact(_)
            | RemoteArtifactUploadError::LocalRead(_) => Self::LocalArtifact(detail),
            RemoteArtifactUploadError::Transport(_)
            | RemoteArtifactUploadError::ProducerJoin(_)
            | RemoteArtifactUploadError::StreamClosed => Self::Unavailable(detail),
            RemoteArtifactUploadError::Rpc(status)
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::ResourceExhausted
                        | tonic::Code::Aborted
                ) =>
            {
                Self::Unavailable(detail)
            }
            RemoteArtifactUploadError::Rpc(_) | RemoteArtifactUploadError::Protocol(_) => {
                Self::Rejected(detail)
            }
            RemoteArtifactUploadError::InvalidConfiguration(_) => Self::Internal(detail),
        }
    }
}

#[cfg(test)]
#[path = "artifact_upload_tests.rs"]
mod tests;
