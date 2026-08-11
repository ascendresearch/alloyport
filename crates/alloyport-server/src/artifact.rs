//! Artifact gRPC edge over durable upload sessions and the filesystem CAS.

use alloyport_artifacts::SqliteUploadStore;

use crate::identity::ConnectionIdentityResolver;
use crate::persistence::ServerPersistence;
use crate::storage::Clock;
use alloyport_artifacts::upload::{BeginUpload, UploadError, UploadSession};
use alloyport_artifacts::{ArtifactIdentity, FilesystemArtifactStore, Sha256Digest};
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactService;
use alloyport_proto::artifact_v1::{
    self, BeginUploadRequest, DownloadChunk, DownloadRequest, FinalizeUploadRequest,
    GetUploadRequest, UploadChunk,
};
use std::fmt::Debug;
use std::io::Read;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::metadata::MetadataMap;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::{Extensions, Request, Response, Status, Streaming};

const MAX_UPLOAD_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const DOWNLOAD_CHUNK_BYTES: usize = 64 * 1024;

/// Resolves authenticated ownership and authorizes reads without trusting request body fields.
#[tonic::async_trait]
pub trait ArtifactAccessPolicy: Debug + Send + Sync {
    /// Resolves the authenticated principal that owns upload sessions.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when authenticated request context is absent or invalid.
    async fn resolve_owner(
        &self,
        metadata: &MetadataMap,
        extensions: &Extensions,
    ) -> Result<String, Status>;

    /// Checks whether an authenticated principal may read an artifact.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when the artifact is not visible to the principal.
    async fn authorize_download(&self, owner_id: &str, digest: Sha256Digest) -> Result<(), Status>;
}

/// Production access policy keyed by the verified mTLS client leaf certificate.
#[derive(Clone, Debug)]
pub struct MtlsArtifactAccessPolicy {
    uploads: Arc<SqliteUploadStore>,
}

impl MtlsArtifactAccessPolicy {
    #[must_use]
    pub fn new(uploads: Arc<SqliteUploadStore>) -> Self {
        Self { uploads }
    }
}

#[tonic::async_trait]
impl ArtifactAccessPolicy for MtlsArtifactAccessPolicy {
    async fn resolve_owner(
        &self,
        _metadata: &MetadataMap,
        extensions: &Extensions,
    ) -> Result<String, Status> {
        let connection = extensions
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .ok_or_else(|| Status::unauthenticated("artifact RPC requires mutual TLS"))?;
        let certificates = connection
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("artifact RPC requires a client certificate"))?;
        let leaf = certificates.first().ok_or_else(|| {
            Status::unauthenticated("artifact RPC client certificate chain is empty")
        })?;
        Ok(format!(
            "mtls:{}",
            Sha256Digest::digest_bytes(leaf.as_ref())
        ))
    }

    async fn authorize_download(&self, owner_id: &str, digest: Sha256Digest) -> Result<(), Status> {
        let uploads = Arc::clone(&self.uploads);
        let owner_id = owner_id.to_owned();
        run_status_blocking(move || authorize_referenced_artifact(&uploads, &owner_id, digest))
            .await
    }
}

/// Stable-owner policy backed by the durable certificate enrollment registry.
#[derive(Clone, Debug)]
pub struct EnrolledArtifactAccessPolicy {
    uploads: Arc<SqliteUploadStore>,
    identities: Arc<dyn ConnectionIdentityResolver>,
}

impl EnrolledArtifactAccessPolicy {
    #[must_use]
    pub fn new(
        uploads: Arc<SqliteUploadStore>,
        identities: Arc<dyn ConnectionIdentityResolver>,
    ) -> Self {
        Self {
            uploads,
            identities,
        }
    }
}

#[tonic::async_trait]
impl ArtifactAccessPolicy for EnrolledArtifactAccessPolicy {
    async fn resolve_owner(
        &self,
        _metadata: &MetadataMap,
        extensions: &Extensions,
    ) -> Result<String, Status> {
        self.identities.resolve_owner(extensions).await
    }

    async fn authorize_download(&self, owner_id: &str, digest: Sha256Digest) -> Result<(), Status> {
        let uploads = Arc::clone(&self.uploads);
        let owner_id = owner_id.to_owned();
        run_status_blocking(move || authorize_referenced_artifact(&uploads, &owner_id, digest))
            .await
    }
}

fn authorize_referenced_artifact(
    uploads: &SqliteUploadStore,
    owner_id: &str,
    digest: Sha256Digest,
) -> Result<(), Status> {
    match uploads.can_read_artifact(owner_id, digest) {
        Ok(true) => Ok(()),
        Ok(false) => Err(Status::permission_denied(
            "artifact is not referenced by this logical owner",
        )),
        Err(error) => Err(upload_status(error)),
    }
}

#[derive(Clone, Debug)]
pub struct ArtifactServiceImpl {
    uploads: Arc<SqliteUploadStore>,
    artifacts: Arc<FilesystemArtifactStore>,
    access: Arc<dyn ArtifactAccessPolicy>,
    clock: Arc<dyn Clock>,
}

impl ArtifactServiceImpl {
    #[must_use]
    pub fn new(
        uploads: Arc<SqliteUploadStore>,
        artifacts: Arc<FilesystemArtifactStore>,
        access: Arc<dyn ArtifactAccessPolicy>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            uploads,
            artifacts,
            access,
            clock,
        }
    }

    async fn owner<T>(&self, request: &Request<T>) -> Result<String, Status> {
        self.access
            .resolve_owner(request.metadata(), request.extensions())
            .await
    }
}

#[tonic::async_trait]
impl ArtifactService for ArtifactServiceImpl {
    type DownloadStream =
        Pin<Box<dyn Stream<Item = Result<DownloadChunk, Status>> + Send + 'static>>;

    async fn begin_upload(
        &self,
        request: Request<BeginUploadRequest>,
    ) -> Result<Response<artifact_v1::UploadSession>, Status> {
        let owner_id = self.owner(&request).await?;
        let request = request.into_inner();
        if request.ttl_ms == 0 || request.ttl_ms > MAX_UPLOAD_TTL_MS {
            return Err(Status::invalid_argument("upload TTL is outside policy"));
        }
        let expected_digest = parse_digest(&request.expected_digest)?;
        let now_ms = self.clock.now_unix_ms();
        let begin = BeginUpload {
            owner_id,
            upload_key: request.upload_key,
            expected_digest,
            expected_size_bytes: request.expected_size_bytes,
            media_type: request.media_type,
            now_ms,
            expires_at_ms: now_ms.saturating_add(request.ttl_ms),
        };
        let uploads = Arc::clone(&self.uploads);
        let session = run_blocking(move || uploads.begin(&begin)).await?;
        Ok(Response::new(session_to_proto(session)))
    }

    async fn get_upload(
        &self,
        request: Request<GetUploadRequest>,
    ) -> Result<Response<artifact_v1::UploadSession>, Status> {
        let owner_id = self.owner(&request).await?;
        let upload_id = request.into_inner().upload_id;
        let uploads = Arc::clone(&self.uploads);
        let session = run_blocking(move || uploads.status(&owner_id, &upload_id)).await?;
        Ok(Response::new(session_to_proto(session)))
    }

    async fn upload(
        &self,
        request: Request<Streaming<UploadChunk>>,
    ) -> Result<Response<artifact_v1::UploadSession>, Status> {
        let owner_id = self.owner(&request).await?;
        let mut inbound = request.into_inner();
        let mut upload_id: Option<String> = None;
        while let Some(chunk) = inbound.next().await.transpose()? {
            if chunk.upload_id.is_empty() {
                return Err(Status::invalid_argument("upload ID is missing"));
            }
            if upload_id
                .as_ref()
                .is_some_and(|expected| expected != &chunk.upload_id)
            {
                return Err(Status::invalid_argument(
                    "one upload stream cannot mix session IDs",
                ));
            }
            upload_id.get_or_insert_with(|| chunk.upload_id.clone());
            let uploads = Arc::clone(&self.uploads);
            let owner = owner_id.clone();
            let now_ms = self.clock.now_unix_ms();
            run_blocking(move || {
                uploads.append(&owner, &chunk.upload_id, chunk.offset, &chunk.data, now_ms)
            })
            .await?;
        }
        let upload_id =
            upload_id.ok_or_else(|| Status::invalid_argument("upload stream is empty"))?;
        let uploads = Arc::clone(&self.uploads);
        let session = run_blocking(move || uploads.status(&owner_id, &upload_id)).await?;
        Ok(Response::new(session_to_proto(session)))
    }

    async fn finalize_upload(
        &self,
        request: Request<FinalizeUploadRequest>,
    ) -> Result<Response<artifact_v1::ArtifactIdentity>, Status> {
        let owner_id = self.owner(&request).await?;
        let upload_id = request.into_inner().upload_id;
        let uploads = Arc::clone(&self.uploads);
        let artifacts = Arc::clone(&self.artifacts);
        let now_ms = self.clock.now_unix_ms();
        let artifact = run_blocking(move || {
            uploads.finalize(&owner_id, &upload_id, artifacts.as_ref(), now_ms)
        })
        .await?;
        Ok(Response::new(identity_to_proto(artifact)))
    }

    async fn download(
        &self,
        request: Request<DownloadRequest>,
    ) -> Result<Response<Self::DownloadStream>, Status> {
        let owner_id = self.owner(&request).await?;
        let request = request.into_inner();
        let digest = parse_digest(&request.digest)?;
        self.access.authorize_download(&owner_id, digest).await?;
        let uploads = Arc::clone(&self.uploads);
        let artifacts = Arc::clone(&self.artifacts);
        let reader = run_blocking(move || {
            uploads.open_referenced_artifact(&owner_id, digest, artifacts.as_ref())
        })
        .await?;
        let (sender, receiver) = mpsc::channel(8);
        tokio::task::spawn_blocking(move || {
            stream_download(reader, request.offset, request.max_bytes, &sender);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, UploadError> + Send + 'static,
{
    ServerPersistence::default()
        .run(operation)
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(upload_status)
}

async fn run_status_blocking<T, F>(operation: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Status> + Send + 'static,
{
    ServerPersistence::default()
        .run(operation)
        .await
        .map_err(|error| Status::internal(error.to_string()))?
}

fn stream_download(
    mut reader: alloyport_artifacts::ArtifactReader,
    offset: u64,
    max_bytes: u64,
    sender: &mpsc::Sender<Result<DownloadChunk, Status>>,
) {
    let result = (|| {
        if offset > reader.identity().size_bytes {
            return Err(Status::out_of_range(
                "download offset exceeds artifact size",
            ));
        }
        let skipped = std::io::copy(&mut reader.by_ref().take(offset), &mut std::io::sink())
            .map_err(|error| Status::internal(format!("skip artifact prefix: {error}")))?;
        if skipped != offset {
            return Err(Status::data_loss("artifact ended before requested offset"));
        }
        let available = reader.identity().size_bytes.saturating_sub(offset);
        let remaining = if max_bytes == 0 {
            available
        } else {
            available.min(max_bytes)
        };
        send_reader(&mut reader.take(remaining), offset, sender)
    })();
    if let Err(status) = result {
        let _ = sender.blocking_send(Err(status));
    }
}

fn send_reader(
    reader: &mut dyn Read,
    mut offset: u64,
    sender: &mpsc::Sender<Result<DownloadChunk, Status>>,
) -> Result<(), Status> {
    let mut buffer = vec![0_u8; DOWNLOAD_CHUNK_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| Status::internal(format!("read artifact: {error}")))?;
        if read == 0 {
            return Ok(());
        }
        sender
            .blocking_send(Ok(DownloadChunk {
                offset,
                data: buffer[..read].to_vec(),
            }))
            .map_err(|_| Status::cancelled("artifact download receiver closed"))?;
        offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
}

fn parse_digest(value: &str) -> Result<Sha256Digest, Status> {
    Sha256Digest::from_str(value)
        .map_err(|error| Status::invalid_argument(format!("invalid artifact digest: {error}")))
}

fn session_to_proto(session: UploadSession) -> artifact_v1::UploadSession {
    artifact_v1::UploadSession {
        upload_id: session.upload_id,
        upload_key: session.upload_key,
        expected_digest: session.expected_digest.to_string(),
        expected_size_bytes: session.expected_size_bytes,
        media_type: session.media_type,
        committed_offset: session.committed_offset,
        state: match session.state {
            alloyport_artifacts::upload::UploadState::Open => artifact_v1::UploadState::Open,
            alloyport_artifacts::upload::UploadState::Finalizing => {
                artifact_v1::UploadState::Finalizing
            }
            alloyport_artifacts::upload::UploadState::Completed => {
                artifact_v1::UploadState::Completed
            }
            alloyport_artifacts::upload::UploadState::Failed => artifact_v1::UploadState::Failed,
        }
        .into(),
        expires_at_ms: session.expires_at_ms,
        artifact: session.artifact.map(identity_to_proto),
    }
}

fn identity_to_proto(identity: ArtifactIdentity) -> artifact_v1::ArtifactIdentity {
    artifact_v1::ArtifactIdentity {
        digest: identity.digest.to_string(),
        size_bytes: identity.size_bytes,
    }
}

pub(crate) fn upload_status(error: UploadError) -> Status {
    match error {
        UploadError::NotFound(_) => Status::not_found(error.to_string()),
        UploadError::OwnerMismatch => Status::permission_denied(error.to_string()),
        UploadError::OffsetConflict { .. } => Status::aborted(error.to_string()),
        UploadError::ChunkTooLarge { .. }
        | UploadError::SizeLimitExceeded { .. }
        | UploadError::QuotaExceeded { .. } => Status::resource_exhausted(error.to_string()),
        UploadError::InvalidRequest(_)
        | UploadError::ConflictingUploadKey
        | UploadError::ConflictingReferenceKey => Status::invalid_argument(error.to_string()),
        UploadError::ReferenceRevoked
        | UploadError::GarbageCollectionPending(_)
        | UploadError::Expired
        | UploadError::Incomplete { .. }
        | UploadError::InvalidState(_) => Status::failed_precondition(error.to_string()),
        UploadError::Artifact(error) => artifact_status(&error),
        UploadError::Storage(_) | UploadError::Io { .. } | UploadError::Corrupt(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn artifact_status(error: &alloyport_artifacts::ArtifactStoreError) -> Status {
    match error {
        alloyport_artifacts::ArtifactStoreError::DigestMismatch { .. }
        | alloyport_artifacts::ArtifactStoreError::SizeMismatch { .. }
        | alloyport_artifacts::ArtifactStoreError::IntegrityViolation { .. } => {
            Status::data_loss(error.to_string())
        }
        alloyport_artifacts::ArtifactStoreError::SizeLimitExceeded { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        alloyport_artifacts::ArtifactStoreError::Io { .. } => Status::internal(error.to_string()),
    }
}
