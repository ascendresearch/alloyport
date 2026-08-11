//! Artifact upload session, quota, reference, and garbage-collection model.

use crate::{ArtifactIdentity, ArtifactReader, ArtifactStore, ArtifactStoreError, Sha256Digest};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginUpload {
    pub owner_id: String,
    pub upload_key: String,
    pub expected_digest: Sha256Digest,
    pub expected_size_bytes: u64,
    pub media_type: String,
    pub now_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum UploadState {
    Open = 1,
    Finalizing = 2,
    Completed = 3,
    Failed = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UploadQuotas {
    pub total_bytes: u64,
    pub per_owner_bytes: u64,
}

impl UploadQuotas {
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            total_bytes: i64::MAX as u64,
            per_owner_bytes: i64::MAX as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuotaScope {
    Total,
    Owner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum ArtifactReferenceKind {
    Upload = 1,
    AssignmentInput = 2,
    AssignmentOutput = 3,
    Receipt = 4,
    RetentionRoot = 5,
    Other = 6,
}

impl ArtifactReferenceKind {
    pub(crate) fn from_i64(value: i64) -> Result<Self, UploadError> {
        match value {
            1 => Ok(Self::Upload),
            2 => Ok(Self::AssignmentInput),
            3 => Ok(Self::AssignmentOutput),
            4 => Ok(Self::Receipt),
            5 => Ok(Self::RetentionRoot),
            6 => Ok(Self::Other),
            _ => Err(UploadError::Corrupt(format!(
                "unknown artifact reference kind {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrantArtifactReference {
    pub owner_id: String,
    pub reference_key: String,
    pub digest: Sha256Digest,
    pub kind: ArtifactReferenceKind,
    pub purpose: String,
    pub now_ms: u64,
    pub retained_until_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactReference {
    pub owner_id: String,
    pub reference_key: String,
    pub digest: Sha256Digest,
    pub kind: ArtifactReferenceKind,
    pub purpose: String,
    pub created_at_ms: u64,
    pub retained_until_ms: Option<u64>,
    pub revoked_at_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GarbageCollectionReport {
    pub collected_objects: u64,
    pub reclaimed_bytes: u64,
    pub skipped_active_readers: u64,
}

impl UploadState {
    pub(crate) fn from_i64(value: i64) -> Result<Self, UploadError> {
        match value {
            1 => Ok(Self::Open),
            2 => Ok(Self::Finalizing),
            3 => Ok(Self::Completed),
            4 => Ok(Self::Failed),
            _ => Err(UploadError::Corrupt(format!(
                "unknown upload state {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadSession {
    pub upload_id: String,
    pub owner_id: String,
    pub upload_key: String,
    pub expected_digest: Sha256Digest,
    pub expected_size_bytes: u64,
    pub media_type: String,
    pub committed_offset: u64,
    pub state: UploadState,
    pub expires_at_ms: u64,
    pub artifact: Option<ArtifactIdentity>,
}

/// Mutable upload-session and staging operations required by the Artifact application service.
#[allow(clippy::missing_errors_doc)]
pub trait ArtifactUploadRepository: Debug + Send + Sync {
    fn begin(&self, request: &BeginUpload) -> Result<UploadSession, UploadError>;
    fn status(&self, owner_id: &str, upload_id: &str) -> Result<UploadSession, UploadError>;
    fn append(
        &self,
        owner_id: &str,
        upload_id: &str,
        offset: u64,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<u64, UploadError>;
    fn finalize(
        &self,
        owner_id: &str,
        upload_id: &str,
        artifacts: &dyn ArtifactStore,
        now_ms: u64,
    ) -> Result<ArtifactIdentity, UploadError>;
    fn open_referenced_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
        artifacts: &dyn ArtifactStore,
    ) -> Result<ArtifactReader, UploadError>;
}

/// Published-object metadata and durable reference operations used by authorization/controllers.
#[allow(clippy::missing_errors_doc)]
pub trait ArtifactMetadataStore: Debug + Send + Sync {
    fn completed_upload_session_by_key(
        &self,
        owner_id: &str,
        upload_key: &str,
    ) -> Result<Option<UploadSession>, UploadError>;
    fn can_read_artifact(&self, owner_id: &str, digest: Sha256Digest) -> Result<bool, UploadError>;
    fn artifact_size_bytes(&self, digest: Sha256Digest) -> Result<Option<u64>, UploadError>;
    fn grant_reference(
        &self,
        request: &GrantArtifactReference,
    ) -> Result<ArtifactReference, UploadError>;
}

#[derive(Debug)]
pub enum UploadError {
    Storage(Box<dyn Error + Send + Sync>),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    Artifact(ArtifactStoreError),
    NotFound(String),
    OwnerMismatch,
    ConflictingUploadKey,
    ConflictingReferenceKey,
    ReferenceRevoked,
    GarbageCollectionPending(Sha256Digest),
    InvalidRequest(&'static str),
    OffsetConflict {
        expected: u64,
        received: u64,
    },
    ChunkTooLarge {
        limit: usize,
        received: usize,
    },
    SizeLimitExceeded {
        limit: u64,
        attempted: u64,
    },
    QuotaExceeded {
        scope: QuotaScope,
        limit: u64,
        used: u64,
        requested: u64,
    },
    InvalidState(UploadState),
    Expired,
    Incomplete {
        expected: u64,
        committed: u64,
    },
    Corrupt(String),
}

impl Display for UploadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "upload metadata storage error: {error}"),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::Artifact(error) => Display::fmt(error, formatter),
            Self::NotFound(id) => write!(formatter, "upload session {id} was not found"),
            Self::OwnerMismatch => write!(formatter, "upload session owner does not match"),
            Self::ConflictingUploadKey => {
                write!(formatter, "upload key was reused with other metadata")
            }
            Self::ConflictingReferenceKey => {
                write!(
                    formatter,
                    "artifact reference key was reused with other metadata"
                )
            }
            Self::ReferenceRevoked => write!(formatter, "artifact reference is revoked"),
            Self::GarbageCollectionPending(digest) => {
                write!(formatter, "artifact {digest} is pending garbage collection")
            }
            Self::InvalidRequest(detail) => write!(formatter, "invalid upload request: {detail}"),
            Self::OffsetConflict { expected, received } => write!(
                formatter,
                "upload offset conflict: expected {expected}, received {received}"
            ),
            Self::ChunkTooLarge { limit, received } => {
                write!(
                    formatter,
                    "upload chunk has {received} bytes, limit is {limit}"
                )
            }
            Self::SizeLimitExceeded { limit, attempted } => {
                write!(
                    formatter,
                    "upload would reach {attempted} bytes, limit is {limit}"
                )
            }
            Self::QuotaExceeded {
                scope,
                limit,
                used,
                requested,
            } => write!(
                formatter,
                "{scope:?} artifact quota exceeded: {used} bytes used or reserved, \
                 {requested} requested, limit is {limit}"
            ),
            Self::InvalidState(state) => write!(formatter, "upload is in {state:?} state"),
            Self::Expired => write!(formatter, "upload session has expired"),
            Self::Incomplete {
                expected,
                committed,
            } => write!(
                formatter,
                "upload is incomplete: expected {expected} bytes, committed {committed}"
            ),
            Self::Corrupt(detail) => write!(formatter, "corrupt upload session: {detail}"),
        }
    }
}

impl Error for UploadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error.as_ref()),
            Self::Io { source, .. } => Some(source),
            Self::Artifact(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ArtifactStoreError> for UploadError {
    fn from(error: ArtifactStoreError) -> Self {
        Self::Artifact(error)
    }
}
