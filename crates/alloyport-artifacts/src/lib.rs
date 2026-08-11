//! Content-addressed artifact storage with a crash-recoverable filesystem implementation.

pub mod adapters;
pub mod upload;

pub use adapters::{filesystem::FilesystemArtifactStore, sqlite::SqliteUploadStore};
pub use alloyport_core::{DigestParseError, Sha256Digest};

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Read};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactIdentity {
    pub digest: Sha256Digest,
    pub size_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestRequest {
    pub expected_digest: Option<Sha256Digest>,
    pub expected_size_bytes: Option<u64>,
}

impl IngestRequest {
    #[must_use]
    pub const fn unverified() -> Self {
        Self {
            expected_digest: None,
            expected_size_bytes: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestDisposition {
    Stored,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestResult {
    pub artifact: ArtifactIdentity,
    pub disposition: IngestDisposition,
}

/// Verified reader positioned at the beginning of one immutable artifact.
pub struct ArtifactReader {
    identity: ArtifactIdentity,
    source: Box<dyn Read + Send>,
}

impl ArtifactReader {
    #[must_use]
    pub fn new(identity: ArtifactIdentity, source: impl Read + Send + 'static) -> Self {
        Self {
            identity,
            source: Box::new(source),
        }
    }

    #[must_use]
    pub const fn identity(&self) -> ArtifactIdentity {
        self.identity
    }
}

impl Debug for ArtifactReader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactReader")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl Read for ArtifactReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.source.read(buffer)
    }
}

/// Object-safe interface for immutable artifact storage.
#[allow(clippy::missing_errors_doc)]
pub trait ArtifactStore: Debug + Send + Sync {
    fn ingest(
        &self,
        source: &mut dyn Read,
        request: IngestRequest,
    ) -> Result<IngestResult, ArtifactStoreError>;

    fn open(&self, digest: Sha256Digest) -> Result<ArtifactReader, ArtifactStoreError>;

    fn contains(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError>;
}

/// Administrative removal port used only after metadata proves an immutable object unreachable.
#[allow(clippy::missing_errors_doc)]
pub trait ArtifactRetentionStore: Debug + Send + Sync {
    fn remove_unreachable(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError>;
}

#[derive(Debug)]
pub enum ArtifactStoreError {
    Io {
        operation: &'static str,
        source: io::Error,
    },
    SizeLimitExceeded {
        limit_bytes: u64,
        observed_at_least_bytes: u64,
    },
    SizeMismatch {
        expected_bytes: u64,
        actual_bytes: u64,
    },
    DigestMismatch {
        expected: Sha256Digest,
        actual: Sha256Digest,
    },
    IntegrityViolation {
        digest: Sha256Digest,
        detail: &'static str,
    },
}

impl Display for ArtifactStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::SizeLimitExceeded {
                limit_bytes,
                observed_at_least_bytes,
            } => write!(
                formatter,
                "artifact exceeds {limit_bytes} byte limit (observed at least {observed_at_least_bytes})"
            ),
            Self::SizeMismatch {
                expected_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "artifact size mismatch: expected {expected_bytes}, received {actual_bytes}"
            ),
            Self::DigestMismatch { expected, actual } => {
                write!(
                    formatter,
                    "artifact digest mismatch: expected {expected}, received {actual}"
                )
            }
            Self::IntegrityViolation { digest, detail } => {
                write!(
                    formatter,
                    "artifact {digest} failed integrity verification: {detail}"
                )
            }
        }
    }
}

impl Error for ArtifactStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
