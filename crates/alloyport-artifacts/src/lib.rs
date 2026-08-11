//! Content-addressed artifact storage with a crash-recoverable filesystem implementation.

pub mod adapters;
pub mod upload;

pub use adapters::{filesystem::FilesystemArtifactStore, sqlite::SqliteUploadStore};

use ring::digest::{Context, SHA256};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter, Write as _};
use std::io::{self, Read};
use std::str::FromStr;

const SHA256_PREFIX: &str = "sha256:";
const SHA256_BYTES: usize = 32;

/// Canonical SHA-256 content identity.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; SHA256_BYTES]);

impl Sha256Digest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; SHA256_BYTES] {
        self.0
    }

    /// Computes the canonical SHA-256 identity of one byte slice.
    #[must_use]
    pub fn digest_bytes(bytes: &[u8]) -> Self {
        let mut context = Context::new(&SHA256);
        context.update(bytes);
        let digest = context.finish();
        let mut value = [0_u8; SHA256_BYTES];
        value.copy_from_slice(digest.as_ref());
        Self(value)
    }

    pub(crate) fn hexadecimal(self) -> String {
        let mut value = String::with_capacity(SHA256_BYTES * 2);
        for byte in self.0 {
            write!(value, "{byte:02x}").expect("writing to a String cannot fail");
        }
        value
    }
}

impl Debug for Sha256Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, formatter)
    }
}

impl Display for Sha256Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{SHA256_PREFIX}{}", self.hexadecimal())
    }
}

impl FromStr for Sha256Digest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hexadecimal = value
            .strip_prefix(SHA256_PREFIX)
            .ok_or(DigestParseError::MissingPrefix)?;
        if hexadecimal.len() != SHA256_BYTES * 2 {
            return Err(DigestParseError::WrongLength(hexadecimal.len()));
        }
        let mut bytes = [0_u8; SHA256_BYTES];
        for (index, pair) in hexadecimal.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]).ok_or(DigestParseError::NonHexadecimal)?;
            let low = hex_nibble(pair[1]).ok_or(DigestParseError::NonHexadecimal)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestParseError {
    MissingPrefix,
    WrongLength(usize),
    NonHexadecimal,
}

impl Display for DigestParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrefix => write!(formatter, "digest must start with sha256:"),
            Self::WrongLength(length) => {
                write!(
                    formatter,
                    "SHA-256 hexadecimal length is {length}, expected 64"
                )
            }
            Self::NonHexadecimal => write!(formatter, "SHA-256 digest contains non-hex data"),
        }
    }
}

impl Error for DigestParseError {}

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

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
