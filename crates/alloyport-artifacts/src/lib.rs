//! Content-addressed artifact storage with a crash-recoverable filesystem implementation.

pub mod adapters;
pub mod upload;

pub use adapters::sqlite::SqliteUploadStore;

use ring::digest::{Context, SHA256};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter, Write as _};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SHA256_PREFIX: &str = "sha256:";
const SHA256_BYTES: usize = 32;
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const STAGING_DIRECTORY: &str = ".staging";
const OBJECT_DIRECTORY: &str = "sha256";

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

    fn hexadecimal(self) -> String {
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

/// Filesystem CAS rooted at one directory. Staging and objects share a filesystem so publication
/// can use an atomic hard link without replacing an existing immutable object.
pub struct FilesystemArtifactStore {
    root: PathBuf,
    staging: PathBuf,
    objects: PathBuf,
    max_artifact_bytes: u64,
    upload_counter: AtomicU64,
}

impl Debug for FilesystemArtifactStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FilesystemArtifactStore")
            .field("root", &self.root)
            .field("max_artifact_bytes", &self.max_artifact_bytes)
            .finish_non_exhaustive()
    }
}

impl FilesystemArtifactStore {
    /// Opens or initializes a store and removes files left in staging by interrupted processes.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the root cannot be initialized or stale staging files cannot be
    /// removed safely.
    pub fn open(
        root: impl AsRef<Path>,
        max_artifact_bytes: u64,
    ) -> Result<Self, ArtifactStoreError> {
        let root = root.as_ref().to_path_buf();
        let staging = root.join(STAGING_DIRECTORY);
        let objects = root.join(OBJECT_DIRECTORY);
        create_directory(&root, "create artifact root")?;
        create_directory(&staging, "create artifact staging directory")?;
        create_directory(&objects, "create artifact object directory")?;
        cleanup_staging(&staging)?;
        Ok(Self {
            root,
            staging,
            objects,
            max_artifact_bytes,
            upload_counter: AtomicU64::new(unique_seed()),
        })
    }

    #[must_use]
    pub const fn max_artifact_bytes(&self) -> u64 {
        self.max_artifact_bytes
    }

    /// Returns the private object path for diagnostics and local executor integration.
    /// Callers must not construct paths from unvalidated digest strings.
    #[must_use]
    pub fn object_path(&self, digest: Sha256Digest) -> PathBuf {
        let hexadecimal = digest.hexadecimal();
        self.objects.join(&hexadecimal[..2]).join(hexadecimal)
    }

    /// Removes one object selected by the metadata layer as unreachable.
    ///
    /// This operation does not decide reachability. Callers must serialize it with publication and
    /// active readers, then durably remove the corresponding metadata.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the object or its fanout directory cannot be updated safely.
    pub fn remove_unreachable(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError> {
        let path = self.object_path(digest);
        match fs::remove_file(&path) {
            Ok(()) => {
                let parent = path.parent().ok_or_else(|| ArtifactStoreError::Io {
                    operation: "resolve artifact fanout directory",
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "content-addressed path has no parent",
                    ),
                })?;
                sync_directory(parent, "sync artifact deletion")?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(ArtifactStoreError::Io {
                operation: "remove unreachable artifact",
                source,
            }),
        }
    }

    fn create_staging_file(&self) -> Result<(PathBuf, File), ArtifactStoreError> {
        for _ in 0..32 {
            let sequence = self.upload_counter.fetch_add(1, Ordering::Relaxed);
            let path = self
                .staging
                .join(format!("upload-{}-{sequence}", std::process::id()));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => return Ok((path, file)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(ArtifactStoreError::Io {
                        operation: "create artifact staging file",
                        source,
                    });
                }
            }
        }
        Err(ArtifactStoreError::Io {
            operation: "allocate unique artifact staging file",
            source: io::Error::new(io::ErrorKind::AlreadyExists, "staging name collision"),
        })
    }

    fn write_staged(
        &self,
        source: &mut dyn Read,
        staged: &mut File,
    ) -> Result<ArtifactIdentity, ArtifactStoreError> {
        let mut context = Context::new(&SHA256);
        let mut size_bytes = 0_u64;
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|source| ArtifactStoreError::Io {
                    operation: "read artifact upload",
                    source,
                })?;
            if read == 0 {
                break;
            }
            let read_u64 = u64::try_from(read).unwrap_or(u64::MAX);
            let next_size = size_bytes.saturating_add(read_u64);
            if next_size > self.max_artifact_bytes {
                return Err(ArtifactStoreError::SizeLimitExceeded {
                    limit_bytes: self.max_artifact_bytes,
                    observed_at_least_bytes: next_size,
                });
            }
            staged
                .write_all(&buffer[..read])
                .map_err(|source| ArtifactStoreError::Io {
                    operation: "write artifact staging file",
                    source,
                })?;
            context.update(&buffer[..read]);
            size_bytes = next_size;
        }
        staged.sync_all().map_err(|source| ArtifactStoreError::Io {
            operation: "sync artifact staging file",
            source,
        })?;
        Ok(ArtifactIdentity {
            digest: digest_from_context(context),
            size_bytes,
        })
    }

    fn validate_request(
        identity: ArtifactIdentity,
        request: IngestRequest,
    ) -> Result<(), ArtifactStoreError> {
        if let Some(expected_size) = request.expected_size_bytes
            && expected_size != identity.size_bytes
        {
            return Err(ArtifactStoreError::SizeMismatch {
                expected_bytes: expected_size,
                actual_bytes: identity.size_bytes,
            });
        }
        if let Some(expected_digest) = request.expected_digest
            && expected_digest != identity.digest
        {
            return Err(ArtifactStoreError::DigestMismatch {
                expected: expected_digest,
                actual: identity.digest,
            });
        }
        Ok(())
    }

    fn publish(
        &self,
        staged_path: &Path,
        identity: ArtifactIdentity,
    ) -> Result<IngestDisposition, ArtifactStoreError> {
        let final_path = self.object_path(identity.digest);
        let parent = final_path
            .parent()
            .expect("content-addressed paths always have a fanout parent");
        create_directory(parent, "create artifact fanout directory")?;
        sync_directory(&self.objects, "sync artifact object directory")?;
        match fs::hard_link(staged_path, &final_path) {
            Ok(()) => {
                sync_directory(parent, "sync artifact fanout directory")?;
                Ok(IngestDisposition::Stored)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Self::verify_path(&final_path, identity.digest)?;
                Ok(IngestDisposition::AlreadyPresent)
            }
            Err(source) => Err(ArtifactStoreError::Io {
                operation: "publish artifact atomically",
                source,
            }),
        }
    }

    fn verify_path(
        path: &Path,
        expected: Sha256Digest,
    ) -> Result<ArtifactIdentity, ArtifactStoreError> {
        let mut file = File::open(path).map_err(|source| ArtifactStoreError::Io {
            operation: "open artifact for verification",
            source,
        })?;
        hash_reader(&mut file).and_then(|identity| {
            if identity.digest == expected {
                Ok(identity)
            } else {
                Err(ArtifactStoreError::IntegrityViolation {
                    digest: expected,
                    detail: "stored bytes do not match their content-addressed path",
                })
            }
        })
    }
}

impl ArtifactStore for FilesystemArtifactStore {
    fn ingest(
        &self,
        source: &mut dyn Read,
        request: IngestRequest,
    ) -> Result<IngestResult, ArtifactStoreError> {
        if request
            .expected_size_bytes
            .is_some_and(|size| size > self.max_artifact_bytes)
        {
            return Err(ArtifactStoreError::SizeLimitExceeded {
                limit_bytes: self.max_artifact_bytes,
                observed_at_least_bytes: request.expected_size_bytes.unwrap_or(u64::MAX),
            });
        }
        let (staged_path, mut staged) = self.create_staging_file()?;
        let staged_result = self.write_staged(source, &mut staged);
        drop(staged);
        let result = staged_result.and_then(|identity| {
            Self::validate_request(identity, request)?;
            seal_staging_file(&staged_path)?;
            self.publish(&staged_path, identity)
                .map(|disposition| IngestResult {
                    artifact: identity,
                    disposition,
                })
        });
        remove_staging_file(&staged_path)?;
        result
    }

    fn open(&self, digest: Sha256Digest) -> Result<ArtifactReader, ArtifactStoreError> {
        let path = self.object_path(digest);
        let mut file = File::open(path).map_err(|source| ArtifactStoreError::Io {
            operation: "open artifact",
            source,
        })?;
        let identity = hash_reader(&mut file)?;
        if identity.digest != digest {
            return Err(ArtifactStoreError::IntegrityViolation {
                digest,
                detail: "stored bytes do not match their content-addressed path",
            });
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|source| ArtifactStoreError::Io {
                operation: "rewind verified artifact",
                source,
            })?;
        Ok(ArtifactReader::new(identity, file))
    }

    fn contains(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError> {
        match fs::metadata(self.object_path(digest)) {
            Ok(metadata) => Ok(metadata.is_file()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(ArtifactStoreError::Io {
                operation: "inspect artifact",
                source,
            }),
        }
    }
}

fn hash_reader(source: &mut dyn Read) -> Result<ArtifactIdentity, ArtifactStoreError> {
    let mut context = Context::new(&SHA256);
    let mut size_bytes = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|source| ArtifactStoreError::Io {
                operation: "read artifact for verification",
                source,
            })?;
        if read == 0 {
            break;
        }
        context.update(&buffer[..read]);
        size_bytes = size_bytes.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
    Ok(ArtifactIdentity {
        digest: digest_from_context(context),
        size_bytes,
    })
}

fn digest_from_context(context: Context) -> Sha256Digest {
    let digest = context.finish();
    let mut bytes = [0_u8; SHA256_BYTES];
    bytes.copy_from_slice(digest.as_ref());
    Sha256Digest::from_bytes(bytes)
}

fn create_directory(path: &Path, operation: &'static str) -> Result<(), ArtifactStoreError> {
    fs::create_dir_all(path).map_err(|source| ArtifactStoreError::Io { operation, source })
}

fn sync_directory(path: &Path, operation: &'static str) -> Result<(), ArtifactStoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ArtifactStoreError::Io { operation, source })
}

fn cleanup_staging(staging: &Path) -> Result<(), ArtifactStoreError> {
    let entries = fs::read_dir(staging).map_err(|source| ArtifactStoreError::Io {
        operation: "scan artifact staging directory",
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| ArtifactStoreError::Io {
            operation: "read artifact staging entry",
            source,
        })?;
        let file_type = entry.file_type().map_err(|source| ArtifactStoreError::Io {
            operation: "inspect artifact staging entry",
            source,
        })?;
        if !file_type.is_file() && !file_type.is_symlink() {
            return Err(ArtifactStoreError::Io {
                operation: "clean artifact staging directory",
                source: io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected non-file entry in staging",
                ),
            });
        }
        fs::remove_file(entry.path()).map_err(|source| ArtifactStoreError::Io {
            operation: "remove stale artifact staging file",
            source,
        })?;
    }
    sync_directory(staging, "sync cleaned artifact staging directory")
}

fn remove_staging_file(path: &Path) -> Result<(), ArtifactStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ArtifactStoreError::Io {
            operation: "remove artifact staging file",
            source,
        }),
    }
}

fn seal_staging_file(path: &Path) -> Result<(), ArtifactStoreError> {
    let mut permissions = fs::metadata(path)
        .map_err(|source| ArtifactStoreError::Io {
            operation: "inspect artifact staging permissions",
            source,
        })?
        .permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions).map_err(|source| ArtifactStoreError::Io {
        operation: "seal artifact staging file read-only",
        source,
    })
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn unique_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Barrier};

    #[test]
    fn digest_parser_accepts_uppercase_and_formats_canonically() -> Result<(), Box<dyn Error>> {
        let parsed = format!("sha256:{}", "AB".repeat(SHA256_BYTES)).parse::<Sha256Digest>()?;
        assert_eq!(
            parsed.to_string(),
            format!("sha256:{}", "ab".repeat(SHA256_BYTES))
        );
        assert!(matches!(
            "sha256:not-a-digest".parse::<Sha256Digest>(),
            Err(DigestParseError::WrongLength(_))
        ));
        Ok(())
    }

    #[test]
    fn streaming_ingest_can_be_verified_and_read_back() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        let mut source = Cursor::new(b"hello".as_slice());
        let result = store.ingest(
            &mut source,
            IngestRequest {
                expected_digest: None,
                expected_size_bytes: Some(5),
            },
        )?;
        assert_eq!(result.disposition, IngestDisposition::Stored);
        assert_eq!(result.artifact.size_bytes, 5);
        assert!(store.contains(result.artifact.digest)?);

        let mut reader = store.open(result.artifact.digest)?;
        assert_eq!(reader.identity(), result.artifact);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        assert_eq!(bytes, b"hello");
        assert!(
            fs::metadata(store.object_path(result.artifact.digest))?
                .permissions()
                .readonly()
        );
        Ok(())
    }

    #[test]
    fn duplicate_content_is_idempotent_without_replacing_the_object() -> Result<(), Box<dyn Error>>
    {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        let mut first_source = Cursor::new(b"same bytes".as_slice());
        let first = store.ingest(&mut first_source, IngestRequest::unverified())?;
        let first_metadata = fs::metadata(store.object_path(first.artifact.digest))?;

        let mut second_source = Cursor::new(b"same bytes".as_slice());
        let second = store.ingest(
            &mut second_source,
            IngestRequest {
                expected_digest: Some(first.artifact.digest),
                expected_size_bytes: Some(first.artifact.size_bytes),
            },
        )?;
        assert_eq!(second.disposition, IngestDisposition::AlreadyPresent);
        assert_eq!(second.artifact, first.artifact);
        assert_eq!(
            fs::metadata(store.object_path(first.artifact.digest))?.len(),
            first_metadata.len()
        );
        assert!(staging_is_empty(&store)?);
        Ok(())
    }

    #[test]
    fn concurrent_identical_uploads_publish_exactly_one_object() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(FilesystemArtifactStore::open(directory.path(), 1_024)?);
        let barrier = Arc::new(Barrier::new(4));
        let handles = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut source = Cursor::new(b"concurrent bytes".as_slice());
                    barrier.wait();
                    store.ingest(&mut source, IngestRequest::unverified())
                })
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("fixture upload thread must not panic"))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results
                .iter()
                .filter(|result| result.disposition == IngestDisposition::Stored)
                .count(),
            1
        );
        assert!(
            results
                .iter()
                .all(|result| result.artifact == results[0].artifact)
        );
        assert!(staging_is_empty(&store)?);
        Ok(())
    }

    #[test]
    fn digest_mismatch_rejects_upload_and_removes_partial_state() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        let expected = Sha256Digest::from_bytes([0_u8; SHA256_BYTES]);
        let mut source = Cursor::new(b"different".as_slice());
        assert!(matches!(
            store.ingest(
                &mut source,
                IngestRequest {
                    expected_digest: Some(expected),
                    expected_size_bytes: None,
                }
            ),
            Err(ArtifactStoreError::DigestMismatch { .. })
        ));
        assert!(!store.contains(expected)?);
        assert!(staging_is_empty(&store)?);
        Ok(())
    }

    #[test]
    fn size_limit_stops_streaming_upload_and_cleans_staging() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 4)?;
        let mut source = Cursor::new(b"five!".as_slice());
        assert!(matches!(
            store.ingest(&mut source, IngestRequest::unverified()),
            Err(ArtifactStoreError::SizeLimitExceeded { limit_bytes: 4, .. })
        ));
        assert!(staging_is_empty(&store)?);
        Ok(())
    }

    #[test]
    fn interrupted_reader_cleans_staging_without_publishing() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        let mut source = InterruptingReader::default();
        assert!(matches!(
            store.ingest(&mut source, IngestRequest::unverified()),
            Err(ArtifactStoreError::Io {
                operation: "read artifact upload",
                ..
            })
        ));
        assert!(staging_is_empty(&store)?);
        Ok(())
    }

    #[test]
    fn reopen_cleans_crash_residue_and_preserves_published_content() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        let mut source = Cursor::new(b"durable".as_slice());
        let artifact = store
            .ingest(&mut source, IngestRequest::unverified())?
            .artifact;
        fs::write(store.staging.join("interrupted-upload"), b"partial")?;
        drop(store);

        let reopened = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        assert!(staging_is_empty(&reopened)?);
        let mut reader = reopened.open(artifact.digest)?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        assert_eq!(bytes, b"durable");
        Ok(())
    }

    #[test]
    fn corrupted_existing_object_is_never_replaced_by_a_retry() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let store = FilesystemArtifactStore::open(directory.path(), 1_024)?;
        let mut source = Cursor::new(b"immutable".as_slice());
        let first = store.ingest(&mut source, IngestRequest::unverified())?;
        let path = store.object_path(first.artifact.digest);
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        #[cfg(not(unix))]
        return Ok(());
        fs::write(&path, b"tampered")?;

        let mut retry = Cursor::new(b"immutable".as_slice());
        assert!(matches!(
            store.ingest(&mut retry, IngestRequest::unverified()),
            Err(ArtifactStoreError::IntegrityViolation { .. })
        ));
        assert_eq!(fs::read(path)?, b"tampered");
        assert!(staging_is_empty(&store)?);
        Ok(())
    }

    fn staging_is_empty(store: &FilesystemArtifactStore) -> Result<bool, io::Error> {
        Ok(fs::read_dir(&store.staging)?.next().is_none())
    }

    #[derive(Default)]
    struct InterruptingReader {
        emitted: bool,
    }

    impl Read for InterruptingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.emitted {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "fixture interruption",
                ));
            }
            self.emitted = true;
            buffer[..4].copy_from_slice(b"part");
            Ok(4)
        }
    }
}
