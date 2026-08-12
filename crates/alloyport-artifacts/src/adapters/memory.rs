//! Non-durable in-memory implementation of the immutable Artifact ports.

use crate::{
    ArtifactIdentity, ArtifactReader, ArtifactRetentionStore, ArtifactStore, ArtifactStoreError,
    IngestDisposition, IngestRequest, IngestResult, Sha256Digest,
};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Read};
use std::sync::{Mutex, MutexGuard};

const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Process-local Artifact storage for tests and explicitly ephemeral embeddings.
///
/// This adapter obeys the same immutable content and verification contract as the filesystem CAS,
/// but loses every object when the process exits. Production composition roots must select a
/// durable adapter when restart recovery matters.
pub struct InMemoryArtifactStore {
    objects: Mutex<BTreeMap<Sha256Digest, Vec<u8>>>,
    max_artifact_bytes: u64,
}

impl InMemoryArtifactStore {
    #[must_use]
    pub const fn new(max_artifact_bytes: u64) -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            max_artifact_bytes,
        }
    }

    fn objects(
        &self,
        operation: &'static str,
    ) -> Result<MutexGuard<'_, BTreeMap<Sha256Digest, Vec<u8>>>, ArtifactStoreError> {
        self.objects.lock().map_err(|_| ArtifactStoreError::Io {
            operation,
            source: io::Error::other("in-memory Artifact store lock is poisoned"),
        })
    }
}

impl Debug for InMemoryArtifactStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryArtifactStore")
            .field("max_artifact_bytes", &self.max_artifact_bytes)
            .finish_non_exhaustive()
    }
}

impl ArtifactStore for InMemoryArtifactStore {
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
        let mut bytes = Vec::new();
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|source| ArtifactStoreError::Io {
                    operation: "read in-memory Artifact upload",
                    source,
                })?;
            if read == 0 {
                break;
            }
            let next_size = u64::try_from(bytes.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
            if next_size > self.max_artifact_bytes {
                return Err(ArtifactStoreError::SizeLimitExceeded {
                    limit_bytes: self.max_artifact_bytes,
                    observed_at_least_bytes: next_size,
                });
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        let identity = ArtifactIdentity {
            digest: Sha256Digest::digest_bytes(&bytes),
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        };
        validate_request(identity, request)?;
        let disposition = match self
            .objects("publish in-memory Artifact")?
            .entry(identity.digest)
        {
            Entry::Vacant(entry) => {
                entry.insert(bytes);
                IngestDisposition::Stored
            }
            Entry::Occupied(entry) if entry.get() == &bytes => IngestDisposition::AlreadyPresent,
            Entry::Occupied(_) => {
                return Err(ArtifactStoreError::IntegrityViolation {
                    digest: identity.digest,
                    detail: "stored bytes do not match their content digest",
                });
            }
        };
        Ok(IngestResult {
            artifact: identity,
            disposition,
        })
    }

    fn open(&self, digest: Sha256Digest) -> Result<ArtifactReader, ArtifactStoreError> {
        let bytes = self
            .objects("open in-memory Artifact")?
            .get(&digest)
            .cloned()
            .ok_or_else(|| ArtifactStoreError::Io {
                operation: "open in-memory Artifact",
                source: io::Error::new(io::ErrorKind::NotFound, "Artifact is not present"),
            })?;
        let identity = ArtifactIdentity {
            digest,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        };
        if Sha256Digest::digest_bytes(&bytes) != digest {
            return Err(ArtifactStoreError::IntegrityViolation {
                digest,
                detail: "stored bytes do not match their content digest",
            });
        }
        Ok(ArtifactReader::new(identity, io::Cursor::new(bytes)))
    }

    fn contains(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError> {
        Ok(self
            .objects("inspect in-memory Artifact")?
            .contains_key(&digest))
    }
}

impl ArtifactRetentionStore for InMemoryArtifactStore {
    fn remove_unreachable(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError> {
        Ok(self
            .objects("remove in-memory Artifact")?
            .remove(&digest)
            .is_some())
    }
}

fn validate_request(
    identity: ArtifactIdentity,
    request: IngestRequest,
) -> Result<(), ArtifactStoreError> {
    if let Some(expected_bytes) = request.expected_size_bytes
        && expected_bytes != identity.size_bytes
    {
        return Err(ArtifactStoreError::SizeMismatch {
            expected_bytes,
            actual_bytes: identity.size_bytes,
        });
    }
    if let Some(expected) = request.expected_digest
        && expected != identity.digest
    {
        return Err(ArtifactStoreError::DigestMismatch {
            expected,
            actual: identity.digest,
        });
    }
    Ok(())
}
