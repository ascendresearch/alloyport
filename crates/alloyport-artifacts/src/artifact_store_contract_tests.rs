//! Reusable behavioral contract for immutable Artifact port implementations.

use crate::{
    ArtifactRetentionStore, ArtifactStore, ArtifactStoreError, FilesystemArtifactStore,
    InMemoryArtifactStore, IngestDisposition, IngestRequest, Sha256Digest,
};
use std::error::Error;
use std::io::{Cursor, Read};

const MAX_ARTIFACT_BYTES: u64 = 32;

#[test]
fn filesystem_store_satisfies_immutable_artifact_contract() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let store = FilesystemArtifactStore::open(directory.path(), MAX_ARTIFACT_BYTES)?;
    immutable_artifact_contract(&store)
}

#[test]
fn memory_store_satisfies_immutable_artifact_contract() -> Result<(), Box<dyn Error>> {
    immutable_artifact_contract(&InMemoryArtifactStore::new(MAX_ARTIFACT_BYTES))
}

fn immutable_artifact_contract(
    store: &(impl ArtifactStore + ArtifactRetentionStore),
) -> Result<(), Box<dyn Error>> {
    let bytes = b"portable-artifact";
    let digest = Sha256Digest::digest_bytes(bytes);
    assert!(!store.contains(digest)?);
    assert!(store.open(digest).is_err());

    let request = IngestRequest {
        expected_digest: Some(digest),
        expected_size_bytes: Some(u64::try_from(bytes.len())?),
    };
    let stored = store.ingest(&mut Cursor::new(bytes), request)?;
    assert_eq!(stored.disposition, IngestDisposition::Stored);
    assert_eq!(stored.artifact.digest, digest);
    assert!(store.contains(digest)?);
    let mut reader = store.open(digest)?;
    let mut recovered = Vec::new();
    reader.read_to_end(&mut recovered)?;
    assert_eq!(reader.identity(), stored.artifact);
    assert_eq!(recovered, bytes);

    let duplicate = store.ingest(&mut Cursor::new(bytes), request)?;
    assert_eq!(duplicate.artifact, stored.artifact);
    assert_eq!(duplicate.disposition, IngestDisposition::AlreadyPresent);

    let rejected = b"rejected";
    let rejected_digest = Sha256Digest::digest_bytes(rejected);
    assert!(matches!(
        store.ingest(
            &mut Cursor::new(rejected),
            IngestRequest {
                expected_digest: Some(Sha256Digest::digest_bytes(b"different")),
                expected_size_bytes: None,
            },
        ),
        Err(ArtifactStoreError::DigestMismatch { .. })
    ));
    assert!(!store.contains(rejected_digest)?);
    assert!(matches!(
        store.ingest(
            &mut Cursor::new(rejected),
            IngestRequest {
                expected_digest: None,
                expected_size_bytes: Some(1),
            },
        ),
        Err(ArtifactStoreError::SizeMismatch { .. })
    ));
    assert!(!store.contains(rejected_digest)?);

    let oversized = vec![0_u8; usize::try_from(MAX_ARTIFACT_BYTES + 1)?];
    assert!(matches!(
        store.ingest(&mut Cursor::new(oversized), IngestRequest::unverified()),
        Err(ArtifactStoreError::SizeLimitExceeded { .. })
    ));

    assert!(store.remove_unreachable(digest)?);
    assert!(!store.remove_unreachable(digest)?);
    assert!(!store.contains(digest)?);
    assert!(store.open(digest).is_err());
    Ok(())
}
