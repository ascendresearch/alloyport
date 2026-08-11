//! Behavioral tests for the Artifact store module.

use super::*;
use std::error::Error;
use std::fs;
use std::io::{self, Cursor, Read};
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
fn duplicate_content_is_idempotent_without_replacing_the_object() -> Result<(), Box<dyn Error>> {
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
