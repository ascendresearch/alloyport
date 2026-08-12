//! Behavioral tests for the `SQLite` upload adapter.

use super::*;
use crate::upload::{
    ArtifactReferenceKind, GarbageCollectionReport, GrantArtifactReference, QuotaScope,
};
use crate::{ArtifactRetentionStore, ArtifactStore, ArtifactStoreError, FilesystemArtifactStore};
use ring::digest::{Context, SHA256};
use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

#[test]
fn upload_resumes_after_reopen_and_finalizes_into_the_cas() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("uploads.sqlite3");
    let upload_root = directory.path().join("uploads");
    let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 1_024)?;
    let request = request(b"hello world");
    let upload_id = {
        let uploads = SqliteUploadStore::open(&database, &upload_root, 1_024, 8)?;
        let session = uploads.begin(&request)?;
        assert_eq!(
            uploads.append("worker-1", &session.upload_id, 0, b"hello ", 2)?,
            6
        );
        session.upload_id
    };

    let uploads = SqliteUploadStore::open(&database, &upload_root, 1_024, 8)?;
    assert_eq!(uploads.status("worker-1", &upload_id)?.committed_offset, 6);
    assert!(matches!(
        uploads.append("worker-1", &upload_id, 5, b"world", 3),
        Err(UploadError::OffsetConflict {
            expected: 6,
            received: 5
        })
    ));
    assert_eq!(uploads.append("worker-1", &upload_id, 6, b"world", 3)?, 11);
    let artifact = uploads.finalize("worker-1", &upload_id, &cas, 4)?;
    assert_eq!(artifact.digest, request.expected_digest);
    assert_eq!(artifact.size_bytes, 11);
    assert_eq!(
        uploads.status("worker-1", &upload_id)?.state,
        UploadState::Completed
    );
    assert!(uploads.owns_completed_artifact("worker-1", artifact.digest)?);
    assert!(!uploads.owns_completed_artifact("other-worker", artifact.digest)?);
    assert_eq!(uploads.finalize("worker-1", &upload_id, &cas, 5)?, artifact);
    let mut reader = cas.open(artifact.digest)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"hello world");
    Ok(())
}

#[test]
fn append_truncates_bytes_not_committed_before_a_simulated_crash() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("uploads.sqlite3");
    let upload_root = directory.path().join("uploads");
    let uploads = SqliteUploadStore::open(&database, &upload_root, 1_024, 8)?;
    let request = request(b"abcdef");
    let session = uploads.begin(&request)?;
    uploads.append("worker-1", &session.upload_id, 0, b"abc", 2)?;
    let path = uploads.data_path(&session.upload_id)?;
    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"uncommitted")?;
    drop(uploads);

    let uploads = SqliteUploadStore::open(&database, &upload_root, 1_024, 8)?;
    assert_eq!(
        uploads.append("worker-1", &session.upload_id, 3, b"def", 3)?,
        6
    );
    assert_eq!(fs::read(path)?, b"abcdef");
    Ok(())
}

#[test]
fn expired_sessions_are_pruned_with_their_partial_bytes() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("uploads"),
        1_024,
        8,
    )?;
    let session = uploads.begin(&request(b"partial"))?;
    uploads.append("worker-1", &session.upload_id, 0, b"part", 2)?;
    assert_eq!(uploads.prune_expired(100)?, 1);
    assert!(matches!(
        uploads.status("worker-1", &session.upload_id),
        Err(UploadError::NotFound(_))
    ));
    assert!(!uploads.data_path(&session.upload_id)?.exists());
    Ok(())
}

#[test]
fn quota_reservation_is_idempotent_and_survives_restart() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("uploads.sqlite3");
    let upload_root = directory.path().join("uploads");
    let limits = UploadQuotas {
        total_bytes: 5,
        per_owner_bytes: 5,
    };
    let first = quota_request("worker-1", "first", b"12345", 1, 100);
    let upload_id = {
        let uploads =
            SqliteUploadStore::open_with_quotas(&database, &upload_root, 100, 100, limits)?;
        let session = uploads.begin(&first)?;
        assert_eq!(uploads.begin(&first)?, session);
        session.upload_id
    };
    let uploads = SqliteUploadStore::open_with_quotas(&database, &upload_root, 100, 100, limits)?;
    assert_eq!(uploads.begin(&first)?.upload_id, upload_id);
    assert!(matches!(
        uploads.begin(&quota_request("worker-2", "second", b"x", 2, 100)),
        Err(UploadError::QuotaExceeded {
            scope: QuotaScope::Total,
            ..
        })
    ));
    Ok(())
}

#[test]
fn concurrent_begin_cannot_overcommit_total_quota() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = Arc::new(quota_store(directory.path(), 6, 6)?);
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (owner, bytes) in [
        ("worker-1", b"aaaa".as_slice()),
        ("worker-2", b"bbbb".as_slice()),
    ] {
        let uploads = Arc::clone(&uploads);
        let barrier = Arc::clone(&barrier);
        let request = quota_request(owner, "first", bytes, 1, 100);
        handles.push(thread::spawn(move || {
            barrier.wait();
            uploads.begin(&request)
        }));
    }
    let results = handles
        .into_iter()
        .map(|handle| handle.join().expect("quota thread must not panic"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(UploadError::QuotaExceeded {
                    scope: QuotaScope::Total,
                    ..
                })
            ))
            .count(),
        1
    );
    Ok(())
}

#[test]
fn pre_quota_schema_is_migrated_and_backfilled() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database_path = directory.path().join("uploads.sqlite3");
    let database = Connection::open(&database_path)?;
    database.execute_batch(
        "CREATE TABLE upload_sessions (
            upload_id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            upload_key TEXT NOT NULL,
            expected_digest TEXT NOT NULL,
            expected_size_bytes INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            committed_offset INTEGER NOT NULL,
            state INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            expires_at_ms INTEGER NOT NULL,
            artifact_digest TEXT,
            UNIQUE(owner_id, upload_key)
        );",
    )?;
    let completed_digest = digest(b"abc").to_string();
    let open_digest = digest(b"de").to_string();
    database.execute(
        "INSERT INTO upload_sessions VALUES
         ('completed', 'worker-1', 'completed', ?1, 3, 'text/plain', 3, 3, 1, 1, 100, ?1)",
        [&completed_digest],
    )?;
    database.execute(
        "INSERT INTO upload_sessions VALUES
         ('open', 'worker-2', 'open', ?1, 2, 'text/plain', 0, 1, 1, 1, 100, NULL)",
        [&open_digest],
    )?;
    drop(database);

    let uploads = SqliteUploadStore::open_with_quotas(
        &database_path,
        directory.path().join("uploads"),
        100,
        100,
        UploadQuotas {
            total_bytes: 5,
            per_owner_bytes: 5,
        },
    )?;
    assert!(uploads.owns_completed_artifact("worker-1", digest(b"abc"))?);
    assert!(matches!(
        uploads.begin(&quota_request("worker-3", "blocked", b"x", 2, 100)),
        Err(UploadError::QuotaExceeded {
            scope: QuotaScope::Total,
            ..
        })
    ));
    Ok(())
}

#[test]
fn garbage_collection_honors_readers_retention_and_releases_quota() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("uploads.sqlite3");
    let upload_root = directory.path().join("uploads");
    let cas_root = directory.path().join("cas");
    let uploads = SqliteUploadStore::open_with_quotas(
        &database,
        &upload_root,
        100,
        100,
        UploadQuotas {
            total_bytes: 5,
            per_owner_bytes: 5,
        },
    )?;
    let cas = FilesystemArtifactStore::open(&cas_root, 100)?;
    let (session, artifact) = complete_upload(&uploads, &cas, "worker-1", "source", b"12345")?;
    let hold = GrantArtifactReference {
        owner_id: "worker-2".into(),
        reference_key: "retention:release-audit".into(),
        digest: artifact.digest,
        kind: ArtifactReferenceKind::RetentionRoot,
        purpose: "minimum audit retention".into(),
        now_ms: 10,
        retained_until_ms: Some(50),
    };
    uploads.grant_reference(&hold)?;
    let reader = uploads.open_referenced_artifact("worker-1", artifact.digest, &cas)?;
    uploads.revoke_reference("worker-1", &format!("upload:{}", session.upload_id), 11)?;
    uploads.revoke_reference("worker-2", &hold.reference_key, 12)?;
    assert_eq!(
        uploads.collect_garbage(&cas, 20, 10)?,
        GarbageCollectionReport::default()
    );
    drop(reader);
    assert_eq!(
        uploads.collect_garbage(&cas, 50, 10)?,
        GarbageCollectionReport {
            collected_objects: 1,
            reclaimed_bytes: 5,
            skipped_active_readers: 0,
        }
    );
    assert!(!cas.contains(artifact.digest)?);
    uploads.begin(&quota_request("worker-3", "after-gc", b"abcde", 51, 100))?;

    drop(uploads);
    let reopened = SqliteUploadStore::open_with_quotas(
        &database,
        &upload_root,
        100,
        100,
        UploadQuotas {
            total_bytes: 5,
            per_owner_bytes: 5,
        },
    )?;
    assert!(!reopened.can_read_artifact("worker-1", artifact.digest)?);
    assert!(!cas.contains(artifact.digest)?);
    Ok(())
}

#[test]
fn garbage_collection_skips_an_active_reader_then_collects() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("uploads"),
        100,
        100,
    )?;
    let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 100)?;
    let (session, artifact) = complete_upload(&uploads, &cas, "worker-1", "source", b"data")?;
    let mut reader = uploads.open_referenced_artifact("worker-1", artifact.digest, &cas)?;
    uploads.revoke_reference("worker-1", &format!("upload:{}", session.upload_id), 10)?;
    assert_eq!(
        uploads.collect_garbage(&cas, 11, 10)?,
        GarbageCollectionReport {
            collected_objects: 0,
            reclaimed_bytes: 0,
            skipped_active_readers: 1,
        }
    );
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"data");
    drop(reader);
    assert_eq!(uploads.collect_garbage(&cas, 12, 10)?.collected_objects, 1);
    Ok(())
}

#[test]
fn garbage_collection_accepts_a_non_filesystem_retention_port() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("uploads"),
        100,
        100,
    )?;
    let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 100)?;
    let (session, artifact) = complete_upload(&uploads, &cas, "worker-1", "source", b"data")?;
    uploads.revoke_reference("worker-1", &format!("upload:{}", session.upload_id), 10)?;
    let retention = RecordingRetentionStore::default();

    assert_eq!(
        uploads
            .collect_garbage(&retention, 11, 10)?
            .collected_objects,
        1
    );
    assert_eq!(
        *retention.removed.lock().expect("retention fixture lock"),
        vec![artifact.digest]
    );
    Ok(())
}

#[test]
fn pending_gc_recovers_after_restart_without_resurrecting_metadata() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("uploads.sqlite3");
    let upload_root = directory.path().join("uploads");
    let cas_root = directory.path().join("cas");
    let cas = FilesystemArtifactStore::open(&cas_root, 100)?;
    let artifact = {
        let uploads = SqliteUploadStore::open(&database, &upload_root, 100, 100)?;
        let (session, artifact) = complete_upload(&uploads, &cas, "worker-1", "source", b"crash")?;
        uploads.revoke_reference("worker-1", &format!("upload:{}", session.upload_id), 10)?;
        uploads.connection()?.execute(
            "INSERT INTO artifact_gc_pending(digest, marked_at_ms) VALUES (?1, 11)",
            [artifact.digest.to_string()],
        )?;
        cas.remove_unreachable(artifact.digest)?;
        artifact
    };
    let uploads = SqliteUploadStore::open(&database, &upload_root, 100, 100)?;
    assert_eq!(uploads.collect_garbage(&cas, 12, 10)?.collected_objects, 1);
    drop(uploads);
    let reopened = SqliteUploadStore::open(&database, &upload_root, 100, 100)?;
    assert!(!reopened.can_read_artifact("worker-1", artifact.digest)?);
    assert!(!cas.contains(artifact.digest)?);
    Ok(())
}

fn complete_upload(
    uploads: &SqliteUploadStore,
    cas: &FilesystemArtifactStore,
    owner_id: &str,
    upload_key: &str,
    bytes: &[u8],
) -> Result<(UploadSession, ArtifactIdentity), UploadError> {
    let request = quota_request(owner_id, upload_key, bytes, 1, 100);
    let session = uploads.begin(&request)?;
    uploads.append(owner_id, &session.upload_id, 0, bytes, 2)?;
    let artifact = uploads.finalize(owner_id, &session.upload_id, cas, 3)?;
    Ok((session, artifact))
}

fn quota_store(
    directory: &Path,
    total_bytes: u64,
    per_owner_bytes: u64,
) -> Result<SqliteUploadStore, UploadError> {
    SqliteUploadStore::open_with_quotas(
        directory.join("uploads.sqlite3"),
        directory.join("uploads"),
        100,
        100,
        UploadQuotas {
            total_bytes,
            per_owner_bytes,
        },
    )
}

fn quota_request(
    owner_id: &str,
    upload_key: &str,
    bytes: &[u8],
    now_ms: u64,
    expires_at_ms: u64,
) -> BeginUpload {
    BeginUpload {
        owner_id: owner_id.to_owned(),
        upload_key: upload_key.to_owned(),
        expected_digest: digest(bytes),
        expected_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        media_type: "application/octet-stream".to_owned(),
        now_ms,
        expires_at_ms,
    }
}

fn request(bytes: &[u8]) -> BeginUpload {
    BeginUpload {
        owner_id: "worker-1".to_owned(),
        upload_key: "attempt-1:stdout".to_owned(),
        expected_digest: digest(bytes),
        expected_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        media_type: "text/plain".to_owned(),
        now_ms: 1,
        expires_at_ms: 100,
    }
}

fn digest(bytes: &[u8]) -> Sha256Digest {
    let mut context = Context::new(&SHA256);
    context.update(bytes);
    let digest = context.finish();
    let mut value = [0_u8; 32];
    value.copy_from_slice(digest.as_ref());
    Sha256Digest::from_bytes(value)
}

#[derive(Debug, Default)]
struct RecordingRetentionStore {
    removed: Mutex<Vec<Sha256Digest>>,
}

impl ArtifactRetentionStore for RecordingRetentionStore {
    fn remove_unreachable(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError> {
        self.removed
            .lock()
            .map_err(|_| ArtifactStoreError::Io {
                operation: "lock retention fixture",
                source: io::Error::other("retention fixture lock poisoned"),
            })?
            .push(digest);
        Ok(true)
    }
}
