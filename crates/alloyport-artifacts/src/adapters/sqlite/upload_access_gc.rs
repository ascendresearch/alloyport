//! Authorized Artifact reads, reader leases, and SQLite-coordinated garbage collection.

use super::upload_gc::{artifact_is_reachable, pending_garbage, stage_garbage_candidates};
use super::upload_store::SqliteUploadStore;
use crate::upload::{GarbageCollectionReport, UploadError};
use crate::{ArtifactReader, ArtifactRetentionStore, ArtifactStore, Sha256Digest};
use rusqlite::TransactionBehavior;
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

struct LeasedArtifactReader {
    reader: ArtifactReader,
    digest: Sha256Digest,
    active_readers: Arc<Mutex<BTreeMap<Sha256Digest, u64>>>,
}

impl Read for LeasedArtifactReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buffer)
    }
}

impl Drop for LeasedArtifactReader {
    fn drop(&mut self) {
        let Ok(mut readers) = self.active_readers.lock() else {
            return;
        };
        let Some(count) = readers.get_mut(&self.digest) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            readers.remove(&self.digest);
        }
    }
}

#[allow(clippy::missing_errors_doc)]
impl SqliteUploadStore {
    /// Opens an Artifact while holding an in-process reader lease against garbage collection.
    pub fn open_referenced_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
        artifacts: &dyn ArtifactStore,
    ) -> Result<ArtifactReader, UploadError> {
        let mut readers = self
            .active_readers
            .lock()
            .map_err(|_| UploadError::Corrupt("artifact reader lock poisoned".into()))?;
        if !self.can_read_artifact(owner_id, digest)? {
            return Err(UploadError::OwnerMismatch);
        }
        let reader = artifacts.open(digest)?;
        let count = readers.entry(digest).or_default();
        *count = count.saturating_add(1);
        let identity = reader.identity();
        Ok(ArtifactReader::new(
            identity,
            LeasedArtifactReader {
                reader,
                digest,
                active_readers: Arc::clone(&self.active_readers),
            },
        ))
    }

    pub fn collect_garbage(
        &self,
        artifacts: &dyn ArtifactRetentionStore,
        now_ms: u64,
        limit: usize,
    ) -> Result<GarbageCollectionReport, UploadError> {
        if limit == 0 {
            return Err(UploadError::InvalidRequest(
                "garbage collection limit must be positive",
            ));
        }
        let _artifact_guard = self.artifact_guard()?;
        let readers = self
            .active_readers
            .lock()
            .map_err(|_| UploadError::Corrupt("artifact reader lock poisoned".into()))?;
        let mut database = self.connection()?;
        stage_garbage_candidates(&mut database, now_ms, limit)?;
        let candidates = pending_garbage(&database, limit)?;
        let mut report = GarbageCollectionReport::default();
        for (digest, size_bytes) in candidates {
            if readers.get(&digest).copied().unwrap_or_default() != 0 {
                database.execute(
                    "DELETE FROM artifact_gc_pending WHERE digest = ?1",
                    [digest.to_string()],
                )?;
                report.skipped_active_readers = report.skipped_active_readers.saturating_add(1);
                continue;
            }
            let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if artifact_is_reachable(&transaction, digest, now_ms)? {
                transaction.execute(
                    "DELETE FROM artifact_gc_pending WHERE digest = ?1",
                    [digest.to_string()],
                )?;
                transaction.commit()?;
                continue;
            }
            artifacts.remove_unreachable(digest)?;
            transaction.execute(
                "DELETE FROM artifact_owner_references WHERE digest = ?1",
                [digest.to_string()],
            )?;
            transaction.execute(
                "DELETE FROM artifact_objects WHERE digest = ?1",
                [digest.to_string()],
            )?;
            transaction.execute(
                "DELETE FROM artifact_gc_pending WHERE digest = ?1",
                [digest.to_string()],
            )?;
            transaction.commit()?;
            report.collected_objects = report.collected_objects.saturating_add(1);
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(size_bytes);
        }
        Ok(report)
    }
}
