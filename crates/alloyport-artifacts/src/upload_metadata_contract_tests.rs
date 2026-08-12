//! Reusable behavioral contract for Artifact upload-session and metadata ports.

use crate::upload::{
    ArtifactMetadataStore, ArtifactReference, ArtifactReferenceKind, ArtifactUploadRepository,
    BeginUpload, GarbageCollectionReport, GrantArtifactReference, QuotaScope, UploadError,
    UploadQuotas, UploadSession, UploadState,
};
use crate::{
    ArtifactIdentity, ArtifactReader, ArtifactRetentionStore, ArtifactStore, ArtifactStoreError,
    InMemoryArtifactStore, IngestRequest, IngestResult, Sha256Digest, SqliteUploadStore,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

const MAX_UPLOAD_BYTES: u64 = 64;
const MAX_CHUNK_BYTES: usize = 8;

trait UploadMetadataContractRepository: ArtifactUploadRepository + ArtifactMetadataStore {
    fn prune_expired(&self, now_ms: u64) -> Result<usize, UploadError>;
    fn reference(
        &self,
        owner_id: &str,
        reference_key: &str,
    ) -> Result<ArtifactReference, UploadError>;
    fn revoke_reference(
        &self,
        owner_id: &str,
        reference_key: &str,
        now_ms: u64,
    ) -> Result<ArtifactReference, UploadError>;
    fn collect_garbage(
        &self,
        artifacts: &dyn ArtifactRetentionStore,
        now_ms: u64,
        limit: usize,
    ) -> Result<GarbageCollectionReport, UploadError>;
}

impl UploadMetadataContractRepository for SqliteUploadStore {
    fn prune_expired(&self, now_ms: u64) -> Result<usize, UploadError> {
        Self::prune_expired(self, now_ms)
    }

    fn reference(
        &self,
        owner_id: &str,
        reference_key: &str,
    ) -> Result<ArtifactReference, UploadError> {
        Self::reference(self, owner_id, reference_key)
    }

    fn revoke_reference(
        &self,
        owner_id: &str,
        reference_key: &str,
        now_ms: u64,
    ) -> Result<ArtifactReference, UploadError> {
        Self::revoke_reference(self, owner_id, reference_key, now_ms)
    }

    fn collect_garbage(
        &self,
        artifacts: &dyn ArtifactRetentionStore,
        now_ms: u64,
        limit: usize,
    ) -> Result<GarbageCollectionReport, UploadError> {
        Self::collect_garbage(self, artifacts, now_ms, limit)
    }
}

#[test]
fn sqlite_upload_metadata_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let mut next = 0_u64;
    upload_metadata_port_contract(|quotas| {
        next = next.saturating_add(1);
        let root = directory.path().join(format!("case-{next}"));
        std::fs::create_dir_all(&root).map_err(|source| UploadError::Io {
            operation: "create SQLite contract directory",
            source,
        })?;
        Ok(Box::new(SqliteUploadStore::open_with_quotas(
            root.join("uploads.sqlite3"),
            root.join("staging"),
            MAX_UPLOAD_BYTES,
            MAX_CHUNK_BYTES,
            quotas,
        )?))
    })
}

#[test]
fn memory_upload_metadata_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    upload_metadata_port_contract(|quotas| {
        Ok(Box::new(MemoryUploadMetadataStore::new(
            MAX_UPLOAD_BYTES,
            MAX_CHUNK_BYTES,
            quotas,
        )))
    })
}

fn upload_metadata_port_contract(
    mut create: impl FnMut(
        UploadQuotas,
    ) -> Result<Box<dyn UploadMetadataContractRepository>, UploadError>,
) -> Result<(), Box<dyn Error>> {
    session_and_publication_contract(create(UploadQuotas::unbounded())?.as_ref())?;
    finalize_failure_contract(create(UploadQuotas::unbounded())?.as_ref())?;
    quota_contract(&mut create)?;
    reference_and_reachability_contract(create(UploadQuotas::unbounded())?.as_ref())?;
    Ok(())
}

fn session_and_publication_contract(
    repository: &dyn UploadMetadataContractRepository,
) -> Result<(), Box<dyn Error>> {
    let artifacts = InMemoryArtifactStore::new(MAX_UPLOAD_BYTES);
    let request = upload_request("worker-1", "attempt-1:stdout", b"hello world", 1, 100);
    let session = repository.begin(&request)?;
    assert_eq!(session.state, UploadState::Open);
    assert_eq!(session.committed_offset, 0);
    assert_eq!(repository.begin(&request)?, session);

    let mut conflicting = request.clone();
    conflicting.media_type = "application/conflict".into();
    assert!(matches!(
        repository.begin(&conflicting),
        Err(UploadError::ConflictingUploadKey)
    ));
    let other_owner = upload_request("worker-2", "attempt-1:stdout", b"other", 1, 100);
    assert_ne!(repository.begin(&other_owner)?.upload_id, session.upload_id);
    assert!(matches!(
        repository.status("worker-2", &session.upload_id),
        Err(UploadError::OwnerMismatch)
    ));
    assert!(
        repository
            .completed_upload_session_by_key("worker-1", &request.upload_key)?
            .is_none()
    );

    assert_eq!(
        repository.append("worker-1", &session.upload_id, 0, b"hello ", 2)?,
        6
    );
    assert!(matches!(
        repository.append("worker-1", &session.upload_id, 5, b"world", 3),
        Err(UploadError::OffsetConflict {
            expected: 6,
            received: 5
        })
    ));
    assert!(matches!(
        repository.finalize("worker-1", &session.upload_id, &artifacts, 3),
        Err(UploadError::Incomplete {
            expected: 11,
            committed: 6
        })
    ));
    assert_eq!(
        repository.append("worker-1", &session.upload_id, 6, b"world", 4)?,
        11
    );
    let artifact = repository.finalize("worker-1", &session.upload_id, &artifacts, 5)?;
    assert_eq!(
        artifact,
        ArtifactIdentity {
            digest: request.expected_digest,
            size_bytes: 11,
        }
    );
    assert_eq!(
        repository.finalize("worker-1", &session.upload_id, &artifacts, 6)?,
        artifact
    );
    let completed = repository
        .completed_upload_session_by_key("worker-1", &request.upload_key)?
        .expect("completed upload must be addressable by owner and key");
    assert_eq!(completed.state, UploadState::Completed);
    assert_eq!(completed.artifact, Some(artifact));
    assert_eq!(repository.artifact_size_bytes(artifact.digest)?, Some(11));
    assert!(repository.can_read_artifact("worker-1", artifact.digest)?);
    assert!(!repository.can_read_artifact("worker-2", artifact.digest)?);
    assert!(matches!(
        repository.open_referenced_artifact("worker-2", artifact.digest, &artifacts),
        Err(UploadError::OwnerMismatch)
    ));
    let mut reader =
        repository.open_referenced_artifact("worker-1", artifact.digest, &artifacts)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"hello world");

    let oversized_chunk = upload_request("worker-1", "oversized-chunk", b"123456789", 10, 100);
    let oversized_chunk = repository.begin(&oversized_chunk)?;
    assert!(matches!(
        repository.append("worker-1", &oversized_chunk.upload_id, 0, b"123456789", 11),
        Err(UploadError::ChunkTooLarge {
            limit: MAX_CHUNK_BYTES,
            received: 9
        })
    ));

    let zero = upload_request("worker-1", "attempt-1:stderr", b"", 10, 100);
    let zero_session = repository.begin(&zero)?;
    assert_eq!(
        repository.finalize("worker-1", &zero_session.upload_id, &artifacts, 11)?,
        ArtifactIdentity {
            digest: zero.expected_digest,
            size_bytes: 0,
        }
    );
    Ok(())
}

fn finalize_failure_contract(
    repository: &dyn UploadMetadataContractRepository,
) -> Result<(), Box<dyn Error>> {
    let transient = upload_request("worker-1", "transient", b"retry", 1, 100);
    let transient_session = repository.begin(&transient)?;
    repository.append("worker-1", &transient_session.upload_id, 0, b"retry", 2)?;
    let flaky = FlakyArtifactStore {
        inner: InMemoryArtifactStore::new(MAX_UPLOAD_BYTES),
        fail_next: AtomicBool::new(true),
    };
    assert!(matches!(
        repository.finalize("worker-1", &transient_session.upload_id, &flaky, 3),
        Err(UploadError::Artifact(ArtifactStoreError::Io { .. }))
    ));
    assert_eq!(
        repository
            .status("worker-1", &transient_session.upload_id)?
            .state,
        UploadState::Finalizing
    );
    assert_eq!(
        repository.finalize("worker-1", &transient_session.upload_id, &flaky, 4)?,
        ArtifactIdentity {
            digest: transient.expected_digest,
            size_bytes: 5,
        }
    );

    let artifacts = InMemoryArtifactStore::new(MAX_UPLOAD_BYTES);
    let mut invalid = upload_request("worker-1", "terminal", b"right", 10, 100);
    invalid.expected_digest = Sha256Digest::digest_bytes(b"wrong");
    let invalid_session = repository.begin(&invalid)?;
    repository.append("worker-1", &invalid_session.upload_id, 0, b"right", 11)?;
    assert!(matches!(
        repository.finalize("worker-1", &invalid_session.upload_id, &artifacts, 12),
        Err(UploadError::Artifact(
            ArtifactStoreError::DigestMismatch { .. }
        ))
    ));
    assert_eq!(
        repository
            .status("worker-1", &invalid_session.upload_id)?
            .state,
        UploadState::Failed
    );
    assert!(
        repository
            .completed_upload_session_by_key("worker-1", "terminal")?
            .is_none()
    );
    assert!(!repository.can_read_artifact("worker-1", invalid.expected_digest)?);
    Ok(())
}

fn quota_contract(
    create: &mut impl FnMut(
        UploadQuotas,
    ) -> Result<Box<dyn UploadMetadataContractRepository>, UploadError>,
) -> Result<(), Box<dyn Error>> {
    let quotas = UploadQuotas {
        total_bytes: 10,
        per_owner_bytes: 6,
    };
    let repository = create(quotas)?;
    repository.begin(&upload_request("worker-1", "first", b"123456", 1, 100))?;
    assert!(matches!(
        repository.begin(&upload_request("worker-1", "owner-full", b"x", 2, 100)),
        Err(UploadError::QuotaExceeded {
            scope: QuotaScope::Owner,
            ..
        })
    ));
    repository.begin(&upload_request("worker-2", "other", b"abcd", 2, 100))?;
    assert!(matches!(
        repository.begin(&upload_request("worker-3", "total-full", b"x", 2, 100)),
        Err(UploadError::QuotaExceeded {
            scope: QuotaScope::Total,
            ..
        })
    ));

    let expiring = create(UploadQuotas {
        total_bytes: 5,
        per_owner_bytes: 5,
    })?;
    let expired = expiring.begin(&upload_request("worker-1", "expired", b"12345", 1, 10))?;
    expiring.begin(&upload_request(
        "worker-2",
        "after-expiry",
        b"abcde",
        10,
        100,
    ))?;
    assert_eq!(expiring.prune_expired(10)?, 1);
    assert!(matches!(
        expiring.status("worker-1", &expired.upload_id),
        Err(UploadError::NotFound(_))
    ));

    let terminal = create(UploadQuotas {
        total_bytes: 5,
        per_owner_bytes: 5,
    })?;
    let artifacts = InMemoryArtifactStore::new(MAX_UPLOAD_BYTES);
    let mut invalid = upload_request("worker-1", "invalid", b"right", 1, 100);
    invalid.expected_digest = Sha256Digest::digest_bytes(b"wrong");
    let session = terminal.begin(&invalid)?;
    terminal.append("worker-1", &session.upload_id, 0, b"right", 2)?;
    assert!(
        terminal
            .finalize("worker-1", &session.upload_id, &artifacts, 3)
            .is_err()
    );
    terminal.begin(&upload_request(
        "worker-2",
        "after-failure",
        b"12345",
        4,
        100,
    ))?;
    Ok(())
}

fn reference_and_reachability_contract(
    repository: &dyn UploadMetadataContractRepository,
) -> Result<(), Box<dyn Error>> {
    let artifacts = InMemoryArtifactStore::new(MAX_UPLOAD_BYTES);
    let request = upload_request("worker-1", "source", b"data", 1, 100);
    let session = repository.begin(&request)?;
    repository.append("worker-1", &session.upload_id, 0, b"data", 2)?;
    let artifact = repository.finalize("worker-1", &session.upload_id, &artifacts, 3)?;
    let grant = GrantArtifactReference {
        owner_id: "worker-2".into(),
        reference_key: "assignment:attempt-1:input".into(),
        digest: artifact.digest,
        kind: ArtifactReferenceKind::AssignmentInput,
        purpose: "attempt input bundle".into(),
        now_ms: 10,
        retained_until_ms: None,
    };
    let reference = repository.grant_reference(&grant)?;
    assert_eq!(repository.grant_reference(&grant)?, reference);
    assert_eq!(
        repository.reference("worker-2", &grant.reference_key)?,
        reference
    );
    assert!(repository.can_read_artifact("worker-2", artifact.digest)?);

    let mut conflicting = grant.clone();
    conflicting.purpose = "different purpose".into();
    assert!(matches!(
        repository.grant_reference(&conflicting),
        Err(UploadError::ConflictingReferenceKey)
    ));
    let missing = GrantArtifactReference {
        reference_key: "assignment:missing".into(),
        digest: Sha256Digest::digest_bytes(b"missing"),
        ..grant.clone()
    };
    assert!(matches!(
        repository.grant_reference(&missing),
        Err(UploadError::NotFound(_))
    ));

    let second = GrantArtifactReference {
        reference_key: "receipt:attempt-1".into(),
        kind: ArtifactReferenceKind::Receipt,
        purpose: "attempt receipt evidence".into(),
        ..grant
    };
    repository.grant_reference(&second)?;
    repository.revoke_reference("worker-2", "assignment:attempt-1:input", 20)?;
    assert!(repository.can_read_artifact("worker-2", artifact.digest)?);
    let revoked = repository.revoke_reference("worker-2", &second.reference_key, 21)?;
    assert_eq!(
        repository.revoke_reference("worker-2", &second.reference_key, 22)?,
        revoked
    );
    assert!(!repository.can_read_artifact("worker-2", artifact.digest)?);
    assert!(matches!(
        repository.grant_reference(&second),
        Err(UploadError::ReferenceRevoked)
    ));

    let hold = GrantArtifactReference {
        owner_id: "worker-3".into(),
        reference_key: "retention:audit".into(),
        digest: artifact.digest,
        kind: ArtifactReferenceKind::RetentionRoot,
        purpose: "minimum audit retention".into(),
        now_ms: 23,
        retained_until_ms: Some(50),
    };
    repository.grant_reference(&hold)?;
    repository.revoke_reference("worker-1", &format!("upload:{}", session.upload_id), 24)?;
    repository.revoke_reference("worker-3", &hold.reference_key, 25)?;
    assert_eq!(
        repository.collect_garbage(&artifacts, 49, 10)?,
        GarbageCollectionReport::default()
    );
    assert_eq!(
        repository.collect_garbage(&artifacts, 50, 10)?,
        GarbageCollectionReport {
            collected_objects: 1,
            reclaimed_bytes: 4,
            skipped_active_readers: 0,
        }
    );
    assert_eq!(repository.artifact_size_bytes(artifact.digest)?, None);
    assert!(!artifacts.contains(artifact.digest)?);
    Ok(())
}

fn upload_request(
    owner_id: &str,
    upload_key: &str,
    bytes: &[u8],
    now_ms: u64,
    expires_at_ms: u64,
) -> BeginUpload {
    BeginUpload {
        owner_id: owner_id.into(),
        upload_key: upload_key.into(),
        expected_digest: Sha256Digest::digest_bytes(bytes),
        expected_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        media_type: "application/octet-stream".into(),
        now_ms,
        expires_at_ms,
    }
}

#[derive(Debug)]
struct FlakyArtifactStore {
    inner: InMemoryArtifactStore,
    fail_next: AtomicBool,
}

impl ArtifactStore for FlakyArtifactStore {
    fn ingest(
        &self,
        source: &mut dyn Read,
        request: IngestRequest,
    ) -> Result<IngestResult, ArtifactStoreError> {
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(ArtifactStoreError::Io {
                operation: "fixture transient failure",
                source: io::Error::other("fixture"),
            });
        }
        self.inner.ingest(source, request)
    }

    fn open(&self, digest: Sha256Digest) -> Result<ArtifactReader, ArtifactStoreError> {
        self.inner.open(digest)
    }

    fn contains(&self, digest: Sha256Digest) -> Result<bool, ArtifactStoreError> {
        self.inner.contains(digest)
    }
}

struct MemoryUploadMetadataStore {
    state: Mutex<MemoryState>,
    max_upload_bytes: u64,
    max_chunk_bytes: usize,
    quotas: UploadQuotas,
}

#[derive(Default)]
struct MemoryState {
    next_id: u64,
    sessions: BTreeMap<String, MemorySession>,
    session_keys: BTreeMap<(String, String), String>,
    objects: BTreeMap<Sha256Digest, u64>,
    references: BTreeMap<(String, String), ArtifactReference>,
}

struct MemorySession {
    value: UploadSession,
    bytes: Vec<u8>,
    quota_reserved_bytes: u64,
}

impl MemoryUploadMetadataStore {
    fn new(max_upload_bytes: u64, max_chunk_bytes: usize, quotas: UploadQuotas) -> Self {
        Self {
            state: Mutex::new(MemoryState::default()),
            max_upload_bytes,
            max_chunk_bytes,
            quotas,
        }
    }

    fn state(&self) -> Result<MutexGuard<'_, MemoryState>, UploadError> {
        self.state
            .lock()
            .map_err(|_| UploadError::Corrupt("memory upload fixture lock poisoned".into()))
    }
}

impl Debug for MemoryUploadMetadataStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryUploadMetadataStore")
            .field("max_upload_bytes", &self.max_upload_bytes)
            .field("max_chunk_bytes", &self.max_chunk_bytes)
            .field("quotas", &self.quotas)
            .finish_non_exhaustive()
    }
}

impl ArtifactUploadRepository for MemoryUploadMetadataStore {
    fn begin(&self, request: &BeginUpload) -> Result<UploadSession, UploadError> {
        validate_begin(request, self.max_upload_bytes)?;
        let mut state = self.state()?;
        let key = (request.owner_id.clone(), request.upload_key.clone());
        if let Some(id) = state.session_keys.get(&key) {
            let existing = &state.sessions[id].value;
            if existing.expected_digest == request.expected_digest
                && existing.expected_size_bytes == request.expected_size_bytes
                && existing.media_type == request.media_type
            {
                return Ok(existing.clone());
            }
            return Err(UploadError::ConflictingUploadKey);
        }
        reserve_memory_quota(&state, request, self.quotas)?;
        state.next_id = state.next_id.saturating_add(1);
        let upload_id = format!("memory-upload-{}", state.next_id);
        let session = UploadSession {
            upload_id: upload_id.clone(),
            owner_id: request.owner_id.clone(),
            upload_key: request.upload_key.clone(),
            expected_digest: request.expected_digest,
            expected_size_bytes: request.expected_size_bytes,
            media_type: request.media_type.clone(),
            committed_offset: 0,
            state: UploadState::Open,
            expires_at_ms: request.expires_at_ms,
            artifact: None,
        };
        state.session_keys.insert(key, upload_id.clone());
        state.sessions.insert(
            upload_id,
            MemorySession {
                value: session.clone(),
                bytes: Vec::new(),
                quota_reserved_bytes: request.expected_size_bytes,
            },
        );
        Ok(session)
    }

    fn status(&self, owner_id: &str, upload_id: &str) -> Result<UploadSession, UploadError> {
        let state = self.state()?;
        let session = state
            .sessions
            .get(upload_id)
            .ok_or_else(|| UploadError::NotFound(upload_id.into()))?;
        authorize(&session.value, owner_id)?;
        Ok(session.value.clone())
    }

    fn append(
        &self,
        owner_id: &str,
        upload_id: &str,
        offset: u64,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<u64, UploadError> {
        if bytes.len() > self.max_chunk_bytes {
            return Err(UploadError::ChunkTooLarge {
                limit: self.max_chunk_bytes,
                received: bytes.len(),
            });
        }
        let mut state = self.state()?;
        let session = state
            .sessions
            .get_mut(upload_id)
            .ok_or_else(|| UploadError::NotFound(upload_id.into()))?;
        authorize(&session.value, owner_id)?;
        ensure_open(&session.value, now_ms)?;
        if offset != session.value.committed_offset {
            return Err(UploadError::OffsetConflict {
                expected: session.value.committed_offset,
                received: offset,
            });
        }
        let next = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if next > session.value.expected_size_bytes || next > self.max_upload_bytes {
            return Err(UploadError::SizeLimitExceeded {
                limit: session.value.expected_size_bytes.min(self.max_upload_bytes),
                attempted: next,
            });
        }
        session.bytes.extend_from_slice(bytes);
        session.value.committed_offset = next;
        Ok(next)
    }

    fn finalize(
        &self,
        owner_id: &str,
        upload_id: &str,
        artifacts: &dyn ArtifactStore,
        now_ms: u64,
    ) -> Result<ArtifactIdentity, UploadError> {
        let (session, bytes) = {
            let mut state = self.state()?;
            let session = state
                .sessions
                .get_mut(upload_id)
                .ok_or_else(|| UploadError::NotFound(upload_id.into()))?;
            authorize(&session.value, owner_id)?;
            if session.value.state == UploadState::Completed {
                return session.value.artifact.ok_or_else(|| {
                    UploadError::Corrupt("completed memory upload lacks artifact".into())
                });
            }
            if !matches!(
                session.value.state,
                UploadState::Open | UploadState::Finalizing
            ) {
                return Err(UploadError::InvalidState(session.value.state));
            }
            if now_ms >= session.value.expires_at_ms {
                return Err(UploadError::Expired);
            }
            if session.value.committed_offset != session.value.expected_size_bytes {
                return Err(UploadError::Incomplete {
                    expected: session.value.expected_size_bytes,
                    committed: session.value.committed_offset,
                });
            }
            session.value.state = UploadState::Finalizing;
            (session.value.clone(), session.bytes.clone())
        };
        let artifact = match artifacts.ingest(
            &mut Cursor::new(bytes),
            IngestRequest {
                expected_digest: Some(session.expected_digest),
                expected_size_bytes: Some(session.expected_size_bytes),
            },
        ) {
            Ok(result) => result.artifact,
            Err(error) => {
                if is_terminal_artifact_error(&error) {
                    let mut state = self.state()?;
                    let session = state.sessions.get_mut(upload_id).ok_or_else(|| {
                        UploadError::Corrupt("finalizing memory upload disappeared".into())
                    })?;
                    session.value.state = UploadState::Failed;
                    session.quota_reserved_bytes = 0;
                }
                return Err(error.into());
            }
        };
        let mut state = self.state()?;
        state
            .objects
            .entry(artifact.digest)
            .or_insert(artifact.size_bytes);
        let upload_reference = ArtifactReference {
            owner_id: session.owner_id.clone(),
            reference_key: format!("upload:{upload_id}"),
            digest: artifact.digest,
            kind: ArtifactReferenceKind::Upload,
            purpose: "completed upload".into(),
            created_at_ms: now_ms,
            retained_until_ms: None,
            revoked_at_ms: None,
        };
        state.references.insert(
            (
                upload_reference.owner_id.clone(),
                upload_reference.reference_key.clone(),
            ),
            upload_reference,
        );
        let stored = state
            .sessions
            .get_mut(upload_id)
            .ok_or_else(|| UploadError::Corrupt("completed memory upload disappeared".into()))?;
        stored.value.state = UploadState::Completed;
        stored.value.artifact = Some(artifact);
        stored.quota_reserved_bytes = 0;
        Ok(artifact)
    }

    fn open_referenced_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
        artifacts: &dyn ArtifactStore,
    ) -> Result<ArtifactReader, UploadError> {
        if !self.can_read_artifact(owner_id, digest)? {
            return Err(UploadError::OwnerMismatch);
        }
        Ok(artifacts.open(digest)?)
    }
}

impl ArtifactMetadataStore for MemoryUploadMetadataStore {
    fn completed_upload_session_by_key(
        &self,
        owner_id: &str,
        upload_key: &str,
    ) -> Result<Option<UploadSession>, UploadError> {
        let state = self.state()?;
        let Some(id) = state
            .session_keys
            .get(&(owner_id.into(), upload_key.into()))
        else {
            return Ok(None);
        };
        Ok(state
            .sessions
            .get(id)
            .map(|session| session.value.clone())
            .filter(|session| session.state == UploadState::Completed))
    }

    fn can_read_artifact(&self, owner_id: &str, digest: Sha256Digest) -> Result<bool, UploadError> {
        let state = self.state()?;
        Ok(state.references.values().any(|reference| {
            reference.owner_id == owner_id
                && reference.digest == digest
                && reference.revoked_at_ms.is_none()
        }))
    }

    fn artifact_size_bytes(&self, digest: Sha256Digest) -> Result<Option<u64>, UploadError> {
        Ok(self.state()?.objects.get(&digest).copied())
    }

    fn grant_reference(
        &self,
        request: &GrantArtifactReference,
    ) -> Result<ArtifactReference, UploadError> {
        validate_reference_grant(request)?;
        let mut state = self.state()?;
        let key = (request.owner_id.clone(), request.reference_key.clone());
        if let Some(existing) = state.references.get(&key) {
            if reference_matches_grant(existing, request) {
                return Ok(existing.clone());
            }
            return Err(if existing.revoked_at_ms.is_some() {
                UploadError::ReferenceRevoked
            } else {
                UploadError::ConflictingReferenceKey
            });
        }
        let size = state
            .objects
            .get(&request.digest)
            .copied()
            .ok_or_else(|| UploadError::NotFound(request.digest.to_string()))?;
        if !owner_has_active_digest(&state, &request.owner_id, request.digest) {
            let used = owner_stored_bytes(&state, &request.owner_id).saturating_add(
                owner_reserved_bytes(&state, &request.owner_id, request.now_ms),
            );
            enforce_quota(QuotaScope::Owner, self.quotas.per_owner_bytes, used, size)?;
        }
        let reference = ArtifactReference {
            owner_id: request.owner_id.clone(),
            reference_key: request.reference_key.clone(),
            digest: request.digest,
            kind: request.kind,
            purpose: request.purpose.clone(),
            created_at_ms: request.now_ms,
            retained_until_ms: request.retained_until_ms,
            revoked_at_ms: None,
        };
        state.references.insert(key, reference.clone());
        Ok(reference)
    }
}

impl UploadMetadataContractRepository for MemoryUploadMetadataStore {
    fn prune_expired(&self, now_ms: u64) -> Result<usize, UploadError> {
        let mut state = self.state()?;
        let expired = state
            .sessions
            .iter()
            .filter(|(_, session)| {
                session.value.state != UploadState::Completed
                    && session.value.expires_at_ms <= now_ms
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &expired {
            if let Some(session) = state.sessions.remove(id) {
                state
                    .session_keys
                    .remove(&(session.value.owner_id, session.value.upload_key));
            }
        }
        Ok(expired.len())
    }

    fn reference(
        &self,
        owner_id: &str,
        reference_key: &str,
    ) -> Result<ArtifactReference, UploadError> {
        self.state()?
            .references
            .get(&(owner_id.into(), reference_key.into()))
            .cloned()
            .ok_or_else(|| UploadError::NotFound(reference_key.into()))
    }

    fn revoke_reference(
        &self,
        owner_id: &str,
        reference_key: &str,
        now_ms: u64,
    ) -> Result<ArtifactReference, UploadError> {
        validate_reference_identity(owner_id, reference_key)?;
        let mut state = self.state()?;
        let reference = state
            .references
            .get_mut(&(owner_id.into(), reference_key.into()))
            .ok_or_else(|| UploadError::NotFound(reference_key.into()))?;
        if reference.revoked_at_ms.is_none() {
            reference.revoked_at_ms = Some(now_ms);
        }
        Ok(reference.clone())
    }

    fn collect_garbage(
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
        let mut state = self.state()?;
        let candidates = state
            .objects
            .iter()
            .filter(|(digest, _)| !artifact_is_reachable(&state, **digest, now_ms))
            .take(limit)
            .map(|(digest, size)| (*digest, *size))
            .collect::<Vec<_>>();
        let mut report = GarbageCollectionReport::default();
        for (digest, size) in candidates {
            artifacts.remove_unreachable(digest)?;
            state.objects.remove(&digest);
            report.collected_objects = report.collected_objects.saturating_add(1);
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(size);
        }
        Ok(report)
    }
}

fn validate_begin(request: &BeginUpload, max: u64) -> Result<(), UploadError> {
    if request.owner_id.trim().is_empty() {
        return Err(UploadError::InvalidRequest("owner is missing"));
    }
    if request.upload_key.trim().is_empty() {
        return Err(UploadError::InvalidRequest("upload key is missing"));
    }
    if request.media_type.trim().is_empty() {
        return Err(UploadError::InvalidRequest("media type is missing"));
    }
    if request.expires_at_ms <= request.now_ms {
        return Err(UploadError::InvalidRequest("expiry must be in the future"));
    }
    if request.expected_size_bytes > max {
        return Err(UploadError::SizeLimitExceeded {
            limit: max,
            attempted: request.expected_size_bytes,
        });
    }
    Ok(())
}

fn authorize(session: &UploadSession, owner_id: &str) -> Result<(), UploadError> {
    if session.owner_id == owner_id {
        Ok(())
    } else {
        Err(UploadError::OwnerMismatch)
    }
}

fn ensure_open(session: &UploadSession, now_ms: u64) -> Result<(), UploadError> {
    if session.state != UploadState::Open {
        return Err(UploadError::InvalidState(session.state));
    }
    if now_ms >= session.expires_at_ms {
        return Err(UploadError::Expired);
    }
    Ok(())
}

fn is_terminal_artifact_error(error: &ArtifactStoreError) -> bool {
    matches!(
        error,
        ArtifactStoreError::SizeLimitExceeded { .. }
            | ArtifactStoreError::SizeMismatch { .. }
            | ArtifactStoreError::DigestMismatch { .. }
            | ArtifactStoreError::IntegrityViolation { .. }
    )
}

fn validate_reference_identity(owner_id: &str, reference_key: &str) -> Result<(), UploadError> {
    if owner_id.trim().is_empty() {
        return Err(UploadError::InvalidRequest("reference owner is missing"));
    }
    if reference_key.trim().is_empty() {
        return Err(UploadError::InvalidRequest("reference key is missing"));
    }
    Ok(())
}

fn validate_reference_grant(request: &GrantArtifactReference) -> Result<(), UploadError> {
    validate_reference_identity(&request.owner_id, &request.reference_key)?;
    if request.kind == ArtifactReferenceKind::Upload {
        return Err(UploadError::InvalidRequest(
            "upload references are created only by finalization",
        ));
    }
    if request.purpose.trim().is_empty() {
        return Err(UploadError::InvalidRequest("reference purpose is missing"));
    }
    if request
        .retained_until_ms
        .is_some_and(|until| until <= request.now_ms)
    {
        return Err(UploadError::InvalidRequest(
            "reference retention must end in the future",
        ));
    }
    Ok(())
}

fn reference_matches_grant(
    reference: &ArtifactReference,
    request: &GrantArtifactReference,
) -> bool {
    reference.digest == request.digest
        && reference.kind == request.kind
        && reference.purpose == request.purpose
        && reference.retained_until_ms == request.retained_until_ms
        && reference.revoked_at_ms.is_none()
}

fn reserve_memory_quota(
    state: &MemoryState,
    request: &BeginUpload,
    quotas: UploadQuotas,
) -> Result<(), UploadError> {
    let total_stored = state.objects.values().copied().sum::<u64>();
    let total_reserved = state
        .sessions
        .values()
        .filter(|session| {
            matches!(
                session.value.state,
                UploadState::Open | UploadState::Finalizing
            ) && session.value.expires_at_ms > request.now_ms
        })
        .map(|session| session.quota_reserved_bytes)
        .sum::<u64>();
    enforce_quota(
        QuotaScope::Total,
        quotas.total_bytes,
        total_stored.saturating_add(total_reserved),
        request.expected_size_bytes,
    )?;
    let owner_used = owner_stored_bytes(state, &request.owner_id).saturating_add(
        owner_reserved_bytes(state, &request.owner_id, request.now_ms),
    );
    enforce_quota(
        QuotaScope::Owner,
        quotas.per_owner_bytes,
        owner_used,
        request.expected_size_bytes,
    )
}

fn enforce_quota(
    scope: QuotaScope,
    limit: u64,
    used: u64,
    requested: u64,
) -> Result<(), UploadError> {
    if used.saturating_add(requested) > limit {
        Err(UploadError::QuotaExceeded {
            scope,
            limit,
            used,
            requested,
        })
    } else {
        Ok(())
    }
}

fn owner_stored_bytes(state: &MemoryState, owner_id: &str) -> u64 {
    state
        .references
        .values()
        .filter(|reference| reference.owner_id == owner_id && reference.revoked_at_ms.is_none())
        .map(|reference| reference.digest)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|digest| state.objects.get(&digest))
        .copied()
        .sum()
}

fn owner_reserved_bytes(state: &MemoryState, owner_id: &str, now_ms: u64) -> u64 {
    state
        .sessions
        .values()
        .filter(|session| {
            session.value.owner_id == owner_id
                && matches!(
                    session.value.state,
                    UploadState::Open | UploadState::Finalizing
                )
                && session.value.expires_at_ms > now_ms
        })
        .map(|session| session.quota_reserved_bytes)
        .sum()
}

fn owner_has_active_digest(state: &MemoryState, owner_id: &str, digest: Sha256Digest) -> bool {
    state.references.values().any(|reference| {
        reference.owner_id == owner_id
            && reference.digest == digest
            && reference.revoked_at_ms.is_none()
    })
}

fn artifact_is_reachable(state: &MemoryState, digest: Sha256Digest, now_ms: u64) -> bool {
    state.references.values().any(|reference| {
        reference.digest == digest
            && (reference.revoked_at_ms.is_none()
                || reference
                    .retained_until_ms
                    .is_some_and(|until| until > now_ms))
    }) || state.sessions.values().any(|session| {
        session.value.expected_digest == digest
            && matches!(
                session.value.state,
                UploadState::Open | UploadState::Finalizing
            )
            && session.value.expires_at_ms > now_ms
    })
}
