//! Durable resumable-upload sessions layered over an immutable artifact store.

use crate::{ArtifactIdentity, ArtifactStore, ArtifactStoreError, IngestRequest, Sha256Digest};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS upload_sessions (
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
    quota_reserved_bytes INTEGER NOT NULL DEFAULT 0,
    UNIQUE(owner_id, upload_key)
);
CREATE INDEX IF NOT EXISTS upload_sessions_expiry
    ON upload_sessions(state, expires_at_ms);
CREATE TABLE IF NOT EXISTS artifact_objects (
    digest TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS artifact_owner_references (
    owner_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    PRIMARY KEY(owner_id, digest),
    FOREIGN KEY(digest) REFERENCES artifact_objects(digest)
);
COMMIT;
";

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

impl UploadState {
    fn from_i64(value: i64) -> Result<Self, UploadError> {
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

#[derive(Debug)]
pub enum UploadError {
    Sqlite(rusqlite::Error),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    Artifact(ArtifactStoreError),
    NotFound(String),
    OwnerMismatch,
    ConflictingUploadKey,
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
            Self::Sqlite(error) => Display::fmt(error, formatter),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::Artifact(error) => Display::fmt(error, formatter),
            Self::NotFound(id) => write!(formatter, "upload session {id} was not found"),
            Self::OwnerMismatch => write!(formatter, "upload session owner does not match"),
            Self::ConflictingUploadKey => {
                write!(formatter, "upload key was reused with other metadata")
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
            Self::Sqlite(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::Artifact(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for UploadError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<ArtifactStoreError> for UploadError {
    fn from(error: ArtifactStoreError) -> Self {
        Self::Artifact(error)
    }
}

pub struct SqliteUploadStore {
    connection: Mutex<Connection>,
    finalize_lock: Mutex<()>,
    upload_root: PathBuf,
    max_upload_bytes: u64,
    max_chunk_bytes: usize,
    quotas: UploadQuotas,
    counter: AtomicU64,
}

impl Debug for SqliteUploadStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteUploadStore")
            .field("upload_root", &self.upload_root)
            .field("max_upload_bytes", &self.max_upload_bytes)
            .field("max_chunk_bytes", &self.max_chunk_bytes)
            .field("quotas", &self.quotas)
            .finish_non_exhaustive()
    }
}

#[allow(clippy::missing_errors_doc)]
impl SqliteUploadStore {
    pub fn open(
        database: impl AsRef<Path>,
        upload_root: impl AsRef<Path>,
        max_upload_bytes: u64,
        max_chunk_bytes: usize,
    ) -> Result<Self, UploadError> {
        Self::open_with_quotas(
            database,
            upload_root,
            max_upload_bytes,
            max_chunk_bytes,
            UploadQuotas::unbounded(),
        )
    }

    pub fn open_with_quotas(
        database: impl AsRef<Path>,
        upload_root: impl AsRef<Path>,
        max_upload_bytes: u64,
        max_chunk_bytes: usize,
        quotas: UploadQuotas,
    ) -> Result<Self, UploadError> {
        if max_chunk_bytes == 0 {
            return Err(UploadError::InvalidRequest("chunk limit must be positive"));
        }
        validate_quotas(quotas)?;
        fs::create_dir_all(upload_root.as_ref()).map_err(|source| UploadError::Io {
            operation: "create upload data directory",
            source,
        })?;
        let mut connection = Connection::open(database)?;
        connection.execute_batch(SCHEMA)?;
        migrate_quota_schema(&mut connection)?;
        let store = Self {
            connection: Mutex::new(connection),
            finalize_lock: Mutex::new(()),
            upload_root: upload_root.as_ref().to_path_buf(),
            max_upload_bytes,
            max_chunk_bytes,
            quotas,
            counter: AtomicU64::new(unique_seed()),
        };
        store.cleanup_completed_data()?;
        Ok(store)
    }

    pub fn begin(&self, request: &BeginUpload) -> Result<UploadSession, UploadError> {
        validate_begin(request, self.max_upload_bytes)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            session_by_key(&transaction, &request.owner_id, &request.upload_key)?
        {
            if existing.expected_digest != request.expected_digest
                || existing.expected_size_bytes != request.expected_size_bytes
                || existing.media_type != request.media_type
            {
                return Err(UploadError::ConflictingUploadKey);
            }
            transaction.commit()?;
            return Ok(existing);
        }
        reserve_quota(&transaction, request, self.quotas)?;
        let upload_id = self.next_upload_id(&transaction)?;
        transaction.execute(
            "INSERT INTO upload_sessions(upload_id, owner_id, upload_key, expected_digest,
                 expected_size_bytes, media_type, committed_offset, state, created_at_ms,
                 updated_at_ms, expires_at_ms, quota_reserved_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?8, ?9, ?5)",
            params![
                upload_id,
                request.owner_id,
                request.upload_key,
                request.expected_digest.to_string(),
                to_i64(request.expected_size_bytes)?,
                request.media_type,
                UploadState::Open as i64,
                to_i64(request.now_ms)?,
                to_i64(request.expires_at_ms)?
            ],
        )?;
        let session = session_by_id(&transaction, &upload_id)?
            .ok_or_else(|| UploadError::Corrupt("inserted upload disappeared".to_owned()))?;
        transaction.commit()?;
        Ok(session)
    }

    pub fn status(&self, owner_id: &str, upload_id: &str) -> Result<UploadSession, UploadError> {
        let database = self.connection()?;
        let session = session_by_id(&database, upload_id)?
            .ok_or_else(|| UploadError::NotFound(upload_id.to_owned()))?;
        authorize(&session, owner_id)?;
        Ok(session)
    }

    pub fn owns_completed_artifact(
        &self,
        owner_id: &str,
        digest: Sha256Digest,
    ) -> Result<bool, UploadError> {
        let database = self.connection()?;
        let found = database.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM artifact_owner_references
                WHERE owner_id = ?1 AND digest = ?2
            )",
            params![owner_id, digest.to_string()],
            |row| row.get(0),
        )?;
        Ok(found)
    }

    pub fn append(
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
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = session_by_id(&transaction, upload_id)?
            .ok_or_else(|| UploadError::NotFound(upload_id.to_owned()))?;
        authorize(&session, owner_id)?;
        ensure_open(&session, now_ms)?;
        if offset != session.committed_offset {
            return Err(UploadError::OffsetConflict {
                expected: session.committed_offset,
                received: offset,
            });
        }
        let next = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if next > session.expected_size_bytes || next > self.max_upload_bytes {
            return Err(UploadError::SizeLimitExceeded {
                limit: session.expected_size_bytes.min(self.max_upload_bytes),
                attempted: next,
            });
        }
        let path = self.data_path(upload_id)?;
        append_file(&path, offset, bytes)?;
        transaction.execute(
            "UPDATE upload_sessions SET committed_offset = ?2, updated_at_ms = ?3
             WHERE upload_id = ?1",
            params![upload_id, to_i64(next)?, to_i64(now_ms)?],
        )?;
        transaction.commit()?;
        Ok(next)
    }

    pub fn finalize(
        &self,
        owner_id: &str,
        upload_id: &str,
        artifact_store: &dyn ArtifactStore,
        now_ms: u64,
    ) -> Result<ArtifactIdentity, UploadError> {
        let _finalize_guard = self
            .finalize_lock
            .lock()
            .map_err(|_| UploadError::Corrupt("finalize lock poisoned".to_owned()))?;
        let session = {
            let mut database = self.connection()?;
            let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let session = session_by_id(&transaction, upload_id)?
                .ok_or_else(|| UploadError::NotFound(upload_id.to_owned()))?;
            authorize(&session, owner_id)?;
            if session.state == UploadState::Completed {
                return session.artifact.ok_or_else(|| {
                    UploadError::Corrupt("completed upload lacks artifact".to_owned())
                });
            }
            if !matches!(session.state, UploadState::Open | UploadState::Finalizing) {
                return Err(UploadError::InvalidState(session.state));
            }
            if now_ms >= session.expires_at_ms {
                return Err(UploadError::Expired);
            }
            if session.committed_offset != session.expected_size_bytes {
                return Err(UploadError::Incomplete {
                    expected: session.expected_size_bytes,
                    committed: session.committed_offset,
                });
            }
            transaction.execute(
                "UPDATE upload_sessions SET state = ?2, updated_at_ms = ?3 WHERE upload_id = ?1",
                params![upload_id, UploadState::Finalizing as i64, to_i64(now_ms)?],
            )?;
            transaction.commit()?;
            session
        };
        let path = self.data_path(upload_id)?;
        let mut file = File::open(&path).map_err(|source| UploadError::Io {
            operation: "open upload data for finalization",
            source,
        })?;
        let artifact = match artifact_store.ingest(
            &mut file,
            IngestRequest {
                expected_digest: Some(session.expected_digest),
                expected_size_bytes: Some(session.expected_size_bytes),
            },
        ) {
            Ok(result) => result.artifact,
            Err(error) => {
                if is_terminal_artifact_error(&error) {
                    self.mark_failed(upload_id, now_ms)?;
                }
                return Err(error.into());
            }
        };
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        record_completed_artifact(&transaction, &session.owner_id, artifact)?;
        transaction.execute(
            "UPDATE upload_sessions
             SET state = ?2, artifact_digest = ?3, updated_at_ms = ?4, quota_reserved_bytes = 0
             WHERE upload_id = ?1",
            params![
                upload_id,
                UploadState::Completed as i64,
                artifact.digest.to_string(),
                to_i64(now_ms)?
            ],
        )?;
        transaction.commit()?;
        remove_if_present(&path)?;
        Ok(artifact)
    }

    pub fn prune_expired(&self, now_ms: u64) -> Result<usize, UploadError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ids = {
            let mut statement = transaction.prepare(
                "SELECT upload_id FROM upload_sessions WHERE state != ?1 AND expires_at_ms <= ?2",
            )?;
            statement
                .query_map(
                    params![UploadState::Completed as i64, to_i64(now_ms)?],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        for id in &ids {
            remove_if_present(&self.data_path(id)?)?;
        }
        for id in &ids {
            transaction.execute("DELETE FROM upload_sessions WHERE upload_id = ?1", [id])?;
        }
        transaction.commit()?;
        Ok(ids.len())
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, UploadError> {
        self.connection
            .lock()
            .map_err(|_| UploadError::Corrupt("database lock poisoned".to_owned()))
    }

    fn next_upload_id(
        &self,
        transaction: &rusqlite::Transaction<'_>,
    ) -> Result<String, UploadError> {
        for _ in 0..32 {
            let id = format!("upload-{}", self.counter.fetch_add(1, Ordering::Relaxed));
            if session_by_id(transaction, &id)?.is_none() {
                return Ok(id);
            }
        }
        Err(UploadError::Corrupt(
            "unable to allocate upload ID".to_owned(),
        ))
    }

    fn data_path(&self, upload_id: &str) -> Result<PathBuf, UploadError> {
        if upload_id.is_empty()
            || !upload_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(UploadError::Corrupt("unsafe upload ID".to_owned()));
        }
        Ok(self.upload_root.join(upload_id))
    }

    fn mark_failed(&self, upload_id: &str, now_ms: u64) -> Result<(), UploadError> {
        self.connection()?.execute(
            "UPDATE upload_sessions
             SET state = ?2, updated_at_ms = ?3, quota_reserved_bytes = 0
             WHERE upload_id = ?1",
            params![upload_id, UploadState::Failed as i64, to_i64(now_ms)?],
        )?;
        Ok(())
    }

    fn cleanup_completed_data(&self) -> Result<(), UploadError> {
        let database = self.connection()?;
        let mut statement =
            database.prepare("SELECT upload_id FROM upload_sessions WHERE state = ?1")?;
        let ids = statement
            .query_map([UploadState::Completed as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(database);
        for id in ids {
            remove_if_present(&self.data_path(&id)?)?;
        }
        Ok(())
    }
}

fn migrate_quota_schema(connection: &mut Connection) -> Result<(), UploadError> {
    let has_reservation_column = {
        let mut statement = connection.prepare("PRAGMA table_info(upload_sessions)")?;
        statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "quota_reserved_bytes")
    };
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !has_reservation_column {
        transaction.execute(
            "ALTER TABLE upload_sessions
             ADD COLUMN quota_reserved_bytes INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
        transaction.execute(
            "UPDATE upload_sessions
             SET quota_reserved_bytes = expected_size_bytes
             WHERE state IN (?1, ?2)",
            params![UploadState::Open as i64, UploadState::Finalizing as i64],
        )?;
    }
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_objects(digest, size_bytes)
         SELECT artifact_digest, expected_size_bytes FROM upload_sessions
         WHERE state = ?1 AND artifact_digest IS NOT NULL",
        [UploadState::Completed as i64],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_owner_references(owner_id, digest, size_bytes)
         SELECT owner_id, artifact_digest, expected_size_bytes FROM upload_sessions
         WHERE state = ?1 AND artifact_digest IS NOT NULL",
        [UploadState::Completed as i64],
    )?;
    let conflicting_object = transaction
        .query_row(
            "SELECT upload_id FROM upload_sessions AS session
             JOIN artifact_objects AS object ON object.digest = session.artifact_digest
             WHERE session.state = ?1 AND object.size_bytes != session.expected_size_bytes
             LIMIT 1",
            [UploadState::Completed as i64],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(upload_id) = conflicting_object {
        return Err(UploadError::Corrupt(format!(
            "completed upload {upload_id} conflicts with artifact object size"
        )));
    }
    let conflicting_reference = transaction
        .query_row(
            "SELECT reference.owner_id, reference.digest
             FROM artifact_owner_references AS reference
             JOIN artifact_objects AS object ON object.digest = reference.digest
             WHERE reference.size_bytes != object.size_bytes
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((owner_id, digest)) = conflicting_reference {
        return Err(UploadError::Corrupt(format!(
            "owner {owner_id} reference conflicts with artifact {digest} size"
        )));
    }
    transaction.commit()?;
    Ok(())
}

fn validate_quotas(quotas: UploadQuotas) -> Result<(), UploadError> {
    if quotas.total_bytes == 0 || quotas.per_owner_bytes == 0 {
        return Err(UploadError::InvalidRequest(
            "artifact quota limits must be positive",
        ));
    }
    if quotas.total_bytes > i64::MAX as u64 || quotas.per_owner_bytes > i64::MAX as u64 {
        return Err(UploadError::InvalidRequest(
            "artifact quota limits exceed SQLite range",
        ));
    }
    if quotas.per_owner_bytes > quotas.total_bytes {
        return Err(UploadError::InvalidRequest(
            "per-owner artifact quota exceeds total quota",
        ));
    }
    Ok(())
}

fn reserve_quota(
    transaction: &rusqlite::Transaction<'_>,
    request: &BeginUpload,
    quotas: UploadQuotas,
) -> Result<(), UploadError> {
    let total_stored = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(size_bytes), 0) FROM artifact_objects",
        [],
    )?;
    let total_reserved = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(quota_reserved_bytes), 0) FROM upload_sessions
         WHERE state IN (?1, ?2) AND expires_at_ms > ?3",
        params![
            UploadState::Open as i64,
            UploadState::Finalizing as i64,
            to_i64(request.now_ms)?
        ],
    )?;
    enforce_quota(
        QuotaScope::Total,
        quotas.total_bytes,
        total_stored.saturating_add(total_reserved),
        request.expected_size_bytes,
    )?;

    let owner_stored = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(size_bytes), 0) FROM artifact_owner_references
         WHERE owner_id = ?1",
        [&request.owner_id],
    )?;
    let owner_reserved = query_bytes(
        transaction,
        "SELECT COALESCE(SUM(quota_reserved_bytes), 0) FROM upload_sessions
         WHERE owner_id = ?1 AND state IN (?2, ?3) AND expires_at_ms > ?4",
        params![
            request.owner_id,
            UploadState::Open as i64,
            UploadState::Finalizing as i64,
            to_i64(request.now_ms)?
        ],
    )?;
    enforce_quota(
        QuotaScope::Owner,
        quotas.per_owner_bytes,
        owner_stored.saturating_add(owner_reserved),
        request.expected_size_bytes,
    )
}

fn query_bytes(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<u64, UploadError> {
    let value = connection.query_row(sql, parameters, |row| row.get::<_, i64>(0))?;
    from_i64(value)
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

fn record_completed_artifact(
    transaction: &rusqlite::Transaction<'_>,
    owner_id: &str,
    artifact: ArtifactIdentity,
) -> Result<(), UploadError> {
    let digest = artifact.digest.to_string();
    let size = to_i64(artifact.size_bytes)?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_objects(digest, size_bytes) VALUES (?1, ?2)",
        params![digest, size],
    )?;
    let stored_size = transaction.query_row(
        "SELECT size_bytes FROM artifact_objects WHERE digest = ?1",
        [&digest],
        |row| row.get::<_, i64>(0),
    )?;
    if stored_size != size {
        return Err(UploadError::Corrupt(format!(
            "artifact {digest} has conflicting recorded sizes"
        )));
    }
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_owner_references(owner_id, digest, size_bytes)
         VALUES (?1, ?2, ?3)",
        params![owner_id, digest, size],
    )?;
    let owner_size = transaction.query_row(
        "SELECT size_bytes FROM artifact_owner_references WHERE owner_id = ?1 AND digest = ?2",
        params![owner_id, digest],
        |row| row.get::<_, i64>(0),
    )?;
    if owner_size != size {
        return Err(UploadError::Corrupt(format!(
            "owner {owner_id} has a conflicting size for artifact {digest}"
        )));
    }
    Ok(())
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

const fn is_terminal_artifact_error(error: &ArtifactStoreError) -> bool {
    matches!(
        error,
        ArtifactStoreError::SizeLimitExceeded { .. }
            | ArtifactStoreError::SizeMismatch { .. }
            | ArtifactStoreError::DigestMismatch { .. }
            | ArtifactStoreError::IntegrityViolation { .. }
    )
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

fn authorize(session: &UploadSession, owner_id: &str) -> Result<(), UploadError> {
    if session.owner_id == owner_id {
        Ok(())
    } else {
        Err(UploadError::OwnerMismatch)
    }
}

fn append_file(path: &Path, committed: u64, bytes: &[u8]) -> Result<(), UploadError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| UploadError::Io {
            operation: "open upload data",
            source,
        })?;
    let length = file
        .metadata()
        .map_err(|source| UploadError::Io {
            operation: "inspect upload data",
            source,
        })?
        .len();
    if length < committed {
        return Err(UploadError::Corrupt(format!(
            "upload data length {length} is behind committed offset {committed}"
        )));
    }
    if length > committed {
        file.set_len(committed).map_err(|source| UploadError::Io {
            operation: "truncate uncommitted upload tail",
            source,
        })?;
    }
    file.seek(SeekFrom::Start(committed))
        .map_err(|source| UploadError::Io {
            operation: "seek upload data",
            source,
        })?;
    file.write_all(bytes).map_err(|source| UploadError::Io {
        operation: "append upload data",
        source,
    })?;
    file.sync_all().map_err(|source| UploadError::Io {
        operation: "sync upload data",
        source,
    })
}

fn remove_if_present(path: &Path) -> Result<(), UploadError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(UploadError::Io {
            operation: "remove upload data",
            source,
        }),
    }
}

fn session_by_key(
    connection: &Connection,
    owner_id: &str,
    upload_key: &str,
) -> Result<Option<UploadSession>, UploadError> {
    query_session(
        connection,
        "owner_id = ?1 AND upload_key = ?2",
        params![owner_id, upload_key],
    )
}

fn session_by_id(
    connection: &Connection,
    upload_id: &str,
) -> Result<Option<UploadSession>, UploadError> {
    query_session(connection, "upload_id = ?1", params![upload_id])
}

fn query_session(
    connection: &Connection,
    predicate: &str,
    parameters: impl rusqlite::Params,
) -> Result<Option<UploadSession>, UploadError> {
    let sql = format!(
        "SELECT upload_id, owner_id, upload_key, expected_digest,
        expected_size_bytes, media_type, committed_offset, state, expires_at_ms, artifact_digest
        FROM upload_sessions WHERE {predicate}"
    );
    let row = connection
        .query_row(&sql, parameters, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })
        .optional()?;
    row.map(|row| {
        let expected_digest = Sha256Digest::from_str(&row.3)
            .map_err(|error| UploadError::Corrupt(error.to_string()))?;
        let expected_size_bytes = from_i64(row.4)?;
        let artifact = row
            .9
            .map(|digest| {
                Sha256Digest::from_str(&digest).map(|digest| ArtifactIdentity {
                    digest,
                    size_bytes: expected_size_bytes,
                })
            })
            .transpose()
            .map_err(|error| UploadError::Corrupt(error.to_string()))?;
        Ok(UploadSession {
            upload_id: row.0,
            owner_id: row.1,
            upload_key: row.2,
            expected_digest,
            expected_size_bytes,
            media_type: row.5,
            committed_offset: from_i64(row.6)?,
            state: UploadState::from_i64(row.7)?,
            expires_at_ms: from_i64(row.8)?,
            artifact,
        })
    })
    .transpose()
}

fn to_i64(value: u64) -> Result<i64, UploadError> {
    i64::try_from(value)
        .map_err(|_| UploadError::Corrupt(format!("value {value} exceeds SQLite range")))
}

fn from_i64(value: i64) -> Result<u64, UploadError> {
    u64::try_from(value).map_err(|_| UploadError::Corrupt(format!("negative stored value {value}")))
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
    use crate::{ArtifactReader, ArtifactStore, FilesystemArtifactStore, IngestResult};
    use ring::digest::{Context, SHA256};
    use std::io::Read;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
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
    fn begin_is_idempotent_per_owner_key_and_rejects_changed_metadata() -> Result<(), Box<dyn Error>>
    {
        let directory = tempfile::tempdir()?;
        let uploads = SqliteUploadStore::open(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("uploads"),
            1_024,
            8,
        )?;
        let request = request(b"content");
        let first = uploads.begin(&request)?;
        assert_eq!(uploads.begin(&request)?, first);
        let mut changed = request;
        changed.media_type = "application/changed".to_owned();
        assert!(matches!(
            uploads.begin(&changed),
            Err(UploadError::ConflictingUploadKey)
        ));
        assert!(matches!(
            uploads.status("other-worker", &first.upload_id),
            Err(UploadError::OwnerMismatch)
        ));
        Ok(())
    }

    #[test]
    fn append_truncates_bytes_not_committed_before_a_simulated_crash() -> Result<(), Box<dyn Error>>
    {
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
    fn digest_failure_is_terminal_and_never_publishes_expected_key() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let uploads = SqliteUploadStore::open(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("uploads"),
            1_024,
            8,
        )?;
        let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 1_024)?;
        let mut request = request(b"right");
        request.expected_digest = digest(b"wrong");
        let session = uploads.begin(&request)?;
        uploads.append("worker-1", &session.upload_id, 0, b"right", 2)?;
        assert!(matches!(
            uploads.finalize("worker-1", &session.upload_id, &cas, 3),
            Err(UploadError::Artifact(
                ArtifactStoreError::DigestMismatch { .. }
            ))
        ));
        assert_eq!(
            uploads.status("worker-1", &session.upload_id)?.state,
            UploadState::Failed
        );
        assert!(!cas.contains(request.expected_digest)?);
        Ok(())
    }

    #[test]
    fn transient_cas_failure_leaves_finalization_retryable() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let uploads = SqliteUploadStore::open(
            directory.path().join("uploads.sqlite3"),
            directory.path().join("uploads"),
            1_024,
            8,
        )?;
        let cas = FlakyStore {
            inner: FilesystemArtifactStore::open(directory.path().join("cas"), 1_024)?,
            fail_next: AtomicBool::new(true),
        };
        let request = request(b"retry");
        let session = uploads.begin(&request)?;
        uploads.append("worker-1", &session.upload_id, 0, b"retry", 2)?;
        assert!(matches!(
            uploads.finalize("worker-1", &session.upload_id, &cas, 3),
            Err(UploadError::Artifact(ArtifactStoreError::Io { .. }))
        ));
        assert_eq!(
            uploads.status("worker-1", &session.upload_id)?.state,
            UploadState::Finalizing
        );
        assert_eq!(
            uploads
                .finalize("worker-1", &session.upload_id, &cas, 4)?
                .digest,
            request.expected_digest
        );
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
        let uploads =
            SqliteUploadStore::open_with_quotas(&database, &upload_root, 100, 100, limits)?;
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
    fn per_owner_quota_does_not_block_another_owner() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let uploads = quota_store(directory.path(), 12, 6)?;
        uploads.begin(&quota_request("worker-1", "first", b"123456", 1, 100))?;
        assert!(matches!(
            uploads.begin(&quota_request("worker-1", "second", b"x", 1, 100)),
            Err(UploadError::QuotaExceeded {
                scope: QuotaScope::Owner,
                ..
            })
        ));
        uploads.begin(&quota_request("worker-2", "first", b"abcdef", 1, 100))?;
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
    fn terminal_failure_and_expiry_release_reservations() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let uploads = quota_store(directory.path(), 5, 5)?;
        let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 100)?;
        let mut invalid = quota_request("worker-1", "invalid", b"right", 1, 100);
        invalid.expected_digest = digest(b"wrong");
        let session = uploads.begin(&invalid)?;
        uploads.append("worker-1", &session.upload_id, 0, b"right", 2)?;
        assert!(matches!(
            uploads.finalize("worker-1", &session.upload_id, &cas, 3),
            Err(UploadError::Artifact(
                ArtifactStoreError::DigestMismatch { .. }
            ))
        ));
        uploads.begin(&quota_request("worker-2", "after-failure", b"12345", 4, 10))?;

        let expiry_directory = tempfile::tempdir()?;
        let expiring = quota_store(expiry_directory.path(), 5, 5)?;
        expiring.begin(&quota_request("worker-1", "expired", b"12345", 1, 10))?;
        expiring.begin(&quota_request(
            "worker-2",
            "after-expiry",
            b"abcde",
            10,
            100,
        ))?;
        assert_eq!(expiring.prune_expired(10)?, 1);
        Ok(())
    }

    #[test]
    fn completed_duplicate_digest_is_not_counted_twice() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let uploads = quota_store(directory.path(), 10, 10)?;
        let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 100)?;
        for key in ["first", "duplicate"] {
            let request = quota_request("worker-1", key, b"12345", 1, 100);
            let session = uploads.begin(&request)?;
            uploads.append("worker-1", &session.upload_id, 0, b"12345", 2)?;
            uploads.finalize("worker-1", &session.upload_id, &cas, 3)?;
        }
        uploads.begin(&quota_request("worker-1", "remaining", b"abcde", 4, 100))?;
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

    #[derive(Debug)]
    struct FlakyStore {
        inner: FilesystemArtifactStore,
        fail_next: AtomicBool,
    }

    impl ArtifactStore for FlakyStore {
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
}
