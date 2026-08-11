//! Schema bootstrap and forward-only migrations for Artifact metadata.

use crate::upload::{ArtifactReferenceKind, UploadError, UploadState};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

pub(super) const SCHEMA: &str = r"
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
CREATE TABLE IF NOT EXISTS artifact_references (
    owner_id TEXT NOT NULL,
    reference_key TEXT NOT NULL,
    digest TEXT NOT NULL,
    kind INTEGER NOT NULL,
    purpose TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    retained_until_ms INTEGER,
    revoked_at_ms INTEGER,
    PRIMARY KEY(owner_id, reference_key)
);
CREATE INDEX IF NOT EXISTS artifact_references_digest
    ON artifact_references(digest);
CREATE INDEX IF NOT EXISTS artifact_references_active_owner_digest
    ON artifact_references(owner_id, digest) WHERE revoked_at_ms IS NULL;
CREATE TABLE IF NOT EXISTS artifact_gc_pending (
    digest TEXT PRIMARY KEY,
    marked_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS artifact_schema_migrations (
    name TEXT PRIMARY KEY
);
COMMIT;
";

pub(super) fn migrate_quota_schema(connection: &mut Connection) -> Result<(), UploadError> {
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
    if !migration_applied(&transaction, "artifact-accounting-v1")? {
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
        mark_migration(&transaction, "artifact-accounting-v1")?;
    }
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

pub(super) fn migrate_reference_schema(connection: &mut Connection) -> Result<(), UploadError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if migration_applied(&transaction, "artifact-references-v1")? {
        transaction.commit()?;
        return Ok(());
    }
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_references(
            owner_id, reference_key, digest, kind, purpose, created_at_ms,
            retained_until_ms, revoked_at_ms
         )
         SELECT owner_id, 'upload:' || upload_id, artifact_digest, ?2,
                'completed upload', updated_at_ms, NULL, NULL
         FROM upload_sessions
         WHERE state = ?1 AND artifact_digest IS NOT NULL",
        params![
            UploadState::Completed as i64,
            ArtifactReferenceKind::Upload as i64
        ],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO artifact_references(
            owner_id, reference_key, digest, kind, purpose, created_at_ms,
            retained_until_ms, revoked_at_ms
         )
         SELECT owner_id, 'legacy-upload:' || digest, digest, ?1,
                'migrated completed upload', 0, NULL, NULL
         FROM artifact_owner_references AS owner_reference
         WHERE NOT EXISTS (
            SELECT 1 FROM artifact_references AS reference
            WHERE reference.owner_id = owner_reference.owner_id
              AND reference.digest = owner_reference.digest
              AND reference.revoked_at_ms IS NULL
         )",
        [ArtifactReferenceKind::Upload as i64],
    )?;
    mark_migration(&transaction, "artifact-references-v1")?;
    transaction.commit()?;
    Ok(())
}

fn migration_applied(connection: &Connection, name: &str) -> Result<bool, UploadError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM artifact_schema_migrations WHERE name = ?1)",
        [name],
        |row| row.get(0),
    )?)
}

fn mark_migration(connection: &Connection, name: &str) -> Result<(), UploadError> {
    connection.execute(
        "INSERT OR IGNORE INTO artifact_schema_migrations(name) VALUES (?1)",
        [name],
    )?;
    Ok(())
}
