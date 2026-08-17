//! `SQLite` implementation of the durable migration intake port.

use crate::migration_task::{
    MigrationTaskError, MigrationTaskRecord, MigrationTaskState, MigrationTaskStore,
    MigrationTaskSubmission,
};
use alloyport_core::Sha256Digest;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::error::Error;
use std::fmt::{self, Debug, Formatter};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS migration_tasks (
    task_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    project_name TEXT NOT NULL,
    project_digest TEXT NOT NULL,
    project_size_bytes INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    state INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(owner_id, request_id)
);
CREATE INDEX IF NOT EXISTS migration_tasks_owner_created
    ON migration_tasks(owner_id, created_at_ms DESC, task_id DESC);
COMMIT;
";

pub struct SqliteMigrationTaskStore {
    connection: Mutex<Connection>,
}

impl Debug for SqliteMigrationTaskStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteMigrationTaskStore")
            .finish_non_exhaustive()
    }
}

impl SqliteMigrationTaskStore {
    /// Opens the task database and installs the intake schema.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the database cannot be created or migrated.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MigrationTaskError> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(Self::storage)?;
        }
        Self::from_connection(Connection::open(path).map_err(adapter)?)
    }

    #[cfg(test)]
    /// Creates an ephemeral store using the production schema.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` initialization fails.
    pub fn in_memory() -> Result<Self, MigrationTaskError> {
        Self::from_connection(Connection::open_in_memory().map_err(adapter)?)
    }

    fn from_connection(connection: Connection) -> Result<Self, MigrationTaskError> {
        connection.execute_batch(SCHEMA).map_err(adapter)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, MigrationTaskError> {
        self.connection
            .lock()
            .map_err(|_| MigrationTaskError::Corrupt("database lock poisoned".to_owned()))
    }

    fn storage(error: impl Error + Send + Sync + 'static) -> MigrationTaskError {
        MigrationTaskError::Storage(Box::new(error))
    }
}

impl MigrationTaskStore for SqliteMigrationTaskStore {
    fn submit(
        &self,
        submission: MigrationTaskSubmission<'_>,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(adapter)?;
        let existing = select_by_request(&transaction, submission.owner_id, submission.request_id)?;
        if let Some(existing) = existing {
            if existing.task_id == submission.task_id
                && existing.project_name == submission.project_name
                && existing.project_digest == submission.project_digest
                && existing.project_size_bytes == submission.project_size_bytes
                && existing.file_count == submission.file_count
            {
                transaction.commit().map_err(adapter)?;
                return Ok(existing);
            }
            return Err(MigrationTaskError::Conflict);
        }
        transaction
            .execute(
                "INSERT INTO migration_tasks (
                task_id, owner_id, request_id, project_name, project_digest,
                project_size_bytes, file_count, state, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    submission.task_id,
                    submission.owner_id,
                    submission.request_id,
                    submission.project_name,
                    submission.project_digest.to_string(),
                    to_i64(submission.project_size_bytes)?,
                    to_i64(submission.file_count)?,
                    MigrationTaskState::Captured as i64,
                    to_i64(submission.created_at_ms)?,
                ],
            )
            .map_err(adapter)?;
        let record = select_by_id(&transaction, submission.owner_id, submission.task_id)?
            .ok_or_else(|| MigrationTaskError::Corrupt("inserted row is absent".to_owned()))?;
        transaction.commit().map_err(adapter)?;
        Ok(record)
    }

    fn get(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        select_by_id(&*self.connection()?, owner_id, task_id)?.ok_or(MigrationTaskError::NotFound)
    }

    fn list(
        &self,
        owner_id: &str,
        limit: usize,
    ) -> Result<Vec<MigrationTaskRecord>, MigrationTaskError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT task_id, project_name, project_digest, project_size_bytes, file_count,
                    state, created_at_ms
             FROM migration_tasks WHERE owner_id = ?1
             ORDER BY created_at_ms DESC, task_id DESC LIMIT ?2",
            )
            .map_err(adapter)?;
        statement
            .query_map(params![owner_id, usize_to_i64(limit)?], row_to_record)
            .map_err(adapter)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(adapter)
    }

    fn cancel(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        let connection = self.connection()?;
        connection
            .execute(
                "UPDATE migration_tasks SET state = ?3
             WHERE owner_id = ?1 AND task_id = ?2 AND state IN (?4, ?5)",
                params![
                    owner_id,
                    task_id,
                    MigrationTaskState::Cancelled as i64,
                    MigrationTaskState::Captured as i64,
                    MigrationTaskState::Running as i64,
                ],
            )
            .map_err(adapter)?;
        select_by_id(&connection, owner_id, task_id)?.ok_or(MigrationTaskError::NotFound)
    }

    fn claim_next(&self) -> Result<Option<MigrationTaskRecord>, MigrationTaskError> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(adapter)?;
        let record = transaction
            .query_row(
                "SELECT owner_id, task_id, project_name, project_digest, project_size_bytes,
                        file_count, state, created_at_ms
                 FROM migration_tasks WHERE state IN (?1, ?2)
                 ORDER BY CASE state WHEN ?2 THEN 0 ELSE 1 END, created_at_ms, task_id LIMIT 1",
                params![
                    MigrationTaskState::Captured as i64,
                    MigrationTaskState::Running as i64,
                ],
                row_to_claimed_record,
            )
            .optional()
            .map_err(adapter)?;
        let Some(mut record) = record else {
            transaction.commit().map_err(adapter)?;
            return Ok(None);
        };
        if record.state == MigrationTaskState::Captured {
            transaction
                .execute(
                    "UPDATE migration_tasks SET state = ?2 WHERE task_id = ?1 AND state = ?3",
                    params![
                        record.task_id,
                        MigrationTaskState::Running as i64,
                        MigrationTaskState::Captured as i64,
                    ],
                )
                .map_err(adapter)?;
            record.state = MigrationTaskState::Running;
        }
        transaction.commit().map_err(adapter)?;
        Ok(Some(record))
    }

    fn finish(&self, task_id: &str, state: MigrationTaskState) -> Result<(), MigrationTaskError> {
        if !matches!(
            state,
            MigrationTaskState::Completed | MigrationTaskState::Failed
        ) {
            return Err(MigrationTaskError::Corrupt(
                "finish requires a completed or failed state".to_owned(),
            ));
        }
        let connection = self.connection()?;
        connection
            .execute(
                "UPDATE migration_tasks SET state = ?2 WHERE task_id = ?1 AND state = ?3",
                params![task_id, state as i64, MigrationTaskState::Running as i64],
            )
            .map_err(adapter)?;
        Ok(())
    }

    fn resume(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        let connection = self.connection()?;
        let changed = connection
            .execute(
                "UPDATE migration_tasks SET state = ?3
             WHERE owner_id = ?1 AND task_id = ?2 AND state = ?4",
                params![
                    owner_id,
                    task_id,
                    MigrationTaskState::Captured as i64,
                    MigrationTaskState::Failed as i64,
                ],
            )
            .map_err(adapter)?;
        if changed == 0 {
            // Deliberately not silent. Resuming a completed, cancelled, or already queued task
            // would look like it worked and do nothing.
            return Err(MigrationTaskError::Corrupt(
                "only a failed migration can be resumed".to_owned(),
            ));
        }
        select_by_id(&connection, owner_id, task_id)?.ok_or(MigrationTaskError::NotFound)
    }

    fn is_cancelled(&self, task_id: &str) -> Result<bool, MigrationTaskError> {
        let connection = self.connection()?;
        let state: Option<i64> = connection
            .query_row(
                "SELECT state FROM migration_tasks WHERE task_id = ?1",
                [task_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(adapter)?;
        state
            .map(|state| state == MigrationTaskState::Cancelled as i64)
            .ok_or(MigrationTaskError::NotFound)
    }
}

fn select_by_request(
    connection: &Connection,
    owner_id: &str,
    request_id: &str,
) -> Result<Option<MigrationTaskRecord>, MigrationTaskError> {
    connection
        .query_row(
            "SELECT task_id, project_name, project_digest, project_size_bytes, file_count,
                    state, created_at_ms
             FROM migration_tasks WHERE owner_id = ?1 AND request_id = ?2",
            params![owner_id, request_id],
            row_to_record,
        )
        .optional()
        .map_err(adapter)
}

fn select_by_id(
    connection: &Connection,
    owner_id: &str,
    task_id: &str,
) -> Result<Option<MigrationTaskRecord>, MigrationTaskError> {
    connection
        .query_row(
            "SELECT task_id, project_name, project_digest, project_size_bytes, file_count,
                    state, created_at_ms
             FROM migration_tasks WHERE owner_id = ?1 AND task_id = ?2",
            params![owner_id, task_id],
            row_to_record,
        )
        .optional()
        .map_err(adapter)
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MigrationTaskRecord> {
    row_to_record_shifted(row, 0)
}

fn row_to_claimed_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MigrationTaskRecord> {
    let owner_id: String = row.get(0)?;
    let mut record = row_to_record_shifted(row, 1)?;
    record.owner_id = owner_id;
    Ok(record)
}

fn row_to_record_shifted(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<MigrationTaskRecord> {
    let digest: String = row.get(offset + 2)?;
    let state: i64 = row.get(offset + 5)?;
    Ok(MigrationTaskRecord {
        owner_id: String::new(),
        task_id: row.get(offset)?,
        project_name: row.get(offset + 1)?,
        project_digest: Sha256Digest::from_str(&digest).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                offset + 2,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        project_size_bytes: from_i64(row.get(offset + 3)?, "project_size_bytes")?,
        file_count: from_i64(row.get(offset + 4)?, "file_count")?,
        state: decode_state(state)?,
        created_at_ms: from_i64(row.get(offset + 6)?, "created_at_ms")?,
    })
}

fn decode_state(state: i64) -> rusqlite::Result<MigrationTaskState> {
    match state {
        1 => Ok(MigrationTaskState::Captured),
        2 => Ok(MigrationTaskState::Running),
        3 => Ok(MigrationTaskState::Completed),
        4 => Ok(MigrationTaskState::Failed),
        5 => Ok(MigrationTaskState::Cancelled),
        _ => Err(invalid_column("state")),
    }
}

fn to_i64(value: u64) -> Result<i64, MigrationTaskError> {
    i64::try_from(value).map_err(|_| MigrationTaskError::Corrupt("integer overflow".to_owned()))
}

fn usize_to_i64(value: usize) -> Result<i64, MigrationTaskError> {
    i64::try_from(value).map_err(|_| MigrationTaskError::Corrupt("integer overflow".to_owned()))
}

fn from_i64(value: i64, field: &'static str) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| invalid_column(field))
}

fn invalid_column(field: &'static str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Integer,
        format!("invalid {field}").into(),
    )
}

fn adapter(error: rusqlite::Error) -> MigrationTaskError {
    MigrationTaskError::Storage(Box::new(error))
}

#[cfg(test)]
#[path = "migration_task_store_tests.rs"]
mod tests;
