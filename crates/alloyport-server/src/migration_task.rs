//! Durable migration intake owned by the persistent server process.

use alloyport_core::Sha256Digest;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum MigrationTaskState {
    Captured = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Cancelled = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationTaskRecord {
    pub task_id: String,
    pub project_name: String,
    pub project_digest: Sha256Digest,
    pub project_size_bytes: u64,
    pub file_count: u64,
    pub state: MigrationTaskState,
    pub created_at_ms: u64,
}

#[derive(Debug)]
pub enum MigrationTaskError {
    Storage(Box<dyn Error + Send + Sync>),
    Conflict,
    NotFound,
    Corrupt(String),
}

impl Display for MigrationTaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => Display::fmt(error, formatter),
            Self::Conflict => formatter.write_str("migration request identity was reused"),
            Self::NotFound => formatter.write_str("migration task was not found"),
            Self::Corrupt(detail) => write!(formatter, "corrupt migration task: {detail}"),
        }
    }
}

impl Error for MigrationTaskError {}

impl From<rusqlite::Error> for MigrationTaskError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

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
        Self::from_connection(Connection::open(path)?)
    }

    #[cfg(test)]
    /// Creates an ephemeral store using the production schema.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` initialization fails.
    pub fn in_memory() -> Result<Self, MigrationTaskError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, MigrationTaskError> {
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    #[allow(clippy::too_many_arguments)]
    /// Creates a task idempotently for one owner/request identity.
    ///
    /// # Errors
    ///
    /// Returns a conflict for changed retry bytes or a storage/corruption error.
    pub fn submit(
        &self,
        owner_id: &str,
        request_id: &str,
        task_id: &str,
        project_name: &str,
        project_digest: Sha256Digest,
        project_size_bytes: u64,
        file_count: u64,
        created_at_ms: u64,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = select_by_request(&transaction, owner_id, request_id)?;
        if let Some(existing) = existing {
            if existing.task_id == task_id
                && existing.project_name == project_name
                && existing.project_digest == project_digest
                && existing.project_size_bytes == project_size_bytes
                && existing.file_count == file_count
            {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(MigrationTaskError::Conflict);
        }
        transaction.execute(
            "INSERT INTO migration_tasks (
                task_id, owner_id, request_id, project_name, project_digest,
                project_size_bytes, file_count, state, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                task_id,
                owner_id,
                request_id,
                project_name,
                project_digest.to_string(),
                to_i64(project_size_bytes)?,
                to_i64(file_count)?,
                MigrationTaskState::Captured as i64,
                to_i64(created_at_ms)?,
            ],
        )?;
        let record = select_by_id(&transaction, owner_id, task_id)?
            .ok_or_else(|| MigrationTaskError::Corrupt("inserted row is absent".to_owned()))?;
        transaction.commit()?;
        Ok(record)
    }

    /// Reads one owner-scoped task.
    ///
    /// # Errors
    ///
    /// Returns not found or a storage/corruption error.
    pub fn get(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        select_by_id(&*self.connection()?, owner_id, task_id)?.ok_or(MigrationTaskError::NotFound)
    }

    /// Lists recent tasks belonging to one owner.
    ///
    /// # Errors
    ///
    /// Returns a storage/corruption error.
    pub fn list(
        &self,
        owner_id: &str,
        limit: usize,
    ) -> Result<Vec<MigrationTaskRecord>, MigrationTaskError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT task_id, project_name, project_digest, project_size_bytes, file_count,
                    state, created_at_ms
             FROM migration_tasks WHERE owner_id = ?1
             ORDER BY created_at_ms DESC, task_id DESC LIMIT ?2",
        )?;
        statement
            .query_map(params![owner_id, usize_to_i64(limit)?], row_to_record)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Marks a captured or running task cancelled, idempotently.
    ///
    /// # Errors
    ///
    /// Returns not found or a storage/corruption error.
    pub fn cancel(
        &self,
        owner_id: &str,
        task_id: &str,
    ) -> Result<MigrationTaskRecord, MigrationTaskError> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE migration_tasks SET state = ?3
             WHERE owner_id = ?1 AND task_id = ?2 AND state IN (?4, ?5)",
            params![
                owner_id,
                task_id,
                MigrationTaskState::Cancelled as i64,
                MigrationTaskState::Captured as i64,
                MigrationTaskState::Running as i64,
            ],
        )?;
        select_by_id(&connection, owner_id, task_id)?.ok_or(MigrationTaskError::NotFound)
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
        .map_err(Into::into)
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
        .map_err(Into::into)
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MigrationTaskRecord> {
    let digest: String = row.get(2)?;
    let state: i64 = row.get(5)?;
    Ok(MigrationTaskRecord {
        task_id: row.get(0)?,
        project_name: row.get(1)?,
        project_digest: Sha256Digest::from_str(&digest).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        project_size_bytes: from_i64(row.get(3)?, "project_size_bytes")?,
        file_count: from_i64(row.get(4)?, "file_count")?,
        state: match state {
            1 => MigrationTaskState::Captured,
            2 => MigrationTaskState::Running,
            3 => MigrationTaskState::Completed,
            4 => MigrationTaskState::Failed,
            5 => MigrationTaskState::Cancelled,
            _ => return Err(invalid_column("state")),
        },
        created_at_ms: from_i64(row.get(6)?, "created_at_ms")?,
    })
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
