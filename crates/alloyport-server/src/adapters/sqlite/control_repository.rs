//! `SQLite` implementation of the durable control repository.

use crate::storage::RepositoryError;
#[cfg(test)]
use crate::storage::{
    ArtifactIdentity, AttemptObservation, ExecutionContract, FinishedObservation, ServerFrameKind,
    WorkerCapabilities,
};
use rusqlite::Connection;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::Mutex;

impl From<rusqlite::Error> for RepositoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl From<serde_json::Error> for RepositoryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(Box::new(error))
    }
}

/// SQLite-backed control repository with explicit schema migrations and transactions.
pub struct SqliteControlRepository {
    connection: Mutex<Connection>,
}

impl Debug for SqliteControlRepository {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteControlRepository")
            .finish_non_exhaustive()
    }
}

impl SqliteControlRepository {
    /// Opens or creates a durable repository and applies migrations.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` cannot open or migrate the database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Creates a process-local `SQLite` repository for tests and ephemeral service construction.
    ///
    /// # Errors
    ///
    /// Returns a repository error if `SQLite` initialization fails.
    pub fn in_memory() -> Result<Self, RepositoryError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(mut connection: Connection) -> Result<Self, RepositoryError> {
        super::control_schema::migrate(&mut connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(super) fn connection(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Connection>, RepositoryError> {
        self.connection
            .lock()
            .map_err(|_| RepositoryError::LockPoisoned)
    }
}

#[cfg(test)]
#[path = "control_repository_tests.rs"]
mod tests;
