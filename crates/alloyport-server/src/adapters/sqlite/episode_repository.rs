//! Crash-durable `SQLite` adapter for provider-neutral Agent Episode state.

use alloyport_core::{
    DurableEpisodeState, EpisodeId, EpisodeRepository, EpisodeRepositoryError,
    VersionedEpisodeState,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS agent_episodes (
    episode_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK(revision >= 0),
    state_json BLOB NOT NULL
);
";

/// `SQLite` compare-and-swap repository for complete Agent Episode snapshots.
pub struct SqliteEpisodeRepository {
    connection: Mutex<Connection>,
}

impl Debug for SqliteEpisodeRepository {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteEpisodeRepository")
            .finish_non_exhaustive()
    }
}

impl SqliteEpisodeRepository {
    /// Opens or creates the episode database.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when `SQLite` cannot open or migrate the database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EpisodeRepositoryError> {
        Self::from_connection(Connection::open(path).map_err(adapter_error)?)
    }

    /// Creates a process-local repository for adapter and composition tests.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when `SQLite` cannot initialize the schema.
    pub fn in_memory() -> Result<Self, EpisodeRepositoryError> {
        Self::from_connection(Connection::open_in_memory().map_err(adapter_error)?)
    }

    fn from_connection(connection: Connection) -> Result<Self, EpisodeRepositoryError> {
        connection.execute_batch(SCHEMA).map_err(adapter_error)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, EpisodeRepositoryError> {
        self.connection
            .lock()
            .map_err(|_| EpisodeRepositoryError::Adapter("SQLite lock poisoned".to_owned()))
    }
}

impl EpisodeRepository for SqliteEpisodeRepository {
    fn create(&mut self, state: DurableEpisodeState) -> Result<(), EpisodeRepositoryError> {
        let episode_id = state.episode().id().clone();
        let bytes = encode(&state)?;
        let connection = self.connection()?;
        let inserted = connection
            .execute(
                "INSERT OR IGNORE INTO agent_episodes(episode_id, revision, state_json)\
                 VALUES (?1, 0, ?2)",
                params![episode_id.to_string(), bytes],
            )
            .map_err(adapter_error)?;
        if inserted == 0 {
            return Err(EpisodeRepositoryError::AlreadyExists(episode_id));
        }
        Ok(())
    }

    fn load(&self, id: &EpisodeId) -> Result<VersionedEpisodeState, EpisodeRepositoryError> {
        let connection = self.connection()?;
        let stored = connection
            .query_row(
                "SELECT revision, state_json FROM agent_episodes WHERE episode_id = ?1",
                [id.to_string()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(adapter_error)?
            .ok_or_else(|| EpisodeRepositoryError::NotFound(id.clone()))?;
        let revision = u64::try_from(stored.0).map_err(|_| {
            EpisodeRepositoryError::Adapter("stored episode revision is negative".to_owned())
        })?;
        let state = decode(&stored.1)?;
        if state.episode().id() != id {
            return Err(EpisodeRepositoryError::Adapter(
                "stored episode identity does not match its key".to_owned(),
            ));
        }
        Ok(VersionedEpisodeState { revision, state })
    }

    fn save(
        &mut self,
        expected_revision: u64,
        state: DurableEpisodeState,
    ) -> Result<u64, EpisodeRepositoryError> {
        let episode_id = state.episode().id().clone();
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or(EpisodeRepositoryError::RevisionExhausted)?;
        let expected_sql = i64::try_from(expected_revision).map_err(|_| {
            EpisodeRepositoryError::Adapter("expected revision exceeds SQLite range".to_owned())
        })?;
        let next_sql = i64::try_from(next_revision).map_err(|_| {
            EpisodeRepositoryError::Adapter("next revision exceeds SQLite range".to_owned())
        })?;
        let bytes = encode(&state)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(adapter_error)?;
        let actual = transaction
            .query_row(
                "SELECT revision FROM agent_episodes WHERE episode_id = ?1",
                [episode_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(adapter_error)?
            .ok_or_else(|| EpisodeRepositoryError::NotFound(episode_id.clone()))?;
        let actual = u64::try_from(actual).map_err(|_| {
            EpisodeRepositoryError::Adapter("stored episode revision is negative".to_owned())
        })?;
        if actual != expected_revision {
            return Err(EpisodeRepositoryError::Conflict {
                expected: expected_revision,
                actual,
            });
        }
        let updated = transaction
            .execute(
                "UPDATE agent_episodes SET revision = ?1, state_json = ?2\
                 WHERE episode_id = ?3 AND revision = ?4",
                params![next_sql, bytes, episode_id.to_string(), expected_sql],
            )
            .map_err(adapter_error)?;
        if updated != 1 {
            return Err(EpisodeRepositoryError::Conflict {
                expected: expected_revision,
                actual,
            });
        }
        transaction.commit().map_err(adapter_error)?;
        Ok(next_revision)
    }
}

fn encode(state: &DurableEpisodeState) -> Result<Vec<u8>, EpisodeRepositoryError> {
    serde_json::to_vec(state).map_err(adapter_error)
}

fn decode(bytes: &[u8]) -> Result<DurableEpisodeState, EpisodeRepositoryError> {
    let state: DurableEpisodeState = serde_json::from_slice(bytes).map_err(adapter_error)?;
    state.validate_recovered().map_err(adapter_error)?;
    Ok(state)
}

fn adapter_error(error: impl std::fmt::Display) -> EpisodeRepositoryError {
    EpisodeRepositoryError::Adapter(error.to_string())
}

#[cfg(test)]
#[path = "episode_repository_tests.rs"]
mod tests;
