//! `SQLite` implementation of canonical interaction persistence.

mod access;
mod events;

use crate::interaction::InteractionError;
use rusqlite::Connection;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS interaction_runs (
    run_id TEXT PRIMARY KEY,
    next_sequence INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS interaction_events (
    run_id TEXT NOT NULL REFERENCES interaction_runs(run_id),
    sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    dedup_key TEXT NOT NULL,
    fingerprint_json TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    PRIMARY KEY(run_id, sequence),
    UNIQUE(run_id, dedup_key)
);
CREATE TABLE IF NOT EXISTS interaction_output_chunks (
    attempt_id TEXT NOT NULL,
    stream INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    payload BLOB NOT NULL,
    run_id TEXT NOT NULL,
    event_sequence INTEGER NOT NULL,
    PRIMARY KEY(attempt_id, stream, byte_offset),
    FOREIGN KEY(run_id, event_sequence) REFERENCES interaction_events(run_id, sequence)
);
CREATE TABLE IF NOT EXISTS interaction_output_offsets (
    attempt_id TEXT NOT NULL,
    stream INTEGER NOT NULL,
    next_offset INTEGER NOT NULL,
    PRIMARY KEY(attempt_id, stream)
);
CREATE TABLE IF NOT EXISTS interaction_run_grants (
    run_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    state INTEGER NOT NULL,
    granted_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER,
    PRIMARY KEY(run_id, owner_id)
);
COMMIT;
";

impl From<rusqlite::Error> for InteractionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl From<serde_json::Error> for InteractionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(Box::new(error))
    }
}

pub struct SqliteInteractionStore {
    connection: Mutex<Connection>,
}

impl Debug for SqliteInteractionStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteInteractionStore")
            .finish_non_exhaustive()
    }
}

impl SqliteInteractionStore {
    /// Opens or creates a durable interaction-event store.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` cannot open or migrate the database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, InteractionError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Creates an in-memory store with the production schema.
    ///
    /// # Errors
    ///
    /// Returns an error if `SQLite` cannot initialize the schema.
    pub fn in_memory() -> Result<Self, InteractionError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, InteractionError> {
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, InteractionError> {
        self.connection
            .lock()
            .map_err(|_| InteractionError::LockPoisoned)
    }
}

fn to_i64(value: u64) -> Result<i64, InteractionError> {
    i64::try_from(value).map_err(|_| InteractionError::ValueOutOfRange(value))
}

#[cfg(test)]
#[path = "interaction_store_tests.rs"]
mod tests;
