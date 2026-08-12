//! `SQLite` implementation of the worker attempt journal.

mod device_lease;
mod lifecycle;
mod outbox;

use crate::journal::AttemptStoreError;
use rusqlite::Connection;
use std::fmt::{self, Debug, Formatter};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);
CREATE TABLE IF NOT EXISTS journal_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    assignment_id TEXT NOT NULL,
    assignment_json TEXT NOT NULL,
    phase INTEGER NOT NULL,
    admitted_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    finished_json TEXT
);
CREATE INDEX IF NOT EXISTS attempts_phase ON attempts(phase, admitted_at_ms);
CREATE TABLE IF NOT EXISTS worker_outbox_messages (
    message_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_outbox_attempt
    ON worker_outbox_messages(attempt_id, created_at_ms);
CREATE TABLE IF NOT EXISTS worker_outbox_deliveries (
    connection_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    message_id TEXT NOT NULL REFERENCES worker_outbox_messages(message_id) ON DELETE CASCADE,
    delivered_at_ms INTEGER NOT NULL,
    PRIMARY KEY(connection_id, sequence)
);
CREATE INDEX IF NOT EXISTS worker_outbox_delivery_message
    ON worker_outbox_deliveries(message_id, delivered_at_ms);
CREATE TABLE IF NOT EXISTS device_leases (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
    device_id TEXT NOT NULL,
    acquired_at_ms INTEGER NOT NULL,
    released_at_ms INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS active_device_lease
    ON device_leases(device_id) WHERE released_at_ms IS NULL;
CREATE TABLE IF NOT EXISTS device_preflights (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
    observation_json TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (3);
COMMIT;
";

impl From<rusqlite::Error> for AttemptStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl From<serde_json::Error> for AttemptStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(Box::new(error))
    }
}

pub struct SqliteAttemptStore {
    connection: Mutex<Connection>,
}

impl Debug for SqliteAttemptStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteAttemptStore")
            .finish_non_exhaustive()
    }
}

impl SqliteAttemptStore {
    /// Opens or creates a durable worker journal.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot open or migrate the journal.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AttemptStoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Creates an ephemeral journal using the same schema and transactions as production.
    ///
    /// # Errors
    ///
    /// Returns an error when the in-memory database cannot be initialized.
    pub fn in_memory() -> Result<Self, AttemptStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, AttemptStoreError> {
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, AttemptStoreError> {
        self.connection
            .lock()
            .map_err(|_| AttemptStoreError::LockPoisoned)
    }
}

#[cfg(test)]
#[path = "attempt_store_tests.rs"]
mod tests;
