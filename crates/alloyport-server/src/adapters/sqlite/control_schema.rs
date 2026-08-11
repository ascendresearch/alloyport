//! Schema creation and forward migrations for the control repository.

use crate::storage::RepositoryError;
use rusqlite::{Connection, Transaction, TransactionBehavior};

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);

CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    registration_json TEXT NOT NULL,
    registered_at_ms INTEGER NOT NULL,
    last_seen_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS worker_connections (
    connection_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL REFERENCES workers(worker_id),
    instance_id TEXT NOT NULL,
    connected_at_ms INTEGER NOT NULL,
    disconnected_at_ms INTEGER,
    last_worker_sequence INTEGER NOT NULL,
    last_server_sequence INTEGER NOT NULL,
    last_server_acknowledged_by_worker INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_connections_worker
    ON worker_connections(worker_id, connected_at_ms);

CREATE TABLE IF NOT EXISTS assignments (
    attempt_id TEXT PRIMARY KEY,
    assignment_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    contract_json TEXT NOT NULL,
    state INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    last_sent_at_ms INTEGER,
    cancellation_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS assignments_assignment_attempt
    ON assignments(assignment_id, attempt_id);
CREATE INDEX IF NOT EXISTS assignments_worker_state
    ON assignments(worker_id, state, created_at_ms);
CREATE TABLE IF NOT EXISTS attempt_reassignments (
    expired_attempt_id TEXT PRIMARY KEY REFERENCES assignments(attempt_id),
    replacement_attempt_id TEXT NOT NULL UNIQUE REFERENCES assignments(attempt_id),
    replacement_worker_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS attempt_leases (
    attempt_id TEXT PRIMARY KEY REFERENCES assignments(attempt_id),
    lease_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    granted_at_ms INTEGER NOT NULL,
    renewed_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    expired_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS attempt_leases_expiry
    ON attempt_leases(expires_at_ms, expired_at_ms);

CREATE TABLE IF NOT EXISTS attempt_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL REFERENCES assignments(attempt_id),
    worker_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    disposition INTEGER NOT NULL,
    observation_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS attempt_observations_attempt
    ON attempt_observations(attempt_id, observation_id);
CREATE TABLE IF NOT EXISTS server_outbox_frames (
    connection_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    kind INTEGER NOT NULL,
    attempt_id TEXT,
    created_at_ms INTEGER NOT NULL,
    acknowledged_at_ms INTEGER,
    PRIMARY KEY(connection_id, sequence)
);
CREATE INDEX IF NOT EXISTS server_outbox_unacknowledged
    ON server_outbox_frames(connection_id, acknowledged_at_ms, sequence);
COMMIT;
";

pub(super) fn migrate(connection: &mut Connection) -> Result<(), RepositoryError> {
    connection.execute_batch(SCHEMA)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !column_exists(
        &transaction,
        "worker_connections",
        "last_server_acknowledged_by_worker",
    )? {
        transaction.execute_batch(
            "ALTER TABLE worker_connections
             ADD COLUMN last_server_acknowledged_by_worker INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !column_exists(&transaction, "assignments", "cancellation_reason")? {
        transaction
            .execute_batch("ALTER TABLE assignments ADD COLUMN cancellation_reason TEXT;")?;
    }
    if !column_exists(&transaction, "server_outbox_frames", "message_id")? {
        transaction.execute_batch(
            "ALTER TABLE server_outbox_frames
             ADD COLUMN message_id TEXT NOT NULL DEFAULT '';",
        )?;
    }
    transaction.execute_batch(
        "INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);
         INSERT OR IGNORE INTO schema_migrations(version) VALUES (3);",
    )?;
    transaction.commit()?;
    Ok(())
}

fn column_exists(
    transaction: &Transaction<'_>,
    table: &str,
    column: &str,
) -> Result<bool, RepositoryError> {
    let query = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let count = transaction.query_row(&query, [column], |row| row.get::<_, i64>(0))?;
    Ok(count == 1)
}
