//! `SQLite` implementation of worker registration and control-connection persistence.

use super::control_records::to_i64;
use super::control_repository::SqliteControlRepository;
use crate::storage::{
    ConnectionRegistration, RepositoryError, WorkerConnectionRepository, WorkerRegistration,
};
use rusqlite::{TransactionBehavior, params};

impl WorkerConnectionRepository for SqliteControlRepository {
    fn register_worker(
        &self,
        registration: &WorkerRegistration,
        connection: &ConnectionRegistration,
    ) -> Result<(), RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let registration_json = serde_json::to_string(registration)?;
        transaction.execute(
            "INSERT INTO workers(worker_id, registration_json, registered_at_ms, last_seen_at_ms)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(worker_id) DO UPDATE SET
                 registration_json = excluded.registration_json,
                 last_seen_at_ms = excluded.last_seen_at_ms",
            params![
                registration.worker_id,
                registration_json,
                to_i64(connection.connected_at_ms)?
            ],
        )?;
        transaction.execute(
            "INSERT INTO worker_connections(
                 connection_id, worker_id, instance_id, connected_at_ms,
                 last_worker_sequence, last_server_sequence,
                 last_server_acknowledged_by_worker
             ) VALUES (?1, ?2, ?3, ?4, 1, 1, 0)",
            params![
                connection.connection_id,
                connection.worker_id,
                connection.instance_id,
                to_i64(connection.connected_at_ms)?
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn update_connection_sequences(
        &self,
        connection_id: &str,
        last_worker_sequence: u64,
        last_server_sequence: u64,
        last_server_acknowledged_by_worker: u64,
        observed_at_ms: u64,
    ) -> Result<(), RepositoryError> {
        let database = self.connection()?;
        database.execute(
            "UPDATE worker_connections
             SET last_worker_sequence = ?2, last_server_sequence = ?3,
                 last_server_acknowledged_by_worker = ?4
             WHERE connection_id = ?1",
            params![
                connection_id,
                to_i64(last_worker_sequence)?,
                to_i64(last_server_sequence)?,
                to_i64(last_server_acknowledged_by_worker)?
            ],
        )?;
        database.execute(
            "UPDATE workers SET last_seen_at_ms = ?2
             WHERE worker_id = (
                 SELECT worker_id FROM worker_connections WHERE connection_id = ?1
             )",
            params![connection_id, to_i64(observed_at_ms)?],
        )?;
        Ok(())
    }

    fn disconnect(&self, connection_id: &str, at_ms: u64) -> Result<(), RepositoryError> {
        self.connection()?.execute(
            "UPDATE worker_connections SET disconnected_at_ms = COALESCE(disconnected_at_ms, ?2)
             WHERE connection_id = ?1",
            params![connection_id, to_i64(at_ms)?],
        )?;
        Ok(())
    }
}
