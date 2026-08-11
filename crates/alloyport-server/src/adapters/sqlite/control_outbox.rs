//! `SQLite` implementation of the durable server-to-worker frame outbox.

use super::control_records::to_i64;
use super::control_repository::SqliteControlRepository;
use crate::storage::{RepositoryError, ServerOutboxFrame, ServerOutboxRepository};
use rusqlite::{TransactionBehavior, params};

impl ServerOutboxRepository for SqliteControlRepository {
    fn record_server_frame(
        &self,
        frame: &ServerOutboxFrame,
        now_ms: u64,
    ) -> Result<(), RepositoryError> {
        self.connection()?.execute(
            "INSERT INTO server_outbox_frames(
                 connection_id, sequence, message_id, worker_id, kind, attempt_id, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                frame.connection_id,
                to_i64(frame.sequence)?,
                frame.message_id,
                frame.worker_id,
                frame.kind as i64,
                frame.attempt_id,
                to_i64(now_ms)?
            ],
        )?;
        Ok(())
    }

    fn compact_server_frames(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
        now_ms: u64,
    ) -> Result<usize, RepositoryError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE server_outbox_frames SET acknowledged_at_ms = ?3
             WHERE connection_id = ?1 AND sequence <= ?2 AND acknowledged_at_ms IS NULL",
            params![
                connection_id,
                to_i64(acknowledged_through)?,
                to_i64(now_ms)?
            ],
        )?;
        let deleted = transaction.execute(
            "DELETE FROM server_outbox_frames
             WHERE connection_id = ?1 AND acknowledged_at_ms IS NOT NULL",
            [connection_id],
        )?;
        transaction.commit()?;
        Ok(deleted)
    }

    fn server_outbox_len(&self, connection_id: &str) -> Result<usize, RepositoryError> {
        let count = self.connection()?.query_row(
            "SELECT COUNT(*) FROM server_outbox_frames WHERE connection_id = ?1",
            [connection_id],
            |row| row.get::<_, i64>(0),
        )?;
        usize::try_from(count)
            .map_err(|_| RepositoryError::Corrupt(format!("negative outbox count {count}")))
    }

    fn prune_orphaned_server_frames(
        &self,
        disconnected_before_ms: u64,
    ) -> Result<usize, RepositoryError> {
        self.connection()?
            .execute(
                "DELETE FROM server_outbox_frames
                 WHERE connection_id IN (
                     SELECT connection_id FROM worker_connections
                     WHERE disconnected_at_ms IS NOT NULL AND disconnected_at_ms < ?1
                 )",
                [to_i64(disconnected_before_ms)?],
            )
            .map_err(RepositoryError::from)
    }
}
