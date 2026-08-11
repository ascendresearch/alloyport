//! Durable logical outbox and connection-delivery persistence.

use super::SqliteAttemptStore;
use crate::journal::{
    AttemptStoreError, StoreOutboxOutcome, WorkerOutboxMessage, WorkerOutboxPayload,
    WorkerOutboxStore,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl WorkerOutboxStore for SqliteAttemptStore {
    fn enqueue_outbox(
        &self,
        message: &WorkerOutboxMessage,
        at_ms: u64,
    ) -> Result<StoreOutboxOutcome, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = enqueue_outbox_transaction(&transaction, message, at_ms)?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn pending_outbox(&self) -> Result<Vec<WorkerOutboxMessage>, AttemptStoreError> {
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT message_id, attempt_id, payload_json
             FROM worker_outbox_messages ORDER BY created_at_ms, message_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .map(|row| {
                let (message_id, attempt_id, payload_json) = row?;
                Ok(WorkerOutboxMessage {
                    message_id,
                    attempt_id,
                    payload: serde_json::from_str(&payload_json)?,
                })
            })
            .collect()
    }

    fn record_outbox_delivery(
        &self,
        connection_id: &str,
        sequence: u64,
        message_id: &str,
        at_ms: u64,
    ) -> Result<(), AttemptStoreError> {
        self.connection()?.execute(
            "INSERT INTO worker_outbox_deliveries(
                 connection_id, sequence, message_id, delivered_at_ms
             ) VALUES (?1, ?2, ?3, ?4)",
            params![connection_id, to_i64(sequence)?, message_id, to_i64(at_ms)?],
        )?;
        Ok(())
    }

    fn acknowledge_outbox(
        &self,
        connection_id: &str,
        acknowledged_through: u64,
    ) -> Result<usize, AttemptStoreError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let message_ids = {
            let mut statement = transaction.prepare(
                "SELECT message_id FROM worker_outbox_deliveries
                 WHERE connection_id = ?1 AND sequence <= ?2",
            )?;
            statement
                .query_map(
                    params![connection_id, to_i64(acknowledged_through)?],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut deleted = 0;
        for message_id in message_ids {
            deleted += transaction.execute(
                "DELETE FROM worker_outbox_messages WHERE message_id = ?1",
                [message_id],
            )?;
        }
        transaction.commit()?;
        Ok(deleted)
    }

    fn prune_outbox_deliveries(&self, older_than_ms: u64) -> Result<usize, AttemptStoreError> {
        self.connection()?
            .execute(
                "DELETE FROM worker_outbox_deliveries WHERE delivered_at_ms < ?1",
                [to_i64(older_than_ms)?],
            )
            .map_err(AttemptStoreError::from)
    }

    fn outbox_len(&self) -> Result<usize, AttemptStoreError> {
        let count = self.connection()?.query_row(
            "SELECT COUNT(*) FROM worker_outbox_messages",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        usize::try_from(count)
            .map_err(|_| AttemptStoreError::Corrupt(format!("negative outbox count {count}")))
    }
}

pub(super) fn enqueue_outbox_transaction(
    transaction: &rusqlite::Transaction<'_>,
    message: &WorkerOutboxMessage,
    at_ms: u64,
) -> Result<StoreOutboxOutcome, AttemptStoreError> {
    let payload_json = serde_json::to_string(&message.payload)?;
    let existing = transaction
        .query_row(
            "SELECT attempt_id, payload_json
             FROM worker_outbox_messages WHERE message_id = ?1",
            [&message.message_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((attempt_id, existing_payload_json)) = existing {
        let compatible_admission = if attempt_id == message.attempt_id {
            let existing_payload: WorkerOutboxPayload =
                serde_json::from_str(&existing_payload_json)?;
            matches!(
                (&existing_payload, &message.payload),
                (
                    WorkerOutboxPayload::AssignmentAccepted {
                        assignment_id: left_assignment,
                        attempt_id: left_attempt,
                        ..
                    },
                    WorkerOutboxPayload::AssignmentAccepted {
                        assignment_id: right_assignment,
                        attempt_id: right_attempt,
                        ..
                    }
                ) if left_assignment == right_assignment && left_attempt == right_attempt
            )
        } else {
            false
        };
        if existing_payload_json != payload_json && !compatible_admission {
            return Err(AttemptStoreError::ConflictingOutboxMessage(
                message.message_id.clone(),
            ));
        }
        return Ok(StoreOutboxOutcome::Duplicate);
    }
    transaction.execute(
        "INSERT INTO worker_outbox_messages(
             message_id, attempt_id, payload_json, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4)",
        params![
            message.message_id,
            message.attempt_id,
            payload_json,
            to_i64(at_ms)?
        ],
    )?;
    Ok(StoreOutboxOutcome::Inserted)
}

pub(super) fn to_i64(value: u64) -> Result<i64, AttemptStoreError> {
    i64::try_from(value)
        .map_err(|_| AttemptStoreError::Corrupt(format!("timestamp {value} exceeds SQLite range")))
}
