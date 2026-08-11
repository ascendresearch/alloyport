//! Canonical event append, output correlation, and replay queries.

use super::{SqliteInteractionStore, to_i64};
use crate::interaction::{
    AppendOutcome, InteractionError, InteractionEventReader, InteractionEventWriter, OutputAppend,
};
use alloyport_events::{Event, EventEnvelope, ProducerEvent, SCHEMA_VERSION};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;

impl InteractionEventWriter for SqliteInteractionStore {
    fn append(
        &self,
        dedup_key: &str,
        frame: &ProducerEvent,
    ) -> Result<AppendOutcome, InteractionError> {
        validate_input(dedup_key, frame)?;
        let fingerprint = fingerprint(frame)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = append_transaction(&transaction, dedup_key, &fingerprint, frame)?;
        transaction.commit()?;
        Ok(outcome)
    }

    fn append_output(
        &self,
        dedup_key: &str,
        attempt_id: &str,
        stream: i32,
        byte_offset: u64,
        payload: &[u8],
        frame: &ProducerEvent,
    ) -> Result<OutputAppend, InteractionError> {
        validate_input(dedup_key, frame)?;
        if attempt_id.trim().is_empty() {
            return Err(InteractionError::InvalidFrame(
                "output attempt identity is missing".into(),
            ));
        }
        let fingerprint = fingerprint(frame)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(stored) = output_chunk(&transaction, attempt_id, stream, byte_offset)? {
            if stored.payload != payload || stored.fingerprint != fingerprint {
                return Err(InteractionError::ConflictingOutput {
                    attempt_id: attempt_id.into(),
                    stream,
                    byte_offset,
                });
            }
            let envelope = event_at(&transaction, &stored.run_id, stored.sequence)?;
            transaction.commit()?;
            return Ok(OutputAppend {
                outcome: AppendOutcome::Duplicate(envelope),
                missing_bytes_before: 0,
            });
        }
        let expected = output_offset(&transaction, attempt_id, stream)?;
        if byte_offset < expected {
            return Err(InteractionError::ConflictingOutput {
                attempt_id: attempt_id.into(),
                stream,
                byte_offset,
            });
        }
        let missing_bytes_before = byte_offset.saturating_sub(expected);
        let outcome = append_transaction(&transaction, dedup_key, &fingerprint, frame)?;
        let sequence = outcome.envelope().sequence;
        transaction.execute(
            "INSERT INTO interaction_output_chunks(
                attempt_id, stream, byte_offset, payload, run_id, event_sequence
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                attempt_id,
                stream,
                to_i64(byte_offset)?,
                payload,
                frame.run_id,
                to_i64(sequence)?
            ],
        )?;
        let next_offset =
            byte_offset.saturating_add(u64::try_from(payload.len()).unwrap_or(u64::MAX));
        transaction.execute(
            "INSERT INTO interaction_output_offsets(attempt_id, stream, next_offset)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(attempt_id, stream) DO UPDATE SET next_offset = excluded.next_offset",
            params![attempt_id, stream, to_i64(next_offset)?],
        )?;
        transaction.commit()?;
        Ok(OutputAppend {
            outcome,
            missing_bytes_before,
        })
    }
}

impl InteractionEventReader for SqliteInteractionStore {
    fn events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError> {
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT envelope_json FROM interaction_events WHERE run_id = ?1 ORDER BY sequence",
        )?;
        statement
            .query_map([run_id], |row| row.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str(&row?)?))
            .collect()
    }

    fn events_after(
        &self,
        run_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>, InteractionError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let database = self.connection()?;
        let mut statement = database.prepare(
            "SELECT envelope_json FROM interaction_events
             WHERE run_id = ?1 AND sequence > ?2
             ORDER BY sequence
             LIMIT ?3",
        )?;
        statement
            .query_map(
                params![run_id, to_i64(after_sequence)?, usize_to_i64(limit)?],
                |row| row.get::<_, String>(0),
            )?
            .map(|row| Ok(serde_json::from_str(&row?)?))
            .collect()
    }

    fn latest_sequence(&self, run_id: &str) -> Result<Option<u64>, InteractionError> {
        let database = self.connection()?;
        database
            .query_row(
                "SELECT MAX(sequence) FROM interaction_events WHERE run_id = ?1",
                [run_id],
                |row| row.get::<_, Option<i64>>(0),
            )?
            .map(from_i64)
            .transpose()
    }
}

#[derive(Serialize)]
struct EventFingerprint<'a> {
    schema_version: u16,
    run_id: &'a str,
    task_id: &'a Option<String>,
    turn_id: &'a Option<String>,
    operation_id: &'a Option<String>,
    parent_operation_id: &'a Option<String>,
    producer_component: &'a str,
    authority: alloyport_events::Authority,
    visibility: alloyport_events::Visibility,
    event: &'a Event,
}

fn fingerprint(frame: &ProducerEvent) -> Result<String, InteractionError> {
    Ok(serde_json::to_string(&EventFingerprint {
        schema_version: frame.schema_version,
        run_id: &frame.run_id,
        task_id: &frame.task_id,
        turn_id: &frame.turn_id,
        operation_id: &frame.operation_id,
        parent_operation_id: &frame.parent_operation_id,
        producer_component: &frame.producer.component,
        authority: frame.authority,
        visibility: frame.visibility,
        event: &frame.event,
    })?)
}

fn validate_input(dedup_key: &str, frame: &ProducerEvent) -> Result<(), InteractionError> {
    if dedup_key.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "deduplication key is missing".into(),
        ));
    }
    if frame.schema_version != SCHEMA_VERSION {
        return Err(InteractionError::InvalidFrame(format!(
            "unsupported schema {}",
            frame.schema_version
        )));
    }
    if frame.run_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run identity is missing".into(),
        ));
    }
    Ok(())
}

fn append_transaction(
    transaction: &Transaction<'_>,
    dedup_key: &str,
    fingerprint: &str,
    frame: &ProducerEvent,
) -> Result<AppendOutcome, InteractionError> {
    if let Some((stored_fingerprint, envelope_json)) = transaction
        .query_row(
            "SELECT fingerprint_json, envelope_json FROM interaction_events
             WHERE run_id = ?1 AND dedup_key = ?2",
            params![frame.run_id, dedup_key],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
    {
        if stored_fingerprint != fingerprint {
            return Err(InteractionError::ConflictingDedupKey(dedup_key.into()));
        }
        return Ok(AppendOutcome::Duplicate(serde_json::from_str(
            &envelope_json,
        )?));
    }
    transaction.execute(
        "INSERT OR IGNORE INTO interaction_runs(run_id, next_sequence) VALUES (?1, 1)",
        [&frame.run_id],
    )?;
    let sequence = from_i64(transaction.query_row(
        "SELECT next_sequence FROM interaction_runs WHERE run_id = ?1",
        [&frame.run_id],
        |row| row.get(0),
    )?)?;
    let envelope = EventEnvelope {
        schema_version: SCHEMA_VERSION,
        event_id: format!("{}:{sequence:020}", frame.run_id),
        run_id: frame.run_id.clone(),
        task_id: frame.task_id.clone(),
        turn_id: frame.turn_id.clone(),
        operation_id: frame.operation_id.clone(),
        parent_operation_id: frame.parent_operation_id.clone(),
        producer_sequence: frame.producer_sequence,
        sequence,
        emitted_at_unix_ms: frame.emitted_at_unix_ms,
        producer: frame.producer.clone(),
        authority: frame.authority,
        visibility: frame.visibility,
        event: frame.event.clone(),
    };
    transaction.execute(
        "INSERT INTO interaction_events(
            run_id, sequence, event_id, dedup_key, fingerprint_json, envelope_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            frame.run_id,
            to_i64(sequence)?,
            envelope.event_id,
            dedup_key,
            fingerprint,
            serde_json::to_string(&envelope)?
        ],
    )?;
    transaction.execute(
        "UPDATE interaction_runs SET next_sequence = ?2 WHERE run_id = ?1",
        params![frame.run_id, to_i64(sequence.saturating_add(1))?],
    )?;
    Ok(AppendOutcome::Inserted(envelope))
}

struct StoredOutputChunk {
    payload: Vec<u8>,
    run_id: String,
    sequence: u64,
    fingerprint: String,
}

fn output_chunk(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    stream: i32,
    byte_offset: u64,
) -> Result<Option<StoredOutputChunk>, InteractionError> {
    transaction
        .query_row(
            "SELECT chunk.payload, chunk.run_id, chunk.event_sequence, event.fingerprint_json
             FROM interaction_output_chunks AS chunk
             JOIN interaction_events AS event
               ON event.run_id = chunk.run_id AND event.sequence = chunk.event_sequence
             WHERE chunk.attempt_id = ?1 AND chunk.stream = ?2 AND chunk.byte_offset = ?3",
            params![attempt_id, stream, to_i64(byte_offset)?],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?
        .map(|(payload, run_id, sequence, fingerprint)| {
            Ok(StoredOutputChunk {
                payload,
                run_id,
                sequence: from_i64(sequence)?,
                fingerprint,
            })
        })
        .transpose()
}

fn output_offset(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    stream: i32,
) -> Result<u64, InteractionError> {
    transaction
        .query_row(
            "SELECT next_offset FROM interaction_output_offsets
             WHERE attempt_id = ?1 AND stream = ?2",
            params![attempt_id, stream],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map_or(Ok(0), from_i64)
}

fn event_at(
    transaction: &Transaction<'_>,
    run_id: &str,
    sequence: u64,
) -> Result<EventEnvelope, InteractionError> {
    let json = transaction.query_row(
        "SELECT envelope_json FROM interaction_events WHERE run_id = ?1 AND sequence = ?2",
        params![run_id, to_i64(sequence)?],
        |row| row.get::<_, String>(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

fn usize_to_i64(value: usize) -> Result<i64, InteractionError> {
    i64::try_from(value).map_err(|_| InteractionError::ValueOutOfRange(u64::MAX))
}

fn from_i64(value: i64) -> Result<u64, InteractionError> {
    u64::try_from(value).map_err(|_| {
        InteractionError::InvalidFrame(format!("negative stored interaction value {value}"))
    })
}
