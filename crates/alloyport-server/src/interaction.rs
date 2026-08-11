//! Durable canonical interaction events translated from worker observations.

use alloyport_events::{Event, EventEnvelope, ProducerEvent, SCHEMA_VERSION};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
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
COMMIT;
";

#[derive(Debug)]
pub enum InteractionError {
    Sqlite(rusqlite::Error),
    Serialization(serde_json::Error),
    InvalidFrame(String),
    ConflictingDedupKey(String),
    ConflictingOutput {
        attempt_id: String,
        stream: i32,
        byte_offset: u64,
    },
    ValueOutOfRange(u64),
    LockPoisoned,
}

impl Display for InteractionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => Display::fmt(error, formatter),
            Self::Serialization(error) => Display::fmt(error, formatter),
            Self::InvalidFrame(detail) => write!(formatter, "invalid interaction event: {detail}"),
            Self::ConflictingDedupKey(key) => {
                write!(
                    formatter,
                    "interaction dedup key {key} has conflicting content"
                )
            }
            Self::ConflictingOutput {
                attempt_id,
                stream,
                byte_offset,
            } => write!(
                formatter,
                "attempt {attempt_id} stream {stream} has conflicting output at {byte_offset}"
            ),
            Self::ValueOutOfRange(value) => {
                write!(formatter, "interaction value {value} exceeds SQLite range")
            }
            Self::LockPoisoned => write!(formatter, "interaction store lock poisoned"),
        }
    }
}

impl Error for InteractionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Serialization(error) => Some(error),
            Self::InvalidFrame(_)
            | Self::ConflictingDedupKey(_)
            | Self::ConflictingOutput { .. }
            | Self::ValueOutOfRange(_)
            | Self::LockPoisoned => None,
        }
    }
}

impl From<rusqlite::Error> for InteractionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for InteractionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum AppendOutcome {
    Inserted(EventEnvelope),
    Duplicate(EventEnvelope),
}

impl AppendOutcome {
    #[must_use]
    pub const fn envelope(&self) -> &EventEnvelope {
        match self {
            Self::Inserted(envelope) | Self::Duplicate(envelope) => envelope,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct OutputAppend {
    pub outcome: AppendOutcome,
    pub missing_bytes_before: u64,
}

pub trait InteractionStore: Debug + Send + Sync {
    /// Appends or idempotently replays one canonical event.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, conflicting deduplication content, or storage failure.
    fn append(
        &self,
        dedup_key: &str,
        frame: &ProducerEvent,
    ) -> Result<AppendOutcome, InteractionError>;

    /// Appends one raw-byte-correlated output preview.
    ///
    /// # Errors
    ///
    /// Returns an error for overlapping/conflicting bytes, invalid input, or storage failure.
    fn append_output(
        &self,
        dedup_key: &str,
        attempt_id: &str,
        stream: i32,
        byte_offset: u64,
        payload: &[u8],
        frame: &ProducerEvent,
    ) -> Result<OutputAppend, InteractionError>;

    /// Returns a run's canonical events in sequence order.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted events cannot be read or decoded.
    fn events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError>;
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

impl InteractionStore for SqliteInteractionStore {
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

fn to_i64(value: u64) -> Result<i64, InteractionError> {
    i64::try_from(value).map_err(|_| InteractionError::ValueOutOfRange(value))
}

fn from_i64(value: i64) -> Result<u64, InteractionError> {
    u64::try_from(value).map_err(|_| {
        InteractionError::InvalidFrame(format!("negative stored interaction value {value}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_events::{Authority, OutputStream, Producer, Visibility};

    #[test]
    fn durable_sequence_dedup_conflict_gap_and_restart() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("events.sqlite3");
        let store = SqliteInteractionStore::open(&database)?;
        let start = frame(Event::RunStarted {
            task: "fixture".into(),
        });
        let mut replayed_start = start.clone();
        replayed_start.emitted_at_unix_ms = 99;
        replayed_start.producer.instance = "restarted-server".into();
        assert!(matches!(
            store.append("run:start", &replayed_start)?,
            AppendOutcome::Inserted(_)
        ));
        assert!(matches!(
            store.append("run:start", &start)?,
            AppendOutcome::Duplicate(_)
        ));
        let conflicting = frame(Event::RunStarted {
            task: "changed".into(),
        });
        assert!(matches!(
            store.append("run:start", &conflicting),
            Err(InteractionError::ConflictingDedupKey(_))
        ));

        let output = output_frame(3, "abc");
        let appended = store.append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?;
        assert_eq!(appended.missing_bytes_before, 3);
        assert!(matches!(
            store
                .append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?
                .outcome,
            AppendOutcome::Duplicate(_)
        ));
        assert!(matches!(
            store.append_output("output:3", "attempt-1", 1, 3, b"xyz", &output),
            Err(InteractionError::ConflictingOutput { .. })
        ));
        let overlap = output_frame(4, "overlap");
        assert!(matches!(
            store.append_output("output:4", "attempt-1", 1, 4, b"overlap", &overlap),
            Err(InteractionError::ConflictingOutput { .. })
        ));
        drop(store);

        let reopened = SqliteInteractionStore::open(&database)?;
        assert!(matches!(
            reopened
                .append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?
                .outcome,
            AppendOutcome::Duplicate(_)
        ));
        let events = reopened.events("task-1")?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].sequence, 2);
        Ok(())
    }

    fn frame(event: Event) -> ProducerEvent {
        let mut frame = ProducerEvent::new("task-1", Producer::new("controller", "server"), event);
        frame.task_id = Some("task-1".into());
        frame.authority = Authority::Observed;
        frame.visibility = Visibility::User;
        frame.emitted_at_unix_ms = 1;
        frame
    }

    fn output_frame(byte_offset: u64, text: &str) -> ProducerEvent {
        let mut frame = frame(Event::CommandOutput {
            stream: OutputStream::Stdout,
            byte_offset,
            text: text.into(),
            display_sanitized: false,
        });
        frame.operation_id = Some("attempt-1".into());
        frame
    }
}
