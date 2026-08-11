//! Durable canonical interaction events translated from worker observations.

use alloyport_events::{Event, EventEnvelope, ProducerEvent, SCHEMA_VERSION};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

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

/// Applies the controller's fail-closed display policy before an observed worker event is persisted.
pub(crate) fn redact_worker_event(event: &mut Event) {
    match event {
        Event::CommandStarted {
            command,
            cwd,
            description,
            ..
        } => {
            *command = sanitize_display_text(command);
            if let Some(cwd) = cwd {
                *cwd = strip_terminal_sequences(cwd);
            }
            if let Some(description) = description {
                *description = sanitize_display_text(description);
            }
        }
        Event::CommandOutput {
            text,
            display_sanitized,
            ..
        } => {
            *text = sanitize_display_text(text);
            *display_sanitized = true;
        }
        Event::Warning { message } | Event::Error { message } => {
            *message = sanitize_display_text(message);
        }
        _ => {}
    }
}

fn sanitize_display_text(input: &str) -> String {
    let stripped = strip_terminal_sequences(input);
    let mut output = String::with_capacity(stripped.len());
    let mut redact_next = false;
    for segment in stripped.split_inclusive(char::is_whitespace) {
        let word = segment.trim_end_matches(char::is_whitespace);
        let whitespace = &segment[word.len()..];
        if word.is_empty() {
            output.push_str(segment);
            continue;
        }
        if redact_next {
            output.push_str("[REDACTED]");
            redact_next = false;
        } else if word.eq_ignore_ascii_case("bearer") {
            output.push_str(word);
            redact_next = true;
        } else if let Some((key, _)) = word.split_once('=') {
            if is_sensitive_key(key) {
                output.push_str(key);
                output.push_str("=[REDACTED]");
            } else {
                output.push_str(word);
            }
        } else {
            output.push_str(word);
            redact_next = looks_like_sensitive_label(word);
        }
        output.push_str(whitespace);
    }
    output
}

fn looks_like_sensitive_label(value: &str) -> bool {
    is_sensitive_key(value)
        && (value.starts_with('-')
            || value.ends_with(':')
            || value
                .chars()
                .all(|character| character.is_ascii_uppercase() || character == '_'))
}

fn is_sensitive_key(value: &str) -> bool {
    let normalized = value
        .trim_matches(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .to_ascii_lowercase();
    [
        "password",
        "passwd",
        "token",
        "secret",
        "api_key",
        "apikey",
        "access_key",
        "private_key",
        "authorization",
        "cookie",
    ]
    .iter()
    .any(|sensitive| normalized.contains(sensitive))
}

fn strip_terminal_sequences(input: &str) -> String {
    #[derive(Clone, Copy)]
    enum State {
        Text,
        Escape,
        ControlSequence,
        OperatingSystemCommand,
        OperatingSystemCommandEscape,
    }

    let mut state = State::Text;
    let mut output = String::with_capacity(input.len());
    for character in input.chars() {
        state = match state {
            State::Text if character == '\u{1b}' => State::Escape,
            State::Text => {
                if !character.is_control() || matches!(character, '\n' | '\t') {
                    output.push(character);
                }
                State::Text
            }
            State::Escape if character == '[' => State::ControlSequence,
            State::Escape if character == ']' => State::OperatingSystemCommand,
            State::Escape => State::Text,
            State::ControlSequence if ('@'..='~').contains(&character) => State::Text,
            State::ControlSequence => State::ControlSequence,
            State::OperatingSystemCommand if character == '\u{7}' => State::Text,
            State::OperatingSystemCommandEscape if character == '\\' => State::Text,
            State::OperatingSystemCommand | State::OperatingSystemCommandEscape
                if character == '\u{1b}' =>
            {
                State::OperatingSystemCommandEscape
            }
            State::OperatingSystemCommand | State::OperatingSystemCommandEscape => {
                State::OperatingSystemCommand
            }
        };
    }
    output
}

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
    InvalidCursor {
        run_id: String,
        after_sequence: u64,
        latest_sequence: u64,
    },
    RevokedRunGrant {
        run_id: String,
        owner_id: String,
    },
    MissingRunGrant {
        run_id: String,
        owner_id: String,
    },
    ValueOutOfRange(u64),
    InvalidSubscriptionCapacity,
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
            Self::InvalidCursor {
                run_id,
                after_sequence,
                latest_sequence,
            } => write!(
                formatter,
                "interaction cursor {after_sequence} is beyond run {run_id} high-water mark {latest_sequence}"
            ),
            Self::RevokedRunGrant { run_id, owner_id } => write!(
                formatter,
                "interaction access for owner {owner_id} to run {run_id} was revoked"
            ),
            Self::MissingRunGrant { run_id, owner_id } => write!(
                formatter,
                "interaction access for owner {owner_id} to run {run_id} does not exist"
            ),
            Self::ValueOutOfRange(value) => {
                write!(formatter, "interaction value {value} exceeds SQLite range")
            }
            Self::InvalidSubscriptionCapacity => {
                write!(
                    formatter,
                    "interaction subscription capacity must be positive"
                )
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
            | Self::InvalidCursor { .. }
            | Self::RevokedRunGrant { .. }
            | Self::MissingRunGrant { .. }
            | Self::ValueOutOfRange(_)
            | Self::InvalidSubscriptionCapacity
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunGrantOutcome {
    Granted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunRevokeOutcome {
    Revoked,
    Duplicate,
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

    /// Returns at most `limit` events strictly after one canonical sequence.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted events cannot be read or decoded.
    fn events_after(
        &self,
        run_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>, InteractionError>;

    /// Returns the greatest canonical sequence currently retained for one run.
    ///
    /// # Errors
    ///
    /// Returns an error if the durable cursor cannot be read.
    fn latest_sequence(&self, run_id: &str) -> Result<Option<u64>, InteractionError>;

    /// Grants one stable owner read access to a run. Revoked grant identities cannot be reused.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, storage failure, or a terminally revoked grant.
    fn grant_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunGrantOutcome, InteractionError>;

    /// Revokes one existing run grant idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error if the grant does not exist or cannot be durably updated.
    fn revoke_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunRevokeOutcome, InteractionError>;

    /// Checks one stable owner's active run access without revealing event contents.
    ///
    /// # Errors
    ///
    /// Returns an error if authorization state cannot be read.
    fn can_read_run(&self, run_id: &str, owner_id: &str) -> Result<bool, InteractionError>;
}

/// Append-through store that publishes only newly inserted canonical envelopes.
#[derive(Debug)]
pub struct InteractionHub {
    store: Arc<dyn InteractionStore>,
    live: Mutex<BTreeMap<String, broadcast::Sender<EventEnvelope>>>,
    live_capacity: usize,
    replay_batch_size: usize,
}

impl InteractionHub {
    /// Creates a bounded notification hub over one authoritative durable store.
    ///
    /// # Errors
    ///
    /// Returns an error when either bound is zero.
    pub fn new(
        store: Arc<dyn InteractionStore>,
        live_capacity: usize,
        replay_batch_size: usize,
    ) -> Result<Self, InteractionError> {
        if live_capacity == 0 || replay_batch_size == 0 {
            return Err(InteractionError::InvalidSubscriptionCapacity);
        }
        Ok(Self {
            store,
            live: Mutex::new(BTreeMap::new()),
            live_capacity,
            replay_batch_size,
        })
    }

    /// Opens a run-scoped stream after the client's last applied canonical sequence.
    ///
    /// The receiver is attached before the durable high-water mark is read, closing the race
    /// between historical replay and live notification delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable high-water mark cannot be read.
    pub fn subscribe(
        &self,
        run_id: impl Into<String>,
        after_sequence: u64,
    ) -> Result<InteractionSubscription, InteractionError> {
        let run_id = run_id.into();
        let receiver = self.receiver(&run_id);
        let replay_through = self.store.latest_sequence(&run_id)?.unwrap_or(0);
        if after_sequence > replay_through {
            return Err(InteractionError::InvalidCursor {
                run_id,
                after_sequence,
                latest_sequence: replay_through,
            });
        }
        Ok(InteractionSubscription {
            store: Arc::clone(&self.store),
            receiver,
            run_id,
            last_sequence: after_sequence,
            replay_through,
            replay_batch_size: self.replay_batch_size,
            replay: VecDeque::new(),
            terminated: false,
        })
    }

    fn publish(&self, outcome: &AppendOutcome) {
        if let AppendOutcome::Inserted(envelope) = outcome {
            let live = self
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(sender) = live.get(&envelope.run_id) {
                let _ = sender.send(envelope.clone());
            }
        }
    }

    fn receiver(&self, run_id: &str) -> broadcast::Receiver<EventEnvelope> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.entry(run_id.to_owned())
            .or_insert_with(|| broadcast::channel(self.live_capacity).0)
            .subscribe()
    }
}

impl InteractionStore for InteractionHub {
    fn append(
        &self,
        dedup_key: &str,
        frame: &ProducerEvent,
    ) -> Result<AppendOutcome, InteractionError> {
        let outcome = self.store.append(dedup_key, frame)?;
        self.publish(&outcome);
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
        let appended =
            self.store
                .append_output(dedup_key, attempt_id, stream, byte_offset, payload, frame)?;
        self.publish(&appended.outcome);
        Ok(appended)
    }

    fn events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError> {
        self.store.events(run_id)
    }

    fn events_after(
        &self,
        run_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>, InteractionError> {
        self.store.events_after(run_id, after_sequence, limit)
    }

    fn latest_sequence(&self, run_id: &str) -> Result<Option<u64>, InteractionError> {
        self.store.latest_sequence(run_id)
    }

    fn grant_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunGrantOutcome, InteractionError> {
        self.store.grant_run_access(run_id, owner_id, now_ms)
    }

    fn revoke_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        self.store.revoke_run_access(run_id, owner_id, now_ms)
    }

    fn can_read_run(&self, run_id: &str, owner_id: &str) -> Result<bool, InteractionError> {
        self.store.can_read_run(run_id, owner_id)
    }
}

/// Terminal reason for a live subscription.
#[derive(Debug)]
pub enum SubscriptionError {
    Store(InteractionError),
    SlowConsumer {
        last_sequence: u64,
        skipped_notifications: u64,
    },
    SequenceGap {
        expected_sequence: u64,
        observed_sequence: u64,
    },
    Closed,
}

impl Display for SubscriptionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => Display::fmt(error, formatter),
            Self::SlowConsumer {
                last_sequence,
                skipped_notifications,
            } => write!(
                formatter,
                "interaction subscriber after sequence {last_sequence} fell behind by {skipped_notifications} notifications"
            ),
            Self::SequenceGap {
                expected_sequence,
                observed_sequence,
            } => write!(
                formatter,
                "interaction subscriber expected sequence {expected_sequence} but observed {observed_sequence}"
            ),
            Self::Closed => write!(formatter, "interaction subscription is closed"),
        }
    }
}

impl Error for SubscriptionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::SlowConsumer { .. } | Self::SequenceGap { .. } | Self::Closed => None,
        }
    }
}

/// One run-scoped replay-to-live cursor.
#[derive(Debug)]
pub struct InteractionSubscription {
    store: Arc<dyn InteractionStore>,
    receiver: broadcast::Receiver<EventEnvelope>,
    run_id: String,
    last_sequence: u64,
    replay_through: u64,
    replay_batch_size: usize,
    replay: VecDeque<EventEnvelope>,
    terminated: bool,
}

impl InteractionSubscription {
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Returns the next canonical envelope, replaying the durable snapshot before live delivery.
    ///
    /// # Errors
    ///
    /// Terminates explicitly on durable gaps, storage failure, or bounded-channel lag. A client can
    /// reconnect after [`Self::last_sequence`] to resume from authoritative storage.
    pub async fn recv(&mut self) -> Result<EventEnvelope, SubscriptionError> {
        if self.terminated {
            return Err(SubscriptionError::Closed);
        }
        loop {
            if let Some(envelope) = self.replay.pop_front() {
                return self.accept(envelope);
            }
            if self.last_sequence < self.replay_through {
                let events = self
                    .store
                    .events_after(&self.run_id, self.last_sequence, self.replay_batch_size)
                    .map_err(|error| self.terminate(SubscriptionError::Store(error)))?;
                self.replay.extend(
                    events
                        .into_iter()
                        .take_while(|event| event.sequence <= self.replay_through),
                );
                if self.replay.is_empty() {
                    let expected_sequence = self.last_sequence.saturating_add(1);
                    return Err(self.terminate(SubscriptionError::SequenceGap {
                        expected_sequence,
                        observed_sequence: self.replay_through,
                    }));
                }
                continue;
            }
            match self.receiver.recv().await {
                Ok(envelope) if envelope.run_id != self.run_id => {}
                Ok(envelope) if envelope.sequence <= self.last_sequence => {}
                Ok(envelope) => return self.accept(envelope),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(self.terminate(SubscriptionError::SlowConsumer {
                        last_sequence: self.last_sequence,
                        skipped_notifications: skipped,
                    }));
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(self.terminate(SubscriptionError::Closed));
                }
            }
        }
    }

    fn accept(&mut self, envelope: EventEnvelope) -> Result<EventEnvelope, SubscriptionError> {
        let expected_sequence = self.last_sequence.saturating_add(1);
        if envelope.sequence != expected_sequence {
            return Err(self.terminate(SubscriptionError::SequenceGap {
                expected_sequence,
                observed_sequence: envelope.sequence,
            }));
        }
        self.last_sequence = envelope.sequence;
        Ok(envelope)
    }

    fn terminate(&mut self, error: SubscriptionError) -> SubscriptionError {
        self.terminated = true;
        error
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

    fn grant_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunGrantOutcome, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(state) = transaction
            .query_row(
                "SELECT state FROM interaction_run_grants WHERE run_id = ?1 AND owner_id = ?2",
                params![run_id, owner_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            if state == 1 {
                transaction.commit()?;
                return Ok(RunGrantOutcome::Duplicate);
            }
            if state == 2 {
                return Err(InteractionError::RevokedRunGrant {
                    run_id: run_id.into(),
                    owner_id: owner_id.into(),
                });
            }
            return Err(InteractionError::InvalidFrame(format!(
                "run grant has unknown state {state}"
            )));
        }
        transaction.execute(
            "INSERT INTO interaction_run_grants(
                run_id, owner_id, state, granted_at_ms
             ) VALUES (?1, ?2, 1, ?3)",
            params![run_id, owner_id, to_i64(now_ms)?],
        )?;
        transaction.commit()?;
        Ok(RunGrantOutcome::Granted)
    }

    fn revoke_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = transaction
            .query_row(
                "SELECT state FROM interaction_run_grants WHERE run_id = ?1 AND owner_id = ?2",
                params![run_id, owner_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| InteractionError::MissingRunGrant {
                run_id: run_id.into(),
                owner_id: owner_id.into(),
            })?;
        if state == 2 {
            transaction.commit()?;
            return Ok(RunRevokeOutcome::Duplicate);
        }
        if state != 1 {
            return Err(InteractionError::InvalidFrame(format!(
                "run grant has unknown state {state}"
            )));
        }
        transaction.execute(
            "UPDATE interaction_run_grants
             SET state = 2, revoked_at_ms = ?3
             WHERE run_id = ?1 AND owner_id = ?2",
            params![run_id, owner_id, to_i64(now_ms)?],
        )?;
        transaction.commit()?;
        Ok(RunRevokeOutcome::Revoked)
    }

    fn can_read_run(&self, run_id: &str, owner_id: &str) -> Result<bool, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let database = self.connection()?;
        Ok(database
            .query_row(
                "SELECT state FROM interaction_run_grants WHERE run_id = ?1 AND owner_id = ?2",
                params![run_id, owner_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            == Some(1))
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

fn validate_run_owner(run_id: &str, owner_id: &str) -> Result<(), InteractionError> {
    if run_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run grant identity is missing".into(),
        ));
    }
    if owner_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run grant owner is missing".into(),
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

fn usize_to_i64(value: usize) -> Result<i64, InteractionError> {
    i64::try_from(value).map_err(|_| InteractionError::ValueOutOfRange(u64::MAX))
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
        assert_eq!(reopened.latest_sequence("task-1")?, Some(2));
        assert_eq!(
            reopened
                .events_after("task-1", 1, 1)?
                .into_iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(reopened.events_after("task-1", 0, 0)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn subscription_replays_then_crosses_to_live_without_a_gap() -> Result<(), Box<dyn Error>>
    {
        let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
        let hub = InteractionHub::new(durable, 4, 1)?;
        hub.append(
            "run:start",
            &frame(Event::RunStarted {
                task: "fixture".into(),
            }),
        )?;

        let mut subscription = hub.subscribe("task-1", 0)?;
        hub.append(
            "warning:live",
            &frame(Event::Warning {
                message: "arrived while replay was pending".into(),
            }),
        )?;

        assert_eq!(subscription.recv().await?.sequence, 1);
        assert_eq!(subscription.recv().await?.sequence, 2);
        assert_eq!(subscription.last_sequence(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn slow_subscriber_terminates_and_reconnects_from_durable_cursor()
    -> Result<(), Box<dyn Error>> {
        let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
        let hub = InteractionHub::new(durable, 2, 1)?;
        let mut slow = hub.subscribe("task-1", 0)?;
        for sequence in 1..=3 {
            hub.append(
                &format!("warning:{sequence}"),
                &frame(Event::Warning {
                    message: format!("warning {sequence}"),
                }),
            )?;
        }

        assert!(matches!(
            slow.recv().await,
            Err(SubscriptionError::SlowConsumer {
                last_sequence: 0,
                skipped_notifications: 1
            })
        ));
        assert!(matches!(slow.recv().await, Err(SubscriptionError::Closed)));

        let mut resumed = hub.subscribe("task-1", slow.last_sequence())?;
        for expected in 1..=3 {
            assert_eq!(resumed.recv().await?.sequence, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn unrelated_run_pressure_does_not_lag_subscriber() -> Result<(), Box<dyn Error>> {
        let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
        let hub = InteractionHub::new(durable, 1, 1)?;
        let mut subscription = hub.subscribe("task-1", 0)?;
        for sequence in 1..=3 {
            hub.append(
                &format!("other:{sequence}"),
                &frame_for(
                    "task-2",
                    Event::Warning {
                        message: format!("unrelated {sequence}"),
                    },
                ),
            )?;
        }
        hub.append(
            "task-1:warning",
            &frame(Event::Warning {
                message: "relevant".into(),
            }),
        )?;

        let envelope = subscription.recv().await?;
        assert_eq!(envelope.run_id, "task-1");
        assert_eq!(envelope.sequence, 1);
        Ok(())
    }

    #[test]
    fn subscription_rejects_cursor_beyond_durable_high_water() -> Result<(), Box<dyn Error>> {
        let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
        let hub = InteractionHub::new(durable, 2, 1)?;
        assert!(matches!(
            hub.subscribe("task-1", 1),
            Err(InteractionError::InvalidCursor {
                after_sequence: 1,
                latest_sequence: 0,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn run_grants_are_durable_idempotent_and_revocation_is_terminal() -> Result<(), Box<dyn Error>>
    {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("events.sqlite3");
        let store = SqliteInteractionStore::open(&database)?;
        assert_eq!(
            store.grant_run_access("task-1", "owner-a", 1)?,
            RunGrantOutcome::Granted
        );
        assert_eq!(
            store.grant_run_access("task-1", "owner-a", 99)?,
            RunGrantOutcome::Duplicate
        );
        assert!(store.can_read_run("task-1", "owner-a")?);
        assert!(!store.can_read_run("task-1", "owner-b")?);
        assert_eq!(
            store.revoke_run_access("task-1", "owner-a", 2)?,
            RunRevokeOutcome::Revoked
        );
        assert_eq!(
            store.revoke_run_access("task-1", "owner-a", 3)?,
            RunRevokeOutcome::Duplicate
        );
        assert!(!store.can_read_run("task-1", "owner-a")?);
        assert!(matches!(
            store.grant_run_access("task-1", "owner-a", 4),
            Err(InteractionError::RevokedRunGrant { .. })
        ));
        drop(store);

        let reopened = SqliteInteractionStore::open(database)?;
        assert!(!reopened.can_read_run("task-1", "owner-a")?);
        assert_eq!(
            reopened.grant_run_access("task-1", "owner-b", 5)?,
            RunGrantOutcome::Granted
        );
        assert!(reopened.can_read_run("task-1", "owner-b")?);
        Ok(())
    }

    #[test]
    fn controller_redaction_strips_terminal_controls_and_common_credentials() {
        let mut event = Event::CommandOutput {
            stream: OutputStream::Stdout,
            byte_offset: 0,
            text: "\u{1b}[31mTOKEN=top-secret\u{1b}[0m\nBearer credential\nordinary secret text"
                .into(),
            display_sanitized: false,
        };
        redact_worker_event(&mut event);
        assert_eq!(
            event,
            Event::CommandOutput {
                stream: OutputStream::Stdout,
                byte_offset: 0,
                text: "TOKEN=[REDACTED]\nBearer [REDACTED]\nordinary secret text".into(),
                display_sanitized: true,
            }
        );
    }

    fn frame(event: Event) -> ProducerEvent {
        frame_for("task-1", event)
    }

    fn frame_for(run_id: &str, event: Event) -> ProducerEvent {
        let mut frame = ProducerEvent::new(run_id, Producer::new("controller", "server"), event);
        frame.task_id = Some(run_id.into());
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
