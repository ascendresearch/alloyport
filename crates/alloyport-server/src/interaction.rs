//! Canonical interaction model, application port, and replay-to-live hub.

use alloyport_events::{Event, EventEnvelope, ProducerEvent};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

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
    Storage(Box<dyn Error + Send + Sync>),
    Encoding(Box<dyn Error + Send + Sync>),
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
            Self::Storage(error) => write!(formatter, "interaction storage error: {error}"),
            Self::Encoding(error) => write!(formatter, "interaction encoding error: {error}"),
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
            Self::Storage(error) | Self::Encoding(error) => Some(error.as_ref()),
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
