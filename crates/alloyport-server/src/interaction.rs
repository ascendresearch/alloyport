//! Canonical interaction model and application persistence port.

mod hub;
mod sanitizer;

pub use hub::{InteractionHub, InteractionSubscription, SubscriptionError};
pub(crate) use sanitizer::redact_worker_event;

use alloyport_events::{EventEnvelope, ProducerEvent};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

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

pub trait InteractionEventWriter: Debug + Send + Sync {
    /// Appends or idempotently replays one canonical event.
    ///
    /// Deduplication identity is scoped to the run. Producer instance, producer sequence, and
    /// emission time may change across a replay; conflicting canonical content must be rejected.
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
    /// Output correlation is independently scoped by attempt, stream, and raw byte offset. Exact
    /// duplicates are idempotent, forward gaps are reported, and overlap is a conflict.
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
}

pub trait InteractionEventReader: Debug + Send + Sync {
    /// Returns a run's canonical events in sequence order.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted events cannot be read or decoded.
    fn events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError>;

    /// Returns at most `limit` events strictly after one canonical sequence.
    /// Results are ordered by increasing run-local sequence; a zero limit returns no events.
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
}

pub trait InteractionRunAccessStore: Debug + Send + Sync {
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

/// Compatibility composition for services that need event I/O and run authorization together.
pub trait InteractionStore:
    InteractionEventWriter + InteractionEventReader + InteractionRunAccessStore
{
}

impl<T> InteractionStore for T where
    T: InteractionEventWriter + InteractionEventReader + InteractionRunAccessStore + ?Sized
{
}
