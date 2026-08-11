//! Versioned interaction events shared by `AlloyPort` executors and renderers.

mod reducer;
mod rendering;

pub use reducer::{ReduceError, RunReducer};
pub use rendering::render_plain;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Producer {
    pub component: String,
    pub instance: String,
}

impl Producer {
    #[must_use]
    pub fn new(component: impl Into<String>, instance: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            instance: instance.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Authority {
    Narrative,
    Reported,
    Observed,
    Verified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    User,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    Assistant,
    User,
    System,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    Binary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileChange {
    pub path: String,
    pub kind: FileChangeKind,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
    pub before_digest: Option<String>,
    pub after_digest: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub digest: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub reference: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Event {
    #[serde(rename = "run.started")]
    RunStarted { task: String },
    #[serde(rename = "turn.started")]
    TurnStarted { turn: u32 },
    #[serde(rename = "turn.completed")]
    TurnCompleted { turn: u32, outcome: String },
    #[serde(rename = "turn.failed")]
    TurnFailed { turn: u32, error: String },
    #[serde(rename = "run.completed")]
    RunCompleted { result: String },
    #[serde(rename = "run.failed")]
    RunFailed { error: String },
    #[serde(rename = "message.started")]
    MessageStarted { role: MessageRole },
    #[serde(rename = "message.delta")]
    MessageDelta { text: String },
    #[serde(rename = "message.completed")]
    MessageCompleted {},
    #[serde(rename = "plan.updated")]
    PlanUpdated { entries: Value },
    #[serde(rename = "tool.started")]
    ToolStarted { name: String, arguments: Value },
    #[serde(rename = "tool.completed")]
    ToolCompleted { name: String, output: String },
    #[serde(rename = "tool.failed")]
    ToolFailed {
        name: String,
        error: String,
        output: Option<String>,
    },
    #[serde(rename = "command.started")]
    CommandStarted {
        command: String,
        cwd: Option<String>,
        execution_site: String,
        description: Option<String>,
    },
    #[serde(rename = "command.output")]
    CommandOutput {
        stream: OutputStream,
        byte_offset: u64,
        text: String,
        display_sanitized: bool,
    },
    #[serde(rename = "command.completed")]
    CommandCompleted {
        exit_code: i32,
        elapsed_ms: u64,
        timed_out: bool,
        output_artifact: Option<ArtifactRef>,
    },
    #[serde(rename = "workspace.delta")]
    WorkspaceDelta {
        changes: Vec<FileChange>,
        diff: Option<String>,
        commit: Option<String>,
    },
    #[serde(rename = "approval.requested")]
    ApprovalRequested {
        action: String,
        reason: String,
        risk: String,
    },
    #[serde(rename = "approval.resolved")]
    ApprovalResolved { decision: String },
    #[serde(rename = "gate.started")]
    GateStarted { gate: String },
    #[serde(rename = "gate.completed")]
    GateCompleted {
        gate: String,
        passed: bool,
        evidence: Vec<ArtifactRef>,
    },
    #[serde(rename = "artifact.produced")]
    ArtifactProduced { artifact: ArtifactRef },
    #[serde(rename = "warning")]
    Warning { message: String },
    #[serde(rename = "error")]
    Error { message: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProducerEvent {
    pub schema_version: u16,
    pub run_id: String,
    pub task_id: Option<String>,
    pub turn_id: Option<String>,
    pub operation_id: Option<String>,
    pub parent_operation_id: Option<String>,
    pub producer_sequence: Option<u64>,
    pub emitted_at_unix_ms: u64,
    pub producer: Producer,
    pub authority: Authority,
    pub visibility: Visibility,
    #[serde(flatten)]
    pub event: Event,
}

impl ProducerEvent {
    #[must_use]
    pub fn new(run_id: impl Into<String>, producer: Producer, event: Event) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            run_id: run_id.into(),
            task_id: None,
            turn_id: None,
            operation_id: None,
            parent_operation_id: None,
            producer_sequence: None,
            emitted_at_unix_ms: unix_time_ms(),
            producer,
            authority: Authority::Observed,
            visibility: Visibility::User,
            event,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: u16,
    pub event_id: String,
    pub run_id: String,
    pub task_id: Option<String>,
    pub turn_id: Option<String>,
    pub operation_id: Option<String>,
    pub parent_operation_id: Option<String>,
    pub producer_sequence: Option<u64>,
    pub sequence: u64,
    pub emitted_at_unix_ms: u64,
    pub producer: Producer,
    pub authority: Authority,
    pub visibility: Visibility,
    #[serde(flatten)]
    pub event: Event,
}

impl EventEnvelope {
    /// Serializes one envelope without a trailing newline.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if an embedded JSON value is invalid.
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Parses one producer JSONL frame.
///
/// # Errors
///
/// Returns a JSON error for malformed or schema-incompatible input.
pub fn producer_event_from_json_line(line: &str) -> Result<ProducerEvent, serde_json::Error> {
    serde_json::from_str(line)
}

#[derive(Clone, Debug)]
pub struct EventSequencer {
    run_id: String,
    next_sequence: u64,
}

impl EventSequencer {
    #[must_use]
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            next_sequence: 1,
        }
    }

    /// Validates and assigns canonical identity and ordering to a producer frame.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when the schema or run identity does not match.
    pub fn ingest(&mut self, frame: ProducerEvent) -> Result<EventEnvelope, ProtocolError> {
        if frame.schema_version != SCHEMA_VERSION {
            return Err(ProtocolError::UnsupportedSchema(frame.schema_version));
        }
        if frame.run_id != self.run_id {
            return Err(ProtocolError::RunMismatch {
                expected: self.run_id.clone(),
                actual: frame.run_id,
            });
        }

        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(EventEnvelope {
            schema_version: SCHEMA_VERSION,
            event_id: format!("{}:{sequence:020}", self.run_id),
            run_id: self.run_id.clone(),
            task_id: frame.task_id,
            turn_id: frame.turn_id,
            operation_id: frame.operation_id,
            parent_operation_id: frame.parent_operation_id,
            producer_sequence: frame.producer_sequence,
            sequence,
            emitted_at_unix_ms: frame.emitted_at_unix_ms,
            producer: frame.producer,
            authority: frame.authority,
            visibility: frame.visibility,
            event: frame.event,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    UnsupportedSchema(u16),
    RunMismatch { expected: String, actual: String },
}

impl Display for ProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(version) => {
                write!(formatter, "unsupported event schema version {version}")
            }
            Self::RunMismatch { expected, actual } => {
                write!(
                    formatter,
                    "event run mismatch: expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl Error for ProtocolError {}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "event_tests.rs"]
mod tests;
