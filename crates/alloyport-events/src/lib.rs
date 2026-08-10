//! Versioned interaction events shared by `AlloyPort` executors and renderers.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter, Write as _};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationKind {
    Message,
    Tool,
    Command,
    Approval,
    Gate,
}

#[derive(Clone, Debug, Default)]
pub struct RunReducer {
    run_id: Option<String>,
    next_sequence: u64,
    started: bool,
    terminal: bool,
    active_operations: BTreeMap<String, OperationKind>,
}

impl RunReducer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_sequence: 1,
            ..Self::default()
        }
    }

    /// Applies one canonical event and checks lifecycle invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ReduceError`] for sequence gaps, invalid run lifecycle, or malformed operation pairs.
    pub fn apply(&mut self, envelope: &EventEnvelope) -> Result<(), ReduceError> {
        if envelope.schema_version != SCHEMA_VERSION {
            return Err(ReduceError::UnsupportedSchema(envelope.schema_version));
        }
        if envelope.sequence != self.next_sequence {
            return Err(ReduceError::UnexpectedSequence {
                expected: self.next_sequence,
                actual: envelope.sequence,
            });
        }
        if let Some(run_id) = &self.run_id {
            if run_id != &envelope.run_id {
                return Err(ReduceError::RunMismatch);
            }
        } else {
            self.run_id = Some(envelope.run_id.clone());
        }
        if self.terminal {
            return Err(ReduceError::EventAfterTerminal);
        }
        if !self.started && !matches!(&envelope.event, Event::RunStarted { .. }) {
            return Err(ReduceError::RunNotStarted);
        }

        match &envelope.event {
            Event::RunStarted { .. } => {
                if self.started || envelope.sequence != 1 {
                    return Err(ReduceError::InvalidRunStart);
                }
                self.started = true;
            }
            Event::RunCompleted { .. } | Event::RunFailed { .. } => {
                if !self.started {
                    return Err(ReduceError::RunNotStarted);
                }
                if !self.active_operations.is_empty() {
                    return Err(ReduceError::OperationsStillActive(
                        self.active_operations.keys().cloned().collect(),
                    ));
                }
                self.terminal = true;
            }
            Event::MessageStarted { .. } => {
                self.start_operation(envelope, OperationKind::Message)?;
            }
            Event::MessageDelta { .. } => {
                self.require_operation(envelope, OperationKind::Message)?;
            }
            Event::MessageCompleted {} => {
                self.finish_operation(envelope, OperationKind::Message)?;
            }
            Event::ToolStarted { .. } => {
                self.start_operation(envelope, OperationKind::Tool)?;
            }
            Event::ToolCompleted { .. } | Event::ToolFailed { .. } => {
                self.finish_operation(envelope, OperationKind::Tool)?;
            }
            Event::CommandStarted { .. } => {
                self.start_operation(envelope, OperationKind::Command)?;
            }
            Event::CommandOutput { .. } => {
                self.require_operation(envelope, OperationKind::Command)?;
            }
            Event::CommandCompleted { .. } => {
                self.finish_operation(envelope, OperationKind::Command)?;
            }
            Event::ApprovalRequested { .. } => {
                self.start_operation(envelope, OperationKind::Approval)?;
            }
            Event::ApprovalResolved { .. } => {
                self.finish_operation(envelope, OperationKind::Approval)?;
            }
            Event::GateStarted { .. } => {
                self.start_operation(envelope, OperationKind::Gate)?;
            }
            Event::GateCompleted { .. } => {
                self.finish_operation(envelope, OperationKind::Gate)?;
            }
            Event::TurnStarted { .. }
            | Event::TurnCompleted { .. }
            | Event::TurnFailed { .. }
            | Event::PlanUpdated { .. }
            | Event::WorkspaceDelta { .. }
            | Event::ArtifactProduced { .. }
            | Event::Warning { .. }
            | Event::Error { .. } => {}
        }

        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    fn operation_id(envelope: &EventEnvelope) -> Result<&str, ReduceError> {
        envelope
            .operation_id
            .as_deref()
            .ok_or(ReduceError::MissingOperationId)
    }

    fn start_operation(
        &mut self,
        envelope: &EventEnvelope,
        kind: OperationKind,
    ) -> Result<(), ReduceError> {
        let id = Self::operation_id(envelope)?;
        if self.active_operations.insert(id.to_owned(), kind).is_some() {
            return Err(ReduceError::DuplicateOperation(id.to_owned()));
        }
        Ok(())
    }

    fn require_operation(
        &self,
        envelope: &EventEnvelope,
        expected: OperationKind,
    ) -> Result<(), ReduceError> {
        let id = Self::operation_id(envelope)?;
        match self.active_operations.get(id) {
            Some(actual) if *actual == expected => Ok(()),
            _ => Err(ReduceError::OperationNotActive(id.to_owned())),
        }
    }

    fn finish_operation(
        &mut self,
        envelope: &EventEnvelope,
        expected: OperationKind,
    ) -> Result<(), ReduceError> {
        self.require_operation(envelope, expected)?;
        let id = Self::operation_id(envelope)?;
        self.active_operations.remove(id);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReduceError {
    UnsupportedSchema(u16),
    UnexpectedSequence { expected: u64, actual: u64 },
    RunMismatch,
    InvalidRunStart,
    RunNotStarted,
    EventAfterTerminal,
    MissingOperationId,
    DuplicateOperation(String),
    OperationNotActive(String),
    OperationsStillActive(Vec<String>),
}

impl Display for ReduceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(version) => write!(formatter, "unsupported schema {version}"),
            Self::UnexpectedSequence { expected, actual } => {
                write!(formatter, "expected sequence {expected}, got {actual}")
            }
            Self::RunMismatch => write!(formatter, "event belongs to a different run"),
            Self::InvalidRunStart => write!(formatter, "run.started must be the first event"),
            Self::RunNotStarted => write!(formatter, "run has not started"),
            Self::EventAfterTerminal => write!(formatter, "event follows a terminal run event"),
            Self::MissingOperationId => write!(formatter, "operation event has no operation_id"),
            Self::DuplicateOperation(id) => write!(formatter, "operation {id} already exists"),
            Self::OperationNotActive(id) => write!(formatter, "operation {id} is not active"),
            Self::OperationsStillActive(ids) => {
                write!(
                    formatter,
                    "run ended with active operations: {}",
                    ids.join(", ")
                )
            }
        }
    }
}

impl Error for ReduceError {}

#[must_use]
pub fn render_plain(envelope: &EventEnvelope) -> String {
    match &envelope.event {
        Event::RunStarted { task } => format!("AlloyPort · {task}\n"),
        Event::TurnStarted { .. } | Event::TurnCompleted { .. } => String::new(),
        Event::TurnFailed { turn, error } => format!("! turn {turn} failed: {error}\n"),
        Event::RunCompleted { .. } => "\n✓ run completed\n".to_owned(),
        Event::RunFailed { error } => format!("\n✗ run failed: {error}\n"),
        Event::MessageStarted { role } => format!("\n{}\n", message_role_label(*role)),
        Event::MessageDelta { text } => text.clone(),
        Event::MessageCompleted {} => "\n".to_owned(),
        Event::PlanUpdated { entries } => format!("plan: {entries}\n"),
        Event::ToolStarted { name, arguments } => format!("\n→ {name} {arguments}\n"),
        Event::ToolCompleted { name, output } => format!("← {name} completed\n{output}\n"),
        Event::ToolFailed {
            name,
            error,
            output,
        } => format!(
            "← {name} failed: {error}{}\n",
            output
                .as_ref()
                .map_or_else(String::new, |value| format!("\n{value}"))
        ),
        Event::CommandStarted {
            command,
            cwd,
            execution_site,
            description,
        } => {
            let description = description
                .as_ref()
                .map_or_else(String::new, |value| format!(" · {value}"));
            let cwd = cwd
                .as_ref()
                .map_or_else(String::new, |value| format!(" · cwd {value}"));
            format!("\n$ {command}\n  @ {execution_site}{cwd}{description}\n")
        }
        Event::CommandOutput { stream, text, .. } => match stream {
            OutputStream::Stdout => text.clone(),
            OutputStream::Stderr => format!("[stderr]\n{text}"),
        },
        Event::CommandCompleted {
            exit_code,
            elapsed_ms,
            timed_out,
            ..
        } => format!(
            "\n  exit {exit_code} · {elapsed_ms} ms{}\n",
            if *timed_out { " · timed out" } else { "" }
        ),
        Event::WorkspaceDelta {
            changes,
            diff,
            commit,
        } => render_workspace_delta(changes, diff.as_deref(), commit.as_deref()),
        Event::ApprovalRequested {
            action,
            reason,
            risk,
        } => format!("approval required [{risk}]: {action}\n  {reason}\n"),
        Event::ApprovalResolved { decision } => format!("approval: {decision}\n"),
        Event::GateStarted { gate } => format!("gate {gate}: running\n"),
        Event::GateCompleted { gate, passed, .. } => format!(
            "{} gate {gate}: {}\n",
            if *passed { "✓" } else { "✗" },
            if *passed { "PASS" } else { "FAIL" }
        ),
        Event::ArtifactProduced { artifact } => {
            format!("artifact {} ({})\n", artifact.reference, artifact.digest)
        }
        Event::Warning { message } => format!("! {message}\n"),
        Event::Error { message } => format!("✗ {message}\n"),
    }
}

fn render_workspace_delta(
    changes: &[FileChange],
    diff: Option<&str>,
    commit: Option<&str>,
) -> String {
    let mut rendered = String::from("\nworkspace changes");
    if let Some(commit) = commit {
        let _ = write!(rendered, " · commit {commit}");
    }
    rendered.push('\n');
    for change in changes {
        let additions = change
            .additions
            .map_or_else(|| "?".to_owned(), |value| value.to_string());
        let deletions = change
            .deletions
            .map_or_else(|| "?".to_owned(), |value| value.to_string());
        let _ = writeln!(
            rendered,
            "  {:?} {} +{additions}/-{deletions}",
            change.kind, change.path
        );
    }
    if let Some(diff) = diff {
        rendered.push_str(diff);
        if !diff.ends_with('\n') {
            rendered.push('\n');
        }
    }
    rendered
}

const fn message_role_label(role: MessageRole) -> &'static str {
    match role {
        MessageRole::Assistant => "assistant",
        MessageRole::User => "user",
        MessageRole::System => "system",
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(run_id: &str, event: Event) -> ProducerEvent {
        ProducerEvent::new(run_id, Producer::new("test", "one"), event)
    }

    fn ingest(
        sequencer: &mut EventSequencer,
        reducer: &mut RunReducer,
        frame: ProducerEvent,
    ) -> EventEnvelope {
        let envelope = sequencer.ingest(frame).expect("valid producer frame");
        reducer.apply(&envelope).expect("valid event lifecycle");
        envelope
    }

    #[test]
    fn jsonl_uses_stable_type_and_escapes_embedded_newlines() {
        let mut sequencer = EventSequencer::new("run-1");
        let envelope = sequencer
            .ingest(frame(
                "run-1",
                Event::Warning {
                    message: "first\nsecond".to_owned(),
                },
            ))
            .expect("frame is accepted");
        let line = envelope.to_json_line().expect("serializable event");

        assert!(line.contains("\"type\":\"warning\""));
        assert!(line.contains("first\\nsecond"));
        assert_eq!(line.lines().count(), 1);
    }

    #[test]
    fn reducer_accepts_nested_tool_and_command_lifecycles() {
        let mut sequencer = EventSequencer::new("run-1");
        let mut reducer = RunReducer::new();
        ingest(
            &mut sequencer,
            &mut reducer,
            frame(
                "run-1",
                Event::RunStarted {
                    task: "port extension".to_owned(),
                },
            ),
        );

        let mut tool = frame(
            "run-1",
            Event::ToolStarted {
                name: "verify".to_owned(),
                arguments: Value::Null,
            },
        );
        tool.operation_id = Some("tool-1".to_owned());
        ingest(&mut sequencer, &mut reducer, tool);

        let mut command = frame(
            "run-1",
            Event::CommandStarted {
                command: "cargo test".to_owned(),
                cwd: Some("/work".to_owned()),
                execution_site: "local".to_owned(),
                description: None,
            },
        );
        command.operation_id = Some("command-1".to_owned());
        command.parent_operation_id = Some("tool-1".to_owned());
        ingest(&mut sequencer, &mut reducer, command);

        let mut command_done = frame(
            "run-1",
            Event::CommandCompleted {
                exit_code: 0,
                elapsed_ms: 12,
                timed_out: false,
                output_artifact: None,
            },
        );
        command_done.operation_id = Some("command-1".to_owned());
        ingest(&mut sequencer, &mut reducer, command_done);

        let mut tool_done = frame(
            "run-1",
            Event::ToolCompleted {
                name: "verify".to_owned(),
                output: "PASS".to_owned(),
            },
        );
        tool_done.operation_id = Some("tool-1".to_owned());
        ingest(&mut sequencer, &mut reducer, tool_done);
    }

    #[test]
    fn reducer_rejects_command_output_without_a_start() {
        let mut sequencer = EventSequencer::new("run-1");
        let mut reducer = RunReducer::new();
        ingest(
            &mut sequencer,
            &mut reducer,
            frame(
                "run-1",
                Event::RunStarted {
                    task: "port extension".to_owned(),
                },
            ),
        );

        let mut output = frame(
            "run-1",
            Event::CommandOutput {
                stream: OutputStream::Stdout,
                byte_offset: 0,
                text: "oops".to_owned(),
                display_sanitized: false,
            },
        );
        output.operation_id = Some("missing".to_owned());
        let envelope = sequencer.ingest(output).expect("protocol frame is valid");

        assert_eq!(
            reducer.apply(&envelope),
            Err(ReduceError::OperationNotActive("missing".to_owned()))
        );
    }

    #[test]
    fn reducer_rejects_terminal_run_with_an_active_operation() {
        let mut sequencer = EventSequencer::new("run-1");
        let mut reducer = RunReducer::new();
        ingest(
            &mut sequencer,
            &mut reducer,
            frame(
                "run-1",
                Event::RunStarted {
                    task: "port extension".to_owned(),
                },
            ),
        );
        let mut message = frame(
            "run-1",
            Event::MessageStarted {
                role: MessageRole::Assistant,
            },
        );
        message.operation_id = Some("message-1".to_owned());
        ingest(&mut sequencer, &mut reducer, message);
        let terminal = sequencer
            .ingest(frame(
                "run-1",
                Event::RunCompleted {
                    result: "too early".to_owned(),
                },
            ))
            .expect("protocol frame is valid");

        assert_eq!(
            reducer.apply(&terminal),
            Err(ReduceError::OperationsStillActive(vec![
                "message-1".to_owned()
            ]))
        );
    }

    #[test]
    fn plain_renderer_shows_command_and_diff_evidence() {
        let mut sequencer = EventSequencer::new("run-1");
        let mut command = frame(
            "run-1",
            Event::CommandStarted {
                command: "python verify.py".to_owned(),
                cwd: Some("/work/demo".to_owned()),
                execution_site: "ascend-worker-2".to_owned(),
                description: Some("run correctness oracle".to_owned()),
            },
        );
        command.operation_id = Some("command-1".to_owned());
        let command = sequencer.ingest(command).expect("valid command");
        assert!(render_plain(&command).contains("ascend-worker-2"));

        let delta = sequencer
            .ingest(frame(
                "run-1",
                Event::WorkspaceDelta {
                    changes: vec![FileChange {
                        path: "src/kernel.cpp".to_owned(),
                        kind: FileChangeKind::Modified,
                        additions: Some(2),
                        deletions: Some(1),
                        before_digest: None,
                        after_digest: None,
                    }],
                    diff: Some("@@ -1 +1 @@\n-old\n+new\n".to_owned()),
                    commit: Some("abc123".to_owned()),
                },
            ))
            .expect("valid delta");
        let rendered = render_plain(&delta);
        assert!(rendered.contains("src/kernel.cpp +2/-1"));
        assert!(rendered.contains("-old\n+new"));
    }
}
