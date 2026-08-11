//! Stateful validation of canonical run event streams.

use crate::{Event, EventEnvelope, SCHEMA_VERSION};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

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
