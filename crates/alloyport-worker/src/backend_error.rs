//! Stable execution-backend failure categories.

use crate::WorkerError;
use crate::executor::ExecutionRuntimeError;
use crate::journal::AttemptStoreError;
use alloyport_artifacts::ArtifactStoreError;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Policy-relevant classification retained across pluggable execution backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendFailureClass {
    Retryable,
    Terminal,
    Policy,
    Integrity,
}

/// Adapter-independent execution-backend failure.
///
/// Backend implementations must classify failures at their boundary. Coordinators can then make
/// retry, rejection, quarantine, and observability decisions without parsing error text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendError {
    Retryable(String),
    Terminal(String),
    Policy(String),
    Integrity(String),
}

impl BackendError {
    #[must_use]
    pub const fn class(&self) -> BackendFailureClass {
        match self {
            Self::Retryable(_) => BackendFailureClass::Retryable,
            Self::Terminal(_) => BackendFailureClass::Terminal,
            Self::Policy(_) => BackendFailureClass::Policy,
            Self::Integrity(_) => BackendFailureClass::Integrity,
        }
    }

    #[must_use]
    pub fn retryable(detail: impl Into<String>) -> Self {
        Self::Retryable(detail.into())
    }

    #[must_use]
    pub fn terminal(detail: impl Into<String>) -> Self {
        Self::Terminal(detail.into())
    }

    #[must_use]
    pub fn policy(detail: impl Into<String>) -> Self {
        Self::Policy(detail.into())
    }

    #[must_use]
    pub fn integrity(detail: impl Into<String>) -> Self {
        Self::Integrity(detail.into())
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Retryable(detail)
            | Self::Terminal(detail)
            | Self::Policy(detail)
            | Self::Integrity(detail) => detail,
        }
    }

    #[must_use]
    pub fn with_context(self, context: impl Display) -> Self {
        let detail = format!("{context}: {}", self.detail());
        match self {
            Self::Retryable(_) => Self::Retryable(detail),
            Self::Terminal(_) => Self::Terminal(detail),
            Self::Policy(_) => Self::Policy(detail),
            Self::Integrity(_) => Self::Integrity(detail),
        }
    }
}

impl Display for BackendError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::Retryable(_) => "retryable backend failure",
            Self::Terminal(_) => "terminal backend failure",
            Self::Policy(_) => "backend policy rejection",
            Self::Integrity(_) => "backend integrity failure",
        };
        write!(formatter, "{category}: {}", self.detail())
    }
}

impl Error for BackendError {}

impl From<ExecutionRuntimeError> for BackendError {
    fn from(error: ExecutionRuntimeError) -> Self {
        match error {
            ExecutionRuntimeError::Worker(error) => Self::from(error),
            ExecutionRuntimeError::Artifact(error) => {
                let detail = error.to_string();
                match error {
                    ArtifactStoreError::Io { .. } => Self::retryable(detail),
                    ArtifactStoreError::SizeLimitExceeded { .. } => Self::policy(detail),
                    ArtifactStoreError::SizeMismatch { .. }
                    | ArtifactStoreError::DigestMismatch { .. }
                    | ArtifactStoreError::IntegrityViolation { .. } => Self::integrity(detail),
                }
            }
            ExecutionRuntimeError::ArtifactInput(error) => Self::from(error),
            ExecutionRuntimeError::Serialization(error) => Self::terminal(error.to_string()),
            ExecutionRuntimeError::Backend(error) => error,
            ExecutionRuntimeError::ArtifactPublication(error) => Self::from(error),
            ExecutionRuntimeError::CleanupAfterCommit(detail) => Self::retryable(detail),
            ExecutionRuntimeError::InvalidConfiguration(detail) => Self::policy(detail),
            ExecutionRuntimeError::AttemptAlreadyRunning(attempt) => {
                Self::retryable(format!("attempt {attempt} already has an executor"))
            }
            ExecutionRuntimeError::MissingAttempt(attempt) => {
                Self::terminal(format!("attempt {attempt} is not admitted"))
            }
            ExecutionRuntimeError::MissingTerminalData(attempt) => {
                Self::terminal(format!("finished attempt {attempt} lacks terminal data"))
            }
            ExecutionRuntimeError::TaskJoin(error) => Self::retryable(error.to_string()),
        }
    }
}

impl From<WorkerError> for BackendError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::InvalidHello(error) | WorkerError::InvalidAssignment(error) => {
                Self::policy(error.to_string())
            }
            WorkerError::ConflictingAttempt(attempt) => {
                Self::integrity(format!("attempt {attempt} has conflicting content"))
            }
            WorkerError::PolicyViolation(detail) => Self::policy(detail),
            WorkerError::AttemptStore(error) => {
                let detail = error.to_string();
                match error {
                    AttemptStoreError::Storage(_)
                    | AttemptStoreError::LockPoisoned
                    | AttemptStoreError::DeviceAlreadyLeased { .. } => Self::retryable(detail),
                    AttemptStoreError::ConflictingAttempt(_)
                    | AttemptStoreError::ConflictingFinished(_)
                    | AttemptStoreError::ConflictingOutboxMessage(_)
                    | AttemptStoreError::WorkerIdentityMismatch { .. }
                    | AttemptStoreError::ConflictingDeviceLease { .. }
                    | AttemptStoreError::ConflictingDevicePreflight(_)
                    | AttemptStoreError::Corrupt(_) => Self::integrity(detail),
                    AttemptStoreError::Encoding(_)
                    | AttemptStoreError::NotFound(_)
                    | AttemptStoreError::InvalidTransition { .. } => Self::terminal(detail),
                }
            }
            WorkerError::PersistenceTask(error) => Self::retryable(error.to_string()),
            WorkerError::Transport(error) => Self::retryable(error.to_string()),
            WorkerError::Rpc(error) => Self::retryable(error.to_string()),
            WorkerError::ArtifactPublication(error) => Self::from(error),
            WorkerError::Backend(error) => error,
            WorkerError::Execution(detail) | WorkerError::Protocol(detail) => {
                Self::terminal(detail)
            }
            WorkerError::StreamClosed => Self::retryable("worker control stream closed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_input::ArtifactInputError;
    use crate::cuda_supervisor::ContainerEngineError;

    #[test]
    fn category_and_detail_are_inspectable_without_parsing_display_text() {
        let error = BackendError::integrity("digest mismatch").with_context("attempt a-1");
        assert_eq!(error.class(), BackendFailureClass::Integrity);
        assert_eq!(error.detail(), "attempt a-1: digest mismatch");
        assert_eq!(
            error.to_string(),
            "backend integrity failure: attempt a-1: digest mismatch"
        );
    }

    #[test]
    fn adapter_and_runtime_errors_map_to_stable_backend_classes() {
        let cases = [
            (
                BackendError::from(ArtifactInputError::Unavailable("offline".into())),
                BackendFailureClass::Retryable,
            ),
            (
                BackendError::from(ExecutionRuntimeError::MissingAttempt("a-1".into())),
                BackendFailureClass::Terminal,
            ),
            (
                BackendError::from(ContainerEngineError::InvalidConfiguration("binary".into())),
                BackendFailureClass::Policy,
            ),
            (
                BackendError::from(ArtifactInputError::Integrity("digest".into())),
                BackendFailureClass::Integrity,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.class(), expected);
        }
    }
}
