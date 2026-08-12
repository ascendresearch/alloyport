//! Stable execution vocabulary shared by server and worker application layers.

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Valid executor selected by an immutable assignment contract.
///
/// Numeric values intentionally match the versioned wire and persisted JSON representation, while
/// the domain type excludes the transport-only unspecified value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i32)]
#[serde(try_from = "i32", into = "i32")]
pub enum ExecutionKind {
    Process = 1,
    Container = 2,
    Shell = 3,
    CudaFixture = 4,
    AscendFixture = 5,
}

impl ExecutionKind {
    /// Stable diagnostic name matching the corresponding wire enum constant.
    #[must_use]
    pub const fn as_str_name(self) -> &'static str {
        match self {
            Self::Process => "EXECUTOR_KIND_PROCESS",
            Self::Container => "EXECUTOR_KIND_CONTAINER",
            Self::Shell => "EXECUTOR_KIND_SHELL",
            Self::CudaFixture => "EXECUTOR_KIND_CUDA_FIXTURE",
            Self::AscendFixture => "EXECUTOR_KIND_ASCEND_FIXTURE",
        }
    }
}

impl From<ExecutionKind> for i32 {
    fn from(kind: ExecutionKind) -> Self {
        kind as Self
    }
}

impl TryFrom<i32> for ExecutionKind {
    type Error = ExecutionKindError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Process),
            2 => Ok(Self::Container),
            3 => Ok(Self::Shell),
            4 => Ok(Self::CudaFixture),
            5 => Ok(Self::AscendFixture),
            _ => Err(ExecutionKindError(value)),
        }
    }
}

/// A wire or persisted executor number is not part of the domain vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionKindError(pub i32);

impl Display for ExecutionKindError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid executor kind {}", self.0)
    }
}

impl Error for ExecutionKindError {}

/// Network access permitted to an execution sandbox.
///
/// `Unspecified` preserves the protocol's backward-compatible default; backend policy decides
/// whether a concrete execution kind requires a more restrictive explicit value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i32)]
#[serde(try_from = "i32", into = "i32")]
pub enum NetworkPolicy {
    Unspecified = 0,
    Disabled = 1,
    DependencyFetch = 2,
}

impl From<NetworkPolicy> for i32 {
    fn from(policy: NetworkPolicy) -> Self {
        policy as Self
    }
}

impl TryFrom<i32> for NetworkPolicy {
    type Error = NetworkPolicyError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::Disabled),
            2 => Ok(Self::DependencyFetch),
            _ => Err(NetworkPolicyError(value)),
        }
    }
}

/// A wire or persisted network-policy number is not part of the domain vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkPolicyError(pub i32);

impl Display for NetworkPolicyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid network policy {}", self.0)
    }
}

impl Error for NetworkPolicyError {}

/// Terminal classification of one execution attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i32)]
#[serde(try_from = "i32", into = "i32")]
pub enum AttemptOutcome {
    Succeeded = 1,
    CandidateFailed = 2,
    TimedOut = 3,
    Cancelled = 4,
    InfraError = 5,
    IntegrityViolation = 6,
}

impl AttemptOutcome {
    /// Stable diagnostic name matching the corresponding wire enum constant.
    #[must_use]
    pub const fn as_str_name(self) -> &'static str {
        match self {
            Self::Succeeded => "ATTEMPT_OUTCOME_SUCCEEDED",
            Self::CandidateFailed => "ATTEMPT_OUTCOME_CANDIDATE_FAILED",
            Self::TimedOut => "ATTEMPT_OUTCOME_TIMED_OUT",
            Self::Cancelled => "ATTEMPT_OUTCOME_CANCELLED",
            Self::InfraError => "ATTEMPT_OUTCOME_INFRA_ERROR",
            Self::IntegrityViolation => "ATTEMPT_OUTCOME_INTEGRITY_VIOLATION",
        }
    }
}

impl From<AttemptOutcome> for i32 {
    fn from(outcome: AttemptOutcome) -> Self {
        outcome as Self
    }
}

impl TryFrom<i32> for AttemptOutcome {
    type Error = AttemptOutcomeError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Succeeded),
            2 => Ok(Self::CandidateFailed),
            3 => Ok(Self::TimedOut),
            4 => Ok(Self::Cancelled),
            5 => Ok(Self::InfraError),
            6 => Ok(Self::IntegrityViolation),
            _ => Err(AttemptOutcomeError(value)),
        }
    }
}

/// A wire or persisted terminal-outcome number is not part of the domain vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptOutcomeError(pub i32);

impl Display for AttemptOutcomeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid attempt outcome {}", self.0)
    }
}

impl Error for AttemptOutcomeError {}

/// Durable reason a worker refused an assignment before execution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[repr(i32)]
#[serde(try_from = "i32", into = "i32")]
pub enum RejectionReason {
    Invalid = 1,
    Unsupported = 2,
    Policy = 3,
    Capacity = 4,
    Conflict = 5,
}

impl From<RejectionReason> for i32 {
    fn from(reason: RejectionReason) -> Self {
        reason as Self
    }
}

impl TryFrom<i32> for RejectionReason {
    type Error = RejectionReasonError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Invalid),
            2 => Ok(Self::Unsupported),
            3 => Ok(Self::Policy),
            4 => Ok(Self::Capacity),
            5 => Ok(Self::Conflict),
            _ => Err(RejectionReasonError(value)),
        }
    }
}

/// A wire or persisted rejection-reason number is not part of the domain vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RejectionReasonError(pub i32);

impl Display for RejectionReasonError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid rejection reason {}", self.0)
    }
}

impl Error for RejectionReasonError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_representation_remains_numeric() -> Result<(), Box<dyn Error>> {
        let encoded = serde_json::to_string(&ExecutionKind::CudaFixture)?;
        assert_eq!(encoded, "4");
        assert_eq!(
            serde_json::from_str::<ExecutionKind>(&encoded)?,
            ExecutionKind::CudaFixture
        );
        assert_eq!(serde_json::to_string(&ExecutionKind::AscendFixture)?, "5");
        assert!(serde_json::from_str::<ExecutionKind>("0").is_err());
        assert_eq!(serde_json::to_string(&NetworkPolicy::Disabled)?, "1");
        assert_eq!(
            serde_json::from_str::<NetworkPolicy>("2")?,
            NetworkPolicy::DependencyFetch
        );
        assert_eq!(
            serde_json::from_str::<NetworkPolicy>("0")?,
            NetworkPolicy::Unspecified
        );
        assert_eq!(serde_json::to_string(&AttemptOutcome::TimedOut)?, "3");
        assert_eq!(
            serde_json::from_str::<AttemptOutcome>("6")?,
            AttemptOutcome::IntegrityViolation
        );
        assert!(serde_json::from_str::<AttemptOutcome>("0").is_err());
        assert_eq!(serde_json::to_string(&RejectionReason::Policy)?, "3");
        assert_eq!(
            serde_json::from_str::<RejectionReason>("5")?,
            RejectionReason::Conflict
        );
        assert!(serde_json::from_str::<RejectionReason>("0").is_err());
        Ok(())
    }
}
