//! Domain primitives for `AlloyPort`'s verified delivery lifecycle.

mod execution;

pub use execution::{
    AttemptOutcome, AttemptOutcomeError, ExecutionKind, ExecutionKindError, NetworkPolicy,
    NetworkPolicyError, RejectionReason, RejectionReasonError,
};

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Target accelerator selected for a porting task.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TargetBackend {
    Ascend,
    AmdHip,
    IntelXpu,
    Other(String),
}

/// Implementation route selected after capture and contract construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Route {
    Keep,
    Reuse,
    Compile,
    PortableKernel,
    NativeKernel,
}

/// Durable task lifecycle. Terminal states have no outgoing transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TaskState {
    Captured,
    Specified,
    Routed,
    Searching,
    Verifying,
    Releasable,
    Released,
    Failed,
}

impl TaskState {
    /// Returns whether moving from this state to `next` preserves the lifecycle invariant.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Captured, Self::Specified | Self::Failed)
                | (Self::Specified, Self::Routed | Self::Failed)
                | (Self::Routed, Self::Searching | Self::Failed)
                | (Self::Searching, Self::Verifying | Self::Failed)
                | (
                    Self::Verifying,
                    Self::Searching | Self::Releasable | Self::Failed
                )
                | (Self::Releasable, Self::Released | Self::Failed)
        )
    }
}

/// A porting task with explicit lifecycle and selected route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Task {
    pub id: String,
    pub source_revision: String,
    pub target: TargetBackend,
    pub state: TaskState,
    pub route: Option<Route>,
}

impl Task {
    /// Moves the task to another valid lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] when the requested transition violates the state machine.
    pub fn transition(&mut self, next: TaskState) -> Result<(), TransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(TransitionError {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }
}

/// An immutable implementation candidate and its lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub id: String,
    pub task_id: String,
    pub route: Route,
    pub parent_id: Option<String>,
    pub source_digest: String,
    pub artifact_digest: Option<String>,
}

/// Gate evaluated independently from candidate generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Gate {
    Contract,
    Build,
    Correctness,
    Performance,
    Integration,
}

impl Gate {
    pub const ALL: [Self; 5] = [
        Self::Contract,
        Self::Build,
        Self::Correctness,
        Self::Performance,
        Self::Integration,
    ];
}

/// Independent decision for one candidate at one gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Verdict {
    pub candidate_id: String,
    pub gate: Gate,
    pub passed: bool,
    pub receipt_digests: Vec<String>,
}

/// Immutable release description presented to integration and deployment tooling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseManifest {
    pub candidate_id: String,
    pub supported_domain: String,
    pub dispatch_guard: String,
    pub fallback: String,
    pub evidence_digests: BTreeSet<String>,
}

impl ReleaseManifest {
    /// Builds a manifest only when all release gates pass and every verdict has evidence.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseError`] for missing, failed, duplicate, or evidence-free gate verdicts.
    pub fn from_verdicts(
        candidate_id: impl Into<String>,
        supported_domain: impl Into<String>,
        dispatch_guard: impl Into<String>,
        fallback: impl Into<String>,
        verdicts: &[Verdict],
    ) -> Result<Self, ReleaseError> {
        let candidate_id = candidate_id.into();
        let mut passed = BTreeSet::new();
        let mut evidence_digests = BTreeSet::new();

        for verdict in verdicts {
            if verdict.candidate_id != candidate_id {
                return Err(ReleaseError::CandidateMismatch);
            }
            if !passed.insert(verdict.gate) {
                return Err(ReleaseError::DuplicateGate(verdict.gate));
            }
            if !verdict.passed {
                return Err(ReleaseError::FailedGate(verdict.gate));
            }
            if verdict.receipt_digests.is_empty() {
                return Err(ReleaseError::MissingEvidence(verdict.gate));
            }
            evidence_digests.extend(verdict.receipt_digests.iter().cloned());
        }

        for gate in Gate::ALL {
            if !passed.contains(&gate) {
                return Err(ReleaseError::MissingGate(gate));
            }
        }

        Ok(Self {
            candidate_id,
            supported_domain: supported_domain.into(),
            dispatch_guard: dispatch_guard.into(),
            fallback: fallback.into(),
            evidence_digests,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionError {
    pub from: TaskState,
    pub to: TaskState,
}

impl Display for TransitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid task transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

impl Error for TransitionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseError {
    CandidateMismatch,
    DuplicateGate(Gate),
    FailedGate(Gate),
    MissingEvidence(Gate),
    MissingGate(Gate),
}

impl Display for ReleaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateMismatch => write!(formatter, "verdict belongs to another candidate"),
            Self::DuplicateGate(gate) => write!(formatter, "duplicate verdict for {gate:?}"),
            Self::FailedGate(gate) => write!(formatter, "gate {gate:?} did not pass"),
            Self::MissingEvidence(gate) => write!(formatter, "gate {gate:?} has no receipts"),
            Self::MissingGate(gate) => write!(formatter, "gate {gate:?} has no verdict"),
        }
    }
}

impl Error for ReleaseError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn passing_verdicts(candidate_id: &str) -> Vec<Verdict> {
        Gate::ALL
            .into_iter()
            .map(|gate| Verdict {
                candidate_id: candidate_id.to_owned(),
                gate,
                passed: true,
                receipt_digests: vec![format!("sha256:{gate:?}")],
            })
            .collect()
    }

    #[test]
    fn lifecycle_allows_rework_after_verification() {
        assert!(TaskState::Verifying.can_transition_to(TaskState::Searching));
        assert!(!TaskState::Released.can_transition_to(TaskState::Searching));
    }

    #[test]
    fn release_requires_every_gate() {
        let mut verdicts = passing_verdicts("candidate-1");
        verdicts.retain(|verdict| verdict.gate != Gate::Performance);

        let error = ReleaseManifest::from_verdicts(
            "candidate-1",
            "M,N,K divisible by 16",
            "shape_guard_v1",
            "torch_reference",
            &verdicts,
        )
        .expect_err("a release without performance evidence must fail");

        assert_eq!(error, ReleaseError::MissingGate(Gate::Performance));
    }

    #[test]
    fn release_collects_content_addressed_evidence() {
        let verdicts = passing_verdicts("candidate-1");
        let manifest = ReleaseManifest::from_verdicts(
            "candidate-1",
            "all tested shapes",
            "always",
            "torch_reference",
            &verdicts,
        )
        .expect("all gates have independent evidence");

        assert_eq!(manifest.evidence_digests.len(), Gate::ALL.len());
    }
}
