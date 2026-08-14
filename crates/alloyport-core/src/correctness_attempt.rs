//! Paired-execution Port and shared correctness error categories.

use crate::correctness::ReductionCorrectnessExperiment;
use crate::{ArtifactDescriptor, Sha256Digest};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

/// Current state of the independently dispatched paired execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReductionCorrectnessAttemptObservation {
    Pending {
        diagnostic_digest: Sha256Digest,
    },
    Finished {
        reference_run: ArtifactDescriptor,
        candidate_run: ArtifactDescriptor,
    },
}

/// Controller-authored paired input. Workers receive bundle Artifacts, never oracle policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReductionCorrectnessAttemptSpec {
    pub experiment: ReductionCorrectnessExperiment,
    pub reference_bundle: ArtifactDescriptor,
    pub candidate_bundle: ArtifactDescriptor,
}

pub type ReductionCorrectnessAttemptFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    ReductionCorrectnessAttemptObservation,
                    ReductionCorrectnessAttemptError,
                >,
            > + Send
            + 'a,
    >,
>;

/// Port that owns independent CUDA-reference and Ascend-candidate execution.
pub trait ReductionCorrectnessAttemptPort: Debug + Send {
    #[must_use]
    fn dispatch<'a>(
        &'a mut self,
        spec: &'a ReductionCorrectnessAttemptSpec,
    ) -> ReductionCorrectnessAttemptFuture<'a>;

    #[must_use]
    fn reconcile<'a>(
        &'a mut self,
        spec: &'a ReductionCorrectnessAttemptSpec,
    ) -> ReductionCorrectnessAttemptFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReductionCorrectnessAttemptError {
    Unavailable(String),
    Rejected(String),
    Integrity(String),
}

impl Display for ReductionCorrectnessAttemptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(detail) => {
                write!(formatter, "correctness attempt unavailable: {detail}")
            }
            Self::Rejected(detail) => write!(formatter, "correctness attempt rejected: {detail}"),
            Self::Integrity(detail) => write!(formatter, "correctness attempt integrity: {detail}"),
        }
    }
}

impl Error for ReductionCorrectnessAttemptError {}

#[derive(Debug)]
pub enum ReductionCorrectnessError {
    InvalidObservation,
    InvalidRunContext,
    DuplicateObservation,
    ReferenceRoleRequired,
    ExperimentIdentityMismatch,
    InvalidCorpus,
    InvalidExecutionSource,
    InvalidExecutionBundle,
    NoiseFloorUnavailable,
    Serialization(serde_json::Error),
}

impl Display for ReductionCorrectnessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObservation => write!(formatter, "invalid reduction observation"),
            Self::InvalidRunContext => write!(formatter, "invalid reduction run context"),
            Self::DuplicateObservation => write!(formatter, "duplicate reduction observation"),
            Self::ReferenceRoleRequired => write!(formatter, "CUDA reference run is required"),
            Self::ExperimentIdentityMismatch => {
                write!(formatter, "correctness experiment identity mismatch")
            }
            Self::InvalidCorpus => write!(formatter, "invalid reduction correctness corpus"),
            Self::InvalidExecutionSource => write!(formatter, "invalid reduction execution source"),
            Self::InvalidExecutionBundle => write!(formatter, "invalid reduction execution bundle"),
            Self::NoiseFloorUnavailable => write!(
                formatter,
                "the reference run carries no second summation order, so this task's own numeric \
                 spread was never measured"
            ),
            Self::Serialization(error) => {
                write!(formatter, "cannot encode correctness evidence: {error}")
            }
        }
    }
}

impl Error for ReductionCorrectnessError {}

impl From<serde_json::Error> for ReductionCorrectnessError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}
