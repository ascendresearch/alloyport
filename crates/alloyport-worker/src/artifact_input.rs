//! Assignment-input materialization port and implementation-independent error categories.

use crate::backend_error::BackendError;
use crate::journal::StoredArtifact;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

/// Future returned by an Artifact input provider.
pub type ArtifactInputFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ArtifactInputError>> + Send + 'a>>;

/// Materializes a declared assignment input into backend-accessible local storage.
pub trait ArtifactInputProvider: Debug + Send + Sync {
    fn materialize<'a>(&'a self, artifact: &'a StoredArtifact) -> ArtifactInputFuture<'a>;
}

/// Stable failure categories exposed to execution backends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactInputError {
    Invalid(String),
    Policy(String),
    Unavailable(String),
    Integrity(String),
    Internal(String),
}

impl Display for ArtifactInputError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let (category, detail) = match self {
            Self::Invalid(detail) => ("invalid Artifact input", detail),
            Self::Policy(detail) => ("Artifact input policy rejected", detail),
            Self::Unavailable(detail) => ("Artifact input unavailable", detail),
            Self::Integrity(detail) => ("Artifact input integrity failure", detail),
            Self::Internal(detail) => ("Artifact input internal failure", detail),
        };
        write!(formatter, "{category}: {detail}")
    }
}

impl Error for ArtifactInputError {}

impl From<ArtifactInputError> for BackendError {
    fn from(error: ArtifactInputError) -> Self {
        let detail = error.to_string();
        match error {
            ArtifactInputError::Unavailable(_) => Self::retryable(detail),
            ArtifactInputError::Invalid(_) | ArtifactInputError::Policy(_) => Self::policy(detail),
            ArtifactInputError::Integrity(_) => Self::integrity(detail),
            ArtifactInputError::Internal(_) => Self::terminal(detail),
        }
    }
}
