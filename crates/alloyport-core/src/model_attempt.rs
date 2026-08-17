//! One possibly billed model request, recorded before it is sent and terminal after it returns.
//!
//! Split out of `model.rs` for the module-size limit. It stays a child module so an attempt can be
//! built from the catalog's resolved identities without either half widening its fields.

use crate::{EpisodeId, ModelAttemptId, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub const MODEL_ATTEMPT_SCHEMA_V1: u16 = 1;
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAttemptStatus {
    Prepared,
    Dispatching,
    Responded,
    Decoded,
    DecodeFailed,
    ConfirmedNotSent,
    Failed,
    Ambiguous,
    CancelledAmbiguous,
}

impl ModelAttemptStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Decoded
                | Self::DecodeFailed
                | Self::ConfirmedNotSent
                | Self::Failed
                | Self::Ambiguous
                | Self::CancelledAmbiguous
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: Option<u64>,
    pub cost_micros: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelAttemptSpec {
    pub id: ModelAttemptId,
    pub episode_id: EpisodeId,
    pub attempt_number: u32,
    pub request_digest: Sha256Digest,
    pub resolved_model_digest: Sha256Digest,
    pub deployment_digest: Sha256Digest,
    pub model_profile_digest: Sha256Digest,
    pub request_budget_digest: Sha256Digest,
    pub predecessor_attempt_id: Option<ModelAttemptId>,
    pub predecessor_continuation_digest: Option<Sha256Digest>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAttemptRecord {
    schema_version: u16,
    id: ModelAttemptId,
    episode_id: EpisodeId,
    attempt_number: u32,
    request_digest: Sha256Digest,
    resolved_model_digest: Sha256Digest,
    deployment_digest: Sha256Digest,
    model_profile_digest: Sha256Digest,
    request_budget_digest: Sha256Digest,
    predecessor_attempt_id: Option<ModelAttemptId>,
    predecessor_continuation_digest: Option<Sha256Digest>,
    status: ModelAttemptStatus,
    response_digest: Option<Sha256Digest>,
    continuation_digest: Option<Sha256Digest>,
    diagnostic_digest: Option<Sha256Digest>,
    actual_model: Option<String>,
    usage: Option<ModelUsage>,
}

impl ModelAttemptRecord {
    /// Creates a prepared attempt record.
    ///
    /// # Errors
    ///
    /// Returns an error when the attempt number is zero.
    pub fn new(spec: ModelAttemptSpec) -> Result<Self, ModelAttemptError> {
        if spec.attempt_number == 0 {
            return Err(ModelAttemptError::ZeroAttemptNumber);
        }
        Ok(Self {
            schema_version: MODEL_ATTEMPT_SCHEMA_V1,
            id: spec.id,
            episode_id: spec.episode_id,
            attempt_number: spec.attempt_number,
            request_digest: spec.request_digest,
            resolved_model_digest: spec.resolved_model_digest,
            deployment_digest: spec.deployment_digest,
            model_profile_digest: spec.model_profile_digest,
            request_budget_digest: spec.request_budget_digest,
            predecessor_attempt_id: spec.predecessor_attempt_id,
            predecessor_continuation_digest: spec.predecessor_continuation_digest,
            status: ModelAttemptStatus::Prepared,
            response_digest: None,
            continuation_digest: None,
            diagnostic_digest: None,
            actual_model: None,
            usage: None,
        })
    }

    /// Marks the point after which dispatch ambiguity must be preserved.
    ///
    /// # Errors
    ///
    /// Returns an error unless the attempt is prepared.
    pub fn mark_dispatching(&mut self) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Prepared)?;
        self.status = ModelAttemptStatus::Dispatching;
        Ok(())
    }

    /// Records the exact provider response identity and optional usage.
    ///
    /// # Errors
    ///
    /// Returns an error unless dispatch started or when `actual_model` is empty.
    pub fn record_response(
        &mut self,
        response_digest: Sha256Digest,
        actual_model: Option<String>,
        usage: Option<ModelUsage>,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Dispatching)?;
        if actual_model
            .as_ref()
            .is_some_and(|model| model.trim().is_empty())
        {
            return Err(ModelAttemptError::EmptyActualModel);
        }
        self.response_digest = Some(response_digest);
        self.actual_model = actual_model;
        self.usage = usage;
        self.status = ModelAttemptStatus::Responded;
        Ok(())
    }

    /// Commits the normalized continuation identity.
    ///
    /// # Errors
    ///
    /// Returns an error unless a provider response was recorded.
    pub fn mark_decoded(
        &mut self,
        continuation_digest: Sha256Digest,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Responded)?;
        self.continuation_digest = Some(continuation_digest);
        self.status = ModelAttemptStatus::Decoded;
        Ok(())
    }

    /// Records a terminal decode failure without discarding the provider response.
    ///
    /// # Errors
    ///
    /// Returns an error unless a provider response was recorded.
    pub fn mark_decode_failed(
        &mut self,
        diagnostic_digest: Option<Sha256Digest>,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Responded)?;
        self.diagnostic_digest = diagnostic_digest;
        self.status = ModelAttemptStatus::DecodeFailed;
        Ok(())
    }

    /// Records a provider response that explicitly rejected the request.
    ///
    /// # Errors
    ///
    /// Returns an error unless a provider response was recorded.
    pub fn mark_response_failed(
        &mut self,
        diagnostic_digest: Option<Sha256Digest>,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Responded)?;
        self.diagnostic_digest = diagnostic_digest;
        self.status = ModelAttemptStatus::Failed;
        Ok(())
    }

    /// Finishes a dispatch that produced no authoritative response bytes.
    ///
    /// # Errors
    ///
    /// Returns an error unless dispatch started or `terminal` is invalid for this path.
    pub fn finish_without_response(
        &mut self,
        terminal: ModelAttemptStatus,
        diagnostic_digest: Option<Sha256Digest>,
    ) -> Result<(), ModelAttemptError> {
        self.require_status(ModelAttemptStatus::Dispatching)?;
        if !matches!(
            terminal,
            ModelAttemptStatus::ConfirmedNotSent
                | ModelAttemptStatus::Failed
                | ModelAttemptStatus::Ambiguous
                | ModelAttemptStatus::CancelledAmbiguous
        ) {
            return Err(ModelAttemptError::InvalidTerminal(terminal));
        }
        self.diagnostic_digest = diagnostic_digest;
        self.status = terminal;
        Ok(())
    }

    /// The published explanation for a failed attempt, when one was stored.
    #[must_use]
    pub const fn diagnostic_digest(&self) -> Option<Sha256Digest> {
        self.diagnostic_digest
    }

    #[must_use]
    pub const fn status(&self) -> ModelAttemptStatus {
        self.status
    }

    #[must_use]
    pub const fn id(&self) -> &ModelAttemptId {
        &self.id
    }

    fn require_status(&self, expected: ModelAttemptStatus) -> Result<(), ModelAttemptError> {
        if self.status == expected {
            Ok(())
        } else {
            Err(ModelAttemptError::InvalidTransition {
                from: self.status,
                expected,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelAttemptError {
    ZeroAttemptNumber,
    EmptyActualModel,
    InvalidTransition {
        from: ModelAttemptStatus,
        expected: ModelAttemptStatus,
    },
    InvalidTerminal(ModelAttemptStatus),
}

impl Display for ModelAttemptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAttemptNumber => write!(formatter, "model attempt number must be positive"),
            Self::EmptyActualModel => write!(formatter, "actual model must not be empty"),
            Self::InvalidTransition { from, expected } => {
                write!(
                    formatter,
                    "model attempt is {from:?}, expected {expected:?}"
                )
            }
            Self::InvalidTerminal(status) => write!(formatter, "invalid model terminal {status:?}"),
        }
    }
}

impl Error for ModelAttemptError {}
