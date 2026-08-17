//! Provider-neutral model gateway port and its deterministic fake.

use crate::model::ModelUsage;
use crate::{EpisodeId, ModelAttemptId, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

pub const TURN_RECORD_SCHEMA_V1: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelTurnRequest {
    pub attempt_id: ModelAttemptId,
    pub episode_id: EpisodeId,
    pub turn_index: u32,
    pub input_digest: Sha256Digest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayToolCall {
    pub native_call_id: String,
    pub name: String,
    pub raw_arguments: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedStopReason {
    Stop,
    ToolCalls,
    Length,
    Refusal,
    ContentFilter,
    ProviderResource,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayTurn {
    pub narrative: Vec<String>,
    pub tool_calls: Vec<GatewayToolCall>,
    pub stop_reason: NormalizedStopReason,
    pub usage: Option<ModelUsage>,
}

/// One semantic turn plus immutable identities for its exact native exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayTurnExchange {
    pub turn: GatewayTurn,
    pub raw_exchange_digest: Sha256Digest,
    pub native_continuation_digest: Sha256Digest,
}

impl GatewayTurn {
    /// Validates the provider-neutral semantic turn.
    ///
    /// # Errors
    ///
    /// Returns an error for empty turns or invalid/duplicate native tool-call identities.
    pub fn validate(&self) -> Result<(), ModelGatewayError> {
        if self.narrative.is_empty() && self.tool_calls.is_empty() {
            return Err(ModelGatewayError::InvalidTurn(
                "turn contains neither narrative nor tool calls".to_owned(),
            ));
        }
        let mut call_ids = BTreeSet::new();
        for call in &self.tool_calls {
            if call.native_call_id.trim().is_empty() || call.name.trim().is_empty() {
                return Err(ModelGatewayError::InvalidTurn(
                    "tool call identity and name must not be empty".to_owned(),
                ));
            }
            if !call_ids.insert(&call.native_call_id) {
                return Err(ModelGatewayError::InvalidTurn(format!(
                    "duplicate native tool call ID {}",
                    call.native_call_id
                )));
            }
        }
        Ok(())
    }
}

/// Immutable identities needed to persist one validated semantic turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TurnSpec {
    pub id: crate::TurnId,
    pub episode_id: EpisodeId,
    pub model_attempt_id: ModelAttemptId,
    pub turn_index: u32,
    pub decoded_turn_digest: Sha256Digest,
    pub raw_exchange_digest: Sha256Digest,
    pub native_continuation_digest: Sha256Digest,
    pub stop_reason: NormalizedStopReason,
    pub tool_call_count: u32,
    pub usage: Option<ModelUsage>,
}

/// Persistable provider-neutral projection; exact native bytes remain content-addressed Artifacts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnRecord {
    schema_version: u16,
    id: crate::TurnId,
    episode_id: EpisodeId,
    model_attempt_id: ModelAttemptId,
    turn_index: u32,
    decoded_turn_digest: Sha256Digest,
    raw_exchange_digest: Sha256Digest,
    native_continuation_digest: Sha256Digest,
    stop_reason: NormalizedStopReason,
    tool_call_count: u32,
    usage: Option<ModelUsage>,
}

impl TurnRecord {
    /// Creates a durable turn projection after gateway validation.
    ///
    /// # Errors
    ///
    /// Returns an error when the episode-relative turn index is zero.
    pub fn new(spec: TurnSpec) -> Result<Self, TurnRecordError> {
        if spec.turn_index == 0 {
            return Err(TurnRecordError::ZeroTurnIndex);
        }
        Ok(Self {
            schema_version: TURN_RECORD_SCHEMA_V1,
            id: spec.id,
            episode_id: spec.episode_id,
            model_attempt_id: spec.model_attempt_id,
            turn_index: spec.turn_index,
            decoded_turn_digest: spec.decoded_turn_digest,
            raw_exchange_digest: spec.raw_exchange_digest,
            native_continuation_digest: spec.native_continuation_digest,
            stop_reason: spec.stop_reason,
            tool_call_count: spec.tool_call_count,
            usage: spec.usage,
        })
    }

    #[must_use]
    pub const fn native_continuation_digest(&self) -> Sha256Digest {
        self.native_continuation_digest
    }

    #[must_use]
    pub const fn id(&self) -> &crate::TurnId {
        &self.id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnRecordError {
    ZeroTurnIndex,
}

impl Display for TurnRecordError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTurnIndex => write!(formatter, "turn index must be positive"),
        }
    }
}

impl Error for TurnRecordError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelGatewayOutcome {
    Turn(GatewayTurnExchange),
    ConfirmedNotSent {
        diagnostic: String,
        diagnostic_digest: Option<Sha256Digest>,
    },
    Rejected {
        response_digest: Sha256Digest,
        diagnostic: String,
        diagnostic_digest: Option<Sha256Digest>,
        retryable: bool,
    },
    DecodeFailed {
        response_digest: Sha256Digest,
        diagnostic: String,
        diagnostic_digest: Option<Sha256Digest>,
    },
    Ambiguous {
        diagnostic: String,
        diagnostic_digest: Option<Sha256Digest>,
    },
}

pub type ModelGatewayFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ModelGatewayOutcome, ModelGatewayError>> + Send + 'a>>;

pub trait ModelGateway: Debug + Send {
    /// Produces one semantic model-turn outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the gateway cannot validate or process the request.
    #[must_use]
    fn invoke<'a>(&'a mut self, request: &'a ModelTurnRequest) -> ModelGatewayFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptedGatewayStep {
    pub expected_turn_index: u32,
    pub expected_input_digest: Sha256Digest,
    pub outcome: ModelGatewayOutcome,
}

/// Deterministic, non-network gateway used before any real codec or transport is connected.
#[derive(Debug)]
pub struct ScriptedFakeModelGateway {
    steps: VecDeque<ScriptedGatewayStep>,
}

impl ScriptedFakeModelGateway {
    #[must_use]
    pub fn new(steps: impl IntoIterator<Item = ScriptedGatewayStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn remaining_steps(&self) -> usize {
        self.steps.len()
    }
}

impl ModelGateway for ScriptedFakeModelGateway {
    fn invoke<'a>(&'a mut self, request: &'a ModelTurnRequest) -> ModelGatewayFuture<'a> {
        Box::pin(async move {
            let step = self
                .steps
                .pop_front()
                .ok_or(ModelGatewayError::ScriptExhausted)?;
            if step.expected_turn_index != request.turn_index
                || step.expected_input_digest != request.input_digest
            {
                return Err(ModelGatewayError::UnexpectedRequest {
                    expected_turn_index: step.expected_turn_index,
                    actual_turn_index: request.turn_index,
                });
            }
            if let ModelGatewayOutcome::Turn(exchange) = &step.outcome {
                exchange.turn.validate()?;
            }
            Ok(step.outcome)
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelGatewayError {
    ScriptExhausted,
    UnexpectedRequest {
        expected_turn_index: u32,
        actual_turn_index: u32,
    },
    InvalidTurn(String),
    Adapter(String),
}

impl Display for ModelGatewayError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScriptExhausted => write!(formatter, "scripted model gateway is exhausted"),
            Self::UnexpectedRequest {
                expected_turn_index,
                actual_turn_index,
            } => write!(
                formatter,
                "expected model turn {expected_turn_index}, received {actual_turn_index}"
            ),
            Self::InvalidTurn(message) => write!(formatter, "invalid model turn: {message}"),
            Self::Adapter(message) => write!(formatter, "model gateway adapter: {message}"),
        }
    }
}

impl Error for ModelGatewayError {}
