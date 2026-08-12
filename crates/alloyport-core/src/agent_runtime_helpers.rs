//! Identity/digest helpers and public errors for the durable Agent Episode reducer.

use crate::agent_runtime_support::{
    AgentLoopAdvance, AgentRuntimeFaultInjector, AgentRuntimeFaultPoint, EpisodeRepositoryError,
    RuntimeToolDescriptor, ToolGatewayError,
};
use crate::{
    AgentRecordError, EpisodeId, EpisodeStatus, ModelAttemptError, ModelAttemptId,
    ModelGatewayError, Sha256Digest, ToolOperationId, TurnId, TurnRecordError,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub(crate) fn progress(status: EpisodeStatus) -> AgentLoopAdvance {
    if status.is_terminal() {
        AgentLoopAdvance::Terminal(status)
    } else if status == EpisodeStatus::Suspended {
        AgentLoopAdvance::Suspended
    } else {
        AgentLoopAdvance::Progressed(status)
    }
}

pub(crate) fn crash_if<F: AgentRuntimeFaultInjector>(
    faults: &mut F,
    point: AgentRuntimeFaultPoint,
) -> Result<(), AgentLoopRuntimeError> {
    if faults.should_crash(point) {
        Err(AgentLoopRuntimeError::InjectedCrash(point))
    } else {
        Ok(())
    }
}

pub(crate) fn usize_from_u32(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

pub(crate) fn digest_label(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

pub(crate) fn digest_semantic_turn(turn: &crate::GatewayTurn) -> Sha256Digest {
    let mut bytes = b"alloyport-semantic-turn-v1".to_vec();
    for narrative in &turn.narrative {
        push_len(&mut bytes, narrative.len());
        bytes.extend_from_slice(narrative.as_bytes());
    }
    for call in &turn.tool_calls {
        push_len(&mut bytes, call.native_call_id.len());
        bytes.extend_from_slice(call.native_call_id.as_bytes());
        push_len(&mut bytes, call.name.len());
        bytes.extend_from_slice(call.name.as_bytes());
        push_len(&mut bytes, call.raw_arguments.len());
        bytes.extend_from_slice(&call.raw_arguments);
    }
    bytes.extend_from_slice(format!("{:?}", turn.stop_reason).as_bytes());
    Sha256Digest::digest_bytes(&bytes)
}

fn push_len(bytes: &mut Vec<u8>, length: usize) {
    bytes.extend_from_slice(&u64::try_from(length).unwrap_or(u64::MAX).to_be_bytes());
}

pub(crate) fn derived_model_attempt_id(
    episode_id: &EpisodeId,
    attempt_number: u32,
) -> Result<ModelAttemptId, AgentLoopRuntimeError> {
    ModelAttemptId::try_from(format!("model-attempt-{episode_id}-{attempt_number}"))
        .map_err(|_| AgentLoopRuntimeError::DerivedIdentity)
}

pub(crate) fn derived_turn_id(
    episode_id: &EpisodeId,
    turn_index: u32,
) -> Result<TurnId, AgentLoopRuntimeError> {
    TurnId::try_from(format!("turn-{episode_id}-{turn_index}"))
        .map_err(|_| AgentLoopRuntimeError::DerivedIdentity)
}

pub(crate) fn derived_tool_operation_id(
    episode_id: &EpisodeId,
    turn_id: &TurnId,
    call_index: usize,
    descriptor: &RuntimeToolDescriptor,
    arguments_digest: Sha256Digest,
    input_identity_digest: Sha256Digest,
) -> Result<ToolOperationId, AgentLoopRuntimeError> {
    let identity = format!(
        "{episode_id}|{turn_id}|{call_index}|{}|{}|{arguments_digest}|{input_identity_digest}",
        descriptor.name, descriptor.version
    );
    let digest = Sha256Digest::digest_bytes(identity.as_bytes());
    ToolOperationId::try_from(format!("tool-{}-{digest}", call_index + 1))
        .map_err(|_| AgentLoopRuntimeError::DerivedIdentity)
}

/// Derives the exact next model-input identity from native continuation and tool results.
#[must_use]
pub fn derive_model_continuation_input_digest(
    continuation: Sha256Digest,
    results: impl IntoIterator<Item = Sha256Digest>,
) -> Sha256Digest {
    let mut bytes = b"alloyport-model-continuation-input-v1".to_vec();
    bytes.extend_from_slice(&continuation.bytes());
    for result in results {
        bytes.extend_from_slice(&result.bytes());
    }
    Sha256Digest::digest_bytes(&bytes)
}

#[derive(Debug)]
pub enum AgentLoopRuntimeError {
    InvalidPolicy,
    CounterExhausted,
    MissingModelAttempt,
    MissingTurn,
    DerivedIdentity,
    InvalidDurableState(&'static str),
    InvalidToolOutcome,
    UntrustedCompletion,
    InjectedCrash(AgentRuntimeFaultPoint),
    Repository(EpisodeRepositoryError),
    AgentRecord(AgentRecordError),
    ModelAttempt(ModelAttemptError),
    TurnRecord(TurnRecordError),
    ModelGateway(ModelGatewayError),
    ToolGateway(ToolGatewayError),
}

impl Display for AgentLoopRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy => write!(formatter, "agent loop policy is invalid"),
            Self::CounterExhausted => write!(formatter, "agent loop counter is exhausted"),
            Self::MissingModelAttempt => write!(formatter, "model attempt is missing"),
            Self::MissingTurn => write!(formatter, "decoded turn is missing"),
            Self::DerivedIdentity => write!(formatter, "derived runtime identity is invalid"),
            Self::InvalidDurableState(message) => {
                write!(formatter, "invalid durable state: {message}")
            }
            Self::InvalidToolOutcome => {
                write!(formatter, "tool returned an invalid terminal outcome")
            }
            Self::UntrustedCompletion => {
                write!(formatter, "untrusted tool result claimed completion")
            }
            Self::InjectedCrash(point) => write!(formatter, "injected runtime crash at {point:?}"),
            Self::Repository(error) => Display::fmt(error, formatter),
            Self::AgentRecord(error) => Display::fmt(error, formatter),
            Self::ModelAttempt(error) => Display::fmt(error, formatter),
            Self::TurnRecord(error) => Display::fmt(error, formatter),
            Self::ModelGateway(error) => Display::fmt(error, formatter),
            Self::ToolGateway(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for AgentLoopRuntimeError {}

macro_rules! runtime_error_from {
    ($source:ty, $variant:ident) => {
        impl From<$source> for AgentLoopRuntimeError {
            fn from(value: $source) -> Self {
                Self::$variant(value)
            }
        }
    };
}

runtime_error_from!(EpisodeRepositoryError, Repository);
runtime_error_from!(AgentRecordError, AgentRecord);
runtime_error_from!(ModelAttemptError, ModelAttempt);
runtime_error_from!(TurnRecordError, TurnRecord);
runtime_error_from!(ModelGatewayError, ModelGateway);
runtime_error_from!(ToolGatewayError, ToolGateway);
