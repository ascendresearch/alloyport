//! Persistence, tool, and fault-injection ports for the durable Agent Episode reducer.

use crate::{
    DurableEpisodeState, EpisodeId, EpisodeStatus, GatewayToolCall, Sha256Digest, ToolEffectClass,
    ToolOperationId, ToolOperationStatus, ToolResultAuthority,
};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

/// Compare-and-swap repository used by the reducer; adapters own physical durability.
pub trait EpisodeRepository: Debug + Send {
    /// Creates an episode at revision zero.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity already exists or persistence fails.
    fn create(&mut self, state: DurableEpisodeState) -> Result<(), EpisodeRepositoryError>;

    /// Loads the current revision and state.
    ///
    /// # Errors
    ///
    /// Returns an error when the episode is absent or persistence fails.
    fn load(&self, id: &EpisodeId) -> Result<VersionedEpisodeState, EpisodeRepositoryError>;

    /// Replaces the state only when `expected_revision` is current.
    ///
    /// # Errors
    ///
    /// Returns an error for missing state, conflicts, or persistence failures.
    fn save(
        &mut self,
        expected_revision: u64,
        state: DurableEpisodeState,
    ) -> Result<u64, EpisodeRepositoryError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedEpisodeState {
    pub revision: u64,
    pub state: DurableEpisodeState,
}

/// Deterministic reference adapter. Keeping it while reconstructing runners simulates restart.
#[derive(Clone, Debug, Default)]
pub struct InMemoryEpisodeRepository {
    episodes: BTreeMap<EpisodeId, VersionedEpisodeState>,
}

impl EpisodeRepository for InMemoryEpisodeRepository {
    fn create(&mut self, state: DurableEpisodeState) -> Result<(), EpisodeRepositoryError> {
        let id = state.episode_id().clone();
        if self.episodes.contains_key(&id) {
            return Err(EpisodeRepositoryError::AlreadyExists(id));
        }
        self.episodes
            .insert(id, VersionedEpisodeState { revision: 0, state });
        Ok(())
    }

    fn load(&self, id: &EpisodeId) -> Result<VersionedEpisodeState, EpisodeRepositoryError> {
        self.episodes
            .get(id)
            .cloned()
            .ok_or_else(|| EpisodeRepositoryError::NotFound(id.clone()))
    }

    fn save(
        &mut self,
        expected_revision: u64,
        state: DurableEpisodeState,
    ) -> Result<u64, EpisodeRepositoryError> {
        let id = state.episode_id().clone();
        let current = self
            .episodes
            .get_mut(&id)
            .ok_or_else(|| EpisodeRepositoryError::NotFound(id.clone()))?;
        if current.revision != expected_revision {
            return Err(EpisodeRepositoryError::Conflict {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        let revision = expected_revision
            .checked_add(1)
            .ok_or(EpisodeRepositoryError::RevisionExhausted)?;
        *current = VersionedEpisodeState { revision, state };
        Ok(revision)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpisodeRepositoryError {
    AlreadyExists(EpisodeId),
    NotFound(EpisodeId),
    Conflict { expected: u64, actual: u64 },
    RevisionExhausted,
    Adapter(String),
}

impl Display for EpisodeRepositoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists(id) => write!(formatter, "episode {id} already exists"),
            Self::NotFound(id) => write!(formatter, "episode {id} was not found"),
            Self::Conflict { expected, actual } => write!(
                formatter,
                "episode revision conflict: expected {expected}, actual {actual}"
            ),
            Self::RevisionExhausted => write!(formatter, "episode revision is exhausted"),
            Self::Adapter(detail) => {
                write!(formatter, "episode repository adapter failed: {detail}")
            }
        }
    }
}

impl Error for EpisodeRepositoryError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeToolDescriptor {
    pub name: String,
    pub version: String,
    pub effect_class: ToolEffectClass,
    pub result_authority: ToolResultAuthority,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolInvocation {
    pub operation_id: ToolOperationId,
    pub call: GatewayToolCall,
    pub input_identity_digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ToolGatewayOutcome {
    Completed {
        status: ToolOperationStatus,
        result_digest: Sha256Digest,
        receipt_digests: Vec<Sha256Digest>,
        satisfies_subtask: bool,
    },
    Pending {
        diagnostic_digest: Sha256Digest,
    },
    Ambiguous {
        diagnostic_digest: Sha256Digest,
    },
}

pub type ToolGatewayFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ToolGatewayOutcome, ToolGatewayError>> + Send + 'a>>;

/// A model-authored call refused before any external effect.
///
/// `result_digest` must name an artifact the model can actually read. A rejection is a tool result
/// like every other tool result: the controller feeds its bytes back into the next model input, so a
/// digest that names nothing does not merely lose the explanation, it fails the whole episode when
/// the context store opens it. Only a component with artifact authority can mint one, which is why
/// this is produced by the gateway rather than by the reducer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolInputRejection {
    pub result_digest: Sha256Digest,
    pub diagnostic: String,
}

pub trait AgentToolGateway: Debug + Send {
    fn descriptor(&self, name: &str) -> Option<RuntimeToolDescriptor>;

    /// Checks a model-authored call before it is authorized or dispatched.
    ///
    /// A defect the model can see and correct — an unknown tool name, malformed JSON, a missing or
    /// unexpected field — is not an infrastructure failure and must not end the migration. It is
    /// deterministic, side-effect-free, and belongs to the model's own turn, so the reducer records
    /// it as a terminal `RejectedAsInvalid` operation and returns the explanation. Implementations
    /// may publish that explanation as an immutable artifact — that is what makes the digest
    /// readable — but must not materialize a candidate, dispatch remote work, or otherwise advance
    /// the migration.
    ///
    /// This method has **no default**, deliberately. It used to default to `Ok(())`, and the
    /// production composition wraps the real gateway in a decorator that forwarded `descriptor`,
    /// `execute`, and `reconcile` but not this — so every call silently skipped validation and
    /// Design 0040's whole correction path was dead in production while its tests passed against
    /// the unwrapped gateway. A defaulted method that quietly disables a safety mechanism is worse
    /// than no method, so omitting it is now a compile error and each implementor must say what it
    /// means.
    ///
    /// # Errors
    ///
    /// Returns the published rejection when the call cannot be dispatched as written.
    fn validate_call(&self, call: &GatewayToolCall) -> Result<(), ToolInputRejection>;

    /// Executes one stable logical operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the fake or adapter cannot process the request.
    #[must_use]
    fn execute<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a>;

    /// Reconciles an operation whose physical outcome is unknown.
    ///
    /// # Errors
    ///
    /// Returns an error when reconciliation cannot be performed.
    #[must_use]
    fn reconcile<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolGatewayAction {
    Execute,
    Reconcile,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScriptedToolStep {
    pub action: ToolGatewayAction,
    pub expected_tool_name: String,
    pub outcome: ToolGatewayOutcome,
}

#[derive(Clone, Debug, Default)]
pub struct ScriptedFakeToolGateway {
    descriptors: BTreeMap<String, RuntimeToolDescriptor>,
    steps: VecDeque<ScriptedToolStep>,
    invocations: Vec<ToolOperationId>,
}

impl ScriptedFakeToolGateway {
    #[must_use]
    pub fn new(
        descriptors: impl IntoIterator<Item = RuntimeToolDescriptor>,
        steps: impl IntoIterator<Item = ScriptedToolStep>,
    ) -> Self {
        Self {
            descriptors: descriptors
                .into_iter()
                .map(|descriptor| (descriptor.name.clone(), descriptor))
                .collect(),
            steps: steps.into_iter().collect(),
            invocations: Vec::new(),
        }
    }

    #[must_use]
    pub fn invocation_count(&self) -> usize {
        self.invocations.len()
    }

    #[must_use]
    pub fn invocation_ids(&self) -> &[ToolOperationId] {
        &self.invocations
    }

    fn invoke(
        &mut self,
        action: ToolGatewayAction,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let step = self
            .steps
            .pop_front()
            .ok_or(ToolGatewayError::ScriptExhausted)?;
        if step.action != action || step.expected_tool_name != request.call.name {
            return Err(ToolGatewayError::UnexpectedRequest);
        }
        self.invocations.push(request.operation_id.clone());
        Ok(step.outcome)
    }
}

impl AgentToolGateway for ScriptedFakeToolGateway {
    fn descriptor(&self, name: &str) -> Option<RuntimeToolDescriptor> {
        self.descriptors.get(name).cloned()
    }

    /// A scripted fake validates nothing: its calls are written by the test, not by a model.
    fn validate_call(&self, _call: &GatewayToolCall) -> Result<(), ToolInputRejection> {
        Ok(())
    }

    fn execute<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
        Box::pin(async move { self.invoke(ToolGatewayAction::Execute, request) })
    }

    fn reconcile<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
        Box::pin(async move { self.invoke(ToolGatewayAction::Reconcile, request) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ToolGatewayError {
    ScriptExhausted,
    UnexpectedRequest,
    Adapter(String),
}

impl Display for ToolGatewayError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScriptExhausted => write!(formatter, "scripted tool gateway is exhausted"),
            Self::UnexpectedRequest => write!(formatter, "scripted tool request did not match"),
            Self::Adapter(message) => write!(formatter, "tool gateway adapter: {message}"),
        }
    }
}

impl Error for ToolGatewayError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentRuntimeFaultPoint {
    AfterModelDispatchCommit,
    AfterModelOutcomeBeforeCommit,
    AfterTurnCommit,
    AfterToolDispatchCommit,
    AfterToolOutcomeBeforeCommit,
    AfterToolResultCommit,
}

pub trait AgentRuntimeFaultInjector {
    fn should_crash(&mut self, point: AgentRuntimeFaultPoint) -> bool;
}

#[derive(Debug, Default)]
pub struct NoAgentRuntimeFault;

impl AgentRuntimeFaultInjector for NoAgentRuntimeFault {
    fn should_crash(&mut self, _point: AgentRuntimeFaultPoint) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct OneShotAgentRuntimeFault {
    point: Option<AgentRuntimeFaultPoint>,
}

impl OneShotAgentRuntimeFault {
    #[must_use]
    pub const fn new(point: AgentRuntimeFaultPoint) -> Self {
        Self { point: Some(point) }
    }
}

impl AgentRuntimeFaultInjector for OneShotAgentRuntimeFault {
    fn should_crash(&mut self, point: AgentRuntimeFaultPoint) -> bool {
        if self.point == Some(point) {
            self.point = None;
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentLoopAdvance {
    Progressed(EpisodeStatus),
    Terminal(EpisodeStatus),
    Suspended,
}
