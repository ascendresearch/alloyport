# 0025: Durable iterative agent runtime and pluggable LLM protocols

- Status: Accepted; implementation started
- Date: 2026-08-12
- Scope: nested loop ownership, durable agent episodes, provider protocols, model configuration,
  tool execution, candidate search, context, budgets, recovery, and the provisional-code migration
- Supersedes when accepted: Design 0024's fixed DeepSeek transport and one-shot authoring path
- Does not supersede: MigrationSpec, Candidate, Gate, oracle, worker, receipt, release, or knowledge
  authority in Designs 0001–0024
- Research basis: [`../research/agent-runtime-and-provider-study.md`](../research/agent-runtime-and-provider-study.md)

## Decision summary

AlloyPort is a verified search factory driven by an iterative runtime agent. It is not a one-shot
source-generation command and not a wrapper around one provider's agent SDK.

AlloyPort owns five nested loops:

```text
Task delivery (goal, lifecycle, audit, release)
  └─ Candidate search (draft feasible -> refine verified -> select incumbent)
       └─ Agent episode (observe -> decide -> tool -> feedback -> repeat)
            ├─ Model attempt (prepare -> send -> persist/decode)
            └─ Tool operation (authorize -> dispatch/reconcile -> receipt)

Cross-task knowledge observes verified outcomes and supplies scoped retrieval to future searches.
```

The current default runtime model remains `deepseek-v4-pro`, selected by configuration. It has no
special Rust implementation. The same model can be reached through an OpenAI Chat Completions or an
Anthropic Messages deployment, and another model can replace it without changing Agent Episode,
Candidate, Gate, or release semantics.

The three initial protocol kinds are:

- `openai_responses` (user-facing shorthand: `openai`);
- `openai_chat_completions` (shorthand: `openai-chat`);
- `anthropic_messages` (shorthand: `anthropic`).

Each codec preserves its native continuation. Provider-neutral events and tool contracts coexist
with, but do not replace, that native state.

## Context

Design 0024 correctly established a model-neutral migration domain and untrusted generated-source
artifacts, but its first transport hard-coded DeepSeek, bearer authentication, Chat Completions JSON,
one forced tool call, and one CLI invocation. That path cannot consume Source/Build/Correctness/
Performance feedback and cannot prove that another protocol works without rewriting orchestration.

The first attempted correction made the opposite mistake by treating structured one-shot generation
as the desired boundary. That contradicts the product: the runtime model must repeatedly inspect,
propose, invoke controlled tools, observe exact failures, revise candidates, and continue within a
budget. The controller, not final text, decides what the episode and task have achieved.

The research report records the evidence from AlloyPort, `ascend-factory`, EvoKernel, provider
protocols, and durable runtimes. This design turns those findings into implementation constraints.

## Goals

- Let a configured runtime model iteratively work through controlled tools.
- Support the three protocol families without flattening required native continuation.
- Keep model/deployment selection outside domain and Gate semantics.
- Resume after controller, transport, or worker failure without duplicating logical side effects.
- Preserve exact model exchanges and tool receipts without treating reasoning as evidence.
- Separate feasibility drafting, verified refinement, delivery, and cross-task learning.
- Reuse AlloyPort's existing SQLite, CAS, event, outbox, attempt, and worker trust primitives.
- Prove the runtime with deterministic fakes before any live or billable model call.

## Non-goals for the first implementation

- multimodal input, Realtime APIs, MCP discovery, subagents, or arbitrary plugins;
- provider-hosted conversation state or managed agent sessions;
- streaming model output or parallel tool execution;
- transparent model switching inside an episode;
- arbitrary shell access from the migration model;
- learned MCTS/evolution/Q-value selection;
- automatic knowledge promotion;
- changing any Gate, oracle, release, worker, or user-approval authority.

## 1. Loop ownership and durable aggregates

### 1.1 Owners

| Loop | Durable owner | Unit | Stops when |
| --- | --- | --- | --- |
| Model attempt | Model gateway | one exact request attempt | response, classified failure, cancellation, or ambiguity recorded |
| Agent episode | Agent runtime | one pinned model/context/tool session | subtask exit contract, incomplete stop, budget, cancellation, or failure |
| Candidate search | Search controller | one bounded candidate frontier | objective met, budget/stall, cancellation, or blocked infrastructure |
| Task delivery | Task controller under Design 0002/0024 | one migration task | audited goal completion, user decision, or terminal failure |
| Knowledge | curator/policy under Design 0008 | cross-task evidence | entries promoted/retracted by evidence policy, never model say-so |

An Agent Episode is the “fresh executor” of Design 0002. It may contain many model and tool turns.
When it ends, a later episode starts from an explicit evidence handoff rather than hidden reasoning or
an assumed portable chat history.

### 1.2 Aggregate identities

- `TaskId`: existing long-horizon goal and lifecycle owner.
- `SearchRunId`: one drafting/refining frontier and its total search budget.
- `CandidateId`: immutable source revision with parentage, producing episode, and Gate references.
- `EpisodeId`: one pinned runtime-model session for a bounded subtask.
- `TurnId`: one decoded model response within an episode.
- `ModelAttemptId`: one possibly billed external model request.
- `ToolOperationId`: one logical tool action, stable across process restart and reconciliation.

Interaction events project these aggregates for users. They are not the source of truth for Gate or
task state.

## 2. Candidate search is not the provider loop

The Search Controller owns a durable candidate graph. Every node records:

- Candidate and parent identities;
- generating episode and tool operation;
- exact source Artifact roots;
- Source, Build, Correctness, Performance, and Integration verdict references as available;
- structured failure classifications and profiler Artifact references;
- selection score and the policy revision that computed it.

Search has two phases, following the useful separation in EvoKernel:

1. **Drafting:** produce candidates until Source, Build, and Correctness pass. Performance provides no
   selection credit to an infeasible candidate.
2. **Refining:** select only feasible parents, generate bounded child revisions, re-run affected
   Gates, and retain the best independently verified incumbent.

The initial selection policy is deterministic bounded best-first with an incumbent:

- prefer the candidate that advanced the most mandatory Gates;
- within the same feasibility level, prefer fewer repeated failure classes and lower verified cost;
- after feasibility, compare only valid Performance verdicts over the same spec/corpus/environment;
- revert to the incumbent when a child regresses or is unverifiable;
- stop on objective satisfaction, total budget, configured no-progress window, cancellation, or a
  blocker requiring user/infrastructure change.

One search expansion may use one complete Agent Episode. A single episode may also submit serial
child candidates while responding to Gate feedback. The durable candidate graph, not conversation
order, remains canonical. A future learned/tree policy can replace selection without changing the
episode or provider contract.

## 3. Agent Episode contract

### 3.1 Immutable episode snapshot

At creation, an episode captures the digests or values of:

- goal/subtask contract and migration specification;
- initial `ContextProjection` and rendering revision;
- resolved runtime model, deployment, protocol codec, model profile, and generation settings;
- system-prompt revision and exact initial tool catalog;
- workspace/input Artifact roots and candidate/search parentage;
- Agent Loop policy, data-boundary policy, and all remaining budgets.

These values never mutate in place. A changed tool catalog, prompt, model, protocol, or context policy
starts a new episode.

### 3.2 Episode state machine

```text
Created
  -> ReadyForModel
  -> ModelAttemptPending
  -> TurnRecorded
       -> ToolWorkPending -> ReadyForModel --------------------+
       -> StopReview -> ReadyForModel -------------------------+
                       |                                       |
                       +-> Succeeded | Incomplete               |
                                                               |
Any non-terminal -> SuspensionRequested -> Suspended ----------+
Any non-terminal -> CancellationPending -> Cancelled
Any non-terminal -> BudgetExhausted | Failed
```

Rules:

- `ReadyForModel` is the only state that may reserve budget and prepare another model attempt.
- exact raw response bytes and a decoded turn are durable before any tool operation is authorized;
- all tool results for a model-emitted call group are durable before the next request is prepared;
- text without tool calls enters `StopReview`; it never directly completes the episode or task;
- `Succeeded` means only that the episode's bounded `SubtaskContract` exit criteria have durable
  Artifact/receipt references. It does not mean the candidate is releasable;
- missing mandatory subtask results may cause one bounded controller feedback turn, then
  `Incomplete`, rather than an unbounded argument with the model;
- cancellation does not erase in-flight ambiguity. An episode remains suspended for reconciliation
  if a dispatched tool's outcome is unknown.

### 3.3 Provider-neutral turn projection

The runtime persists a small semantic projection for policy and UI:

```rust
pub struct DecodedTurn {
    pub narrative: Vec<BoundedTextSegment>,
    pub tool_calls: Vec<DecodedToolCall>,
    pub stop_reason: NormalizedStopReason,
    pub usage: Option<ModelUsage>,
    pub native_continuation: ArtifactRef,
    pub raw_exchange: ModelExchangeRef,
}

pub struct DecodedToolCall {
    pub native_call_id: String,
    pub name: String,
    pub raw_arguments: BoundedBytes,
    pub parsed_arguments: Option<serde_json::Value>,
}
```

Malformed arguments remain exact. If the native call ID is valid and unique, schema/argument errors
become correlated tool-error results for the next turn. Empty, duplicate, or structurally invalid
call IDs make the provider response invalid and no tool executes.

## 4. Model attempt durability

### 4.1 Attempt state

```text
Prepared -> Dispatching -> Responded -> Decoded
                    |          |
                    |          +-> DecodeFailed
                    +-> ConfirmedNotSent | Failed | Ambiguous | CancelledAmbiguous
```

The prepared record binds exact request bytes, endpoint/deployment/profile digests, request budget,
episode/turn predecessor, and an attempt number. Secrets and authentication headers are excluded.

Once `Dispatching` is committed, a crash may have occurred before or after network send. Recovery
therefore marks the attempt `Ambiguous` unless the transport can prove that no request body was sent
or the protocol exposes a documented idempotency/reconciliation mechanism. Retrying creates a new
linked attempt and consumes the configured ambiguity/billing budget. It never overwrites history or
pretends the new nondeterministic response belongs to the old attempt.

Transport-internal retries are allowed only for failures proven to precede request transmission, or
under explicit protocol idempotency semantics. SDK default retries must be disabled or made visible
when they cannot meet this rule.

### 4.2 Durable ordering and crash behavior

| Crash point | Resume behavior |
| --- | --- |
| before prepared record | no model action exists |
| after prepare, before dispatch claim | dispatcher may claim the same exact attempt |
| after `Dispatching`, before response commit | mark/reconcile as ambiguous; policy may create a new attempt |
| after raw response CAS ingest, before decode | decode the same immutable bytes idempotently |
| after decoded turn, before tool authorization | derive the same Tool Operation IDs and continue |
| after all tool results, before next request | codec rebuilds from the same native continuation and results |
| after terminal decision, before event rendering | reducer re-emits/deduplicates the semantic terminal event |

Model usage, provider request IDs, latency, rate-limit fields, and actual returned model name are
recorded when present. Missing usage is `unknown`, never zero.

## 5. Provider architecture

### 5.1 Separate configuration axes

The terms have distinct meanings:

- **runtime model alias:** operator-facing name such as `deepseek-v4-pro-default`;
- **wire model:** string sent in the protocol request, such as `deepseek-v4-pro`;
- **vendor:** informational ownership/contract boundary, such as `deepseek` or `openai`;
- **deployment:** endpoint, region/data boundary, auth reference, rate limits, and protocol;
- **protocol codec:** Chat Completions, Responses, or Anthropic Messages wire semantics;
- **model profile:** model-family capabilities and schema/reasoning quirks independent of endpoint;
- **generation settings:** typed, validated inference options captured per episode;
- **transport:** bounded HTTP execution, TLS, redirect, proxy, cancellation, and diagnostics.

This follows the useful model/provider/profile distinction found in Pydantic AI while making protocol
kind explicit. A vendor is not a codec, and “OpenAI-compatible” is not a capability guarantee.

### 5.2 Composition boundary

```text
AgentRuntime
    |
    v
ModelGateway -- resolves and persists ModelAttempt
    |
    +--> ProtocolCodec -- native request/response/continuation only
    |
    +--> ModelTransport -- endpoint/auth/TLS/timeouts/byte bounds only
    |
    +--> ModelCatalog -- alias/deployment/profile/settings/capability resolution
```

Logical Rust interfaces are:

```rust
pub trait ProtocolCodec: Send + Sync {
    fn kind(&self) -> ProtocolKind;
    fn prepare(&self, input: NativeTurnInput<'_>) -> Result<PreparedPayload, CodecError>;
    fn decode(
        &self,
        request: &PreparedPayload,
        response: RawModelResponseRef,
    ) -> Result<DecodedTurn, CodecError>;
    fn append_tool_results(
        &self,
        continuation: NativeContinuationRef,
        results: &[ModelVisibleToolResult],
    ) -> Result<NativeContinuation, CodecError>;
}

pub trait ModelTransport: Send + Sync {
    fn dispatch(
        &self,
        deployment: &ResolvedDeployment,
        auth: &ResolvedSecret,
        request: &PreparedPayload,
    ) -> Result<RawModelResponse, TransportOutcome>;
}
```

The codec never reads secrets or executes tools. The transport never interprets tool calls or
completion. The Agent Runtime never branches on vendor/model strings.

### 5.3 Strict versioned configuration

Configuration is schema-versioned and denies unknown fields. A representative shape is:

```json
{
  "schema_version": 1,
  "default_runtime_model": "deepseek-v4-pro-default",
  "runtime_models": {
    "deepseek-v4-pro-default": {
      "wire_model": "deepseek-v4-pro",
      "deployment": "deepseek-official-chat",
      "profile": "deepseek-v4",
      "settings": {
        "max_output_tokens": 16384,
        "temperature_millis": 200,
        "reasoning": {"mode": "enabled", "effort": "high"}
      }
    }
  },
  "deployments": {
    "deepseek-official-chat": {
      "vendor": "deepseek",
      "protocol": {"kind": "openai_chat_completions"},
      "endpoint": "https://api.deepseek.com/chat/completions",
      "auth": {"kind": "bearer_file", "path": "/run/secrets/deepseek-api-key"},
      "data_boundary": "external_provider",
      "conformance_receipt_digest":
        "sha256:6c1d6cedf276ea3fd7486a212fd62b32fbb9ecaa4dea70545486737199dd24ed"
    }
  },
  "profiles": {
    "deepseek-v4": {
      "supported_protocols": ["openai_chat_completions", "anthropic_messages"],
      "supports_tools": true,
      "supports_parallel_tool_calls": true,
      "supports_reasoning": true,
      "tool_schema_dialect": "json_schema",
      "max_context_tokens": 1000000,
      "max_output_tokens": 384000
    }
  }
}
```

This is illustrative, not permission to store a production secret path in a repository. An
Anthropic-format DeepSeek deployment is a second deployment using the same wire model/profile and
`anthropic_messages`; it is not another DeepSeek-specific adapter.

Protocol options use tagged, protocol-specific schemas. There is no arbitrary `extra_body`, custom
header map, or untyped capability dictionary in schema 1. Authentication remains a typed deployment
axis rather than being inferred from protocol or vendor: for example, an Anthropic Messages codec
may be paired with bearer authentication by a compatible deployment without changing the loop.

### 5.4 Capability resolution and conformance

Effective capability is the intersection of:

- what the codec implements;
- what the deployment declares and its conformance receipt proves;
- what the model profile supports;
- what the episode requires.

The model profile includes typed fields for tool calling, parallel call emission, supported schema
dialect, reasoning/continuation behavior, context/output bounds, usage availability, prompt caching,
and allowed generation settings. Startup fails closed when required tools or continuation semantics
are unsupported. Model-name heuristics and a successful `/models` lookup are not proof.

Every deployable adapter path must pass recorded conformance fixtures for:

- exact two-tool-turn correlation;
- malformed and schema-invalid arguments;
- multiple calls, duplicate IDs, empty output, truncation, refusal/content filtering, and unknown
  stop reasons;
- reasoning/thinking continuation preservation where enabled;
- usage and actual-model capture;
- response, diagnostic, and continuation bounds;
- retry and cancellation classification.

Conformance results are environment/config evidence, not Candidate Gates.

## 6. Native continuation for the three protocols

All initial adapters use local replay and provider storage off where supported. The exact raw request
and response bodies are CAS Artifacts. A codec-versioned continuation Artifact contains only the
bounded native items needed for the next request and is digest-chained to its predecessor, episode,
prompt, tool catalog, and resolved model profile.

### 6.1 Two-tool-turn mapping

| Step | `openai_chat_completions` | `openai_responses` | `anthropic_messages` |
| --- | --- | --- | --- |
| Initial | native `messages`; nested `tools[].function` | typed `input` Items; function tools | top-level `system`; content-block messages; `input_schema` tools |
| Tool A call | persist exact assistant message and `tool_calls[].id`; arguments remain raw JSON string | persist every output Item, including reasoning, plus `function_call.call_id` | persist exact assistant blocks including thinking/signature and `tool_use.id/input` |
| Tool A result | append `role=tool` with matching `tool_call_id` | append `function_call_output` with matching `call_id` | append user `tool_result` with matching `tool_use_id` |
| Tool B call/result | repeat complete assistant/tool message pair | repeat complete output/function-output Item sequence | repeat exact assistant/user block sequence |
| Stop proposal | assistant text with no tool calls | output text with no client function call | text/end-turn with no client tool use |

### 6.2 Protocol-specific rules

**OpenAI Chat Completions**

- Preserve complete assistant tool-call messages and correlated tool messages.
- Function arguments are bounded raw strings first and JSON second.
- Compatible-deployment extensions such as `reasoning_content` are allowed only through a typed
  profile/codec revision and are retained when required for the next turn.
- `strict` tool schemas are used only when the conformance profile proves support; local validation
  remains mandatory.

**OpenAI Responses**

- Use typed input/output Items and correlate calls/results by `call_id`.
- In stateless reasoning mode, replay every required reasoning Item and encrypted continuation field;
  do not reduce them to messages or display text.
- `store: false` is the schema-1 default. `previous_response_id` and Conversations are deferred.
- Provider-hosted tools are not exposed in the first slice.

**Anthropic Messages**

- Preserve assistant content-block order exactly and return tool results in a following user message.
- Preserve required thinking/redacted-thinking/signature blocks without displaying or interpreting
  them as evidence.
- Use `x-api-key`, the required API-version header, and only documented typed beta/version options.
- Client tools only are allowed initially; server tools that execute beyond AlloyPort are deferred.

Reasoning/thinking continuation is restricted protocol state. It is not shown in normal interaction
events, used as a Gate input, promoted to knowledge, or required for a later episode handoff.

## 7. Tool boundary

### 7.1 Immutable descriptor

Each episode sees a content-addressed, immutable subset of registered tools. A descriptor binds:

- stable tool name, semantic version, description, input and output JSON Schemas;
- effect class: `read_only`, `candidate_write`, `remote_execution`, or `authority_request`;
- result authority: `narrative`, `reported`, `observed`, or `verified_reference`;
- permission class and whether human confirmation can be required;
- execution site and data boundary;
- timeout, resource, output, Artifact, and monetary budgets;
- idempotency-key derivation, reconciliation strategy, and cancellation support;
- redaction and model-visible projection policy.

The model can request only the pinned subset. It cannot add tools or weaken a descriptor.

### 7.2 Tool operation state

```text
Requested -> RejectedAsInvalid
         -> AwaitingPermission -> Denied
         -> Authorized -> Dispatching -> Succeeded | CandidateFailed | InfraFailed
                                      -> TimedOut | Cancelled | Ambiguous -> Reconciling
```

`ToolOperationId` is derived from episode, turn, native call ID/index, tool version, canonical
arguments, input Artifact identities, and relevant environment identity. Re-observing a completed
logical operation returns its recorded result. An operation in `Dispatching` is reconciled before
retry. Existing worker `AttemptId`, deterministic execution object identity, journal, outbox, and CAS
publication satisfy this rule for remote execution tools.

The promise is one logical operation with idempotent replay/reconciliation. The design does not claim
universal exactly-once physical execution.

### 7.3 Typed model-visible result

Every result records full durable facts and renders a bounded provider-neutral projection:

```rust
pub struct ModelVisibleToolResult {
    pub operation_id: ToolOperationId,
    pub status: ToolResultStatus,
    pub summary: BoundedText,
    pub observations: Vec<TypedObservation>,
    pub artifacts: Vec<ArtifactRef>,
    pub receipts: Vec<ReceiptRef>,
    pub authority: ResultAuthority,
    pub retry_hint: RetryHint,
    pub omitted: Option<OmissionNotice>,
}
```

Failures are distinguished as invalid request, denied, candidate failure, timeout, infrastructure
failure, cancellation, and ambiguous outcome. Truncation or redaction is explicit and points to the
full restricted Artifact where policy allows. Tool exceptions do not crash the loop unless integrity
or state consistency is in doubt.

### 7.4 Staged migration tools

The first end-to-end runtime slice exposes only:

- bounded read/search over declared migration sources and deterministic inspection evidence;
- scoped retrieval of evidence-backed examples and relevant negative knowledge;
- `submit_candidate_bundle`, which validates a complete `GeneratedSourceBundle`, creates a new
  immutable Candidate/CAS root, and assigns no Gate authority;
- `request_source_gate` for that exact Candidate.

A failed Source Gate returns structured feedback and permits another candidate submission in the
same episode while budget remains. Build, Correctness, profiler/Performance, and Integration request
tools are added in that order as their existing independent services are connected. The generic
runtime does not need redesign for those additions.

Never expose tools that directly set a verdict, release a candidate, authorize a goal amendment,
promote/retract knowledge, alter hidden corpus/oracle policy, or access controller/worker secrets.

## 8. Loop policy and completion

`AgentLoopPolicy` is validated and captured at episode creation. It includes:

- maximum model turns and ambiguous model retries;
- input/output/total token limits when reported, plus pessimistic limits when not reported;
- wall time and optional monetary budget;
- maximum tool calls per turn and total tool operations;
- response, continuation, narrative, diagnostic, and model-visible tool-result byte bounds;
- sequential call-group execution and deterministic ordering;
- empty/truncated-turn recovery count;
- semantic repeat/no-progress window;
- provider failure, cancellation, suspension, and completion-review behavior.

The repeat key includes tool version, canonical arguments, relevant Artifact/environment identities,
and Gate state. A completed idempotent operation is replayed, not re-executed. Repetition is blocked
only when the inputs and evidence state have not changed; a justified revision is not mistaken for a
loop.

At `StopReview`, the controller chooses:

- `continue`: required output is still reachable and one bounded feedback turn remains;
- `succeeded`: the episode's subtask exit contract has durable references;
- `incomplete`: the model stopped without satisfying it or no progress remains;
- `suspend`: approval, infrastructure, or reconciliation is required.

Task completion still follows Designs 0002 and 0024. A candidate becomes `Released` only after every
mandatory independent Gate and applicable human policy passes.

## 9. Context, episode handoff, and knowledge

### 9.1 Three separate memories

- **native episode continuation** makes the next protocol turn valid;
- **task context** is a reproducible `ContextProjection` built from canonical state and evidence;
- **cross-task knowledge** is the scoped, tiered store governed by Design 0008.

Native continuation is never canonical task truth or knowledge. The semantic event transcript is not
sufficient to resume the same episode unless its native continuation Artifact also validates.

### 9.2 Context construction

An initial episode projection contains only:

- immutable goal/subtask and MigrationSpec facts;
- declared source and deterministic inspection Artifacts;
- current candidate/frontier summaries with exact references;
- selected Gate failures, diagnostics, negative routes, and profiler evidence;
- hard-scope-matched knowledge entries with tier, counterexamples, staleness, and citations;
- tool descriptions and remaining budgets.

Source, retrieved text, compiler output, and prior model text are delimited as untrusted data. They
cannot modify system instructions, tools, authority, or policy. Prompt inputs and the rendering
revision are content-addressed so the initial request can be reproduced.

### 9.3 Context limit and model change

Schema 1 does not lossy-compact an active native conversation. When its context, policy, or model
limit is reached:

1. seal the episode and preserve exact exchanges/continuation under retention policy;
2. derive an evidence handoff from goal, Candidate graph, verified/observed facts, failures, budgets,
   and selected raw Artifacts;
3. record the handoff inputs and renderer revision;
4. start a new episode, which may use another model/deployment/protocol.

Hidden reasoning is never required or claimed to be portable. Later provider-native compaction can
be added only as a codec capability with replay and omission tests.

### 9.4 Learning

Search outcomes can create `ResearchNote`, `FailurePattern`, or `OptimizationPattern` proposals.
Promotion and reuse follow Design 0008. EvoKernel-style stage-specific value may rank candidates for
retrieval after sufficient evidence exists, but its reward is computed from independent Gate results
and cannot promote an entry, candidate, or release.

## 10. Budgets, cost, and scheduling

Budgets are hierarchical and cannot be borrowed silently:

- Task: total wall time, model cost, device time, and release deadline;
- Search Run: candidate expansions and scarce verification attempts;
- Episode: turns, tokens/cost, tool operations, and context bytes;
- Model Attempt: reserved output tokens, timeout, request/response bytes;
- Tool Operation: timeout, CPU/memory/device/output/Artifact limits.

The controller reserves budget before dispatch and reconciles from receipts afterward. An ambiguous
model attempt is charged pessimistically until provider evidence says otherwise. Pricing is a
versioned deployment fact and may be unknown; provider usage is recorded rather than recomputed from
hard-coded current prices. Rate and concurrency limits apply per deployment/credential pool, while
accelerator scheduling remains under the existing worker controller.

Search budget counts Gate/verification attempts separately from model turns. This preserves the
scarce-resource insight from EvoKernel without hard-coding its fixed budget of 30.

## 11. Security and data boundary

Schema 1 requires:

- HTTPS endpoints except an explicit loopback-only test mode;
- exact parsed endpoint allowlisting, no userinfo/fragments, and redirects disabled;
- TLS verification and minimum policy set by deployment configuration;
- proxy use disabled unless explicitly configured by operations;
- typed auth only: `bearer_file` for OpenAI-style endpoints and `x_api_key_file` for Anthropic-style
  endpoints, with owner-only regular non-symlink secret files;
- protocol-owned headers only; no arbitrary header/body escape hatch;
- secrets supplied outside command arguments and never stored in request Artifacts, events, or
  diagnostics;
- bounded/sanitized error bodies and secret-value redaction;
- declared data boundary and retention policy per deployment;
- restricted access to raw source-bearing exchanges and opaque reasoning continuation.

Provider clients receive no worker, oracle, KB-promotion, release, or Artifact-publisher credential.
Candidate execution remains in per-attempt sandboxes without network or control-plane credentials
under Designs 0007 and 0020.

## 12. Events and observability

The runtime emits typed Design-0010 interaction events for:

- episode/model turn lifecycle;
- model attempt prepared/dispatched/responded/ambiguous with usage and latency;
- narrative summaries with `narrative` authority;
- tool requested/authorized/started/result/reconciliation;
- Candidate creation and Gate result references;
- budget warning/exhaustion, repetition, cancellation, suspension, and terminal review.

Exact request/response/tool bytes stay in CAS and are referenced by digest subject to authorization.
The controller assigns canonical event identity and sequence. Event replay may reconstruct the user
view, but only aggregate records, receipts, and audit events reconstruct authoritative state.

Tracing export is a subscriber. It must be explicitly configured per data boundary and cannot
silently send source or model content to a vendor unrelated to the selected deployment.

## 13. Migration of the provisional implementation

| Current item | Disposition after acceptance |
| --- | --- |
| `CandidateAuthoringRequest` | retain as the initial migration context/domain input; stop treating it as one complete model request |
| `GeneratedSourceBundle` validation | retain; use as `submit_candidate_bundle` input |
| `CandidateProposal` | retain untrusted semantics; bind to Candidate/Episode/Tool Operation rather than one adapter call |
| `ModelInvocation` | replace/extend with resolved alias, wire model, deployment, protocol, profile/settings digests, episode and attempt facts |
| `CandidateAuthor` one-shot trait | remove after fake Episode Runtime is ready |
| `DeepSeekCandidateAuthor` | delete; move Chat JSON into `OpenAiChatCodec`; default selection moves to config |
| `ChatCompletionTransport` | replace with protocol-agnostic bounded `ModelTransport` |
| `DeepSeekTransportConfig`/`DeepSeekCurlTransport` | replace with strict catalog/deployment/auth/HTTP configuration; retain proven secret-file and byte-bound techniques |
| `candidate_command.rs` | turn into composition/bootstrap for a durable episode or remove when controller owns creation |
| `persist_candidate_inputs` | generalize from one exchange to Episode/Model Attempt/Candidate manifests; retain CAS ingestion and exact bytes |
| write-once materialization and verification | retain for immutable Candidate roots |
| `deepseek-transport-config.example.json` | replace with a versioned runtime-model catalog example, clearly non-production |
| current unit tests | keep domain/CAS/security cases; migrate wire fixtures into three codec conformance suites |

No provisional module is extended in place before the fake durable loop proves the new boundaries.

## 14. Implementation sequence after review

This ADR authorizes no code while it remains “Proposed for review.” After acceptance, implementation
must proceed in this order:

1. **Domain and fake gateway:** add IDs, episode/model/tool/search records, reducers, strict model
   catalog types, and a scripted fake codec/transport. No network.
2. **Durable fake loop:** run two tool turns, candidate submission, fake Source Gate failure,
   correction, stop review, restart/fault injection, cancellation, and budget exhaustion.
3. **Codec conformance:** implement fixture-driven Chat Completions, Responses, and Anthropic
   codecs, including native continuation and malformed/duplicate call cases. Still no live calls.
4. **Bounded transport and config:** implement endpoint/auth/TLS/limits, ambiguous-attempt policy,
   and a config-selected `deepseek-v4-pro` default with no vendor branch in the loop.
5. **First real vertical slice:** connect `submit_candidate_bundle` and independent Source Gate to
   the reduction specimen; demonstrate at least one feedback/correction turn.
6. **Gate/search expansion:** connect Build then Correctness, establish Drafting, then add
   Performance/profiler feedback and Refining.
7. **Opt-in live validation:** run explicitly authorized, budget-capped protocol checks and record
   conformance receipts. Live access is never a unit-test requirement.

Implementation status on 2026-08-12: steps 1 through 5 are complete. Step 3 is backed by local
golden fixtures for two successive client-tool turns in all three protocols, exact native
reasoning/thinking replay, malformed arguments, call/result correlation, stop normalization, and
bounded fail-closed decoding. Step 4 adds an independent LLM Provider SDK, async Agent-loop gateway,
strict catalog-selected deployment/auth composition, and a bounded `reqwest`/`rustls` transport.
The previous one-shot DeepSeek/curl path was deleted. Step 5 adds a provider-neutral
`submit_candidate_bundle` adapter, immutable CAS-backed materialization, an independently authored
structural Source Gate receipt, and a deterministic same-episode failure/correction test. All model
tests use deterministic doubles; no live provider validation has been performed.

Source Gate implementation does not begin before steps 1–4 make the feedback loop durable and
provider-neutral.

## 15. Verification matrix

### Runtime and recovery

- scripted model performs tool A, consumes result, performs tool B, consumes result, then stops;
- Source Gate failure causes a new candidate submission rather than episode termination;
- final text cannot satisfy an unmet subtask, Gate, or Task requirement;
- restart at every durable boundary produces no duplicate logical tool operation or lost result;
- ambiguous model dispatch is recorded and charged; retry creates linked history;
- worker/tool reconciliation reuses stable Attempt/Operation identity;
- cancellation, suspension, budget exhaustion, and incomplete stop reduce deterministically;
- event replay is idempotent and cannot advance authoritative state.

### Provider codecs

- golden two-tool-turn fixtures for all three protocols;
- exact native reasoning/thinking continuation preservation where configured;
- malformed JSON, invalid schema, unknown tool, duplicate/missing IDs, multiple calls, empty text,
  truncation, refusal/filtering, unknown stop reason, and oversized payload cases;
- round-trip continuation digest and tool-result correlation tests;
- no codec can execute a tool, read a secret, or decide completion.

### Configuration and security

- unknown fields, unsupported capability combinations, unsafe endpoints, redirect, unapproved proxy,
  insecure/missing/symlinked/permissive secret files, and oversized diagnostics fail closed;
- the same DeepSeek wire model resolves over Chat and Anthropic deployments without loop changes;
- OpenAI Responses, OpenAI Chat, and Anthropic profiles can be selected by config only;
- secrets never appear in argv, CAS exchanges, events, errors, or tracing fixtures;
- raw exchanges and reasoning continuation obey authorization and retention policy.

### Search and authority

- infeasible candidates receive no performance credit;
- candidate parentage/frontier/incumbent survive restart;
- repeated unchanged failures stop under policy, while changed evidence permits another expansion;
- Candidate, model text, provider response, worker log, and interaction event cannot author a Gate;
- memory proposals remain T0 until Design-0008 promotion evidence exists;
- release still requires every Design-0024 Gate.

## 16. Rejected alternatives

### Keep the fixed DeepSeek path and add conditionals

Rejected because vendor, protocol, auth, transport, and model behavior would remain coupled and each
new provider would branch throughout the loop.

### Normalize all protocols into Chat messages

Rejected because Responses reasoning Items and Anthropic thinking/signature blocks may be required
for valid continuation. A lowest-common-denominator transcript is useful for UI, not replay.

### Let an OpenAI/Anthropic SDK or LiteLLM own the loop

Rejected because their completion, retry, session, permission, and tool semantics cannot advance
AlloyPort's domain state safely. They may be implementation details below a codec only if all raw
state and retry behavior remain visible.

### Treat one conversation as candidate search and long-horizon task state

Rejected because context/model changes would erase the frontier, incumbent, budgets, and verified
progress. Provider episodes are disposable; Candidate and Task records are durable.

### Retry everything until it works

Rejected because model requests may be billed after ambiguous failure and tools may have side
effects. Retry is a typed policy over classified attempts and reconciliation.

### Implement every agent feature now

Rejected because the reduction slice needs text, client function tools, local replay, sequential
execution, and bounded search. Streaming, multimodal, MCP, subagents, managed sessions, and learned
search remain explicit extensions.

## Consequences

- The provider seam is larger than one `complete()` trait because correct continuation is a product
  requirement, not an HTTP detail.
- Durable records and conformance tests add work before the next Gate, but prevent a second rewrite
  when correction turns and another provider arrive.
- The default model is operational configuration. DeepSeek can be changed or reached through another
  supported protocol without changing migration or trust semantics.
- Episodes remain bounded and replaceable while iterative behavior is preserved within them.
- Search can later become more sophisticated without granting the model evidence or release
  authority.
- Existing CAS, worker, outbox, event, receipt, and isolation work remains foundational rather than
  being bypassed by an external agent framework.

## Review outcome

The user accepted this design on 2026-08-12. Implementation follows the ordered slices in section
14. The accepted constraints are:

- the five-loop decomposition matches the intended factory;
- AlloyPort, not a provider SDK, owns the Agent Episode;
- native continuation is retained per protocol and episodes are model-pinned;
- `deepseek-v4-pro` is only a configurable default;
- the first real proof is an iterative failure/correction loop, not a successful one-shot response;
- the implementation order and deferred features do not expand the current product slice.
