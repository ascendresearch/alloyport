# Agent runtime and pluggable-provider research report

- Started: 2026-08-12
- Completed: 2026-08-12
- Status: Complete for Design 0025
- Scope: AlloyPort's iterative runtime agent, provider boundary, durable execution, candidate search,
  and evidence-backed memory
- Implementation freeze: remains active until Design 0025 is reviewed by the user

## Executive finding

AlloyPort must not be designed as a one-shot source generator, and it must not make one provider SDK
the owner of the product loop. It is a verified search factory with five nested control loops:

1. a single provider request and response attempt;
2. a bounded agent episode that repeatedly calls tools and consumes their results;
3. a candidate search that drafts a feasible implementation and then refines verified candidates;
4. a task delivery loop that advances only through independent audit and Gates;
5. a cross-task knowledge loop that promotes lessons only after evidence-backed review.

`ascend-factory` proves the value of an iterative bare-model driver, a master-owned trust boundary,
ephemeral execution, receipts, and incident-derived loop policy. Its current Python harness is not a
durable runtime and its provider-neutral `Message` representation is too lossy for all three target
protocols. EvoKernel proves that drafting and refining need different objectives, that failed and
successful trajectories both matter, and that the search state must outlive a chat. Mature agent
runtimes reinforce checkpointing and idempotency, but none can own AlloyPort's Gate, release, or
knowledge authority without weakening the existing architecture.

The resulting design direction is:

- AlloyPort owns the loop and durable state machine;
- model, vendor, deployment, protocol, model profile, authentication, and transport are separate;
- each bounded episode pins one resolved configuration and preserves its protocol-native
  continuation;
- provider-neutral events are an audit/interaction projection, not a lossy replacement for native
  continuation;
- tool operations use durable identities, explicit authority, and tool-specific reconciliation;
- a model's final text is only a stop proposal;
- candidate feasibility, optimization, task completion, and reusable knowledge are decided outside
  the model.

## Research method and source status

Primary documentation, papers, and source code were preferred. Frameworks were studied for design
lessons, not selected as dependencies.

| Source family | Status | What was inspected |
| --- | --- | --- |
| AlloyPort Designs 0001, 0002, 0005–0010, 0015–0024 | Read | authority, canonical state, events, CAS, worker recovery, product slice |
| AlloyPort provisional candidate-authoring code | Read | one-shot and DeepSeek/Chat/auth/transport coupling; reusable CAS/domain pieces |
| `ascend-factory` positioning and architecture docs | Read | product identity, PORT/CURATE/REVIEW planes, observer boundary, incidents |
| `ascend-factory/harness/loop.py` and tests | Read | iterative policy and current durability/completion gaps |
| `ascend-factory/harness/providers/*` and config | Read | intended registry, implemented Chat codec, model/provider/protocol aliasing |
| `ascend-factory` tools, events, workspace, worker, box and KB gates | Read | tool contracts, receipts, isolation, event shape and trust placement |
| EvoKernel paper and official project page | Read | memory MDP, two-stage search, rewards, budgets and verifier design |
| EvoKernel implementation | Unavailable | the official paper/project page exposes results and data, but no official source repository was found |
| OpenAI Chat, Responses, reasoning, function calling and Agents SDK | Read | wire state, client/server continuation, loop/provider/session seams |
| Anthropic Messages, tool use, thinking and SDK/tool-runner guidance | Read | content blocks, manual loop, permissions, thinking preservation |
| DeepSeek V4 Chat and Anthropic compatibility documentation | Read | one vendor exposing two protocols and protocol-specific capability gaps |
| LangGraph durable execution and persistence | Read | checkpoints, pending writes, restart and idempotent tasks |
| Temporal durable-execution guidance | Read | deterministic orchestration and retryable external activities |
| Pydantic AI model/provider/profile abstraction | Read | separation of API shape, deployment/auth, and model-family capabilities |
| LiteLLM documentation | Read for contrast | broad OpenAI-shaped normalization and its lowest-common-denominator risk |

## Evidence ledger

The type labels below are normative: **observation** means repository behavior, **external fact**
means a primary source claim, and **inference** is an AlloyPort design conclusion.

### Product and authority

- **E-001 — observation:** AlloyPort is a verified CUDA-to-Ascend-C migration factory. Designs 0001,
  0005–0009, and 0024 put candidate generation outside Gate, oracle, release, and knowledge authority.
- **E-002 — observation:** Design 0002 already defines a long-horizon Manage–Execute–Audit loop in
  which a fresh, bounded executor may iterate locally, but its report cannot advance canonical state.
- **E-003 — inference:** “fresh executor” in Design 0002 means a fresh bounded *episode*, not a fresh
  provider request. Protocol continuation is retained inside an episode and discarded at the next
  evidence handoff. This resolves the apparent conflict between bounded contexts and iterative work.
- **E-004 — observation:** `ascend-factory` describes the runtime LLM as the driver that proposes and
  acts, while the oracle judges and the knowledge system remembers. The observer improves the
  harness, tools, and Gates rather than steering the model through a target implementation.
- **E-005 — observation:** `ascend-factory` teaching cases show recurring failures from
  self-certifying evidence, unpassable Gates, missing tools, weak instrument calibration, silent
  provenance loss, prescriptive hints, and confusing the observer with the driver.
- **E-006 — inference:** Model text, reasoning, self-assessment, and final answers are narrative. A
  controller may use them as proposals, but only independently produced receipts and verdicts can
  change task, release, or knowledge authority.

### Agent loop and tool behavior

- **E-007 — observation:** `ascend-factory/harness/loop.py` performs the intended iterative cycle:
  model response, tool validation/execution, tool-result feedback, and another model turn.
- **E-008 — observation:** its empty-turn recovery, truncation nudge, exact repeated-call breaker,
  remaining-turn feedback, faithful malformed-argument diagnostic, and event-driven knowledge
  write-back nudge were learned from actual failed runs rather than invented as generic features.
- **E-009 — observation:** the current loop is in memory, sequential, string-result based, and treats
  final text as completion. It has no crash-consistent model/tool ledger, no idempotent resume, no
  durable candidate frontier, and no Gate-backed terminal decision.
- **E-010 — observation:** `ascend-factory/harness/tools.py` and its runtime keep execution and Gate
  authority in the harness. Receipts are produced by controlled operations; the model cannot author
  them. The worker/box receives only a bounded execution bundle and returns exact outputs.
- **E-011 — inference:** every model-visible tool requires a versioned schema, immutable catalog
  digest, effect and authority class, permission, timeout/resource budget, output bound, durable
  operation ID, and recovery policy. A generic function callback is insufficient.

### Provider and conversation semantics

- **E-012 — observation:** the provisional AlloyPort path hard-codes DeepSeek, `deepseek-v4-pro`, the
  Chat Completions endpoint, bearer authentication, Chat JSON, one forced submission tool, and a
  one-shot CLI composition across four modules.
- **E-013 — observation:** `ascend-factory` correctly begins to separate an alias, provider instance,
  protocol kind, and wire model, but only its `openai_chat` adapter exists. Its common `Message`
  model drops protocol-native continuation details.
- **E-014 — external fact:** OpenAI Chat Completions, OpenAI Responses, and Anthropic Messages encode
  tool calls, tool results, and continuation differently. Responses uses typed Items and `call_id`;
  Anthropic uses ordered assistant/user content blocks and `tool_use_id`; Chat uses assistant
  `tool_calls` and `role=tool` messages.
- **E-015 — external fact:** stateless OpenAI Responses reasoning workflows may require replaying all
  returned reasoning Items, including encrypted continuation content. Anthropic tool/thinking
  workflows require preserving the relevant thinking blocks, signatures, and ordering.
- **E-016 — external fact:** DeepSeek V4 exposes both OpenAI Chat Completions and Anthropic-format
  endpoints for the same `deepseek-v4-pro` model, with different field support. Therefore DeepSeek is
  a vendor/deployment, not a protocol codec.
- **E-017 — inference:** a normalized semantic transcript cannot be the only next-turn state. Store
  exact bounded raw exchanges and codec-versioned native continuation, while separately emitting
  provider-neutral semantic events.
- **E-018 — inference:** a model/deployment/protocol/profile is pinned for one episode. Cross-model
  fallback starts a new episode from an explicit evidence handoff; it must not translate hidden or
  opaque reasoning state and pretend the conversation is continuous.

### Durable execution and recovery

- **E-019 — external fact:** LangGraph checkpoints task results and warns that interrupted or failed
  work may be re-executed; external calls and side effects still need idempotency keys or an existing
  result check. Its pending-write mechanism avoids rerunning already completed peers.
- **E-020 — external fact:** durable workflow systems separate deterministic orchestration history
  from retryable external activities. “Resume” does not create a universal exactly-once guarantee
  for an arbitrary remote side effect.
- **E-021 — observation:** AlloyPort already has stronger domain-specific primitives than the Python
  harness: stable attempt identities, SQLite-backed control/event state, CAS artifacts, outbox replay,
  offset deduplication, write-once materialization, and worker reconciliation after restart.
- **E-022 — inference:** model calls and tool operations must be separate durable records. A request
  lost after network send is a possibly billed ambiguous model attempt; a tool lost after dispatch
  is reconciled by its stable operation/attempt identity before any retry.
- **E-023 — inference:** the achievable promise is exactly one logical tool operation with
  tool-specific idempotency/reconciliation, not magical exactly-once physical execution.

### Search and memory

- **E-024 — external fact:** EvoKernel models synthesis as a memory-based MDP and separates cold-start
  drafting from continual refining. Drafting reward is feasibility; refining reward is latency
  improvement after feasibility.
- **E-025 — external fact:** EvoKernel retrieves heterogeneous API templates, success/failure
  summaries, traces, best practices, child candidates, and profiler feedback, with stage-specific
  value filtering. It preserves feasible variants in the search space and uses a bounded total
  verification budget.
- **E-026 — external fact:** EvoKernel's verifier returns anti-hack, compilation, correctness, and
  latency separately. Its evaluation shows value-guided cross-task memory can materially improve
  frontier models, while weaker models benefit much less.
- **E-027 — inference:** a provider turn loop and a candidate search loop are different. One search
  action may be a complete agent episode. Candidate lineage, feasibility, incumbent selection,
  profiler feedback, budget, and stopping policy must persist outside model conversation state.
- **E-028 — inference:** AlloyPort should adopt EvoKernel's phase-specific retrieval and reward
  separation, but not copy fixed tolerances, an LLM anti-hack auditor as sole authority, or online
  memory promotion without AlloyPort receipts and scope review.

### Framework and abstraction lessons

- **E-029 — external fact:** OpenAI Agents SDK owns a run loop and supports client-managed histories,
  sessions, or OpenAI server-managed continuation. It also exposes a `ModelProvider`, but recommends
  validating feature differences across Responses, Chat, and third-party adapters.
- **E-030 — external fact:** Anthropic's SDK tool runner owns the loop, but its documentation directs
  applications needing approval, conditional execution, or custom logging to use a manual loop.
- **E-031 — external fact:** Pydantic AI distinguishes model/API implementation, provider
  authentication/endpoint, and a model profile describing request quirks and capabilities. This is a
  better conceptual split than “one provider class per vendor.”
- **E-032 — inference:** AlloyPort should adopt these seams conceptually, but not delegate its durable
  state, tool permissions, or completion semantics to an agent SDK. Current Rust/SQLite/CAS/outbox
  infrastructure is the appropriate first implementation base.

## Detailed findings

### 1. What to retain from `ascend-factory`

| Area | Retain | Correct before reuse |
| --- | --- | --- |
| Positioning | bare LLM iterates; harness supplies instruments; oracle judges | make task/search/episode loops explicit |
| Trust | master owns receipts, Gates, oracle, KB and release | encode authority in typed records, not conventions |
| Workspace | immutable upstream/oracle and isolated candidate workspace | bind AlloyPort CAS/attempt identities directly |
| Tools | narrow tools, truthful errors, bounded outputs, learned contracts | typed results, effect classes, idempotency and recovery |
| Loop policy | bounded turns, empty/truncation recovery, repeat breaker, budget feedback | persist every decision and do not equate text with success |
| Events | run/task/turn/operation correlation and presentation-neutral frames | use AlloyPort's canonical sequencer and keep events non-authoritative |
| Worker | ephemeral execution surface, exact stdout/stderr/exit, no hidden trust assets | use existing per-attempt supervisor and receipt contracts |
| Knowledge | propose/promote/retract and event-driven capture | derive promotion only from AlloyPort verdicts and scope checks |
| Providers | registry and alias → protocol/model resolution direction | implement native codecs and preserve native continuation |

The predecessor's most important lesson is not a particular prompt. It is that the model receives
enough truthful instruments to make progress, while all authority remains outside the loop. Its most
important deficiency is that this behavior exists as an in-memory Python conversation rather than a
durable search runtime.

### 2. What EvoKernel changes in the design

EvoKernel prevents us from treating “agent loop” as one flat repeated conversation. It separates:

- **drafting:** find any candidate that passes anti-hack/build/correctness;
- **refining:** begin only from feasible candidates and optimize verified latency;
- **retrieval:** choose different memory for feasibility and optimization;
- **evaluation:** return structured failure/performance feedback;
- **search state:** retain candidate variants and an incumbent outside the LLM;
- **budget:** count scarce verification attempts across both stages.

For AlloyPort, the verifier is stronger and more explicit: Source, Build, Correctness, Performance,
and Integration remain independent Gates. A failed correctness candidate receives no performance
reward. Stage-specific value may later rank evidence for retrieval, but it never promotes a memory
entry or candidate by itself.

### 3. Protocol mapping through two tool turns

The following sequence is the minimum conformance case for every codec: the model requests tool A,
receives result A, requests tool B, receives result B, and then proposes a stop.

| Step | OpenAI Chat Completions | OpenAI Responses | Anthropic Messages |
| --- | --- | --- | --- |
| Initial request | `messages` plus nested function tools | typed `input` Items plus function tools | top-level `system`, `messages`, `tools[].input_schema` |
| Tool A response | exact assistant message with `tool_calls[].id` and JSON-string arguments | preserve complete `output`, including reasoning Item and `function_call.call_id` | preserve exact assistant blocks, including thinking/signature and `tool_use.id` |
| Result A | append `role=tool` with `tool_call_id` | append `function_call_output` with the same `call_id` | append user `tool_result` with the same `tool_use_id` |
| Tool B response | append the next exact assistant tool-call message | preserve the next complete output Item sequence | preserve the next exact assistant block sequence |
| Result B | another correlated tool message | another correlated function-call-output Item | another correlated user tool-result block |
| Final response | assistant text/no tool call is a stop proposal | output text/no client function call is a stop proposal | text/`end_turn` without client tool use is a stop proposal |

The codec must retain unknown-but-allowed native fields needed for continuation, while rejecting
unbounded arbitrary JSON configuration. Exact raw bodies are stored in CAS. Normalized events expose
only bounded narrative, tool intents, usage, stop reasons, and Artifact references.

### 4. Why AlloyPort owns the loop

OpenAI and Anthropic SDKs are useful when the application wants the SDK to decide when to execute a
tool and when text is final. AlloyPort requires different semantics:

- tool permission depends on task contracts and worker trust boundaries;
- an execution may survive process, network, or worker restart;
- model text cannot complete a Gate or task;
- every source/build/oracle/performance operation must bind immutable identities;
- the candidate search tree and long-horizon task state outlive an SDK session;
- local/private deployments and three protocol families must behave consistently.

Therefore SDKs may be used below a future codec only as HTTP/API clients if they do not hide retries,
state, or raw response fields. They cannot be the orchestration authority.

### 5. Durable commit boundaries

The research supports these required boundaries:

```text
persist prepared model attempt
    -> send request
    -> persist exact response (or ambiguous/failure outcome)
    -> decode and persist semantic turn + native continuation
    -> validate tool calls against pinned catalog
    -> persist logical tool operations and authorization
    -> dispatch/reconcile each operation
    -> persist full results/receipts
    -> render bounded protocol-native tool results
    -> next model attempt
```

A crash before request send is retryable with the same prepared attempt. A crash or timeout after
send creates an ambiguous model attempt and a new attempt only if policy and budget permit. A crash
after tool dispatch requires reconciliation; the controller does not call the tool again merely
because no result was observed yet.

### 6. Context and memory boundaries

There are three different forms of memory:

- **episode continuation:** exact protocol-native state needed for the next model turn;
- **task context:** a reproducible `ContextProjection` of goal, current subtask, verified facts,
  candidate state, failure evidence, and budgets;
- **cross-task knowledge:** scoped, evidence-backed facts/procedures/patterns under Design 0008.

They are not interchangeable. Protocol continuation is never promoted to knowledge. When an episode
reaches its context or policy limit, AlloyPort seals it and starts a fresh episode from an explicit
evidence handoff; it does not silently summarize an active native conversation. Retrieved knowledge
enters a task as a hypothesis until current evidence confirms applicability.

## Nested loop model selected for Design 0025

```text
Task delivery (goal, lifecycle, audit, release)
  └─ Candidate search (draft feasible -> refine verified -> select incumbent)
       └─ Agent episode (observe -> decide -> tool -> feedback -> repeat)
            ├─ Provider attempt (prepare -> send -> persist/decode)
            └─ Tool operation (authorize -> dispatch/reconcile -> receipt)

Cross-task knowledge observes completed Gate outcomes and supplies scoped retrieval to future search.
```

Each loop has a different owner and stopping rule. Collapsing any two recreates one of the failures
seen in the provisional implementation or predecessor harness.

## Decisions resolved by the research

- AlloyPort owns the iterative agent loop.
- First implementation uses locally managed protocol-native replay; provider-hosted conversation
  state is deferred.
- One episode pins model, deployment, protocol, profile, prompt, tool catalog, and policy.
- Cross-model/provider change starts a new episode with a durable evidence handoff.
- Tool calls execute sequentially in the first slice even if a provider emits several; results from
  one assistant group are returned together and order is deterministic.
- First transport mode is non-streaming. Future streaming produces preview events, while only the
  assembled terminal response advances state.
- Existing SQLite/CAS/outbox/attempt infrastructure is extended rather than importing LangGraph,
  Temporal, OpenAI Agents SDK, Anthropic Agent SDK, Pydantic AI, or LiteLLM.
- `deepseek-v4-pro` remains a configurable default model profile, not a Rust constant or special
  code path.
- The first search policy is bounded and deterministic; EvoKernel-style learned value retrieval is
  a later evidence-backed policy, not part of provider implementation.

## Deliberately deferred

- server-hosted Conversations/`previous_response_id` and managed-agent sessions;
- mid-episode protocol or model switching;
- streaming model responses;
- parallel tool execution;
- server-side provider tools that bypass AlloyPort's worker/tool boundary;
- multimodal messages, MCP tool discovery, subagents, and arbitrary user plugins;
- learned MCTS/evolution/Q-value search policy;
- lossy in-place compaction of active protocol-native continuation;
- automatic memory promotion or LLM-only anti-hack verdicts.

These are extension points, not missing requirements for the first verified migration slice.

## Design traps rejected

- equating the default model with a provider or protocol;
- defining “provider-neutral” as one lowest-common-denominator message list;
- implementing only the first successful generation;
- equating no tool call or final text with product completion;
- retrying a request or side effect without ambiguity and idempotency policy;
- letting an SDK, worker, model, or interaction event own Gate state;
- copying `ascend-factory`'s provisional weaknesses along with its learned lessons;
- copying EvoKernel's fixed oracle/tolerances or memory reward into a stronger trust model;
- building a universal agent platform before the reduction migration slice works.

## Design acceptance gate

The research portion is complete. Design 0025 may be presented for review only after it contains:

- the five nested loops and their aggregate ownership;
- complete durable episode/model-attempt/tool-operation state machines and crash behavior;
- two-tool-turn mappings for all three protocols;
- typed model/deployment/protocol/profile/auth/transport configuration;
- tool authority, permission, idempotency, result, and cancellation contracts;
- context, data-boundary, budget, cost, and retrieval rules;
- candidate drafting/refining/search semantics and Gate-based stopping;
- migration disposition for every provisional module;
- fake-loop, codec, restart/fault, security, and search-controller tests;
- synchronized work order, handoff, README, and design index.

Implementation remains frozen until the user reviews that design.

## Primary external sources

- [EvoKernel paper](https://arxiv.org/abs/2603.10846) and
  [official project page](https://evokernel.zhuo.li/)
- [OpenAI function calling](https://developers.openai.com/api/docs/guides/function-calling),
  [Responses migration and state](https://developers.openai.com/api/docs/guides/migrate-to-responses),
  [reasoning guidance](https://developers.openai.com/api/docs/guides/reasoning),
  [Agents SDK run loop and sessions](https://openai.github.io/openai-agents-python/running_agents/),
  and [Agents SDK model/provider guidance](https://openai.github.io/openai-agents-python/models/)
- [Anthropic Messages API](https://platform.claude.com/docs/en/api/messages/create),
  [tool-use loop](https://platform.claude.com/docs/en/agents-and-tools/tool-use/how-tool-use-works),
  [extended thinking](https://platform.claude.com/docs/en/build-with-claude/extended-thinking), and
  [manual loop versus tool runner](https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-runner)
- [DeepSeek V4 protocol announcement](https://api-docs.deepseek.com/updates/),
  [Chat Completions schema](https://api-docs.deepseek.com/api/create-chat-completion), and
  [Anthropic-format compatibility](https://api-docs.deepseek.com/guides/anthropic_api)
- [LangGraph durable functional API](https://docs.langchain.com/oss/python/langgraph/functional-api)
  and [persistence model](https://docs.langchain.com/oss/python/langgraph/persistence)
- [Temporal durable execution overview](https://docs.temporal.io/)
- [Pydantic AI model/provider/profile overview](https://pydantic.dev/docs/ai/models/overview/)
- [LiteLLM normalization model](https://docs.litellm.ai/)
