# 0010: Interactive terminal and event stream

- Status: Accepted
- Date: 2026-08-09
- Scope: user-visible run experience, event protocol, Python compatibility, replay, and renderer boundaries

## Context

The bootstrap Python harness exposes an agent run as a sequence of `print()` calls. It prints a
completed assistant message, a shortened tool invocation, and a shortened tool result. This was
enough to observe early experiments, but it is not an adequate product interface for a migration
factory whose work may take hours and whose result must be inspectable while it is being produced.

Important facts already exist below that presentation layer. The worker protocol preserves command
exit code, stdout, and stderr separately. Patch operations create commits and report a diff stat.
Verification tools produce structured receipts. The current loop flattens these facts into strings
and then truncates the observer's view, so users cannot reliably distinguish activity, claims,
changes, failures, and evidence.

The required product behavior is Codex-like: AlloyPort communicates in natural language while it
works, shows commands and their live outcomes, shows source changes as diffs, exposes approvals and
errors, and finishes with an evidence-linked result. This is a product requirement, not optional
logging polish.

## Decision

AlloyPort is event-first. The runtime emits a typed, append-only interaction stream; terminal,
plain-text, JSONL, replay, and future GUI clients are reducers over that stream. Renderers never
infer tool activity by scraping human-formatted logs.

The Rust control plane owns event identity, ordering, persistence, redaction policy, and rendering.
During the transition, the Python executor emits protocol events through a compatibility adapter.
It remains usable without a full rewrite, but direct `print()` output is no longer the source of
truth for a run.

The same semantic stream supports three modes:

- `alloyport run`: interactive terminal UI when attached to a capable TTY;
- non-TTY output: readable, append-only text without cursor control or hidden state;
- `alloyport run --jsonl`: one protocol envelope per stdout line for automation.

`alloyport replay <run-id>` reduces stored events again. Replay must reproduce semantic content and
ordering; visual layout may change with terminal width, theme, or renderer version.

## User experience contract

An interactive run must expose the following without requiring debug mode.

### Agent communication

- Stream concise working updates and the final response as separate narrative items.
- Render Markdown and code blocks, but never expose or require private chain-of-thought.
- Keep narrative visibly distinct from observed evidence. An assistant statement is a claim, not a
  command result or verification verdict.
- Show the active plan and update it in place while preserving prior versions in the event stream.

### Commands

Before execution, show the purpose, exact display command, working directory, execution site, and
approval state. The execution site distinguishes the local controller, an isolated build worker,
the CUDA reference environment, and an Ascend device worker.

While running, stream stdout and stderr as separate ordered channels. On completion, show exit code,
elapsed time, timeout or signal information, and a stable link to the full output artifact. The UI
may collapse or tail large output, but truncation is a presentation choice and must never discard
the stored bytes.

### File changes

Show file changes as unified or split diffs with paths, hunks, line numbers, and insertion/deletion
counts. A collapsed summary such as `kernel.cpp +41/-12` expands to the actual diff. Binary,
oversized, generated, and secret-bearing files use an explicit safe summary rather than pretending
that no change occurred.

For mutating tools, AlloyPort observes the workspace before and after execution and emits the
observed delta. A tool-supplied patch or diff can be shown as a proposal, but only the independently
observed post-state delta can be labeled applied. The applied event records before/after content
digests and, when available, the resulting commit or tree identity.

### Migration evidence

Generic tool cards are insufficient for the product's decisive results. The renderer has first-class
summaries for:

- CUDA intake and unsupported-construct findings;
- generated Ascend C and host/build integration changes;
- compilation, static checks, and target invocation proof;
- correctness oracle verdicts and calibration;
- performance measurements, environment identity, and comparison basis;
- evidence bundle and release-manifest production.

These summaries link to receipts and artifacts. Color or an icon never carries the only indication
of pass/fail status.

### Approvals and intervention

Approval requests are typed events with the proposed action, reason, affected scope, risk class,
and available responses. The command or patch under review remains visible while the user decides.
Headless mode applies an explicit policy and records that policy decision; it must not silently
simulate interactive approval.

## Protocol envelope

Every persisted event uses a stable envelope. Names below are logical fields; the Rust type and
published JSON Schema are authoritative once implemented.

```json
{
  "schema_version": 1,
  "event_id": "019...",
  "run_id": "run_...",
  "task_id": "task_...",
  "turn_id": "turn_...",
  "operation_id": "op_...",
  "parent_operation_id": null,
  "sequence": 42,
  "emitted_at_unix_ms": 1786344930123,
  "producer": {"component": "python-executor", "instance": "worker-2"},
  "authority": "observed",
  "visibility": "user",
  "type": "command.completed",
  "payload": {}
}
```

The Rust ingestor assigns `event_id` and the monotonically increasing per-run `sequence`. A producer
may supply a `producer_sequence` for diagnostics, but cannot choose canonical order. `operation_id`
correlates start, output, approval, and completion events. Nested operations use parent identifiers,
so a high-level verification tool can contain build and device commands without flattening them.

## Implementation progress

The first vertical slice was implemented on 2026-08-10:

- the dependency-light `alloyport-events` crate defines versioned producer frames, canonical
  envelopes, event payloads, a sequencer, lifecycle reducer, JSONL serialization, and plain renderer;
- `alloyport-cli render-events` ingests Python producer JSONL, assigns canonical identity and order,
  validates lifecycle pairs, and emits plain text or canonical JSONL;
- `alloyport-cli event-demo` exercises narrative, tool, command, output, completion, and unified-diff
  rendering without a model or accelerator;
- the bootstrap Python loop emits run, turn, message, tool, warning, error, and terminal events through
  `EventSink`; `run_agent.py --jsonl` reserves stdout for producer frames;
- the isolated worker emits logical command start/output/completion events with working directory and
  execution site; current SSH transport returns output at command completion rather than live chunks;
- local recipe patches and `kernel_write` emit independently observed Git deltas after their commits.

Design 0017 adds the first durable server ingestor: worker command lifecycle and previews are
explicitly translated into canonical SQLite-backed events with stable replay identity, offset
conflict detection, visible disconnect gaps, and controller-restart recovery. Design 0016 provides
the content-addressed terminal output/receipt boundary used by those events.

Design 0019 adds the authorized public replay/subscription RPC, durable run grants, mTLS identity,
bounded delivery, reconnect cursors, and the first controller redaction policy. This does not make
the design fully implemented: streaming provider adapters, approvals, retention scheduling, richer
redaction coverage, and the interactive `ratatui` renderer remain open.

`authority` is one of:

- `narrative`: model- or user-authored communication;
- `reported`: a component's self-description not independently observed;
- `observed`: controller- or worker-observed execution or workspace state;
- `verified`: an accepted gate result backed by referenced evidence.

The renderer uses this distinction, and the state machine enforces it. No narrative event can satisfy
a migration requirement.

## Initial event vocabulary

| Event family | Required event types | Purpose |
| --- | --- | --- |
| Run and turn | `run.started`, `turn.started`, `turn.completed`, `turn.failed`, `run.completed`, `run.failed` | Lifecycle and terminal outcomes |
| Narrative | `message.started`, `message.delta`, `message.completed` | Streamed user-visible text |
| Plan | `plan.updated` | Versioned task steps and status |
| Tool | `tool.started`, `tool.completed`, `tool.failed` | Generic tool lifecycle and structured input/result summary |
| Command | `command.started`, `command.output`, `command.completed` | Exact execution, channel chunks, exit and timing |
| Change | `patch.proposed`, `workspace.delta`, `patch.applied`, `patch.failed` | Proposed versus observed file mutations |
| Approval | `approval.requested`, `approval.resolved` | Human or headless-policy authorization |
| Artifact | `artifact.produced` | Digest, media type, size, retention, and URI/reference |
| Evidence | `gate.started`, `gate.completed`, `measurement.recorded` | Domain verdicts and evidence linkage |
| Context | `context.usage`, `context.compaction` | Long-run context visibility without private reasoning |
| Diagnostic | `warning`, `error` | Recoverable and terminal failures |

Event payloads are typed per event rather than accepting an unbounded common `data` object. Unknown
event types are preserved and ignored by older renderers, not treated as a run failure.

## Command output and backpressure

`command.output` includes stream (`stdout` or `stderr`), byte offset, bytes or decoded text, encoding,
and whether display sanitization occurred. Chunk boundaries carry no semantic meaning. Reducers
reassemble by operation, stream, and byte offset.

The full output is spooled to a content-addressed artifact as it arrives. The interaction stream may
contain bounded preview chunks and a final artifact reference. Under backpressure, the runtime may
coalesce adjacent preview chunks, but it cannot drop command completion, error, file-change, gate,
or approval events. A visible `output_suppressed` count records any preview coalescing.

Child processes never write directly into the JSONL control stream. Their stdout and stderr are
captured by the executor and encoded inside command events. ANSI control sequences, terminal title
escapes, and hyperlinks are sanitized according to policy before display; raw bytes remain subject
to artifact access and secret policy.

## Python executor bridge

The existing harness is migrated incrementally rather than replaced before the interface is usable.

### Step 1: event sink at the loop boundary

Introduce an `EventSink` interface and make the agent loop emit run, turn, message, tool, warning,
and completion events. The current `verbose` output becomes a renderer subscriber. Tests use an
in-memory sink and assert event sequences instead of capturing terminal strings.

The current provider is completion-based, so the first adapter emits one message completion rather
than fake token deltas. When a provider implements `stream()`, it can emit real deltas without
changing the renderer or stored protocol.

### Step 2: structured tool results

Extend each tool with presentation metadata: semantic kind, side-effect class, execution site, and
redacted display input. Replace the universal `dict -> str` result boundary with a `ToolResult` that
contains a model-facing result, user-facing structured facts, artifact references, and nested
operations. The model can continue receiving the complete text it expects during compatibility.

Instrumentation belongs at shared chokepoints:

- the agent loop emits generic tool lifecycle events;
- the box/worker command boundary emits exact command lifecycle and output events;
- the workspace/patch boundary emits proposed and observed change events;
- gate code emits verdict and measurement events.

This avoids adding ad-hoc printing to every tool and allows a single high-level tool call to expose
the commands it actually launches.

### Step 3: transport to Rust

For the first bridge, Python emits JSONL on a dedicated inherited file descriptor. If platform
constraints require stdout, `--events-jsonl` reserves stdout exclusively for protocol frames and
sends process diagnostics to stderr. Arbitrary child output is always captured, never forwarded raw
onto the protocol channel.

Rust validates every frame against the supported schema, attaches canonical envelope fields,
persists it, and broadcasts it to renderers. Invalid frames produce a diagnostic event and fail the
affected operation explicitly; the UI must not silently revert to scraping text.

The protocol is transport-neutral. A later local socket, gRPC stream, WebSocket, or Agent Client
Protocol adapter must carry the same event semantics rather than create another UI-specific model.

## Rust boundaries

The target decomposition is:

```text
Python Executor / Rust-native operations
             |
             v
       Event Ingestor ----> append-only interaction store ----> replay
             |
             v
       Run State Reducer
          /    |     \
         v     v      v
       TUI   plain   JSONL
```

Protocol types should live in a dependency-light Rust crate and derive serialization plus JSON
Schema. Runtime state and rendering types remain separate: terminal width, colors, collapsed state,
spinners, and syntax-highlight caches must never enter persisted event payloads.

The interactive renderer may use `ratatui`, but adopting a full-screen framework is an implementation
choice, not the architecture. Start with a correct reducer and inline/plain rendering, then add
scrollback, folding, mouse selection, clickable paths, and split-diff views without changing
producers.

## Interaction stream versus canonical task state

The interaction stream and Design 0002's audit event log are related but not interchangeable.

- Interaction events answer: what is the system saying and doing now?
- Audit records answer: what verified facts are allowed to advance the durable task state?

Interaction events may reference immutable command receipts, workspace snapshots, gate reports, and
artifacts. Those evidence objects can support an `AuditReport`. Replaying a green terminal card or a
model's final message cannot by itself recreate a verified state transition. This separation keeps
the UI honest and prevents a richer presentation layer from becoming another path around the gates.

## Persistence, replay, and reconnect

- Persist accepted events before acknowledging them to a remote producer when the event is required
  for correctness or lifecycle closure.
- Resume a client from its last applied `sequence`; reducers apply events idempotently by `event_id`.
- After a crash, any operation with a start but no terminal event is materialized as interrupted,
  unless a worker receipt proves a later outcome.
- Store renderer version and protocol version with exports, not with each semantic payload.
- Apply retention independently to interaction previews and immutable evidence. Removing an old UI
  preview must not remove evidence required by a completed requirement.

## Security and privacy

- Producers provide both execution values and redacted display values where secrets may occur; the
  controller applies a second redaction pass.
- Environment variables, credentials, signed URLs, and model-provider headers are deny-by-default
  in user-visible and persisted payloads.
- Approval panels render the exact effective scope after path and command resolution.
- Workspace deltas respect ignore, generated-file, size, binary, and sensitive-path policies while
  still reporting that a hidden change occurred.
- Remote worker identity and target environment are explicit. A command must never appear local when
  it actually ran on an Ascend device host.

## Verification plan

The design is not implemented until automated tests demonstrate:

- narrative, tool, command, change, approval, gate, and terminal events form valid lifecycle pairs;
- arbitrary child stdout containing JSON, ANSI sequences, invalid UTF-8, and partial lines cannot
  corrupt JSONL framing;
- stdout and stderr order is reproducible within the documented ordering guarantees;
- large output remains available as an artifact when its live display is collapsed or coalesced;
- a mutating tool's claimed diff cannot override a different observed workspace delta;
- command failure displays exit code and full-output reference and reaches the model untruncated;
- non-streaming and streaming providers reduce to the same completed message semantics;
- replay and reconnect do not duplicate events or lose terminal outcomes;
- secrets are absent from golden JSONL traces and terminal snapshots;
- a Python-produced trace and an equivalent Rust-native trace render the same semantic transcript;
- interaction events alone cannot advance an audited requirement in the long-horizon state machine.

Golden traces should include a successful CUDA-extension migration, a compiler failure followed by
a source diff, an approval denial, a disconnected Ascend worker, a correctness failure, and a long
command whose output exceeds the live-display budget.

## Lessons adopted from reference systems

Codex's documented non-interactive mode exposes a JSONL stream with thread/turn/item lifecycles and
typed items for agent messages, command executions, file changes, web searches, and plan updates.
The useful lesson is the structured stream shared with automation, not its exact public event names.

Grok Build separates its Rust agent runtime, ACP conversion, headless reducers, and TUI. Its tool
updates carry structured raw input/output and rich diff content; its headless `streaming-json` mode
reduces the same updates to line-delimited events. Its command and edit blocks track incremental
output, completion status, elapsed time, diff hunks, and collapsed summaries. AlloyPort adopts the
separation of semantic protocol from rendering, but adds migration-specific authority and evidence
semantics.

References inspected for this decision:

- [Codex non-interactive mode](https://developers.openai.com/codex/noninteractive/)
- [Codex SDK](https://developers.openai.com/codex/sdk/)
- [Grok Build repository](https://github.com/xai-org/grok-build)
- [Grok Build ACP conversion](https://github.com/xai-org/grok-build/blob/75e73f3d6ac0350d211f12ae7d57c2c0aad72576/crates/codegen/xai-grok-shell/src/session/acp_conversion.rs)
- [Grok Build headless event reducer](https://github.com/xai-org/grok-build/blob/75e73f3d6ac0350d211f12ae7d57c2c0aad72576/crates/codegen/xai-grok-pager/src/headless/reducer/acp.rs)
- [Grok Build command block](https://github.com/xai-org/grok-build/blob/75e73f3d6ac0350d211f12ae7d57c2c0aad72576/crates/codegen/xai-grok-pager/src/scrollback/blocks/tool/execute.rs)
- [Grok Build diff extraction and rendering](https://github.com/xai-org/grok-build/blob/75e73f3d6ac0350d211f12ae7d57c2c0aad72576/crates/codegen/xai-grok-pager/src/diff.rs)

## Rejected alternatives

### Improve the existing `print()` format

Rejected because strings cannot provide stable lifecycle, correlation, replay, machine consumption,
or trustworthy distinction between claimed and observed changes.

### Let the TUI call Python tools directly

Rejected because presentation would become an execution authority and headless or future clients
would require duplicate orchestration logic.

### Store only the final transcript

Rejected because a rendered transcript loses structured inputs, stream boundaries, evidence links,
approval decisions, full outputs, and enough information for a different renderer to replay it.

### Copy Codex or Grok Build event names exactly

Rejected because their vocabularies do not encode AlloyPort's migration contracts, execution sites,
authority levels, oracle evidence, or release gates. They are reference designs, not our protocol.
