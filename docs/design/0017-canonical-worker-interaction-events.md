# 0017: Canonical worker interaction-event ingestion

- Status: Accepted
- Date: 2026-08-10
- Scope: durable event identity, worker-protocol translation, lifecycle deduplication, output offset
  handling, controller restart recovery, and authority boundaries

## Context

Worker control already carried accepted, started, output, finished, and cancellation traffic, while
`alloyport-events` defined the user-visible command and Artifact vocabulary. The server validated and
persisted control lifecycle but discarded live output and did not assign canonical event identity.
Consumers therefore could not replay what a worker did, and attaching a UI directly to gRPC frames
would have coupled presentation to one transient transport session.

Worker protocol messages, interaction events, and durable audit state remain different type systems.
This decision adds an explicit translation boundary; it does not turn transport frames into audit
authority.

## Decision

`SqliteInteractionStore` is an append-only canonical event repository. It stores per-run next
sequence, stable event envelopes, semantic deduplication fingerprints, and raw output-chunk metadata.
The production control service opens it in the same SQLite database file as control state, using
separate tables and a separate connection. Tests can inject an independent store.

An assignment's `task_id` is the interaction `run_id`. The first durable assignment for a task
idempotently appends `run.started`. Worker observations translate as follows:

- `ExecutionStarted` becomes `command.started`, using the immutable stored argv, working directory,
  and stable worker execution site rather than trusting new display fields from the frame;
- `OutputChunk` becomes `command.output`, preserving stdout/stderr, byte offset, decoded text, and
  display-sanitization status while retaining the raw payload for conflict checks;
- terminal stdout, stderr, and receipt become three `artifact.produced` events;
- `ExecutionFinished` becomes `command.completed`, linked to the stdout Artifact.

All translated worker events use `observed` authority. They cannot create `verified` gate events,
oracle verdicts, or Design 0002 audit transitions. Assignment acceptance and cancellation ACK remain
control facts and do not create user-visible command events in this slice.

The server persists lifecycle events before acknowledging their durable worker frames. Control state
is committed first; if the process stops before event append or transport ACK, worker outbox replay
re-enters translation. Stable deduplication then fills a missing event or returns the original
envelope without allocating another sequence.

## Identity and deduplication

Each event has a caller-defined stable semantic key within the task run, such as:

- `task:<task_id>:run-started`;
- `attempt:<attempt_id>:command-started`;
- `attempt:<attempt_id>:output:<stream>:<byte_offset>`;
- `attempt:<attempt_id>:artifact:<role>`;
- `attempt:<attempt_id>:command-completed`.

The stored fingerprint includes schema, run/task/operation correlation, producer component,
authority, visibility, and typed payload. It deliberately excludes observation wall time, producer
sequence, and producer process instance, which may legitimately change across reconnect or process
restart. Reuse with different semantic content is a protocol conflict. The first accepted envelope's
timestamp and producer instance remain canonical.

Canonical sequence allocation, event insertion, and next-sequence update occur in one immediate
SQLite transaction. Reopening the controller returns byte-identical envelopes and continues the
sequence without renumbering.

The repository also exposes bounded pages strictly after a caller's last applied canonical
sequence and a durable high-water mark. `InteractionHub` attaches a run-scoped bounded notification
receiver before reading that high-water mark, replays the captured durable prefix in bounded pages,
then crosses to live delivery. Events inserted during that transition are therefore either in the
captured prefix or already queued; duplicate notifications are discarded by sequence.

Live queues are isolated per run. A full queue never blocks or weakens the authoritative append. A
lagged subscriber is terminated with its last applied canonical sequence and the number of skipped
notifications; it reconnects from that sequence and obtains the events from SQLite. A cursor beyond
the durable high-water mark and any observed sequence gap fail explicitly rather than silently
skipping or inventing events. Semantic duplicate appends do not emit a second notification.

## Output offsets and disconnect gaps

Raw preview chunks are indexed by attempt, stream, and byte offset. An exact replay with identical raw
bytes and semantic frame is idempotent. A changed payload or semantic frame at an existing offset is
a conflict. A previously unseen offset below the stream's durable next offset is an overlapping
conflict.

A forward offset is accepted even when it skips bytes. Live previews are intentionally ephemeral and
a worker may continue running while its control stream is disconnected. The server records the new
chunk, advances to its end, and appends a visible warning describing the missing range. The completed
stdout/stderr Artifact remains the source for full output bytes. A gap is therefore observable but
does not invent bytes or fail an otherwise recoverable execution.

The controller validates that output comes from the worker assigned to the attempt and only while the
attempt is durably `Running`. Unknown streams, cross-worker output, and output before start or after
terminal state are rejected.

## Deliberate limits

There is no public replay/subscription RPC yet. The repository and bounded in-process subscription
foundation are implemented, but the production control service is not yet wired through the hub.
Public transport must first bind a durable run owner to an enrolled identity and apply controller
redaction before returning persisted envelopes.
There is no `run.completed` translation because a task may contain multiple attempts and the task
controller does not yet own a final run verdict. Rejection, cancellation-request, and lease-expiry
diagnostics are not yet rendered as first-class interaction events.

Output text currently applies UTF-8 lossy decoding and propagates the worker sanitization flag. A
controller redaction/ANSI policy, preview coalescing budget, retention scheduler, public transport,
and user/task ownership authorization remain future work. Canonical interaction rows do not replace
immutable output Artifacts or audit/evidence records.

## Verification

Repository tests cover canonical sequencing, semantic replay across changed timestamps/instances,
conflicting dedup keys, raw output replay, changed-byte conflicts, forward gaps, and SQLite reopen.
Subscription tests cover one-event replay pages, the replay-to-live transition, explicit
slow-consumer termination and durable cursor resume, invalid future cursors, and per-run pressure
isolation.
The loopback fake-execution test reduces the persisted run/command/output/artifact/completion stream
through `RunReducer`. Controller restart coverage verifies that canonical envelopes survive without
duplication or renumbering.
