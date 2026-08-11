# 0015: Typed deterministic fake executor runtime

- Status: Accepted
- Date: 2026-08-10
- Scope: executor domain input/output, bounded preview delivery, timeout/cancellation classification,
  worker-local Artifact spooling, durable terminal ordering, event production, and restart behavior

## Context

The worker control plane durably admits assignments and reports lifecycle messages, but it previously
had no component between `Accepted` and `Finished`. Connecting Docker or accelerator processes at
that point would mix process-management uncertainty with unproven output, cancellation, Artifact,
event, and crash-ordering semantics.

A deterministic fake executor provides the smallest executable boundary that can prove those
invariants without enabling a shell or claiming real candidate execution.

## Decision

`alloyport-worker::executor` translates an already validated and durably stored assignment into a
typed `ExecutorInput`. It contains assignment/task/candidate identities, argv, sandbox-relative
working directory, an ordered environment map, timeout, and output limit. It never accepts a host
path or an unqualified shell string.

`FakeExecutor` consumes an explicit plan of stdout chunks, stderr chunks, and logical delays, then
returns an exit or infrastructure outcome. Stdout and stderr offsets are independent. Preview chunks
flow through a bounded Tokio channel so backpressure is observable and memory use is bounded. The
full streams remain in the executor result subject to the assignment output limit.

Timeout, cancellation, output-limit exhaustion, closed preview receiver, nonzero exit, and
infrastructure failure are distinct terminal classifications. Fake elapsed time is logical: completed
delays sum deterministically and a timeout reports the declared timeout. Wall-clock scheduling and
preview backpressure therefore cannot change a successful fake receipt digest.

## Durable runtime ordering

`FakeExecutionRuntime` operates only on a journal-admitted attempt:

1. claim the attempt in-process so two fake executors cannot run it concurrently;
2. durably transition to `Running`, which creates the started outbox message;
3. execute while producing typed `alloyport-events` producer observations;
4. write complete stdout, stderr, and a JSON fake receipt to the worker-local filesystem CAS;
5. durably transition to `Finished`, atomically storing terminal fields and creating the finished
   outbox message;
6. return Artifact-produced and command-completed observations.

An already finished attempt returns its exact stored terminal result without execution or duplicate
events. A journal-restored `Running` fake attempt may execute the deterministic plan again; local CAS
publication is digest-idempotent. This recovery rule applies only to the side-effect-free fake
executor. A real process executor needs durable process identity and attach/terminate policy before
it can make the same transition.

The runtime emits owner-neutral reference intents for stdout/stderr as `AssignmentOutput` and the
receipt as `Receipt`. These are not server grants. Only the controller may turn them into Design
0014 references after the bytes enter the server Artifact service.

The emitted interaction objects are producer events with observed authority. The server remains
responsible for canonical IDs and ordering under Design 0010; tests verify the events can be accepted
by the canonical sequencer.

## Deliberate limits

The fake runtime is a library component and is not yet launched by `OutboundWorker::run_session` or
the worker binary. Live output chunks are not yet multiplexed onto the gRPC control stream,
control-plane cancellation is not connected to its token, and worker-local spool artifacts are not
uploaded to the remote Artifact service. Reference intents therefore do not yet become durable
server grants.

There is no process/container executor, process identity, OS signal delivery, CPU/memory/disk
enforcement, sandbox provisioning, device access, output coalescing, or spool retention policy. The
fake receipt is a typed bootstrap record, not the signed complete RunReceipt from Design 0007.

## Verification

Tests cover independent byte offsets, bounded-channel backpressure, success, timeout, cancellation,
output exhaustion, stdout/stderr/receipt CAS persistence, event ordering, typed reference intents,
terminal replay without duplicate outbox rows, deterministic recovery from a stored `Running` state,
and rejection of concurrent executors for one attempt.
