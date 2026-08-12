# Design 0026: Ascend Build Gate connection

- Status: Implemented
- Date: 2026-08-12
- Scope: one Source-Gate-authorized candidate, one bounded Ascend build attempt, durable waiting,
  reconciliation, and model-visible build feedback

## Context

Design 0025 authorizes Build as the first Gate added after the real Source Gate loop. Two existing
interfaces cannot simply be joined without refinement:

1. an Agent tool operation currently has no state for an acknowledged remote attempt that is still
   running; treating normal worker latency as `Ambiguous` would suspend a healthy Episode;
2. the existing `AscendFixture` worker path is an allowlisted `ascend-add-v1` build-and-run harness.
   It accepts one fixed source schema and must not be presented as evidence that an arbitrary
   reduction candidate compiled.

The shared worker substrate already provides the mechanisms this slice should reuse: immutable
`AssignmentContract`, typed attempt identity/outcomes, CAS input grants, resource limits, fixed
worker capabilities, durable worker journal/outbox, stable container identity, terminal Artifact
publication, and reconciliation. This design adds a new build contract over those mechanisms rather
than weakening the fixed fixture.

## Scope lock

This slice does only the following:

- require an exact passing Source Gate receipt before build dispatch;
- package the immutable candidate files into one bounded build-input Artifact;
- dispatch exactly one `AscendBuild` assignment through a port backed by the worker control plane;
- wait and reconcile by stable attempt identity without blocking one Agent-loop step;
- turn the worker terminal observation into an independent Build Gate receipt;
- return structured build failure to the same Episode so the model may submit a child candidate.

It does not execute the candidate workload, compare CUDA/Ascend results, measure performance,
generalize arbitrary commands, select workers, call a live LLM, or change release authority.

## Decisions

### 1. Build is not an Ascend fixture alias

Add a distinct `ExecutionKind::AscendBuild` and `ascend-build-v1` worker feature. The existing
`AscendFixture` contract and its locally pinned bundle digest remain unchanged. A worker may expose
the build capability only when composed with a pinned build image, fixed build runner, exact CANN
environment facts, resource ceilings, and an explicit sandbox root.

The assignment cannot supply a shell, environment, mount, host path, image, network policy, or
runner. Its variable input is only the controller-authored build bundle.

### 2. The controller authors the build bundle

The model supplies only the candidate-manifest digest and Source Gate receipt digest to
`request_ascend_build`. The tool recomputes the Source Gate over the immutable materialized tree and
requires the supplied receipt digest to match. It then writes a versioned bundle containing:

- candidate, task, migration-spec, manifest, and Source Gate identities;
- every generated path, kind, size, digest, and UTF-8 byte string;
- the fixed public symbol and target architecture needed by the trusted runner.

The worker validates the outer Artifact identity, strict schema, path set, per-file size/digest,
declared build file, and aggregate bounds before create-only materialization. Source Gate authority
is not reimplemented in the worker.

### 3. Remote waiting is a durable tool state

Extend the generic tool lifecycle as follows:

```text
Authorized -> Dispatching -> Running -> Succeeded | CandidateFailed | InfraFailed
                         \-> Ambiguous -> Reconciling -> Running | terminal
```

`Pending` from a tool gateway means the stable logical operation has an acknowledged/reconcilable
remote attempt but no terminal result yet. The reducer persists `Running`; a later Agent-loop
advance calls reconciliation with the same `ToolOperationId`. A crash while still `Dispatching`
retains the existing ambiguous/reconciliation rule.

The tool port is asynchronous because dispatch and observation adapters may perform bounded I/O.
Source-only tools return immediately through the same interface.

### 4. Attempt identity and retry are deterministic

Assignment, attempt, idempotency, bundle, image, candidate, and Source Gate identities are derived
from the stable Tool Operation and controller configuration. Reconciliation may redispatch the
same immutable assignment idempotently; it never invents a second attempt. Lease-expiry
reassignment remains controller policy and is outside this first connection.

### 5. Worker facts are evidence, not the verdict by themselves

The worker publishes bounded stdout, stderr, and its run receipt before reporting terminal state.
The Build Gate adapter validates the terminal observation against the exact assignment and authors
a separate immutable receipt containing:

- candidate/manifest/Source Gate and assignment/attempt identities;
- build-bundle and pinned image identities;
- architecture, CANN, driver, and firmware facts from the trusted worker receipt;
- outcome, exit code, elapsed time, bounded diagnostic, and stdout/stderr/worker-receipt refs.

Only `Succeeded` with exit code zero and the trusted build-complete marker passes. A compiler or
linker failure becomes `CandidateFailed` and is model-visible. Timeout, cancellation,
infrastructure failure, identity mismatch, missing Artifacts, or malformed receipts do not pass and
retain their distinct status.

## Ports

The candidate tool depends on an `AscendBuildAttemptPort`, not on gRPC, SQLite, Docker, or the
server application. The port has asynchronous `dispatch` and `reconcile` operations over the exact
assignment and returns `Pending` or a typed terminal observation. The server composition supplies
the production adapter; deterministic contract fakes prove the Agent behavior without hardware.

The worker build backend continues to depend on the existing `ExecutionBackend`, Artifact input,
publication, device guard, container engine, and journal ports.

## Verification

This slice is complete only when tests prove:

- Build cannot dispatch for a missing, failing, foreign, or mismatched Source Gate receipt;
- the build bundle contains exactly the immutable materialized candidate files;
- assignment fields cannot select command, environment, network, image, mounts, or limits beyond
  policy;
- a pending remote attempt survives repeated Agent-loop advances and crash reconciliation;
- a compiler failure receipt returns to the same Episode and causes a child candidate submission;
- a corrected candidate can reach a passing Build receipt without any Correctness claim;
- fixed `AscendFixture` behavior and all existing worker/server contract suites remain unchanged;
- no real device, provider call, or network access is required by unit tests.

## Implementation order

1. add asynchronous/pending tool-operation semantics and recovery tests;
2. add build bundle, receipt, and attempt-port contracts with deterministic fakes;
3. add the policy-bound `AscendBuild` worker contract and backend;
4. add the worker-control adapter and terminal observation query;
5. run the complete same-Episode build-failure/correction scenario and update the handoff.
