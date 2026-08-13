# Design 0034: Durable provider turn context

- Status: Implemented persistence slice
- Date: 2026-08-12
- Extends: Designs 0025 and 0033
- Scope: native continuation, exact exchange Artifacts, and model-visible tool-result recovery

## Context

The durable Episode repository can recover reducer state, but `ProviderModelGateway` also requires
the exact protocol-native continuation and the results of every pending tool call. Its only prior
context store supplied one in-memory turn. A process restart therefore could not construct the next
provider request, and an independently persisted reducer input digest could drift from provider
context.

## Decisions

`alloyport-server` now owns a `SqliteModelContextStore` backed by the immutable Artifact store. It
creates one immutable initial prompt/tool context, records every exact native request, response, and
continuation Artifact, and indexes each committed exchange by model-attempt and Episode/turn
identity. Recommitting an identical exchange is idempotent; changed bytes or identities conflict.

Pending native call IDs are stored in continuation order. `ContextRecordingToolGateway` wraps the
real Candidate tool gateway and commits each terminal result Artifact before returning success to
the reducer. Reconciliation may repeat the same result, but a different result for the same native
call fails closed. Only after every pending call has a result does the store derive the next model
input identity using the same `derive_model_continuation_input_digest` function as the reducer.
Loading a later turn requires that exact digest and rechecks continuation bounds, Artifact digests,
UTF-8 tool output, cardinality, order, and call identity.

Tool definitions are now strict serializable configuration values so the context store can recover
the exact catalog supplied on the first turn. Provider request and response bodies remain Artifact
data; `SQLite` stores only their digests and correlation facts.

## Scope boundary

This slice does not start a model call. A controller application use case must still resolve the
runtime-model catalog, construct matching Episode and context records, own the runner to a terminal
state, compose the Candidate/Build/Correctness tool ports, and expose explicit operator budgets.
Live provider validation and candidate acceptance remain unclaimed.
