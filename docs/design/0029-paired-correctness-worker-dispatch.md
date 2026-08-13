# Design 0029: Paired correctness worker dispatch

- Status: Implemented control-plane slice
- Date: 2026-08-12
- Extends: Designs 0011, 0016, 0027, and 0028
- Scope: typed dual assignments, durable reconciliation, and terminal receipt validation

## Context

Design 0028 made the exact reference and DUT inputs dispatchable as separate immutable Artifacts.
The remaining controller gap was a production implementation of `ReductionCorrectnessAttemptPort`.
The existing worker protocol and repository already provide durable assignment identity, replay,
terminal Artifact publication, and partial-dispatch recovery; correctness should extend those
contracts rather than introduce a second scheduler or terminal protocol.

## Decisions

### 1. Protocol minor 6 adds two default-deny execution kinds

The shared execution vocabulary and worker wire protocol add:

- `CudaCorrectness` / `EXECUTOR_KIND_CUDA_CORRECTNESS = 7`;
- `AscendCorrectness` / `EXECUTOR_KIND_ASCEND_CORRECTNESS = 8`.

Their features are `cuda-reduction-correctness-v1` and
`ascend-reduction-correctness-v1`. Hello validation binds each feature to its accelerator backend.
Worker admission remains default-deny and provides role-specific exclusive policies; merely knowing
the enum value does not authorize execution.

### 2. One Port operation becomes two independent assignments

`WorkerCorrectnessAttemptAdapter` is configured with distinct CUDA and Ascend worker identities,
role-specific pinned image descriptors, offline one-device resource contracts, and timeouts. It
derives stable assignment and attempt IDs from the experiment, role, execution bundle, and image.

Both assignments have exactly one fixed argv token, an empty environment, sandbox-relative working
directory, disabled network, and one role feature. The server grants each worker read access only to
its own execution bundle. Dispatch and reconciliation may re-enqueue both assignments because the
existing control repository treats exact repeats idempotently; this also recovers a crash after only
one side became durable.

### 3. Existing terminal fields carry the structured run receipt

No protobuf terminal field is added. A trusted correctness runner writes exactly one serialized
`ReductionRunReceipt` to stdout and diagnostics to stderr. The normal worker runtime publishes its
generic worker receipt plus the complete stdout and stderr Artifacts.

The paired adapter validates the generic receipt against the stored assignment and terminal outcome,
including the stdout digest. It then parses the stdout Artifact and validates experiment, role,
candidate, corpus, and implementation identity against the assigned execution bundle. Only those
validated stdout descriptors cross the correctness-attempt Port.

### 4. Pair state remains fail closed

The Port returns pending until both durable assignments finish. Rejection, cancellation, lease
expiry, missing terminal observations, missing Artifacts, changed assignment identity, nonzero
execution, malformed JSON, or crossed run identity cannot become correctness evidence. The
controller comparator remains the sole verdict authority after both runs are available.

## Rejected alternatives

- One assignment that can access both bundles or devices: it would weaken execution independence.
- A generic container executor: server-selected argv, mounts, or environment would expand authority.
- Put the run receipt in the worker receipt: the generic runtime receipt and operator-specific
  observations have different schemas and evolution owners.
- Parse a live output preview: previews are best effort and are not Artifact authority.
- Add a new paired scheduler table: existing immutable assignments and idempotent enqueue already
  provide the required recovery semantics.

## Verification

Automated tests prove stable role-separated assignments, fixed features and argv, empty environment,
offline single-device target validation, backend-bound hello features, default-deny worker admission,
and rejection of a structured run receipt with a foreign implementation digest. Existing protocol,
server, and worker tests continue to exercise numeric persistence and control-plane compatibility.

This slice does not claim executable correctness workers. The next slice must add the fixed trusted
CUDA and Ascend harness policies, bundle materialization, container plans, runtime publication, and
real-device acceptance for these two execution kinds.
