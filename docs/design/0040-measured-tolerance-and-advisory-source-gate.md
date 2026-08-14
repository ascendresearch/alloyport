# 0040 — Measured tolerance, recoverable tool inputs, and an advisory Source Gate

- Status: Accepted
- Date: 2026-08-14
- Supersedes parts of: [0005](0005-correctness-oracle-and-calibration.md),
  [0027](0027-reduction-differential-oracle-and-calibration.md),
  [0028](0028-controller-authored-correctness-execution-bundles.md)
- Evidence: [`reduction-noise-floor-20260814.md`](../evidence/reduction-noise-floor-20260814.md)

## Context

Three defects were found by reading the gates against the question this project already asks of
everything else: *what does this trust, and was that verified or assumed?*

1. **A malformed tool call ended the migration.** `task-addd999597dcf12eded7489d` died because one
   file object in a corrected `submit_candidate_bundle` omitted `path`. The gateway mapped a
   model-authored JSON defect to `ToolGatewayError::Adapter`, which escapes the Agent loop.
2. **The correctness tolerance was asserted, and its battery could not bound it.** The gate shipped
   `absolute 1.0e-04, relative 2.0e-05`, described in the source as "the policy used by the first
   reduction specimen". Nothing measured it. Calibration ran ten mutants against it — every one
   orders of magnitude larger than the tolerance, so the same battery passes at a tolerance a
   hundred times too loose — and checked "identity" by comparing the reference against *itself*,
   which is true by construction. Measured against the real GB10 record, the frozen relative term
   is **38× looser** than this task's own spread and the absolute term is **19.5× tighter**.
3. **The Source Gate prescribed a method.** It required generated device source to contain
   `kernel_operator.h`, `__aicore__`, and `GM_ADDR`/`GlobalTensor`, and host source to contain
   `aclrtlaunch_`/`ACLRT_LAUNCH_KERNEL` and the literal `int alloyport_reduce_sum_f32`. Those
   strings are trivially satisfiable by a kernel that computes the wrong answer, and they reject a
   correct kernel written against a different Ascend C surface. It also hard-coded the specimen's
   ABI and CMake target in `alloyport-core`.

## Decision

### Model-authored input defects are recoverable, not fatal

`AgentToolGateway` gains `validate_call`, evaluated before authorization. A gateway that cannot
decode a call publishes a readable explanation and returns its digest; the reducer finishes the
operation as terminal `RejectedAsInvalid` and the Episode continues.

The rejection **must** name a real artifact. The controller opens `result_digest` to build the next
model input, so a synthetic digest does not merely lose the explanation, it fails the following
turn. Only a component with artifact authority can mint one, which is why this is a gateway
responsibility rather than a reducer one.

A Source Gate that found something blocking is the same class: `request_ascend_build` now returns
`CandidateFailed` carrying the receipt instead of an adapter error.

Infrastructure failures and ambiguous external effects keep their existing durable semantics.

### The tolerance is measured from the record, not supplied

- `measure_reduction_noise_floor` derives this task's numeric spread from the authority run alone,
  across two sources a correct implementation is equally entitled to differ by: the authority's own
  repetitions (`alloyport_reduce_sum_blocks` ends in `atomicAdd`, so it does not reproduce itself),
  and `reorder_output_bits`, a second legitimate summation order the trusted harness now emits.
- `ReductionTolerancePlan` — slack and repetitions — is what binds the experiment identity, because
  it is the only part knowable before dispatch. Freezing a number there would freeze a guess and
  make the experiment identity vouch for it.
- The gate derives the tolerance from the reference receipt itself. No caller supplies a policy, so
  no caller can widen one to make a candidate pass.
- Calibration fails unless the floor was measured, a reordered authority still passes that
  tolerance, and every mutant is caught. It lists whatever it missed in `undetected`.
- `JustOutsideTolerance` is sized by the policy. Widening the tolerance widens the mutant, so the
  battery cannot be satisfied by loosening the gate.
- `ReductionCalibrationReceipt::passed` is recomputed on read. A stored `passed: true` is the
  applicant's own word about rules that may no longer hold.
- `battery_scope` records that every mutant perturbs a receipt rather than an implementation.
  Nothing yet shows that a broken kernel would be caught on the way to the comparator.

### The Source Gate reports; the compiler and the oracle judge

`SourceGateFailure` carries a severity.

**Blocking** — what text can actually establish about the product boundary: the artifact set and
digests match, the source is UTF-8, no delegation to a framework or prebuilt operator, a non-empty
Ascend C device source exists, a host source exposes the migration's public symbol with C linkage,
build integration references every generated source and defines the migration's declared build
target, and the component mapping is complete.

**Advisory** — `UnrecognizedKernelStructure`, reported when device source carries none of the usual
Ascend C markers. It never blocks. Whether the source is a valid kernel is decided by the compiler
on the Build Gate and the Correctness Gate after it.

The public symbol and build target come from `MigrationSpec.public_entry`. `alloyport-core` no
longer names a specimen in either gate.

## Consequences

- The archived 2026-08-12 calibration no longer reads as a passing gate. It is kept as a record and
  its test now asserts what it actually is.
- `MigrationSpec` schema 1 requires `public_entry.build_target`. The checked-in reduction intake
  carries it; its digest changed.
- The trusted reduction harness emits `reorder_output_bits` for every successful case. Partial
  coverage is refused rather than sampled.
- Both boundary scripts now fail closed when ripgrep is absent. `check_sql_boundaries.sh` used
  `|| true` on every search, so a missing `rg` printed a clean pass.

## What this does not do

- No implementation-level mutant is compiled or run. `battery_scope` stays `ComparatorOnly` until
  known-bad kernels go through the real worker path.
- `implementation_invoked` and `synchronized` are still emitted as literals by the trusted harness,
  so the mutants that move them exercise fields no real candidate can move.
- Nothing here addresses the specimen types in `alloyport-core` and the protocol
  (`ReductionRunReceipt`, `ReductionCorpus`, protocol-minor-6 executor kinds). Onboarding a second
  operator family still costs a protocol version.
- There is still no performance evidence path, and no knowledge lifecycle implementation.
