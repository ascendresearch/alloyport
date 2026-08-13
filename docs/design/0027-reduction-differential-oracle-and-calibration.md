# Design 0027: Reduction differential oracle and mutation calibration

- Status: Implemented first bounded slice
- Date: 2026-08-12
- Extends: Designs 0005, 0024, 0025, and 0026
- Scope: independent reduction-run evidence, calibration, verdicts, and Agent-tool connection

## Context

A passing Source Gate proves that a candidate contains the required generated source. A passing
Build Gate proves that the exact source compiled in a pinned Ascend environment. Neither proves
that the public function preserves the CUDA behavior. Worker success, a process exit code, and model
narrative are observations rather than correctness authority.

The first product specimen needs one bounded correctness seam before new execution policy is added.
That seam must make the CUDA and Ascend paths independently executable, join them under one stable
experiment identity, keep comparison outside both candidate processes, and refuse to pass unless the
oracle first demonstrates that it detects deliberate defects.

## Scope lock

This slice implements:

- a structured reduction-run receipt for the original CUDA authority path and generated Ascend DUT;
- a stable correctness experiment bound to task, candidate, MigrationSpec, Source Gate, Build Gate,
  corpus, and comparator policy identities;
- a pure differential comparator with explicit status, fp32 tolerance, positive-zero,
  non-finite, invocation, synchronization, repetition, and input-identity checks;
- a mutation battery and immutable calibration receipt;
- the four verdicts `PASS`, `FAIL`, `UNVERIFIABLE`, and `INFRA_ERROR`;
- an asynchronous paired-execution Port with durable pending/reconciliation semantics;
- `request_reduction_correctness`, which accepts only an exact passing Build Gate receipt and makes
  Correctness—not Build—the terminal verified result in an Agent Episode.

This slice does not yet implement the production CUDA/Ascend worker adapter, a new worker execution
kind, hardware execution, performance measurement, release assembly, or generalized operator
oracles. Deterministic tests provide independently authored run Artifacts through the same Port.

## Decisions

### 1. One experiment joins two independent runs

`ReductionCorrectnessExperiment` is derived from controller-owned identities:

- task and candidate;
- MigrationSpec and candidate manifest;
- exact Source and Build Gate receipts;
- frozen corpus and numeric policy.

Both run receipts must name that experiment and corpus. The CUDA receipt has no candidate identity;
the Ascend receipt must name the exact candidate. Changed or crossed identities are integrity errors,
not numerical failures.

### 2. Runners report observations; the controller judges them

A trusted runner authors ordered observations containing case identity, repetition, element count,
input digest, public API status, and optional fp32 output bits. It also reports whether the intended
implementation executed and whether the device was synchronized before observation.

The candidate process cannot provide verdict, tolerance, expected output, corpus selection, or
calibration results. The comparator runs over immutable run Artifacts outside both executions.

### 3. The first corpus is reduction-specific

The v1 policy preserves the fixture tolerance of `max(1e-4, abs(reference) * 2e-5)` and requires two
observations per case. The execution adapter must cover zero elements, one element, block/tail
boundaries, the maximum supported size, deterministic randomized values, invalid pointers, and the
first unsupported size. Inputs and hidden case material are controller-owned and delivered
identically to both paths.

The reference tier is the original CUDA implementation for the frozen migration contract. The
result therefore supports correctness relative to the declared source behavior; it does not claim
generality outside the MigrationSpec domain.

### 4. Calibration precedes candidate judgment

The exact reference run, corpus, policy, and oracle revision are calibrated together. The required
battery contains:

1. arithmetic scaling;
2. indexing/result swaps;
3. boundary-mask loss;
4. accumulation error;
5. invalid-status corruption;
6. negative zero for the zero-element contract;
7. non-finite output;
8. fallback/no-op path bypass;
9. missing synchronization;
10. nondeterministic repeated output.

Calibration first requires the identity comparison to pass and then requires every mutant to fail.
Any survivor makes the calibration receipt non-passing. Candidate evaluation with a missing,
foreign, changed, or non-passing calibration is `UNVERIFIABLE`, never `PASS`.

### 5. Verdict and infrastructure categories remain distinct

- `PASS`: calibrated comparator finds no failure.
- `FAIL`: reproducible semantic, status, instrumentation, synchronization, or determinism failure.
- `UNVERIFIABLE`: required calibration authority is absent or does not match.
- `INFRA_ERROR`: paired execution cannot produce interpretable evidence.

Only `PASS` satisfies an Agent subtask. `FAIL` is model-visible candidate feedback.
`UNVERIFIABLE` and `INFRA_ERROR` are infrastructure failures and cannot promote a candidate.

### 6. Paired execution is a Port, not oracle code

`ReductionCorrectnessAttemptPort` owns dispatch and reconciliation of the independent CUDA and
Ascend runs. It returns pending state or exact run descriptors. The oracle module has no gRPC,
SQLite, container, device, provider, or Artifact-store dependency. A production adapter may use the
existing worker control plane, but it cannot move comparison or corpus authority into a worker.

## Rejected alternatives

- Treat Build success as correctness: compilation does not exercise semantics.
- Let the candidate emit a PASS marker: the judged process would own the verdict.
- Compare only one aggregate stdout line: it cannot bind cases, inputs, statuses, repetitions, or
  implementation-path evidence.
- Accept a comparator without mutation calibration: a constant-zero or excessively loose oracle
  would appear healthy.
- Put hidden corpus or tolerance fields in model tool arguments: candidate generation could weaken
  or target the judge.
- Convert missing evidence into a warning: absence of authority is `UNVERIFIABLE`.

## Verification

Automated tests prove:

- all ten required mutants are detected by the fixture policy;
- an excessively weak tolerance cannot produce a calibrated PASS;
- an exact independent run pair passes and a numeric mismatch fails;
- duplicate observations and crossed experiment identities fail closed;
- non-Build evidence cannot dispatch correctness work;
- pending and reconciliation reuse one stable experiment;
- one passing Build-Gate candidate reaches a calibrated Correctness PASS;
- a durable Agent Episode completes only after that Correctness result.

The next slice must implement the production paired-run adapter and policy-bound worker runners,
then exercise the frozen corpus on real CUDA and Ascend workers. That hardware evidence is not
claimed by this design's deterministic contract tests.
