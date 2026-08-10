# 0005: Correctness oracle and calibration

- Status: Proposed
- Date: 2026-08-09
- Scope: authority separation, differential testing, oracle calibration, and verdict semantics

## Context

Generated accelerator kernels can compile, run, and pass ordinary golden tests while still being
wrong. Common escapes include incorrect edge domains, aliasing or mutation violations, asynchronous
races, faulty gradients, accidental fallback, loose tolerances, and tests constructed by the same
agent that wrote the candidate.

## Decision

Correctness is decided by an independent oracle service over immutable receipts. Candidate authors,
executors, and optimization agents cannot define the release corpus, expected results, tolerances,
or final verdict.

## Authority model

- The authority path runs the approved reference on an independent environment when practical.
- The device-under-test path runs the candidate with instrumentation proving which implementation
  executed.
- Inputs are generated from the versioned corpus and delivered identically to both paths.
- Comparison occurs outside the candidate process.
- Hidden cases and oracle mutants are unavailable to candidate generation.

A reference implementation may establish **no regression relative to the reference**. It establishes
absolute correctness only when the reference itself is authoritative for the declared semantics.
The verdict records this distinction.

## Verdicts

Correctness evaluation returns one of:

- `PASS`: all required checks passed under a calibrated oracle.
- `FAIL`: at least one reproducible semantic mismatch or integrity violation exists.
- `UNVERIFIABLE`: required authority, coverage, or calibration evidence is missing.
- `INFRA_ERROR`: the run cannot be interpreted because infrastructure failed.

`UNVERIFIABLE` and `INFRA_ERROR` never promote a candidate and are never converted to `PASS` by
retry count.

## Check families

The applicable set is derived from `OperatorSpec`:

- exact and tolerance-based differential comparison;
- dtype-aware absolute/relative/ULP policies with NaN, Inf, and signed-zero rules;
- boundary and randomized inputs across the declared domain;
- metamorphic and algebraic properties;
- mutation, aliasing, view, overlap, and `out=` behavior;
- forward, backward, higher-order gradient, and autocast behavior;
- determinism and random-number-state consumption;
- invalid-input errors and warnings;
- eager, compiled, exported, fake/meta, and dynamic-shape behavior;
- sanitizer, synchronization, bounds, and resource checks where supported;
- optimized-path invocation and accidental-fallback detection;
- model-fragment and end-to-end integration checks.

Numerical tolerance is part of the operator contract. A candidate cannot widen it, drop failing
cases, reduce repetitions, or change comparison dtype.

## Oracle calibration

Before judging real candidates, the oracle must reject a suite of deliberate non-equivalent mutants.
Mutants cover arithmetic, indexing, boundary masks, strides, broadcasting, accumulation precision,
aliasing, synchronization, dispatch guards, fake fallback, gradients, and nondeterminism.

Calibration produces a `CalibrationReceipt` binding oracle version, corpus, comparator policy,
mutant set, environment, and detection results. A missing expected detection blocks correctness
claims until the gap is explained and the oracle or specification is repaired.

Surviving mutants are first-class coverage debt, not acceptable noise.

## Asynchrony and synchronization

Device completion is synchronized at observation boundaries. Repeated stress, varied scheduling,
and static synchronization analysis complement differential tests because some cross-unit races may
not manifest in a particular simulator, driver, or run.

Target-specific static tools are attached as independent gate inputs. Their absence is visible in
the verdict and may restrict the releasable domain or backend version.

## Rejected alternatives

- Let candidates generate their own acceptance tests: author and judge would share blind spots.
- Accept ordinary random differential tests alone: rare boundaries and races require directed checks.
- Treat agreement with any reference as absolute correctness: reference authority must be explicit.
- Turn missing evidence into a warning: unverifiable behavior cannot support a release.

## Verification plan

- Tests prove candidate code cannot read hidden expected outputs or write oracle results.
- Every comparator policy has pass, fail, NaN/Inf, and near-threshold fixtures.
- The oracle rejects all required mutants before evaluating a candidate.
- Injected fallback and no-op kernels are detected even when outputs match.
- Repeated asynchronous runs expose intentionally missing synchronization where static checks apply.
- A reference defect can be recorded without rewriting historical receipts.
- Verdict replay produces the same decision from the same immutable inputs.

## Research and implementation basis

- [CANN Bench](https://arxiv.org/abs/2607.20518) treats compilation, functional correctness, and
  performance as independent axes and designs evaluation to resist reward hacking.
- [AccelSync](https://arxiv.org/abs/2605.07881) shows why golden testing and simulation can miss
  cross-unit synchronization defects, motivating static barrier-sufficiency checks and mutation tests.
- [PyTorch `torch.library.opcheck`](https://docs.pytorch.org/docs/stable/library.html) motivates
  schema, mutation, fake-tensor, autograd, and compilation registration checks.
- [KernelBench](https://arxiv.org/abs/2502.10517) motivates execution feedback and joint fast-and-
  correct metrics while also showing that kernel generation remains difficult.
