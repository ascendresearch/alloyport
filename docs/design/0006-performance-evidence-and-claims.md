# 0006: Performance evidence and claims

- Status: Proposed
- Date: 2026-08-09
- Scope: benchmark protocol, baselines, uncertainty, profiler evidence, and release claims

## Context

Accelerator measurements are easy to bias through missing synchronization, warm-up effects, compile
time, cache state, device contention, power or frequency changes, selective shapes, and weak
baselines. A fast isolated kernel may have no visible project impact or may simply move work to a
fallback or host synchronization point.

## Decision

Performance is an independent gate evaluated only after correctness for the same candidate, spec,
corpus, and environment digests. Every published claim is a structured `PerformanceClaim` backed by
raw samples and profiler receipts.

## Baseline hierarchy

Measure against all applicable baselines:

1. target-framework eager implementation on the same hardware;
2. best audited reusable backend implementation;
3. currently released AlloyPort candidate;
4. original project end-to-end behavior on the target;
5. an analytical or empirical hardware-anchored limit when defensible.

CUDA performance is useful for migration context but is not the primary proof that a target-native
candidate is optimized. Baseline identity includes implementation and environment digests.

## Measurement protocol

Each benchmark receipt records:

- device model, topology, firmware, driver, runtime, compiler, framework, clocks, power mode, and
  relevant environment variables;
- source, build, candidate, spec, corpus, and container-image digests;
- input case, route, dispatch result, and proof that the expected kernel ran;
- warm-up policy, synchronization points, repetition count, ordering, timeout, and cache policy;
- raw host and device timestamps rather than only an aggregate;
- median, dispersion, tail values, confidence interval, and outlier policy;
- profiler trace digests and known measurement overhead;
- compilation/startup latency reported separately from steady-state latency.

Runs are rejected when the device is busy, thermally unstable, reset during measurement, or outside
the configured state envelope. Paired or interleaved baseline/candidate runs are preferred when they
reduce temporal drift.

## Claim domains

A speedup is defined over a weighted workload distribution, not the best observed shape. The claim
includes supported dtype/shape/layout domains, regression limits, confidence, and out-of-domain
fallback behavior.

Operator-level claims report latency, throughput, memory traffic, workspace, and compile cost as
applicable. Project-level claims report end-to-end latency or throughput and use measured hotspot
weight to explain the possible contribution. Amdahl's law is a sanity check against implausible
whole-project claims.

## Hardware interpretation

Profiler evidence classifies a case as launch/host-bound, compute-bound, bandwidth-bound,
synchronization-bound, or limited by layout/conversion overhead. Roofline or hardware-anchored
analysis is used to estimate remaining headroom, not to replace measurement.

Search should stop when improvement is statistically indistinguishable, below product relevance,
near a justified hardware limit, or dominated by a different end-to-end bottleneck.

## Anti-gaming rules

- A correctness failure yields no performance credit.
- Missing samples, timeouts, crashes, and fallback are not dropped from the workload distribution.
- Candidate-specific input caching or precomputation must be part of the declared API contract.
- Benchmark code and iteration counts are controlled by the evaluator, not the candidate.
- Output materialization and required synchronization cannot be removed for timing convenience.
- Compile-time and steady-state claims cannot be substituted for each other.
- A geometric or arithmetic average never hides a required case that exceeds a regression ceiling.

## Rejected alternatives

- Publish the best observed run or shape: this rewards noise and workload selection.
- Time asynchronous launches without completion: this measures enqueue latency, not required work.
- Compare only with CUDA or a weak handwritten baseline: target-side alternatives define real value.
- Infer project benefit from kernel speedup: end-to-end measurement remains mandatory.

## Verification plan

- Known delay, missing-synchronization, fake-fallback, cache, and selective-shape mutants are caught.
- Repeated baseline runs establish the noise floor before speedup thresholds are accepted.
- The same raw samples deterministically reproduce reported aggregates.
- Profiler traces prove launch count and expected kernel identity.
- Weighted operator gains reconcile with measured end-to-end change within an explained margin.
- A change in device state, baseline digest, corpus weights, or timing policy invalidates reuse of the
  prior claim.

## Research and implementation basis

- [CANN Bench](https://arxiv.org/abs/2607.20518) compares against PyTorch-on-Ascend and a per-case
  Hardware-Anchored Performance limit while keeping compile, correctness, and performance separate.
- [KernelBench](https://arxiv.org/abs/2502.10517) defines adjustable fast-and-correct thresholds over
  a broad PyTorch workload suite rather than rewarding isolated successful examples.
- [TritonBench](https://arxiv.org/abs/2502.14752) uses real-world operators and explicitly profiles
  efficiency, reflecting that syntactic generation is not performance engineering.
- [Triton-Ascend architecture](https://github.com/Ascend/triton-ascend/blob/main/docs/en/architecture_design_and_core_features.md)
  exposes target-specific tiling, multibuffering, core balance, layout, and synchronization options
  that profiler-guided search must reason about.
