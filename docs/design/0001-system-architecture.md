# 0001: System architecture

- Status: Product boundary superseded by Design 0009; trust boundary and gates retained
- Date: 2026-08-09
- Scope: product boundary, trust boundary, routing, and verification gates

> [Design 0009](0009-product-definition-and-staged-cuda-scope.md) supersedes this document's
> PyTorch-first mission, unit-of-work definition, and route ladder. The trust boundary, independent
> gates, receipts, and evidence principles remain applicable until replaced by a later design.

## Mission

Given a runnable PyTorch workload and a target accelerator, AlloyPort produces a releasable port:
an implementation plus evidence that it is correct, faster where claimed, reproducible, and safe
to integrate.

The system optimizes for verified delivery rather than the number of generated kernels.

## Units of work

The project/model remains the intake and end-to-end acceptance unit. Operators are the smaller
delivery units because their semantic contracts, candidate lineage, performance domains, dispatch
guards, and fallbacks can be independently reviewed and released.

Seven durable objects form the initial vocabulary:

1. `Task`: requested source workload, target, lifecycle, and policy.
2. `OperatorSpec`: semantic contract and supported input domain.
3. `WorkloadCorpus`: authoritative, boundary, randomized, and performance cases.
4. `Candidate`: an immutable implementation revision with parentage and route.
5. `RunReceipt`: content-addressed record of code, environment, device, command, and outputs.
6. `Verdict`: an independent gate decision over receipts.
7. `ReleaseManifest`: the selected candidate, supported domain, evidence, dispatch, and fallback.

The bootstrap crate models the core lifecycle, candidate lineage, verdicts, and release readiness.
Persistence and wire formats will be added after the first real corpus proves which fields are
stable.

Long-running execution state, goal persistence, and context projection are specified separately in
[Design 0002](0002-long-horizon-runtime.md).

## Trust boundary

Workers compile and execute untrusted candidates on isolated devices. Workers do not own the
reference implementation, hidden tests, gate policy, knowledge base, or release decision.

The controller owns specifications and policy. The oracle owns comparisons and verdicts. Every
claim must point to immutable receipts that bind source revision, build inputs, environment image,
toolchain, target device state, command, and measurements.

Agents may understand code, form hypotheses, and propose candidates. They may not define their own
acceptance tests, approve a verdict, publish a release, or mutate canonical knowledge directly.

## Route policy

Routing selects the lowest-cost trustworthy implementation path:

1. `Keep`: device-neutral PyTorch already meets acceptance criteria.
2. `Reuse`: an audited ecosystem implementation exists.
3. `Compile`: graph capture and a supported compiler backend are sufficient.
4. `PortableKernel`: use a portable kernel DSL with backend-specific scheduling.
5. `NativeKernel`: use the target's native programming model for the remaining hotspots.

Source translators are candidate generators, never proof of correctness.

## Independent gates

- **G0 Contract:** reference, corpus, tolerances, baseline, target, and environment are complete.
- **G1 Build:** compile, static checks, resource checks, and target-specific safety checks pass.
- **G2 Correctness:** differential, boundary, randomized, property, metamorphic, gradient, aliasing,
  NaN/Inf, fallback-detection, and oracle-calibration checks pass as applicable.
- **G3 Performance:** controlled device state, warm-up, repetitions, uncertainty, profiler evidence,
  target baseline, workload weighting, and end-to-end impact support the claim.
- **G4 Integration:** dispatch guards, fallback, packaging, framework compilation/export behavior,
  and end-to-end execution pass.

A release requires all applicable gates. Compilation, correctness, and performance are separate
axes; passing one must never imply another.

## Initial delivery sequence

The first milestone targets one fixed Ascend/CANN environment and two real PyTorch projects.
Before adding autonomous search, build a corpus for roughly twenty measured hotspots, establish
replayable CUDA and NPU references, and calibrate the oracle with deliberate mutants.

The next milestone adds one candidate-generating agent across reuse, graph compilation,
Triton-Ascend, and Ascend C routes. It succeeds only when several operator families are correct,
some beat the target baseline, and all evidence replays.

Only after that should AlloyPort add parallel isolated workers, persistent scheduling, profiler and
roofline analysis, framework registration, guarded dispatch, packaging, and a demonstrated
end-to-end model improvement.
