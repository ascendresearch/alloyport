# 0004: Candidate routing and backend strategy

- Status: Proposed
- Date: 2026-08-09
- Scope: implementation-route selection, backend boundaries, fallback, and candidate lineage

> Revision required: [Design 0009](0009-product-definition-and-staged-cuda-scope.md) requires every
> successful migration route to produce Ascend C source. The `KEEP`, framework-only `REUSE`, graph
> `COMPILE`, and portable-kernel outcomes below cannot satisfy the product contract as written. This
> proposal must be replaced before acceptance.

## Context

Not every CUDA dependency should become a newly generated native kernel. A workload may already be
device-neutral, supported by an existing operator library, capturable by a graph compiler, suitable
for a portable kernel DSL, or truly dependent on target-native behavior. Choosing the most complex
route too early creates maintenance cost and may reduce portability without improving end-to-end
performance.

## Decision

AlloyPort uses an ordered route ladder:

```text
KEEP -> REUSE -> COMPILE -> PORTABLE_KERNEL -> NATIVE_KERNEL
```

Routing selects the lowest-cost route that can satisfy the operator contract and measured project
goal. It produces a versioned `RouteDecision`; it does not directly modify code.

## Route semantics

- `KEEP`: existing PyTorch code already runs correctly and meets the target performance objective.
- `REUSE`: bind an audited vendor, framework, or ecosystem implementation.
- `COMPILE`: lower a captured graph through an existing compiler backend.
- `PORTABLE_KERNEL`: implement shared dataflow in a kernel DSL while allowing target-specific
  scheduling, tiling, layout, tensorization, and pipeline choices.
- `NATIVE_KERNEL`: use Ascend C or another target-native programming model for a measured hotspot
  whose requirements cannot be met by lower-cost routes.

Source translators such as HIPIFY or SYCLomatic are candidate generators inside a route. Their
output has exactly the same proof obligations as hand-written or agent-written code.

## RouteDecision

The decision records:

- source dependency class and affected call sites;
- framework and backend capability probes;
- applicable operator-library implementations and their version/license;
- graph capture result, breaks, guards, dynamic-shape behavior, and compile-cache implications;
- expected engineering cost, portability, maintenance surface, and fallback quality;
- measured hotspot weight and maximum plausible end-to-end contribution;
- rejected routes and evidence needed to reconsider them;
- selected route and escalation conditions.

An agent may propose this record. Policy and measured evidence approve it.

## Backend boundary

Rust owns orchestration, durable types, policy, scheduling, receipts, and verdict flow. Backend
adapters own framework-specific and vendor-specific execution. The control plane communicates with
adapters through versioned requests and content-addressed artifacts rather than importing vendor
runtimes into the core process.

An adapter declares capabilities instead of relying on backend-name conditionals:

- framework/device/runtime versions;
- supported capture, compile, operator, dtype, layout, profiler, sanitizer, and distributed features;
- native toolchains and kernel languages;
- compatibility constraints and known defects;
- probes that demonstrate each declared capability in the current environment.

Capabilities expire when a relevant version or device digest changes.

## Portability boundary

Portability means shared semantics, interfaces, corpus, evidence schema, and release policy. It does
not require identical low-level schedules across hardware. Dataflow may be shared while memory
hierarchy, core binding, layout, tiling, synchronization, and pipelines remain target-specific.

## Dispatch and fallback

Every optimized implementation ships with a machine-checkable domain predicate. Dispatch chooses
the candidate only when target, versions, dtype, shape, layout, alignment, and other guards match.
The fallback must remain usable, tested, observable, and free from recursion into the optimized
path. Silent fallback is a correctness and performance failure during verification.

## Rejected alternatives

- Translate every CUDA extension directly: translation does not choose the cheapest valid route.
- Generate native kernels first: this spends the highest maintenance cost before measuring need.
- Force one low-level schedule across hardware: shared semantics do not imply shared memory hierarchy.
- Encode backends with scattered name checks: capability contracts are testable and versionable.

## Verification plan

- Capability claims are backed by executable probes and invalidated on environment changes.
- Route selection refuses native work without a measured hotspot and failed lower-cost alternatives.
- Candidate lineage preserves translator inputs, generated output, manual edits, and parent revision.
- Dispatch property tests exercise both sides of every domain boundary.
- Integration traces distinguish optimized execution, intentional fallback, and accidental fallback.
- A backend adapter can be removed without changing core domain types or historical receipts.

## Research and implementation basis

- [PyTorch custom compiler backends](https://docs.pytorch.org/docs/stable/torch.compiler_custom_backends.html)
  provide a narrow graph-module-to-callable integration contract.
- [FlagGems](https://github.com/flagos-ai/FlagGems) demonstrates ATen-level interception,
  eager-mode operation, per-function dispatch, and multi-backend Triton operator reuse.
- [Triton-Ascend architecture](https://github.com/Ascend/triton-ascend/blob/main/docs/en/architecture_design_and_core_features.md)
  separates target-independent code from Ascend-specific compiler and driver behavior.
- [TileLang](https://arxiv.org/abs/2504.17577) motivates separating dataflow from scheduling,
  layout, tensorization, and pipelines.
- [HIPIFY](https://github.com/ROCm/HIPIFY) and
  [SYCLomatic](https://github.com/oneapi-src/SYCLomatic) are evidence that translation can accelerate
  a port while still requiring editing, verification, and target-specific optimization.
- [TVM](https://arxiv.org/abs/1802.04799) motivates graph-level and operator-level optimization with
  explicit hardware-specific schedules and cost models.
