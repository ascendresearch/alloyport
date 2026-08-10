# 0003: Operator specification and workload corpus

- Status: Proposed
- Date: 2026-08-09
- Scope: semantic contracts, input domains, reference behavior, and test-corpus ownership

> Revision required: [Design 0009](0009-product-definition-and-staged-cuda-scope.md) establishes
> CUDA source as the migration object and Ascend C source as a mandatory output. This proposal must
> be replaced by a CUDA `MigrationSpec` design before it can be accepted. Its corpus ownership and
> evidence-separation principles remain useful; its PyTorch-operator framing is not authoritative.

## Context

A syntactically valid kernel is not necessarily a valid PyTorch operator. Correct behavior includes
shape and dtype rules, broadcasting, layouts, aliasing, mutation, gradients, numerical behavior,
errors, compilation behavior, and interaction with surrounding model code. These properties must
be fixed before candidate generation; otherwise the candidate can redefine the problem it is judged
against.

## Decision

Every operator task starts with a versioned `OperatorSpec` and `WorkloadCorpus`. Candidate search is
blocked until the contract gate confirms that both are complete enough for the requested claim.

The project/model is the source of workload observations and the end-to-end acceptance unit. The
operator specification is the smaller implementation and release contract.

## OperatorSpec

The initial schema records:

- stable operator identity, PyTorch schema, source locations, and call sites;
- input/output pytree structure and tensor metadata rules;
- valid and invalid dtype, device, shape, stride, layout, alignment, and scalar domains;
- broadcasting, promotion, accumulation precision, and output dtype rules;
- mutation, view, storage alias, overlap, contiguity, and `out=` behavior;
- forward, backward, higher-order gradient, and autocast requirements;
- determinism, random-number consumption, NaN/Inf, signed-zero, and empty-tensor behavior;
- error and warning behavior that callers depend on;
- eager, `torch.compile`, `torch.export`, fake/meta tensor, and dynamic-shape expectations;
- supported target backends and an explicit out-of-domain fallback;
- correctness tolerances and their rationale, owned by the specification rather than a candidate;
- baseline implementation and reference-environment digests.

Observed behavior and intended behavior are separate fields. When source code, documentation, and
production traces disagree, the specification remains unresolved until an authority decision is
recorded.

## WorkloadCorpus

The corpus is immutable and partitioned by purpose:

1. **Authority cases:** minimal examples with trusted reference outputs.
2. **Boundary cases:** zero sizes, singleton dimensions, extreme values, non-contiguous layouts,
   unusual strides, misalignment, and domain edges.
3. **Generated cases:** seeded randomized and property-based samples from the declared domain.
4. **Metamorphic cases:** transformations whose output relation is known without a second kernel.
5. **Gradient cases:** forward/backward coupling, finite-difference probes where appropriate, and
   higher-order behavior when required.
6. **Adversarial cases:** aliasing, overlap, NaN/Inf, invalid inputs, fallback detection, and known
   defect mutants.
7. **Performance cases:** production-weighted shapes plus explicitly labeled stress and roofline
   probes. These are not allowed to replace correctness coverage.
8. **Integration cases:** captured model fragments and complete project commands.

Public development cases, hidden release cases, and oracle-calibration mutants are stored as
separate views. Candidate-generating agents cannot modify any of them and do not receive hidden
expected outputs.

## Capture

Capture combines static repository inspection, runtime observation, framework graph capture, and
user-provided production distributions. Runtime samples are evidence about frequency, not proof of
the full valid domain.

`torch.export` and `torch.compile` graphs are useful normalized views, but graph breaks and export
failures are themselves captured facts rather than silently ignored calls. Custom operators must
also expose framework registration and fake/meta behavior needed by compilation.

## Versioning

A corpus or specification change creates a new digest. Existing candidate verdicts remain attached
to the old digest and cannot automatically satisfy the new contract. Narrowing a supported domain
requires an explicit release decision and a dispatch guard; it cannot be hidden in tests.

## Rejected alternatives

- Infer the contract from the winning implementation: this lets a candidate redefine semantics.
- Treat production traces as the valid domain: observed frequency does not cover boundary behavior.
- Use one shared public test set: it encourages overfitting and cannot calibrate the oracle honestly.

## Verification plan

- Schema tests reject missing reference, tolerance, fallback, and domain information.
- Mutation tests demonstrate that boundary, aliasing, gradient, and fallback defects are detected.
- Corpus generation is deterministic from a recorded seed and generator version.
- Every performance case traces to a production observation or an explicitly labeled synthetic goal.
- A released candidate is rejected when replayed against a different spec or corpus digest.
- End-to-end capture proves that the optimized operator is actually invoked.

## Research and implementation basis

- [PyTorch accelerator integration](https://docs.pytorch.org/docs/main/accelerator/index.html)
  describes runtime, operator, frontend, profiler, compiler, and distributed integration as distinct
  surfaces rather than a device-name substitution.
- [PyTorch `torch.library`](https://docs.pytorch.org/docs/stable/library.html) and `opcheck` motivate
  checking schema, mutation, fake-tensor, autograd registration, and compilation compatibility.
- [PyTorch `torch.export`](https://docs.pytorch.org/docs/stable/export.html) provides a normalized
  graph contract with explicit constraints, useful for capture but not sufficient as the sole oracle.
- [KernelBench](https://arxiv.org/abs/2502.10517) and
  [TritonBench](https://arxiv.org/abs/2502.14752) motivate real PyTorch workloads and separate
  functional and performance evaluation.
