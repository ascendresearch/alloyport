# Design 0028: Controller-authored correctness execution bundles

- Status: Implemented prerequisite slice
- Date: 2026-08-12
- Extends: Designs 0024, 0026, and 0027
- Scope: immutable paired-run inputs, exact corpus coverage, and callable candidate ABI

## Context

Design 0027 established the differential oracle and an asynchronous paired-execution Port. A
production adapter still could not execute that Port honestly for two reasons:

1. the Port carried only an experiment identity, not the exact CUDA and Ascend source/corpus inputs
   each worker must execute; and
2. Source Gate required a public-symbol reference but did not require a stable callable ABI or
   build target that a trusted harness could link without understanding model-authored build logic.

Sending only a corpus digest is insufficient. Two faulty runners could omit the same hidden case and
still present equal observation sets. Likewise, accepting an arbitrary successful CMake build does
not prove that a correctness harness can invoke the generated implementation.

## Scope lock

This slice implements the controller and domain prerequisites for production paired execution:

- a validated, versioned reduction corpus containing case recipes but no expected outputs or
  tolerances;
- separate immutable CUDA-reference and Ascend-candidate execution bundles;
- an attempt specification that carries exact Artifact descriptors for both bundles;
- role-separated source roots and derived implementation identities;
- exact corpus-key and input-identity enforcement in oracle calibration and comparison;
- candidate-tool reconstruction of the exact manifest materialization before dispatch;
- returned-run binding to the assigned implementation bundles;
- Source Gate v2 requirements for a fixed callable C ABI and CMake target.

It does not implement worker execution kinds, trusted harness binaries, server dispatch/reconcile
composition, or hardware evidence. Those remain the next production slice.

## Decisions

### 1. The controller authors both execution bundles

`ReductionCorrectnessAttemptSpec` carries one experiment plus two content-addressed bundle
descriptors. Both bundles contain the same frozen corpus and exact experiment. Their source trees
are deliberately disjoint:

- the CUDA authority bundle contains only `input/` files captured from the immutable intake;
- the Ascend DUT bundle contains only `generated/` files reread from the candidate manifest and
  create-only materialization.

Bundle deserialization recomputes corpus, file, and implementation identities. A worker receives
inputs and case recipes, never comparator tolerances, expected outputs, calibration mutants, or a
verdict.

### 2. Corpus identity means exact case coverage

The frozen first-product corpus contains valid sizes at zero, one, block/tail boundaries, a large
randomized boundary, and the supported maximum, plus null-input, null-output, and first-unsupported
size behavior. Every case has two repetitions and a deterministic input seed.

The oracle now requires the reference observation keys to equal the corpus keys exactly. It also
recomputes each case's input digest from the controller-authored recipe before comparing candidate
evidence. Missing the same case on both sides therefore fails calibration instead of passing by
agreement.

### 3. Run receipts bind the exact assigned implementation

Each execution bundle derives an implementation digest from its ordered source paths and content
digests. The candidate tool rereads both immutable bundles when terminal receipts arrive and rejects
a reference or candidate receipt whose implementation digest does not match its assignment. This
check occurs before mutation calibration or numerical judgment.

### 4. Correctness uses one stable candidate ABI

Source Gate revision `source-gate-v2` requires host source evidence for:

```c
extern "C" int alloyport_reduce_sum_f32(
    const float *input,
    size_t elements,
    float *output);
```

It also requires build integration to define `alloyport_reduction_candidate`. A trusted harness can
declare the ABI independently, add the generated project, and link the fixed target. The candidate
still owns its maintainable implementation and build integration; it does not own the harness,
corpus, expected values, or verdict.

## Rejected alternatives

- Let workers discover candidate files from a shared workspace: that would bypass immutable
  Artifact identity and make replay host-dependent.
- Put expected outputs in the execution bundle: the DUT process must not receive oracle authority.
- Accept matching reference/candidate observation subsets: coordinated omission would become a
  false PASS.
- Reuse Source Gate v1 after strengthening its semantics: old passing receipts would be ambiguous.
- Treat the Build Gate output as the callable DUT artifact: the current Build Gate proves pinned
  compilation but does not publish a harness-linkable executable contract.

## Verification

Automated tests prove:

- reference and candidate bundles reject crossed source roots, forged file identity, forged
  implementation identity, and invalid corpus identity;
- the built-in corpus contains all 24 case/repetition pairs;
- oracle calibration rejects a reference that omits one frozen case;
- the candidate tool constructs both bundles only after exact Build Gate and manifest validation;
- its paired fake Port reads and validates the assigned bundle Artifacts rather than inventing an
  independent corpus;
- Source Gate v2 rejects candidates without the fixed ABI or build target;
- the existing durable Build-to-Correctness Episode still reaches PASS through the strengthened
  contracts.

The next slice must add policy-bound CUDA-reference and Ascend-candidate worker runners plus the
server-side paired adapter, then capture hardware run receipts for these exact bundles.
