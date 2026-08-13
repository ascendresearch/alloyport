# Design 0032: Pinned correctness images and CUDA hardware diagnostic

- Status: Implemented diagnostic slice
- Date: 2026-08-12
- Extends: Designs 0030 and 0031
- Scope: reproducible runner images, domain-generated diagnostics, and real CUDA calibration

## Context

Reusable worker policy does not prove that a selected image contains the tools assumed by its
trusted entrypoint. The first real CUDA attempts exposed two missing dependencies before candidate
execution: the minimal Python package omitted `json`, and the CUDA development base omitted CMake.
Neither condition was visible to fake-engine tests.

## Decisions

The repository now owns separate CUDA and Ascend correctness image definitions. They take immutable
base manifests as required build arguments and contain no corpus, implementation, expected result,
or oracle policy. CUDA explicitly installs Python, CMake, G++, and Make and uses the base CUDA
compiler. Ascend fixes the CANN compiler/runtime environment around its already complete devel
base. The trusted runner and role bundle remain create-only worker materialization inputs.

Two small `alloyport-core` examples support diagnostics without reimplementing domain identities.
One creates a validated CUDA-reference bundle from a source root and clearly marks all upstream Gate
identities diagnostic. The other deserializes an exact run receipt and invokes the production
mutation calibration function. Neither tool can create an Ascend candidate or a Correctness verdict.

## Verification and scope

Both images were built from recorded RepoDigests and inspected for their required tools. A real idle
GB10 ran the complete offline/read-only CUDA plan and produced 24 structured observations. The
production oracle detected all ten mutants against that exact receipt. Checked-in evidence is
schema-tested by the worker test suite.

This does not close product-plan item 5. A genuine generated Ascend candidate must first pass Source
and Build Gates; only then may the standalone Ascend worker run the paired bundle and the controller
issue a Correctness receipt.
