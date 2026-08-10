# 0009: Product definition and staged CUDA scope

- Status: Accepted
- Date: 2026-08-09
- Scope: migration object, mandatory output, Phase 1 unit, and later project-level expansion
- Supersedes: the PyTorch-first product boundary and route outcomes in Design 0001

## Context

An earlier bootstrap definition described AlloyPort as a system for running PyTorch workloads on
non-NVIDIA accelerators. That definition changes both the sample distribution and the engineering
goal: it makes PyTorch models the input object and accelerator-backend coverage the output. It would
push AlloyPort toward implementing PyTorch device, operator, compiler, profiler, and distributed
support for new hardware.

That is a different product. AlloyPort exists to migrate CUDA source code to Ascend C source code.
PyTorch may be one caller of a CUDA extension, but it is neither the universal input nor the target
platform AlloyPort is responsible for completing.

## Product definition

AlloyPort is a verified CUDA-to-Ascend-C source migration and optimization factory.

Given CUDA source, its build and execution context, a runnable workload, and a fixed Ascend target,
AlloyPort analyzes CUDA parallel-program semantics, generates maintainable Ascend C and required
host/build integration, and delivers correctness, performance, and reproducibility evidence from
real target hardware.

## Product invariants

1. CUDA source code is the migration object.
2. Ascend C source code is a mandatory release artifact.
3. A result that only switches a framework backend, calls an existing binary operator, or emits a
   non-Ascend-C DSL does not complete the migration contract.
4. Templates, libraries, translators, and agents may generate or support candidates; they do not
   remove the Ascend C output requirement.
5. Correctness is established against the source program's specified behavior, not inferred from
   successful compilation or framework execution.
6. Performance is measured on the declared Ascend environment and reported separately from
   correctness.
7. Framework-specific glue is generated only when required to preserve the source project's public
   call path. It does not expand the product into general framework enablement.

## Input contract

A migration intake contains:

- CUDA `.cu`, `.cuh`, and relevant C/C++ source;
- build configuration and compiler flags;
- host-side launch and runtime code in scope;
- the public function or extension entry point to preserve;
- a runnable command, input generator, and source-side reference environment;
- expected behavior, tolerances, and known constraints when available;
- target Ascend hardware, CANN, compiler, driver, and runtime versions;
- licensing and redistribution constraints for source and dependencies.

Missing information is recorded as contract debt. An agent cannot silently invent a public API,
valid input domain, tolerance, or unsupported dependency policy.

## Mandatory release artifacts

- human-maintainable Ascend C device implementation;
- required Ascend-side host launch and runtime glue;
- build configuration and deterministic build command;
- integration patch preserving the agreed public call path;
- mapping from source CUDA components to generated Ascend C components;
- supported and unsupported domains, limitations, and dependency substitutions;
- correctness and performance evidence bundles;
- reproducible environment and run receipts;
- known fallback or failure behavior when inputs fall outside the supported domain.

Generated source is part of the product, not an incidental intermediate hidden behind a binary.

## Phase 1: bounded CUDA extension

The first product milestone targets scope level 2: a CUDA extension rather than an isolated device
kernel or an arbitrary CUDA project.

The migration unit includes:

- one public extension or native function call path;
- the CUDA device kernel or bounded set of kernels reachable from that entry point;
- host-side argument preparation, launch configuration, stream use in the supported subset, and
  error propagation;
- build scripts or CMake/setuptools integration needed to compile the source component;
- sufficient caller integration to execute the same logical operation after migration;
- reference and target workloads covering the declared domain.

Phase 1 is successful only when:

- the Ascend C source and integration build from a clean recorded environment;
- target execution invokes the generated Ascend C implementation rather than hidden CUDA or a
  silent unrelated fallback;
- required correctness gates pass against the CUDA/reference behavior;
- target performance is measured against a declared Ascend baseline;
- the public call path runs in its actual host project or a faithful extension harness;
- another compatible worker can replay the evidence.

Phase 1 does not claim arbitrary support for whole repositories. Multi-process orchestration,
complex multi-stream/event graphs, CUDA Graphs, dynamic loading, arbitrary inline PTX, and broad
third-party CUDA-library replacement are supported only when explicitly included in a narrower
contract. Their general handling belongs to later scope.

## Later phase: CUDA modules and projects

After Phase 1 is reliable, AlloyPort expands toward scope level 3:

- multiple public entry points and interacting extensions;
- multi-kernel pipelines and cross-kernel intermediate state;
- streams, events, overlap, and asynchronous host/device execution;
- CUDA Graphs and more complex synchronization;
- dependencies on CUB, Thrust, cuBLAS, cuDNN, or custom CUDA libraries;
- mixed migrated and unmigrated components with explicit boundaries;
- repository-wide build, packaging, deployment, and end-to-end acceptance;
- coordinated optimization where local kernel choices affect a larger pipeline.

This is an architectural direction, not a Phase 1 compatibility claim. Phase 1 records unsupported
constructs structurally so later project-level migration can compose extension-level results rather
than restart from raw transcripts.

## Role of PyTorch and other frameworks

PyTorch is an optional integration environment. When the CUDA input is a PyTorch extension,
AlloyPort may need to preserve its Python-visible schema, tensor metadata behavior, autograd or
compilation integration, and packaging. These obligations belong to that extension's migration
contract.

AlloyPort does not attempt to implement a general PyTorch accelerator backend, close global ATen
operator coverage, or make every PyTorch model run on a new NPU. The same migration core must also
accept CUDA components called from standalone C++, inference engines, TensorFlow, or other hosts.

## Sample and benchmark definition

Primary samples are CUDA migration cases, not PyTorch models:

```text
CUDA source + host launch + build context + executable workload
    -> Ascend C source + host/build integration + verified evidence
```

Useful samples include standalone CUDA extensions, PyTorch CUDA extensions, inference-engine
plugins, small CUDA libraries, and later multi-kernel modules. A PyTorch operator benchmark is
relevant only when it contains or faithfully represents the CUDA source transformation being judged.

Evaluation must separately score source intake, semantic analysis, Ascend C generation, compilation,
correctness, performance, integration, and evidence replay. Generating plausible code without
executing it on Ascend earns no completion credit.

## Migration pipeline

```text
CUDA Intake
    -> Source and Build Analysis
    -> CUDA Semantic Model / MigrationSpec
    -> Ascend C Candidate Generation
    -> Build and Static Verification
    -> CUDA/Authority versus Ascend Differential Verification
    -> Ascend Profiling and Optimization
    -> Host Integration
    -> Ascend C Source Release with EvidenceBundle
```

All candidate-generation strategies converge on Ascend C. Reuse may supply templates, algorithms,
intrinsics, or library-assisted building blocks, but a route that bypasses the required source
deliverable is rejected as out of product scope.

## Consequences for existing designs

- Design 0001 retains its trust boundary, independent gates, receipts, and evidence principles, but
  its PyTorch-first mission and route ladder are superseded.
- Design 0003 must be replaced by a CUDA `MigrationSpec` and migration-corpus design. PyTorch-specific
  semantics become an optional integration profile.
- Design 0004 must be replaced by Ascend-C-producing generation strategies rather than outcome
  routes that can stop at framework reuse or a portable DSL.
- Designs 0002 and 0005 through 0008 remain broadly applicable after terminology and test-family
  alignment.

## Open Phase 1 decisions

Separate designs must define the initially supported subsets of CUDA language/runtime constructs,
third-party libraries, stream semantics, host build systems, Ascend C programming patterns, and
framework adapters. Unsupported constructs must fail intake explicitly rather than fall through to
best-effort generation.

## Verification plan

- Product acceptance tests reject outputs without Ascend C source.
- A framework-backend-only patch cannot satisfy a migration task.
- Phase 1 fixtures include device code, host launch, build integration, and a real public call path.
- Intake detects and reports constructs reserved for the later project-level phase.
- Samples and metrics identify CUDA-source coverage independently from framework coverage.
- Generated artifacts trace every released Ascend C component to source CUDA and verification
  evidence.

## Rejected alternatives

### Define the product as PyTorch portability

Rejected because it changes the input from CUDA source to framework workloads and changes the goal
from Ascend C generation to broad accelerator-backend enablement.

### Start with isolated device kernels only

Rejected as the primary product milestone because it omits host launch, build, and integration work
needed for a usable migration. Isolated kernels remain useful unit fixtures.

### Start with arbitrary complete CUDA projects

Rejected for Phase 1 because multiple APIs, build systems, libraries, streams, and deployment paths
would make failures difficult to localize before the extension-level loop is reliable.
