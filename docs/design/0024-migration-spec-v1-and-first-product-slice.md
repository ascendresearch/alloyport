# 0024: MigrationSpec v1 and the first product slice

- Status: Accepted
- Date: 2026-08-12
- Scope: Phase-1 intake, generation strategies, lifecycle, release source gate, and first specimen
- Supersedes: Design 0003's `OperatorSpec` and Design 0004's outcome route model

> **Provider amendment accepted.** The MigrationSpec, Candidate, lifecycle, and Gate decisions in
> this document remain accepted. The fixed DeepSeek transport and one-shot authoring paragraphs are
> frozen historical implementation notes superseded by
> [Design 0025](0025-pluggable-llm-provider-architecture.md). They must not receive new callers or
> features while the ordered replacement is implemented.

## Context

Design 0009 changed AlloyPort from framework portability into verified CUDA-to-Ascend-C source
migration. The implementation still exposes the earlier `TargetBackend` and
`KEEP -> REUSE -> COMPILE -> PORTABLE_KERNEL -> NATIVE_KERNEL` model. Those values allow a task to
finish without an Ascend C source deliverable and therefore contradict the accepted product.

The distributed runtime has proved fixed CUDA and Ascend execution, Artifact transport, receipts,
and device lifecycle. It does not yet represent the CUDA migration contract that those facilities
must execute.

## Decision

Introduce a versioned `MigrationSpec` as the immutable contract for every Phase-1 task. Replace
outcome routes with generation strategies whose common postcondition is an Ascend C source release.
Align the task lifecycle with the migration pipeline and make generated source evidence a separate
mandatory release gate.

## MigrationSpec v1

The first schema contains:

- source revision identity;
- relative CUDA device, host, and build-file paths;
- the public symbol and its caller-visible contract;
- a shell-free source reference command and working directory;
- fixed Ascend SoC, CANN, compiler, driver, and runtime identity;
- supported input domain and explicitly unsupported constructs;
- required out-of-domain fallback behavior.

Validation rejects a spec before generation when:

- any device, host, or build source set is empty;
- a source path is absolute, contains `..`, or is otherwise not bundle relative;
- the public entry point or supported-domain statement is empty;
- the reference command has no executable argv;
- any required Ascend environment identity is absent;
- fallback behavior is not declared.

The spec is content-addressed outside the value itself. Tasks and candidates carry its digest so a
verdict for one specification cannot approve another.

## Generation strategies

The first strategy vocabulary describes how Ascend C is produced, not whether it is produced:

- `DirectAscendC`: generate the target directly from the CUDA contract;
- `AscendSimtBootstrap`: use the Ascend SIMT mapping as an initial Ascend C candidate;
- `VerifiedTemplateAdaptation`: adapt a versioned, evidence-backed Ascend C example;
- `MemoryGuidedSynthesis`: retrieve prior attempts and evidence to guide generation.

Libraries, agents, and translators may implement these strategies. None may satisfy the source gate
with a framework backend switch, prebuilt operator call, or non-Ascend-C DSL artifact.

The runtime LLM is an adapter-selected candidate generator, not part of the domain model. The current
default is `deepseek-v4-pro`, but changing it must not change `MigrationSpec`, Gate, or release
semantics. Model/provider identity and generation parameters are execution receipt facts.

The first authoring contract passes only a reproducibly successful intake, its declared source
bytes, and deterministic evidence to the model. It accepts only an untrusted `generated/` source
bundle containing Ascend C device code, host glue, build integration, and component mapping. The
adapter, rather than the model, attaches request lineage and model-invocation facts. Gate verdicts,
receipts, candidate IDs, and release state are not fields in the model response schema, and unknown
fields are rejected.

The initial concrete transport is explicit and shell-free. It fixes the DeepSeek HTTPS endpoint,
loads the bearer token from an owner-only regular file, supplies it through a mode-0600 temporary
header file rather than process arguments, and enforces request, response, diagnostic, and wall-time
bounds. Successful exchanges content-address the exact provider request and response, proposal,
generated files, and candidate-input manifest. Source materialization reads those immutable file
Artifacts into a fresh `generated/` directory and independently checks the complete file set and
digests. Neither transport success nor materialization integrity is a Gate verdict.

## Lifecycle

```text
Captured -> Specified -> Generating -> Building -> Verifying
                                            ^          |
                                            |          v
                                            +----- Optimizing -> Integrating -> Releasable -> Released
```

Build or correctness failures may return to generation. An optimization candidate returns through
build and verification. Integration failure may return to generation. `Failed` is terminal and is
reachable from every non-terminal state.

## Release gates

The mandatory gates are:

1. Contract
2. Source
3. Build
4. Correctness
5. Performance
6. Integration

The Source gate requires immutable references to generated Ascend C, host glue, build integration,
and source-to-target mapping artifacts. It is separate from Build because compiling a hidden binary
or framework fallback must not satisfy the source migration contract.

## First acceptance specimen

Use a bounded reduction extension with CUDA device code, explicit block synchronization, host launch,
CMake, and one public C/C++ function. The specimen is the smallest case that exercises more than
elementwise translation while remaining finishable as one product slice.

The observer may provide the original CUDA extension, workload, reference environment, and expected
public behavior. The observer must not provide the target compute implementation or a completed
Ascend host/build skeleton. Those are product outputs.

## Consequences

- `TargetBackend` is removed from the product task; Phase 1 targets Ascend by definition.
- the old route vocabulary is removed from core and CLI output;
- fixed executor fixtures remain runtime tests and do not become migration candidates;
- Designs 0003 and 0004 retain useful evidence/corpus ideas but are no longer implementation
  authorities;
- public APIs, UI, retention, and general scheduler work remain deferred until this slice passes.

## Verification plan

- unit tests reject every incomplete MigrationSpec field and unsafe path class;
- deterministic inspection binds its report to the spec and declared-source digests, checks CUDA
  kernel/indexing, host launch/error handling, public symbol, and build references, and rejects the
  initial unsupported CUDA construct set before generation;
- authoring tests reject stale inspection/source pairings, undeclared context, missing output
  categories, source-overwriting or escaping paths, and model-authored authority fields;
- lifecycle tests exercise correction loops and terminal-state behavior;
- candidate tests bind candidates to both strategy and MigrationSpec digest;
- release tests fail when the Source gate or its evidence is absent;
- architecture documentation points to the product execution plan as the only active work order.
