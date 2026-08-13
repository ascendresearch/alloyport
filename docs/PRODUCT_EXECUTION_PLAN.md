# AlloyPort product execution plan

- Date: 2026-08-12
- Status: Active
- Governing product decision: [Design 0009](design/0009-product-definition-and-staged-cuda-scope.md)
- First implementation contract: [Design 0024](design/0024-migration-spec-v1-and-first-product-slice.md)
- Active implementation decision: [Design 0025](design/0025-pluggable-llm-provider-architecture.md),
  accepted by the user on 2026-08-12

## Outcome

The next milestone is not another control-plane capability. It is the first complete demonstration
that AlloyPort can migrate a bounded CUDA extension into maintainable Ascend C and release the
result with independent evidence.

```text
CUDA extension intake
  -> MigrationSpec v1
  -> Ascend C candidate generation
  -> independent CUDA reference execution
  -> independent Ascend build and execution
  -> correctness and performance gates
  -> host/build integration patch
  -> EvidenceBundle and release manifest
```

The fixed CUDA and Ascend fixtures remain infrastructure health checks. They are not product
acceptance because their target implementations and execution contracts are prepared in advance.

## Development and runtime roles

The repository developer and coach builds deterministic contracts, tools, gates, fixtures, and
feedback. The developer does not act as the migration driver and must not prewrite the target
implementation to make an acceptance specimen pass.

The runtime LLM is replaceable. The current configurable default is `deepseek-v4-pro`; it is not a
provider codec or special code path. Its resolved model, deployment, protocol, profile, generation
parameters, and consumed context belong in episode/attempt receipts, not in the migration domain
model. The runtime model iteratively analyzes, requests controlled tools, and authors candidates; it
may not approve correctness, performance, knowledge, or release.

## Scope lock

Until the first product slice passes, infrastructure is frozen by default. Work may repair a defect
that blocks the slice, but may not add a new general platform capability merely because it would be
useful later.

Explicitly deferred:

- interaction retention, cursor-expiry policy, and a terminal UI;
- a public task-submission API or general scheduler policy;
- automatic Artifact garbage-collection scheduling;
- multi-worker optimization, server replication, and control-plane high availability;
- certificate issuance and online enrollment;
- additional fixed execution fixtures unrelated to the product slice;
- wholesale rewrites of the Python oracle or agent harness for language uniformity;
- retirement or modification of the separate `ascend-factory` repository.

## First product specimen

The first acceptance specimen is a bounded reduction extension. It must contain:

- CUDA device source with a block-level reduction and explicit synchronization;
- CUDA host launch code and error propagation;
- a CMake build target;
- one C or C++ public function preserved by the migration;
- deterministic correctness inputs plus boundary and randomized cases;
- a source-side CUDA reference command;
- an Ascend C implementation, host launch code, and target build produced by the migration path.

Vector addition remains a smoke test. It is too weak to validate synchronization, reduction
semantics, launch mapping, or meaningful source analysis. The reduction specimen is deliberately
small enough to finish while exposing those product obligations.

## Delivery phases

### P1 — Contract and domain correction

Deliver:

- `MigrationSpec v1` and validation;
- Ascend-C-producing generation strategies that replace the obsolete outcome routes;
- a lifecycle aligned with specification, generation, build, verification, optimization, and
  integration;
- a release gate that rejects a candidate without generated source artifacts;
- the reduction specimen's checked-in intake contract and expected release inventory.

Exit criteria:

- the domain model cannot represent a successful backend-only or portable-DSL-only delivery;
- invalid or incomplete Phase-1 intake fails before candidate generation;
- every candidate is bound to an immutable MigrationSpec digest and generation strategy.

### P2 — One correct end-to-end migration

Deliver:

- source/build inspection sufficient to construct the specimen's MigrationSpec;
- one durable, bounded Agent Episode that demonstrates a Gate-failure/candidate-correction turn using
  a configuration-selected runtime model and curated evidence from `ascend-factory`;
- CUDA and Ascend runs joined by one experiment identity while retaining separate receipts;
- an independent oracle and calibration mutants;
- generated Ascend C, host glue, CMake changes, component mapping, supported domain, and fallback.

Exit criteria:

- no observer-written target compute or hidden framework fallback is needed;
- the original public function runs through the generated Ascend implementation;
- a clean compatible worker can replay the build and evidence;
- the release manifest points to source, correctness, performance, and integration evidence.

### P3 — Correctness-constrained optimization

Only after P2 proves the durable correction loop and a feasible candidate, expand the already
persisted candidate frontier into EvoKernel-inspired correctness-constrained optimization:

- drafting reward from anti-bypass, build, and correctness gates;
- refinement reward from measured latency of correctness-passing candidates;
- cross-task success and failure memory;
- retrieval value learned from observed gate and performance outcomes rather than semantic
  similarity alone;
- profiler-guided candidate selection and an explicit stopping budget.

The first implementation may use simple persisted reward statistics. It does not require a general
reinforcement-learning platform.

### P4 — Second specimen and generality test

Select a second bounded CUDA extension from a different semantic family. Re-run the same contracts
without changing product infrastructure for that specimen. Only evidence from this phase may
justify promoting repeated transformation patterns into reusable knowledge.

## Reuse policy for `ascend-factory`

Reuse behavior and evidence, not its old product boundary:

| Reuse | Rebuild behind AlloyPort contracts | Do not carry forward |
|---|---|---|
| Oracle reference tiers and mutation calibration | Agent loop and candidate authoring | PyTorch-project-first intake |
| Same-byte reference/port lineage | Evidence-backed memory and retrieval | SSH/SCP runtime transport |
| Verified Ascend C vector and Cube examples | Workspace and source release assembly | Regex recipes as the primary migration model |
| CANN/SIMT falsification results | Profiler and optimization adapters | Model self-reports as evidence |

Python/NumPy verification code may remain an external tool behind a typed contract during P2. A
Rust rewrite is justified only by a measured deployment, trust, or operability need.

## Work order

Only the first unfinished item is active. Items 1 through 3 have landed. Item 4 is split so that
model connectivity cannot silently acquire execution or verification authority. The provisional
DeepSeek-specific, one-shot integration exposed a faulty provider and control-loop boundary. The
research report and complete Design 0025 now precede provider refactoring or Source Gate work:

1. [x] Implement and test Design 0024 domain contracts.
2. [x] Add the reduction extension intake fixture and frozen expected public behavior.
3. [x] Implement source/build inspection for that fixture.
4. [x] Build the durable iterative candidate loop, then connect it to the existing worker substrate.
   - [x] Define the provider-neutral, untrusted authoring request/source-bundle domain contract.
   - [x] Materialize the proposed `generated/` tree create-only from immutable file Artifacts and
     independently reread its exact file set, sizes, and digests.
   - [x] Complete the source-linked agent-runtime/provider study, including `ascend-factory`,
     EvoKernel, provider protocols, durable agent runtimes, and context/recovery behavior.
   - [x] Rewrite Design 0025 from that evidence, including five loop owners, durable recovery,
     protocol-native continuation, tool authority, search, security, budgets, migration, and tests.
   - [x] Obtain user review and acceptance of Design 0025.
   - [x] Implement episode/model-attempt/decoded-turn/tool/search domain records, strict
     model-catalog resolution, and a scripted fake gateway without network access.
   - [x] Prove the durable multi-tool-turn failure/correction loop with restart, cancellation,
     reconciliation, stop review, and budget fault injection.
   - [x] Implement fixture-driven `openai_responses`, `openai_chat_completions`, and
     `anthropic_messages` codecs with exact native continuation.
   - [x] Implement bounded transport over the strict catalog/deployment/auth records, and provide a
     config-selected `deepseek-v4-pro` default without vendor branches in the loop.
   - [x] Connect iterative candidate submission to CAS-backed create-only materialization and
     implement the independent Source Gate over the exact materialized tree, including one
     same-episode failure/correction turn.
   - [x] Only after the Source Gate passes, dispatch one bounded, independently receipted Ascend
     build attempt and return compiler failure to the same Episode.
5. Add the independent differential oracle and calibration battery. This remains the only active
   item.
   - [x] Implement the reduction run/experiment/verdict contracts, ten-mutant calibration battery,
     paired-execution Port, and Build-Gate-authorized Agent tool.
   - [x] Prove a Build-passing candidate reaches calibrated Correctness PASS through a durable fake
     Episode without device or provider access.
   - [x] Freeze controller-authored paired execution bundles, exact corpus coverage, and the callable
     candidate ABI required by trusted worker harnesses.
   - [x] Connect the paired-execution Port to two typed, idempotent worker-control assignments and
     validate their generic and structured terminal receipt chain.
   - [x] Implement policy-bound CUDA-reference and Ascend-candidate bundle materialization, trusted
     harnesses, fixed container plans, runtime registration, and structured receipt publication.
   - [x] Add strict standalone correctness-worker configuration and production runtime composition.
   - [ ] Capture real CUDA/Ascend run, calibration, and Correctness receipts for the frozen corpus.
6. Produce the first complete release and evidence bundle.
7. Add two-stage optimization and value-backed memory.
8. Attempt a second specimen before expanding platform scope.

The reusable authoring domain recomputes intake inspection before exposing source bytes, omits
undeclared files, validates a four-part `GeneratedSourceBundle` below `generated/`, and rejects
model-authored authority. Under Design 0025 this bundle becomes an iterative
`submit_candidate_bundle` tool input; it is no longer the required final response of a one-shot
adapter. No live provider call is made and no generated target source is checked in during
development or tests. The old `author-candidate` command and DeepSeek/curl transport have been
deleted. Their useful security lessons survive in the bounded provider transport; their one-shot
control flow and vendor coupling do not.

## Non-goals for the first slice

- arbitrary CUDA repositories;
- CUDA Graphs, dynamic loading, inline PTX, or general third-party CUDA-library replacement;
- a framework-wide PyTorch backend;
- multi-tenant product APIs;
- a claim that a successful reduction migration generalizes to other operator families.
