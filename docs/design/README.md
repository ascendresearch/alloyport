# Design documents

This directory is the canonical home for AlloyPort architecture and system-design decisions.
Documents are numbered so that discussions, implementation work, and evidence can reference a
stable identifier.

## Index

| ID | Document | Status |
| --- | --- | --- |
| 0001 | [System architecture](0001-system-architecture.md) | Product boundary superseded by 0009 |
| 0002 | [Long-horizon runtime and goal persistence](0002-long-horizon-runtime.md) | Proposed |
| 0003 | [Operator specification and workload corpus](0003-operator-spec-and-workload-corpus.md) | Proposed; revision required by 0009 |
| 0004 | [Candidate routing and backend strategy](0004-candidate-routing-and-backend-strategy.md) | Proposed; revision required by 0009 |
| 0005 | [Correctness oracle and calibration](0005-correctness-oracle-and-calibration.md) | Proposed |
| 0006 | [Performance evidence and claims](0006-performance-evidence-and-claims.md) | Proposed |
| 0007 | [Worker isolation, receipts, and reproducibility](0007-worker-isolation-receipts-and-reproducibility.md) | Proposed |
| 0008 | [Evidence-backed knowledge lifecycle](0008-evidence-backed-knowledge-lifecycle.md) | Proposed |
| 0009 | [Product definition and staged CUDA scope](0009-product-definition-and-staged-cuda-scope.md) | Accepted |
| 0010 | [Interactive terminal and event stream](0010-interactive-terminal-and-event-stream.md) | Accepted; first vertical slice implemented |
| 0011 | [Outbound worker control plane](0011-outbound-worker-control-plane.md) | Accepted; first contract slice implemented |
| 0012 | [Filesystem artifact content-addressed store](0012-filesystem-artifact-cas.md) | Accepted; implemented |
| 0013 | [Durable certificate enrollment and stable worker identity](0013-durable-certificate-enrollment.md) | Accepted; implemented |
| 0014 | [Durable Artifact references and conservative garbage collection](0014-artifact-references-and-garbage-collection.md) | Accepted; implemented |
| 0015 | [Typed deterministic fake executor runtime](0015-typed-fake-executor-runtime.md) | Accepted; implemented |
| 0016 | [Gated remote execution Artifact publication](0016-gated-remote-artifact-publication.md) | Accepted; implemented |
| 0017 | [Canonical worker interaction-event ingestion](0017-canonical-worker-interaction-events.md) | Accepted; implemented |
| 0018 | [Fixed CUDA container execution contract](0018-fixed-cuda-container-contract.md) | Accepted; contract slice implemented |
| 0019 | [Authorized interaction replay and subscription](0019-authorized-interaction-replay-and-subscription.md) | Accepted; implemented |
| 0020 | [Worker supervisor placement and per-attempt isolation](0020-worker-supervisor-placement-and-attempt-isolation.md) | Accepted |
| 0021 | [Fixed Ascend worker execution contract](0021-fixed-ascend-worker-contract.md) | Accepted; direct device gate passed, outbound gate pending |
| 0022 | [Standalone worker configuration and shared device selection](0022-standalone-worker-configuration-and-device-selection.md) | Accepted; first implementation slice complete |

## Convention

Each design document records its status, context, decision, applicable invariants, rejected
alternatives, and verification plan. Use one of these states:

- **Proposed:** open for design review; implementation must be treated as experimental.
- **Accepted:** the current implementation target.
- **Implemented:** the design is backed by code and automated verification.
- **Superseded:** retained for history and linked to its replacement.

Accepted documents should not be silently rewritten when an architectural decision changes. Add a
new numbered document, mark the old one as superseded, and preserve the reasoning trail.
