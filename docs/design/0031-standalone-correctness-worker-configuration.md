# Design 0031: Standalone correctness worker configuration

- Status: Implemented
- Date: 2026-08-12
- Extends: Designs 0022 and 0030
- Scope: strict process configuration and production composition for correctness-only workers

## Context

Design 0030 provided trusted policies, supervisors, and reusable runtimes, but the standalone worker
binary could still assemble only fixed smoke fixtures. Treating correctness as a flag on a fixture
configuration would retain irrelevant fixed-bundle authority and make the advertised execution kind
ambiguous.

## Decisions

The unified schema-1 worker file adds two explicit backend tags: `cuda_correctness` and
`ascend_correctness`. They have dedicated deny-unknown-fields policy documents and do not accept a
fixture ID or bundle digest. The controller supplies a different immutable execution-bundle
Artifact per assignment; the local file exclusively owns image identity, device selection,
environment facts, sandbox/CAS roots, command paths, transfer bounds, and resource ceilings.

CUDA startup uses the established bounded `nvidia-smi` inventory and eligibility path, binds one
device, constructs the role-specific policy from the advertised environment, and attaches a
`CudaCorrectness`-only runtime. Ascend startup requires the complete configured device identity,
exact host device-node set, fixed driver path, bounded `npu-smi` discovery, and environment match
before attaching an `AscendCorrectness`-only runtime.

Both compositions retain the existing filesystem CAS, remote Artifact downloader/publisher,
Docker engine, durable journal, bound-device heartbeat, device preflight/lease/cleanup, and outbound
control session. Runtime attachment adds only the matching correctness feature and exclusive local
admission policy.

## Rejected alternatives

- Add `correctness: true` to fixture policy: one file could silently carry two incompatible bundle
  and output contracts.
- Put the assignment bundle digest in the worker file: correctness experiments need immutable but
  per-assignment bundle identities.
- Infer correctness mode from worker ID or features: configuration authority must be explicit and
  schema validated.
- Add environment-variable fragments for the new mode: backend facts remain atomic in the single
  worker file.

## Verification

Tests parse both backend variants, reject a crossed backend document, and parse the checked-in CUDA
and Ascend correctness examples as part of the strict schema suite. Clippy, architecture limits,
and all existing worker runtime tests remain green. Real-device execution is deliberately separate:
the examples contain unusable placeholder identities, and no hardware receipt is claimed until an
operator replaces them with independently observed pinned facts.
