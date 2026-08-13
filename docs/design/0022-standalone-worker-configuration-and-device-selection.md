# 0022: Standalone worker configuration and shared device selection

- Status: Accepted; first implementation slice complete; locator amended by Design 0039
- Date: 2026-08-11
- Scope: open-source trial deployment, worker bootstrap, image identity, and GPU/NPU selection
- Revises: the registry-only image and environment-variable bootstrap portions of Designs 0018 and
  0021

## Context

An open-source user must be able to run a worker against a local AlloyPort server with Docker and a
locally built fixture image. Requiring access to an operator-provided OCI registry is an integration
option, not a valid prerequisite for evaluating the project.

The previous worker binary also split one logical configuration across connection, TLS, identity,
capability, journal, and backend-specific environment variables plus a separate policy JSON file.
That made it easy to start a worker whose advertised facts did not match its execution policy.

Finally, the safety rule discovered for the shared Ascend host is not NPU-specific. A CUDA device is
also ineligible when it is unhealthy, has a visible compute process, or is protected by a durable
worker lease. Backend tools produce different raw data, but selection policy should not diverge.

## Decision

### One worker configuration

The production worker starts from one strict schema-1 JSON file. It contains:

- server endpoint and, for a remote endpoint, client identity, server CA, and server name;
- stable worker ID and worker-local SQLite journal path;
- exactly one runtime backend;
- backend environment facts and a complete local execution policy.

The command is `alloyport-worker --config PATH`. `ALLOYPORT_WORKER_CONFIG` may locate the same file
for service managers, but individual policy facts are not assembled from environment variables.
Design 0039 later adds deterministic executable-sibling and system-wide discovery when neither
explicit locator is present.
Unknown fields, partial backend policy, relative security-sensitive binary paths, and remote
plaintext endpoints fail closed. Loopback HTTP remains available for local evaluation.

### Registry-optional immutable image identity

There are two accepted image identity modes:

1. A registry-backed installation uses `repository@sha256:...`, an OCI image-manifest digest in the
   assignment, and separately verifies the resolved local Docker image ID.
2. A standalone installation may use a local Docker name or tag. Its assignment image digest must
   equal the exact `docker image inspect .Id`, and its media type is
   `application/vnd.oci.image.config.v1+json`.

In both modes the supervisor resolves the reference immediately before reconciliation and requires
the exact configured local image ID. A tag by itself is never evidence: changing the image behind a
local tag changes its ID and causes execution to fail. An OCI registry is therefore optional without
weakening the immutable execution identity.

The controller must construct the assignment image Artifact using the same digest and media type as
the worker policy. Fixture image bytes are not uploaded to AlloyPort's Artifact service; image
distribution remains an installation concern and may be a local build, archive load, or registry
pull.

### Shared selection policy

Backend adapters retain responsibility for trustworthy discovery:

- NVIDIA uses bounded, shell-free fixed `nvidia-smi` queries;
- Ascend uses bounded, shell-free fixed `npu-smi` queries.

Both feed one backend-neutral selector. A candidate is eligible only when all of these are true:

- its static identity and dynamic observation are complete and mutually consistent;
- it is inside the optional local allowlist;
- health is `Ready`;
- visible compute process count is zero;
- no active durable worker device lease names it.

An optional preferred device changes ordering only; it never overrides eligibility. Utilization and
memory counters are telemetry and are not substitutes for process occupancy. Inconsistent or
incomplete probe output fails closed.

The selected static identity is written into the worker's registered capabilities before the
control session. Heartbeat telemetry is then produced through a backend-neutral bound-device view
that performs a fresh device-specific observation and reports only that identity. A single-device
worker must not leak or claim the other accelerators discovered on its host.

NVIDIA health is derived from the documented `gpu_recovery_action`. Only `None` maps to `Ready`;
reset/reboot/drain actions map to `Unhealthy`, unsupported evidence maps to `Degraded`, and unknown
values invalidate the observation. Successful `nvidia-smi` execution alone has no health meaning.

CUDA and Ascend then use the same durable `DeviceGuard` for every attempt. It acquires the
worker-local lease before probing, persists immutable preflight evidence before `Running`, and
retains the lease across crashes and uncertain cleanup. After terminal publication and commit, the
container is removed first; only a fresh `Ready` plus zero-process observation permits release.
Visible unattributed processes or failed/unauthorized recovery retain quarantine.

At process startup, a retained lease is recovery state rather than a candidate-selection input. A
single-device worker binds the exact leased device even if it is currently unhealthy or occupied so
terminal reconciliation can proceed; capacity remains consumed and no new attempt is admitted.
Leases spanning multiple device IDs fail closed.

The first CUDA slice may select any eligible allowlisted GPU and binds the resulting device into its
local fixture policy. The first Ascend slice additionally requires the selected device's complete
identity to match its configured identity because the current receipt and durable recovery contract
are device-specific. Multi-device attempt dispatch is later work; this decision supplies the common
eligibility primitive, not a server-side fleet scheduler.

## Security and portability consequences

- No registry credential, SSH credential, remote host, or server-selected host path enters worker
  configuration.
- Registry publication may improve distribution and provenance, but absence of a registry cannot
  block local trials.
- Device choice remains local worker authority. The server requests one accelerator but cannot name
  a host device.
- GPU and NPU command execution shares the same timeout and retained-output boundary while keeping
  parsers backend-specific.
- Example files contain unusable digests and placeholder hardware facts. Users obtain the local
  image ID with Docker inspection and hardware facts from the backend probe on their own worker.

## Deliberate limits

- The repository is distributed under the MIT License.
- The server has no public task-submission API yet. It is intentionally deferred until a real
  outbound fixture has run through the existing internal assignment use case.
- Automatic reset remains unauthorized. A device that cannot be proven reusable must stay excluded
  or quarantined.

## Verification

Tests cover both immutable image modes, rejection of a mutable tag whose assignment digest differs
from the local image ID, strict unified configuration, selection around busy/unhealthy/leased
devices, inconsistent inventory rejection, explicit NVIDIA recovery-action health, bounded command
execution, CUDA/Ascend durable preflight, cleanup replay, and quarantine retention. Workspace fmt,
clippy, architecture checks, and tests remain the release gate.
