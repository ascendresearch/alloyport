# 0018: Fixed CUDA container execution contract

- Status: Accepted
- Date: 2026-08-10
- Scope: first real CUDA fixture, local admission authority, immutable bundle/image identity,
  sandbox planning, resource bounds, and the boundary before process supervision

## Context

The outbound control plane, fake executor, resumable Artifact publication, and canonical event
ingestion prove distributed ordering without starting a real candidate process. The next stage must
execute CUDA source on one real worker without turning the existing generic `Container` or `Shell`
fields into a remote-code-execution escape hatch.

The legacy harness reaches a CUDA host over SSH, copies a directory, and assembles a `docker run`
shell string. That path is useful for parity evidence but is not the product executor. Its mutable
host address, key, remote root, image tag, arbitrary shell, and `StrictHostKeyChecking=no` behavior
must not cross into the worker protocol.

A read-only environment probe on 2026-08-10 established the first development target: one NVIDIA
GB10, driver 580.159.03, and Docker 29.2.1. The existing `af-worker-cuda` image runs CUDA 13 PyTorch
but lacks `nvcc`. A pre-existing CUDA 13.0.1 development image has a registry manifest digest and
compiler, but is large and not AlloyPort-owned. It is suitable for an initial manually approved
probe, not the final minimal worker image.

## Decision

Protocol minor 3 adds `EXECUTOR_KIND_CUDA_FIXTURE`. It is not an alias for generic container
execution. Default worker policy rejects it, and an operator must explicitly enable it. The first
accepted fixture is `cuda-vectoradd-v1`, a self-contained `.cu` program with device code, host launch,
CUDA allocation/copy/synchronization, and deterministic result verification.

`CudaFixturePolicy` is a worker-local allowlist binding all of the following:

- fixture ID and the only accepted argv value;
- exact content-addressed fixture bundle;
- exact OCI image manifest digest and the expected local image filesystem ID;
- one enumerated CUDA device;
- a worker-owned absolute sandbox root;
- maximum CPU, memory, tmpfs disk, process, and output budgets.

The assignment must request only the `cuda-fixture-v1` feature, an empty environment, fixture-root
working directory, one device, and disabled networking. Missing, zero, unknown, or excessive limits
are rejected. The server cannot add mounts, host paths, environment variables, Docker options, or a
shell command.

## Bundle and materialization

The first bundle media type is `application/vnd.alloyport.cuda-fixture.v1+json`. It contains a schema
version, fixture ID, source bytes, and an independent source digest. The enclosing Artifact digest
binds the complete manifest, while the inner digest makes source identity visible in receipts and
parity tools.

The worker opens the bundle through its verified local CAS reader, checks declared size and both
digests, validates a process-safe attempt ID, and writes only two fixed filenames beneath its local
sandbox root: the CUDA source and a worker-owned runner. Materialization uses create-once semantics.
An identical existing file is restart-idempotent; changed bytes are an integrity failure. No archive
path or server-chosen filename is extracted.

Before a CUDA assignment is stored or sent, the controller requires its bundle digest to exist in
the managed Artifact metadata with the exact declared size, then creates an idempotent
`AssignmentInput` reference for the assigned worker. A CUDA assignment is rejected when Artifact
metadata is unavailable. The worker's bounded downloader requires contiguous server offsets and the
exact final size/digest before ingesting into its verified local CAS. An already verified local copy
makes reconnect/restart idempotent without another download.

The trusted runner invokes `nvcc` and then `exec`s the resulting fixture binary using argument
vectors. It does not parse assignment text or invoke a shell.

## Container plan

The worker derives, rather than receives, the Docker create argv. The plan has a deterministic
container name and attempt/bundle/image labels for later recovery. It selects:

- `--network none`, a read-only root filesystem, all capabilities dropped, and
  `no-new-privileges`;
- CPU quota, memory, PID, and output policy from validated bounded limits;
- exactly one worker-selected GPU;
- one read-only worker-owned bundle mount;
- size-bounded executable tmpfs filesystems for compilation output and compiler temporary files,
  whose combined capacity equals the assignment disk budget;
- the locally pinned image and worker-owned Python runner, with no shell.

This revision deliberately produces the exact create plan but does not yet invoke Docker. The next
implementation slice must add a durable container supervisor that verifies the local image ID,
creates or reattaches by deterministic identity, streams bounded logs, stops on cancellation or
timeout, retains an exited container until its Artifact publication and terminal journal commit,
and records driver/device/image facts in a Design 0007 receipt.

## Evidence and parity

The fixed source lives in `fixtures/cuda-vectoradd-v1/vector_add.cu`. Success requires its own
device-result comparison and a deterministic `PASS` record; exit zero alone is insufficient. The
event stream and terminal Artifact gate remain observed execution evidence, not an oracle verdict.

Once process supervision is attached, the same source bundle will run as two distinct attempts:

1. the legacy SSH/container path, retained only as a parity reference;
2. the outbound AlloyPort worker path.

Parity compares bundle and source digests, requested manifest and resolved image identity,
driver/device/toolkit facts, stdout/stderr, exit classification, and receipt fields. It does not
claim that transport parity proves CUDA-to-Ascend correctness.

## Rejected alternatives

- Reuse `Shell`: rejected because a server-authored shell string bypasses local executor policy.
- Treat generic `Container` as sufficient: rejected because it does not bind mounts, device,
  image, command family, or recovery identity tightly enough for the first real execution.
- Copy the old SSH wrapper: rejected because SSH is an installation/break-glass mechanism, not the
  product control or Artifact plane.
- Use the mutable `af-worker-cuda:<env-sha>` tag alone: rejected because a tag is not executed image
  identity, and that image currently lacks the required compiler.
- Start with matrix multiplication: rejected because compiler/runtime/process failures should be
  separated from performance and accumulation-tolerance questions in the first slice.

## Verification

Current tests prove default-deny admission, explicit typed opt-in, controller input grants only for a
published size-matched bundle, bounded digest-verified download and local replay, field/limit allowlisting,
verified bundle parsing, independent source-digest rejection, restart-idempotent materialization,
conflicting sandbox-byte rejection, stable process identity, and Docker argv construction without a
shell, network, arbitrary mounts, or server-selected devices.

The supervisor slice must add fake-engine state-machine tests followed by an explicitly invoked real
GB10 smoke test. The first explicit probe compiled and verified 1,048,576 elements on the target,
producing the deterministic checksum `670562424`. No CI test may silently depend on a GPU or Docker
daemon.
