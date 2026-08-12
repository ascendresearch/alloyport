# 0018: Fixed CUDA container execution contract

> Design 0022 revises this document's registry-only image identity, per-backend bootstrap file, and
> fixed device configuration. Registry manifests and standalone local image IDs are both accepted
> immutable identities; the worker now uses one configuration file and shared device selection.

- Status: Accepted
- Date: 2026-08-10
- Scope: first real CUDA fixture, local admission authority, immutable bundle/image identity,
  sandbox planning, resource bounds, and durable container reconciliation

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

Design 0020 formalizes the placement boundary used here: the trusted worker supervisor remains
outside candidate execution, even if operations later packages that supervisor in its own container.

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
- a worker-selected `json-file` log driver with two size-bounded files, so daemon-side candidate
  output storage cannot grow without bound before collection;
- exactly one worker-selected GPU;
- one read-only worker-owned bundle mount;
- size-bounded executable tmpfs filesystems for compilation output and compiler temporary files,
  whose combined capacity equals the assignment disk budget;
- the locally pinned image and worker-owned Python runner, with a locally fixed `python3` entrypoint
  that bypasses image-authored entrypoint wrappers and no shell.

The engine-neutral supervisor verifies the resolved local image ID before inspecting or creating a
container. It reconciles the deterministic name through `Created`, `Running`, and `Exited` states,
requires exact attempt/bundle/manifest/image identity on every reattach, and refuses to delete or
reuse a conflicting container. Missing containers are created once, created containers are started,
running containers are waited without another start, and exited containers are replayed only to
recover their terminal result and logs. Cancellation, timeout, and running-output exhaustion stop
the same identified container and then wait for its terminal state. Output exhaustion, nonzero exit,
and exit zero without the fixture's `PASS` marker have distinct fail-closed outcomes.

`DockerCliEngine` implements this boundary with `std::process::Command` argv and no shell. Image and
container inspect JSON are size-bounded and parsed into exact identities and phases. An inspect
failure is considered absence only when a second exact-name `container list` succeeds and returns no
container. `docker wait` is cross-checked against the inspected exit code, while inspected RFC 3339
start/finish timestamps recover elapsed time after restart. Stdout and stderr pipes are drained
concurrently under one combined byte counter, retain only bounded bytes, and preserve an exhaustion
flag for terminal classification. For a running container, `docker logs --follow` is itself
terminated as soon as that counter exceeds the budget; the supervisor then stops and waits for the
identified container. Removal is a separate idempotent operation.

`CudaExecutionRuntime` marks `Running` before supervision, spools stdout/stderr and a typed receipt to
the local CAS, optionally publishes all three objects, and only then commits terminal journal/outbox
state. The receipt binds bundle/source/manifest/resolved-image/device identity plus configured worker
architecture, driver, and toolkit observations. Container removal occurs after the terminal commit;
if removal fails, the terminal result stays durable and a terminal replay retries cleanup without
rerunning the fixture. Supervisor or publication failures instead leave the attempt `Running` and
retain the identified container for safe reconciliation.

As revised by Design 0022, `Running` is now preceded by the shared durable `DeviceGuard`: an
exclusive worker-local device lease is committed before a fresh process/health probe, and immutable
preflight evidence is persisted before the phase transition. The receipt records the lease and
pre/post observations. Terminal commit does not release the lease; container removal and a fresh
`Ready`, process-free observation must both succeed. Recovery-action faults, unknown processes, or
probe failure retain quarantine across replay.

`OutboundWorker::with_cuda_executor` explicitly attaches this runtime, changes local admission to
CUDA-fixture-only, verifies worker/runtime identity plus CUDA architecture/driver/toolkit facts, and
uses the same active-attempt cancellation registry as the fake runtime. Session startup retries
idempotent cleanup for terminal CUDA attempts without allowing cleanup failure to block durable
terminal outbox delivery. Before CUDA supervision, an optionally attached remote downloader fetches
the granted exact bundle into the verified local CAS; retry reuses an already verified object.

As revised by Design 0022, the worker binary constructs this stack from one strict schema-1 JSON
configuration. Unknown fields and partial policies are rejected. It pins the fixture, bundle, image
artifact and resolved local image ID, local device-selection policy, absolute non-overlapping
sandbox/CAS roots, ceilings, Artifact bounds, absolute Docker and `nvidia-smi` paths, and stop grace
period. The same file produces matching CUDA hello environment facts, concurrency one, and Docker
capability. Absence of the file retains default-deny admission. The binary always attaches both the
input downloader and terminal
publisher over its authenticated controller endpoint; there is no binary mode that executes CUDA but
reports unauthoritative digest strings without publication. The log follower enforces early
output-limit termination and forwards bounded best-effort chunks with independent stdout/stderr
offsets. A bounded internal channel uses nonblocking sends: preview pressure can omit chunks but can
never block pipe draining, change the shared output-budget reservation, or alter terminal bytes.
Later delivered offsets make omissions visible to canonical ingestion. The runtime does not emit a
second terminal copy when live following was active. Real GB10 validation, including this preview
path, is complete.

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
published size-matched bundle, bounded digest-verified download and local replay, field/limit
allowlisting, verified bundle parsing, independent source-digest rejection, restart-idempotent
materialization, conflicting sandbox-byte rejection, stable process identity, and Docker argv
construction without a shell, network, arbitrary mounts, or server-selected devices.

Fake-engine state-machine tests cover missing-container create/start, exited replay without another
create/start, running reattach, cancellation and timeout stop/wait, exact-identity conflict,
resolved-image mismatch, active stop on running-output exhaustion, nonzero exit, and exit zero
without the fixture marker.
Docker-adapter tests cover exact inspect identity/timestamp parsing, unsupported-state rejection,
bounded pipe draining, one combined stdout/stderr follow budget, exact `logs --follow` argv,
missing-container discrimination, log-exhaustion propagation, and idempotent terminal removal
without contacting a daemon. Runtime coverage proves
that publication observes `Running`, terminal commit precedes removal, a cleanup failure preserves
the terminal receipt/container, and replay retries only cleanup. A loopback gRPC test sends a real
typed CUDA assignment through the controller, runs it with a fake container engine, publishes all
three terminal Artifacts, survives a post-commit cleanup failure, reconnects, reports the terminal
outbox, and removes without rerunning. Its worker CAS begins empty, so the same test also proves that
the controller grant authorizes an exact remote bundle download before supervision. Binary config
parsing rejects unknown fields, overlapping roots, unpinned identities, and unsafe partial policy.

An ignored, explicitly configured real-engine test passed on the GB10 on 2026-08-11. It exercised
outbound loopback gRPC, a remotely granted bundle download into an empty worker CAS, the real Docker
CLI engine, CUDA compilation and kernel execution, terminal Artifact publication, controller
acceptance, and post-commit removal. The receipt bound bundle `sha256:04b086...610e`, source
`sha256:631447...9a95`, manifest `sha256:54b468...584a`, resolved image
`sha256:79a186...d884`, device `0`, `sm_121`, driver `580.159.03`, and toolkit `13.0`; it recorded
exit 0, empty stderr, 1,334 ms, and the deterministic checksum `670562424`. A separate direct
SSH/Docker legacy reference attempt produced the identical single-line stdout and exit. The first
real run exposed output from the image-authored NVIDIA entrypoint, so the locally derived plan now
sets `--entrypoint python3` explicitly; the rerun restored exact stdout parity. No normal CI test may
silently depend on a GPU or Docker daemon. A later live-preview rerun preserved the same terminal
digests and canonical stdout with a 1,335 ms receipt. Unit and loopback coverage additionally prove
combined-budget reservation across streams, independent offsets, nonblocking preview drop, exact
terminal CAS bytes, and absence of duplicate terminal previews.
