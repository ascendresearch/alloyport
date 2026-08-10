# AlloyPort handoff

- Handoff date: 2026-08-10
- Repository: `/data/projects/shinesheep/alloyport`
- Branch: `main`
- Baseline: this file is part of the repository's initial commit; run `git rev-parse HEAD` to obtain
  its local commit ID
- Project state: architecture bootstrap with the first worker-control and interaction-event slices

This document is the entry point for a new Codex session. It separates product intent, accepted
architecture, implemented behavior, and planned behavior so that work does not drift simply because
the previous conversation is unavailable.

## Read first

Read these documents before changing architecture or implementation:

1. [`../README.md`](../README.md) for the product boundary and development commands.
2. [`design/0009-product-definition-and-staged-cuda-scope.md`](design/0009-product-definition-and-staged-cuda-scope.md)
   for the accepted product definition.
3. [`design/0002-long-horizon-runtime.md`](design/0002-long-horizon-runtime.md) for long-running goal,
   audit, and anti-drift semantics.
4. [`design/0010-interactive-terminal-and-event-stream.md`](design/0010-interactive-terminal-and-event-stream.md)
   for the Codex-like user experience and canonical interaction events.
5. [`design/0011-outbound-worker-control-plane.md`](design/0011-outbound-worker-control-plane.md)
   for the accepted server/worker architecture.
6. [`design/0007-worker-isolation-receipts-and-reproducibility.md`](design/0007-worker-isolation-receipts-and-reproducibility.md)
   before implementing real execution or artifact persistence.
7. [`design/0012-filesystem-artifact-cas.md`](design/0012-filesystem-artifact-cas.md) for the first
   immutable artifact storage boundary and its remaining service-layer gaps.

Design documents state intended behavior. Tests and code state what this revision actually implements.

## Product invariant

AlloyPort is a verified CUDA-to-Ascend-C source migration and optimization factory.

- The input object is CUDA source code.
- A maintainable Ascend C implementation is a mandatory output.
- Phase 1 targets one bounded CUDA extension, including device kernels, host launch code, build
  integration, and one runnable public path.
- A later phase expands to complete CUDA modules/projects.
- PyTorch may be part of an input project's integration environment. AlloyPort is not a PyTorch NPU
  backend project and must not drift into implementing a framework backend for new hardware.
- Generation, compilation, execution, correctness, performance, evidence, and release are distinct
  stages. Agent narrative or a green terminal card is never proof of correctness.

The related bootstrap implementation at `/data/projects/ascend-factory` is useful source material and
still contains the Python agent harness and SSH/SCP worker path. It is a separate project and is not
automatically writable or in scope for changes from this repository.

## Accepted distributed topology

```text
user / CLI / future UI
          |
          v
AlloyPort server
  controller + scheduler + canonical events + receipts/artifact metadata
          ^
          | outbound bidirectional gRPC over mTLS
     +----+----+
     |         |
 CUDA worker  Ascend worker
     |         |
 isolated local executors and accelerator devices
```

Workers initiate outbound connections. They do not expose an AlloyPort command port. SSH is intended
to disappear from the product execution path and remain, if deployment policy needs it, only as a
separately authorized installation and break-glass operations channel.

There are three deliberately separate type systems:

1. Protobuf/gRPC worker messages carry live distributed control traffic.
2. `alloyport-events` carries the user-visible, replayable interaction stream.
3. Durable domain, receipt, and audit records decide long-horizon state and release authority.

Do not merge these into one generic event/message object. The server must explicitly validate and
translate between them.

## Implemented workspace

The workspace requires Rust 1.88 or newer.

### `alloyport-artifacts`

Defines an object-safe immutable `ArtifactStore`, canonical `Sha256Digest`, streaming `Read`-based
ingestion, verified readers, and a filesystem CAS. The filesystem implementation:

- hashes and writes through a bounded 64 KiB buffer rather than loading an artifact into memory;
- enforces a configured per-artifact byte limit plus optional declared digest and size;
- writes to a same-filesystem private staging directory, syncs and seals the file read-only, then
  atomically publishes with a hard link so an existing digest path is never overwritten;
- treats concurrent or repeated identical uploads as idempotent and verifies an existing object
  before reporting it as already present;
- removes failed uploads immediately and cleans crash-left staging files when the store reopens;
- verifies stored bytes against their digest when opening them and reports tampering as an integrity
  violation;
- uses `sha256/<first-two-hex>/<full-hex>` fanout paths while keeping media type and retention outside
  the content identity.

The crate also implements a SQLite-backed resumable upload session layer:

- sessions bind an authenticated-owner placeholder, idempotent upload key, expected digest/size,
  media type, committed offset, state, and expiry;
- chunks must match the exact committed offset and stay within configured chunk and upload limits;
- bytes are appended and synced before the SQLite offset transaction commits; after a crash, a
  longer uncommitted file tail is truncated back to the durable offset on the next append;
- finalization is serialized, idempotent, and publishes through the CAS; integrity failures become
  terminal while transient CAS I/O failures remain retryable in `Finalizing` state;
- completed data files and expired partial sessions are cleaned without deleting published CAS
  objects.

This is the storage and resumable-session core, not yet a network Artifact service. Owner identity is
an authorization seam supplied by the future service, not yet bound to mTLS identity. There is no
Artifact RPC, total-store quota, reference tracking, or garbage collection.

### `alloyport-core`

Dependency-light domain primitives for tasks, candidates, gates, verdicts, and release manifests.
Release construction requires every gate and independent evidence.

### `alloyport-events`

Typed producer events, canonical event envelopes, per-run sequencing, lifecycle reduction, JSONL,
and plain rendering. This is the first Codex-like interaction vertical slice. The server has not yet
translated worker protocol messages into these events.

### `alloyport-cli`

Provides `about`, lifecycle output, an event demo, and Python-producer JSONL ingestion/rendering. It
does not yet call the worker scheduler.

### `alloyport-proto`

Defines `alloyport.worker.v1` and generates Rust client/server bindings with `tonic` and `prost`.
Important files:

- `crates/alloyport-proto/proto/alloyport/worker/v1/worker_control.proto`
- `crates/alloyport-proto/build.rs`
- `crates/alloyport-proto/src/lib.rs`

The service uses one bidirectional `OpenControlStream`. The method is not called `Connect` because
tonic generates a client endpoint constructor named `connect`, producing a Rust method collision.

The schema currently includes:

- hello/welcome and protocol version fields;
- stable logical message IDs for durable control traffic plus a non-durable cumulative ACK frame;
- static worker/backend capabilities and dynamic heartbeat state;
- typed assignment and execution specifications;
- accepted/rejected, started, output, and finished lifecycle messages;
- cancel, cancellation-acknowledged, and drain messages;
- content-addressed artifact references and resource limits.

Validation currently enforces supported protocol major version, worker identity/capacity, typed
executor, sandbox-relative working directory, non-empty argv, and `sha256:` artifact digests.

Code generation is reproducible without system `protoc`: build dependencies pin `prost-build 0.14.4`,
`tonic-prost-build 0.14.6`, and `protoc-bin-vendored 3.2.0`. Clippy allowances are limited to the
generated `v1` module; do not weaken workspace lints for hand-written code.

### `alloyport-server`

Implements the gRPC service, in-process connection registry, crash-durable SQLite control repository,
non-terminal replay, attempt leases, and worker lifecycle classification. Generated Protobuf types
are translated at the RPC edge into separate storage-domain records.

Current behavior:

- requires the first worker frame to be a valid hello with sequence 1;
- registers logical worker ID separately from process instance and connection ID;
- sends welcome followed by queued/non-terminal assignments;
- applies an explicit SQLite migration for worker registrations, connection observations, immutable
  assignments, lifecycle observations, and leases;
- commits assignments before sending and commits the `Sent` state plus bounded lease before placing
  an assignment frame on the network channel;
- suppresses an identical attempt submitted twice;
- rejects reuse of an attempt ID with different content or worker identity;
- checks monotonically increasing worker sequence numbers;
- rejects cumulative acknowledgements that regress or name a server sequence not yet sent;
- records accepted, rejected, running, and finished states without regressing accepted/running state
  during replay;
- renews active leases from heartbeats, expires them with a periodic reaper, and retains a late
  finished observation as stale rather than replacing the expired attempt state;
- recovers queued and non-terminal assignments after a server process restart when the worker
  reconnects;
- persists cancellation requests, replays them after reconnect, records the worker acknowledgement
  separately from terminal completion, and prevents cancellation from reviving expired work;
- records assignment/cancel frame references in a durable per-connection server outbox before send
  and compacts only sequences cumulatively acknowledged after domain processing;
- retains disconnected-session frame mappings for seven days before policy-driven pruning while
  reconstructing still-relevant assignments and cancellations from durable domain records;
- exposes an explicit transactional lease-expiry reassignment operation that copies the immutable
  contract into a caller-supplied fresh attempt ID, increments the attempt number, and preserves the
  expired attempt for stale-result classification;
- marks a worker disconnected only if the closing stream still owns its connection ID, so an old
  superseded stream cannot disconnect a newer session.

`WorkerControlService::new()` remains an in-memory SQLite convenience for tests. The server binary
uses `ALLOYPORT_DATABASE` or defaults to `alloyport-control.sqlite3`, so normal server state survives
process restart.

The `alloyport-server` binary listens on `127.0.0.1:50051` by default. Plaintext is rejected on a
non-loopback bind address. Remote mode requires:

- `ALLOYPORT_LISTEN`
- `ALLOYPORT_TLS_CERT`
- `ALLOYPORT_TLS_KEY`
- `ALLOYPORT_TLS_CLIENT_CA`

The TLS configuration requests a client certificate, establishing the basis for per-worker mTLS.
Certificate enrollment, rotation, revocation, and authorization storage are not implemented.

### `alloyport-worker`

Implements an outbound client session, hello/welcome negotiation, heartbeat, cumulative server
acknowledgement, disk-backed attempt knowledge, local admission, and reconnectable session state.

Current behavior:

- validates its hello before connecting;
- includes locally accepted, running, and finished attempts from its SQLite journal in hello/heartbeat
  messages;
- checks monotonically increasing server sequence numbers and rejects regressing/future cumulative
  acknowledgements;
- validates every assignment locally and commits immutable admission before acknowledging it;
- returns `already_known` for an identical attempt replay;
- rejects reuse of an attempt ID with changed content;
- persists accepted/running/finished lifecycle and terminal result fields across worker process
  restart, and replays a durable finished result after an idempotent assignment replay;
- persists acceptance/rejection, started, finished, and cancellation-acknowledged messages in a
  logical outbox before send, maps them to connection-local sequences, replays them on reconnect,
  and deletes them only after an explicit server cumulative ACK;
- retains logical worker messages indefinitely until acknowledgement while pruning obsolete
  per-connection delivery mappings after seven days;
- denies the shell executor by default; only an explicit local `AdmissionPolicy` can allow it;
- exits a session on drain; cancellation is durably acknowledged and becomes terminal immediately
  while no executor process exists.

The worker binary uses `ALLOYPORT_WORKER_DATABASE` or defaults to `alloyport-worker.sqlite3`.
Admission identity is now disk backed, but do not enable real candidate execution until output
spooling, executor process identity, and crash recovery are durable too.

The `alloyport-worker` binary reconnects with capped exponential backoff. Required identity variables:

- `ALLOYPORT_SERVER`
- `ALLOYPORT_WORKER_ID`
- `ALLOYPORT_BACKEND` (`cuda`, `ascend`, or `npu`)
- optional capability fields: `ALLOYPORT_ARCH`, `ALLOYPORT_DEVICE_COUNT`,
  `ALLOYPORT_MAX_CONCURRENCY`, `ALLOYPORT_DRIVER_VERSION`, `ALLOYPORT_TOOLKIT_VERSION`, and
  `ALLOYPORT_CONTAINER_RUNTIME`

Remote TLS additionally requires:

- `ALLOYPORT_TLS_CERT`
- `ALLOYPORT_TLS_KEY`
- `ALLOYPORT_TLS_SERVER_CA`
- `ALLOYPORT_TLS_SERVER_NAME`

Remote plaintext endpoints are rejected. Loopback HTTP is permitted for tests and development.

## Delivery semantics that must survive future work

- Promise at-least-once delivery, never exactly-once remote execution.
- `attempt_id` is process-attempt identity; stream sequence is only transport ordering.
- Persist an assignment before send and persist worker admission before execution.
- Duplicate delivery of the same attempt must not start another process.
- Reassign only after a durable lease expires or the old attempt is definitively terminal.
- Retain and classify late/stale results; never silently overwrite accepted evidence.
- Cancellation is requested, acknowledged, and eventually terminal. Connection loss is not
  cancellation and is not an execution verdict.
- A worker authenticates the server but still enforces local executor, image, mount, device, network,
  secret, and resource policy.
- The server cannot name arbitrary host paths or device nodes.
- Full permitted output must be spooled as an artifact even when live previews are coalesced.
- Never use serialized Protobuf bytes as a digest/cache key. Protobuf serialization is not a canonical
  representation. Hash canonical files or a separately specified canonical domain form.
- A worker observation may create an interaction event; it cannot directly publish an oracle verdict
  or advance audited long-horizon state.

## Verification baseline

The following commands passed at the latest handoff verification:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo +1.88.0 test --workspace --locked
```

There are 51 Rust tests. Control-plane coverage includes a real loopback gRPC stream and SQLite
repository tests for:

- hello/welcome and worker registration;
- assignment delivery and worker acceptance;
- duplicate enqueue suppression;
- queuing while disconnected and replay after reconnect;
- queued and already-accepted assignment recovery after closing and reopening the server database,
  without lifecycle regression;
- durable lease creation before send, heartbeat renewal, expiry with a controllable clock, and stale
  late-result classification;
- worker-journal reopen with accepted/running/finished recovery, conflicting replay rejection, and
  finished-result replay after constructing a new worker process object;
- monotonic cumulative acknowledgement validation in both directions;
- cancellation request/acknowledgement/terminal ordering, cancellation-before-admission races,
  duplicate cancellation, and lease-expiry/cancellation races;
- server outbox persistence and cumulative compaction boundaries;
- worker outbox persistence, reconnect remapping, cumulative compaction boundaries, and orphaned
  delivery pruning without logical-message loss;
- explicit expired-attempt reassignment with a new identity and stale old-result retention;
- worker-local idempotency and changed-content conflict;
- default shell-executor denial and explicit local opt-in;
- protocol validation for sandbox paths and artifact digests.

Artifact coverage includes streaming read/write, canonical digest parsing, digest and size rejection,
interrupted-reader cleanup, concurrent duplicate publication, read-only publication, restart cleanup,
verified readback, refusal to replace a corrupted existing object, idempotent session creation,
offset-conflict rejection, crash-tail truncation, reopen/resume/finalize, expiry pruning, and terminal
versus retryable finalization failures.

CI runs stable fmt/clippy/tests and a separate Rust 1.88.0 locked-dependency test job.

Local smoke setup:

```bash
# terminal 1
cargo run -p alloyport-server

# terminal 2
ALLOYPORT_WORKER_ID=cuda-dev \
ALLOYPORT_BACKEND=cuda \
cargo run -p alloyport-worker
```

This proves connection/heartbeat only. There is no public scheduling API in the binary yet.

## Known gaps: do not claim these are implemented

- Lease-expiry reassignment is explicit and durable, but there is no scheduler policy that chooses a
  replacement worker or proactively invokes it for every expired lease.
- The worker journal and lifecycle outbox are disk backed, but executor process identity and
  artifact/output spooling are not.
- Durable lifecycle replay and seven-day orphaned-delivery retention are implemented. Heartbeats,
  status, output previews, welcomes, and ACK-only frames deliberately remain ephemeral; there is no
  generalized durable message bus or server replication.
- The filesystem content-addressed store and durable resumable-upload session core are implemented.
  There is no Artifact RPC, mTLS-to-owner authorization binding, object-store adapter, reference
  metadata, total quota accounting, or GC.
- No container/process executor, output streaming, resource enforcement, running-process signal
  delivery, or device reset. Cancellation currently terminates admitted no-executor attempts only.
- No CUDA/NPU discovery commands or dynamic health/occupancy scheduler.
- No translation from worker messages into `alloyport-events` command events.
- No external scheduling API, task controller integration, or terminal UI worker view.
- mTLS compiles and is configured in binaries, but lacks certificate fixtures, enrollment, rotation,
  revocation, and an end-to-end TLS integration test.
- No persisted RunReceipt, signature, oracle integration, or audit transition.
- No server replication, shared registry, load balancer/session ownership, quotas, or artifact GC.
- The Python harness in `/data/projects/ascend-factory/harness/worker.py` and `box.py` still uses
  SSH/SCP. No cutover has happened.

## Recommended next implementation order

### 1. Content-addressed Artifact service

The filesystem CAS and durable upload-session state machine are implemented. Next add separate
upload/download streaming RPCs over the existing begin/status/append/finalize operations; do not put
bundles or full logs on the control stream. Bind the session owner seam to authenticated identity,
then add total-store quota, reference reachability, and garbage collection.

### 2. Executor abstraction and fake executor

Define typed executor input/output and a fake executor before Docker integration. It must consume
logical artifact/mount references, never server-supplied host paths. Wire start/output/finish to both
the control protocol and `alloyport-events`. Make output offsets, stdout/stderr separation,
backpressure, full-output artifact production, timeout, cancellation, and terminal receipt mandatory.

Keep shell execution disabled. If later enabled for probes, require an explicit worker policy and a
separate executor kind; do not reduce assignments to a shell string.

### 3. One CUDA vertical slice

Port the existing content-addressed bundle-to-container behavior from the Python harness into the new
worker path. Do not copy the old SSH wrapper. Run a fixed fixture once through the old path and once
through the new path as separate attempts, then compare bundle digest, image/environment identity,
stdout/stderr, exit classification, and receipt fields.

### 4. One Ascend vertical slice

Add device discovery, CANN/driver identity, enumerated device-node policy, occupancy/health reporting,
device leases, driver mount rules, and post-crash health/reset handling. Preserve the epistemic split:
CUDA reference output and Ascend target output are independently executed receipts joined by an
experiment and judged by the oracle.

### 5. Cut over and remove SSH from runtime

Only after evidence parity, make the outbound worker client the sole scheduler path. Remove SSH host,
key, root-directory, remote-shell, and SCP configuration from AlloyPort runtime. Keep any operational
SSH procedure separate and document that manually produced output is not authoritative AlloyPort
evidence.

## Suggested first task for the next Codex session

Add the network and resumable-session layer over the filesystem CAS:

> Read `docs/HANDOFF.md`, Design 0007, Design 0011, and Design 0012. Define a separate versioned
> Artifact gRPC service with begin/status/finalize unary operations plus streaming chunk upload and
> download. Adapt RPC traffic onto the existing SQLite upload sessions and filesystem CAS without
> buffering whole artifacts in memory or trusting client paths. Bind owner identity through an
> injectable authorization resolver, enforce bounded chunks, and add loopback reconnect/download
> tests. Keep all bulk bytes off `OpenControlStream`; follow with total-store quota accounting.
