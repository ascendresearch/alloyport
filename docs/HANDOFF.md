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
- static worker/backend capabilities and dynamic heartbeat state;
- typed assignment and execution specifications;
- accepted/rejected, started, output, and finished lifecycle messages;
- cancel and drain messages;
- content-addressed artifact references and resource limits.

Validation currently enforces supported protocol major version, worker identity/capacity, typed
executor, sandbox-relative working directory, non-empty argv, and `sha256:` artifact digests.

Code generation is reproducible without system `protoc`: build dependencies pin `prost-build 0.14.4`,
`tonic-prost-build 0.14.6`, and `protoc-bin-vendored 3.2.0`. Clippy allowances are limited to the
generated `v1` module; do not weaken workspace lints for hand-written code.

### `alloyport-server`

Implements the gRPC service, worker registry, assignment repository state model, non-terminal replay,
and worker lifecycle classification.

Current behavior:

- requires the first worker frame to be a valid hello with sequence 1;
- registers logical worker ID separately from process instance and connection ID;
- sends welcome followed by queued/non-terminal assignments;
- persists assignments in the server object's in-memory state before sending;
- suppresses an identical attempt submitted twice;
- rejects reuse of an attempt ID with different content or worker identity;
- checks monotonically increasing worker sequence numbers;
- records accepted, rejected, running, and finished states;
- marks a worker disconnected only if the closing stream still owns its connection ID, so an old
  superseded stream cannot disconnect a newer session.

The repository is in-memory only. The word "persists" above means survives a stream reconnect while
the server process remains alive, not survives process restart.

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
acknowledgement, in-process attempt knowledge, local admission, and reconnectable session state.

Current behavior:

- validates its hello before connecting;
- includes locally known attempts in hello/heartbeat messages;
- checks monotonically increasing server sequence numbers;
- validates every assignment locally before acknowledging it;
- returns `already_known` for an identical attempt replay;
- rejects reuse of an attempt ID with changed content;
- denies the shell executor by default; only an explicit local `AdmissionPolicy` can allow it;
- exits a session on drain and leaves cancel as a no-op because no executor exists yet.

The worker state is in memory and is not durable across process restart. Do not enable real candidate
execution until admitted attempt identity and output spooling are disk backed.

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

The following commands passed at handoff:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo +1.88.0 test --workspace --locked
```

There are 15 Rust tests. Control-plane coverage includes a real loopback gRPC stream for:

- hello/welcome and worker registration;
- assignment delivery and worker acceptance;
- duplicate enqueue suppression;
- queuing while disconnected and replay after reconnect;
- worker-local idempotency and changed-content conflict;
- default shell-executor denial and explicit local opt-in;
- protocol validation for sandbox paths and artifact digests.

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

- No crash-durable server database, lease timer, reassignment engine, or startup reconciliation.
- No disk-backed worker journal or artifact/output spool.
- Sequence acknowledgement fields exist, but complete replay-from-cursor and acknowledgement
  compaction are not implemented.
- No content-addressed Artifact service or object-store adapter.
- No container/process executor, output streaming, resource enforcement, cancellation, or device reset.
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

### 1. Durable control repository and leases

Define a storage trait around workers, assignments, attempts, connection observations, and leases.
Implement SQLite first with explicit migrations and transactional "store before send" behavior. Keep
generated Protobuf types out of storage tables; translate into domain/storage records. Add restart,
lease-expiry, stale-result, and cancellation-race tests with a controllable clock.

Add a worker-side append-only journal or small SQLite store before executing anything. On reconnect,
reconcile server assignments with locally accepted/running/finished attempts. Implement cumulative
acknowledgement validation and replay cursors without treating sequence numbers as attempt identity.

### 2. Content-addressed Artifact service

Start with a filesystem CAS behind a trait, using SHA-256, atomic temporary-file-to-digest promotion,
size limits, and digest verification. Add separate upload/download streaming RPCs; do not put bundles
or full logs on the control stream. Add tests for partial uploads, reconnect, digest mismatch,
duplicate content, quota exhaustion, and garbage-collection reachability.

### 3. Executor abstraction and fake executor

Define typed executor input/output and a fake executor before Docker integration. It must consume
logical artifact/mount references, never server-supplied host paths. Wire start/output/finish to both
the control protocol and `alloyport-events`. Make output offsets, stdout/stderr separation,
backpressure, full-output artifact production, timeout, cancellation, and terminal receipt mandatory.

Keep shell execution disabled. If later enabled for probes, require an explicit worker policy and a
separate executor kind; do not reduce assignments to a shell string.

### 4. One CUDA vertical slice

Port the existing content-addressed bundle-to-container behavior from the Python harness into the new
worker path. Do not copy the old SSH wrapper. Run a fixed fixture once through the old path and once
through the new path as separate attempts, then compare bundle digest, image/environment identity,
stdout/stderr, exit classification, and receipt fields.

### 5. One Ascend vertical slice

Add device discovery, CANN/driver identity, enumerated device-node policy, occupancy/health reporting,
device leases, driver mount rules, and post-crash health/reset handling. Preserve the epistemic split:
CUDA reference output and Ascend target output are independently executed receipts joined by an
experiment and judged by the oracle.

### 6. Cut over and remove SSH from runtime

Only after evidence parity, make the outbound worker client the sole scheduler path. Remove SSH host,
key, root-directory, remote-shell, and SCP configuration from AlloyPort runtime. Keep any operational
SSH procedure separate and document that manually produced output is not authoritative AlloyPort
evidence.

## Suggested first task for the next Codex session

Implement step 1 as a tested vertical slice:

> Read `docs/HANDOFF.md`, Designs 0002, 0007, 0010, and 0011. Add a storage-domain crate or a clear
> storage module with a repository trait and SQLite implementation for workers, immutable assignments,
> attempt lifecycle, and leases. Use an injectable clock. Prove store-before-send, server restart
> recovery, lease expiry, duplicate idempotency, and late-result classification. Preserve all existing
> tests and strict Clippy. Do not add real command execution yet.

Before coding, inspect `git status`, run the baseline tests, and update Design 0011's implementation
progress without rewriting its accepted decisions.
