# AlloyPort handoff

- Handoff date: 2026-08-12
- Repository: `/data/projects/shinesheep/alloyport`
- Branch: `main`
- Baseline: the 2026-08-12 closeout includes `8382ab5` (paired correctness worker dispatch);
  run `git log --oneline` for the handoff commit and later local history
- Project state: modular-monolith architecture evolution with working fixed CUDA and fixed Ascend
  runtime composition

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
8. [`design/0013-durable-certificate-enrollment.md`](design/0013-durable-certificate-enrollment.md)
   for stable mTLS owner mapping, rotation, revocation, and worker-control identity binding.
9. [`design/0014-artifact-references-and-garbage-collection.md`](design/0014-artifact-references-and-garbage-collection.md)
   for controller-granted reachability, retention, reader coordination, and conservative GC.
10. [`design/0015-typed-fake-executor-runtime.md`](design/0015-typed-fake-executor-runtime.md) for
    executor inputs/outcomes, output spooling, terminal ordering, and fake restart semantics.
11. [`design/0016-gated-remote-artifact-publication.md`](design/0016-gated-remote-artifact-publication.md)
    for resumable worker publication, terminal gating, controller validation, and typed grants.
12. [`design/0017-canonical-worker-interaction-events.md`](design/0017-canonical-worker-interaction-events.md)
    for durable worker-event translation, replay identity, output offset conflicts, and gap handling.
13. [`design/0018-fixed-cuda-container-contract.md`](design/0018-fixed-cuda-container-contract.md)
    for the fixed CUDA fixture, local allowlisting, bundle materialization, derived Docker plan, and
    engine-neutral durable supervisor.
14. [`design/0019-authorized-interaction-replay-and-subscription.md`](design/0019-authorized-interaction-replay-and-subscription.md)
    for public canonical replay, stable run ownership, mTLS authorization, bounded subscription,
    revocation, and controller redaction.
15. [`design/0020-worker-supervisor-placement-and-attempt-isolation.md`](design/0020-worker-supervisor-placement-and-attempt-isolation.md)
    for the decision to keep the trusted worker supervisor outside per-attempt candidate sandboxes,
    including acceptable containerized-supervisor deployments and reconsideration criteria.
16. [`design/0021-fixed-ascend-worker-contract.md`](design/0021-fixed-ascend-worker-contract.md)
    for fixed Ascend identity, device-node policy, dynamic health/occupancy, and worker-durable device
    leases.
17. [`design/0022-standalone-worker-configuration-and-device-selection.md`](design/0022-standalone-worker-configuration-and-device-selection.md)
    for the unified worker file, registry-optional image identity, and shared GPU/NPU selection and
    device-guard rules.
18. [`design/0023-versioned-server-configuration.md`](design/0023-versioned-server-configuration.md)
    for strict server bootstrap, configuration precedence, local defaults, and shared identity
    administration.
19. [`design/0024-migration-spec-v1-and-first-product-slice.md`](design/0024-migration-spec-v1-and-first-product-slice.md)
    for the accepted migration domain, lifecycle, generated-source release Gate, and first specimen.
20. [`design/0025-pluggable-llm-provider-architecture.md`](design/0025-pluggable-llm-provider-architecture.md)
    for the complete proposed five-loop runtime, protocol-native provider boundary, durable recovery,
    tool authority, candidate search, and implementation sequence. The user accepted it on
    2026-08-12; it is now implementation authority.
21. [`research/agent-runtime-and-provider-study.md`](research/agent-runtime-and-provider-study.md)
    for the completed evidence ledger covering `ascend-factory`, EvoKernel, three provider
    protocols, durable runtimes, and the design choices carried into 0025.
22. [`design/0027-reduction-differential-oracle-and-calibration.md`](design/0027-reduction-differential-oracle-and-calibration.md)
    for reduction run/experiment identities, mutation calibration, verdict semantics, the paired
    execution Port, and the Build-to-Correctness Agent tool.
23. [`design/0028-controller-authored-correctness-execution-bundles.md`](design/0028-controller-authored-correctness-execution-bundles.md)
    for exact paired-run bundle inputs, frozen-corpus coverage, implementation binding, and the
    stable callable candidate ABI required before production worker execution.
24. [`design/0029-paired-correctness-worker-dispatch.md`](design/0029-paired-correctness-worker-dispatch.md)
    for protocol-minor-6 execution kinds, dual assignment/reconciliation, and the worker-to-run
    Artifact trust chain.
25. [`design/0030-trusted-reduction-correctness-worker-harness.md`](design/0030-trusted-reduction-correctness-worker-harness.md)
    for role-bound bundle materialization, fixed CUDA/Ascend container plans, trusted structured
    output, and correctness runtime registration.
26. [`design/0031-standalone-correctness-worker-configuration.md`](design/0031-standalone-correctness-worker-configuration.md)
    for strict correctness-only worker files and production process composition.
27. [`design/0032-pinned-correctness-images-and-cuda-diagnostic.md`](design/0032-pinned-correctness-images-and-cuda-diagnostic.md)
    for RepoDigest-pinned runner images and the real CUDA diagnostic/calibration boundary.
28. [`design/0033-durable-agent-episode-repository.md`](design/0033-durable-agent-episode-repository.md)
    for restart-safe Agent Episode snapshots and the server-owned `SQLite` CAS adapter.
29. [`design/0034-durable-provider-turn-context.md`](design/0034-durable-provider-turn-context.md)
    for recoverable native continuation, exact exchange Artifacts, and tool-result input binding.
30. [`design/0035-controller-episode-application.md`](design/0035-controller-episode-application.md)
    for catalog-derived runtime identities, exact recovery, and controller runner ownership.
31. [`design/0036-gated-candidate-episode-composition.md`](design/0036-gated-candidate-episode-composition.md)
    for the fixed Candidate/Source/Build/Correctness tool and worker-control assembly.
32. [`design/0037-explicit-candidate-episode-bootstrap.md`](design/0037-explicit-candidate-episode-bootstrap.md)
    for strict operator preflight, per-run provider authorization, worker readiness, and supervised
    Candidate execution.
33. [`design/0038-standalone-ascend-build-worker.md`](design/0038-standalone-ascend-build-worker.md)
    for the strict Build-only worker file and production NPU/runtime composition.

For structural work, also read
[`ARCHITECTURE_EVOLUTION_PLAN.md`](ARCHITECTURE_EVOLUTION_PLAN.md). It records the active,
incremental composition-root, port, API, configuration, and lifecycle plan. It does not reopen the
completed broad remediation baseline.

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

Defines an object-safe immutable `ArtifactStore`, narrow administrative `ArtifactRetentionStore`,
canonical `Sha256Digest`, streaming `Read`-based ingestion, and verified readers. The concrete
filesystem CAS is isolated in `adapters::filesystem` and re-exported for compatibility. Its
implementation:

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
- begin atomically reserves declared bytes against global and per-owner stored-plus-reserved quotas;
  finalization converts that reservation into digest-deduplicated global usage and owner/digest usage;
- idempotent begin and duplicate completed digests do not double-count, concurrent begin cannot
  overcommit, retryable finalization retains its reservation, and terminal failure or expiry releases
  capacity;
- opening a pre-quota database migrates and backfills active reservations plus completed artifact
  usage.

This is the storage and resumable-session core used by the Artifact RPC adapter. Completed upload
records create typed durable upload references. Controller operations can add idempotent assignment
input/output, receipt, retention-root, and other owner/digest references with purpose and optional
minimum-retention metadata; revoked reference keys remain terminal.

Active references grant their stable owner read access and count a digest once toward owner quota.
Revoking the final active owner/digest reference removes access and releases owner quota, while
optional retention can continue protecting physical bytes. Explicit bounded GC collects only when
there is no active reference, unexpired retention, live upload session, or active in-process reader.
A durable pending marker recovers deletion interrupted between the filesystem and SQLite; global
quota is released only after object collection. Collection is not yet scheduled automatically.

The immutable object and administrative-removal Ports now have one reusable behavioral contract
suite. It runs unchanged against the crash-recoverable `FilesystemArtifactStore` and the explicitly
non-durable `InMemoryArtifactStore`; worker tests use that shared conforming memory adapter instead
of maintaining a private fake. Adapter-specific filesystem crash, tamper, and concurrency tests
remain separate. [`PORT_CONTRACTS.md`](PORT_CONTRACTS.md) records the remaining conformance order.

### `alloyport-core`

Dependency-light domain primitives for tasks, candidates, gates, verdicts, and release manifests.
Release construction requires every gate and independent evidence.

### `alloyport-events`

Typed producer events, canonical event envelopes, per-run sequencing, lifecycle reduction, JSONL,
and plain rendering. Schema/sequencing, stateful lifecycle reduction, and presentation are separate
modules with their original root-level API re-exported. This is the first Codex-like interaction
vertical slice. The server now translates assigned worker command lifecycle, output previews, and
terminal Artifact observations into a durable canonical stream.

### `alloyport-cli`

Provides `about`, lifecycle output, an event demo, and Python-producer JSONL ingestion/rendering. It
does not yet call the worker scheduler.

### `alloyport-proto`

Defines `alloyport.worker.v1`, `alloyport.artifact.v1`, and `alloyport.interaction.v1`, and generates
Rust client/server bindings with `tonic` and `prost`.
Important files:

- `crates/alloyport-proto/proto/alloyport/worker/v1/worker_control.proto`
- `crates/alloyport-proto/proto/alloyport/artifact/v1/artifact_service.proto`
- `crates/alloyport-proto/proto/alloyport/interaction/v1/interaction_service.proto`
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
- protocol minor 6 execution vocabulary, including fixed-Ascend executor identity, explicitly
  enumerated static device identities, paired reduction correctness executors, dynamic
  health/occupancy observations, and worker-durable device-lease reporting.

Validation currently enforces supported protocol major version, worker identity/capacity, typed
executor, sandbox-relative working directory, non-empty argv, and `sha256:` artifact digests.

Code generation is reproducible without system `protoc`: build dependencies pin `prost-build 0.14.4`,
`tonic-prost-build 0.14.6`, and `protoc-bin-vendored 3.2.0`. Clippy allowances are limited to the
generated protocol modules; do not weaken workspace lints for hand-written code.

### `alloyport-server`

Implements the gRPC service, in-process connection registry, crash-durable SQLite control repository,
non-terminal replay, attempt leases, and worker lifecycle classification. Generated Protobuf types
are translated at the RPC edge into separate storage-domain records. Persistence capabilities are
segregated into worker-connection, assignment-read/write, attempt/lease, and server-outbox ports;
the complete service consumes their `ControlRepository` composition. SQLite connection and outbox
operations live in dedicated implementation modules, as do assignment and attempt/lease operations;
the shared repository shell now owns only connection creation, migrations, and locking.
The service internally stores five narrow capability objects rather than one broad repository;
assignment reads and writes are distinct application ports. `with_repository_capabilities` supports
independently composed connection, assignment-read, assignment-write, attempt, and outbox
implementations. Existing composite constructors and the four-port constructor remain compatible.
The public `storage::*` contract is a compatibility facade over separate clock policy,
transport-independent model, and capability-port/error modules.
Server and worker durable contracts share `alloyport_core::ExecutionKind`, `NetworkPolicy`,
`AttemptOutcome`, and `RejectionReason`; persisted JSON stays numeric for compatibility, while
Protobuf conversion is confined to transport mappings and observation ingestion.
Immutable server/worker assignment contracts also share validated `AttemptId`, `AssignmentId`,
`TaskId`, and `CandidateId` types whose JSON and SQLite representations remain the existing strings;
assignment validation now requires candidate identity. Core task/candidate/release models use the
same task/candidate types. Trusted worker outbox lifecycle payloads retain typed assignment/attempt
identities through SQLite; rejected assignments retain raw identity text because malformed IDs must
remain reportable. Server observation ingress converts those wire identities before its repository
and SQLite adapter.
Canonical `alloyport_core::Sha256Digest` and `ArtifactDescriptor` are shared by protocol validation,
core release evidence, server control storage, and the worker journal. The artifacts crate re-exports
the digest for compatibility; its two-field CAS `ArtifactIdentity` remains distinct from the
three-field transport-independent descriptor because stored-object verification and declared media
metadata are different concepts.
The complete immutable assignment vocabulary now lives in `alloyport-core`: assignment, execution,
environment, resource limits, identities, enums, and Artifact descriptors. Existing server storage
and worker journal type names are compatibility aliases, so public paths and persisted JSON remain
stable without maintaining parallel structures.
Canonical Interaction persistence is capability-segregated into event-write, event-read, and
run-access ports. Its SQLite schema shell, event/output log, and authorization grants are separate
implementation modules, alongside independent replay-to-live broadcast and display sanitization.
The public Interaction gRPC delivery service is separate from its enrolled-certificate and durable
run-grant authorization policy; live streams revalidate that policy throughout delivery.
Worker registration, replay reconstruction, inbound stream consumption, identity revalidation, and
disconnect handling live in a dedicated session-lifecycle module outside the service facade.
Assignment admission/reassignment/cancellation, durable frame preparation with connection sequence
and lease allocation, and abandoned-preparation reconciliation are three separate use-case modules.
Inbound frame sequence/ACK persistence, durable attempt-state observation, and canonical
Interaction projection are likewise separate modules, so transport replay mechanics do not own
user-visible event construction.

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
- appends a durable canonical `run.started` when a task is first assigned and explicitly translates
  worker start, output, terminal Artifact, and command-completion observations before acknowledging
  their durable frames;
- assigns stable per-task event identity and sequence in SQLite, deduplicates lifecycle replay across
  changed transport sessions and process instances, and preserves the original canonical envelope;
- checks raw output replay by attempt, stream, and byte offset, rejects changed or overlapping bytes,
  and emits a visible warning for accepted forward gaps while treating final Artifacts as complete
  output authority;
- pages durable events strictly after a canonical sequence and provides a bounded per-run
  replay-to-live subscription foundation that attaches before capturing the SQLite high-water mark;
- terminates lagged subscribers with an explicit resumable cursor, rejects future cursors and
  sequence gaps, and isolates one run's notification pressure from another without blocking append;
- persists idempotent multi-owner run grants with terminal revocation and exposes a trusted
  owner-aware enqueue path that never accepts ownership from worker or public request bodies;
- registers mTLS-authorized bounded replay and replay-to-live RPCs that stream exact canonical
  envelope JSON, revalidate certificate and grant state, and fail slow consumers with a resumable
  cursor;
- applies controller terminal-control and common-credential redaction to worker display text before
  canonical persistence while retaining raw terminal Artifacts as complete output authority;
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

The server library also implements a separate `ArtifactServiceImpl` adapter:

- begin, status, and finalize are unary RPCs over the durable SQLite upload-session core;
- upload is client-streaming and accepts reconnects as independent streams at the exact durable
  offset; one stream cannot mix upload session IDs;
- download is server-streaming, supports a byte offset and optional byte limit, and moves data with
  a bounded 64 KiB buffer;
- blocking SQLite and bounded filesystem metadata work runs outside Tokio async workers behind the
  server-wide eight-permit persistence executor;
- an injectable `ArtifactAccessPolicy` receives both RPC metadata and transport extensions, returns
  the shared authenticated request context, and authorizes digest reads, so no request-body owner or
  client filesystem path is trusted; the extensions expose tonic's verified TLS peer-certificate
  information;
- client-streaming upload revalidates the retained credential before committing every chunk, so
  rotation or revocation cannot authorize bytes received later on an already-open stream.

The access-policy contracts are asynchronous, so identity-registry and reference checks cannot
accidentally execute synchronously on an RPC task. The server binary registers this service
alongside worker control. `EnrolledArtifactAccessPolicy`
requires tonic's verified TLS connection information, maps the client leaf-certificate fingerprint
through the durable identity registry, ignores client-supplied owner metadata, and permits a download
only when that stable owner has an active typed reference for the digest. Completed uploads create
the initial reference and controller grants can add others. Rotation preserves those references;
replacement and revocation fail closed. Consequently, Artifact RPCs return
`Unauthenticated` on the permitted plaintext loopback development server.

Remote `WorkerControlService` uses the same registry. The verified certificate must resolve to the
`WorkerHello.worker_id`, and every later frame revalidates the original fingerprint so a rotated or
revoked connection is terminated on its next heartbeat or lifecycle message. Plaintext loopback
worker control remains an explicit development bypass.

Control, Artifact, and Interaction now retain the same `AuthenticatedRequestContext`: a stable
logical owner plus an optional verified connection identity. Explicit local contexts support tests
and in-process adapters; enrolled remote policies require the verified identity. Authentication is
shared, while Artifact references and Interaction run grants remain service-owned authorization
decisions. Control revalidates per inbound frame, Artifact per committed upload chunk, and
Interaction throughout replay/live delivery.

The internal gRPC transports no longer inherit implicit framework message defaults. Shared
`alloyport-proto` constants bound Control request/response frames, Interaction requests/events, and
Artifact download responses on both production endpoints. Backend-neutral worker coordination
splits output previews into at most 64 KiB payloads with continuous offsets, and the server rejects
an oversized preview before canonical persistence. Artifact upload envelopes remain derived from
the configured chunk size so operators can tune transfer throughput without widening Control.

At the application boundary, mutable upload staging/session/finalization depends on
`ArtifactUploadRepository`, published-object references and visibility depend on
`ArtifactMetadataStore`, and immutable content depends on `ArtifactStore`. The SQLite metadata and
filesystem CAS types are selected only in the server binary composition root.

The worker follows the same CAS boundary: execution runtimes, CUDA bundle materialization, remote
downloads, and resumable publication consume `ArtifactStore`; `FilesystemArtifactStore` is chosen
only in the worker binary composition root.

`WorkerControlService::new()` remains an in-memory SQLite convenience for tests. The server binary
uses `ALLOYPORT_DATABASE` or defaults to `alloyport-control.sqlite3`, so normal server state survives
process restart.

The `alloyport-server` binary listens on `127.0.0.1:50051` by default. It accepts a strict schema-1
JSON file through `--config PATH` or `ALLOYPORT_SERVER_CONFIG`; explicit CLI location wins, while
individual environment values override file values and defaults. JSON-relative paths resolve from
the file directory. Plaintext is rejected on a non-loopback bind address. Remote mode requires:

- `ALLOYPORT_LISTEN`
- `ALLOYPORT_TLS_CERT`
- `ALLOYPORT_TLS_KEY`
- `ALLOYPORT_TLS_CLIENT_CA`
- `ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS` (default 10) for cooperative listener/background-task drain

Artifact storage configuration is:

- `ALLOYPORT_ARTIFACT_ROOT` (default `alloyport-artifacts`), containing `cas/`, `upload-data/`, and
  `uploads.sqlite3`;
- `ALLOYPORT_ARTIFACT_MAX_BYTES` (default 8 GiB) for both expected uploads and CAS objects;
- `ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES` (default 1 MiB) for each streamed upload message;
- `ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES` (default 64 GiB) for managed stored objects plus active
  reservations;
- `ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES` (default 16 GiB) for each stable owner;
- `ALLOYPORT_IDENTITY_DATABASE` (default `<ALLOYPORT_ARTIFACT_ROOT>/identities.sqlite3`) for durable
  certificate enrollment.

The TLS configuration requests a client certificate. Before remote use, an operator manages its
stable owner mapping offline:

```bash
cargo run -p alloyport-server -- --config server.json identity enroll WORKER_ID client.pem
cargo run -p alloyport-server -- --config server.json identity rotate WORKER_ID old.pem new.pem
cargo run -p alloyport-server -- --config server.json identity revoke client.pem
```

These commands load the same configuration and identity database as serving. See
[server configuration](server-configuration.md) and Design 0023.

This is application-level authorization above CA verification; certificate issuance, online
enrollment, CA revocation, and replicated identity storage are not implemented.

### `alloyport-worker`

Implements an outbound client session, hello/welcome negotiation, heartbeat, cumulative server
acknowledgement, disk-backed attempt knowledge, local admission, and reconnectable session state.
The worker journal exposes separate attempt-lifecycle and durable-outbox ports; `AttemptStore`
composes them for the complete outbound worker while narrower consumers can depend on one capability.
The SQLite adapter mirrors that boundary with separate lifecycle and outbox modules behind a small
schema/connection composition shell; lifecycle transitions enqueue authoritative messages in the
same transaction.
Assignment admission/cancellation, execution task/update coordination, and durable/ephemeral frame
delivery plus terminal Artifact publication are separate application modules.
The public `ExecutionBackend` execution and cleanup futures return `BackendError`, whose retryable,
terminal, policy, and integrity variants survive the completion-update path into `WorkerError`.
Artifact input/publication, journal/runtime, CUDA contract, and container-engine adapters classify
their own failures; callers do not parse backend error text or receive `ExecutionRuntimeError`.

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
  while no executor is attached.

The worker library also implements the Design 0015 deterministic fake executor runtime:

- keeps Artifact publication contracts/local spooling and canonical event projection in shared
  modules used by both fake and CUDA runtimes, outside the fake execution coordinator;
- translates only validated, journal-stored assignments into typed executor inputs with logical
  paths, argv, environment, timeout, and output limits;
- produces independently offset stdout/stderr chunks through a bounded channel and classifies
  success, nonzero exit, timeout, cancellation, output exhaustion, and infrastructure failure;
- uses logical fake elapsed time so scheduler jitter and preview backpressure do not change receipts;
- commits `Running` before execution, spools complete stdout, stderr, and a JSON receipt to a local
  filesystem CAS, then atomically commits terminal journal data plus the durable finished outbox;
- emits observed `alloyport-events` command/artifact producer frames and typed Design 0014 output and
  receipt reference intents without granting server authorization;
- returns an existing finished record without rerunning or duplicating events, safely reruns only the
  side-effect-free fake plan from a restored `Running` state, and prevents two in-process executors
  from claiming one attempt;
- can be explicitly attached to `OutboundWorker`, which launches it only after durable admission and
  accepted-message delivery, keeps one active-attempt task registry across stream reconnects,
  multiplexes raw-byte stdout/stderr previews with correct independent offsets, and routes a durable
  cancellation acknowledgement to the registered token before terminal completion;
- treats live observations as bounded and ephemeral while retaining started/finished authority in
  the journal outbox, so disconnecting a stream neither cancels execution nor loses its terminal
  replay;
- optionally attaches a Design 0016 remote publisher that resumes each stdout/stderr/receipt upload
  from the server's committed offset, validates the completed remote identity, and prevents the
  terminal journal/outbox commit until all three objects are finalized.

Design 0018 adds the first real-CUDA contract and reconciliation boundary without enabling Docker
execution in the worker binary yet:

- protocol minor 3 defines a dedicated `CudaFixture` executor kind that remains default-deny;
- local policy binds one fixture, bundle digest, OCI manifest digest, expected local image ID,
  enumerated device, sandbox root, and resource ceilings;
- verified JSON bundle materialization writes only fixed worker-owned filenames, survives restart
  idempotently, and rejects changed bytes or an inner source-digest mismatch;
- Docker create argv is derived without a shell, arbitrary environment, server-selected mount/device,
  or network, and splits the declared disk budget across bounded `/work` and `/tmp` tmpfs.
- CUDA enqueue requires a published size-matched bundle, grants the assigned worker a typed
  `AssignmentInput` reference before delivery, and the worker can download it with bounded contiguous
  offsets plus final digest/size verification into its local CAS.
- an engine-neutral supervisor verifies the resolved image ID, reconciles stable container identity
  across missing/created/running/exited states, concurrently follows bounded logs, stops on
  cancellation, timeout, or combined output-budget exhaustion, and rejects identity conflicts or a
  zero exit without the fixed `PASS` marker.
- an argv-only Docker CLI engine performs bounded image/container inspection, distinguishes a
  genuinely absent container from an inspect failure, cross-checks wait/inspect exit state, recovers
  elapsed time from daemon timestamps, follows stdout/stderr under one combined in-memory budget,
  terminates its log follower when that budget is exhausted, and exposes explicit terminal removal.
- a CUDA runtime spools stdout/stderr plus a bundle/source/image/device/environment receipt, gates
  the terminal commit on optional remote publication, removes only after that commit, and retries a
  failed cleanup from terminal replay without rerunning the fixture.
- `OutboundWorker` can explicitly attach that runtime, enforce CUDA-fixture-only admission, match
  hello and receipt environment facts, share cancellation/task ownership, and retry terminal cleanup
  at session startup without blocking terminal outbox delivery.
- the worker binary attaches the complete CUDA stack from one schema-1 worker configuration carrying
  server/TLS, identity, journal, environment, local image and device-selection policy; it then
  downloads the granted bundle into its verified CAS before execution and requires remote
  stdout/stderr/receipt publication before terminal commit.

The self-contained fixture covers CUDA compilation, allocation/copy, a real kernel launch,
synchronization, and deterministic device-result verification. On 2026-08-11 the ignored real-engine
outbound loopback passed on the development GB10 through assignment grant, empty-worker-CAS bundle
download, real Docker supervision, terminal Artifact upload, controller commit, and container
removal. It produced exactly `PASS` for 1,048,576 elements and checksum `670562424`, with bundle
`sha256:04b086...610e`, source `sha256:631447...9a95`, manifest `sha256:54b468...584a`, resolved image
`sha256:79a186...d884`, empty stderr, exit 0, and a 1,334 ms receipt. A separate direct SSH/Docker
legacy reference attempt produced the same stdout and exit classification. The locally derived
Docker plan fixes `--entrypoint python3`; without it, the image-authored NVIDIA entrypoint polluted
stdout with a license banner even though the kernel passed. A subsequent real-engine rerun with live
preview forwarding passed in 1,335 ms with identical terminal digests and canonical stdout bytes.

When Artifact metadata is attached to `WorkerControlService`, the controller accepts terminal
stdout/stderr/receipt only when the reporting stable worker finalized exact owner-scoped upload keys
with matching digest, size, and media type. It then creates idempotent `AssignmentOutput` and
`Receipt` roots before recording `Finished`. A wire digest cannot manufacture remote evidence.

Design 0021 defines the fixed Ascend contract. The fixed runtime implementation now provides:

- `ExecutionKind::AscendFixture` and wire `EXECUTOR_KIND_ASCEND_FIXTURE` are distinct from generic
  container/shell and from the CUDA fixture;
- static worker capabilities can enumerate stable device ID, product, serial, and firmware identity;
- bounded heartbeats can separately report device health, process occupancy, utilization, memory,
  temperature, power, and crash-durable attempt/device leases;
- the fixed `ascend-add-v1` local policy binds exact bundle/image, CANN/driver/firmware, one selected
  device, enumerated `davinci*`/manager/HDC nodes, the read-only driver mount, sandbox, and ceilings;
- its derived Docker plan contains no shell or server-selected host path/device/environment and sets
  `ASCEND_RT_VISIBLE_DEVICES` only from worker-local policy;
- the worker journal has a segregated `DeviceLeaseStore`: acquisition is exclusive and idempotent,
  survives SQLite reopen, and release stays explicit so terminal failure cannot make a device
  reusable before health/reset handling;
- immutable preflight observations are stored separately before `Running`; recovery of a `Running`
  attempt reads those original facts instead of performing a new probe that could observe its own or
  a completed candidate process;
- the controller grants published input bundles to both fixed CUDA and fixed Ascend assignments;
- a bounded shell-free local `npu-smi` adapter discovers static IDs/product/serial/firmware identity
  and parses dynamic health, process count, utilization, HBM, temperature, and power. It uses an
  absolute binary, bounded time/output, a captured fixed-driver regression fixture, and fails closed
  on incomplete or mismatched inventory;
- a distinct `AscendContainerEngine` port and supervisor reconcile stable container identity across
  missing, `Created`, `Running`, and `Exited` states and enforce image identity, cancellation,
  timeout, output bounds, and the fixed verification marker;
- backend-neutral `DeviceGuard` is used by CUDA and Ascend. It acquires the durable lease before
  preflight, persists immutable evidence before `Running`, never resets a device with an unattributed
  visible process, retains uncertain/unhealthy state as quarantine, and releases after terminal
  execution only after container removal plus `Ready` and zero processes. Current adapters authorize
  no reset, so unhealthy cleanup remains safely quarantined.
- `AscendFixtureBundle` is size/digest/schema checked and materialized write-once with a trusted local
  dispatcher; the assignment cannot provide host paths, mounts, environment, Docker options, or a
  shell command;
- the existing argv-only Docker CLI implements the separate Ascend engine port without changing the
  derived create argv;
- `AscendExecutionRuntime` and `AscendExecutionBackend` compose lease/preflight, supervisor, local
  CAS, mandatory terminal publication, receipt creation, and post-commit cleanup. The independent
  receipt records bundle/source/image IDs, environment, exact device/lease, pre/post observations,
  output digests, outcome, and cleanup intent;
- publication failure leaves the attempt `Running`; replay reconciles the exited container without
  recreating it. Terminal cleanup failure leaves terminal state and the device lease durable; replay
  retries only cleanup;
- the unified worker configuration attaches this stack default-deny in the production binary after
  exact capability, `npu-smi` identity, and host character-device inventory checks. The checked-in
  example deliberately uses unusable digests and placeholder identities.
- `fixtures/ascend-add-v1/image` now contains the trusted CANN-image harness and fixed host/tiling
  closure; `make_bundle.py` produces canonical bundle JSON from the candidate kernel. On 2026-08-11
  it built from CANN base RepoDigest `sha256:a7770a...9f7d` and passed a direct real 950PR gate on an
  independently rechecked unoccupied card: ASC compiled/linked for `dav-3510`, exit 0, and stdout was
  exactly `PASS fixture=ascend-add-v1 elements=16384 checksum=3d2cf971e11e0383`. This was a direct
  diagnostic container, not an outbound AlloyPort receipt.
- least-capability probing found `cap-drop=ALL` alone makes `aclInit` fail because the selected
  `davinciN` node is driver-group-owned `0660`; adding back only `DAC_OVERRIDE` succeeds. The final
  plan retains disabled networking, read-only root/driver/source, `no-new-privileges`, and a bounded
  tmpfs for build, HOME, temporaries, and CANN logs.

A read-only 2026-08-11 environment probe found seven Ascend 950PR devices (`davinci0..6`), host
driver `25.7.rc1.6`, pinned-image CANN `9.1.0-beta.1`, and the required `davinci_manager`/`hisi_hdc`
nodes. Concurrent processes, 99% utilization, and `Alarm` health on different cards confirmed that
device count, occupancy, utilization, and health cannot be collapsed. Login material remains only in
the separate legacy project/operator environment and was not copied into AlloyPort.

The worker binary does not attach the fake runtime. CUDA and Ascend remain mutually exclusive and
default-deny. Fixture and correctness modes are also explicit, mutually exclusive backend variants.
`alloyport-worker --config PATH` loads one complete schema-1 JSON file containing the server/TLS
connection, worker ID, journal path, backend facts, and local policy;
`ALLOYPORT_WORKER_CONFIG` is only an equivalent file locator.
[The CUDA fixture template](cuda-worker-config.example.json),
[the Ascend fixture template](ascend-worker-config.example.json),
[the CUDA correctness template](cuda-correctness-worker-config.example.json), and
[the Ascend correctness template](ascend-correctness-worker-config.example.json)
contain deliberately unusable all-zero digests that operators must replace with the exact granted
image identities; only fixture variants also pin one bundle identity. A standalone tag is accepted
only when `image_digest == image_id` and
the assignment declares the OCI image-config media type; a registry-backed manifest-pinned reference
remains optional. The templates also declare local device selection, absolute non-overlapping
sandbox/CAS roots, resource ceilings, bounded Artifact
download/upload settings, absolute Docker CLI, and stop grace period. Ascend additionally requires
the exact startup-enumerated host `davinci*`/manager/HDC character-device set and an absolute
`npu-smi` binary; CUDA requires an absolute `nvidia-smi`. Enabling either path produces matching
hello facts with `device_count=1`,
`max_concurrency=1`, and `container_runtime=docker`.

The `alloyport-worker` binary reconnects with capped exponential backoff. Remote TLS certificate,
private-key, server-CA, and server-name paths are fields of the same file. Remote plaintext endpoints
are rejected; loopback HTTP is permitted for tests and development. See
[worker configuration](worker-configuration.md) and Design 0022.

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

The architecture-remediation round closed on 2026-08-11. Its layering, persistence, plugin, typed
error, and module-size constraints are recorded in `docs/ARCHITECTURE_REMEDIATION.md` and enforced by
the two boundary scripts. The fixed Ascend runtime extends those ports through the existing backend
registry with verified bundle materialization, Docker composition, durable device evidence,
Artifact-gated receipt publication, and production-binary configuration. The trusted pinned-image
harness and explicit real-environment acceptance have passed. Remaining Ascend work is a separately
recorded legacy-SSH parity attempt plus production-binary reconnect/configuration exercise, not a
new control-state machine or another execution of the already accepted fixture. Both remaining
exercises completed on 2026-08-12 without rerunning the accepted outbound fixture.

The following commands passed at the closing verification:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/check_architecture_boundaries.sh
bash scripts/check_sql_boundaries.sh
cargo test --workspace --quiet -- --test-threads=1
```

There are 287 passing Rust tests and two ignored by default because they explicitly require Docker
and a CUDA or Ascend device. Control-plane coverage includes real loopback gRPC streams and SQLite
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

The closing API-boundary slices also prove centralized domain-error-to-gRPC status mapping, one
authenticated request context across Control/Artifact/Interaction, credential revalidation before
each committed Artifact upload chunk, shared client/server message-size envelopes, server rejection
of oversized preview frames, gap-free 64 KiB backend-neutral preview splitting, and preservation of
valid UTF-8 character boundaries. Artifact upload framing remains derived from its configured chunk
limit; large authoritative output remains in the CAS rather than widening Control or Interaction.

The loopback control-plane suite also attaches the fake executor, disconnects and reconnects while
the task is still running, verifies terminal replay without a second executor/receipt, and cancels a
running fake task through the server control API.
Another combined control/Artifact loopback test begins with a partially committed stdout upload,
resumes it from the durable server offset, finalizes empty stderr and the receipt, creates typed
controller references, and only then accepts the terminal lifecycle frame. It also reduces the
persisted canonical run/start/output/Artifact/completion stream. Restart coverage verifies that a
replayed accepted assignment neither duplicates nor renumbers its canonical run event.

Interaction repository coverage includes transactional per-run sequencing, semantic replay across
changed timestamps and producer instances, conflicting deduplication keys, exact raw output replay,
changed and overlapping output rejection, visible forward gaps, SQLite close/reopen recovery,
bounded cursor paging, replay-to-live handoff, explicit slow-consumer termination and resume,
future-cursor rejection, per-run notification-pressure isolation, durable owner grants and terminal
revocation, authorized public paging, public delivery timeout, and controller display redaction.

Fake executor coverage includes independent output offsets, bounded preview backpressure, timeout,
cancellation, output exhaustion, deterministic elapsed time, stdout/stderr/receipt CAS spooling,
event sequencing, typed reference intents, terminal idempotency, restored-`Running` recovery, and
single-executor ownership. Publication-gate coverage verifies that a failed publisher leaves the
attempt `Running` with no finished outbox row and that an idempotent retry can commit once.

Artifact coverage includes streaming read/write, canonical digest parsing, digest and size rejection,
interrupted-reader cleanup, concurrent duplicate publication, read-only publication, restart cleanup,
verified readback, refusal to replace a corrupted existing object, idempotent session creation,
offset-conflict rejection, crash-tail truncation, reopen/resume/finalize, expiry pruning, and terminal
versus retryable finalization failures, including direct finalization of a zero-byte object. A real
loopback Artifact gRPC test begins a session, resumes
it through two independent client streams, finalizes it, reads completed status, and downloads a
bounded range from a nonzero offset.
The Artifact loopback also downloads a complete assignment input into a worker-local verified CAS
and proves a repeated fetch reuses the local object. CUDA enqueue coverage rejects missing Artifact
metadata and grants only an already published bundle with the exact declared size.
CUDA supervisor coverage uses a fake engine to prove create/start, running reattach, exited replay,
cancellation and timeout stop/wait, image and durable-identity mismatch rejection, output exhaustion,
active stop on running-output exhaustion, nonzero exit, and mandatory fixture verification-marker
classification.
Docker adapter coverage proves exact inspect identity and timestamp parsing, ambiguous-state
rejection, bounded pipe draining, combined stdout/stderr following budgets, exact `logs --follow`
argv dispatch, missing-container discrimination, log-exhaustion propagation, and idempotent removal
without requiring Docker in CI. CUDA runtime
coverage proves publication-before-terminal and terminal-before-removal ordering, durable receipt
facts, cleanup-failure retention, and cleanup-only terminal replay. A dedicated CUDA control-plane
test covers CUDA-only outbound admission, Artifact-gated completion, a post-commit cleanup failure,
session reconnect, durable terminal replay, cleanup without a second execution, and download of the
granted input bundle into an initially empty worker CAS. The ignored real-engine variant exercises
the same outbound path against Docker/GB10 and verifies exact live canonical stdout plus terminal
receipt identities; normal CI never silently depends on it. CUDA log-reader/runtime coverage proves
stdout/stderr preview offsets, a shared combined byte budget, nonblocking drop under preview-channel
pressure, terminal CAS independence, and no duplicate terminal preview emission.

An end-to-end mutual-TLS test creates one CA, a server identity, and three client identities. It
proves forged worker hello rejection, cross-owner Artifact and interaction isolation, termination of
old worker and interaction streams after rotation, preservation of existing Artifact/run access
through the replacement certificate, explicit controller grant/revoke, and denial after revocation.
Identity unit and command integration tests cover durable reopen, idempotency, conflicts, replacement
state, and offline enrollment administration.
Quota coverage includes reservation recovery after restart, idempotent and concurrent begin,
per-owner isolation, terminal-failure and expiry release, duplicate digest accounting, pre-quota
schema migration/backfill, and gRPC `ResourceExhausted` mapping.
Reference/GC coverage includes concurrent idempotent grants, typed conflicts, multiple references,
revocation, retention, active-reader protection, quota release, pending-delete restart recovery, and
metadata non-resurrection after reopen.

CI runs stable fmt/clippy/tests and a separate Rust 1.88.0 locked-dependency test job.

Configured worker startup:

```bash
# terminal 1
cargo run -p alloyport-server

# terminal 2; copy a checked-in template and replace every placeholder first
cargo run -p alloyport-worker -- --config /absolute/path/to/worker.json
```

Startup validates connection policy, backend probe, image policy, and device eligibility before the
control session. There is no public scheduling API in the binary yet.

On 2026-08-12 the production `alloyport-worker` binary was also exercised from a populated,
uncommitted schema-1 Ascend configuration against a loopback production server without submitting
an assignment. A fresh read-only inventory found NPU 5 (`Ascend950PR`, serial `10265D495399`) as the
only `OK`, process-free device, and the worker registered only that device. Accepted heartbeats
advanced the first connection through worker sequence 7. After two deliberate controller restarts,
the same worker process and instance reconnected with fresh connection-local sequences; the final
connection advanced through sequence 5. Observed retry delays were exactly 1, 2, 4, 8, 16, 30, and
30 seconds, proving the production reconnect loop reaches and retains its 30-second cap. No fixture,
container, assignment, or device workload ran during this exercise. The development host's normal
dynamically linked worker binary required glibc 2.39 while the 950PR host provides glibc 2.34, so the
exercise used a static-musl binary built from the same revision; portable release packaging remains
a deployment concern rather than an execution-contract change. That packaging gap is now closed by
[`build_portable_linux_binaries.sh`](../scripts/build_portable_linux_binaries.sh) and its Rust 1.88.0
CI job. The script builds locked static-musl release server/worker binaries, verifies static linkage,
and emits checksums. The release worker was directly loaded on the glibc 2.34 host and reached normal
configuration validation without a loader error.

The separately identified legacy attempt
[`legacy-ascend-parity-20260812T092354Z`](evidence/ascend-add-v1-legacy-parity-20260812.json) then ran
the unchanged fixed source once through the legacy SSH/SCP container path. NPU 3 was `OK` and
process-free before and after; the exact image, CANN, driver, firmware, deterministic stdout, exit 0,
and successful classification agree with the accepted outbound attempt. The comparison and its
explicit stderr-archive limitation are recorded in
[`ascend-add-v1-parity-20260812.md`](evidence/ascend-add-v1-parity-20260812.md). The legacy observation
is not an authoritative AlloyPort receipt or correctness verdict.

The worker process entry is now deliberately thin. `application/config.rs` owns the unified
connection/identity schema, `application/backend_config.rs` owns CUDA/Ascend local policy schemas,
`application/assembly.rs` selects and wires concrete probes, stores, transport clients, supervisors,
and runtimes, while `application/runtime.rs` owns reconnect and shutdown behavior. Architecture CI
prevents those responsibilities from returning to `main.rs` or leaking concrete runtime adapters
into configuration and lifecycle modules.

That server slice is now implemented as well. `alloyport-server` has separate process configuration,
offline identity administration, concrete service assembly, and runtime supervision modules. The
gRPC listener, lease reaper, and assignment-preparation reconciler share a cooperative shutdown
signal; Ctrl-C or any unexpected task exit stops the others and drains them within
`ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS` (default 10) before abort is used as a last-resort bound.
Architecture CI keeps environment parsing out of assembly, concrete storage out of configuration,
and process supervision out of the six-line binary entry point. A strict schema-1 server file now
provides explicit locator/value precedence, file-relative paths, fail-closed validation, and the
same identity database for serving and offline administration. The first Port conformance slice
also runs immutable Artifact semantics against filesystem and memory implementations. A second
suite now applies the same known-attempt, exclusivity, idempotency, immutable preflight, terminal
quarantine, and explicit-release rules to the SQLite worker journal and a focused memory fake;
SQLite restart durability remains an adapter-specific test. A third suite applies identity,
monotonic observation, duplicate, renewal/expiry, non-resurrection, stale-result, and cancellation
semantics to the server's SQLite attempt/lease adapter and a focused reference. Cancellation
acknowledgement remains a control receipt; only the terminal observation proves execution ended.
The server assignment contract is complete as well: SQLite and a focused reference share immutable
admission, preparation/defer ordering, dispatch/replay, conflict rollback, and reassignment rules.
SQLite-specific tests retain reopen, migration, and connection-sequence rollback evidence. Artifact
upload metadata now has the same substitutability evidence: SQLite and a focused memory reference
share owner-scoped idempotency, exact offsets, publication failure classes, quotas, references,
retention, and reachability semantics. Reopen/migration, crash-tail repair, concurrent quota
admission, active readers, and pending-GC recovery remain SQLite-specific. Interaction persistence
now also has shared substitutability evidence: SQLite and a focused memory reference share run-local
sequence, canonical deduplication/conflict, raw-output gap/overlap, bounded cursor, run isolation,
and terminal grant-revocation rules. SQLite retains reopen evidence, while replay-to-live delivery,
slow-consumer recovery, authorization, and sanitization remain application-level tests. Execution
backend commonality has advanced as well: CUDA and Ascend now use one transport-neutral container
identity reconciler, one running-container cancellation/timeout/output supervisor, and one terminal
outcome classifier with backend-specific typed policy constants. Their create plans, image/device
checks, environment facts, receipts, and hardware gates remain deliberately separate. gRPC adapter
status policy is now centralized too: Control repository, Interaction, Artifact, and certificate
identity errors have one audited domain-category-to-`tonic::Code` mapping with table-driven tests,
and duplicate service-local mappings were removed without changing existing codes. The following
gRPC slice is complete too: Control, Artifact, and Interaction share one authenticated request
context, with transport-specific revocation checks kept visible. Artifact upload now revalidates
before every committed chunk, covered by a test that revokes access between two chunks.

The real GB10 gate is explicit and remains ignored during normal test runs:

```bash
ALLOYPORT_CUDA_SMOKE_IMAGE_MANIFEST_DIGEST=sha256:54b468554100ecc85701eaad1013cf11d7cde22f30e987f610de71c2cb85584a \
ALLOYPORT_CUDA_SMOKE_IMAGE_REFERENCE=lmsysorg/sglang@sha256:54b468554100ecc85701eaad1013cf11d7cde22f30e987f610de71c2cb85584a \
ALLOYPORT_CUDA_SMOKE_IMAGE_ID=sha256:79a186a4a784f1c3b53976e2a712a86ea6067e47faee4aa59829e35ae42dd884 \
cargo test -p alloyport-server --test cuda_control_plane \
  cuda_runtime_completes_through_real_docker_outbound_loopback --locked -- \
  --ignored --exact --nocapture
```

## Known gaps: do not claim these are implemented

- Lease-expiry reassignment is explicit and durable, but there is no scheduler policy that chooses a
  replacement worker or proactively invokes it for every expired lease.
- The worker journal, lifecycle outbox, and fake executor's local Artifact spool are disk backed. An
  explicitly configured outbound worker launches that fake runtime and preserves its task across
  stream reconnects. An optional publisher uploads its spool and gates terminal reporting, but the
  worker binary does not attach the fake component. The binary now constructs the CUDA runtime,
  downloader, and mandatory publisher only from one explicit local policy file. Execution code
  depends on the immutable `ArtifactStore` port rather than the filesystem adapter. Publisher
  failures retain stable local-Artifact, unavailable, rejected, and internal categories across both
  initial execution and terminal replay instead of being flattened into error strings. CUDA
  contract fixtures live in a separate test module so production module metrics and CAS boundary
  checks cover the actual application code. The CUDA container-engine plugin port exposes typed
  invalid-configuration, unavailable, command-failed, invalid-response, and internal failures;
  its port and transport-neutral values, durable supervisor state machine, and pure output-budget/
  terminal-outcome policy live in separate modules. Supervisor reconciliation invariants remain
  application errors rather than engine failures. The Docker adapter separates container lifecycle
  orchestration, bounded CLI process/log I/O, and its dedicated JSON/scalar response parser into
  three modules.
- Durable lifecycle replay and seven-day orphaned-delivery retention are implemented. Heartbeats,
  status, output previews, welcomes, and ACK-only frames deliberately remain ephemeral; there is no
  generalized durable message bus or server replication.
- The filesystem content-addressed store, durable resumable-upload sessions, registered Artifact
  gRPC service, stable certificate-enrolled owner binding, typed read authorization, and transactional
  quotas are implemented. Worker terminal ingestion now creates typed output/receipt references;
  other controller/public grant operations and automatic retention/collection scheduling remain
  absent. The SQLite adapter separates upload-session/staging writes, metadata/reference operations,
  and authorized reader leases/garbage collection; SQL remains inside these implementation modules.
  Garbage collection depends on the narrow `ArtifactRetentionStore` removal port rather than the
  filesystem CAS. There is no object-store adapter or filesystem-capacity monitor.
- No automatic device reset. The Docker boundary follows running logs, forwards
  bounded best-effort stdout/stderr previews with independent offsets, and actively stops the
  identified container on combined output-budget exhaustion. A full preview queue never blocks log
  draining or changes terminal bytes; later chunks expose gaps to the server warning path. Preview
  coalescing and an explicit `output_suppressed` count are not implemented. Binary configuration is
  explicit rather than discovered.
  Attached fake runs emit ephemeral gRPC output previews and accept control-stream cancellation;
  workers with no executor attached still terminate cancelled admitted attempts directly.
- CUDA has a bounded `nvidia-smi` adapter whose `Ready` state requires explicit
  `gpu_recovery_action=None`; recovery actions are unhealthy and unsupported evidence is degraded.
  Both backends use the common Design 0022 selector and durable per-attempt `DeviceGuard`, including
  immutable preflight, cleanup replay, and quarantine retention. There is no automatic reset or
  dynamic per-attempt multi-device scheduler. Startup binds a uniquely leased quarantine device for
  recovery without making it eligible for new work, preventing cleanup replay deadlock. The trusted
  local Ascend image ID can drive the
  outbound gate without an OCI registry when the assignment uses the OCI image-config media type.
- Assigned worker lifecycle and raw previews are durably translated into observed canonical events.
  The production service now shares a bounded per-run hub with mTLS-authorized replay/subscription
  RPCs and durable run grants. There is no interaction retention scheduler, cursor-expired response,
  terminal UI worker view, or general redaction policy for future non-worker producer adapters.
  Preview gaps are visible and final Artifacts retain the complete bytes; preview coalescing is not
  implemented.
- No external scheduling API or task controller integration. The worker translator intentionally
  does not emit `run.completed`, gate verdicts, or audited transitions because it does not own those
  decisions.
- mTLS enrollment, rotation, revocation, Artifact authorization, and worker hello binding are covered
  end to end. Certificate issuance, online enrollment, CA revocation/expiry monitoring, pool/role
  authorization, and replicated identity storage are not implemented.
- CUDA attempts can persist a fixture-specific receipt, but there is no signed/attested general
  Design 0007 RunReceipt, oracle integration, or audit transition.
- No server replication, shared registry, load balancer/session ownership, or cross-process GC reader
  coordination.
- The Python harness in `/data/projects/ascend-factory/harness/worker.py` and `box.py` still uses
  SSH/SCP as a separate legacy/reference project. AlloyPort's runtime and process configuration have
  cut over to outbound gRPC and contain no SSH/SCP path; architecture CI prevents those fields and
  transports from re-entering the runtime. Retiring or rewriting the separate Python project is not
  an AlloyPort runtime requirement.

## Product course correction and active work order

The fixed CUDA and Ascend execution slices, outbound-only runtime, Artifact transport, authenticated
control, canonical events, and portable worker packaging form a completed infrastructure baseline.
They prove that prepared workloads can be transported and executed with durable evidence. They do
not prove that AlloyPort can analyze CUDA source, generate Ascend C, preserve a public call path, or
release an independently verified migration.

The previous next task, interaction retention, is deferred. Continuing to improve the control plane
before a real migration loop exists would increase platform completeness without reducing the main
product risk.

[`PRODUCT_EXECUTION_PLAN.md`](PRODUCT_EXECUTION_PLAN.md) is now the only active work order. Its scope
is deliberately narrow:

1. implement [Design 0024](design/0024-migration-spec-v1-and-first-product-slice.md), replacing the
   obsolete multi-backend route model with `MigrationSpec v1` and Ascend-C-producing strategies;
2. add one bounded reduction extension containing CUDA device code, host launch, CMake, a public
   C/C++ call path, and an independently runnable CUDA reference;
3. generate the target Ascend C compute, host glue, and build integration rather than accepting an
   observer-written target skeleton;
4. execute CUDA reference and Ascend candidate independently, join their receipts by experiment, and
   judge them with a calibrated oracle;
5. release source, component mapping, supported domain, fallback, correctness evidence, performance
   evidence, and integration evidence;
6. only then add EvoKernel-inspired drafting/refining rewards and value-backed cross-task memory.

Infrastructure is frozen by default until that slice passes. Interaction retention, TUI, public task
submission, general scheduling, automatic GC, server replication, online certificate issuance, and
additional unrelated fixed fixtures are explicitly deferred. A change in one of those areas requires
a concrete blocker observed in the active migration slice.

The first implementation slices are complete in the current working tree: `alloyport-core` owns a
validated, versioned `MigrationSpec`, Ascend-C-producing generation strategies, the migration
lifecycle, candidate/spec binding, and a mandatory generated-source release gate. The original CUDA
reduction intake and frozen JSON contract live under
[`fixtures/migrations/cuda-reduction-v1`](../fixtures/migrations/cuda-reduction-v1); the intake
deliberately contains no Ascend target skeleton. Deterministic source/build inspection now binds its
report to spec/source digests and checks the declared files, CUDA kernel/indexing, host launch/error
path, public symbol, CMake references, and initial unsupported constructs. The checked-in reduction
intake passes with 12 source evidence records; deliberate missing-launch, missing-build-reference,
undeclared-copy, and cooperative-groups cases fail before generation.

The reusable portion of the first candidate-authoring boundary is implemented. `alloyport-core`
constructs migration context only after reproducing the passing intake inspection, excludes
undeclared files, and validates a bounded four-category source bundle under `generated/`. The
provisional DeepSeek-specific one-shot authoring command and its curl subprocess transport have now
been deleted. Their vendor/model coupling is not part of the runtime architecture. Useful transport
constraints—owner-only secret files, exact bytes, explicit timeouts and bounds, and sanitized
diagnostics—were carried into the provider-neutral SDK and Tokio-native HTTPS adapter.

The repository developer is the system developer and coach, not the runtime migration driver. The
runtime LLM is replaceable and defaults by configuration to `deepseek-v4-pro`; vendor, deployment,
protocol, model profile, and generation parameters are separate receipt facts. Design 0025 now
defines five loops: model attempt, Agent Episode, candidate search, task delivery, and cross-task
knowledge. It preserves native continuation separately for `openai_responses`,
`openai_chat_completions`, and `anthropic_messages`. The model iterates through controlled tools but
cannot approve its output, Gates, knowledge, or release.

The first Design 0025 implementation slice is complete in `alloyport-core`. Validated identities and
checked records now cover Agent Episodes, Model Attempts, decoded Turns, Tool Operations, and Search
Runs. Episode, attempt, turn, and search records explicitly bind their immutable subtask, input,
model/deployment/profile, continuation, policy, and budget identities needed for later recovery.
Episode completion must pass through stop review; terminal tool results must be committed before the
loop continues; ambiguous external effects must reconcile before success; and refinement requires a
recorded incumbent. A strict schema-1 model catalog resolves alias, wire model, deployment,
protocol-specific options, typed secret-file auth, profile, settings, data boundary, and conformance
receipt into an immutable episode snapshot. `deepseek-v4-pro` appears only as test configuration:
the same wire model resolves through Chat Completions or Anthropic Messages without vendor branches.
A semantic `ModelGateway` port and scripted non-network fake preserve native call IDs, reject empty
or duplicate calls, and deterministically prove multiple tool turns.

The second Design 0025 slice is also complete. A stateless `AgentLoopRunner` advances one durable
boundary at a time over a compare-and-swap `EpisodeRepository`; the checked-in in-memory adapter is
only the deterministic reference fake, not the eventual production persistence adapter. Model and
tool dispatch first commit `Dispatching`. An uncertain model dispatch is charged and retried as a
new linked attempt within budget, while an uncertain tool action retains its stable logical
operation identity and must reconcile before progress. The scripted scenario submits a candidate,
consumes an independent fake Source Gate failure, submits a corrected candidate, consumes a passing
Gate reference, and succeeds only after controller stop review. Tests reconstruct the runner across
every ordinary boundary and cover injected dispatch/outcome crashes, explicit reconciliation,
cancellation with and without ambiguity, verified-result authority, incomplete narrative stops, and
model-turn budget exhaustion.

The third Design 0025 slice is complete in `alloyport-core`. `ProtocolCodec` now has independent
`OpenAiResponsesCodec`, `OpenAiChatCompletionsCodec`, and `AnthropicMessagesCodec`
implementations over a shared bounded request/continuation contract. Golden fixtures replay two
successive client-tool turns for every protocol. Chat retains the complete assistant message and
uses `tool_call_id`; Responses retains every typed output item, including encrypted reasoning, and
uses `call_id`; Anthropic retains ordered thinking/signature/redacted-thinking blocks and emits a
following user `tool_result` message keyed by `tool_use_id`. Malformed argument strings remain
exact, while missing/duplicate call IDs, undeclared tools, result-set mismatches, unsupported native
items, empty turns/results, invalid JSON/schema, and exceeded bounds fail closed. Tests also cover
usage/actual-model capture, refusal/filter/truncation/unknown stop normalization, raw-response and
continuation digests, and result ordering independent of execution order. The codec layer has no
secret, transport, tool execution, or completion authority.

The fourth slice is complete. `alloyport-llm-provider` is the independent provider SDK used through
the Agent Runtime's async `ModelGateway`; it resolves the configured alias, selects one of the three
protocol codecs, appends correlated tool results, dispatches exactly once, decodes the response, and
commits exact request/response/continuation bytes through a context-store port before exposing the
semantic turn. `alloyport-model-http` supplies `reqwest`/`rustls` transport with HTTPS-only,
redirect/proxy/retry/decompression disabled, explicit TLS/timeouts, bounded streaming response, and
typed dispatch certainty. The default `deepseek-v4-pro` remains configuration only. Deterministic
tests prove the real Agent loop calls the SDK without network access.

The fifth slice is also complete. `alloyport-candidate-tools` implements the iterative
`submit_candidate_bundle` and `request_source_gate` adapters without depending on any provider or
HTTP implementation. Submission binds controller-owned task/spec/strategy facts around the
untrusted source bundle, ingests each file and the manifest into CAS, derives a task-scoped content
identity, and publishes the candidate tree through same-directory staging plus create-only rename.
Every reuse rereads the exact file set, sizes, and digests; unsafe candidate path segments,
symlinks, extra files, cross-task manifests, and changed bytes fail closed. The independent
structural Source Gate checks exact Artifact identity, UTF-8 source, forbidden framework/prebuilt
fallbacks, Ascend C kernel structure, preserved host entry point/launch, build references, and
complete input/output component mapping. Its immutable receipt is the verified model-visible tool
result. A deterministic real-tool Agent Episode test consumes a failing receipt identity, submits a
child correction, consumes a passing receipt, and succeeds only after stop review. Correctness,
live provider validation, API, scheduler, UI, and generalized execution remain absent.

The sixth slice is complete under [Design 0026](design/0026-ascend-build-gate-connection.md). The
generic Agent tool lifecycle now distinguishes a healthy remote `Running` operation from an
ambiguous dispatch and reconciles it with the same logical operation identity. A Source-Gate-passing
candidate can request one controller-authored build bundle and stable `AscendBuild` assignment;
the tool rereads the exact published Source Gate receipt Artifact and rejects missing, failing,
foreign, or byte-mismatched evidence before dispatch. The server adapter uses the existing durable
worker-control repositories and CAS receipts, while the worker retains an independent, fixed
`ascend-build-v1` policy with no shell, network, caller-selected image, mounts, environment, or
limits. Its trusted runner invokes CMake through argument arrays and requires a build-complete
marker before success. A deterministic full Episode proves compiler failure, model-visible
feedback, child-candidate submission, a second stable build attempt, and final Build Gate success;
it makes no Correctness or Performance claim.

The seventh bounded slice is complete under
[Design 0027](design/0027-reduction-differential-oracle-and-calibration.md). `alloyport-core` now
owns structured CUDA-reference/Ascend-candidate run receipts, stable paired experiment identity,
the four correctness verdicts, a frozen fp32 comparator policy, and a ten-mutant calibration
battery. Candidate tools accept only the exact passing Build Gate receipt, dispatch/reconcile one
stable `ReductionCorrectnessAttemptPort` experiment, independently persist calibration and
Correctness receipts, and satisfy an Episode only on calibrated `PASS`. Deterministic tests prove
Build-to-Correctness connection and durable Episode completion without a device or provider. The
production paired CUDA/Ascend worker adapter and real hardware receipts remain unimplemented.

The production-execution prerequisite slice is complete under
[Design 0028](design/0028-controller-authored-correctness-execution-bundles.md). The controller now
authors separate immutable CUDA-reference and Ascend-candidate execution bundles containing the
same validated 24-observation corpus and exact role-separated source trees. The attempt Port carries
both Artifact descriptors; candidate tooling rereads the immutable manifest/materialization before
dispatch and binds returned run receipts to the assigned implementation digests. The oracle requires
exact corpus coverage and recomputes case input identities, so coordinated case omission cannot
calibrate. Source Gate v2 additionally requires the fixed reduction C ABI and
`alloyport_reduction_candidate` CMake target.

The paired controller-dispatch slice is complete under
[Design 0029](design/0029-paired-correctness-worker-dispatch.md). Protocol minor 6 adds separate
default-deny CUDA-reference and Ascend-candidate correctness executors with backend-bound features.
`WorkerCorrectnessAttemptAdapter` maps one Port operation to two stable offline single-device
assignments on distinct configured workers, idempotently recovers partial dispatch, and waits for
both durable terminal observations. It validates each generic worker receipt, its stdout digest,
and the structured `ReductionRunReceipt` against the assigned bundle before returning run
descriptors. The server grants each worker only its role bundle.

The trusted worker runtime slice is complete under
[Design 0030](design/0030-trusted-reduction-correctness-worker-harness.md). A shared role-bound
policy now verifies and create-only materializes exact execution bundles, derives shell-free pinned
CUDA and Ascend Docker plans, and installs a trusted harness that invokes the fixed reduction ABI
over the frozen corpus. CUDA/Ascend supervisors and generic runtimes publish the corresponding
exclusive feature and accept exit zero only with a validated structured run receipt; a fake Docker
engine proves that fixture markers cannot cross into correctness evidence.

Standalone correctness-worker composition is complete under
[Design 0031](design/0031-standalone-correctness-worker-configuration.md). The unified worker file
now has separate `cuda_correctness` and `ascend_correctness` variants with no fixture ID or fixed
bundle digest. Their production assembly retains strict local image/device/environment authority,
device lifecycle, CAS transfer, generic receipt publication, and outbound control behavior while
advertising only the matching correctness feature. Checked-in examples are parsed by tests. Paired
device evidence remains unimplemented.

The pinned-image and first hardware diagnostic slice is complete under
[Design 0032](design/0032-pinned-correctness-images-and-cuda-diagnostic.md). Separate CUDA and
Ascend correctness Dockerfiles contain only their pinned build/runtime environments. Real attempts
on the idle GB10 exposed and then closed missing Python-standard-library and CMake dependencies.
The final image executed all 24 frozen CUDA observations under the offline/read-only container
boundary, and the production oracle detected all ten mutants. The exact diagnostic run and
calibration receipts plus image/base identities are in
[`evidence/reduction-correctness-harness-diagnostic-20260812.md`](evidence/reduction-correctness-harness-diagnostic-20260812.md).
The Ascend image is built and toolchain-checked, but no developer-authored target was substituted
for the still-missing generated candidate.

The first production Episode-composition blocker is closed under
[Design 0033](design/0033-durable-agent-episode-repository.md). Complete provider-neutral Episode
state is now strictly deserializable, schema-checked on recovery, and persisted through a server-owned
`SQLite` compare-and-swap repository. Reopen, duplicate creation, stale revision, reference-adapter
parity, and malformed-state rejection are covered.

That provider context bridge is now complete under
[Design 0034](design/0034-durable-provider-turn-context.md). Exact request, response, and native
continuation bytes are immutable Artifacts; `SQLite` correlates them with Episode/attempt/turn
identity. Terminal Candidate-tool results are recorded idempotently before reducer success, and the
next provider input must equal the reducer's continuation-plus-results digest. Reopen recovery,
wrong input, unrelated result, exchange idempotency, and exact multi-result order are covered.

That application composition is now complete under
[Design 0035](design/0035-controller-episode-application.md). The controller derives model,
deployment, profile, tool-catalog, and loop-policy identities, creates or exactly recovers both
stores, owns the provider gateway/tool recorder/runner, and offers the bounded production HTTPS
composition. Durable Episode schema 2 separately retains the immutable initial input identity so a
progressed Episode can be checked on recovery. Deterministic tests run a complete provider turn to
terminal state, reopen it without another dispatch, and reject changed prompt context.

The Candidate gateway assembly itself is now complete under
[Design 0036](design/0036-gated-candidate-episode-composition.md). It binds the existing Build and
paired Correctness worker-control adapters, derives Candidate authority from the MigrationSpec, and
replaces caller tool definitions with the fixed four strict schemas. Model arguments cannot choose
workers, images, devices, commands, corpus, or tolerance policy. The remaining bootstrap is a
deliberate operator process/configuration path and bounded polling owner. No non-example runtime
model catalog, secret file, or relevant key/token environment variable was present in the workspace
or process environment during this session, so no provider dispatch was attempted.

The explicit process boundary is now complete under
[Design 0037](design/0037-explicit-candidate-episode-bootstrap.md). Normal `serve` remains inert.
`candidate-episode validate` performs strict local catalog/credential/spec/reference/prompt/path and
worker-policy preflight without binding or network dispatch. The live form requires the literal
`--authorize-provider-dispatch`, starts the shared services, waits for three distinct connected
workers with exact advertised features, and polls pending Gate work under bounded configuration.
The example remains deliberately unrunnable until real digests, image descriptor sizes, context
digests, a secure catalog secret, and runtime paths are supplied.

The previously missing Build process role is now complete under
[Design 0038](design/0038-standalone-ascend-build-worker.md). The unified worker schema accepts an
`ascend_build` variant without fixture/bundle authority, and production assembly binds the existing
Build policy/supervisor/runtime to one dynamically verified NPU while advertising only
`ascend-build-v1`. The checked-in example still requires actual host/image/TLS facts before launch.

## Suggested first task for the next Codex session

The next action remains product-plan item 5, now narrowed to worker deployment and the first genuine
candidate plus paired hardware acceptance:

> Prepare all three exact worker deployment files, then run the configuration-selected model
> through the Source/Build correction
> loop until it produces a
> genuine Source- and Build-Gate-passing Ascend reduction candidate. Then
> execute the exact frozen corpus through both standalone correctness workers and capture the paired
> run and Correctness receipts. Keep comparison and hidden corpus authority on the controller. Do
> not yet
> add performance optimization, release automation, live provider validation, API, scheduler, UI,
> retention policy, or generalized execution.
