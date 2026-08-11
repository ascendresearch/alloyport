# AlloyPort architecture remediation plan

- Status: Active
- Started: 2026-08-11
- Scope: architecture, persistence boundaries, extensibility, and maintainability

## Objective

Evolve the current reliable infrastructure vertical slice into a layered system that can add
scheduling, Ascend execution, and the migration pipeline without concentrating new behavior in the
existing server and worker coordinators.

The remediation must preserve the current trust boundary, durable replay semantics, Artifact
integrity, and externally visible protocols. Refactoring is complete only when those invariants are
covered by automated tests.

## Target dependency rule

```text
transport (gRPC / CLI)
          |
          v
application use cases -----> domain model
          ^                       ^
          | ports                 |
adapters (SQLite / CAS / executor / event delivery)
```

Dependencies point inward. Domain code does not depend on Tokio, tonic, rusqlite, generated
Protobuf types, Docker, or filesystem implementations. Application services depend on domain types
and narrow ports. Composition roots select concrete adapters.

## Non-negotiable persistence rule

SQL and database-driver types may exist only under an `adapters/sqlite/` implementation boundary.
Outside that boundary:

- no SQL statements or migration text;
- no `rusqlite::Connection`, `Transaction`, `Row`, `Params`, or `rusqlite::Error`;
- no repository error variant named after SQLite;
- no transaction object exposed through a port;
- no gRPC status or HTTP extension type implemented by a database adapter.

Repository ports express atomic business operations, not generic table CRUD. Operations spanning
different durable stores use explicit preparing states, idempotent steps, an outbox, or a
reconciliation loop rather than pretending to be one ACID transaction.

## Findings and workstreams

### R1 — Runtime-aware worker admission (P0)

An assignment must not be durably accepted unless an attached execution backend supports its
executor kind. Admission-only behavior remains an explicit harness mode and is never the production
default.

Acceptance:

- an unconfigured production worker rejects an otherwise valid assignment;
- the rejection is durable and identifies the unsupported executor;
- CUDA and fake runtimes admit only their declared executor kinds;
- restart/replay tests use an explicit admission-only harness where execution is intentionally out
  of scope.

### R2 — Durable preparation and reconciliation (P0)

Replace cross-repository enqueue ordering with a durable preparation workflow. A replacement
attempt cannot become replayable until its input grants and canonical run projection are ready.

Acceptance:

- assignment preparation has explicit `Preparing` and `Dispatchable` states;
- failures after every durable step converge after retry or restart;
- conflicting attempt IDs cannot leave usable grants for an unrelated worker;
- replay selects only dispatchable assignments;
- same-control-database lease, outbox, and connection-sequence updates commit atomically.

Implemented in the first R2 slice:

- new attempts are stored as `Preparing` and become `Dispatchable` only after Artifact grants and
  the canonical run-started projection succeed;
- `Preparing` attempts are excluded from reconnect replay and can be completed by an idempotent
  enqueue retry;
- lease grant, `Sent` transition, assignment outbox insertion, and connection sequence advancement
  are one SQLite transaction;
- the atomic assignment-delivery SQL has moved into the SQLite adapter, with rollback coverage for
  a deliberately failed outbox insert.

The second R2 slice adds autonomous recovery:

- startup reconciliation runs after Artifact and interaction adapters are attached but before the
  server begins listening, so reconnecting workers only see fully prepared assignments;
- a periodic reconciler continues retrying dependency failures after startup;
- scans are bounded to 128 rows, and deferred rows rotate behind unseen work by durable retry
  timestamp, preventing one unavailable Artifact from starving the queue;
- failures are isolated per assignment and returned in a structured reconciliation report;
- restart residue, idempotent event replay, failure isolation, retry rotation, and batch rollback
  have automated coverage.

### R3 — Real domain and application layers (P1)

Make `alloyport-core` (or a clearly named successor) the home of stable typed identities, immutable
assignment contracts, executor/backend capabilities, Artifact descriptors, outcomes, and lifecycle
rules used by the running server and worker. Keep wire, canonical interaction, and durable state
representations distinct and map them at edges.

Acceptance:

- server and worker application code share the stable immutable contract vocabulary;
- generated Protobuf numeric enums do not appear in durable domain records;
- IDs and digests cannot be constructed in invalid states;
- persistence and transport mappings have focused contract tests.

### R4 — Pluggable execution backends (P1)

Replace the closed `AttachedRuntime::{Fake, Cuda}` dispatch with an execution-backend port and
registry. Backend-owned validation declares capability, recovery, execution, and cleanup behavior.

Acceptance:

- adding a test backend requires no edit to the control-session state machine;
- backend errors retain retryable, terminal, policy, and integrity classifications;
- Artifact input/output services are execution-context ports rather than CUDA branches;
- an Ascend backend can be added through composition.

Implemented in the first R4 slice:

- the closed `AttachedRuntime::{Fake, Cuda}` dispatch has been removed;
- `ExecutionBackend` declares owned executor kinds, execution, and restart-cleanup behavior;
- worker composition registers backends by executor kind and rejects overlapping ownership before
  a session starts;
- Fake and CUDA are built-in adapters behind the same port, and a probe backend test demonstrates
  that adding an implementation does not edit the control-session or attempt state machine.

The remaining R4 follow-up is to refine all backend failures into explicit retryable, terminal,
policy, and integrity categories; Artifact input failures now already preserve these broad classes.

### R5 — Split oversized coordinators by use case (P1)

Split `WorkerControlService`, `OutboundWorker`, Artifact upload metadata, control storage, and
interaction storage along cohesive behavior. Prefer modules inside existing crates before adding
new crates.

Initial target modules:

- server: connection session, assignment coordinator, attempt observer, interaction projector;
- worker: control session, admission service, attempt coordinator, execution registry;
- artifacts: upload service, reference catalog/quota, garbage-collection coordinator;
- events: model, reducer/sequencer, rendering.

No mechanical line-count limit replaces design judgment, but production modules above 800 physical
lines require an explicit cohesion justification. New modules target roughly 200–500 lines.

### R6 — Async-safe persistence boundary (P1)

Synchronous SQLite calls must not run on Tokio worker threads or while holding connection-state
locks. Use a dedicated persistence actor or a consistent bounded blocking adapter. Measure before
selecting a different database library.

Acceptance:

- no synchronous repository call occurs while a Tokio mutex guard is held;
- persistence concurrency is bounded independently from network tasks;
- slow-database tests demonstrate that one worker does not block unrelated sessions;
- application APIs remain independent of the chosen async isolation mechanism.

### R7 — Complete Artifact ports and typed errors (P2)

Separate immutable CAS, mutable upload staging, metadata/references, authorization, and GC ports.
Remove concrete `FilesystemArtifactStore` and `SqliteUploadStore` dependencies from application
services. Replace string errors at plugin boundaries with typed categories.

The worker execution context now consumes `ArtifactInputProvider` rather than
`RemoteArtifactDownloader`. The remote gRPC downloader is an adapter behind that port and maps its
configuration, quota, transport, integrity, and local failures to stable typed input categories.
Artifact output was already exposed through `ArtifactPublisher`; remaining R7 work is to remove
concrete stores from server application services and separate upload staging from metadata ports.

### R8 — Persistence implementation isolation (P1, with P0 transaction slices)

Migrate the five originally mixed database modules into model/port/application and SQLite adapter
code. All five originally mixed database modules are now separated:

| Context | Application port | SQLite adapter |
| --- | --- | --- |
| server control | `ControlRepository` | `adapters/sqlite/control_repository.rs` |
| worker journal | `AttemptStore` | `adapters/sqlite/attempt_store.rs` |
| interactions | `InteractionEventStore`, `RunGrantStore` | two SQLite adapters |
| identity | `IdentityRegistry` | `adapters/sqlite/identity_registry.rs` |
| Artifact metadata | upload/reference domain types | `adapters/sqlite/upload_store.rs` and cohesive query modules |

Migrations live beside their adapters and are tested from every supported historical schema.
Repository contract suites run against the SQLite implementation and application tests use fakes.

## Delivery sequence

### Phase A — Stop new architectural debt

1. Enforce runtime-aware admission.
2. Extract Identity persistence as the first SQL-isolation slice.
3. Add a SQL-location architecture check.
4. Require new database work to follow the target layout.

### Phase B — Repair consistency boundaries

1. Add assignment preparation state and reconciliation.
2. Make control delivery preparation atomic inside the control store.
3. Add fault injection between Artifact, control, and interaction durable steps.

### Phase C — Establish domain/application structure

1. Move immutable contract types and lifecycle rules into the domain layer.
2. Extract server and worker application coordinators.
3. Move gRPC/Protobuf conversion to transport adapters.

### Phase D — Open execution and Artifact extension points

1. Introduce the execution-backend registry.
2. Separate Artifact metadata, staging, CAS, authorization, and GC ports.
3. Add an alternate in-memory adapter to prove interfaces are implementation-independent.

### Phase E — Scale and simplify

1. Isolate synchronous persistence from async runtimes.
2. Split remaining oversized modules based on measured change coupling.
3. Enforce canonical interaction lifecycle where authority requires it.

## Verification gates

Every remediation slice must pass:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Persistence slices additionally require migration, rollback, restart, concurrency, idempotency, and
repository-contract tests. The SQL-location check must report only files beneath SQLite adapter
directories (including their migration and adapter-test files).

## Progress

- [x] Architecture review and code-size baseline completed.
- [x] Persistence SQL inventory completed: SQL is currently concentrated in five mixed modules.
- [x] R1 runtime-aware admission: production defaults reject assignments without a backend;
  admission-only control-plane behavior is explicit.
- [x] First R8 extraction: Identity registry and mTLS resolver separation.
- [x] Server control domain/port separated from `SqliteControlRepository`; `storage.rs` contains no
  SQL or database-driver types.
- [x] Worker journal domain/port separated from `SqliteAttemptStore`; `journal.rs` contains no SQL
  or database-driver types.
- [x] Interaction model/port/live hub separated from `SqliteInteractionStore`; `interaction.rs`
  contains no SQL or database-driver types.
- [x] Artifact upload model separated from `SqliteUploadStore`; `upload.rs` contains no SQL or
  database-driver types.
- [x] Artifact SQLite implementation split by responsibility: the lifecycle orchestrator is 678
  lines (down from 2,204), while schema/migrations, quota accounting, durable references, garbage
  collection, record mapping/staging helpers, and adapter tests live in separate modules. No
  production module in this adapter exceeds the 800-line review threshold.
- [x] Server control coordinator split by use case: `lib.rs` is 616 lines (down from 2,030), with
  assignment coordination (464), attempt observation/projection (488), gRPC transport and wire
  mapping (311), and tests (211) isolated in cohesive modules. Protobuf/domain conversion now lives
  at the transport edge, and no module in this slice exceeds the 800-line review threshold.
- [x] Worker outbound coordinator split by use case: `lib.rs` is 391 lines (down from 1,838), with
  local admission/journal state (230), control-session framing (272), attempt execution coordination
  (537), wire/journal mapping (188), and tests (289) isolated in cohesive modules. No module in this
  slice exceeds the 800-line review threshold.
- [x] First R4 execution-backend slice: closed Fake/CUDA dispatch replaced by an executor-kind
  registry and public `ExecutionBackend` composition port, including duplicate-capability and
  third-party probe-backend coverage.
- [x] Worker executor responsibilities split: durable execution/Artifact coordination is 606 lines
  (down from a 1,395-line mixed module), deterministic fake process behavior is 344 lines, and its
  460-line behavioral suite is isolated from production code. Existing `executor::*` imports remain
  source-compatible through explicit re-exports.
- [x] Worker Artifact input port: execution backends depend on `ArtifactInputProvider`, while the
  remote downloader maps adapter-specific failures into typed Invalid/Policy/Unavailable/Integrity/
  Internal categories.
- [x] Repository-wide production module size gate reached: tests were separated from CUDA Docker,
  CUDA supervisor/runtime, Artifact CAS, and event reducer modules. The largest production Rust
  module is now 740 lines; no production module exceeds the 800-line review threshold.
- [x] First R6 async-persistence slice: worker control and execution paths use an immutable shared
  state handle and route journal operations through a four-permit bounded blocking adapter. No
  SQLite-backed journal call runs while holding a Tokio state mutex; a slow-operation concurrency
  test guards both event-loop responsiveness and the independent persistence limit.
- [x] Server R6 control-stream slice: inbound observation projection and outbound assignment,
  cancellation, and acknowledgement persistence run behind an eight-permit blocking boundary.
  Connection-state snapshots are released before repository work, while a dedicated delivery
  coordinator preserves outbound sequence allocation; slow persistence cannot occupy network
  executor threads.
- [x] Server R6 lifecycle slice: worker registration/disconnection, lease reaping, assignment
  enqueue/reassignment/cancellation, and abandoned-preparation reconciliation use the same bounded
  persistence boundary. Multi-store preparation remains one blocking-domain operation so Artifact
  grants, interaction projection, retry deferral, and dispatchability preserve their ordering.
- [x] SQL-location architecture check has no legacy allowlist entries.
- [x] R2 safe assignment preparation and atomic delivery transaction.
- [x] R2 autonomous reconciliation of abandoned `Preparing` assignments.
- [ ] Remaining workstreams.
