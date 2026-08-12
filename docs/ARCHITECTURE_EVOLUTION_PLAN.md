# Architecture evolution plan

- Date: 2026-08-11
- Status: Active
- Motivation: retain AlloyPort's verified accelerator lifecycle while adopting the useful
  modular-monolith, port, composition-root, and API-boundary practices demonstrated by Shepherd

## Outcome

AlloyPort remains a small modular monolith with separate server and outbound-worker processes. It
will not copy Shepherd's crate count or expose a broad public API before a real product use case
requires one. The target is replaceable infrastructure, explicit process assembly, narrow contracts,
and identical domain behavior whether a capability is invoked locally or through a transport.

```text
CLI / future UI
       |
       v
public API adapters (deliberately small)
       |
       v
application use cases -> narrow ports -> domain/core
       |                    |
       |                    +-> SQLite / filesystem / device / container adapters
       +-> internal worker-control transport adapters

server composition root                   worker composition root
config -> assemble -> tasks -> shutdown   config -> assemble -> reconnect/shutdown
```

## Rules

1. Domain and application use-case code do not read process arguments or environment variables;
   only the process configuration module may do so.
2. A binary entry point constructs a runtime and delegates immediately; it does not contain backend,
   database, TLS, device-probe, or container policy.
3. Application use cases consume the narrowest capability port. Composite compatibility facades may
   remain at composition boundaries but must not spread back into use-case modules.
4. Protobuf is a transport contract, not the durable domain model. Validation and mapping stay at
   transport edges.
5. Public customer APIs and internal worker-control APIs remain separate packages and services.
6. GPU and NPU share lifecycle policy and device-selection abstractions; only trustworthy probe,
   container, and receipt details remain backend-specific.
7. Configuration is schema-versioned and fail-closed. A worker continues to use one complete file;
   remote plaintext and incomplete identity or device policy remain invalid.
8. Every spawned long-running task has an owner, cancellation path, and bounded drain policy.
9. New crates require a dependency-direction or independent-release justification. Prefer cohesive
   modules in the existing seven-crate workspace.

## Existing foundation

The repository already has most of the inward-facing structure this plan needs:

- dependency-light domain types in `alloyport-core`;
- capability-segregated server repositories and worker journal ports;
- abstract Artifact, execution-backend, device-lifecycle, and container-engine boundaries;
- explicit Protobuf/domain conversion at transport edges;
- one strict worker configuration shared by CUDA and Ascend;
- shared GPU/NPU selection, durable device guards, preflight evidence, and quarantine;
- SQL and filesystem implementations contained in adapter modules;
- architecture, formatting, Clippy, and workspace test gates.

The goal is therefore incremental hardening, not another broad rewrite.

## Delivery plan

### Phase 1 — Thin process entry points and explicit composition

Split each process into configuration, assembly, background-task ownership, and runtime/shutdown
stages. Keep concrete adapters in assembly modules only.

Worker acceptance:

- `main.rs` is at most 20 lines and calls one application entry point;
- worker file/schema parsing, backend policy, CUDA/Ascend assembly, and reconnect lifecycle are
  physically separate;
- neither configuration nor runtime lifecycle modules know concrete device/container adapters;
- existing CUDA/NPU startup validation and execution behavior remain unchanged.

Server acceptance:

- environment/config parsing is independent of service assembly;
- control, Artifact, Interaction, identity, TLS, and persistence adapters are assembled in one typed
  application object;
- lease reaper and preparation reconciler are owned task handles, use cooperative cancellation, and
  receive bounded drain behavior rather than relying primarily on `abort`;
- identity administration is separated from the serving runtime.

### Phase 2 — Port and adapter conformance

Create reusable contract suites for the most consequential ports: assignment state, attempt/device
leases, Artifact metadata, interaction replay, and execution backends. Run the same behavioral
contract against SQLite/filesystem implementations and focused in-memory fakes where meaningful.

For internal gRPC boundaries, test that transport adapters preserve typed failure categories,
identity, idempotency, and replay semantics. Do not create a central `alloyport-ports` crate merely
to collect trait names; move a port only when two bounded contexts genuinely need to own the same
contract.

### Phase 3 — API service discipline

Keep worker control, Artifact transfer, and Interaction replay as separate internal/service APIs.
Add common server middleware for authentication context, request limits, tracing, and stable error
mapping before adding more services.

The public task-submission API remains deliberately deferred until at least one representative CUDA
and Ascend workflow is exercised end to end through product-level orchestration. When introduced,
it will delegate to application ports rather than call SQLite or worker sessions directly, and it
will not expose internal assignment or device identifiers as customer-controlled host policy.

### Phase 4 — Operator configuration and topology readiness

After the server composition root is isolated, add a versioned server configuration file with
precedence `CLI > environment > file > defaults`. Preserve the zero-dependency loopback trial and do
not require an OCI registry, external database, certificate service, or service discovery system.

Role-split server processes are not a current goal. If measurements later justify splitting a role,
the relevant application port must first have a transport adapter and local/remote parity tests.

### Phase 5 — Lifecycle and observability parity

Standardize readiness, structured startup summaries, task-failure propagation, graceful drain, and
per-service metrics. Bound queues and persistence concurrency explicitly. Continue treating
canonical Interaction events, wire messages, and durable audit/evidence as separate type systems.

## Current progress

- [x] Baseline architecture and dependency direction reviewed.
- [x] Worker binary reduced to a thin runtime constructor.
- [x] Worker configuration, backend policy, concrete assembly, and reconnect lifecycle separated.
- [x] Architecture checks prevent worker responsibilities from returning to `main.rs` or crossing
  from composition into configuration/runtime modules.
- [x] Server configuration, identity administration, concrete assembly, task ownership, and serve
  lifecycle separated; periodic tasks cooperatively cancel and drain within a configured bound.
- [x] Reusable Port contract suites run immutable Artifact/retention semantics against filesystem
  and memory adapters, and shared GPU/NPU device-lease semantics against SQLite and a focused fake.
- [x] Server attempt/lease lifecycle contract runs identity, transition, duplicate, renewal,
  expiry, stale-result, and cancellation semantics against SQLite and a focused reference.
- [x] Server assignment contract runs immutable admission, preparation/defer, dispatch/replay,
  conflict rollback, and reassignment semantics against SQLite and a focused reference.
- [ ] Remaining high-value Port contract suites implemented in the order recorded in
  `PORT_CONTRACTS.md`, beginning with Artifact upload metadata.
- [ ] Common gRPC middleware/error policy established.
- [x] Strict schema-1 server configuration, explicit locator precedence, file-relative paths, and a
  shared serving/identity-administration command boundary introduced.

## Verification

Every slice must pass:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/check_sql_boundaries.sh
bash scripts/check_architecture_boundaries.sh
cargo test --workspace
```

Real GPU/NPU gates remain separately invoked, explicitly configured evidence gates. Structural
refactoring must not silently rerun hardware workloads or reinterpret an earlier receipt as evidence
for changed execution code.
