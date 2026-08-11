# AlloyPort

AlloyPort is a verified CUDA-to-Ascend-C source migration and optimization factory.

For a new development session, start with [`docs/HANDOFF.md`](docs/HANDOFF.md).

The project treats migration as evidence-backed engineering, not source-to-source translation.
A successful delivery includes the implementation, its supported domain, correctness and
performance evidence, a reproducible environment, dispatch guards, and a fallback path.

> Status: architecture bootstrap. The name has passed a preliminary public availability check;
> it has not received formal trademark clearance.

## Product boundary

- CUDA source code is the migration object.
- Maintainable Ascend C source code is a mandatory release artifact.
- Phase 1 migrates a bounded CUDA extension: device kernels, host launch code, build integration,
  and one runnable public call path.
- A later phase expands to complete CUDA modules and projects with multiple kernels, streams,
  events, and third-party CUDA-library dependencies.
- PyTorch may be an input project's integration environment, but AlloyPort does not implement a
  general PyTorch backend for new accelerators.

## Pipeline

```text
CUDA Intake -> Semantic Analysis -> MigrationSpec -> Ascend C Generation
            -> Compile -> Differential Verification -> Optimize -> Release
```

All accepted generation routes must produce Ascend C. Translators, templates, libraries, and agents
may help produce candidates, but only independent gates can approve them.

## Run experience

AlloyPort is designed as an observable coding agent, not a silent batch translator. Interactive
runs stream agent updates, exact command lifecycles, applied source diffs, approvals, gate verdicts,
and evidence links. The same typed event stream drives the terminal UI, plain-text output, JSONL
automation, and replay; an agent's narrative remains distinct from verified migration evidence.

## Workspace

- `alloyport-artifacts`: content-addressed artifact interfaces, the crash-recoverable filesystem
  SHA-256 store, durable typed references, quotas, and conservative garbage collection.
- `alloyport-core`: dependency-light domain model and lifecycle invariants.
- `alloyport-events`: versioned producer/canonical events, lifecycle reduction, JSONL, and plain
  rendering shared with the Python executor bridge.
- `alloyport-proto`: versioned worker-control and Artifact Protobuf/gRPC protocols plus strict edge
  validation.
- `alloyport-server`: worker connection registry, SQLite control repository, attempt leases, and gRPC
  control service, plus the mTLS-authorized Artifact service and filesystem CAS.
- `alloyport-worker`: outbound worker client, local assignment admission, reconnect/heartbeat, and a
  typed deterministic fake executor with worker-local Artifact spooling.
- `alloyport-cli`: command-line entry point; orchestration will grow here only until service
  boundaries become justified by measurements.
- `docs/design/`: numbered architecture decisions, including the trust boundary, verification
  gates, and long-horizon runtime.

## Development

Rust 1.88 or newer is required.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p alloyport-cli -- about
cargo run -p alloyport-cli -- event-demo
# Python producer JSONL -> validated Rust rendering:
python3 /data/projects/ascend-factory/harness/test_event_protocol.py --fixture \
  | cargo run -q -p alloyport-cli -- render-events
```

The first worker-control slice can be exercised locally without device hardware:

```bash
# terminal 1; plaintext is intentionally restricted to loopback
cargo run -p alloyport-server

# terminal 2
ALLOYPORT_WORKER_ID=cuda-dev \
ALLOYPORT_BACKEND=cuda \
cargo run -p alloyport-worker
```

Remote server mode requires `ALLOYPORT_TLS_CERT`, `ALLOYPORT_TLS_KEY`, and
`ALLOYPORT_TLS_CLIENT_CA`. A remote worker requires its certificate/key plus
`ALLOYPORT_TLS_SERVER_CA` and `ALLOYPORT_TLS_SERVER_NAME`; remote plaintext endpoints are rejected.
The server database is selected with `ALLOYPORT_DATABASE` and defaults to
`alloyport-control.sqlite3`; the worker journal uses `ALLOYPORT_WORKER_DATABASE` and defaults to
`alloyport-worker.sqlite3`. Artifact state is rooted at `ALLOYPORT_ARTIFACT_ROOT` (default
`alloyport-artifacts`); `ALLOYPORT_ARTIFACT_MAX_BYTES` and
`ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES` set positive byte limits;
`ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES` and `ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES` configure
transactional stored-plus-reserved quotas. Certificate enrollment state uses
`ALLOYPORT_IDENTITY_DATABASE` or defaults to `alloyport-artifacts/identities.sqlite3`. Enroll and
manage identities before starting a remote server:

```bash
cargo run -p alloyport-server -- identity enroll WORKER_ID client.pem
cargo run -p alloyport-server -- identity rotate WORKER_ID old.pem new.pem
cargo run -p alloyport-server -- identity revoke client.pem
```

Artifact RPCs require mTLS even when worker control uses permitted loopback plaintext. Remote worker
control binds the verified certificate to `WorkerHello.worker_id`; rotation preserves the stable
Artifact owner and revocation fails closed. The current slice proves registration, heartbeat,
durable assignment admission, server/worker restart reconciliation, finished-result replay,
cancellation ordering, server-side lease expiry, and enrolled artifact transfer. It does not yet
expose an external scheduling API or execute assignments on devices.

No license has been selected yet. Do not publish packages or redistribute the code until that
decision is recorded.
