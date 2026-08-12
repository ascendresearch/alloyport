# AlloyPort

AlloyPort is a verified CUDA-to-Ascend-C source migration and optimization factory.

For a new development session, start with [`docs/HANDOFF.md`](docs/HANDOFF.md).

The completed architecture remediation baseline is tracked in
[`docs/ARCHITECTURE_REMEDIATION.md`](docs/ARCHITECTURE_REMEDIATION.md).

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
- `alloyport-proto`: versioned worker-control, Artifact, and interaction Protobuf/gRPC protocols plus
  strict edge validation.
- `alloyport-server`: worker connection registry, SQLite control repository, attempt leases, and gRPC
  control service, plus mTLS-authorized Artifact and canonical interaction services and the
  filesystem CAS.
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

Start the server locally, then run a configured GPU or NPU worker:

```bash
# terminal 1; plaintext is intentionally restricted to loopback
cargo run -p alloyport-server

# terminal 2; first copy the relevant example and replace every placeholder
cargo run -p alloyport-worker -- --config /absolute/path/to/worker.json
```

The worker's single JSON configuration carries its server connection, TLS paths, worker ID, journal,
backend environment, local image identity, device-selection policy, and execution limits. Loopback
HTTP may omit TLS; remote workers must provide the TLS block and remote plaintext endpoints are
rejected. See [worker configuration](docs/worker-configuration.md) and the checked-in
[CUDA](docs/cuda-worker-config.example.json) and
[Ascend](docs/ascend-worker-config.example.json) examples.

Remote server mode requires `ALLOYPORT_TLS_CERT`, `ALLOYPORT_TLS_KEY`, and
`ALLOYPORT_TLS_CLIENT_CA`.
The server database is selected with `ALLOYPORT_DATABASE` and defaults to
`alloyport-control.sqlite3`; the worker journal path is explicit in `worker.journal`. Artifact state
is rooted at `ALLOYPORT_ARTIFACT_ROOT` (default
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

Artifact and interaction RPCs require mTLS even when worker control uses permitted loopback
plaintext. Remote worker control binds the verified certificate to `WorkerHello.worker_id`; rotation
preserves stable Artifact and run ownership, while revocation fails closed. The current slices prove
registration, heartbeat, durable assignment admission, server/worker restart reconciliation,
finished-result replay, cancellation ordering, server-side lease expiry, enrolled Artifact transfer,
authorized event replay/subscription, and one fixed CUDA fixture on an explicitly configured Docker
worker. Protocol minor 4 and the worker now implement the fixed Ascend contract: static and dynamic
device facts, an exact startup-checked device-node policy, crash-durable leases and preflight
evidence, bounded shell-free `npu-smi`, verified bundle materialization, argv-only Docker
reconciliation, Artifact-gated independent receipts, and fail-closed post-terminal quarantine. The
production path remains default-deny and requires an explicit worker configuration. A pinned image
carrying the trusted `ascend-add-v1` harness now builds and passes a direct, least-capability 950PR
gate. Standalone trials may bind a local Docker tag to its exact inspected image ID; registry-backed
deployments may instead use a manifest-pinned reference. No external scheduling API is attached yet.

CUDA and Ascend share the same durable per-attempt device guard: lease before preflight, immutable
preflight evidence before `Running`, and lease release only after terminal container cleanup plus a
fresh `Ready`/process-free observation. NVIDIA health is fail-closed from the explicit
`gpu_recovery_action` result rather than inferred from successful `nvidia-smi` execution.
The selected device identity is registered in worker capabilities, and the shared heartbeat adapter
reports only that bound device even on a multi-accelerator host.

On 2026-08-11 the ignored real Ascend outbound gate passed on an Ascend950PR: the worker selected an
`OK`, process-free NPU, downloaded the verified bundle, ran the local image pinned to its exact image
ID, published terminal Artifacts and receipt, committed success, removed the container, and released
the durable lease after a fresh `Ready`/zero-process observation. The deterministic result was
`PASS fixture=ascend-add-v1 elements=16384 checksum=3d2cf971e11e0383`.

AlloyPort is open-source software licensed under the [MIT License](LICENSE).
