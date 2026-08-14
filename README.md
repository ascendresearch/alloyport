# AlloyPort

AlloyPort is a verified CUDA-to-Ascend-C source migration and optimization factory.

For a new development session, start with [`docs/NEXT_SESSION.md`](docs/NEXT_SESSION.md).
[`docs/HANDOFF.md`](docs/HANDOFF.md) is the accumulated architecture record behind it.

The completed architecture remediation baseline is tracked in
[`docs/ARCHITECTURE_REMEDIATION.md`](docs/ARCHITECTURE_REMEDIATION.md).
The infrastructure evolution baseline is tracked in
[`docs/ARCHITECTURE_EVOLUTION_PLAN.md`](docs/ARCHITECTURE_EVOLUTION_PLAN.md). The active product work
is tracked in [`docs/PRODUCT_EXECUTION_PLAN.md`](docs/PRODUCT_EXECUTION_PLAN.md).

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

The runtime model is replaceable and currently defaults by configuration to `deepseek-v4-pro`.
AlloyPort owns an iterative, tool-using Agent Episode; model text and candidate proposals remain
untrusted while independent Gates own evidence and release state. The complete provider/runtime
design is [Design 0025](docs/design/0025-pluggable-llm-provider-architecture.md), accepted on
2026-08-12 and now being implemented in its documented order. Its first five slices have landed:
provider-neutral records, a durable Agent Episode loop, independent protocol codecs, strict model
configuration, bounded Tokio-native HTTPS transport, and the first real candidate-submission / Source
Gate correction loop. The `alloyport-llm-provider` SDK composes model connectivity behind the async
`ModelGateway`; independent candidate tools retain materialization and Gate authority. No live
provider call is part of the test suite.

## Run experience

AlloyPort is designed as an observable coding agent, not a silent batch translator. Interactive
runs stream agent updates, exact command lifecycles, applied source diffs, approvals, gate verdicts,
and evidence links. The same typed event stream drives the terminal UI, plain-text output, JSONL
automation, and replay; an agent's narrative remains distinct from verified migration evidence.

## Workspace

- `alloyport-artifacts`: content-addressed artifact interfaces, contract-tested filesystem and
  explicitly non-durable memory adapters, durable typed references, quotas, and conservative
  garbage collection.
- `alloyport-core`: dependency-light domain model and lifecycle invariants.
- `alloyport-candidate-tools`: create-only candidate materialization plus independently authored
  structural Source Gate receipts for the current migration slice.
- `alloyport-llm-provider`: provider-neutral SDK and Agent-loop gateway composition.
- `alloyport-model-http`: bounded `reqwest`/`rustls` transport adapter with no redirects, proxy,
  decompression, or internal retry.
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
cargo run -p alloyport-cli -- inspect-migration \
  fixtures/migrations/cuda-reduction-v1/migration-spec-v1.json \
  fixtures/migrations/cuda-reduction-v1
cargo run -p alloyport-cli -- event-demo
# Python producer JSONL -> validated Rust rendering:
python3 /data/projects/ascend-factory/harness/test_event_protocol.py --fixture \
  | cargo run -q -p alloyport-cli -- render-events
```

Provider calls are explicit and potentially billable; no provider call occurs during normal tests
or intake inspection. The removed one-shot candidate command has not been replaced by another
one-shot path: the supported composition boundary is the iterative Agent Episode plus
`alloyport-llm-provider`. The checked-in
[`runtime-model-catalog.example.json`](docs/runtime-model-catalog.example.json) is deliberately
non-routable and contains no credential.

For glibc-independent x86-64 deployment artifacts, use the checked
[portable Linux build](docs/portable-linux-builds.md). CI builds the static server and worker with
Rust 1.88.0 and verifies that neither result is dynamically linked.

Bootstrap the persistent server and each GPU or NPU worker, then submit a migration:

```bash
# one-time deployment bootstrap: state directories, PKI, identities, and config templates
alloyport-server bootstrap ./alloyport-deployment

# terminal 1
alloyport-server --config ./alloyport-deployment/server.json

# on a CUDA executor, after copying its enrollment bundle from the server host
alloyport-worker bootstrap cuda-correctness \
  ./cuda-correctness-worker-1 ./alloyport-worker https://SERVER_ADDRESS:50051
# replace the image/hardware placeholders reported by bootstrap, then keep the agent running
alloyport-worker --config ./alloyport-worker/worker.json

# on an Ascend executor, one agent serves Build and Correctness sequentially on one NPU
alloyport-worker bootstrap ascend-candidate \
  ./ascend-worker-1 ./alloyport-worker https://SERVER_ADDRESS:50051
alloyport-worker --config ./alloyport-worker/worker.json

# from the server host
alloyport-cli --config ./alloyport-deployment/clients/admin/client.json server status
alloyport-cli --config ./alloyport-deployment/clients/admin/client.json workers
alloyport-cli --config ./alloyport-deployment/clients/admin/client.json \
  migrate /absolute/path/to/cuda/project
alloyport-cli --config ./alloyport-deployment/clients/admin/client.json attach TASK_ID
```

For a TLS deployment, start the generated daemon with
`alloyport-server --config ./alloyport-deployment/server.json` and query it with
`alloyport-cli --config ./alloyport-deployment/clients/admin/client.json server status`. Bootstrap is
create-only and refuses a nonempty destination; it never overwrites certificates or state.

`alloyport-cli` is the user-facing client; it does not own the daemon or worker lifecycle. Set
`ALLOYPORT_SERVER_ENDPOINT` only when the server is not using the local default. `migrate` skips
`.git`, `target`, `build`, and `.cache`, uploads the remaining bounded regular files into the
server's content-addressed store, and creates a durable `captured` task. Use `status TASK_ID`,
`runs`, or `cancel TASK_ID` from any later CLI process. `attach TASK_ID` replays canonical events
and then follows the live run stream; exiting that view does not cancel the task. When the Server
configuration enables `migration_runtime`, its dispatcher claims captured tasks, materializes the
CAS project under the configured persistent runtime root, and drives the existing Candidate Episode.

The server can use its zero-dependency loopback defaults or one strict schema-1 JSON configuration.
`--config PATH` takes locator precedence over `ALLOYPORT_SERVER_CONFIG`; individual environment
values override file values and then defaults. File paths are relative to the configuration file.
See [server configuration](docs/server-configuration.md) and the checked-in
[example](docs/server-config.example.json).

The worker's single JSON configuration carries its server connection, TLS paths, worker ID, journal,
backend environment, local image identity, device-selection policy, and execution limits. Loopback
HTTP may omit TLS; remote workers must provide the TLS block and remote plaintext endpoints are
rejected. See [worker configuration](docs/worker-configuration.md) and the checked-in
[CUDA fixture](docs/cuda-worker-config.example.json),
[Ascend fixture](docs/ascend-worker-config.example.json),
[CUDA correctness](docs/cuda-correctness-worker-config.example.json), and
[Ascend correctness](docs/ascend-correctness-worker-config.example.json) examples.

Remote server mode requires a complete JSON `tls` block or `ALLOYPORT_TLS_CERT`,
`ALLOYPORT_TLS_KEY`, and `ALLOYPORT_TLS_CLIENT_CA`.
The server database is selected with `ALLOYPORT_DATABASE` and defaults to
`alloyport-control.sqlite3`; the worker journal path is explicit in `worker.journal`. Artifact state
is rooted at `ALLOYPORT_ARTIFACT_ROOT` (default
`alloyport-artifacts`); `ALLOYPORT_ARTIFACT_MAX_BYTES` and
`ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES` set positive byte limits;
`ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES` and `ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES` configure
transactional stored-plus-reserved quotas. Certificate enrollment state uses
`ALLOYPORT_IDENTITY_DATABASE` or defaults to `alloyport-artifacts/identities.sqlite3`. Enroll and
manage identities before starting a remote server. `ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS` sets the
positive bounded drain window and defaults to 10 seconds:

```bash
cargo run -p alloyport-server -- identity enroll WORKER_ID client.pem
cargo run -p alloyport-server -- identity rotate WORKER_ID old.pem new.pem
cargo run -p alloyport-server -- identity revoke client.pem
```

Artifact and interaction RPCs require mTLS even when worker control uses permitted loopback
plaintext. Remote worker control binds the verified certificate to `WorkerHello.worker_id`; rotation
preserves stable Artifact and run ownership, while revocation fails closed. Control, Artifact, and
Interaction share one authenticated request context but keep authorization at each service edge;
long-lived Control and Interaction streams revalidate during delivery, and Artifact upload
revalidates before every committed chunk. Internal gRPC message sizes are explicit protocol
contracts; oversized backend previews are split into bounded control frames, while large durable
results remain Artifacts. The current slices prove
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
deployments may instead use a manifest-pinned reference. No external task-submission API is attached
yet.

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
