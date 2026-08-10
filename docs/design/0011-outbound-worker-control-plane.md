# 0011: Outbound worker control plane

- Status: Accepted
- Date: 2026-08-10
- Scope: server/worker topology, Protobuf and gRPC transport, delivery semantics, artifacts, identity,
  security, and migration away from SSH

## Context

The bootstrap harness reaches CUDA and Ascend hosts through SSH. `worker.py` opens commands over SSH,
copies a content-addressed bundle with SCP, and starts a container remotely. `box.py` maintains a
persistent SSH stdio channel carrying JSONL commands. These paths solved early quoting, stale-file,
and output-corruption problems, but they make every controller instance responsible for host
addresses, SSH keys, reachability, upload mechanics, reconnect behavior, and remote shell policy.

The desired product is a service. CUDA and NPU machines may sit behind NAT, change addresses, be
temporarily unavailable, or join a shared device pool. AlloyPort must schedule them without opening
an inbound remote-code-execution port on each worker. Long-running attempts must survive transient
connection loss without silently duplicating work or losing their evidence.

Design 0007 already defines workers as replaceable execution surfaces outside the decision trust
boundary. Design 0010 defines the user-visible interaction stream. This document chooses the
transport and distributed state machine between that control plane and the workers; it does not
weaken either earlier boundary.

## Decision

AlloyPort becomes the server-side control plane. A Rust `alloyport-worker` daemon runs on each CUDA
or Ascend execution host and initiates a long-lived, mutually authenticated connection to the
server. The server sends typed assignments over that connection; the worker executes them under its
local policy and returns status, bounded live output, artifact references, and a signed or
authenticated run receipt.

The initial transport is a bidirectional gRPC stream over HTTP/2 with Protocol Buffers:

```proto
package alloyport.worker.v1;

service WorkerControl {
  rpc OpenControlStream(stream WorkerToServer) returns (stream ServerToWorker);
}

message WorkerToServer {
  uint64 sequence = 1;
  uint64 acknowledges_server_through = 2;
  string message_id = 3;
  oneof message {
    WorkerHello hello = 10;
    Heartbeat heartbeat = 11;
    AssignmentAccepted assignment_accepted = 12;
    AssignmentRejected assignment_rejected = 13;
    ExecutionStarted execution_started = 14;
    OutputChunk output_chunk = 15;
    ExecutionFinished execution_finished = 16;
    WorkerStatus status = 17;
  }
}

message ServerToWorker {
  uint64 sequence = 1;
  uint64 acknowledges_worker_through = 2;
  string message_id = 3;
  oneof message {
    ServerWelcome welcome = 10;
    Assignment assignment = 11;
    CancelAttempt cancel = 12;
    DrainWorker drain = 13;
    ControlAcknowledgement acknowledgement = 14;
  }
}
```

The sketch fixes the service shape, not final field names. The published schema will use the package
`alloyport.worker.v1`, split major message types into small files, give every enum an `UNSPECIFIED =
0` value, and reserve deleted field names and numbers. Server and worker negotiate protocol versions
and feature bits during `hello`/`welcome`; deployments cannot assume both sides update atomically.

The Rust implementation uses `tonic` for gRPC and `prost` for generated Protobuf types. The workspace
MSRV is raised from Rust 1.85 to 1.88 to match `tonic` 0.14.6. `protoc-bin-vendored` 3.2.0 supplies the
compiler to `prost-build`, so code generation does not depend on an arbitrary host installation.

## Implementation progress

The contract, durable repository, and assignment-level reconciliation slices were implemented on
2026-08-10:

- `alloyport-proto` publishes the `alloyport.worker.v1` schema, generated client/server bindings, and
  strict hello/assignment validation;
- `alloyport-server` accepts an outbound bidirectional stream and separates live connection sessions
  from a storage-domain `ControlRepository`; its SQLite implementation migrates and transactionally
  stores worker registrations, connection observations, immutable assignments, lifecycle
  observations, and attempt leases without storing generated Protobuf messages;
- the server commits assignments before send, grants/renews bounded leases, periodically expires
  them using an injectable clock, recovers queued and non-terminal assignments after process restart,
  and retains a result arriving after lease expiry as stale instead of overwriting attempt state;
- `alloyport-worker` uses a storage-domain `AttemptStore` and SQLite journal to commit immutable
  admission before acknowledgement, retain accepted/running/finished state across process restart,
  populate hello/heartbeat reconciliation snapshots, and replay durable finished results;
- both stream directions reject cumulative acknowledgements that regress or exceed the sequence
  actually sent; cancellation is durably requested, independently acknowledged, eventually terminal,
  replayed after reconnect, and tested against admission and lease-expiry races;
- the server direction records assignment/cancel frame references before send and compacts them only
  after a valid cumulative acknowledgement and successful domain-message processing;
- durable control messages now carry stable logical message IDs distinct from connection-local
  sequence numbers; the worker persists lifecycle messages and delivery mappings before send,
  replays pending logical messages with fresh sequences on reconnect, and compacts only after the
  server emits an explicit cumulative acknowledgement;
- obsolete per-connection delivery mappings have a seven-day retention policy without deleting
  unacknowledged worker logical messages, and disconnected server frame mappings follow the same
  retention window while replay remains driven by durable assignment/cancellation records;
- an explicit transactional operation reassigns only lease-expired work into a fresh attempt ID,
  increments the attempt number, and preserves the old record and late-result classification;
- runnable server and worker binaries support mTLS from environment-provided certificates and permit
  plaintext only on loopback for development;
- loopback and repository tests cover handshake, assignment delivery, worker acceptance,
  store-before-send lease creation, duplicate/conflicting enqueue handling, disconnected replay,
  SQLite reopen/restart recovery on both sides, heartbeat renewal, lease expiry, late-result and
  finished-result replay, acknowledgement bounds, and cancellation races without CUDA or Ascend
  hardware.

This is not the complete control plane. Replacement-worker selection and automatic invocation of
reassignment are not implemented, ephemeral heartbeat/output-preview traffic is intentionally not
durable, execution and running-process signal delivery are not wired to a container, and the
artifact service is not implemented. Those omissions keep the implementation at Stage 1 rather than
claiming production readiness.

## Product topology

```text
                       user / CLI / future UI
                                  |
                                  v
                  +--------------------------------+
                  | AlloyPort server               |
                  | API, long-horizon controller,  |
                  | scheduler, canonical events,   |
                  | receipts and artifact metadata |
                  +------------+-------------------+
                               ^
                   outbound mTLS gRPC connections
                         /                 \
                        /                   \
          +------------+------+       +-----+-------------+
          | CUDA worker client |       | Ascend worker     |
          | policy + executor  |       | client + executor |
          +---------+----------+       +---------+---------+
                    |                            |
             isolated container          isolated container
                    |                            |
                  CUDA                       Ascend C
```

Workers make outbound connections and expose no AlloyPort listening port. The first implementation
uses one server instance and one logical pool. Later server replicas may share a durable worker and
lease registry; a load balancer must not split a live stream from the scheduler state that owns it.

SSH and SCP are removed from the product execution path. SSH may remain as a separately authorized
break-glass administration mechanism for installing, inspecting, or repairing a worker host. A run
performed manually through SSH is not an AlloyPort run unless its artifacts and receipt enter through
the same evidence gates.

## Three protocols, not one object model

AlloyPort has three related but deliberately separate representations:

1. The worker RPC protocol carries live control messages across a partially reliable connection.
2. The interaction event protocol from Design 0010 records what users can observe and replay.
3. Durable task, receipt, and audit types record facts that may advance long-horizon state.

The server translates worker observations into canonical interaction events. For example,
`ExecutionStarted` becomes `command.started`, `OutputChunk` becomes `command.output`, and
`ExecutionFinished` closes the command and links its receipt and output artifacts. The server assigns
canonical per-run event order; a worker cannot mint authoritative UI sequence numbers or verified
gate results.

RPC messages are not stored wholesale as either UI events or audit records. Storage types and API
types evolve for different reasons, so explicit validation and translation layers are required.

## Worker registration and capability model

The first message on every connection is `WorkerHello`. It identifies:

- stable logical `worker_id` and short-lived process `instance_id`/boot identity;
- supported protocol versions and feature bits;
- worker binary and executor versions;
- backend (`CUDA` or `ASCEND`), device architecture, count, memory, and supported features;
- driver, CUDA or CANN/toolkit, container runtime, kernel, and image capabilities;
- labels, admission policy version, maximum concurrency, and artifact transport support;
- the last durably processed server sequence and any locally active attempts.

Static capability is registration data; dynamic availability is heartbeat data. Heartbeats report
health, device occupancy, active leases, spool pressure, and drain state. The scheduler matches an
assignment's required environment contract to both, and leases concrete devices for the attempt.
CUDA-reference and Ascend-target attempts can share an experiment identifier, but are scheduled and
receipted independently.

## Assignment contract

An assignment is a typed execution contract, not an unqualified shell string. It contains at least:

- immutable task, candidate, bundle, image, and environment references by digest;
- `assignment_id`, `attempt_id`, attempt number, and idempotency key;
- executor kind, argument vector, relative working directory, and sanitized environment entries;
- explicit timeout, CPU, memory, disk, process, output, network, and device budgets;
- logical read-only inputs, writable outputs, declared artifacts, and required measurements;
- server policy identity and the worker capabilities required to accept it.

The default command representation is an argument vector. An explicit shell executor may exist for
porting probes that genuinely need shell syntax, but it is a separate, policy-gated executor kind.
The worker resolves only logical paths inside a fresh sandbox; the server cannot choose host paths,
device nodes, Docker socket mounts, or arbitrary secrets.

Before acknowledging acceptance, the worker independently validates the assignment against its
local admission policy and records the attempt identity durably. A valid server certificate does not
override local safety policy. Candidate execution runs unprivileged wherever device-runtime
constraints permit and never receives control-plane credentials.

## Delivery, leases, and reconnect

The protocol promises at-least-once delivery, not exactly-once execution.

1. The server persists an assignment before sending it and grants a bounded attempt lease.
2. The worker durably records the `attempt_id`, then accepts or rejects it. Duplicate delivery of an
   accepted attempt returns its existing state instead of starting another process.
3. Heartbeats renew the lease and include the worker's view of active attempts.
4. A worker continues an accepted attempt across a transient stream failure when local policy permits,
   spooling output and the final receipt locally.
5. On reconnect, both sides exchange sequence acknowledgements and active-attempt snapshots, replay
   missing messages, and reconcile discrepancies.
6. The server may reassign work only after the lease expires or the old attempt is definitively
   terminal. A late result from an expired attempt is retained as stale evidence; it cannot overwrite
   the accepted attempt result.

Cancellation is a request with its own acknowledgement and eventual terminal outcome. A disconnected
server cannot claim that a process was cancelled. Server restart recovery reads durable assignments
and leases before accepting reconciliation from workers.

Sequence numbers are scoped to a connection direction and acknowledged cumulatively. Domain identity
comes from `assignment_id` and `attempt_id`, not stream position. This allows connection replacement
without confusing network replay with new work.

## Output and artifact plane

The control stream carries small messages and bounded live previews. It is not the bulk data plane.
Source bundles, build trees, complete stdout/stderr, binaries, profiles, traces, and receipts use a
content-addressed artifact service.

An `OutputChunk` identifies attempt, logical stream, byte offset, payload encoding, and suppression or
redaction metadata. The worker simultaneously spools the original permitted bytes to a local artifact.
Flow control may coalesce preview chunks, but it cannot discard terminal status or the full-output
artifact. After completion, the server can reconstruct the display from previews and retrieve exact
bytes by digest.

The first artifact implementation may use separate streaming upload/download RPCs. Large-deployment
support may later use an OCI-compatible store or presigned object-storage transfers without changing
assignment semantics. Credentials are narrowly scoped to the named digest and expire quickly.

Protobuf bytes are never used directly as a content digest or cache key: Protobuf does not guarantee
stable serialization across builds. Domain digests are computed from canonical files or a separately
specified canonical representation. Maps and other order-unstable structures are avoided in any
object whose semantic digest matters.

## Completion and receipts

`ExecutionFinished` distinguishes successful execution, candidate failure, timeout, cancellation,
infrastructure failure, and integrity violation. It links rather than embeds the full `RunReceipt`
from Design 0007. The receipt binds:

- exact accepted assignment and policy digests;
- bundle, source tree, container image, tools, driver, device, and environment identities;
- worker, process instance, attempt lease, start/end times, exit/signal, and resource observations;
- stdout/stderr, produced files, profiles, traces, and structured-result artifact digests;
- truncation, reconnect, cancellation, contention, reset, and integrity annotations.

Transport authentication proves which enrolled worker sent a message. It does not by itself prove
candidate correctness; only the oracle and audit gates can issue those verdicts.

## Security

- TLS authenticates the server and a distinct client certificate authenticates each worker. Bootstrap
  enrollment credentials only issue short-lived worker credentials; they are not permanent bearer
  tokens.
- Worker certificates are rotatable and revocable. Authorization is based on worker identity, pool,
  capabilities, and server policy, not merely successful TLS negotiation.
- The worker daemon has no database, model-provider, oracle, or canonical knowledge-store credential.
- Local admission rules constrain executor kinds, images, mounts, devices, network, limits, and secret
  exposure even if the server is compromised.
- Message sizes, metadata, output rates, concurrent attempts, and local spool growth are bounded.
- A container is a reproducibility boundary and one security layer, not proof of hostile-code
  isolation. Dedicated hosts or stronger sandboxing remain deployment options.
- Secrets and signed artifact URLs are excluded from user-visible events and durable general logs.

If an assignment crosses a queue or intermediary outside the authenticated stream's trust boundary,
the assignment envelope may additionally be signed. For the direct mTLS stream, a durable assignment
digest in the receipt is initially sufficient and avoids creating a second identity system.

## Rust component boundaries

The target workspace adds:

```text
alloyport-proto     generated RPC types plus strict conversion/validation
alloyport-server    connection registry, scheduler, leases, reconciliation, event translation
alloyport-worker    outbound client, local admission policy, executor, artifact spool
alloyport-artifacts content-addressed upload/download interfaces (may begin inside server/worker)
```

`alloyport-events` remains the interaction-domain crate. Generated Protobuf types do not leak into
the scheduler, receipt, or event domain models; conversion at the edges makes missing fields,
unsupported enum values, version skew, and policy violations explicit errors.

## Migration plan

### Stage 1: contract simulator

Define the versioned schemas and state machines. Test an in-memory server and fake workers for
duplicate assignment delivery, reordered acknowledgements, disconnects, server restart, lease expiry,
late results, cancellation races, output backpressure, and version skew. No device is required.

### Stage 2: one real worker vertical slice

Run a Rust worker daemon on one CUDA host. Reproduce the existing bundle-to-container operation through
gRPC and the artifact service. Emit the same command events and a Design 0007 receipt. SSH is used only
to install or inspect the daemon during development.

### Stage 3: Ascend worker and evidence parity

Add one Ascend host, device discovery, health/occupancy reporting, explicit device leasing, driver
mount policy, and post-failure health checks. Run fixed fixtures once through SSH and once through the
new plane in separate attempts, then compare their bundles, outputs, environment facts, and verdicts.
Do not shadow-run a side-effecting assignment simultaneously on both paths.

### Stage 4: control-plane cutover

Make the worker client the only scheduler execution path. Remove host addresses, SSH keys, remote
shell assembly, SCP staging, `worker.sh`, and `box.py` from runtime configuration. Retain a documented,
separately authorized operations path if deployment policy requires it.

### Stage 5: pool and resilience

Add multi-worker scheduling, draining, certificate rotation, durable server restart recovery, artifact
garbage collection, quotas, and rolling protocol upgrades. Scale the server only after single-instance
reconciliation invariants are proven.

## Invariants

- A worker never executes an assignment it has not durably identified and admitted locally.
- A retransmitted assignment cannot create a second process for the same attempt.
- Connection loss never becomes evidence of command success, failure, or cancellation.
- A late or duplicate result is retained and classified; it cannot silently replace accepted evidence.
- Full permitted output survives UI preview backpressure and is referenced by digest.
- No worker message can directly advance canonical long-horizon state or declare an oracle verdict.
- The server cannot name arbitrary worker-host paths or bypass the sandbox through a shell string.
- Protocol evolution preserves field-number compatibility and supports a rolling old/new deployment.
- SSH availability is not a prerequisite for normal scheduling or execution.

## Verification plan

- Property/state-machine tests cover reconnect and replay at every transition between assignment,
  acceptance, start, cancellation, and completion.
- Killing the stream, worker, server, container, and device runtime at deterministic points produces
  one classified outcome per attempt and no silent duplicate process.
- An older worker accepts additive compatible messages, rejects unsupported required features, and
  continues to reconnect during a rolling server upgrade.
- Forged worker identity, expired/revoked certificates, cross-pool assignment, invalid artifact digest,
  host-path injection, disallowed shell mode, oversized messages, and output flooding are rejected.
- CUDA and Ascend fixture runs expose live command events, exact terminal status, full-output artifacts,
  and receipts that pass the existing oracle/audit boundary.
- Network interruption longer than the display buffer but shorter than the attempt lease recovers all
  durable output and exactly one terminal receipt.
- A server restart reconciles active workers and does not make expired work authoritative.

## Rejected alternatives

### Continue using SSH as the product transport

SSH remains useful for administration, but it couples scheduling to host reachability, login policy,
keys, shell behavior, and file-copy mechanics. It lacks the explicit lease, capability, backpressure,
and reconnect state model needed by a device pool.

### Open a command server on every worker

Rejected because inbound reachability is harder behind NAT/firewalls and exposes an RCE-shaped service
on accelerator hosts. Outbound worker connections give the server the same scheduling channel with a
smaller deployment surface.

### Send raw shell commands in generic Protobuf fields

Rejected because binary framing fixes quoting but does not define authority. Typed executors,
resources, paths, and policies are necessary for validation and auditable receipts.

### Put bundles and complete logs on the control stream

Rejected because large transfers create head-of-line pressure, memory risk, reconnect ambiguity, and
poor retention behavior. The stream carries control and live preview; immutable bulk data has its own
content-addressed path.

### Promise exactly-once execution

Rejected because a network partition can make the server unable to know whether a remote process
started or finished. At-least-once delivery, durable attempt identity, leases, idempotency, and
reconciliation state the achievable guarantees honestly.

## References

- [gRPC core concepts](https://grpc.io/docs/what-is-grpc/core-concepts/) documents bidirectional
  streaming and per-call ordering.
- [gRPC authentication](https://grpc.io/docs/guides/auth/) documents TLS server authentication and
  optional mutual authentication with client certificates.
- [`tonic`](https://docs.rs/tonic/latest/tonic/) provides an asynchronous Rust gRPC implementation over
  HTTP/2.
- [`prost`](https://docs.rs/prost/latest/prost/) generates idiomatic Rust types from Protocol Buffer
  definitions.
- [Protocol Buffers best practices](https://protobuf.dev/best-practices/dos-donts/) requires safe tag
  evolution, warns that clients and servers do not update atomically, recommends separate RPC and
  storage messages, and forbids relying on serialization stability for cache keys.
