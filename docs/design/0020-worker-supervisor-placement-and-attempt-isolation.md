# 0020: Worker supervisor placement and per-attempt isolation

- Status: Accepted
- Date: 2026-08-11
- Scope: worker deployment, trust boundaries, execution sandbox ownership, crash recovery, and
  acceptable containerized-agent variants

## Context

An accelerator worker needs a long-lived component that maintains the outbound control connection,
admits assignments against local policy, downloads immutable inputs, supervises execution, spools and
publishes Artifacts, and records enough durable state to reconcile after interruption.

There are two superficially similar deployment models:

1. a worker supervisor runs outside the workload sandbox and creates one isolated execution
   container for each attempt;
2. operations keeps one CUDA or Ascend container running, the worker agent lives inside it, and
   candidate programs execute as child processes in that same container.

Both models can provide a long-lived outbound agent. The material architectural question is not
whether the agent binary itself is packaged as a host service or an OCI image. It is whether the
trusted supervisor and the candidate native code share one failure, credential, filesystem, process,
and lifecycle boundary.

AlloyPort is expected to execute generated or migrated native accelerator code. Such code may be
incorrect, may leak processes or state, and must not be assumed safe merely because it came through
an authenticated controller. The worker holds credentials and durable records that candidate code
must not be able to read or modify.

## Decision

The worker supervisor remains outside every candidate execution sandbox. Each attempt receives a
separately identifiable, resource-bounded execution environment whose lifecycle the supervisor owns.

For the current CUDA vertical slice, this means a host worker daemon invokes the local Docker engine
to create and reconcile a per-attempt container. The attempt container receives no controller
credential, no worker journal, no Artifact publisher credential, no arbitrary host path, and no
network. It receives only worker-selected immutable inputs, bounded scratch storage, an enumerated
device, and the locally derived fixed command.

This is a logical placement decision, not a permanent requirement that the worker be installed as a
bare host process. Operations may package the worker supervisor in a long-lived container if all of
the following remain true:

- candidate code executes in a different sandbox with an independently controlled lifecycle;
- persistent journal, CAS, and spool state survive supervisor replacement;
- the supervisor reaches the executor through a deliberately constrained host service, CRI adapter,
  or equivalent broker;
- candidate code cannot access the supervisor's credentials, control socket, journal, or publisher;
- restart reconciliation can still identify and inspect an attempt independently of the supervisor
  container instance.

A Kubernetes deployment with a long-lived worker Pod and per-attempt Job or Pod can satisfy this
decision. A containerized worker with an unrestricted Docker socket is functionally host-privileged
and is not considered a security improvement by itself; it is acceptable only as an explicitly
trusted deployment mechanism while stronger broker isolation remains possible.

## Decision drivers

### Keep execution outside the credential boundary

The supervisor owns the worker mTLS identity, assignment journal, local CAS, Artifact upload
authority, admission policy, and terminal outbox. Candidate code must not be able to forge a
lifecycle observation, alter a receipt before publication, steal an identity, or mutate recovery
state.

Running candidate code as another Unix user inside the same container is not an equivalent boundary.
Native-code faults, inherited descriptors, permissive mounts, shared process namespaces, or a local
privilege escalation would still expose the trusted control plane. The per-attempt container is one
defense layer between these components, though it is not treated as proof of hostile-code isolation.

### Preserve attempts across supervisor failure

The execution object has a deterministic name and exact attempt, bundle, manifest, and resolved-image
labels. If the worker process stops, the external container runtime retains `Created`, `Running`, or
`Exited` state. A replacement worker process can inspect that object, validate its identity, reattach
to a running attempt, or recover an exited attempt without starting it again.

Terminal journal and outbox state is committed before cleanup. Therefore a cleanup failure causes a
later cleanup retry rather than a second execution. A supervisor-container restart must preserve this
property; merely persisting SQLite is insufficient if the running candidate process dies with that
same container.

### Bind environment identity per attempt

The trusted worker binary and the accelerator toolchain have different release cadences. Keeping them
separate allows each attempt to bind an immutable workload image manifest and resolved local image
ID without rebuilding or redeploying the worker. It also permits multiple pinned toolchain images and
historical receipt replay on one compatible worker.

A single resident CUDA/CANN agent image instead makes the operations deployment the implicit
execution environment. Mutable in-container caches, installed packages, entrypoint behavior, and
prior runs are then harder to distinguish from the declared attempt contract.

### Make resource and cleanup boundaries explicit

Per-attempt sandboxes allow the worker to apply CPU, memory, process, disk, output, network, mount,
and device policy to the complete process tree. Cancellation and timeout stop the identified
execution object rather than attempting to discover every descendant process.

A shared resident container would require a second isolation mechanism inside it—typically cgroups,
namespaces, a subreaper, process-tree accounting, and device reset policy. Granting enough privilege
to implement those mechanisms can erase the operational simplicity that motivated the resident
container and may expose the host through a Docker socket or privileged container.

### Limit cross-attempt contamination

Compilation and accelerator execution can leave child processes, shared-memory objects, temporary
files, locks, loaded libraries, caches, and unhealthy device state. Destroying an attempt sandbox
removes much of the process and filesystem residue. Device health and reset still require explicit
host policy and are not solved by OCI isolation alone.

## Consequences

Positive consequences:

- worker credentials and durable authority remain outside candidate code;
- a worker restart can reconcile a still-existing attempt instead of automatically losing it;
- each receipt can bind an independently pinned workload image and environment;
- cancellation, timeout, output exhaustion, and cleanup operate on one stable execution identity;
- state leakage between attempts is reduced;
- CUDA and Ascend executors can share the same control-plane contract while applying different local
  device policy.

Costs and risks:

- the supervisor needs access to a host executor, which is operationally more complex than one
  resident environment;
- Docker or equivalent runtime control is highly privileged; compromising the supervisor may
  compromise the execution host;
- per-attempt sandbox creation adds latency and image/cache management overhead;
- ordinary containers do not fully isolate hostile native code or accelerator-driver vulnerabilities;
- device reset, host health, runtime upgrades, and orphan cleanup remain explicit operator concerns;
- a production deployment may need a constrained executor broker, dedicated hosts, rootless
  facilities, or stronger isolation such as microVMs.

## Alternatives considered

### Execute every attempt directly inside one operations-managed agent container

This is simpler to deploy and can keep compiler and framework caches warm. It is a reasonable option
for trusted, homogeneous laboratory fixtures where losing the active task on agent restart is
acceptable.

It is not the default AlloyPort model because it combines controller credentials, durable lifecycle
authority, mutable toolchain state, and candidate native code in one boundary. Agent replacement can
also destroy an active attempt while leaving insufficient external state for exact reconciliation.

### Run the agent in a container with the host Docker socket

This preserves per-attempt child containers and can be operationally convenient. It does not provide
meaningful privilege isolation for the agent: an unrestricted Docker socket is effectively host-root
authority. This form is permitted as packaging for a trusted supervisor, but must not be described as
a security boundary. A narrow executor broker is preferred when deployment complexity permits.

### Run Docker-in-Docker inside the resident agent container

This restores a child-container boundary but adds a nested daemon, image store, storage driver,
device forwarding, logging, and recovery domain. Supervisor-container loss can also take the nested
daemon and its attempts with it. It is rejected for the initial worker because it makes recovery and
GPU operations more complex without improving the current evidence contract.

### Maintain a pool of prewarmed attempt sandboxes

This can reduce startup latency while keeping the supervisor outside candidate execution. It remains
compatible with this decision if every lease validates image and policy identity, starts from a
defined clean state, and performs fail-closed teardown or reset before reuse. It should be considered
only after measurements show sandbox startup is material.

## Reconsideration criteria

The direct resident-container model may be reconsidered for a specific worker pool when all of these
conditions are documented and tested:

- workloads are trusted and constrained to a fixed, homogeneous fixture family;
- agent restart is allowed to interrupt the active attempt;
- no controller, Artifact, or signing credential is reachable by candidate processes;
- process-tree, resource, filesystem, and device reset semantics are independently enforced;
- cross-attempt contamination tests pass;
- the latency or operational benefit is measured and outweighs the reduced recovery and isolation;
- receipts explicitly identify the resident environment revision and its mutable-state policy.

Changing one pool under these constraints does not change the default trust model for generated
CUDA or Ascend code.

## Invariants

- Candidate execution never receives the worker's control-plane or Artifact credentials.
- A worker cannot treat its own narrative or logs as a verified result.
- Each attempt has a stable identity that survives a supervisor process restart.
- The supervisor validates local image, mount, command, resource, and device policy independently of
  the controller request.
- Terminal state and immutable output publication precede destructive cleanup.
- Failure to clean an attempt never causes automatic re-execution of an already terminal attempt.
- Containerization is one reproducibility and defense layer, not proof of hostile-code isolation.
- Packaging the supervisor as a container must not collapse the supervisor/workload boundary.

## Verification implications

Executor tests must continue to cover supervisor restart during `Created`, `Running`, `Exited`,
publication, terminal commit, and cleanup. They must prove exact identity checking before reattach,
no duplicate execution after terminal commit, complete process-tree termination on cancellation and
limits, credential absence inside the attempt sandbox, and cross-attempt filesystem/process cleanup.

Any future containerized-supervisor deployment needs an integration test that replaces the
supervisor container while an attempt remains alive, then proves that a new supervisor instance can
reconcile and finish that same attempt from persistent state.
