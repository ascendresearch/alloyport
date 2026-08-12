# 0021: Fixed Ascend worker execution contract

> Design 0022 revises this document's registry-only image identity and environment-variable
> bootstrap. A standalone local Docker image ID is an accepted immutable identity, and server/TLS,
> worker identity, journal, and backend policy now live in one worker configuration file.

- Status: Accepted; fixed runtime and direct device gate implemented, outbound gate pending
- Date: 2026-08-11
- Scope: fixed Ascend executor identity, device inventory and telemetry, local device-node policy,
  and worker-durable device leases

## Context

The fixed CUDA path proves outbound assignment delivery, immutable input download, supervised
container execution, terminal Artifact publication, and restart cleanup. Ascend cannot be added by
renaming that executor. Its runtime discovers devices through manager nodes, its kernel-coupled
driver is mounted from the host, and the development machine is a shared pool whose visible device
count is not its schedulable capacity.

A read-only probe of the development worker on 2026-08-11 found seven Ascend 950PR devices exposed
as `davinci0` through `davinci6`, plus `davinci_manager` and `hisi_hdc`. The host driver reports
`25.7.rc1.6`; the pinned development image contains CANN `9.1.0-beta.1`. At the same instant some
devices had other users' processes, one reported 99 percent utilization, and three reported `Alarm` even
when utilization was zero. Consequently, device count, occupancy, utilization, and health are four
different facts.

The legacy Python harness knows how to reach this machine through SSH and assemble a Docker command.
That remains useful for a separately recorded parity attempt, but its host, key, remote path, shell,
and SCP behavior are not part of the AlloyPort execution contract.

## Decision

Protocol minor 4 adds `EXECUTOR_KIND_ASCEND_FIXTURE`. It is a dedicated, default-deny executor kind,
not generic `Container` or `Shell`. The first policy name is `ascend-add-v1`.

Static device identity and dynamic scheduling state are distinct:

- `WorkerCapabilities.devices` reports an explicitly enumerated identity for each advertised device:
  worker-local device ID, product, serial number, and firmware version.
- each heartbeat may report per-device process count, utilization, HBM use, temperature,
  power, observation time, and bounded detail;
- each heartbeat separately reports worker-durable attempt/device leases;
- unknown health is not treated as ready, zero utilization does not prove idleness, and a visible
  device without an explicit identity is not eligible for the fixed executor.

The server validates shape, enum values, uniqueness, bounds, and lease identity at the RPC edge. This
first slice transports the observations but does not yet persist telemetry history or make scheduler
choices from it.

## Local admission policy

`AscendFixturePolicy` is worker-owned and binds:

- the one fixture ID and exact bundle digest;
- an OCI manifest digest, digest-pinned image reference, and expected local image filesystem ID;
- exact CANN, host-driver, and firmware observations;
- one selected device identity;
- a complete enumerated device-node allowlist;
- the fixed read-only `/usr/local/Ascend/driver` mount;
- one worker-owned sandbox root and resource ceilings.

The allowlist must exactly match the host's startup-enumerated `/dev/davinciN`,
`/dev/davinci_manager`, and `/dev/hisi_hdc` character-device set. The runtime discovers the fleet
through the manager before `ASCEND_RT_VISIBLE_DEVICES` restricts execution to the configured device.
No assignment can add or select a host node, mount, environment entry, Docker option, or shell
command.

An accepted fixed assignment requires exactly `ascend-fixture-v1`, one device, disabled networking,
an empty server-authored environment, fixture-root working directory, and bounded nonzero resources.
The worker derives the container environment, including `ASCEND_RT_VISIBLE_DEVICES`, from local policy.
The derived plan mounts the driver read-only, passes only enumerated device nodes, drops every
capability before adding back only the empirically required `DAC_OVERRIDE`, uses
`no-new-privileges`, disables networking, and contains no shell.

## Durable device lease

The worker journal owns an independent `DeviceLeaseStore` capability. Acquiring a lease:

1. requires an existing non-terminal durable attempt;
2. atomically prevents another attempt from holding the same local device;
3. is idempotent for the same attempt/device pair;
4. rejects changing an attempt to a different device;
5. survives process and stream restart.

Release is explicit and idempotent. Terminal journal state does not implicitly release an Ascend
device: after a timeout, crash, fatal runtime result, or worker restart, the backend must retain the
lease until its cleanup hook has inspected health and performed the locally authorized reset or
quarantine action. Active leases are reported in heartbeats during that interval.

The control-plane attempt lease and the worker device lease are intentionally different. The former
governs whether the controller may reassign distributed work; the latter prevents unsafe local device
reuse. Neither constitutes correctness evidence.

## Evidence boundary

The independent Ascend receipt binds the accepted assignment, bundle/source, image, CANN, driver,
firmware, exact device, pre/post device observations, lease, outputs, and reset/quarantine annotations.
It remains independent from the CUDA reference receipt. A controller-owned experiment may join both
receipts for an oracle, but an Ascend worker never emits a correctness verdict, gate transition, or
release decision.

## Implemented follow-up

The implemented fixed runtime includes:

- a bounded local `npu-smi` adapter using an absolute binary, argv-only invocation, a five-second
  timeout, a one-MiB retained-output ceiling, explicit static inventory, and dynamic observation;
- a parser regression fixture captured from the pinned development driver. It fails closed when the
  static and dynamic inventories differ or a complete device observation is absent;
- a dedicated `AscendContainerEngine` port and durable supervisor for exact image/attempt identity,
  `Created`/`Running`/`Exited` recovery, cancellation, timeout, output exhaustion, and verification-
  marker classification;
- transport-neutral container value objects shared with the CUDA supervisor while keeping CUDA and
  Ascend create plans and engine capabilities separate;
- Design 0022's backend-neutral `DeviceGuard`, which acquires the durable lease before preflight,
  is shared with CUDA. It releases a healthy but
  already occupied device before any candidate starts, retains unknown/unhealthy state as
  quarantine, never resets while an unattributed process is visible, and releases after terminal
  execution only when health is `Ready` with zero processes before or after an authorized recovery.
- immutable preflight evidence written to a separate SQLite table before `Running`; a recovered
  `Running` attempt reuses those exact facts and never relabels a post-execution probe as preflight;
- digest- and size-verified `ascend-add-v1` bundle materialization into a write-once attempt sandbox;
- the shared argv-only Docker CLI adapter behind the distinct Ascend engine port;
- a composed Ascend runtime/backend that records stdout, stderr, source/image/environment identity,
  exact pre/post observations, lease, and cleanup intent in an independent receipt, publishes all
  terminal Artifact references before terminal journal commit, and retries cleanup without rerun;
- default-deny production-binary composition through Design 0022's unified schema-validated worker
  configuration, exact capability/device/inventory/node checks, verified input download, mandatory
  Artifact upload, and dynamic heartbeat status.
- a trusted image harness with fixed host/tiling code, deterministic 16,384-element `float32` input,
  ASC compilation for `dav-3510`, exact element verification, and canonical PASS output. Its
  size-bounded tmpfs owns compiler temporaries and CANN logs. A 950PR direct gate established the
  minimum runtime capability as `DAC_OVERRIDE` after dropping all others; `no-new-privileges`,
  read-only root/driver/source, and disabled networking remain enforced.

The current `NpuSmi` implementation deliberately reports reset as unsupported. This makes cleanup
retain the lease instead of pretending an unhealthy card is reusable.

## Deliberate limits

- The trusted harness has been built from a RepoDigest-pinned CANN base and passed a direct
  real-device gate. Its exact local Docker image ID now satisfies Design 0022's standalone immutable
  identity mode; registry publication remains optional. The direct gate is diagnostic evidence, not
  an AlloyPort receipt, so the composed outbound gate still must run.
- No reset command is authorized and no automatic multi-device scheduling decision is made. The
  guard's reuse rule is intentionally only `Ready` plus zero visible processes.
- Telemetry history remains ephemeral on the control stream; attempt-relevant pre/post observations
  and immutable preflight evidence are durable.
- The checked development environment is evidence for the initial policy shape, not a portable claim
  that every Ascend host uses seven devices or the same nodes and versions.

## Rejected alternatives

- Reuse the CUDA fixture kind: rejected because CANN/driver identity, manager-node discovery, health,
  reset, and device leasing have different safety semantics.
- Let the server send `/dev` paths or driver mounts: rejected because the worker remains the local
  host-policy authority.
- Derive availability from `device_count` or utilization alone: rejected by the observed shared-host
  state and by the distinction between process occupancy and hardware health. A probe cannot call a
  process foreign until local lease/PID attribution establishes that fact.
- Release the device when `ExecutionFinished` is committed: rejected because fatal outcomes may leave
  a device unsafe for immediate reuse.
- Put SSH credentials in worker configuration: rejected because SSH is an operational/parity path,
  not the product transport.

## Verification

Automated tests cover protocol numeric compatibility, static identity and dynamic heartbeat
validation, fixed-policy assignment rejection, exact derived mounts/devices/environment without a
shell, bounded `npu-smi` discovery/observation parsing, device-lease acquisition conflicts,
idempotent release and SQLite reopen, immutable preflight replay, bundle materialization, exact Docker
argv, fake-engine restart/reattach/timeout/cancellation/output recovery, Artifact publication and
terminal-cleanup failure ordering, independent receipt contents, backend/binary composition, and
quarantine/recovery ordering. The remaining acceptance step is an explicit real-device outbound gate
and a separately receipted legacy parity attempt using the same source and declared environment.

Huawei's container guidance independently identifies the manager/HDC/device-node and read-only host
driver mounts used here; some product families also expose `devmm_svm`, which this 950PR host does not
provide and the exact startup inventory therefore does not invent:

- <https://www.hiascend.com/document/detail/en/mindie/230/quickstart/mindie_quickstart_0003.html>
- <https://www.hiascend.com/document/detail/en/mindx-dl/300/dluserguide/toolboxug/toolboxug_0008.html>
