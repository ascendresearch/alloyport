# Design 0038: Standalone Ascend Build worker

- Status: Implemented
- Date: 2026-08-12
- Extends: Designs 0026, 0022, and 0037
- Scope: strict process configuration and production composition for the existing Build executor

## Context

The controller could dispatch a typed `AscendBuild` assignment and the worker crate already had a
policy-bound build materializer, fixed build runner, container supervisor, durable execution
runtime, and exclusive admission feature. The worker process configuration exposed only the fixed
Ascend fixture and correctness roles, so no production process could advertise `ascend-build-v1`.

## Decisions

The unified worker schema now accepts an `ascend_build` runtime variant. It has no fixture ID or
fixed bundle digest: the controller supplies the immutable Source-Gate-authorized candidate build
bundle. Local configuration retains exclusive authority over the exact OCI image identity, one
Ascend device and its complete node set, driver mount, CANN/driver/firmware facts, sandbox/CAS
roots, upload limits, Docker binary, and resource ceilings.

Production assembly repeats dynamic NPU inventory and occupancy checks, binds the configured device
identity, constructs `AscendBuildPolicy`, selects `AscendContainerSupervisor::new_build`, attaches
the normal durable Artifact downloader/publisher and device guard, and registers only the
`AscendBuild` executor through the existing exclusive admission path. A checked-in example and
strict parse tests cover the process boundary.

## Boundary

This change makes the role deployable but does not choose a host, create TLS identities, install an
image, or submit a candidate. Those remain explicit deployment/operator actions.

## Amendment, 2026-08-17: a build does not occupy an accelerator

- Status of this amendment: implemented for the contract and the container; the worker's advertised
  capacity is unchanged and is now the remaining gate.
- Evidence: [`ascend-build-nodevice-20260817.md`](../evidence/ascend-build-nodevice-20260817.md)

The build contract required `device_count == 1`, the worker leased a card per attempt, and the
container mounted every device node with `ASCEND_RT_VISIBLE_DEVICES` set. **The build runner is two
`cmake` calls.** It never opens an accelerator, and `fixtures/ascend-add-v1` — a kernel a person
wrote — compiles and links inside the pinned image with none attached.

What the lease bought was one fact: a card was free. The build receipt names no device, and the
architecture and firmware it attests are configuration, cross-checked against the configured device
at policy construction time rather than read from hardware at build time. That cross-check is kept;
it costs nothing and it is what binds the receipt's environment claim to something.

What the lease cost was the pipeline. On a shared host where every Ready card carried another user's
process, every build queued behind work it did not need, and on 2026-08-17 that blocked the day.

So: a build asks for `device_count == 0`, mounts no device node, sets no visible-device variable, and
takes no per-attempt lease. Correctness runs are untouched and still lease a real card, because they
execute.

### What this does not yet change, and the decision it needs

A worker advertises **one** capacity number, computed as
`min(max_concurrency, usable_devices) - active`, where `usable_devices` counts cards that are Ready,
process-free, and unleased. A build-capable worker therefore still advertises zero slots when every
card is busy, even though its builds need none. The combined `ascend-candidate` worker this
deployment runs is in exactly that state.

This design's own standalone build worker does not escape it either: `attach_ascend_build` binds a
device, requires `capabilities.device_count == 1`, and attaches a status provider, so its capacity is
clamped the same way.

Three ways out, and the choice is a real trade rather than an oversight:

1. **Let a build-only worker advertise unclamped capacity**, keyed on the `exclusive_executor ==
   AscendBuild` that already exists, and run the standalone build worker beside the correctness one.
   Faithful to this design; costs a second worker identity and enrollment.
2. **Advertise capacity per feature.** Honest for a combined worker and a protocol change.
3. **Stop clamping a combined worker's capacity by devices.** Cheapest and worst: a correctness
   assignment would then be dispatched with no free card and fail at attempt time, converting a queue
   into a failure.

Nothing here picks one.
