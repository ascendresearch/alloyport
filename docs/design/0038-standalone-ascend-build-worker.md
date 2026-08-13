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
