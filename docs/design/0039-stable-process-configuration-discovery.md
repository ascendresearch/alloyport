# 0039: Stable process configuration discovery

- Status: Implemented
- Date: 2026-08-12
- Scope: server and worker installation layout and configuration bootstrap
- Amends: only the configuration-locator portions of Designs 0022 and 0023

## Context

Explicit `--config` paths and service-manager environment variables are appropriate in managed
deployments, but the first hardware preparation copied binaries and configuration into `/tmp`.
That location is disposable, encourages process-working-directory coupling, and is not a credible
restart or host-recovery contract. A standalone binary installation should have an obvious stable
configuration location without weakening explicit operator control.

One Ascend host can also run multiple mutually exclusive worker roles. A single global default must
not force those roles to share identity, journal, image, device, or resource policy.

## Decision

The server locates its configuration in this order:

1. the explicit `--config PATH` argument;
2. `ALLOYPORT_SERVER_CONFIG`;
3. `alloyport-server.json` beside the running executable;
4. `/etc/alloyport-server/server.json`;
5. its existing loopback development defaults.

The worker uses the analogous first four levels with `ALLOYPORT_WORKER_CONFIG`,
`alloyport-worker.json`, and `/etc/alloyport-worker/worker.json`. It has no built-in configuration
and fails with an actionable locator error if none exists.

Explicit command-line and environment paths are authoritative even when missing or invalid: their
load errors do not trigger fallback. Implicit discovery selects only an existing regular file. The
process working directory and bare `server.json` or `worker.json` names are never searched.
Configuration contents retain the existing strict schema and validation contracts.

A host with one process role may use the system-wide path. A host with multiple worker roles should
use separate executable directories, each containing its own fixed sibling filename, or explicit
role-specific files selected by its service definitions. Thus discovery adds installation
convenience without collapsing role authority.

## Invariants and consequences

- CLI intent has precedence over service-manager configuration, which has precedence over installed
  defaults.
- Relocating a binary together with its sibling configuration preserves its bootstrap behavior.
- Starting from another working directory cannot change which file is selected.
- The server's no-file local-development behavior remains compatible.
- A worker never silently invents connection, identity, backend, image, device, or resource policy.
- Private keys remain referenced by path and should be separately permissioned; sibling discovery
  does not imply embedding secret material in a binary or repository.
- `/tmp` may still be used for bounded ephemeral sandbox data when explicitly configured, but not
  as the deployment configuration contract.

## Rejected alternatives

- Searching the working directory was rejected because service managers, shells, and offline admin
  commands commonly start with different working directories.
- Searching several generic names was rejected because it creates ambiguity and makes stale files
  unexpectedly authoritative.
- Giving `/etc` precedence over the binary sibling was rejected because self-contained role
  directories are necessary for multiple workers on one host.
- Supplying worker defaults was rejected because every worker field participates in a trust or
  resource boundary.

## Verification

Unit tests exercise exact explicit, environment, sibling, and system precedence plus server fallback
and worker failure when no file exists. Existing strict-schema and command tests continue to prove
that discovery changes only location, not configuration meaning. Deployment verification stages
each role in a persistent install directory and starts it without a command-line configuration
locator.
