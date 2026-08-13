# 0023: Versioned server configuration

- Status: Accepted; first implementation slice complete; locator amended by Design 0039
- Date: 2026-08-12
- Scope: server bootstrap, offline identity administration, and open-source local trials

## Context

The server process previously assembled one logical deployment from environment variables only.
That was service-manager friendly but difficult for an open-source evaluator to inspect, copy, and
reproduce. It also allowed the serving process and offline identity commands to derive their
identity database through separate argument paths.

AlloyPort must retain a zero-infrastructure local path. A configuration improvement must not imply
that users provide an OCI registry, external database, certificate service, or discovery system.

## Decision

The server accepts an optional strict schema-1 JSON file. `--config PATH` locates it explicitly and
has precedence over `ALLOYPORT_SERVER_CONFIG`; no working-directory file is discovered
automatically. Individual values use `environment > file > built-in defaults`. The defaults remain
a plaintext loopback listener and local SQLite/filesystem state.

Design 0039 later replaces only the no-discovery locator rule with deterministic
executable-sibling and system-wide locations. The value-precedence and local-default decisions in
this document remain unchanged.

Unknown fields and unsupported schema versions fail closed. Positive limits remain positive-only,
remote plaintext remains forbidden, and environment TLS paths remain an all-or-none group. Paths
inside a file resolve relative to the canonical configuration-file directory. Environment path
overrides remain relative to the process working directory for compatibility with existing service
definitions.

Serving and offline identity administration share the same command parser and configuration
loader. Consequently, enroll, rotate, and revoke always use the same configured identity database
as the server they prepare.

## Boundaries

- Configuration parses process arguments, environment, and JSON but knows no SQLite, filesystem
  Artifact, gRPC, or background-task implementation.
- Assembly selects concrete storage, authorization, TLS, and transport adapters but does not read
  environment variables.
- Runtime owns listener and task lifecycles but does not reinterpret deployment settings.
- The file contains paths to credentials, not credential material. Operators should keep keys and
  deployment-specific configuration outside source control.

## Consequences

- A checked-in example makes local topology reviewable and reproducible without adding a public
  task-submission API prematurely.
- Environment-only deployments continue to work unchanged.
- A misspelled future or obsolete field stops startup instead of silently weakening policy.
- Schema evolution requires an explicit version decision rather than opportunistic field meaning
  changes.

## Deliberate limits

- Schema 1 does not configure public task submission because that API remains deferred until real
  product-level CUDA and Ascend use cases run end to end.
- It does not introduce secret storage, remote configuration distribution, or role-split servers.
- Worker configuration remains a separate trust boundary governed by Design 0022.

## Verification

Unit tests cover configuration-file-relative paths, environment value precedence, strict unknown
field and schema rejection, and non-loopback plaintext rejection. Command tests cover the same
configuration locator for serving and identity administration. Architecture checks keep concrete
adapters out of configuration and environment reads out of assembly and runtime. Workspace format,
Clippy, boundary checks, and tests remain the acceptance gate.
