# Server configuration

`alloyport-server` can start with its local-development defaults or one strict schema-1 JSON file:

```bash
cargo run -p alloyport-server -- --config /absolute/path/to/server.json
```

`ALLOYPORT_SERVER_CONFIG=/absolute/path/to/server.json` is the equivalent locator for a service
manager. Configuration-file discovery is deterministic and never consults the working directory:

1. `--config PATH`;
2. `ALLOYPORT_SERVER_CONFIG`;
3. `alloyport-server.json` in the executable's directory;
4. `/etc/alloyport-server/server.json`;
5. built-in loopback defaults when none of those files is present.

An explicitly named missing or invalid file fails startup; it does not fall through to another
location. For individual settings, environment variables override file values, which override
built-in loopback defaults.

Start from the checked-in [example](server-config.example.json). Relative paths written in the JSON
file are resolved from that file's directory, so behavior does not depend on the process working
directory. Relative paths supplied through environment variables retain normal process-relative
semantics.

## Schema 1

The root object accepts these fields:

- `schema_version`: required and exactly `1`;
- `listen`: worker-control listen address, default `127.0.0.1:50051`;
- `database`: control and Interaction SQLite database, default `alloyport-control.sqlite3`;
- `artifact`: Artifact storage limits and paths;
- `identity_database`: enrollment SQLite database, default
  `<artifact.root>/identities.sqlite3`;
- `tls`: server certificate, private-key, and client-CA paths, or `null` for loopback plaintext;
- `migration_runtime`: optional daemon dispatcher settings. When enabled it contains
  `candidate_template`, an optional persistent `root` (default `migration-runtime` beside the
  config), and optional positive `poll_interval_ms` (default `500`);
- `shutdown_timeout_seconds`: positive bounded drain window, default `10`.

The `artifact` object accepts `root`, `max_bytes`, `max_chunk_bytes`, `total_quota_bytes`, and
`owner_quota_bytes`. All byte limits must be positive. Unknown root, Artifact, and TLS fields are
rejected so misspelled safety settings cannot silently fall back to defaults.

`artifact.max_chunk_bytes` also determines the Artifact upload gRPC encoding/decoding envelope,
with a conservative Protobuf framing allowance. Control and Interaction message limits are fixed
protocol contracts rather than deployment tuning knobs: worker previews are split into at most
64 KiB payloads, and both endpoints configure tonic with the shared protocol limits. Large durable
results belong in the Artifact service, not control or Interaction messages.

## Environment overrides

Existing deployments can override individual values with:

- `ALLOYPORT_LISTEN` and `ALLOYPORT_DATABASE`;
- `ALLOYPORT_ARTIFACT_ROOT`, `ALLOYPORT_ARTIFACT_MAX_BYTES`,
  `ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES`, `ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES`, and
  `ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES`;
- `ALLOYPORT_IDENTITY_DATABASE`;
- `ALLOYPORT_TLS_CERT`, `ALLOYPORT_TLS_KEY`, and `ALLOYPORT_TLS_CLIENT_CA`;
- `ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS`.

The three TLS variables must be set together. Plaintext remains restricted to loopback; a
non-loopback `listen` value requires a complete TLS block or all three TLS environment overrides.
The local trial path requires no external database, OCI registry, certificate service, or service
discovery system.

To make `alloyport-cli migrate` start Episodes automatically, set the deployment-level Candidate
template once in the Server configuration:

```json
"migration_runtime": {
  "candidate_template": "candidate.json",
  "root": "state/migrations"
}
```

The dispatcher replaces the template's migration input paths and Episode/task/search identities for
each submitted task. Provider catalog, prompts, generation policy, worker IDs, pinned images,
resource limits, and timeouts remain deployment policy in that template. Without this block,
submitted tasks remain safely `captured` and can still be inspected or cancelled.

## Identity administration

Offline enrollment commands load the same configuration and therefore update the configured
`identity_database`:

```bash
cargo run -p alloyport-server -- --config server.json identity enroll WORKER_ID client.pem
cargo run -p alloyport-server -- --config server.json identity rotate WORKER_ID old.pem new.pem
cargo run -p alloyport-server -- --config server.json identity revoke client.pem
```

When `--config` is omitted, the normal discovery sequence applies. The configuration option
precedes the `identity` subcommand, and serving and identity administration always use the same
discovered file.

## Explicit Candidate Episode command

The normal server command never starts an Agent Episode. A Candidate run requires a second strict
schema-1 file and an exact command-line authorization token. Start from
[the Candidate example](candidate-episode-config.example.json); its zero digests and sizes are
deliberately invalid placeholders.

First perform the network-free configuration and credential preflight:

```bash
cargo run -p alloyport-server -- --config server.json \
  candidate-episode validate candidate.json
```

This checks the complete runtime-model catalog, secure secret file and protocol headers,
MigrationSpec and reference sources, derives an exact delimited source context and its input-root
identity, derives subtask/data-boundary/episode-budget/request-budget identities from their actual
policies, enforces prompt bounds and durable paths, and validates three distinct worker targets,
OCI image descriptors, resource ceilings, and loop/codec budgets. It does not bind a listener,
contact a worker, or send a provider request.

The live command starts the normal worker-control/Artifact/Interaction services, waits for the
configured Build, CUDA Correctness, and Ascend Correctness workers to be connected with their exact
features, and only then advances the Episode:

```bash
cargo run -p alloyport-server -- --config server.json \
  candidate-episode run candidate.json --authorize-provider-dispatch
```

The final flag is intentionally not configurable through JSON or environment variables. It is the
operator's per-invocation acknowledgement that advancing the Episode may make billable external
provider calls. Relative paths in `candidate.json` resolve from that file. Candidate image and
resource fields are controller-owned and are never exposed as model tool arguments. Network policy
inside all three assignment contracts is always forced to `Disabled`.
