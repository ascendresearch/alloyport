# Server configuration

`alloyport-server` can start with its local-development defaults or one strict schema-1 JSON file:

```bash
cargo run -p alloyport-server -- --config /absolute/path/to/server.json
```

`ALLOYPORT_SERVER_CONFIG=/absolute/path/to/server.json` is the equivalent locator for a service
manager. The explicit `--config` locator takes precedence over `ALLOYPORT_SERVER_CONFIG`; there is
no implicit file discovery. For individual settings, environment variables override file values,
which override built-in loopback defaults.

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
- `shutdown_timeout_seconds`: positive bounded drain window, default `10`.

The `artifact` object accepts `root`, `max_bytes`, `max_chunk_bytes`, `total_quota_bytes`, and
`owner_quota_bytes`. All byte limits must be positive. Unknown root, Artifact, and TLS fields are
rejected so misspelled safety settings cannot silently fall back to defaults.

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

## Identity administration

Offline enrollment commands load the same configuration and therefore update the configured
`identity_database`:

```bash
cargo run -p alloyport-server -- --config server.json identity enroll WORKER_ID client.pem
cargo run -p alloyport-server -- --config server.json identity rotate WORKER_ID old.pem new.pem
cargo run -p alloyport-server -- --config server.json identity revoke client.pem
```

When `--config` is omitted, `ALLOYPORT_SERVER_CONFIG` and then the normal defaults apply. The
configuration option precedes the `identity` subcommand.
