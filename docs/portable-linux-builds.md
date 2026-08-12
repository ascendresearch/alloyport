# Portable Linux server and worker builds

The production server and worker can be built as static `x86_64-unknown-linux-musl` executables so
their deployment does not inherit the build host's glibc version. This is the supported portable
build for the current x86-64 worker hosts; normal `cargo build` remains appropriate for local
development.

Install the Rust target and a musl compiler, then run:

```bash
rustup target add x86_64-unknown-linux-musl
# Debian/Ubuntu: apt-get install musl-tools
scripts/build_portable_linux_binaries.sh
```

The script builds locked release binaries for `alloyport-server` and `alloyport-worker`, rejects a
dynamically linked result, and writes `SHA256SUMS` beside them under
`target/x86_64-unknown-linux-musl/release/`. Set `ALLOYPORT_PORTABLE_CARGO` only when a deployment
builder deliberately selects a pinned Cargo toolchain executable.

CI exercises this path with Rust 1.88.0, the workspace minimum supported Rust version. Accelerator
hardware, Docker, an OCI registry, and SSH are not involved in the build.

This packaging choice does not change the worker execution contract. Runtime image, driver,
firmware, device, Artifact, and mTLS validation remain exactly the same.
