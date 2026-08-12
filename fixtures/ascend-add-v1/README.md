# ascend-add-v1

This fixed fixture compiles one verified `add_custom.cpp` bundle member against a trusted host and
tiling harness baked into a CANN development image. The candidate contract is the
`add_custom_kernel(GM_ADDR x, GM_ADDR y, GM_ADDR z, GM_ADDR tiling)` entry point and the checked-in
`AddTilingData` layout. The harness uses 16,384 deterministic `float32` elements, verifies every
result exactly, and emits one canonical `PASS fixture=ascend-add-v1 ...` line.

Build only from a digest-pinned CANN base and record both the resulting manifest digest and local
image ID in the uncommitted worker policy:

```bash
docker build \
  --build-arg CANN_IMAGE=registry.example/cann@sha256:REPLACE_WITH_BASE_MANIFEST \
  --tag alloyport-ascend-add-v1:local \
  fixtures/ascend-add-v1/image
docker image inspect alloyport-ascend-add-v1:local --format '{{.Id}}'
```

The worker ignores the image-authored entrypoint and invokes the trusted harness by absolute path.
Compilation happens in the attempt's size-bounded tmpfs; the verified bundle and host driver remain
read-only. The harness uses argv-only subprocesses and never invokes a shell. Runtime capability
policy drops every capability and adds back only `DAC_OVERRIDE`, which the 950PR host requires to
open its driver-owned `0660` device node; `no-new-privileges` remains enabled.

Create the canonical bundle bytes locally; the command prints the Artifact digest and byte size that
must be published and copied into the local policy:

```bash
python3 fixtures/ascend-add-v1/make_bundle.py \
  --output /tmp/ascend-add-v1.bundle.json
```
