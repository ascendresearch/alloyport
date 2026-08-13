# Reduction correctness trusted images

The worker materializes `run_correctness.py` and the exact role bundle at execution time. These
images provide only the pinned compiler/runtime environment required by that worker-owned runner.
They do not contain a corpus, candidate implementation, expected output, or oracle policy.

Build from immutable registry manifests, then record both the base RepoDigest and resulting local
image ID in deployment evidence. For CUDA:

```bash
docker build \
  --build-arg CUDA_IMAGE=docker.io/nvidia/cuda@sha256:REPLACE_WITH_BASE_MANIFEST \
  --tag alloyport-cuda-correctness-v1:local \
  fixtures/reduction-correctness-v1/cuda-image
```

For Ascend:

```bash
docker build \
  --build-arg CANN_IMAGE=registry.example/cann@sha256:REPLACE_WITH_BASE_MANIFEST \
  --tag alloyport-ascend-correctness-v1:local \
  fixtures/reduction-correctness-v1/ascend-image
```

Use the resulting `sha256:...` image ID for both `image_digest` and `image_id` in a standalone
local-image worker configuration. The assignment then uses OCI image-config media type. A registry
deployment may instead publish the built image and use its manifest digest plus exact local image
ID. Mutable tags alone are never execution identity.

For a direct CUDA runner diagnostic before a controller-generated candidate exists, create a bundle
through the domain constructor:

```bash
cargo run -p alloyport-core --example reduction_reference_bundle -- \
  fixtures/migrations/cuda-reduction-v1/input /tmp/execution-bundle.json
```

The example marks every upstream identity as diagnostic. Its output can validate the trusted CUDA
runner and frozen corpus on hardware, but it is not Build Gate, paired-run, calibration, Correctness,
or release evidence.
