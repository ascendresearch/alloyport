# Worker configuration

`alloyport-worker` uses one strict JSON file for the controller connection, worker identity, local
journal, accelerator backend, image identity, device selection, and execution limits:

```bash
cargo run -p alloyport-worker -- --config /absolute/path/to/worker.json
```

`ALLOYPORT_WORKER_CONFIG=/absolute/path/to/worker.json` is an equivalent locator for a service
manager. Backend facts are not split across environment variables. Start from the checked-in
[CUDA fixture](cuda-worker-config.example.json),
[Ascend fixture](ascend-worker-config.example.json),
[Ascend Build](ascend-build-worker-config.example.json),
[CUDA correctness](cuda-correctness-worker-config.example.json), or
[Ascend correctness](ascend-correctness-worker-config.example.json) example and replace every
placeholder and all-zero digest.

The `ascend_build`, `cuda_correctness`, and `ascend_correctness` variants intentionally contain no
fixture ID or bundle digest. They admit only their role-specific execution kind; each controller
assignment supplies its immutable build or correctness bundle while the file retains authority over
the local image, device, environment, sandbox, and resource ceilings.

## Server connection

Loopback development may use `http://127.0.0.1:50051` without TLS. Any non-loopback endpoint must
include:

```json
"tls": {
  "certificate": "/absolute/path/to/worker.pem",
  "private_key": "/absolute/path/to/worker-key.pem",
  "server_ca": "/absolute/path/to/server-ca.pem",
  "server_name": "alloyport.example.com"
}
```

The certificate enrollment must resolve to the same ID as `worker.id`. Keep private keys outside the
repository.

## Local images: no registry required

Build or load the fixture image on the worker, then inspect its immutable Docker image ID:

```bash
docker image inspect --format '{{.Id}}' alloyport-ascend-add-v1:local
```

For a standalone installation, set `image_reference` to the local name or tag and set both
`image_digest` and `image_id` to that exact `sha256:...` ID. The controller assignment must use the
same digest with media type `application/vnd.oci.image.config.v1+json`. The worker resolves the tag
again before execution and rejects it if the ID changed.

A registry is optional. If one is available, `image_reference` may instead be
`repository@sha256:...`; `image_digest` is that OCI manifest digest, while `image_id` remains the
exact expected local Docker image ID. The assignment then uses media type
`application/vnd.oci.image.manifest.v1+json`.

## Device selection

CUDA discovers devices with bounded fixed `nvidia-smi` queries. Ascend uses bounded fixed
`npu-smi` queries. Both require a device to be `Ready`, have zero visible compute processes, and
have no active durable worker lease.

For NVIDIA, `Ready` requires `gpu_recovery_action=None`. Reset, reboot, or drain actions are
`Unhealthy`; an unsupported value is `Degraded`, and an unknown value invalidates the probe. A
successful command alone is never treated as health evidence.

CUDA's `device_selection.allowed_device_ids` is an optional local allowlist; an empty list permits
all discovered GPUs. `preferred_device_id` changes ordering but cannot make an occupied or unhealthy
device eligible. Ascend currently binds its complete configured identity to one device and applies
the same eligibility check before startup.

After selection, the worker registers that exact identity in `WorkerHello` and restricts heartbeat
observations to the same device. This is a shared CUDA/Ascend boundary: a single-device worker never
advertises or reports unrelated accelerators present on a multi-device host.

Selection is repeated as a durable per-attempt preflight. The worker acquires the device lease first,
stores the original observation before `Running`, and releases only after terminal container cleanup
and a new reusable observation. Probe uncertainty or an unattributed visible process retains the
lease as quarantine; neither backend currently authorizes an automatic reset.

Utilization and memory counters are telemetry only. Zero utilization does not prove that a device is
safe to reuse. NVIDIA unified-memory devices may report memory counters as `[N/A]`; the worker
records zero plus `memory_counters=unavailable` in the observation detail while continuing to base
eligibility on explicit recovery health, compute processes, and durable leases.
