# Worker configuration

`alloyport-worker` uses one strict JSON file for the controller connection, worker identity, local
journal, accelerator backend, image identity, device selection, and execution limits:

```bash
cargo run -p alloyport-worker -- --config /absolute/path/to/worker.json
```

`ALLOYPORT_WORKER_CONFIG=/absolute/path/to/worker.json` is an equivalent locator for a service
manager. With no command-line locator, the worker uses this exact order:

1. `ALLOYPORT_WORKER_CONFIG`;
2. `alloyport-worker.json` in the executable's directory;
3. `/etc/alloyport-worker/worker.json`.

The working directory is never searched, and a missing explicit or environment-selected file fails
startup rather than falling through. Unlike the server, a worker has no safe built-in configuration
and reports every supported location when discovery finds nothing. Backend facts are not split
across environment variables. Start from the checked-in
[CUDA fixture](cuda-worker-config.example.json),
[Ascend fixture](ascend-worker-config.example.json),
[combined Ascend candidate](ascend-candidate-worker-config.example.json),
[Ascend Build](ascend-build-worker-config.example.json),
[CUDA correctness](cuda-correctness-worker-config.example.json), or
[Ascend correctness](ascend-correctness-worker-config.example.json) example and replace every
placeholder and all-zero digest.

The combined `ascend_candidate` variant is the normal NPU deployment. One persistent worker
discovers and reports every host NPU, advertises both Build and Ascend Correctness, and
`max_concurrency=1` serializes execution. Each attempt immediately leases any Ready, process-free
card; it does not require two NPUs or a configured device number.

For a single worker role per host, `/etc/alloyport-worker/worker.json` is the conventional
system-wide location. For multiple roles on one host, install each binary and its
`alloyport-worker.json` in a separate role directory (for example,
`/opt/alloyport-worker/ascend-build/` and `/opt/alloyport-worker/ascend-correctness/`), or use
distinct explicit files under `/etc/alloyport-worker/`. Configuration, journals, Artifact stores,
and sandboxes should use persistent deployment directories rather than `/tmp`.

The `ascend_candidate`, `ascend_build`, `cuda_correctness`, and `ascend_correctness` variants
intentionally contain no fixture ID or bundle digest. Each controller assignment supplies its
immutable build or correctness bundle while the file retains authority over the local image,
device, environment, sandbox, and resource ceilings.

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

`device_selection.allowed_device_ids` is an optional local allowlist for CUDA and Ascend candidate
workers; an empty list permits all discovered cards. `preferred_device_id` changes ordering but
cannot make an occupied or unhealthy device eligible. The worker registers the complete inventory
in `WorkerHello` and reports every device in heartbeats. Ascend derives `/dev/davinciN`,
`/dev/davinci_manager`, and `/dev/hisi_hdc` only after an attempt selects device N.

Selection is repeated as a durable per-attempt preflight. The worker acquires the device lease first,
stores the original observation before `Running`, and releases only after terminal container cleanup
and a new reusable observation. Probe uncertainty or an unattributed visible process retains the
lease as quarantine; neither backend currently authorizes an automatic reset.

Utilization and memory counters are telemetry only. Zero utilization does not prove that a device is
safe to reuse. NVIDIA unified-memory devices may report memory counters as `[N/A]`; the worker
records zero plus `memory_counters=unavailable` in the observation detail while continuing to base
eligibility on explicit recovery health, compute processes, and durable leases.
