# Measured accelerator-probe latency and the 5 s bound it replaces

- Date: 2026-08-16
- Hosts: the Ascend950PR (`tritondev_1`) and NVIDIA GB10 (`dgx-spark`) executors recorded in
  `.alloyport-local/host-connections.md`
- Reproduce (Ascend, the exact sequence a worker runs at startup — `info -l`, then `info` twice):

  ```bash
  for i in $(seq 1 12); do
    a=$( { /usr/bin/time -f "%e" /usr/local/bin/npu-smi info -l >/dev/null; } 2>&1 )
    b=$( { /usr/bin/time -f "%e" /usr/local/bin/npu-smi info >/dev/null; } 2>&1 )
    c=$( { /usr/bin/time -f "%e" /usr/local/bin/npu-smi info >/dev/null; } 2>&1 )
    echo "$i info-l=$a info=$b info=$c"; sleep 2
  done
  ```

- Reproduce (CUDA — the adapter's two fixed queries):

  ```bash
  /usr/bin/time -f "%e" nvidia-smi --query-gpu=index,name,uuid,vbios_version,utilization.gpu,\
memory.used,memory.total,temperature.gpu,power.draw,gpu_recovery_action \
    --format=csv,noheader,nounits >/dev/null
  /usr/bin/time -f "%e" nvidia-smi --query-compute-apps=gpu_uuid,pid \
    --format=csv,noheader,nounits >/dev/null
  ```

## What produced this

The Ascend candidate worker would not start after redeployment:

```
Error: Unavailable("accelerator probe exceeded its 5s timeout")
```

Nothing was wrong with the host. All seven NPUs reported `OK`; six had no processes. The bound was
`const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5)`, hard-coded identically in
`ascend_smi.rs` and `nvidia_smi.rs`, and no record shows it was ever measured.

## What was measured

Twelve startup sequences on the Ascend host, 36 invocations, while the machine carried unrelated
external load (load average 2.75–5.96, 16 logged-in users — this is a shared development host and
that is its normal state):

| probe | min | max | over the 5 s bound |
|---|---:|---:|---:|
| `npu-smi info -l` | 0.46 s | 2.60 s | 0 / 12 |
| `npu-smi info` (first after `info -l`) | 2.17 s | **7.16 s** | 4 / 12 |
| `npu-smi info` (immediately repeated) | 2.17 s | 2.40 s | 0 / 12 |

Ten sequences on the idle GB10, 20 invocations:

| probe | min | max | over the 5 s bound |
|---|---:|---:|---:|
| `nvidia-smi --query-gpu=…` | 0.02 s | 0.03 s | 0 / 10 |
| `nvidia-smi --query-compute-apps=…` | 0.02 s | 0.02 s | 0 / 10 |

## What it says

**The bound was inside the spread of the command it bounds.** On the Ascend host one startup
sequence in three exceeded it, so a healthy, vendor-supported machine could not run a worker. The
failure is not confined to startup: `ascend_runtime.rs` probes again when an attempt selects its
device, so the same spread could have failed a paid migration attempt mid-run.

**One constant could not serve both backends.** The identical 5 s was ~0.7× the slowest observed
`npu-smi` and ~170× the slowest observed `nvidia-smi`. That is the argument for configuration rather
than for a better constant.

**The repeat measurement matters more than the maximum.** `npu-smi info` is consistently ~2.2 s when
run back to back and slow only on the first call after `info -l`, which suggests driver-side
caching rather than random noise. A retry would therefore usually succeed — and would have hidden
the fact that this host's probe is two orders of magnitude slower than the other's.

## What was changed

`device_probe_timeout_ms` is now a worker configuration field on all six backend policies, with
`DEFAULT_DEVICE_PROBE_TIMEOUT_MS = 30000` — roughly 4× the slowest probe observed on real hardware,
so it sits outside that command's spread while still bounding a hang to half a minute. The timeout
message now names the probe and the field.

## What this did not measure

- **Nothing here explains *why* `npu-smi info` costs seconds.** The driver was not instrumented; the
  caching hypothesis above is unverified.
- **The default is not a measurement of any host.** It is derived from one loaded Ascend host's
  maximum. A slower host must measure and set the field; these numbers do not license the default
  anywhere else.
- **No sample was taken while an AlloyPort attempt occupied the device**, which is when contention
  would be worst.
- **The GB10 samples were taken on an idle machine** (load average 0.04) with the CUDA worker
  connected. They say nothing about that probe under load.
