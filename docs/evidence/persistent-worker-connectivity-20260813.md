# Persistent worker connectivity — 2026-08-13

This record validates stable process configuration discovery, persistent deployment paths, and the
mutually authenticated transport needed before the first real Candidate Episode. It is not a Build
or Correctness execution receipt: no assignment, accelerator container, provider request, or process
eviction was authorized or performed.

## Installed processes and configuration

- local controller binary: `/home/dawei/.local/lib/alloyport-server/alloyport-server`, digest
  `sha256:174603c4da2c5afc9b4a33cfc05f97175c3a63b15dbe64cdac91ba00966809ef`;
- local controller sibling configuration digest:
  `sha256:5b4c5f1f127b1751097de2d30f08691e793eb30e204a4b4e65155b9397e71309`;
- GB10 CUDA Correctness binary digest:
  `sha256:5113897e39210e2c945c066ec18401418bf012d5641e108e0a51e5193d2d04b1`;
- GB10 sibling configuration digest:
  `sha256:ae052a650669f04f235f9ab74a90d58c14b8c09db949d99436d8a8f38086f463`;
- both x86_64 Ascend worker binary digests:
  `sha256:cb232d173b79a5c77496726f33d1c76891551352f45c011628f5a0698e1179e3`;
- Ascend Build sibling configuration digest:
  `sha256:dca5cd91f945cc5d4cd4c6767a1fab908a31972a9947941dc9149708cb65a500`;
- Ascend Correctness sibling configuration digest:
  `sha256:7ce58b7d704e7481dc2bd48e2434fc71e10d778731c14d759ce64e4e301913c2`.

CUDA state is under `/home/dawei/.local/state/alloyport-worker/cuda-correctness`. Ascend Build and
Correctness state are under `/var/lib/alloyport-worker/ascend-build` and
`/var/lib/alloyport-worker/ascend-correctness`. The local controller uses
`/home/dawei/.local/state/alloyport-server`. Every process was started from `/` with no `--config`
argument, proving that behavior does not depend on its working directory. The superseded
`/tmp/alloyport-worker-state` trees were deleted from both hardware hosts after migration.

## Authentication and transport

A deployment CA signed one server certificate for `alloyport-controller` and three client-only
certificates. Their leaf fingerprints were enrolled as their exact stable worker IDs:

- `cuda-correctness-worker-1`:
  `sha256:d7785cc37d09d57933f6b77a6b3585a9f52f762627ec9743616cd25ca4d3bd7b`;
- `ascend-build-worker-1`:
  `sha256:a6e08584998fd043647b61e1f2807ea9eac6b7b7f92cc60d1753fd757a8a2614`;
- `ascend-correctness-worker-1`:
  `sha256:8efb97db239335ac0a5825482cc1f2f598e2ec0c2e51b359d2f16f6c1d8f225d`.

CA, server, and worker private keys are outside the repository and mode `0600`. Only public
fingerprints and paths are recorded here. The controller remained loopback-only. Each hardware host
used an SSH reverse tunnel that exposed the controller only on that host's loopback interface; the
worker then connected to `https://127.0.0.1:50051` while verifying the
`alloyport-controller` certificate name.

The CUDA worker completed startup preflight and maintained an established mTLS control connection
to the production controller. Both Ascend client identities independently completed verified TLS
1.3 handshakes through their tunnel. The Ascend worker processes themselves failed closed before
`WorkerHello` because their configured NPUs were not simultaneously Ready, process-free, and
unleased. No existing NPU process was disturbed, so this record does not claim Ascend control-plane
registration.

## Shutdown and remaining gate

All bounded worker probes, SSH tunnels, and the controller were stopped after verification; port
50051 had no listener and neither hardware host retained an AlloyPort worker process. The remaining
deployment gate is an operator-provided idle healthy Ascend device (or a separately authorized
maintenance window). Only then can both Ascend workers complete their startup preflight and be
registered for the first real Candidate Episode. Provider dispatch remains separately gated by the
exact command-line authorization token.
