# 0007: Worker isolation, receipts, and reproducibility

- Status: Proposed
- Date: 2026-08-09
- Scope: untrusted execution, environment identity, content-addressed evidence, and replay

## Context

Workers compile and execute generated code with access to scarce accelerator devices. Candidate code
may crash runtimes, corrupt shared state, consume excessive resources, inspect evaluator data, or
produce misleading logs. Meanwhile, a result that cannot be tied to exact code, tools, device state,
and inputs cannot support a release decision.

## Decision

Workers are replaceable execution surfaces outside the decision trust boundary. The controller owns
policy and scheduling; the oracle owns comparison and verdicts; an immutable evidence store owns
receipts and artifacts. A worker never publishes a release or mutates canonical knowledge.

## Execution bundle

The controller dispatches a content-addressed bundle containing:

- task, subtask, operator spec, corpus, and candidate identifiers;
- source commit and patch/tree digest;
- exact commands, working directory, declared outputs, and time/resource budgets;
- OCI image manifest digest plus mounted toolchain/runtime descriptors;
- device capability requirements and allowed device IDs;
- network, filesystem, environment-variable, secret, and syscall policy;
- receipt schema version and required measurements.

Mutable image tags, floating dependencies, implicit home-directory state, and worker-local source
edits are forbidden in release evidence.

## Isolation

- Each attempt receives a clean workspace and isolated process/container boundary.
- Network access is disabled by default and enabled only for a recorded build dependency phase.
- Secrets are scoped, short-lived, redacted from logs, and unavailable to candidate processes unless
  the contract explicitly requires them.
- CPU, memory, disk, process, wall-time, output-size, and device-time limits are enforced externally.
- Hidden oracle material and authority outputs are never mounted into the candidate environment.
- Device reset and health probes run before reuse after crashes, timeouts, or fatal runtime errors.
- Worker host identity is authenticated; host-key verification and least-privilege service accounts
  are mandatory outside disposable development environments.

Containers improve reproducibility but are not assumed to be a complete hostile-code security
boundary. Deployment policy may require dedicated hosts or stronger sandboxing according to device
runtime constraints and threat model.

## RunReceipt

A receipt is append-only and binds:

- bundle and candidate digests;
- worker identity and controller-issued attempt nonce;
- source tree, dependencies, compiler, framework, runtime, driver, firmware, device, and image digests;
- relevant device health, clocks, power, topology, and contention observations;
- command, sanitized environment, start/end times, exit status, signals, and resource use;
- stdout/stderr and structured result artifact digests;
- input/output, profiler, trace, sanitizer, and measurement artifact digests;
- integrity violations, truncation, timeout, reset, and infrastructure annotations.

The receipt distinguishes `SUCCEEDED`, `CANDIDATE_FAILED`, `TIMED_OUT`, `INFRA_ERROR`, and
`INTEGRITY_VIOLATION`. Process exit code alone does not determine semantic success.

## Attestation and storage

Artifacts are addressed by digest. Receipts use an in-toto-style statement envelope that binds a
typed predicate to immutable subjects. Signatures authenticate who produced a receipt; they do not
assert that the candidate is correct. Oracle verdicts and release manifests are separate typed
attestations over receipt subjects.

Large blobs may be stored in an OCI-compatible registry or other content-addressed store. The
database stores metadata and references, not the only copy of evidence.

## Replay

Replay resolves immutable artifacts, provisions a compatible worker, runs the exact command, and
emits a new receipt linked to the original. It never overwrites the original receipt. Exact bitwise
output is required only when the operator contract declares determinism; otherwise the oracle
re-evaluates the new outputs under the recorded semantic policy.

Non-replayable evidence becomes stale or unverifiable and cannot silently remain release authority.

## Rejected alternatives

- Trust worker logs as proof: logs are claims unless bound to independently checkable artifacts.
- Identify environments by mutable image tag: tags cannot reproduce the executed filesystem.
- Store only final aggregates: raw samples and traces are needed to audit measurement policy.
- Give workers direct database or knowledge-base credentials: execution compromise must stay local.

## Verification plan

- A candidate cannot access hidden expected outputs, controller credentials, or another attempt's
  workspace.
- Tampered artifacts, receipt fields, signatures, and digest mismatches are rejected.
- Timeout, output flooding, process forking, device crash, and host-loss fixtures terminate cleanly.
- Image tags are resolved to manifests before dispatch and the digest appears in every receipt.
- Replay on a compatible clean worker reproduces the verdict or records an explicit discrepancy.
- Audit and candidate processes cannot modify the evidence store directly.

## Standards basis

- [OCI Image Manifest](https://github.com/opencontainers/image-spec/blob/main/manifest.md) motivates
  content-addressed environment manifests and multi-architecture descriptors.
- [in-toto Statement v1](https://github.com/in-toto/attestation/blob/main/spec/v1/statement.md)
  provides a small envelope binding typed predicates to immutable subjects by digest.
- [SLSA provenance](https://slsa.dev/spec/v1.2/provenance) defines verifiable information about
  where, when, and how an artifact was produced. AlloyPort reuses this principle while adding device
  state, semantic verdicts, and performance measurements specific to accelerator work.
