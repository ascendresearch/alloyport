# Port contract suites

- Date: 2026-08-12
- Status: Active
- Purpose: prove adapter substitutability at application boundaries instead of testing each
  implementation against unrelated examples

## Contract-suite rule

A Port contract suite is one reusable behavioral function invoked unchanged for every conforming
adapter. It tests externally observable invariants, typed failure categories, idempotency, and
recovery semantics; adapter-specific tests remain responsible for implementation details such as
SQLite migration, filesystem crash residue, or backend command parsing.

Test doubles that participate in application tests should implement the same contract. A fake is
not considered compatible merely because it implements the Rust trait.

## Inventory and order

1. **Immutable Artifact objects — implemented.** `ArtifactStore` plus `ArtifactRetentionStore` run
   one contract against `FilesystemArtifactStore` and `InMemoryArtifactStore`. The suite covers
   verified ingestion, exact reads, content-addressed idempotency, no publication after digest/size
   failure, configured size bounds, presence, and idempotent administrative removal. Filesystem
   tests separately retain crash recovery, atomic publication, tamper detection, and concurrency.
2. **Worker device leases — implemented.** `DeviceLeaseStore` runs one contract against the SQLite
   journal and a focused memory fake. It covers known-attempt enforcement, exclusive device
   ownership, idempotent acquisition/release, immutable device-matched preflight evidence, terminal
   quarantine, and attempt-transition restrictions. A separate SQLite test proves leases and
   preflight evidence survive reopen. This is the persistence foundation shared by GPU/NPU guards.
3. **Server attempt leases — implemented.** `AttemptLifecycleRepository` runs one contract against
   SQLite and a focused memory reference. It covers identity binding, monotonic observation
   transitions and duplicate classification, renewal/expiry, non-resurrection, stale late results,
   cancel-before-send, cancellation acknowledgement versus execution termination, and terminal
   cancellation. A SQLite-only test retains durable observation auditing.
4. **Server assignment dispatch — implemented.** Assignment read/write behavior runs unchanged
   against SQLite and a focused reference. The suite covers immutable admission, bounded preparing
   reads, preparation visibility and defer ordering, dispatchability/replay, reassignment linkage,
   and the atomic state/lease/outbox permission boundary including conflict rollback. SQLite-only
   tests retain reopen durability, schema migration, and connection-sequence rollback evidence.
5. **Artifact upload metadata — implemented.** `ArtifactUploadRepository` and
   `ArtifactMetadataStore`, plus the narrow administrative test harness, run one contract against
   SQLite and a focused memory reference. The suite covers immutable idempotency keys, exact
   offsets, owner isolation, zero-byte publication, retryable versus terminal finalization, quota
   reservation/release, typed reference idempotency/conflict/revocation, retention, and reachability
   through garbage collection. SQLite-only tests retain reopen/migration, crash-tail repair,
   concurrent quota admission, reader leases, and pending-GC recovery.
6. **Interaction persistence and replay — implemented.** `InteractionEventWriter`,
   `InteractionEventReader`, and `InteractionRunAccessStore` run one persistence contract against
   SQLite and a focused memory reference. It covers run-local sequencing, restart-tolerant canonical
   deduplication/conflict, raw-output gaps/duplicates/overlap, bounded cursors, independently scoped
   streams/runs, and terminal run-grant revocation. SQLite-only tests retain reopen durability;
   application tests retain replay-to-live, slow-consumer, cross-run notification, cursor rejection,
   authorization, and sanitization behavior.
7. **Fixed-accelerator container execution — implemented.** CUDA and Ascend retain separate typed
   policies, create plans, device/environment facts, and receipt data, but now share container
   identity reconciliation, cancellation/timeout/output supervision, stop-and-wait semantics, and
   terminal outcome classification. One contract vector suite applies success, candidate failure,
   cancellation, timeout, output exhaustion, and missing verification-marker rules to both fixture
   policies. Adapter tests retain image/device policy and engine-command evidence; real hardware
   remains an explicitly configured gate.
8. **gRPC domain-error status policy — implemented.** Control repository, Interaction, Artifact
   upload/storage, and certificate identity failures map to gRPC codes in one internal transport
   policy. Table-driven contract tests lock not-found, authentication/authorization, validation,
   conflict, resource exhaustion, failed-precondition, integrity/data-loss, and internal classes;
   service adapters retain request validation and use-case orchestration rather than duplicating
   mappings.
9. **Authenticated gRPC request context — implemented.** `AuthenticatedRequestContext` carries one
   stable logical owner plus the optional verified connection identity needed for later revocation
   checks. Control, Artifact, and Interaction resolve that common context without trusting body
   ownership. Streaming behavior remains explicit at each service boundary: Control revalidates
   before every inbound frame, Interaction revalidates credential and run grant during delivery,
   and Artifact revalidates before committing every upload chunk. A loopback test proves that
   revocation between chunks leaves only the already-authorized prefix committed. Artifact/run
   visibility stays in the owning access policy rather than generic authentication middleware;
   explicit local contexts are limited to tests and in-process adapters.
10. **Internal gRPC message envelopes — implemented.** Shared protocol constants bound worker-to-
    server frames at 128 KiB, server-to-worker frames at 4 MiB, Interaction requests at 64 KiB,
    canonical Interaction events at 512 KiB, and Artifact download messages at 128 KiB. The server
    and outbound worker configure tonic from the same constants instead of framework defaults.
    Best-effort output preview payloads have a stricter 64 KiB protocol limit; shared execution
    coordination splits larger backend observations without gaps, while the server validates the
    invariant before persistence. Artifact upload decoding and client encoding remain derived from
    the operator-configured upload chunk limit plus conservative Protobuf framing allowance.

## Non-goals

- Do not create an `alloyport-ports` crate solely to collect traits.
- Do not make the in-memory Artifact adapter a production composition default; it is explicitly
  non-durable.
- Do not force implementation-specific behavior into a Port contract. Durability mechanisms,
  migrations, probes, and filesystem layout keep their own adapter tests.
- Do not add a public task-submission API as a side effect of test refactoring.
