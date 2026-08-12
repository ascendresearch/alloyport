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
6. **Interaction persistence and replay — next.** Cover per-run sequence,
   deduplication/conflict, cursors,
   run grants/revocation, and replay-to-live boundaries without folding transport delivery into the
   persistence Port.
7. **Execution backends and gRPC adapters.** Preserve typed failure classes, immutable assignment
   identity, cancellation, terminal Artifact gating, and replay semantics across fake, CUDA, Ascend,
   and transport adapters. Hardware evidence remains a separate explicitly configured gate.

## Non-goals

- Do not create an `alloyport-ports` crate solely to collect traits.
- Do not make the in-memory Artifact adapter a production composition default; it is explicitly
  non-durable.
- Do not force implementation-specific behavior into a Port contract. Durability mechanisms,
  migrations, probes, and filesystem layout keep their own adapter tests.
- Do not add a public task-submission API as a side effect of test refactoring.
