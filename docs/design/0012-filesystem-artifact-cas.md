# 0012: Filesystem artifact content-addressed store

- Status: Accepted
- Date: 2026-08-10
- Scope: immutable artifact identity, local filesystem layout, streaming ingestion, atomic
  publication, integrity verification, and crash cleanup

## Context

Assignments and receipts already refer to `sha256:` artifacts, but no component owned the bytes.
The worker control stream is intentionally a small-message control plane and cannot become the bulk
bundle, log, trace, or receipt transport. Executor and network service work therefore need a storage
boundary that is independently testable and does not leak Protobuf types into evidence storage.

## Decision

`alloyport-artifacts` owns canonical SHA-256 identities and an object-safe immutable `ArtifactStore`.
The first implementation is a single-filesystem CAS. Ingestion consumes a `Read` stream through a
bounded buffer, hashes exactly the bytes written, applies a configured per-artifact limit, and may
require an expected size and digest.

The on-disk layout is:

```text
artifact-root/
  .staging/upload-<process>-<counter>
  sha256/<first-two-hex>/<full-64-character-hex-digest>
```

Staging and objects must be on the same filesystem. After the staging file is flushed, synced,
validated, and made read-only, publication uses `hard_link(staging, object)`. Creating a hard link is
atomic and fails if the digest path already exists; unlike Unix `rename`, it never replaces an
existing immutable object. A duplicate is successful only after the existing bytes are re-hashed and
verified. A corrupt object is reported and never repaired implicitly.

The digest covers bytes only. Media type, producer, retention, authorization, references, and
reachability are metadata maintained above the CAS. Serialized Protobuf messages are not used as
canonical artifact inputs.

## Failure and recovery semantics

- Read, write, size, size-declaration, and digest failures remove the current staging file.
- A process crash may leave staging residue; opening the store removes regular files and symlinks in
  `.staging` before accepting new work.
- Unexpected directories in staging fail initialization instead of triggering recursive deletion.
- Fanout and object directory entries are synced around publication.
- Opening an artifact hashes the opened file before returning a reader positioned at byte zero.
- Concurrent uploads of identical bytes result in one `Stored` result and idempotent
  `AlreadyPresent` results.

## Deliberate limits

The original CAS decision did not define the network Artifact service or resumable upload sessions;
the local session follow-up is recorded below. Authorization, global quota accounting, reference
metadata, garbage collection, remote object-store adapters, and replication remain out of scope.
The next slice adds a separate streaming RPC; bulk bytes stay off the worker control stream.

The filesystem implementation assumes hard-link support within one storage root. A future object
store adapter must provide an equivalent create-if-absent publication primitive and integrity check,
not emulate atomicity by overwriting a digest key.

## Resumable session follow-up

The local resumable-session state machine was added after the CAS core. SQLite binds a session to an
owner seam, idempotent upload key, expected identity, exact committed offset, state, and expiry. Each
append syncs bytes before committing its new offset. If a crash leaves bytes beyond the committed
offset, the next append truncates that tail before continuing. Finalization is serialized and
idempotent; transient storage I/O remains retryable, while digest, size, or existing-object integrity
failures become terminal.

The owner string is not yet authentication. The Artifact service must supply it from an injectable
resolver bound to the authenticated connection rather than accepting it as an authoritative request
field.

## Verification

Tests cover canonical digest parsing, bounded streaming read/write, declared digest mismatch,
per-artifact size exhaustion, interrupted readers, staging cleanup, restart recovery, duplicate and
concurrent publication, read-only objects, verified readback, and corrupted-object refusal.
