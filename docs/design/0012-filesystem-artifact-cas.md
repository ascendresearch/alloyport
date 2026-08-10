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
the implemented follow-ups are recorded below. General reference metadata, garbage collection,
remote object-store adapters, and replication remain out of scope. Durable certificate enrollment
and rotation are defined separately by Design 0013. Bulk bytes stay off the worker control stream.

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

The owner string is not itself authentication. The Artifact RPC adapter supplies it through an
injectable access policy rather than accepting it as an authoritative request field. The first
binary policy used the SHA-256 fingerprint of tonic's verified mTLS client leaf certificate; Design
0013 replaces that direct owner with a durable fingerprint-to-stable-owner enrollment mapping.

## Artifact RPC follow-up

`alloyport.artifact.v1.ArtifactService` is a separate service from worker control. Begin, status, and
finalize are unary operations; upload accepts a client stream of exact-offset chunks; download is a
bounded-buffer server stream with offset and optional length. Reconnection opens a new upload stream
and resumes at the session's durable committed offset. A single stream cannot mix session IDs.

The server adapter moves SQLite and filesystem operations onto blocking tasks and delegates owner
resolution and digest-read authorization to `ArtifactAccessPolicy`. The policy receives both RPC
metadata and tonic transport extensions, allowing a production implementation to inspect verified
TLS peer certificates. The adapter never accepts a client host path and never buffers a whole
artifact. The server binary registers the service; plaintext calls remain unauthenticated because
the production policy requires TLS connection information. A completed upload record is the first
durable owner-to-digest read reference.

## Transactional quota follow-up

SQLite accounts immutable objects once globally by digest and once per owner/digest reference.
Every new upload session atomically reserves its declared size against both the global and owner
limits in the same `BEGIN IMMEDIATE` transaction that creates the session. Existing idempotency keys
do not reserve twice, and concurrent begins cannot both observe stale capacity.

Finalization converts a reservation into object/reference usage in one transaction after CAS
publication. Duplicate digests do not increase global usage, and a repeated owner/digest does not
increase owner usage. Retryable CAS failures keep their reservation; terminal integrity failures
release it. Expired sessions no longer count toward admission and pruning deletes their rows and
partial bytes. Schema migration backfills reservations and completed object/reference usage from
pre-quota upload databases.

These are logical limits over objects and sessions managed through this upload database. They do not
replace filesystem free-space monitoring, and externally inserted CAS files are not discovered by
scanning the object tree.

## Verification

Tests cover canonical digest parsing, bounded streaming read/write, declared digest mismatch,
per-artifact size exhaustion, interrupted readers, staging cleanup, restart recovery, duplicate and
concurrent publication, read-only objects, verified readback, corrupted-object refusal, session
reopen/resume/finalize, and failure-state semantics. A loopback gRPC integration test resumes one
upload across two client streams, finalizes it, and downloads a bounded range from a nonzero offset.
An end-to-end mutual-TLS test proves that certificates enrolled to separate owners cannot cross
session or artifact boundaries, rotation preserves existing-artifact access for the stable owner,
and revocation removes access.
Quota tests cover restart recovery, idempotent and concurrent begin, per-owner isolation, terminal
failure and expiry release, duplicate-digest completion, old-schema backfill, and RPC
`ResourceExhausted` mapping.
