# 0014: Durable Artifact references and conservative garbage collection

- Status: Accepted
- Date: 2026-08-10
- Scope: controller-granted Artifact reachability, owner read authorization, retention metadata,
  quota release, active-download coordination, and crash-recoverable filesystem collection

## Context

The first Artifact service authorized a digest only for the stable owner that completed its upload.
Assignments, outputs, receipts, and audit retention need durable references independent of who
uploaded the bytes. A controller must be able to grant an input to another worker without copying
the object, then revoke that access while retaining evidence for policy-defined periods.

Deleting an immutable CAS path is more subtle than deleting one metadata row. Upload finalization
may publish the same digest concurrently, downloads may already hold a verified reader, quota usage
must reflect deduplication, and a process can stop after removing the file but before committing the
metadata update.

## Decision

The SQLite Artifact metadata database stores owner-scoped references. Each reference has an
immutable idempotency key, digest, typed kind, purpose, creation time, optional minimum retention
deadline, and optional revocation time. Defined kinds are completed upload, assignment input,
assignment output, receipt, retention root, and other controller policy.

An active reference grants its stable owner read access to the digest. `retained_until_ms` is not an
access-token expiry: after revocation it can continue protecting the physical object until the
deadline, while read authorization is removed immediately. Reusing an active reference key with
identical metadata is idempotent; changed metadata conflicts, and a revoked key is terminal rather
than silently reactivated.

Upload finalization creates an `Upload` reference keyed by its upload session. Existing completed-
upload owner mappings are migrated to typed references. A controller grant may refer only to an
object already managed by the metadata database and cannot manufacture an `Upload` reference.
Multiple references from one owner to one digest count once toward that owner's quota. Granting the
first such reference checks stored plus actively reserved owner usage. Revoking the last active
owner/digest reference releases owner quota. Global stored usage is released only after the physical
object is collected.

## Garbage collection

Collection is explicit and bounded. A digest is eligible only when all of these are false:

- an active Artifact reference exists;
- a reference has an unexpired minimum retention deadline;
- a non-expired open or finalizing upload session expects the digest;
- an in-process verified download reader is active.

Candidate digests are first written to `artifact_gc_pending`. Finalization, begin, grant, revoke,
and collection share an Artifact lifecycle lock; opening a download atomically rechecks owner
authorization and registers an active reader before opening the CAS file. Collection holds the
reader registry while deleting, skips already-open readers, removes the filesystem object, then
deletes object/accounting metadata and its pending marker in one SQLite transaction.

The pending marker makes a stopped deletion recoverable. If the file is already absent when a later
collection resumes, metadata cleanup still completes. Finalizing or beginning a fresh upload for
the same digest cancels a stale pending marker under the lifecycle lock. One-time migration markers
prevent historical completed sessions from resurrecting metadata after a collected object is
reopened.

## Authorization boundary

The worker-facing Artifact RPC does not expose grant, revoke, or collect methods. Those are
controller/library operations, because a worker must never grant itself an assignment or evidence
reference. Download continues to resolve the stable mTLS owner from Design 0013. The service policy
checks that owner, then the upload store rechecks it while acquiring the reader lease, closing the
authorization-to-open race.

## Deliberate limits

Collection is not yet scheduled automatically and has no public administration or controller API.
The active-reader registry coordinates one server process only; multi-server deployment requires a
shared lease/epoch protocol or an object store with equivalent deletion guards. GC does not scan for
objects inserted outside the managed upload path, monitor filesystem free space, prune historical
revoked-reference tombstones, or define legal/compliance retention policy.

Filesystem deletion and SQLite commit cannot be one atomic primitive. Pending recovery fails closed,
but an operator must run collection again after a crash. Remote object stores need their own
versioned conditional-delete design rather than copying filesystem assumptions.

## Verification

Tests cover typed and conflicting reference keys, concurrent idempotent grants, multiple references
to one owner/digest, idempotent revocation, cross-owner denial and explicit mTLS grant/revoke,
retention after revocation, active-reader protection, quota release after collection, pending-delete
restart recovery, and prevention of metadata resurrection after reopen. Existing upload, quota,
rotation, and download tests continue to exercise the same stable owner boundary.
