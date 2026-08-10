# 0013: Durable certificate enrollment and stable worker identity

- Status: Accepted
- Date: 2026-08-10
- Scope: verified client-certificate enrollment, stable logical ownership, atomic rotation,
  revocation, Artifact authorization, and worker-control identity binding

## Context

Mutual TLS proves that a client holds a private key for a certificate accepted by the configured
CA. It does not decide which AlloyPort worker that certificate represents. Using the leaf
certificate fingerprint directly as the Artifact owner prevented metadata impersonation, but it
made certificate rotation create a new owner and left `WorkerHello.worker_id` unrelated to the
transport credential.

AlloyPort needs a durable application-authorization boundary above TLS verification. Artifact
references must survive routine credential rotation, and one certificate must not be able to claim
another worker's control identity.

## Decision

`alloyport-server` stores certificate enrollments in SQLite. A SHA-256 fingerprint of tonic's
verified client leaf certificate maps to one stable logical `owner_id`. Enrollments have three
states:

- `Active`: accepted for new RPCs and control frames;
- `Replaced`: inactive and linked to its replacement fingerprint;
- `Revoked`: inactive without granting access through another credential.

A partial unique index permits only one active certificate for each owner. Enrollment is
idempotent only for the same active owner/fingerprint pair and rejects cross-owner reuse or a
second active certificate. Rotation runs in one immediate transaction: it validates that the old
fingerprint is active for the named owner, rejects a previously used replacement, marks the old
row replaced, and inserts the new active row. Repeating that exact completed rotation is
idempotent. Revocation is durable and idempotent for an already revoked fingerprint; an inactive
fingerprint cannot be re-enrolled.

The stable owner, rather than the certificate fingerprint, is the Artifact upload-session owner and
owner-to-digest reference key. Rotation therefore preserves access to completed artifacts without
rewriting artifact metadata. `WorkerControlService` resolves the same verified connection identity
and requires `WorkerHello.worker_id` to equal the enrolled owner. It revalidates the original
fingerprint for every later worker frame, so the next heartbeat or lifecycle frame terminates a
stream whose certificate was replaced or revoked.

The server binary uses `ALLOYPORT_IDENTITY_DATABASE`, defaulting to
`<ALLOYPORT_ARTIFACT_ROOT>/identities.sqlite3`. The first administration surface is deliberately
offline and explicit:

```text
alloyport-server identity enroll OWNER CERT.pem
alloyport-server identity rotate OWNER OLD.pem NEW.pem
alloyport-server identity revoke CERT.pem
```

Remote TLS worker control and every Artifact RPC require an active enrollment. Plaintext worker
control remains a loopback-only development bypass. Artifact RPCs have no plaintext bypass and
return `Unauthenticated` without verified TLS connection information.

## Failure and concurrency semantics

- SQLite `BEGIN IMMEDIATE` serializes conflicting enroll, rotate, and revoke operations.
- Registry reopen preserves active, replaced, and revoked states.
- A CA-valid but unknown certificate is unauthenticated at the application boundary.
- A certificate enrolled for one owner cannot forge another `WorkerHello.worker_id`.
- Replacement and revocation take effect for new RPCs immediately and for an existing control
  stream when it next sends a frame.
- Registry storage errors fail closed rather than falling back to a fingerprint owner.

## Deliberate limits

The registry does not issue certificates or implement an online enrollment protocol, bootstrap
tokens, CA/CRL/OCSP management, certificate-expiry monitoring, pool membership, roles, or
capability authorization. The commands assume a separately authorized operator and trusted local
access to the registry. A single SQLite database serves one server instance; replication and
cross-instance cache invalidation are not defined.

Revocation here is AlloyPort application authorization, not CA revocation. An idle already-open
control stream is observed as inactive only when it sends its next frame or the transport closes.

## Verification

Unit tests cover reopen persistence, idempotent enrollment and rotation, conflicting owners and
certificates, replacement state, and revocation. The command integration test exercises enroll,
conflict, rotate, and revoke against a durable database. The end-to-end mTLS test proves forged
hello rejection, cross-owner Artifact isolation, old-stream termination after rotation,
existing-artifact access through the replacement certificate, and denial after revocation.
