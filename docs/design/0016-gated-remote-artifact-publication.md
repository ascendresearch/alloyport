# 0016: Gated remote execution Artifact publication

- Status: Accepted
- Date: 2026-08-10
- Scope: resumable worker uploads, terminal publication ordering, controller validation, and typed
  output/receipt reachability

## Context

The fake runtime durably spooled stdout, stderr, and its receipt in a worker-local CAS before writing
`ExecutionFinished`. Sending those local digests over worker control was insufficient: the controller
could persist identities whose bytes did not exist in its Artifact service, and a worker observation
could effectively manufacture evidence reachability.

The Artifact RPC already provides stable-owner upload sessions, idempotent upload keys, durable
committed offsets, and idempotent finalization. The worker should compose that state machine rather
than add a second local network-upload journal.

## Decision

`RemoteArtifactPublisher` opens each local CAS object through a verified reader and streams bounded
chunks to the Artifact service. The fake runtime supplies stable reference intents as upload keys:

- `output:<attempt_id>:stdout`;
- `output:<attempt_id>:stderr`;
- `receipt:<attempt_id>`.

`BeginUpload` is idempotent for the authenticated stable worker owner. A retry receives the existing
session and resumes from its durable `committed_offset`; it never trusts a client-side guessed
offset. The publisher validates the returned upload contract, skips exactly that many verified local
bytes, streams the suffix, finalizes, and verifies the returned digest and size. Completed sessions
are accepted only when their recorded identity matches the local intent. Zero-byte streams finalize
directly through an empty reader and do not require a synthetic upload chunk.

The executor ordering is now:

1. durably mark `Running`;
2. execute and spool complete stdout, stderr, and receipt locally;
3. finalize all three objects in the remote Artifact service;
4. durably mark `Finished` and create the worker lifecycle outbox row;
5. send `ExecutionFinished`.

A publication failure leaves the attempt `Running` and creates no finished outbox row. The fake
executor may retry after session recovery; deterministic output and content-addressed publication
make already completed objects idempotent.

Before opening a control stream, a publisher-enabled worker also republishes every pending terminal
outbox entry. This fail-closed reconciliation covers journals created by an older local-only worker
revision and prevents a legacy `ExecutionFinished` frame from reaching the controller before its
bytes. Reconciliation is idempotent for terminals that were already published normally.

## Controller validation and grants

The production worker-control service shares the Artifact metadata store used by the Artifact RPC.
Before accepting a terminal observation, it requires stdout, stderr, and receipt to match completed
upload sessions owned by the reporting stable worker under the exact keys above. Digest, size, and
media type must all agree. Merely naming an existing digest, or an Artifact available through some
other reference, is not sufficient.

After validation, the controller creates idempotent `AssignmentOutput` references for stdout/stderr
and a `Receipt` reference for the receipt, then records the finished observation. Artifact metadata
and control state use separate SQLite databases, so this is deliberately fail-safe rather than
atomically coupled: a stop after grants but before control commit leaves conservative reachability;
terminal redelivery repeats the grants idempotently and completes the observation. The reverse order,
which could expose a terminal record without bytes or roots, is forbidden.

The worker-facing Artifact RPC still cannot grant these references. Upload ownership comes from the
verified mTLS identity, and worker-control identity binding ensures the reporting worker ID is the
same stable owner used for validation.

## Deliberate limits

The remote publisher is an explicit worker-library attachment and the current binary still does not
launch the fake executor. Uploads are sequential and use a fixed configured chunk size; there is no
parallel multipart policy, bandwidth scheduling, background spool pruning, or retry/backoff loop
inside one execution task. Recovery occurs through the existing worker session supervisor and stable
upload keys.

Controller grants currently target the reporting worker because assignments do not yet carry a task
or user Artifact owner. Later task ownership must add separate controller grants rather than changing
upload ownership. Canonical interaction-event ingestion remains separate and is not implemented by
this decision.

## Verification

Tests prove that publication failure leaves terminal state uncommitted, retry can then finish exactly
once, a pre-existing partial stdout upload resumes from its server offset, empty stderr finalizes,
all three remote sessions complete before the server accepts `Finished`, typed controller grants are
created, and a terminal frame cannot cite artifacts that were not finalized by the reporting worker.
