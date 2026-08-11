# 0019: Authorized interaction replay and subscription

- Status: Accepted; implemented
- Date: 2026-08-11
- Scope: public event transport, durable run grants, mTLS identity, reconnect cursors, bounded
  delivery, revocation, and controller display redaction

## Context

Design 0017 made worker interaction events canonical and durable. The first subscription foundation
could cross from SQLite replay to bounded live delivery without a race, but it was a library-only
facility. A public client still needed an authorization boundary and a transport that did not mint a
second event vocabulary or attach directly to ephemeral worker-control frames.

## Decision

`alloyport.interaction.v1.InteractionService` exposes two server-streaming RPCs:

- `ReplayRun` returns a bounded page strictly after the client's last applied canonical sequence;
- `SubscribeRun` replays after that cursor and then remains attached to the run's bounded live queue.

Each wire item contains the JSON encoding of exactly one `alloyport_events::EventEnvelope`. Clients
read `event_id` and `sequence` from that envelope. The Protobuf transport deliberately does not
define parallel payload variants or another authoritative sequence.

Replay uses 256 events by default and rejects limits above 4,096. The production hub pages its
durable snapshot in batches of 256 and gives each run a 1,024-notification live queue. The outgoing
RPC channel holds 32 items. If one send remains blocked for five seconds, the stream terminates with
`RESOURCE_EXHAUSTED` and names the last sequence the client can safely use as `after_sequence`.
Hub-level lag uses the same status and cursor principle. A cursor beyond the durable high-water mark
is `INVALID_ARGUMENT`; an unexpected canonical gap is `DATA_LOSS`.

The hub receiver is attached before the SQLite high-water mark is read. Events appended during the
handoff are therefore either part of the durable prefix or already in the live queue. Sequence
checks discard only duplicate notifications. Notification pressure is isolated by run and never
blocks authoritative append.

## Authorization and revocation

`interaction_run_grants` durably binds a run to one or more stable owner IDs. Grants are idempotent,
survive restart, and can be revoked idempotently. A revoked owner/run pair is terminal and cannot be
silently reactivated; a new explicit owner may still be granted access.

The trusted controller can enqueue through `enqueue_assignment_for_owner` or manage a grant
explicitly. Worker frames and public request bodies cannot select an owner.

Production RPC authorization resolves the verified client certificate through the durable enrollment
registry, then checks the active run grant. Live delivery revalidates both certificate enrollment and
the grant before and after waiting for the next event. Certificate rotation terminates the old stream;
the replacement certificate retains access through the stable owner. Grant or certificate revocation
terminates an existing stream before another event is released. Plaintext loopback therefore receives
`UNAUTHENTICATED` from this public service, just as it does from the enrolled Artifact service.

## Redaction and authority

Observed worker display fields pass through a controller policy before canonical persistence. The
current policy removes terminal control sequences and unsafe control characters, masks common
credential assignments and bearer values, and marks command output display text as sanitized. Raw
bytes remain available only through the separately authorized terminal Artifact and continue to be
used for output replay conflict detection. Redacted preview text is not execution evidence.

Future producer adapters must pass their user-visible fields through an equivalent controller policy
before append. This decision does not authorize rewriting stored canonical envelopes at the RPC edge,
because doing so would make an event ID describe different semantic content for different clients.

## Deliberate limits

Interaction rows are not pruned yet, so every valid historical cursor remains retained. Before a
retention scheduler deletes a prefix, it must add an explicit cursor-expired status containing the
earliest retained sequence; it must not reuse `DATA_LOSS` or silently start later.

There is no public scheduling/task-creation RPC, TUI, WebSocket adapter, per-role diagnostic-event
policy, or replicated event bus. Public subscription does not turn interaction events into audit,
oracle, gate, or release authority.

## Verification

Tests cover durable multi-owner grants, idempotent grant/revoke, terminal revocation, restart,
authorized bounded replay, cross-owner denial, replay-to-live delivery, live grant revocation,
public slow-consumer termination with a resumable cursor, and controller redaction. A real tonic
mTLS integration test covers generated RPC transport, stable certificate owner mapping, cross-owner
denial, termination on rotation, access through the replacement certificate, and denial after
revocation. A control-plane integration test proves owner-aware enqueue publishes through the same
hub used by subscribers and that its grant survives controller restart.
