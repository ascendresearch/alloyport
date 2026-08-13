# Design 0033: Durable Agent Episode repository

- Status: Implemented persistence slice
- Date: 2026-08-12
- Extends: Design 0025
- Scope: restart-safe provider-neutral Episode snapshots and compare-and-swap persistence

## Context

The Agent reducer previously accepted an abstract `EpisodeRepository`, but its only implementation
was an in-memory reference adapter. The complete runtime snapshot was serializable but not
deserializable. Consequently, reconstructing a runner in tests exercised reducer identity while a
real controller process still could not recover an Episode after restart. This blocks an honest
configuration-selected model run before Source, Build, or Correctness authority is considered.

## Decisions

`DurableEpisodeState` and its nested provider-neutral records now support strict deserialization
with unknown fields rejected. The top-level state carries an explicit schema revision and validates
that revision, loop policy, record counts, and recovered semantic turns before execution resumes.
Repository-specific failures remain typed through the core port without introducing `SQLite` into
the domain crate.

`alloyport-server` owns a `SqliteEpisodeRepository`. It stores one complete state snapshot and
monotonic revision per Episode, implements create-once identity, and replaces a snapshot only when
the caller's expected revision matches. Save uses an immediate transaction, negative or oversized
revisions fail closed, the stored state must reproduce its row identity, malformed state is
rejected, and reopening returns the exact typed snapshot. Its contract is exercised against both
the in-memory reference and `SQLite` adapters.

## Scope boundary

This slice provides physical Episode durability but does not yet compose a live model run. The
production path still needs a durable provider context store that binds initial input, exact native
continuation, tool-result Artifacts, and the reducer-derived next-input digest. It then needs a
controller application use case that owns the Episode runner and Candidate tools. No provider call,
generated candidate, or Gate receipt is claimed by this design.
