# 0044 — Git as the candidate record, not the store and not the interface

- Status: Accepted and implemented, with the two amendments in §Amended on acceptance
- Date: 2026-08-17
- Follows: [0042](0042-model-visible-receipt-references.md), and the inheritance added after the
  first migration reached a compiler
- Evidence: [`fatal-harness-defects-20260816.md`](../evidence/fatal-harness-defects-20260816.md),
  and what the record showed on three real runs:
  [`candidate-record-20260817.md`](../evidence/candidate-record-20260817.md)

## Context

A complete candidate bundle costs 90–100% of one model response. Correcting a single line meant
re-emitting every file, and one submission truncated its own JSON at exactly the 16 384-token
ceiling. The fix let a candidate inherit the files it did not change: parent tree plus a delta,
content-addressed, with a parent link.

That is git's data model with different names.

| AlloyPort | git |
|---|---|
| CAS object | blob |
| `manifest.files` | tree |
| `parent_candidate_id` | commit parent |
| manifest digest | commit id |

The observation that prompted this decision is that the resemblance is not accidental and should be
faced rather than extended one field at a time.

There is a second reason, independent of storage. Seven runs on 2026-08-16–17 produced a real
history of model attempts — the last one alone left three candidate trees with a known parent chain
— and that history is currently unreadable. Answering "what changed between the build that failed on
`acl/acl.h` and the one that failed on `kernel_operator.h`" was done by eye, comparing two full file
dumps, several times in one session.

## Decision

**Git is adopted as the record: a projection of candidate lineage into a real repository. It is not
adopted as the store, and not as the model-facing interface.**

After a submission assembles a candidate, the controller commits that candidate's tree in a
per-task repository, with the parent commit taken from `parent_candidate_id` and the gate outcome in
the commit message. The workspace already materializes each candidate as
`workspace/<candidate_id>/generated/…`, so the tree exists before the commit does.

The manifest remains authoritative for identity, gates, and evidence. Nothing in the trust path
reads git.

### Why not git as the store

**The manifest is not a tree.** It binds `task_id`, `migration_spec_digest`, `generation_strategy`,
`public_symbol`, `build_target`, and `input_source_paths` — the controller-owned facts
`matches_manifest` uses to refuse a candidate belonging to another migration. Moving those into
commit metadata puts the gate boundary on a string convention in a commit trailer, and requires the
controller to author every commit anyway to keep the model out of them. The boundary would be no
stronger and considerably easier to get wrong.

**Commits are not deterministic.** A commit embeds author and committer identity and two timestamps,
so the same tree yields a different commit id at a different second. This project's whole identity
discipline is that the same bytes produce the same digest. Verified on git 2.53: two repositories
built from identical content produce the identical tree object `4f2ebdd1…`, while commit identity
holds only when dates *and* identities are pinned. Trees are safe for identity; commits are a trap
of exactly the kind this codebase has been paying for.

**The hash is solvable but is a standing constraint.** `git init --object-format=sha256` works on git
2.53, so a git store need not introduce SHA-1 beside a sha256 system. It would, however, become a
deployment requirement nobody would remember.

**It would add a dependency to a workspace that ships static musl binaries**, for a capability the
CAS already provides with quotas, conservative garbage collection, and durable references.

### Why not git as the interface

Letting the model send a patch rather than files is the tempting version and is deliberately not
taken yet.

Most of the benefit is already banked: inheritance moved a correction from 16 384 output tokens to
788, measured on a live run. What remains is the difference between a small file and a smaller
diff.

Against that, a patch introduces a failure mode worse than the one it replaces. "Here is the file"
cannot fail to apply; "this patch does not apply" depends on exact context lines and is harder for a
model to repair than to have avoided. This session spent its length on failure modes that the model
could not act on, and this would add one.

The record makes the question answerable instead of arguable: with a real history we can measure how
large the actual diffs were and whether patches would have applied cleanly. That evidence should
come before the interface changes.

## Amended on acceptance

Two things in the Decision above could not be built as written. Both are recorded here rather than
quietly resolved in code.

### The projection is built afterwards, not during the run

The Decision says the controller commits *as a submission is assembled*, with the gate outcome in the
commit message. Those two cannot both hold: when a candidate is submitted no gate has run, so the
outcome does not exist yet. Something had to move.

What moved is when. `alloyport-server candidate-record TASK_ID --into DIRECTORY` reads the Episode
and the CAS after the fact and builds the whole repository. By then every verdict is known, so the
message the Decision asked for is writable.

The stronger reason is the one this session was already paying for. Seven migrations on 2026-08-16
died in the harness and **not one died from a bad kernel**; every fix in that session removed a way
for the harness to end a paid run. Committing inside a submission adds a step, a subprocess, and a
filesystem to the path a live migration takes. Built afterwards it cannot cost a run anything, and it
matches this decision's own consequence — the repository is a projection and may be rebuilt from
manifests at any time.

It is a `alloyport-server` subcommand beside `bootstrap`, `identity`, and `candidate-episode`,
because it reads the Episode database and the CAS directly. The CLI reaches those only over gRPC, so
putting it there would mean a new management RPC — control-plane growth the product plan freezes —
and an architecture check deliberately keeps gate documents out of the CLI.

### SHA-1 objects, tags for candidates, sequence numbers for dates

- **The default object format is used, not `--object-format=sha256`.** The Decision noted sha256 was
  available and would become a deployment requirement nobody would remember. Since git is not the
  store, its object ids carry no authority, and a 40-hex commit id is *visually distinct* from a
  `sha256:` digest — which is the property worth having, so nobody mistakes a commit for an identity.
- **Each candidate is a lightweight tag, `refs/tags/c001-<12 hex>`.** Lineage forks — in
  `task-ea9792931f2c781324ce536b` two candidates share one parent — so a single branch would rewind
  or refuse. `refs/candidates/…` was tried first and rejected: `git log --all --decorate` silently
  omits refs outside the standard namespaces, which produced a history whose commits could not be
  told apart.
- **Author and committer dates are the submission's sequence number in seconds after the epoch.** The
  Decision asks for dates derived from recorded attempt identity; the Episode records no timestamp
  anywhere, so there is no time to derive. A sequence number keeps `git log` in submission order and
  is byte-reproducible, and a 1970 date is conspicuous enough that nobody reads it as a time. Each
  commit says so in its own message.

### What the implementation verifies

After the import it re-reads the repository through `git cat-file` and rehashes every blob against
the manifest digest, checks each tree's path set, and checks each parent link. A projection that
disagreed with the manifest would otherwise be a wrong record that survives being rebuilt. This is
the shape `verify_source_gate_receipt` already has: re-evaluate, never trust the writer.

## Consequences

- Commit identity is pinned — fixed author, fixed committer, fixed dates derived from recorded
  attempt identity rather than the clock — so a replayed history is reproducible.
- The repository is a projection and may be rebuilt from manifests at any time. It is not a backup
  and losing it loses no evidence.
- A release can hand over the repository as the deliverable, which is the natural form for
  "maintainable Ascend C source".

## What this does not do

- **It does not change how a candidate is submitted.** `inherit_from_manifest_digest` stays as it is.
- **It does not make git readable by any gate**, oracle, or receipt.
- **It does not decide the release format.** Whether the delivered artifact is this repository, an
  archive of it, or a branch pushed somewhere is a later decision with its own evidence.
- **It answers the patch-interface question with evidence rather than settling it.** The record now
  exists over three real runs, so how large the actual diffs were is measurable. The interface does
  not change until that measurement is taken.
