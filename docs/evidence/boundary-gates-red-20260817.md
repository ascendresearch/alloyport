# Both boundary gates have been red since 2026-08-14, and nothing said so

- Date: 2026-08-17
- Found by: running them. `ripgrep` exists on the dev host at
  `/home/dawei/.cache/opencode/bin/rg`, which nobody had looked for.
- Status when found: `scripts/check_architecture_boundaries.sh` exit 1 with nine violations,
  `scripts/check_sql_boundaries.sh` exit 1 with one. Both are named in `CLAUDE.md` as part of the
  verification baseline.

## What is red

```
Architecture boundary check failed:
  crates/alloyport-core/src/correctness.rs             1311 lines (limit 800)
  crates/alloyport-candidate-tools/src/tests.rs        1531
  crates/alloyport-cli/src/main.rs                     1069
  crates/alloyport-worker/src/application/assembly.rs   913
  crates/alloyport-core/src/agent_runtime.rs            901
  crates/alloyport-candidate-tools/src/gateway.rs       886
  crates/alloyport-server/src/application/candidate_config.rs 827
  crates/alloyport-core/src/model.rs                    812
  server task lifecycle gained configuration, identity administration, or storage assembly:
    crates/alloyport-server/src/application/runtime.rs uses crate::migration_task::SqliteMigrationTaskStore

SQL or rusqlite escaped the SQLite implementation boundary:
  crates/alloyport-server/src/migration_task.rs
```

Eight are the module-size limit. The ninth and the SQL finding are the same change seen twice:
`migration_task.rs` holds `rusqlite` outside `adapters/sqlite/`, and `application/runtime.rs` depends
on it directly.

## When

Both checks were green at `b61e0c0` (2026-08-12) and red at `8a7b2d3` (2026-08-14), verified by
running them in a detached worktree at each commit. So the baseline has been red for roughly three
days and about thirty commits.

## Why nobody knew

Three things had to line up, and they did.

1. **`rg` is not on the dev host's `PATH`**, so both scripts exit 2 on every local run. That is
   correct behaviour — `CLAUDE.md` requires a gate that cannot run to fail loudly rather than print a
   pass — and it means the local verification baseline has been two commands that never gave a verdict.
2. **`NEXT_SESSION.md` §4.9 recorded the situation and then supplied a false comfort**: *"They exit 2
   rather than printing a pass. CI still runs them."* CI does run them, on push. **Nothing has been
   pushed.** The same document's header says so in its second line: *"working tree clean, nothing
   pushed"*. Two true sentences, four pages apart, whose conjunction is that no one has run these
   gates since 2026-08-14.
3. **Nobody looked for a ripgrep.** One was sitting in `~/.cache/opencode/bin`.

This is `CLAUDE.md`'s one mistake in a new costume. *We apply "don't trust, verify" to the model and
exempt ourselves* — and here what went unverified was the verifier. The gate did not lie; the record
of the gate did, by pairing "it cannot run here" with "CI covers it" while CI was unreachable.

The general form is worth keeping: **a gate whose fallback is another runner must state whether that
runner has actually run.** "CI still runs them" is a claim about a fact — the last push — and it was
never checked against one.

## What was and was not done about it

- **Not fixed.** Eight module splits and one layering repair are a refactor of their own, and this
  session's work was Design 0044.
- **Not made worse.** The 0044 implementation adds five modules, the largest 497 lines, and adds no
  line to `gateway.rs` (886) or `agent_runtime.rs` (901). Re-running both scripts after it reports the
  same nine violations and no new one.
- **The scripts were not weakened** to run without ripgrep. The content greps genuinely need it, and
  a gate that degrades to a partial pass is the defect this file is about.

## The one thing that should change first

`NEXT_SESSION.md` §4.9 should not say "CI still runs them" unless something has been pushed. Either
push, or state that the gates are unrun and red. The cheapest durable fix is a `PATH` that finds a
ripgrep on this host, so the local baseline gives a verdict again.
