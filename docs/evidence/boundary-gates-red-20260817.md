# Both boundary gates had been red since 2026-08-14, and nothing said so

- Date: 2026-08-17
- Found by: running them. `ripgrep` exists on the dev host at
  `/home/dawei/.cache/opencode/bin/rg`, which nobody had looked for.
- Status when found: `scripts/check_architecture_boundaries.sh` exit 1 with nine violations,
  `scripts/check_sql_boundaries.sh` exit 1 with one. Both are named in `CLAUDE.md` as part of the
  verification baseline.
- Status now: **both pass.** See [Both gates are now green](#both-gates-are-now-green).

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

Four things had to line up, and they did.

1. **`rg` is not on the dev host's `PATH`**, so both scripts exit 2 on every local run. That is
   correct behaviour — `CLAUDE.md` requires a gate that cannot run to fail loudly rather than print a
   pass — and it means the local verification baseline has been two commands that never gave a verdict.
2. **`NEXT_SESSION.md` §4.9 recorded the situation and then supplied a false comfort**: *"They exit 2
   rather than printing a pass. CI still runs them."* Nothing had been pushed since 2026-08-13. The
   same document's header says so in its second line: *"working tree clean, nothing pushed"*. Two true
   sentences, four pages apart, whose conjunction is that no one had run these gates in days.
3. **Nobody looked for a ripgrep.** One was sitting in `~/.cache/opencode/bin`.
4. **The runner has no ripgrep either.** Found by pushing: the first CI run in four days failed with
   `SQL boundary check cannot run: ripgrep (rg) is not installed` and exit 2. So the fallback runner
   could not run these gates at all, and the one CI run that did report an architecture violation —
   `31665174485` on 2026-08-13 — was from an older image that still carried `rg`. The workflow now
   installs it and prints its version, the same way the portable-linux job installs `musl-tools`.

   The 2026-08-13 run is worth its own line: CI **did** evaluate the architecture gate, it **did**
   fail, and four more days of work went on top. That run also failed `cargo test --workspace
   --locked` on the declared MSRV. So the gate's report was available and unread, which is a
   different failure from the gate being unable to run — and this repository managed both at once.

This is `CLAUDE.md`'s one mistake in a new costume. *We apply "don't trust, verify" to the model and
exempt ourselves* — and here what went unverified was the verifier. The gate did not lie; the record
of the gate did, by pairing "it cannot run here" with "CI covers it" while CI was unreachable.

The general form is worth keeping: **a gate whose fallback is another runner must state whether that
runner has actually run.** "CI still runs them" is a claim about a fact — the last push — and it was
never checked against one.

## Both gates are now green

```
Architecture boundary check passed; production modules <= 800 lines and plugin ports are
abstract and typed
SQL boundary check passed; legacy database modules remaining: 0
```

Five commits, each of which builds, tests, and gate-checks on its own — verified by running the
suite and both scripts at every one in a detached worktree. The gates go green monotonically and
never regress:

| commit | what it did | arch | sql |
|---|---|---|---|
| `6a69e8c` | migration intake SQL behind the adapter boundary; `runtime.rs` on a port | fail | **pass** |
| `a6e07a6` | `correctness.rs` 1311 → 658, three child modules | fail | pass |
| `f67cf42` | `agent_runtime.rs` 901 → 372, `model.rs` 812 → 552 | fail | pass |
| `87c6c82` | `gateway.rs` 886 → 522, its 1531-line test module split three ways | fail | pass |
| `f9d6391` | `candidate_config.rs`, worker `assembly.rs`, `main.rs` 1069 → 590 | **pass** | pass |

### What the nine violations actually were

Two were real architecture, seven were one module holding two jobs.

- **`migration_task.rs` held `rusqlite` and nine SQL statements**, and `application/runtime.rs`
  depended on the concrete store. Both findings were one change: the module keeps the model and gains
  a `MigrationTaskStore` port, the SQLite implementation moved beside the other stores, and only the
  composition root names it. Its `submit` took eight positional arguments, five of them strings; it
  now takes one struct, so transposing an owner and a request identity is a compile error.
- **The size violations each had an obvious seam** — measure/calibrate/evaluate in the oracle, model
  turn versus tool turn in the agent loop, catalogue versus attempt in the model module, submission
  versus recovery in the gateway, connection versus rendering in the CLI. Child modules were used
  throughout so no receipt or record had to widen a private field to be split.
- **Three inline test modules** moved to the `*_tests.rs` sibling this repository already uses
  everywhere else. That is convention, not gate-avoidance: two of the three files were left with
  683 and 732 lines of production code, comfortably inside the limit either way.

### Two things the gates caught while being repaired, which is the point

- **Moving a `match` on tool-name constants silently turned it into a catch-all.** In
  `gateway_recovery.rs` the constants were not in scope, so Rust read `READ_REFERENCE_TOOL` as a fresh
  binding and every arm matched everything. Three tests failed and clippy reported the unreachable
  arms. Nothing about the diff looked wrong; the compiler and the suite are what found it.
- **The architecture check pinned a behaviour to a filename.** It asserted
  `tools.reconcile(&invocation).await` appears in `agent_runtime.rs`; the call moved to
  `agent_runtime_tools.rs` with the method that makes it. The gate failed, the path was updated with
  the code, and that is the gate working — a location assertion is how it notices a move at all.

### What was deliberately not done

- **The scripts were not weakened** to run without ripgrep, and the 800-line limit was not raised.
  The content greps genuinely need `rg`, and a gate that degrades to a partial pass is the defect
  this file is about.
- **The 800-line limit was not raised** and no exemption was widened.
