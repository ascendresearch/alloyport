# Start here

- Session closeout: 2026-08-17 (second session that day; the first ends at `6302541`)
- Branch: `main`, working tree clean, **pushed** — `origin/main` is current
- 374 passing tests, 2 ignored because they require Docker and a real device
- **The whole verification baseline is green, including both boundary gates**, which had been red
  since 2026-08-13 and unrun by anyone. All nine violations are repaired
  ([`boundary-gates-red-20260817.md`](evidence/boundary-gates-red-20260817.md)). Locally they need
  `/home/dawei/.cache/opencode/bin/rg` on `PATH`; CI now installs ripgrep itself.
- CI's `rust` job passes, including both gates — the first time either has evaluated anything in CI.
  **`msrv` fails**: SIGSEGV in the `alloyport-server` test binary under Rust 1.88.0. Pre-existing,
  also failing on 2026-08-13, unexamined.
- **The deployment is current at `9e4a9c4`** — server, x86-64 Ascend worker, GB10-native aarch64
  worker. Both workers connected READY. Server, tunnels and workers were left running.

This is the lean entry point. [`CLAUDE.md`](../CLAUDE.md) is how this project decides what counts as
evidence. [`HANDOFF.md`](HANDOFF.md) is the accumulated architecture record — read it for what a
subsystem does, not for what to do next.

---

## 1. The one fact that should shape the next session

**The rebuild loop closed, and the blocker is now the build image rather than the harness.**
`task-002ee08d6d5540c05e5f7361` ran on the redeployed stack and produced seven candidates, six Source
Gate runs and **four builds** — each build after the first on a candidate the model had corrected
after reading the previous build's diagnostics. Read → correct → rebuild had never completed once
before; it completed three times
([`rebuild-loop-closed-20260817.md`](evidence/rebuild-loop-closed-20260817.md)).

**Then the control refuted the obvious conclusion.** Builds 2–4 all failed identically inside CANN's
own `kernel_operator.h`, on an include it could not resolve, and that looked like a broken image. It
is not: the repository's own person-written kernel, `fixtures/ascend-add-v1`, **compiles and links in
that exact image** using CANN's CMake `ASC` language package, which owns the include set.

What was broken is that **the prompt prescribed a method that cannot work** — a raw `ccec` command
line with one `-I` — and the model obeyed it for three of its last turns. The supported pattern is
documented in eight corpus files, every one of them among the 972 `read_reference` cannot serve, and
the model had asked for one of them by name a day earlier and been refused
([`ascend-build-path-20260817.md`](evidence/ascend-build-path-20260817.md)).

Walking that gate before the model did found a second trap: `MissingBuildReference` required every
generated source to appear in the build text, so the supported composition — one translation unit
listed, the kernel `#include`d — would have been refused. **This repository's own specimen would have
failed its own Source Gate.** Both are fixed; the image and its digest are untouched.

Nothing is correct yet. All four builds failed before the compiler formed any opinion about the
model's Ascend C, so no candidate has been shown to be wrong either.

**Across eight live runs, not one has failed because the model wrote a bad kernel.** Seven died in
the harness ([`fatal-harness-defects-20260816.md`](evidence/fatal-harness-defects-20260816.md)); the
eighth ran out of turns against a build image that cannot compile Ascend C. No compiler has yet
formed an opinion about a generated kernel.

---

## 2. What changed

**This session: [0044](design/0044-git-as-the-candidate-record.md) is accepted and implemented.**
`alloyport-server candidate-record TASK_ID --into DIRECTORY` projects one task's candidate lineage
into a real git repository — one commit per candidate, tree exactly that candidate, tagged
`c001-…` in submission order, gate outcomes and the compiler's first error in the message. Two
amendments to the decision as proposed, both recorded in it: the projection is built **after** a run
rather than during it (a commit written at submission time cannot carry the gate outcome the decision
also asked for, and nothing new belongs in a paid run's path), and it is a `alloyport-server`
subcommand rather than a CLI one (it reads the Episode database and the CAS directly). After importing
it re-reads every blob through `git cat-file` and rehashes it against the manifest digest.

It ran against the three real Episodes from 2026-08-16 and immediately said three things
([`candidate-record-20260817.md`](evidence/candidate-record-20260817.md)): the correction loop had not
closed, 18–20 corpus reads happen before the first line of Ascend C, and **a patch interface is not
worth building** — measured, a unified diff is 27–70% of the whole tree it changes and 113% across a
fork, against the 20× inheritance already banked. That question is now closed with evidence rather
than deferred.

**Then the baseline itself was repaired.** Nine violations, in five commits that each build, test,
and gate-check on their own: migration intake SQL moved behind the adapter boundary and the server's
task lifecycle onto a port; `correctness.rs` 1311 → 658 across measure / calibrate / evaluate;
`agent_runtime.rs` 901 → 372 across model turn and tool turn; `model.rs` 812 → 552; `gateway.rs`
886 → 522 across submission and recovery; `main.rs` 1069 → 590 across connection and rendering; and
three inline test modules moved to the `*_tests.rs` siblings this repository already uses.

Two things the gates caught while being repaired, which is what they are for: moving a `match` on
tool-name constants into a module where those constants were out of scope silently turned every arm
into a catch-all (three tests failed, clippy reported the unreachable arms), and the architecture
check noticed that `tools.reconcile` had moved file — a location assertion is how it notices a move
at all, so the path was updated with the code.

### The previous session (through `6302541`)

Nine defects fixed, each verified red against the code exactly as it shipped. In the order they
would bite again:

- **Citations the model could not obtain.** Gate results now name their receipt, so
  `request_ascend_build`, `read_build_diagnostics`, and the Correctness Gate became callable at all
  ([0042](design/0042-model-visible-receipt-references.md)).
- **`validate_call` never ran in production.** A decorator forwarded three trait methods and
  inherited a defaulted fourth, so Design 0040's whole correction path was dead while its tests
  passed. The default is deleted; omitting it is now a compile error.
- **Wrong citation versus broken machine** were one error variant. `Citation` is separate now, and
  one chokepoint turns it into a correction turn.
- **Retry guidance was computed and discarded.** `Never` stops, `AfterMillis` waits, and a
  byte-identical repeat is treated as deterministic instead of retried 21 times.
- **Failure diagnostics were hashed and thrown away** at six sites. They are published before they
  are recorded.
- **Budget exhaustion could not be represented.** The allowance left Episode identity, so a finished
  Episode is reopenable and each grant is recorded
  ([0043](design/0043-allowance-outside-episode-identity.md)).
- **The controller could not hand a worker anything it wrote itself**, and a half-enqueued
  assignment was unrecoverable. Both fixed at the funnel every dispatch path shares.
- **A complete bundle cost a whole model response.** A candidate may inherit the files it did not
  change: measured 16 384 → 788 output tokens on a live correction.
- **One quarantined NPU idled a seven-card worker.** Capacity is now the lesser of what may run at
  once and what is left to run it on.

Also: blocking Source Gate failures name the paths they are missing, rejections name the move that
fixes them, the build environment is stated instead of discovered, and the accelerator probe bound
is measured rather than asserted
([`device-probe-timeout-20260816.md`](evidence/device-probe-timeout-20260816.md)).

---

## 3. Do this next

**Make the Ascend build image able to compile Ascend C.** Everything else is now downstream of this;
four builds have failed without the compiler ever reaching the model's kernel. In order of how much
each assumes:

1. **Invoke the vendor's supported driver.** CANN ships `ascendc`/`bisheng` wrappers that set their
   own include paths. The trusted build runner building a raw `ccec` command line with one `-I` is
   this repository's choice, and it is the choice that fails. This assumes least: it stops the
   harness from having an opinion about CANN's internal layout.
2. **Or give the runner the full include set**, so `kernel_operator.h` resolves. That is a fact about
   the image and belongs in the image's contract, not in a candidate's `CMakeLists.txt`.
3. **Either way, tell the model what the toolchain layout is.** Its `GLOB_RECURSE` move shows it will
   use such a thing correctly; today it can only search the one subtree it was told about.

Walk the honest path first: build `fixtures/ascend-add-v1` — a kernel a person wrote — inside that
image by hand. If it does not compile either, no candidate could have.

**Then fix `alloyport-cli attach`**, which prints `run event sequence is invalid: run.started must be
the first event` and stops. Every observation in
[`rebuild-loop-closed-20260817.md`](evidence/rebuild-loop-closed-20260817.md) had to come from
reading SQLite and the CAS by hand. Watching a run is how you decide whether to keep paying for it.

**Then re-run the migration**, with the record as the way to read it:

```
alloyport-cli --config <client.json> migrate fixtures/migrations/cuda-reduction-v1 --retry
alloyport-server --config <server.json> candidate-record <task-id> --into <dir>
git -C <dir> log --all --graph --oneline
```

Deployment state is in `.alloyport-local/host-connections.md` — that file, not recollection. The
stack is already running at `9e4a9c4`; a rebuild is only needed if the worker changes.

---

## 4. Known open defects and gaps

1. **`read_reference` serves 127 of 1099 vendored files.** Not theoretical: the cards cite the other
   972 by path, the model followed those citations and was refused six times in one run, and in
   another it called the unreachable files "the critical piece". The ledger has one row per card, so
   serving sub-files is a ledger decision, not a reader change.
2. **Nothing observes that a candidate ran on the device.** Unchanged from the last session. Every
   verdict still carries `unverified: [device_execution, runner_attestation]`.
3. **`read_reference` serving 127 of 1099 files has now cost a migration, measured.** Eight corpus
   files document `find_package(ASC)`, the supported Ascend C build, and all eight are unreachable.
   The model asked for one of them by name on 2026-08-16 and was refused. See gap 1; this is the
   same gap with a price on it ([`ascend-build-path-20260817.md`](evidence/ascend-build-path-20260817.md)).
4. **`alloyport-cli attach` is broken.** `run event sequence is invalid: run.started must be the
   first event`, two lines in, on a run the server executed correctly. The operator has no live view.
5. **No context compaction, and corpus reading is what fills the context.** Improving: 9 reads before
   the first candidate and 14 total on the latest run, against 18–20 and 24 the day before, with the
   largest single input 72 916 rather than 98 815. Still nothing summarises, and no behaviour is
   defined for reaching the 128 000 ceiling. On the latest run **model turns**, not context, were the
   binding constraint — three of them spent on an error nothing could fix.
6. **Explaining a failed run still means reading SQLite by hand for anything but candidates.**
   `candidate-record` now covers candidate lineage and gate outcomes. Turn-level history, model
   attempts, and rejections are still hand-read JSON out of `episode.sqlite3`.
7. **A first submission has no headroom.** Inheritance fixes corrections, not the first candidate,
   which must be complete and measured 89.9% and 100.0% of the output ceiling on two runs. The model
   worked around it by writing tersely; a draft mechanism is the real answer and is unbuilt. A patch
   interface is **not** the answer and is now refuted with measurements, not argument
   ([`candidate-record-20260817.md`](evidence/candidate-record-20260817.md) §3).
8. **Performance has rules but no execution path**, and **knowledge has an admission gate but no
   store**. Both unchanged.
9. **The specimen is welded into the types**, and **the vendored corpus has no license text**. Both
   unchanged.
10. **Two states, `Created` and `CancellationPending`, do not permit `Failed`.** No runtime path was
   found that attempts it; that is an unverified absence, not a cleared one.
11. **CI's `msrv` job fails with a SIGSEGV** in the `alloyport-server` test binary under Rust
    1.88.0, the declared minimum. Pre-existing — it also failed on 2026-08-13 — and nobody has looked
    at it. The workspace's MSRV claim is therefore unverified.

---

## 5. Rules these sessions kept paying for

**Read the record, not the applicant — including your own record.** The claim that the correction loop
had closed was written from watching two runs live. The record says they were two migrations and no
rebuild ever finished. Nothing was dishonest; nobody went back and looked.

**A gate whose fallback is another runner must say whether that runner ran.** §4.9 said "CI still
runs them" while the same page's second line said nothing had been pushed. Both true, and together
they hid three days of a red baseline.

**Fix the class, not the site.** Corrected four times, and the fourth was caught by a compiler rather
than by reading. A net at one call site is a fix that buys exactly one more run.

**We apply "don't trust, verify" to the model and exempt ourselves.** Twice in one day, in the same
codebase: a test that took its digest from the gateway's return value, and a test that exercised a
gateway production never uses unwrapped. Both passed for months.

**A check that cannot fail proves nothing.** Three assertions written this session were true by
construction — a configured value equal to its default, a digest compared against a stored copy of
itself — and each was found by mutating the implementation rather than by rereading the test.

**A defect the model can read and fix must not end the migration.** Stated in `CLAUDE.md`, designed
in 0040, and dead in production for three days because a decorator inherited a default.
