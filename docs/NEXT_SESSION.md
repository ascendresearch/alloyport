# Start here

- Session closeout: 2026-08-17 (second session that day; the first ends at `6302541`)
- Branch: `main`, working tree clean, **nothing pushed**
- See `git log --oneline 80769dd..HEAD`
- 373 passing tests, 2 ignored because they require Docker and a real device
- **Both boundary gates are red and have been since 2026-08-14.** They are named in `CLAUDE.md` as
  part of the verification baseline and nothing had run them, because `rg` is not on this host's
  `PATH` and nothing has been pushed to CI. There is a ripgrep at
  `/home/dawei/.cache/opencode/bin/rg`. Nine violations, none of them new this session:
  [`boundary-gates-red-20260817.md`](evidence/boundary-gates-red-20260817.md).

This is the lean entry point. [`CLAUDE.md`](../CLAUDE.md) is how this project decides what counts as
evidence. [`HANDOFF.md`](HANDOFF.md) is the accumulated architecture record — read it for what a
subsystem does, not for what to do next.

---

## 1. The one fact that should shape the next session

**Model-authored Ascend C reached a real compiler, twice, and no corrected candidate has ever been
rebuilt.** Seven migrations ran on the live deployment. Two passed the Source Gate, dispatched to the
NPU worker, and got real compiler answers — `acl/acl.h: No such file or directory` in one run,
`kernel_operator.h: No such file or directory` in another. In both, the model read the message through
`read_build_diagnostics` and submitted a correction; in one the correction passed the Source Gate.

**Read → correct → submit closed. Read → correct → rebuild has never completed.** One run's budget
ended one operation after the corrected submission; the other's rebuild was still `running` when the
session stopped.

The earlier version of this section said the two compiler errors were one loop going a layer deeper.
They are two separate migrations on differently structured candidates. That claim was assembled from
watching runs live and was never checked against the record; the candidate record built this session
checks it in one command
([`candidate-record-20260817.md`](evidence/candidate-record-20260817.md) §1).

Nothing is correct yet. No candidate has compiled successfully, so no Correctness Gate has ever
judged a generated kernel, and every mechanism downstream of Build remains unproven on hardware.

**Not one of the seven runs failed because the model wrote a bad kernel.** Every failure was in the
harness. That is the second fact, and it is why most of this session is fixes rather than features:
[`fatal-harness-defects-20260816.md`](evidence/fatal-harness-defects-20260816.md) lists them with
the run that found each.

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

Also found: **both boundary gates are red**, see the header.

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

**Rebuild and redeploy all three binaries, then run the migration.** The deployed server and both
workers predate this session's changes, and the worker changed — capacity accounting and both
container runner scripts are compiled into it, so a server-only deploy will not do.

1. Rebuild: `cargo build --release -p alloyport-server -p alloyport-cli`, and the workers per
   [`portable-linux-builds.md`](portable-linux-builds.md) for x86-64 plus a native build on the GB10
   for aarch64.
2. Restart the server, both reverse tunnels, and both workers from
   `.alloyport-local/host-connections.md` — that file, not recollection, is the authority.
3. `alloyport-cli migrate fixtures/migrations/cuda-reduction-v1 --retry`, then `attach`.

The migration will resume the loop where it stalled: the model must point its build files at the
CANN include paths, which the deployment prompt now states. Watch whether it does, and whether the
next compiler error moves again. **The thing to watch for specifically is a rebuild of a corrected
candidate** — that has never completed, and both previous attempts ran out of room before it.

Afterwards, build its record and read it: `alloyport-server --config <server.json> candidate-record
<task-id> --into <dir>`, then `git -C <dir> log --all --graph --oneline`.

**Then repair the two boundary gates.** Eight module splits and one layering fix
([`boundary-gates-red-20260817.md`](evidence/boundary-gates-red-20260817.md)). Until they are green,
`CLAUDE.md`'s verification baseline is four commands and two absent verdicts.

---

## 4. Known open defects and gaps

1. **`read_reference` serves 127 of 1099 vendored files.** Not theoretical: the cards cite the other
   972 by path, the model followed those citations and was refused six times in one run, and in
   another it called the unreachable files "the critical piece". The ledger has one row per card, so
   serving sub-files is a ledger decision, not a reader change.
2. **Nothing observes that a candidate ran on the device.** Unchanged from the last session. Every
   verdict still carries `unverified: [device_execution, runner_attestation]`.
3. **No context compaction, and corpus reading is what fills the context.** One run climbed
   4 130 → 98 815 input tokens against a 128 000 ceiling at roughly 10 k per corpus read. The record
   now names the volume: **18 and 20 `read_reference` calls before the first line of Ascend C**, 24 of
   32 operations in one run. Nothing summarises and no behaviour is defined for reaching the ceiling.
4. **No corrected candidate has ever been rebuilt.** Both runs that reached a compiler read the
   diagnostics, corrected, and then ran out of room: one exhausted its budget one operation after the
   corrected submission, the other left its rebuild `running`. This is the next thing a live run has
   to demonstrate, and it is one hop, not a new mechanism.
5. **Explaining a failed run still means reading SQLite by hand for anything but candidates.**
   `candidate-record` now covers candidate lineage and gate outcomes. Turn-level history, model
   attempts, and rejections are still hand-read JSON out of `episode.sqlite3`.
6. **A first submission has no headroom.** Inheritance fixes corrections, not the first candidate,
   which must be complete and measured 89.9% and 100.0% of the output ceiling on two runs. The model
   worked around it by writing tersely; a draft mechanism is the real answer and is unbuilt. A patch
   interface is **not** the answer and is now refuted with measurements, not argument
   ([`candidate-record-20260817.md`](evidence/candidate-record-20260817.md) §3).
7. **Performance has rules but no execution path**, and **knowledge has an admission gate but no
   store**. Both unchanged.
8. **The specimen is welded into the types**, and **the vendored corpus has no license text**. Both
   unchanged.
9. **Two states, `Created` and `CancellationPending`, do not permit `Failed`.** No runtime path was
   found that attempts it; that is an unverified absence, not a cleared one.
10. **Both boundary gates are red**, and were unrun for three days because `rg` is off this host's
    `PATH` and nothing has been pushed to CI. Nine violations, none new this session
    ([`boundary-gates-red-20260817.md`](evidence/boundary-gates-red-20260817.md)). A ripgrep exists at
    `/home/dawei/.cache/opencode/bin/rg`.

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
