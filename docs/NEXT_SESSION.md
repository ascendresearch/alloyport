# Start here

- Session closeout: 2026-08-17
- Branch: `main`, working tree clean, **nothing pushed**
- This session sits on top of `80769dd`; see `git log --oneline 80769dd..HEAD`
- 361 passing tests, 2 ignored because they require Docker and a real device

This is the lean entry point. [`CLAUDE.md`](../CLAUDE.md) is how this project decides what counts as
evidence. [`HANDOFF.md`](HANDOFF.md) is the accumulated architecture record — read it for what a
subsystem does, not for what to do next.

---

## 1. The one fact that should shape the next session

**Model-authored Ascend C reached a real compiler for the first time.** Seven migrations ran on the
live deployment. One passed the Source Gate, dispatched to the NPU worker, and got an answer in
270 ms:

```
fatal error: acl/acl.h: No such file or directory
```

Then it read that message through `read_build_diagnostics`, fixed the include path, and the next
build failed one layer deeper on `kernel_operator.h`. **The correction loop closed.** That is the
thing 71 000 lines of control plane existed to make possible, and until this session it had never
run.

Nothing is correct yet. No candidate has compiled successfully, so no Correctness Gate has ever
judged a generated kernel, and every mechanism downstream of Build remains unproven on hardware.

**Not one of the seven runs failed because the model wrote a bad kernel.** Every failure was in the
harness. That is the second fact, and it is why most of this session is fixes rather than features:
[`fatal-harness-defects-20260816.md`](evidence/fatal-harness-defects-20260816.md) lists them with
the run that found each.

---

## 2. What changed

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
next compiler error moves again.

**Then decide [0044](design/0044-git-as-the-candidate-record.md).** It proposes projecting candidate
lineage into a real git repository so the attempt history is readable, and explicitly rejects git as
the store and as the model-facing interface, with reasons. Nothing is implemented.

---

## 4. Known open defects and gaps

1. **`read_reference` serves 127 of 1099 vendored files.** Not theoretical: the cards cite the other
   972 by path, the model followed those citations and was refused six times in one run, and in
   another it called the unreachable files "the critical piece". The ledger has one row per card, so
   serving sub-files is a ledger decision, not a reader change.
2. **Nothing observes that a candidate ran on the device.** Unchanged from the last session. Every
   verdict still carries `unverified: [device_execution, runner_attestation]`.
3. **No context compaction.** One run climbed 4 130 → 98 815 input tokens against a 128 000 ceiling
   at roughly 10 k per corpus read. Nothing summarises and no behaviour is defined for reaching it.
4. **Explaining a failed run means reading SQLite and the CAS by hand.** Diagnostics are published
   now, so there is something worth reading; there is still no command that reads it.
5. **A first submission has no headroom.** Inheritance fixes corrections, not the first candidate,
   which must be complete and measured 89.9% and 100.0% of the output ceiling on two runs. The model
   worked around it by writing tersely; a draft mechanism is the real answer and is unbuilt.
6. **Performance has rules but no execution path**, and **knowledge has an admission gate but no
   store**. Both unchanged.
7. **The specimen is welded into the types**, and **the vendored corpus has no license text**. Both
   unchanged.
8. **Two states, `Created` and `CancellationPending`, do not permit `Failed`.** No runtime path was
   found that attempts it; that is an unverified absence, not a cleared one.
9. **Neither boundary script runs on the dev host** (`rg` absent). They exit 2 rather than printing
   a pass. CI still runs them.

---

## 5. Rules this session kept paying for

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
