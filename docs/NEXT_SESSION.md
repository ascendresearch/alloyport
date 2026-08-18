# Start here

- Session closeout: 2026-08-18 (the session began 2026-08-17; the one before it ends at `6302541`)
- Branch: `main`, working tree clean, **pushed** — `origin/main` is current
- `git log --oneline 6302541..HEAD` — 22 commits
- 386 passing tests, 2 ignored because they require Docker and a real device
- **The whole verification baseline is green**, including both boundary gates, which had been red
  since 2026-08-13 and which nobody had run
- CI: `rust` and `portable-linux` pass. **`msrv` fails** — SIGSEGV in the `alloyport-server` test
  binary under Rust 1.88.0. Pre-existing, also failing on 2026-08-13, unexamined.

This is the lean entry point. [`CLAUDE.md`](../CLAUDE.md) is how this project decides what counts as
evidence. [`HANDOFF.md`](HANDOFF.md) is the accumulated architecture record — read it for what a
subsystem does, not for what to do next.

## What is deployed and running

| | identity | state |
|---|---|---|
| controller | `41ce7ab3…`, current with HEAD | running, `0.0.0.0:50051` |
| Ascend worker (x86-64) | `f2635a2e…`, current with HEAD | running, connected |
| Ascend image (build + correctness) | `sha256:17b67083…` | pinned in three places, see below |
| GB10 worker (aarch64) | `91af4250…`, **stale** (built at `9e4a9c4`) | **down** |

One reverse tunnel per host. `.alloyport-local/host-connections.md` is the authority for paths,
digests, and how to restart any of it — that file, not recollection.

---

## 1. The one fact that should shape the next session

**Every obstacle between the model and a compiled kernel was ours, and they are now removed except
one.** Eight live migrations have run. Not one failed because the model wrote a bad kernel; no
compiler has ever formed an opinion about a generated kernel at all.

The last two obstacles are worth stating together because they were the same mistake in different
clothes:

- **We told the model to compile the wrong way.** The prompt listed `kernel_operator.h`'s directory
  and a `ccec` binary — every path real, every path probed, nobody had ever compiled with them. That
  invocation cannot work, and the model spent three of its last turns obeying it. CANN's CMake `ASC`
  language package is the supported path; it is documented in eight corpus files, all eight among the
  972 that `read_reference` cannot serve, one of which the model had asked for by name and been
  refused. Fixed; the model immediately took the supported path and its first candidate passed the
  Source Gate on the first attempt.
- **Our own Source Gate would have refused the supported composition.** `MissingBuildReference`
  required every generated source to appear in the build text, while the supported build lists one
  translation unit and `#include`s the kernel into it. `fixtures/ascend-add-v1` — a kernel a person
  wrote, which compiles — would have failed its own gate. Fixed.

What remains is one device coupling: `prepare_attempt` still leases and health-checks a card for
every attempt, including a build that opens none. See §3.

---

## 2. What changed

**Design 0044 is implemented.** `alloyport-server candidate-record TASK_ID --into DIRECTORY` projects
a task's candidate lineage into a real git repository — one commit per candidate, tree exactly that
candidate, tagged in submission order, gate outcomes and the compiler's first error in the message.
Built after a run rather than during it, because a commit written at submission time cannot carry the
gate outcome the design also asked for, and nothing new belongs in a paid run's path. It re-reads
every blob through `git cat-file` and rehashes it against the manifest.

It immediately corrected the previous closeout: `acl/acl.h` and `kernel_operator.h` were two separate
migrations, not one loop going deeper, and **no corrected candidate had ever been rebuilt**. It also
measured that a patch interface is not worth building — a unified diff is 27–70% of the tree it
changes, 113% across a fork, against the 20× inheritance already banked.

**The verification baseline was repaired.** Nine boundary violations, in five commits that each
build, test, and gate-check on their own: intake SQL moved behind the adapter boundary, and seven
modules that each held two jobs split along the seam. CI now installs the ripgrep both gates refuse
to run without — they had never evaluated anything in CI.

**The Ascend build path was straightened.** The prompt states what the toolchain provides instead of
prescribing a command line; the Source Gate asks about reachability; the image states its own
toolchain contract as OCI labels and was re-pinned (`sha256:521fea11…` → `sha256:17b67083…`).

**Four fatal defects the runs surfaced.** A stop-feedback re-ask that never rebound its input digest
and killed an Episode before any provider call; two producers both starting one run, which made
`attach` refuse every migration; `attach` aborting on a contract violation instead of showing the
run; and a build asking for an accelerator it never opens.

**Capacity and readiness are split by role.** A worker advertises device-bound and device-free
capacity, the readiness preflight asks for the one the role consumes, and it waits only for builders
— verifiers are deferred to the Correctness Gate, because a run that has never compiled anything
should not be stopped by a gate it may never reach.

---

## 3. Do this next

**1. Find out why the worker preflights device 0.** The last run had a Source-Gate-passing candidate,
dispatched a build, and the worker failed repeatedly on `device 0 is Unhealthy` while devices 3 and 4
were `OK` and process-free. That is not explained. The likely thread: `AscendRuntime::new` constructs
its supervisor against `inventory[0]`, and the execution path may not be using the per-attempt
runtime `for_attempt` returns. Start there, and read before changing — a shortcut in this exact area
was tried and reverted on 2026-08-17 for making things worse.

**2. Then decide what a device-free build receipt says.** `AscendRunReceipt` attests `device`,
`lease`, and both device observations for every attempt. A build that genuinely holds no card needs
that receipt able to say *no device*. That decision, not a runtime branch, is what removes the last
coupling.

**3. Then get one build to succeed.** Everything else is downstream: no compiler has yet judged a
generated kernel. The candidate from `task-162932dc10916e06aa0b88d2` is durable and readable with
`candidate-record`; it uses the supported ASC build and has visible defects a compiler will name
(`$ASCEND_HOME_PATH` where CMake needs `$ENV{…}`, `lib/` for `lib64/`, `find_package` after
`project`). Those are the errors a correction turn exists to fix.

Host conditions to check first, because both are outside this repository: the GB10's `nvidia-smi`
returns status 9, and Ascend devices 0–2 sit in `Alarm` health.

---

## 4. Known open defects and gaps

1. **`read_reference` serves 127 of 1099 vendored files**, and the cost is now measured: the eight
   files documenting the supported Ascend C build are all unreachable, and the model asked for one by
   name and was refused. The ledger has one row per card, so serving sub-files is a ledger decision.
2. **Nothing observes that a candidate ran on the device.** Every verdict still carries
   `unverified: [device_execution, runner_attestation]`.
3. **A build still leases a card through the device guard**, which requires `Ready` and process-free.
   Removing it needs `AscendRunReceipt` to be able to say *no device*.
4. **A cancelled task leaves its accepted attempt on the worker forever.** `task-498e257f…` was
   cancelled and its build attempt held the worker's only concurrency slot until the row was deleted
   by hand. With `max_concurrency: 1` this bricks a worker silently.
5. **No context compaction.** Improving — 9 corpus reads before the first candidate on one run,
   against 18–20 the day before — but nothing summarises and no behaviour is defined for the 128 000
   ceiling.
6. **Explaining a failed run means reading SQLite by hand** for anything but candidate lineage, which
   `candidate-record` now covers.
7. **A first submission has no headroom.** Inheritance fixes corrections, not the first candidate. A
   patch interface is **not** the answer and is refuted with measurements.
8. **Performance has rules but no execution path**, and **knowledge has an admission gate but no
   store**.
9. **The specimen is welded into the types**, and **the vendored corpus has no license text**.
10. **Two states, `Created` and `CancellationPending`, do not permit `Failed`.** An unverified
    absence, not a cleared one.
11. **CI's `msrv` job fails with a SIGSEGV** under Rust 1.88.0, the declared minimum. The MSRV claim
    is unverified.
12. **The GB10 worker is stale and down**, and its host's NVIDIA driver is not answering. It serves
    only the Correctness Gate, which nothing has reached.

---

## 5. Rules these sessions kept paying for

**Read the record, not the applicant — including your own record.** The claim that the correction
loop had closed was written from watching runs live. The record said otherwise, in one command.

**A probe that lists ingredients is not a recipe that was cooked.** Every path in the build-environment
prompt was real and individually probed. Nobody had compiled with them, and the combination could not
work. Same shape as the tolerance nobody measured.

**Walk the honest path through every gate you tighten, yourself, before the model does.** Doing it
found that this repository's own person-written kernel would have failed this repository's own Source
Gate.

**A gate whose fallback is another runner must say whether that runner ran.** "CI still runs them"
was true and useless: nothing had been pushed, and the runner had no ripgrep either.

**Fix the class, not the site.** A build's device requirement lived in four places. Fixing three left
it blocked, and the fourth is a receipt schema, not a branch.

**A check that cannot fail proves nothing** — and a change nothing can catch is the same defect. The
mutation that ignored a worker's role compiled cleanly and no test noticed until the decision was
lifted out of the loop it was buried in.

**When you move fast in a device path, you break a device path.** A shortcut that pinned builds to
`inventory[0]` was worse than what it replaced and had to be reverted the same evening.
