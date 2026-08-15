# Start here

- Session closeout: 2026-08-14
- Branch: `main`, working tree clean, **nothing pushed**
- Tip: `a79a1a6`. This session added three commits; `git log --oneline -3`.

This is the lean entry point. [`HANDOFF.md`](HANDOFF.md) remains the accumulated architecture
record — read it for what a subsystem does, not for what to do next. Design documents state intended
behavior; tests and code state what this revision implements.

---

## 1. The one fact that should shape the next session

**The product has never produced its product.** 69 109 lines of Rust, 40 design documents, 314
tests, an authenticated mTLS deployment across two real hosts — and by the record's own account, no
generated Ascend C has ever been compiled and judged correct. The only `PASS` on hardware is
`fixtures/ascend-add-v1`, an add kernel a person wrote.

Effort has not gone where the risk is:

| | lines | |
|---|---:|---|
| `alloyport-worker` + `alloyport-server` | 42 995 | control plane: mTLS, CAS, quotas, GC, leases, outboxes |
| `alloyport-core` | 13 925 | domain, gates, oracle, agent loop |
| everything else | 12 189 | |

`PRODUCT_EXECUTION_PLAN.md` already froze infrastructure for this reason. The freeze is the right
call; the next session's job is to spend the budget on the first genuine migration, not on the
platform around it.

---

## 2. What this session changed

Three commits, each verified on its own in a detached worktree (build, tests, `fmt --check`,
`clippy -D warnings`), so the history bisects cleanly.

| commit | | tests |
|---|---|---:|
| `8a7b2d3` | Return malformed model tool arguments to the model | 307 |
| `3acb3c4` | Derive the correctness tolerance from a measured floor | 311 |
| `a79a1a6` | State the product boundary in the Source Gate instead of a method | 314 |

The decision record is [Design 0040](design/0040-measured-tolerance-and-advisory-source-gate.md),
which supersedes parts of 0005, 0027, and 0028. The measurement behind it is
[`evidence/reduction-noise-floor-20260814.md`](evidence/reduction-noise-floor-20260814.md).

The finding worth carrying forward, because it was invisible until something measured it:
`alloyport_reduce_sum_blocks` ends in `atomicAdd`, so the CUDA authority **does not reproduce
itself**. On the archived GB10 record it disagrees with itself by `1.95e-03` absolute and
`5.20e-07` relative. Against that spread the frozen tolerance was wrong in both directions at once
— its relative term 38× looser, its absolute term 19.5× tighter. Nobody had ever compared the two
numbers, and its ten-mutant battery could not have revealed it: every mutant was orders of magnitude
larger than the tolerance it was testing.

---

## 3. Do this next

**Run the first real migration end to end.** Everything below it is deferred until that finishes.

1. Rebuild and redeploy only the server. Restart the server and both persistent worker/tunnel
   processes from `.alloyport-local/host-connections.md` — that file, not recollection, is the
   authority for SSH targets, tunnels, and installed paths.
2. Retry the existing migration through `alloyport-cli`. Observe which Ascend device the durable
   lease selects.
3. Continue through Source → sequential Ascend Build → paired CUDA/Ascend Correctness, and capture
   the immutable receipts.

Three things are new and unproven on hardware. Watch them, and record what they say rather than
adjusting anything by hand:

- **`reorder_output_bits`.** The trusted CUDA harness now computes a pairwise-tree sum beside the
  authority. If that order disagrees with the reference by much more than its run-to-run spread, the
  derived tolerance widens and calibration will start listing mutants in `undetected` instead of
  silently passing. That report is the signal. Widening anything by hand would put the guess back.
- **`battery_scope: ComparatorOnly`** on every calibration receipt. Nothing yet shows a broken
  kernel would be caught before the comparator sees it.
- **The advisory Source Gate.** A candidate can now reach the Build Gate carrying
  `UnrecognizedKernelStructure`. If the compiler then rejects it, that is the gate working as
  intended — the compiler is the structural judge. If a candidate that *should* have been stopped
  gets through to a wrong Correctness verdict, that is a real finding and belongs in evidence.

---

## 4. Known open defects and gaps

Ordered by how much each would cost to leave. None is scheduled; all are observations with their
evidence, not adopted decisions.

1. **The specimen is welded into the types and the trusted harness — but not into the wire.**
   Measured: `Reduction*` appears 351 times in `alloyport-core`, 101 in `alloyport-server`, 92 in
   `alloyport-candidate-tools`, 75 in `alloyport-worker`, and **0 times in `alloyport-proto`**. The
   executor kinds are role×backend (`CUDA_CORRECTNESS`, `ASCEND_CORRECTNESS`, `ASCEND_BUILD`), so a
   second operator family reuses them unchanged.

   *An earlier revision of this list claimed a second specimen would cost a protocol version. It
   does not; the claim had never been checked against the proto. Corrected here rather than quietly
   edited, because the same discipline applies to this document.*

   The expensive part is the trusted correctness harness, which is inside the trust boundary and
   hard-codes the specimen: the callable ABI, the call sites, and both CMake target names. Note it
   is `include_str!`'d into the worker binary and materialized at execution time, so changing it
   costs a worker redeploy and **not** an image rebuild. `PRODUCT_EXECUTION_PLAN.md` phase P4
   assumes onboarding costs a manifest; it currently costs a code change in four crates.
2. **Nothing observes that the candidate ran on the device.** This is the real shape of what used
   to be listed here as "the harness self-reports two oracle inputs". Plain host C++ that sums the
   input passes the Source Gate (its kernel-structure check is advisory since 0040 — that change
   widened this), compiles on the Ascend build worker because it is just C++, links, is called
   through the same ABI, and matches the authority exactly. `implementation_invoked` and
   `synchronized` are literals emitted by the trusted runner, so they cannot catch it.

   Partly addressed: every verdict now carries `unverified: [device_execution, runner_attestation]`,
   so a `PASS` states this rather than implying the opposite, and the two mutants that flipped those
   literals are retired from the battery — a mutant no real candidate can produce was scoring a
   guaranteed detection. **The observation itself is still missing.** The cheapest candidate is a
   link-time check that the built candidate depends on the ACL runtime; it is necessary rather than
   sufficient, and it cannot be designed honestly without a device to try it against.
3. **No performance evidence path exists.** `grep -ril 'roofline\|throughput\|speedup' crates/*/src`
   returns nothing. Design 0006 is unimplemented. The product sentence says "migration and
   optimization factory"; the second half currently has no gate, no metric, and no receipt.
4. **No knowledge lifecycle.** Design 0008 is unimplemented; outside of `acknowledge*` the word
   appears twice in the workspace, both as "durable local attempt knowledge" in the worker.
   Acceptable while the first migration is unfinished, but the
   promote/retract gates are needed the moment there are verdicts worth keeping.
5. **The Agent has four tools** — submit, source gate, build, correctness — and no way to inspect,
   measure, or learn. That is enough to iterate on a candidate and not enough to diagnose one. It is
   also the prerequisite for (3) and (4).
6. **Neither boundary script can run on the dev host** (`rg` is absent). They now exit 2 saying so
   instead of printing a pass; CI still runs them. Nothing has verified the architecture boundaries
   locally this session.
7. **Slow shutdown drain.** The server can report `server tasks did not drain within 10s` after
   Ctrl-C. `cc511dc` stopped it from overwriting the primary diagnostic; the drain itself is
   undiagnosed.

---

## 5. Two rules this session kept paying for

Both are cheap to state and were each violated by working code here.

**Read the record, not the applicant.** A field the subject fills in is a claim. The tolerance was
supplied by its caller; a calibration receipt's stored `passed` was believed on read; a rejection
named a digest with no artifact behind it. All three are now derived or recomputed from what was
actually recorded.

**A gate must accept the evidence it demands, and must fail when it cannot run.** A gate tighter
than the task's own noise rejects correct work. A battery of sledgehammers cannot locate an edge. A
boundary check that swallows a missing tool prints a pass. Before tightening any gate here, walk the
honest path through it yourself first.
