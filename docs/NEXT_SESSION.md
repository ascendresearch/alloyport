# Start here

- Session closeout: 2026-08-16
- Branch: `main`, working tree clean, **nothing pushed**
- This work sits on top of `79f914b`; see `git log --oneline 79f914b..HEAD`
- 340 passing tests, 2 ignored because they require Docker and a real device

This is the lean entry point. [`CLAUDE.md`](../CLAUDE.md) is how this project decides what counts as
evidence. [`HANDOFF.md`](HANDOFF.md) is the accumulated architecture record — read it for what a
subsystem does, not for what to do next. Design documents state intended behavior; tests and code
state what this revision implements.

---

## 1. The one fact that should shape the next session

**The product has never produced its product.** 71 217 lines of Rust, 41 design documents, 340
tests, an authenticated mTLS deployment across two real hosts — and no generated Ascend C has ever
been compiled and judged correct. The only `PASS` on hardware is `fixtures/ascend-add-v1`, an add
kernel a person wrote.

Effort has not gone where the risk is:

| | lines | |
|---|---:|---|
| `alloyport-worker` + `alloyport-server` | 43 107 | control plane: mTLS, CAS, quotas, GC, leases, outboxes |
| `alloyport-core` | 15 157 | domain, gates, oracle, agent loop, evidence rules |
| everything else | 12 953 | |

`PRODUCT_EXECUTION_PLAN.md` froze infrastructure for this reason, and the freeze is right. Two
sessions of gate and instrument work are now done. **The next thing that should happen is a real
migration**, because most of what remains cannot be designed honestly without one.

---

## 2. What the last two sessions changed

Recorded in [Design 0040](design/0040-measured-tolerance-and-advisory-source-gate.md) and
[Design 0041](design/0041-instruments-and-evidence-domains.md), with the measurement behind 0040 in
[`evidence/reduction-noise-floor-20260814.md`](evidence/reduction-noise-floor-20260814.md).

Gates corrected:

- A malformed tool argument no longer ends the migration; it becomes a recoverable rejection
  carrying a readable artifact.
- The correctness tolerance is derived from a floor measured on the reference run instead of an
  asserted constant. The finding that produced it: `alloyport_reduce_sum_blocks` ends in
  `atomicAdd`, so **the CUDA authority does not reproduce itself** — on the archived GB10 record it
  disagrees with itself by `1.95e-03` absolute and `5.20e-07` relative. The frozen tolerance was
  wrong in both directions at once, its relative term 38× looser and its absolute term 19.5×
  tighter, and its ten-mutant battery could not have revealed it because every mutant was orders of
  magnitude larger than the tolerance it tested.
- The Source Gate states the product boundary instead of prescribing how a kernel is written.

Instruments added: `read_build_diagnostics` — the compiler's opinion of the model's own source was
previously unreachable, because the receipt carried a descriptor and no tool opened one — and
`read_reference` over a 127-document vendored corpus that travels with its trust ledger.

Evidence domains added, both without execution paths on purpose: `performance` decides when timings
said anything, `knowledge` decides admission from resolved citations.

---

## 3. Do this next

**Run the first real migration end to end.**

1. Rebuild and redeploy the server and workers. Restart the server and both persistent
   worker/tunnel processes from `.alloyport-local/host-connections.md` — that file, not
   recollection, is the authority for SSH targets, tunnels, and installed paths.
2. Point the `candidate-episode` config at the vendored corpus. Two new optional fields,
   `reference_corpus_root` and `reference_corpus_ledger`, are both set in
   `docs/candidate-episode-config.example.json`; without them the tool is not offered at all.
3. Retry the migration through `alloyport-cli`. Continue through Source → sequential Ascend Build →
   paired CUDA/Ascend Correctness, and capture the immutable receipts.

Five things are new and unproven on hardware. **Record what they say; do not adjust them by hand.**

- **`reorder_output_bits`.** The trusted CUDA harness now computes a pairwise-tree sum beside the
  authority. If that order disagrees with the reference by much more than its run-to-run spread, the
  derived tolerance widens and calibration starts listing mutants in `undetected` instead of
  silently passing. That report is the signal; widening anything by hand puts the guess back.
- **The advisory Source Gate.** A candidate can reach the Build Gate carrying
  `UnrecognizedKernelStructure`. The compiler rejecting it afterwards is the gate working. A
  candidate that *should* have been stopped reaching a wrong Correctness verdict is a real finding
  and belongs in `docs/evidence/`.
- **`read_build_diagnostics`** has never run against a real compiler. Watch whether 64 KiB of the
  head is the useful part, and whether `truncated` fires.
- **`read_reference`** has never been offered to a live model. Watch whether it reads the corpus at
  all, and whether the `suspect` caution on the two perf documents changes what it does with them.
- **The callable names now travel in the execution bundle.** The trusted runner reads them instead
  of hard-coding this specimen; a mismatch shows up as a link error rather than a wrong answer.

---

## 4. Known open defects and gaps

Ordered by how much each would cost to leave. None is scheduled; all are observations with their
evidence, not adopted decisions.

1. **Nothing observes that the candidate ran on the device.** Plain host C++ that sums the input
   passes the Source Gate, compiles on the Ascend build worker because it is just C++, links, is
   called through the same ABI, and matches the authority exactly. `implementation_invoked` and
   `synchronized` are literals emitted by the trusted runner and cannot catch it.

   Partly addressed: every verdict carries `unverified: [device_execution, runner_attestation]`, so
   a `PASS` states this rather than implying the opposite, and the two mutants that flipped those
   literals are retired. **The observation itself is still missing.** Cheapest candidate: a
   link-time check that the built candidate depends on the ACL runtime — necessary rather than
   sufficient, and not designable honestly without a device to try it against.

2. **Performance has rules but no execution path.** `alloyport_core::performance` refuses a summary
   under five samples, calls an effect inside the combined noise `NoResult`, never lets a proxy
   reach `Improved`, and refuses a cross-environment comparison. Nothing runs a timed workload.

   It needs two things that do not exist: a **workload shape** distinct from the correctness corpus,
   which is chosen for coverage (zero elements, null pointers) rather than for timing; and a
   **measured roof**, since the only honest ceiling for a memory-bound reduction is bandwidth probed
   on this hardware. Importing the sibling project's figure would be the imported-number-as-fact
   mistake the reference ledger exists to prevent.

3. **Knowledge has an admission gate but no store.** `alloyport_core::knowledge` admits on resolved
   citations, treats negative knowledge as first class, and `audit` runs the gate backwards.
   Deliberately unbuilt: the store, retrieval, and write-back, because no migration has completed
   and there is not yet one entry. When they are built: retrieval filters hard scope *before*
   similarity, and **write-back is event-driven** — in the sibling project two runs finished an
   entire port and hit the turn limit with everything still in their heads, because "write back what
   you learned" was step eight and step eight never arrives for a budget-limited agent.

4. **The specimen is welded into the types, though not into the wire.** `Reduction*` appears 351
   times in `alloyport-core`, 101 in `alloyport-server`, 92 in `alloyport-candidate-tools`, 75 in
   `alloyport-worker`, and **0 times in `alloyport-proto`** — the executor kinds are role×backend,
   so a second operator family reuses them unchanged. The trusted runner no longer names this
   specimen. `PRODUCT_EXECUTION_PLAN.md` phase P4 assumes onboarding costs a manifest; it currently
   costs a code change in four crates.

5. **The vendored corpus has no license text.** Upstream states CANN Open Software License 2.0 and
   its README links a `LICENSE` the original snapshot did not capture, so nothing here establishes
   that it may be redistributed inside an MIT distribution. Recorded in `vendor/README.md` with the
   two obligations. **Until that closes it is an internal development corpus.**

6. **The model still has no scratch compile.** Trying a ten-line construct costs a full
   submit/gate/build cycle.

7. **Neither boundary script can run on the dev host** (`rg` is absent). They exit 2 saying so
   rather than printing a pass; CI still runs them. Nothing has verified the architecture
   boundaries locally.

8. **Slow shutdown drain.** The server can report `server tasks did not drain within 10s` after
   Ctrl-C. `cc511dc` stopped it overwriting the primary diagnostic; the drain is undiagnosed.

---

## 5. Rules these sessions kept paying for

Each was violated by working code here, which is why they are stated with their instance rather than
as advice. The full list is in [`CLAUDE.md`](../CLAUDE.md).

**Read the record, not the applicant.** The tolerance was supplied by its caller; a calibration
receipt's stored `passed` was believed on read; a rejection named a digest with no artifact behind
it. All three are now derived or recomputed from what was recorded.

**A gate must accept the evidence it demands, must fail when it cannot run, and must state what it
did not check.** A gate tighter than the task's own noise rejects correct work. A battery of
sledgehammers cannot locate an edge. A boundary check that swallows a missing tool prints a pass. A
verdict that cannot name its blind spots invites every reader to assume it has none.

**Measure before refactoring, and correct the record in place.** Two claims written into this
document during these sessions did not survive being checked — that a second specimen would cost a
protocol version, and that `grep -ril knowledge` returned only `already_known`. Both were corrected
where they stood, with the correction stated, because the discipline applies to this document too.
