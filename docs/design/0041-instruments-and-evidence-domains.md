# 0041 — Instruments for the model, and evidence domains for speed and knowledge

- Status: Accepted
- Date: 2026-08-16
- Follows: [0040](0040-measured-tolerance-and-advisory-source-gate.md)
- Partially implements: [0006](0006-performance-evidence-and-claims.md),
  [0008](0008-evidence-backed-knowledge-lifecycle.md)

## Context

0040 corrected three gates. Working through what the runtime model could actually see and say
afterwards exposed four more gaps, none of which is a missing feature — each is something the
system already claimed to have.

1. **The model could not read the compiler's opinion of its own source.** `AscendBuildReceipt`
   carries `stdout`/`stderr` as `ArtifactDescriptor`, `detail` is a `&'static str` policy constant,
   and no tool in the catalog opened an artifact. The correction loop that 0026's Episode test
   demonstrates uses a fake attempt port whose `detail` carries an invented message.
2. **The model had no reference material.** It wrote Ascend C against an API known only from its
   weights.
3. **Nothing observed that a candidate ran on the accelerator.** Host C++ that sums the input passes
   the Source Gate, compiles on the Ascend build worker, links, and matches the authority exactly.
   0040 widened this by making the kernel-structure check advisory.
4. **The trusted correctness runner hard-coded the specimen** — the callable symbol and both CMake
   target names — inside the trust boundary.

## Decision

### Instruments are distinct from gates

`read_build_diagnostics` and `read_reference` are `ReadOnly` / `Observed` and cannot satisfy a
subtask. The catalog stays closed: model arguments still cannot choose a worker, image, device,
command, corpus, or tolerance. The catalog test now asserts that property rather than a tool count.

Authority flows from a receipt, never from a digest the model names. `read_build_diagnostics`
resolves the cited Build Gate receipt to a manifest belonging to this migration and returns only the
artifacts that receipt itself points at. Bounded output reports `returned_bytes`, `total_bytes`, and
`truncated`, because a layer that shortens the ground truth quietly is how the line that mattered
disappears.

### A vendored corpus travels with its trust ledger

`vendor/cannbot-skills/` holds 127 documents; `vendor/cannbot-skills-audit.jsonl` holds one row
each. Snapshotting freezes content, not truth. Nothing is `validated` — AlloyPort has run no probe.
41 rows are `reviewed` with verdicts recorded as `imported_review` from the sibling project against
these exact bytes; 86 are `unaudited`. The reader refuses a corpus and ledger that have drifted
apart rather than applying half of it, and `content_sha` retires a verdict when the bytes move.

The two documents an optimization task reaches for first are both `suspect`: one hollow by its own
admission, one carrying numbers validated on the previous hardware generation while this product
targets `Ascend950PR`. Serving the corpus without its ledger would present those as facts.

Licensing is unresolved and recorded in `vendor/README.md`.

### A verdict states what it did not check

Every correctness verdict carries `unverified: [device_execution, runner_attestation]`. The two
mutants that flipped runner-emitted literals are retired from the battery — a mutant no real
candidate can produce scores a guaranteed detection — while their enum variants remain so archived
receipts still parse.

### Performance evidence, without an execution path

`alloyport_core::performance` decides whether timings said anything: at least five samples, kept; an
effect inside the combined noise is `NoResult` rather than a small win; a proxy never reaches
`Improved`; a cross-environment comparison is refused, because a migration's speedup is not measured
across the migration.

### Knowledge admission, without a store

`alloyport_core::knowledge` decides an entry's status from resolved citations rather than from the
entry. Negative knowledge is supported by the receipt that *failed*: a gate that only accepts
passing receipts leaves the most valuable kind of entry with no honest way in, and a gate with no
honest path gets routed around. `audit` runs the gate backwards over stored entries.

## What this does not do

- **No timed workload runs.** The execution path needs a workload shape distinct from the
  correctness corpus, which is chosen for coverage rather than timing, and a roof measured on this
  hardware. The first real migration should say what it needs; importing the sibling project's
  bandwidth figure would be the imported-number-as-fact mistake the reference ledger prevents.
- **No knowledge is stored, retrieved, or written back.** No migration has completed, so there is
  not yet one entry. When they are built: retrieval filters hard scope before similarity, and
  write-back is event-driven — a final step never arrives for a budget-limited agent.
- **Device execution is still unobserved.** The cheapest candidate is a link-time check that the
  built candidate depends on the ACL runtime, which is necessary rather than sufficient and cannot
  be designed honestly without a device to try it against.
- **The specimen is still welded into the types.** `Reduction*` appears 351 times in
  `alloyport-core` and 0 times in `alloyport-proto`; the wire names roles, not operators.
