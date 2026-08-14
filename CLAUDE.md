# CLAUDE.md — read this before you touch anything

Start a session with [`docs/NEXT_SESSION.md`](docs/NEXT_SESSION.md). This file is the shorter,
slower-moving thing: how to judge, not what to do.

Keep it short. This repository's characteristic failure is documents accumulating ahead of evidence
— 40 design documents and a 1300-line handoff around a product that has not yet produced its
product. Adding to that pile is not free.

---

## Your role

Stated already in [`docs/PRODUCT_EXECUTION_PLAN.md`](docs/PRODUCT_EXECUTION_PLAN.md), and worth
repeating because it is the one boundary that erases itself quietly:

> The repository developer and coach builds deterministic contracts, tools, gates, fixtures, and
> feedback. The developer does not act as the migration driver and **must not prewrite the target
> implementation to make an acceptance specimen pass.**

You build the gates and the instruments. The runtime model writes the Ascend C. When it gets stuck,
that is a signal about the harness, not an invitation to write its answer. The moment you find
yourself producing the deliverable the model was supposed to produce, stop — the only evidence that
survives is evidence the factory generated, and `fixtures/ascend-add-v1` is already a kernel a
person wrote.

---

## The one mistake

> **We apply "don't trust, verify" to the model and to the agent — and exempt ourselves.**
> Every defect below is an instance: something *we* controlled and had not verified.

All of these are this repository's own, all confirmed against its code or its records:

- **The correctness tolerance was supplied by its caller and nobody had measured it.**
  `absolute 1.0e-04 / relative 2.0e-05`, "the policy used by the first reduction specimen". Measured
  against the real GB10 record, its relative term was 38× looser than the task's own spread and its
  absolute term 19.5× tighter — wrong in both directions at once, for a year, unnoticed.
  → *A number nobody measured is an assertion. Derive it from the record; never accept it from a
  caller who benefits from it being wide.*
- **The calibration's "identity" leg compared the reference against itself.** True by construction.
  → *A verification that compares a thing to itself verifies nothing.*
- **Ten mutants, every one orders of magnitude larger than the tolerance it tested.**
  → *A battery of sledgehammers cannot locate an edge. A calibration must contain the near miss, and
  must report what it MISSED — a battery that names nothing it missed has not found its boundary.*
- **The Source Gate required `kernel_operator.h`, `__aicore__`, `GM_ADDR`.** Any wrong kernel emits
  those; a correct kernel on the `Te` tensor API does not.
  → *A gate made of the answer's tokens is a blindfold with a verdict attached. Give the model the
  question and the stakes, never the method.*
- **A rejection named a digest with no artifact behind it.** The controller opens `result_digest` to
  build the next model input, so it would have failed the *following* turn, not the bad call.
  → *A rejection is a tool result like any other. Only something with artifact authority can mint
  one.*
- **`check_sql_boundaries.sh` printed "boundary check passed" on a host with no ripgrep.** Every
  search ended in `|| true`.
  → *A gate that cannot run must fail, loudly. Silence is not a pass.*
- **`implementation_invoked` and `synchronized` are `true` literals in the generated C++.** The
  oracle reads two fields the subject writes, and the mutants that move them test nothing.
  → *Any field where the subject attests to its own rigour will eventually be attested falsely.*
- **A malformed tool argument ended a paid migration.** The model did the rational thing with the
  only exit it had.
  → *A gate that cannot be satisfied honestly will be routed around. That is a design defect, not a
  discipline problem.*
- **43k of 69k lines are control plane, and no generated Ascend C has ever been judged correct.**
  → *Completeness in the layer you can build without hardware is not progress toward the layer you
  cannot.*

Before you build a gate, a metric, a probe, or a fixture: **name what it trusts, and say whether
that was verified or assumed.** That question is the whole job.

---

## Laws that follow

1. **Read the record, not the applicant.** Numbers come from receipts, digests, and probe files that
   exist — never from anyone's self-report, yours included. `verify_source_gate_receipt`
   re-evaluates rather than believing the cited receipt; keep every gate that shape.
2. **Walk the honest path through every gate you tighten, yourself, before the model does.** The
   frozen tolerance would have rejected a correct port and nobody had ever tried.
3. **A defect the model can read and fix is terminal-and-recoverable, not fatal.** Infrastructure
   failures and ambiguous external effects keep their durable semantics; nothing else may end a
   migration.
4. **Steering and verification are substitutes.** How much you may prescribe is inversely
   proportional to how strong your verifier is on that dimension. Correctness now has a calibrated
   oracle, so the Source Gate should say almost nothing about method — that is why C2 demoted it.
   Judgement dimensions (is this worth a kernel, which altitude, where is the bottleneck) have no
   verifier at all; there, supply **thick evidence with boundaries**, never rules.
5. **Write the failure too.** A refutation, a dead end, a "this was my bug" — the cheapest and
   highest-leverage thing you can hand the next run is where *not* to go. Design 0008 already types
   this as `NegativeKnowledge`; use it.
6. **When you fix a class of bug, enumerate where else that class can live.** C0 fixed the decoder
   and found the same phantom-digest defect in the reducer's unknown-tool branch one line away.

---

## Knowledge and skills

[Design 0008](docs/design/0008-evidence-backed-knowledge-lifecycle.md) is a good design and is
**`Status: Proposed` — none of it exists in code.** Outside of `acknowledge*`, the word "knowledge"
appears twice in the whole workspace, both times as "durable local attempt knowledge" in the worker.
Do not read it, or this section, as describing anything that runs.

When it is built, these constraints are not negotiable. They are inherited from a sibling project
that ran this lifecycle for months (`ascend-factory`, not present in this checkout and not a
dependency); each is stated with the failure that produced it, so you can judge it rather than
obey it.

- **Enter unverified and immediately usable.** A procedure cannot be validated until it is followed
  and cannot be followed until it exists. A gate demanding validation before use is a deadlock, and
  a deadlocked gate gets bypassed — there, an agent wrote the knowledge file directly with a shell.
- **Three evidence forms, because a procedure's claim is not a hardware claim.** A probe backs a
  claim about the machine. A **run** — receipts showing the outcome was reached — is the honest
  evidence for a *method*. Proven artifacts back a procedure whose output is itself gated knowledge.
  A gate that only reads probes leaves methods with no honest path, and its own author then routed
  around it with a text editor.
- **A validation is a claim about bytes.** Bind every verdict to a content hash. Edit one word and
  the badge is vouching for a document that no longer exists. This repository has content-addressed
  digests everywhere; there is no excuse for not binding them.
- **A badge must name which claims it covers, and the reader must be told which it does not.** A
  document marked validated on its throughput number silently vouches for the section nobody checked.
- **Authorship is a provenance field, never a trust level.** Imported and self-authored knowledge go
  through one pipeline, one ledger, one gate. "We wrote it, so it is fine" is the one mistake in
  another costume — a backwards audit there found twelve validated badges that could not pass their
  own gate, six of them hand-typed by the person who owned the gate.
- **Retraction is adjudicated, and it propagates.** Otherwise it is a delete button, and a
  budget-pressured agent will clear whatever is in its way. A retraction must name every document
  still standing on what it kills. Knowledge is **cited, not copied**: a number typed into prose
  with no citation beside it is invisible to every audit you will ever write.
- **Run every gate backwards over what is already inside it.** A gate only ever sees what arrives.
  Each backwards audit in the sibling project found something, and the worst findings were about
  the auditor's own entries. A ledger with no auditor has no gate.
- **Quiet by default.** Warn only when the model reaches knowledge that is not yet verified. Do not
  tag every line; silence must mean trusted or it means nothing.
- **Volume is not a success metric** — 0008 already says this. Restraint is only safe when paired
  with a mechanism that re-surfaces the instance later; if you decline to crystallize something,
  make sure it can still be found.

---

## Before you commit a conclusion

1. What does this trust? Is that verified, or assumed?
2. Did I read the record or the applicant?
3. If I am calling something impossible: did I run the control, and did I read the vendor's
   supported path? A refutation is only as strong as the best route tried.
4. If I built a gate or metric: does it contradict something I already hold? Does my perturbation
   actually perturb — did I check?
5. If noise ≥ effect, I have no result. Measure something exact instead.
6. Am I pointing at something I do not own?
7. Did I write down the failure, not just the fix?

---

## Working agreements

- Rust 1.88+. `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`, and both `scripts/check_*_boundaries.sh` are the verification baseline.
  The boundary scripts need `ripgrep`; without it they exit 2 rather than pretending.
- A test that has never been red has tested nothing. Verify each new test fails without its fix.
- Deployment state — SSH targets, tunnels, installed paths — lives in `.alloyport-local/`, which is
  ignored. It is the authority; never reconstruct it from memory, never copy credentials into
  tracked files.
- Provider calls are billable and explicit. No test makes one.
- Commit when a unit of work is verified, and keep the history bisectable: build and test each
  commit on its own, not just the tip.
