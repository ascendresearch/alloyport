# Five migrations, four fatal harness defects, one class

- Date: 2026-08-16
- Runs: `task-c36ab7b63cbf64234498b88b`, `task-436fe144a291b285ec9547db`,
  `task-1cadf422a8fed170618c775a`, `task-16322b4520fe363c08e9117c`,
  `task-ccd149dfc0f421d97ed7feb4`
- Detail of the first run: [`first-real-migration-20260816.md`](first-real-migration-20260816.md)
- Decision that came out of it: [0042](../design/0042-model-visible-receipt-references.md)

Four paid migrations died before any Ascend C was compiled. Not one died because the model wrote a
bad kernel. Every one died inside the harness, and all four are the same class:

> **The harness ended a paid migration over a condition the model could not see, could not satisfy,
> or could have complied with had it been told — enforced by something that should not have had the
> power to end anything.**

| run | died on | why it could not be avoided |
|---|---|---|
| 1 | `request_ascend_build`: `SourceGateReceiptMismatch` | required a digest no result had ever shown the model |
| 2 | `read_reference`: unknown document | a `ReadOnly` instrument ending a migration over a one-letter name |
| 3 | `TurnRecorded -> BudgetExhausted` | a transition the state machine forbade; the branch was dead on arrival |
| 4 | `submit_candidate_bundle`: unknown variant | 0040's recovery existed but was bypassed in production |

## What each one actually was

**1 — a value that did not exist.** `request_ascend_build` requires
`source_gate_receipt_digest`. No Source Gate receipt has ever carried it, it cannot be added to the
receipt body because the check hashes those exact bytes, and a tool result reaches the model as
artifact bytes with the result digest never rendered. The turn-12 request body carried 17 distinct
digests and not that one. Design 0025 §7.3 had specified a projection with `receipts:
Vec<ReceiptRef>`; the implementation is `{native_call_id, output}`.

**2 — an instrument with the power to kill.** The model asked for
`ops/ascendc-register-invoke-template`; the corpus holds `ascendc-registry-invoke-template`. 0041
says an instrument is `ReadOnly` and cannot satisfy a subtask — it grants no authority — and it
ended the run anyway.

**3 — a branch that never worked.** `plan_turn_tools` runs in `TurnRecorded` and ends the Episode
there when a budget is spent. `TurnRecorded` was the one status that never listed `BudgetExhausted`
among its successors; every other status that can reach it listed it. The branch could only ever
produce `invalid episode transition`. **Both** conditions it guarded were dead, so the
`max_total_tool_operations` budget could never be enforced either — including the 60 it was raised
to earlier the same day. The tested budget route exhausts model turns and exits from a status that
does permit the transition; the untested route was broken from the start.

**4 — a fix that was never running.** `AgentToolGateway::validate_call` carried Design 0040's whole
correction path and had a default returning `Ok(())`. Production wraps the gateway in
`ContextRecordingToolGateway`, which forwards `descriptor`, `execute`, and `reconcile` and never
forwarded that one. **No production call had ever been validated.** On 2026-08-13
`task-addd999597dcf12eded7489d` died on a malformed `submit_candidate_bundle`; 0040 was written to
fix exactly that; three days later run 4 died the same way on the same tool.

## The shape underneath

Two of the four were verified by tests that passed, because the tests exempted the thing standing in
for the real caller:

| defect | what the test verified | what it exempted |
|---|---|---|
| receipt digest | the gateway handed the digest to the test as a return value | the model has no such channel |
| `validate_call` | `CandidateToolGateway` unwrapped | production never uses it unwrapped |

This is `CLAUDE.md`'s one mistake, twice, in one codebase, found in one day:

> *We apply "don't trust, verify" to the model and to the agent — and exempt ourselves.*

The other two are the same question asked of a state machine and a trait: **what does this trust?**
Defect 3 trusted a transition table nobody had walked. Defect 4 trusted a default nobody had asked
to justify itself.

## What was changed

- Gate results are wrappers naming their receipt, so every required digest is one the model was
  shown; enforced by a test that may not read the gateway's return value (0042).
- Citation mismatches and instrument failures are recoverable, at a single chokepoint rather than
  per site.
- `validate_call` lost its default: omitting it is now a compile error, so a decorator cannot
  inherit permission by saying nothing.
- Budget exhaustion is reachable and terminal; a turn that is merely too wide costs a turn, charges
  no operations it never ran, and is no longer recorded as budget exhaustion — run 3 ended holding
  52 of its 60 operations.
- The four-calls-per-turn bound is stated in the system prompt, because it is invisible from the
  tools.

## What still is not established

- **No Ascend C has been compiled.** The Build Gate has never run on a model-authored candidate.
- **No correctness verdict, no `reorder_output_bits` observation, no calibration on real output.**
- **`read_reference` reaches 127 of 1099 vendored files**, and run 4 showed this is not theoretical:
  the model followed citations inside the cards to `references/vf-declaration.md`,
  `references/kernel_launch_details.md` and four more, and was refused six times. The ledger has one
  row per card, so serving sub-files is a ledger decision.
- **Two states, `Created` and `CancellationPending`, still do not permit `Failed`.** No runtime path
  was found that attempts it. That is an unverified absence, not a cleared one.
- **Whether the caps are right.** 20 turns and 60 operations were chosen once, and four of run 4's
  seven turns went to corpus reading before a line of code was written.
