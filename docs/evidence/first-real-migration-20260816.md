# The first real migration, and the gate that could not be satisfied

- Date: 2026-08-16
- Task: `task-c36ab7b63cbf64234498b88b`, specimen `cuda-reduction-v1`
- Episode database:
  `/home/dawei/.local/lib/alloyport-server/deployment/state/migrations/task-c36ab7b63cbf64234498b88b/episode.sqlite3`
- Runtime model: `deepseek-v4-pro`, 12 turns, 551 714 input tokens, 30 571 output tokens
- Terminal state: **failed** — `tool gateway adapter: invalid candidate build contract:
  SourceGateReceiptMismatch`

This is the first time a model-authored Ascend C candidate passed the Source Gate. It is also the
first migration killed by a gate whose required argument the model was never given.

## What the run did

| turn | action | outcome |
|---|---|---|
| 1 | `read_reference` (list) | corpus listed |
| 2–5 | `read_reference` × 13 documents | read before writing any code |
| 6 | `submit_candidate_bundle` (15 668 output tokens) | manifest `sha256:086e8da7…` |
| 7 | `request_source_gate` | `candidate_failed`: `missing_build_reference`, `incomplete_component_mapping` |
| 8–9 | corrected bundle, gate again | `candidate_failed`: `incomplete_component_mapping` |
| 10–11 | corrected bundle, gate again | **passed** — manifest `sha256:98913b93…` |
| 12 | `request_ascend_build` | fatal `SourceGateReceiptMismatch` |

21 tool operations of a 60 budget; 12 turns of 20. Budget was not the constraint.

## The defect

`request_ascend_build` requires `source_gate_receipt_digest`. **No Source Gate result — passing or
failing — has ever carried it.** All three receipts in this run contain exactly
`schema_version`, `gate_revision`, `candidate_id`, `manifest_digest`, `passed`,
`inspected_artifacts`, `failures`.

The digest cannot be added to the receipt body either: the check is
`Sha256Digest::digest_bytes(&supplied) != receipt_digest`, so a receipt containing its own hash is
circular by construction.

Nor does the digest reach the model by another route. A tool result becomes
`OwnedToolResult { native_call_id, output }`, where `output` is the artifact *bytes*;
`result_digest` is used to fetch those bytes and to bind the continuation input digest, and is
never rendered. Checked against the record rather than inferred: the turn-12 request body
(`sha256:9b81777b…`, 274 224 bytes) contains 17 distinct `sha256:` digests, and the required
`sha256:34316738…` **is not one of them**.

The model said so before it died:

> "I need the source gate receipt digest — it wasn't explicitly returned in the response. […] The
> Source Gate response didn't include a `receipt_digest` field this time."

It then passed the manifest digest, which *was* in its input. There was no correct value available
to it. This is the rule from `CLAUDE.md` arriving on schedule:

> *A gate that cannot be satisfied honestly will be routed around. That is a design defect, not a
> discipline problem.*

## Why nothing caught it

`crates/alloyport-candidate-tools/src/tests.rs` locks this behavior with a test that asserts the
`Adapter` error. The test obtains its digests from `let (_, receipt) = execute(&mut gateway, …)` —
the gateway hands a *test caller* the result digest as a return value. The mechanism was therefore
verified for a caller who is given the digest, and nobody asked whether the runtime model is such a
caller. It is not.

That is this repository's one mistake in its usual costume: **we applied "don't trust, verify" to
the model, and exempted the test scaffolding that stood in for it.** The scaffolding supplied the
one thing the real caller can never obtain.

## The second defect, one line away

`verify_source_gate_receipt` was written by Design 0040 and is *half* correct. When the Source Gate
finds something blocking it publishes the receipt and returns a recoverable `CandidateFailed` —
that is 0040 working. When the gate **passes** and the cited digest does not match, it returns
`adapter_error(…)`, which escapes the Agent loop and fails the migration.

So 0040's class fix — model-authored input defects are recoverable, not fatal — was applied to the
"your candidate is bad" branch and missed the "you cited the wrong receipt" branch in the same
function. `CLAUDE.md` law 6 asks for exactly this enumeration and it was not done.

Note also what the citation buys: `verify_source_gate_receipt` re-evaluates the Source Gate from the
manifest and sources anyway. The model's digest is never the source of the verdict — it can only
disagree with a verdict already computed. It adds no authority and one fatal failure mode.

## The same break exists at every remaining link

Enumerating where else this class lives, as law 6 requires, the chain is broken past the first hop:

| tool | required argument | obtainable by the model? |
|---|---|---|
| `request_source_gate` | `manifest_digest` | **yes** — `submit_candidate_bundle` returns it |
| `request_ascend_build` | `source_gate_receipt_digest` | no |
| `read_build_diagnostics` | `build_gate_receipt_digest` | no |
| `request_reduction_correctness` | `manifest_digest`, `source_gate_receipt_digest`, `build_gate_receipt_digest` | one of three |

Even had turn 12 succeeded, the run would have died one hop later for the same reason. Design 0041
added `read_build_diagnostics` so the model could finally read the compiler's opinion of its own
source; **the model cannot invoke it**, because its only argument is the Build Gate receipt's own
digest.

`AscendBuildReceipt` illustrates the pattern exactly: it carries `manifest_digest` and
`source_gate_receipt_digest` — its predecessors' digests — and never its own.

### Why the one working link works

`submit_candidate_bundle` returns a *wrapper document* that names a **separate** artifact:

```json
{"candidate_id": "candidate-3fb3ae54…",
 "manifest": {"digest": "sha256:98913b93…", "size_bytes": 2088,
              "media_type": "application/vnd.alloyport.candidate-source-manifest+json"},
 "source_bundle_digest": "sha256:7695909d…"}
```

A result can name another artifact's digest. It can never name its own — that is circular under a
content hash. Every broken link asks the model for the tool result's own digest; the working link
asks for a neighbour's. The shape that already works in this codebase is the shape the others need.

## What worked, and should be said plainly

- **`read_reference` was used, first and heavily.** The model's opening move was to list the corpus,
  and it read 13 documents across four turns before writing a line. The `NEXT_SESSION` question
  "whether it reads the corpus at all" is answered: yes, unprompted.
- **The Source Gate correction loop works on a real model.** Two blocking findings, two corrections,
  then a pass — durable across turns, with each candidate a child of the last.
- **The advisory Source Gate did not obstruct.** `UnrecognizedKernelStructure` never fired; the
  blocking findings were both product-boundary facts (`missing_build_reference`,
  `incomplete_component_mapping`), which is what 0040 intended the gate to be able to say.
- **The measured probe bound held.** The Ascend worker stayed connected and READY throughout, on the
  configuration written in `device-probe-timeout-20260816.md`.

## A third finding: the corpus is 127 of 1099 files

`read_reference` serves one `SKILL.md` card per document id. The vendored tree holds 1099 files;
972 of them — complete Ascend C kernels, `CMakeLists.txt`, `main.cpp`,
`references/kernel_launch_details.md` — are unreachable, **and the cards cite them by path**. The
model hit this on turn 3:

> "The direct-invoke template references `references/add_custom/` sub-files that I can't directly
> read from this index."

This is not a reader bug to patch quietly. The trust ledger has exactly 127 rows, one per card, so
the 972 sub-files carry no trust state at all; serving them is a ledger decision, not a code change.

## What this run did not establish

- **No Ascend C was compiled.** The Build Gate was never reached, so nothing here says whether the
  generated kernel is valid, and `read_build_diagnostics` still has never run against a real
  compiler.
- **No correctness verdict, no `reorder_output_bits` observation, no calibration report.** Every
  hardware-unproven mechanism listed in `NEXT_SESSION.md` §3 except `read_reference` remains
  unproven.
- **Whether the generated kernel is any good is completely unknown.** It passed a structural text
  gate. That gate deliberately says almost nothing about method, so a Source Gate pass is not
  evidence about the code — 0040 made it that way on purpose.
- **The digest chain was proved unobtainable, not the tools themselves wrong.** Whether
  `read_build_diagnostics` and the Correctness Gate behave correctly once reachable is still
  untested on hardware.
- **Nothing was adjusted by hand to make this run pass**, and nothing should be until the two
  defects above are decided.
