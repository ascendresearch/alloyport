# Measured reduction noise floor and the tolerance it replaces

- Date: 2026-08-14
- Source record: [`cuda-reduction-correctness-diagnostic-20260812.json`](cuda-reduction-correctness-diagnostic-20260812.json)
  — the real CUDA reference run captured on the idle GB10 under Design 0032. No new hardware run
  was performed; this measurement was computed from that existing record.
- Reproduce:
  `cargo run --example calibrate_reduction_receipt -- docs/evidence/cuda-reduction-correctness-diagnostic-20260812.json OUT.json`

## What was measured

The frozen corpus makes the CUDA authority sum every input twice. `alloyport_reduce_sum_blocks`
finishes with `atomicAdd` across blocks, so the authority accumulates its partial sums in whatever
order the blocks retire and does not reproduce itself bit for bit. That disagreement is this task's
own numeric spread, and it was already present in the record.

| case | elements | repetition 1 | repetition 2 | absolute | relative |
|---|---:|---:|---:|---:|---:|
| `valid-1048576` | 1 048 576 | −4282.895996 | −4282.897949 | 1.953e-03 | 4.56e-07 |
| `valid-65536` | 65 536 | 1408.927979 | 1408.928711 | 7.324e-04 | 5.20e-07 |
| all seven others | ≤ 4 097 | — | — | 0 | 0 |

Measured floor: **absolute 1.953e-03, relative 5.20e-07**, over 9 repetition pairs, not
deterministic.

## What it replaces

The gate shipped with `absolute 1.0e-04, relative 2.0e-05`, described in the source as "the policy
used by the first reduction specimen". Nothing measured those numbers. Comparing them against the
floor above, in both directions:

| | frozen | measured floor | derived at 50% slack |
|---|---:|---:|---:|
| absolute | 1.0e-04 | 1.953e-03 | 2.93e-03 |
| relative | 2.0e-05 | 5.20e-07 | 7.80e-07 |

- The frozen **absolute** term is **19.5× tighter** than the authority's own observed spread. It did
  not reject anything only because the comparator takes `max(absolute, relative · |expected|)` and
  the relative term dominates above |sum| = 5.
- The frozen **relative** term is **38× looser** than the measured spread. On `valid-1048576` it
  admits an error of 0.0857 where the authority's own noise is 0.00195: a candidate could be wrong
  by 44× the task's own spread and pass.

The derived tolerance is **26× stricter on the largest case** (3.34e-03 versus 0.0857) and still
admits the authority's own second repetition with margin.

## Consequences recorded in code

- `measure_reduction_noise_floor` derives the floor from the authority run alone.
- `ReductionTolerancePlan` — slack and repetitions, the only part knowable before the run — is what
  binds the experiment identity. The tolerance is derived from the reference receipt by the gate
  itself, so no caller can supply or widen it.
- `calibrate_reduction_oracle` fails unless the floor was measured, a reordered authority is still
  admitted, and every mutant is caught, and it lists in `undetected` whatever it missed.
- `ReductionCalibrationReceipt::passed` is recomputed on read rather than trusted from the stored
  field, so the 2026-08-12 archived calibration — which caught all ten of its mutants under an
  asserted tolerance — no longer reads as a passing gate.

## What this does not establish

- **`reorder_pairs = 0`.** The floor above is run-to-run spread of one summation order. The
  component that bounds a candidate whose *reduction tree* differs is not in this record. The
  trusted harness now emits `reorder_output_bits` (a pairwise tree with an unrelated leaf block), so
  the next real reference run will measure it; the number here will move.
- **`battery_scope = ComparatorOnly`.** Every mutant perturbs a receipt, not an implementation.
  Nothing here shows that a genuinely broken kernel would be caught on the way to the comparator.
- `implementation_invoked` and `synchronized` are emitted as literals by the trusted harness, so the
  `FallbackBypass` and `MissingSynchronization` mutants exercise fields no real candidate can move.
