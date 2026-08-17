# The candidate record, and the three things it said the first time it ran

- Date: 2026-08-17
- Decision: [0044](../design/0044-git-as-the-candidate-record.md), now accepted and implemented
- Command: `alloyport-server --config <server.json> candidate-record TASK_ID --into DIRECTORY`
- Read against: the real Episode databases and CAS from 2026-08-16, copied to a scratch directory so
  nothing wrote to the deployment

Design 0044 was motivated by a question that had been answered by eye several times in one session:
*what changed between the build that failed on `acl/acl.h` and the one that failed on
`kernel_operator.h`?* The record now answers it with `git diff`. It also answered three questions
nobody asked, and one of those contradicts `NEXT_SESSION.md`.

## What it produced

Three real runs, projected. Each candidate is one commit whose tree is exactly that candidate, tagged
`c<NNN>-<12 hex>`, parented by what its manifest names.

```
$ git -C record-0874 log --all --graph --oneline --decorate
* b82b593 (HEAD -> main, tag: c003-cb350622868d) c003 source_gate passed
* 88f2f19 (tag: c002-2282e784549b) c002 source_gate passed | ascend_build exit 1: \
    /alloyport/bundle/generated/src/reduce_sum_kernel.cpp:1:10: fatal error: kernel_operator.h: No such file or directory
* a2fc46c (tag: c001-3f18debb6d4e) c001 source_gate failed: missing_build_reference, incomplete_component_mapping
```

| run | candidates | what the gates said |
|---|---|---|
| `task-c36ab7b63cbf64234498b88b` | 3 | Source Gate failed, failed, passed. No build outcome — this is the run that died on `SourceGateReceiptMismatch`. |
| `task-ea9792931f2c781324ce536b` | 3 | c001 failed; c002 passed then **built and failed on `acl/acl.h`**; c003 submitted, **no gate ran**. |
| `task-0874a0d2d131f6d1af13dc4b` | 3 | c001 failed; c002 passed then **built and failed on `kernel_operator.h`**; c003 passed the Source Gate, no build outcome. |

Every one of those verdicts matches what the Episodes recorded, and every blob was re-read out of the
written repository and rehashed against its manifest digest before the command returned.

## 1. The correction loop did not close. It got one turn further than that.

`NEXT_SESSION.md` §1 says:

> One passed the Source Gate, dispatched to the NPU worker, and got an answer in 270 ms:
> `fatal error: acl/acl.h: No such file or directory`. Then it read that message through
> `read_build_diagnostics`, fixed the include path, and the next build failed one layer deeper on
> `kernel_operator.h`. **The correction loop closed.**

The two compiler errors are from **two different migrations**. Read from the operation lists:

```
task-ea9792931f2c781324ce536b   32 operations, terminal status budget_exhausted
  19 submit_candidate_bundle  succeeded        <- c001
  20 request_source_gate      candidate_failed
  23 submit_candidate_bundle  succeeded        <- c002
  24 request_source_gate      succeeded
  25 request_ascend_build     candidate_failed <- acl/acl.h
  26 read_build_diagnostics   succeeded        <- the model read the compiler
  27-30 read_reference        succeeded  x4
  31 submit_candidate_bundle  rejected_as_invalid
  32 submit_candidate_bundle  succeeded        <- c003, and the budget ended here

task-0874a0d2d131f6d1af13dc4b   31 operations, terminal status tool_work_pending
  26 request_ascend_build     candidate_failed <- kernel_operator.h, on this run's FIRST build
  27 read_build_diagnostics   succeeded
  29 submit_candidate_bundle  succeeded        <- c003
  30 request_source_gate      succeeded
  31 request_ascend_build     running          <- still in flight when the session ended
```

So `kernel_operator.h` was not one layer deeper than `acl/acl.h`; it was a different run's *first*
build, of a differently structured candidate — `generated/src/reduce_sum_kernel.cpp` against
`generated/op_host/reduce_sum_launch.cpp`. **No corrected candidate has ever been rebuilt.** In
`task-ea979` the budget ran out one operation after the corrected submission; in `task-0874` the
rebuild was still `running`.

What is true, and worth keeping: the model read `read_build_diagnostics` in both runs, corrected, and
its correction passed the Source Gate in `task-0874`. Read → correct → *submit* closed. Read →
correct → **rebuild** has never completed once.

This is the failure mode `CLAUDE.md` names first — *read the record, not the applicant*. The claim was
assembled from two runs watched live, and no one went back to the record to check whether it was one
loop. The record makes that check a single command.

## 2. Eighteen corpus reads before the first line of code

| run | `read_reference` calls | before the first submission | total operations |
|---|---|---|---|
| `task-ea979` | 24 | **18** | 32 |
| `task-0874` | 20 (one refused) | **20** | 31 |

`fatal-harness-defects-20260816.md` records "four of run 4's seven turns went to corpus reading" and
asks whether the caps are right. These two runs put a number on it: 18 and 20 corpus reads before a
single line of Ascend C, and 24 of 32 operations spent reading in one of them. `NEXT_SESSION.md` §4.3
measures corpus reads at roughly 10 k input tokens each and a climb to 98 815 against a 128 000
ceiling; whether that climb was one of these two runs is not recorded, but reading at this volume is
the only thing in an Episode with that shape. Both of these runs ended without completing a rebuild.

Two open gaps are the same gap seen from different sides: there is no compaction, and a first
submission has no headroom. The record shows what is consuming the room.

## 3. A patch interface is not worth taking, measured

0044 deliberately deferred letting the model send a diff rather than files, and said the record would
make the question answerable. It now is. Unified `git diff` size against the whole tree it changes:

| step | tree | diff | diff as share of tree |
|---|---|---|---|
| `c36a` c001→c002 | 9 575 B | 5 204 B | 54% |
| `c36a` c002→c003 | 8 993 B | 6 112 B | 68% |
| `ea97` c001→c002 | 7 696 B | 5 409 B | 70% |
| `ea97` c002→c003 (siblings, not a step) | 7 032 B | 7 980 B | **113%** |
| `0874` c001→c002 | 8 383 B | 3 356 B | 40% |
| `0874` c002→c003 | 9 630 B | 2 586 B | 27% |

A diff is 27–70% of the tree, and across a fork it is larger than the tree. Inheritance already took
a correction from 16 384 to 788 output tokens by sending only the files that changed; a patch would
shave the unchanged lines *inside* those files, worth roughly a factor of two at best against the
factor of twenty already banked — and it would buy that with "this patch does not apply", a failure
mode the model cannot always repair.

**Do not build the patch interface.** The measurement, not the argument, is the reason.

## 4. What the record also showed in passing

- **Lineage forks.** In `task-ea979`, c002 and c003 both name c001 as parent: the corrected candidate
  was not built on the one that had just failed to compile. A single branch could not have recorded
  this, which is why each candidate is its own tag.
- **`validate_call` is working in production.** Three `submit_candidate_bundle` operations are
  `rejected_as_invalid` — one in `task-ea979`, two in `task-0874` — and each was followed by a
  successful submission. That is Design 0040's recovery path running on a live model, twice more than
  the one instance previously recorded.
- **`task-0874` ended in `tool_work_pending`**, holding a dispatched build. Whether that build ever
  produced a receipt is not in the Episode.

## What this does not establish

- **Nothing about whether any generated kernel is correct.** No build has succeeded, so no
  Correctness Gate has run, and the record only reports what the gates said.
- **Nothing in the trust path reads this repository**, by construction. The manifest remains
  authoritative for identity, gates, and evidence.
- **The record was built from copies.** Opening the CAS and the Episode database both perform small
  writes — staging cleanup and `CREATE TABLE IF NOT EXISTS` — so the deployment's own state was left
  untouched. A record built in place would touch those two files and nothing else.
- **`git` is now required to build a record**, and only to build a record. If it is absent the command
  fails saying so; it does not skip.
