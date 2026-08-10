# 0002: Long-horizon runtime and goal persistence

- Status: Proposed
- Date: 2026-08-09
- Scope: multi-session execution, context management, state authority, and drift prevention

## Context

Porting and optimizing a real CUDA extension can span many kernels, host integration points,
devices, experiments, context windows, and human review points. A larger model context does not by
itself preserve the goal. Raw histories accumulate obsolete observations, local optimization
targets, failed attempts, and unverified claims. Passive summaries can then preserve an early
mistake while removing the evidence needed to correct it.

AlloyPort therefore treats long-horizon reliability as a verified state-management problem rather
than a context-window-size problem.

## Goals

- Preserve the user's original objective and hard constraints across sessions and agents.
- Resume work from compact, typed state without replaying a complete transcript.
- Prevent an executor's completion claim from becoming canonical state without verification.
- Retain enough provenance to recover details hidden by summarization.
- Detect stalled progress, repeated failures, contradictory facts, and local-goal substitution.
- Support explicit user amendments without silently rewriting the original contract.

## Non-goals

- Preserving private chain-of-thought as durable project state.
- Asking one unbounded agent session to remember and execute the entire project.
- Treating an LLM-written summary as an authoritative record of the environment.
- Allowing memory retrieval or reflection to bypass normal correctness and release gates.

## Decision

Long-running tasks use a repeated Manage-Execute-Audit loop over an external canonical state:

```text
immutable GoalContract
         |
         v
Manager reads verified state and creates one bounded SubtaskContract
         |
         v
Executor runs in a fresh, budgeted context and emits an untrusted ExecutionReport
         |
         v
Read-only Auditor inspects artifacts, tests, devices, and receipts
         |
         v
StateStore appends verified AuditReport and derives the next TaskStateSnapshot
         |
         +------------------------------> next round
```

Raw executor history is disposable. The goal contract, event log, immutable evidence, audit
reports, and derived task snapshot survive across rounds.

Design 0010 defines a separate user-visible interaction event stream for narrative, commands,
changes, approvals, and live results. It can reference the same immutable receipts and artifacts,
but it is not this design's authoritative audit event log. Replaying a terminal transcript cannot
advance canonical task state.

## Core invariants

1. The original `GoalContract` is immutable. A changed objective creates a versioned
   `GoalAmendment` authorized by the user.
2. `ExecutionReport` is never evidence and cannot directly mark a requirement complete.
3. Canonical state advances only through an `AuditReport` backed by independently observed
   evidence.
4. The auditor is read-only with respect to task artifacts. An audit-time mutation invalidates the
   report.
5. Every completed requirement points to immutable receipt or artifact digests.
6. Hypotheses, observations, and verified facts are different record types and cannot be silently
   promoted between trust levels.
7. Every executor invocation receives a bounded context projection and may be discarded without
   losing canonical progress.
8. `done` is valid only when the audited state satisfies the current goal contract and no mandatory
   requirement remains pending, blocked, untrusted, or contradicted.

The most important type distinction is:

```text
ExecutionReport != AuditReport != TaskStateSnapshot
```

## Durable records

### GoalContract

Records the user objective, success measures, hard constraints, permitted side effects, target
hardware, acceptance scope, and required human decisions. It is content-addressed and versioned.

### GoalAmendment

Records an explicit addition, removal, or reinterpretation of the goal. It identifies its author,
reason, affected requirements, and predecessor contract. Agents may propose an amendment but may
not authorize it.

### Requirement

Represents one testable obligation derived from the goal. Its status is one of `pending`,
`in_progress`, `completed`, `blocked`, `untrusted`, or `contradicted`. Completion includes evidence
references and the audit decision that changed the status.

### SubtaskContract

Defines exactly one bounded unit of execution:

- immediate objective and parent requirements;
- dependencies and known facts;
- allowed tools and side effects;
- time, token, device, and retry budgets;
- acceptance criteria and expected evidence;
- conditions requiring user authorization or early termination.

### ExecutionReport

Describes actions attempted, files or environments possibly changed, outputs claimed, failures,
and suggested follow-up. It is useful navigation data for the auditor but has no authority.

### AuditReport

Records independently observed facts, evidence digests, acceptance results, integrity status,
remaining gaps, and proposed state transitions. Reports are append-only. A later report may
supersede a finding but may not erase its history.

### TaskStateSnapshot

Provides a compact materialized view derived from the goal, amendments, and audit event log. It
contains the active requirements, verified facts, open hypotheses, artifacts, rejected routes,
budgets, and current blockers. It can always be rebuilt from durable records.

### MemoryPage and MemoryCue

Large raw observations and trajectories are stored in immutable pages. Compact cues summarize why
a page may matter and point back to its digest. Retrieval returns both the cue and the relevant raw
page, preventing a lossy summary from becoming the sole historical record.

### ContextProjection

This is a disposable, task-specific view assembled for one executor round from:

- the current goal contract and applicable amendments;
- the selected subtask contract;
- relevant verified facts and prior audit reports;
- explicit failed routes and unresolved contradictions;
- retrieved memory pages;
- the latest environment snapshot when needed.

It is not stored as canonical truth. Its inputs and rendering policy are recorded so the supplied
context can be reproduced.

## Round protocol

### 1. Manage

The manager compares the audited snapshot with the current goal contract. It selects one unresolved
requirement whose dependencies are satisfied, creates a subtask contract, and chooses `execute`,
`ask`, `blocked`, or `done`.

The manager has no state-changing environment tools. This prevents planning convenience from
silently altering the facts on which the plan is based.

### 2. Execute

The executor receives a fresh context projection and only the tools permitted by the contract. It
may iterate locally, but its context and budget are bounded. It returns an execution report and
machine-generated receipts for actions that can be recorded deterministically.

### 3. Audit

The auditor starts from a fresh context, independently inspects the actual environment, and checks
the subtask acceptance criteria. It has read-only access to protected artifacts. The controller
rejects audit reports when mutation monitoring detects an integrity violation.

### 4. Commit state

The state store validates report signatures and referenced digests, appends the audit event, and
derives a new snapshot. No in-place LLM edit of canonical state is permitted.

## Active context maintenance

Every completed round updates compact interaction memory while retaining the supporting raw pages.
A context-maintenance pass checks for:

- stalled requirements or no verified progress across a configured number of rounds;
- repeated or near-duplicate attempts against an unchanged environment;
- new evidence contradicting a currently active fact or hypothesis;
- disproportionate effort on a subtask with low contribution to end-to-end acceptance;
- acceptance criteria that no longer trace to the current goal contract;
- summaries whose cited evidence is missing, stale, or no longer reproducible;
- token growth caused by irrelevant or redundant state.

When triggered, reflection may reorganize cues, priorities, hypotheses, and the context projection.
It cannot rewrite the goal, upgrade trust, mark work complete, or delete audit history.

## Goal-drift controls

Before dispatch, the controller calculates a deterministic alignment checklist:

1. Does the subtask trace to at least one pending requirement?
2. Are its acceptance criteria implied by the goal contract or an authorized amendment?
3. Are all required dependencies verified rather than merely claimed?
4. Does the proposed work fit the remaining budgets and permitted side effects?
5. Is an equivalent failed route already recorded without new evidence that justifies retrying it?
6. Would completing it materially advance operator release or project-level acceptance?

A failed check blocks dispatch or routes the decision to human review. An LLM-based drift score may
be added as an advisory signal, but it cannot override deterministic policy.

## Recovery and restart

After a crash, context rollover, model change, or worker replacement, recovery requires only the
goal contract, audit event log, immutable evidence store, and latest derivable snapshot. The new
executor does not inherit the previous executor's self-assessment or hidden reasoning.

If the event log and materialized snapshot disagree, the event log wins and the snapshot is rebuilt.
If evidence referenced by a completed requirement is unavailable or fails replay, that requirement
returns to `untrusted` and downstream release decisions are invalidated.

## Verification plan

The design is not implemented until tests demonstrate:

- an executor report alone cannot advance task state;
- invalid state transitions and unauthorized goal amendments are rejected;
- audit-time artifact mutation invalidates completion;
- state can be rebuilt byte-for-byte from the event log;
- fresh executors can resume from a context projection without raw chat history;
- deliberate stale summaries, false completion claims, repeated routes, and contradictory evidence
  are detected;
- a completed task still fails closure when one mandatory requirement lacks evidence;
- memory compaction preserves references needed to recover original observations.

Long-horizon benchmarks for AlloyPort must include multi-session operator ports, injected worker
failures, deliberate oracle mutants, changing device availability, rejected optimization routes,
and an end-to-end performance objective that prevents local kernel speedups from replacing the
actual project goal.

## Rejected alternatives

### Keep the complete transcript in one growing context

Rejected because context availability does not ensure that old constraints continue to influence
decisions. Cost and attention dilution also grow with the trajectory.

### Periodically replace history with one summary

Rejected because summaries are lossy, can preserve early errors, and usually lack provenance for
recovering omitted evidence.

### Let the executor maintain and approve its own checklist

Rejected because execution and completion assessment then share the same errors and incentives.

### Use reflection as an unrestricted state editor

Rejected because reflection can reorganize attention but cannot establish environmental truth.

## Research basis

- [ARC: Active and Reflection-driven Context Management for Long-Horizon Information Seeking
  Agents](https://arxiv.org/abs/2601.12030) motivates per-turn incremental memory plus selectively
  triggered repair of memory and checklist state.
- [SAM: State-Adaptive Memory for Long-Horizon Reasoning Agent](https://arxiv.org/abs/2605.24468)
  motivates compact cues backed by recoverable raw trajectory pages.
- [LongHorizon-Harness: Advancing Long-Horizon Agents for Real-World
  Tasks](https://arxiv.org/abs/2608.01964) motivates external task state, fresh-context execution,
  and independent read-only auditing.
- [InfiAgent: An Infinite-Horizon Framework for General-Purpose Autonomous
  Agents](https://aclanthology.org/2026.findings-acl.1787/) supports bounded reasoning contexts
  reconstructed from file-centric persistent state.
