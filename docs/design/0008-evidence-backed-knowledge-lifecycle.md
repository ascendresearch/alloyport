# 0008: Evidence-backed knowledge lifecycle

- Status: Proposed
- Date: 2026-08-09
- Scope: facts, failed routes, reusable procedures, promotion, retrieval, staleness, and retraction

## Context

An operator factory should improve with experience, but self-authored memory can amplify false
conclusions. Raw transcripts are noisy; success-only recipes hide boundary conditions; concise
summaries lose critical detail; framework, compiler, driver, and hardware changes make old advice
stale. Knowledge must therefore retain both evidence and scope.

## Decision

AlloyPort maintains a typed, evidence-backed knowledge base separate from task state. Agents may
propose entries. Only independent verdicts and policy can promote them for automatic reuse.

## Entry types

- `Fact`: an observed, scoped property such as an unsupported dtype or compiler behavior.
- `FailurePattern`: symptom, diagnosis, rejected routes, and evidence that distinguishes it.
- `Procedure`: ordered diagnostic or implementation steps with prerequisites and stop conditions.
- `OptimizationPattern`: dataflow/schedule idea, applicable domain, expected bottleneck, and risks.
- `BackendCapability`: versioned feature claim backed by a probe.
- `NegativeKnowledge`: a route that failed under a precise scope and what new evidence would justify
  trying it again.
- `ResearchNote`: useful external knowledge that has not yet been validated in AlloyPort.

Hypotheses remain task-local until evidence supports generalization. A successful candidate is not
automatically a reusable procedure.

## Evidence tiers

- `T0 Proposed`: authored or extracted, with no AlloyPort verification.
- `T1 Observed`: backed by one valid receipt in one environment.
- `T2 Reproduced`: independently replayed or observed across multiple relevant cases.
- `T3 Release-backed`: used by released artifacts with continuing audits and explicit scope.

Promotion is monotonic in evidence requirements, not permanent truth. Entries can become stale,
contradicted, or retracted at any tier.

## Required provenance

Every non-T0 entry references source material or immutable receipts, applicable operator/backend/
hardware/toolchain versions, corpus domains, author and reviewer identities, creation time, last
verification time, and known counterexamples. Derived entries retain links to all parents.

Natural-language text is explanatory. Machine-checkable scope and evidence references control reuse.

## Promotion and retraction

Promotion checks:

- supporting verdicts are authoritative and still replayable;
- the proposed scope does not exceed tested cases;
- contradictory and failed evidence is included rather than filtered out;
- the entry adds information not already represented by a narrower or stronger item;
- reuse instructions include prerequisites, validation steps, and fallback;
- no candidate-generating agent is also the sole reviewer.

New contradictory evidence immediately marks the entry `contested`. Automated retrieval may still
surface it as a warning, but cannot apply it as trusted procedure. Retraction preserves the entry,
reason, evidence, and descendants requiring re-audit.

## Retrieval

Retrieval filters by hard scope before semantic similarity: operator family, framework API,
backend, hardware, compiler/runtime/driver versions, dtype, layout, and task phase. Results include
confidence tier, counterexamples, staleness, and evidence links.

The context builder favors a small set of high-scope-match entries plus relevant negative knowledge.
It does not merge them into an uncited summary. Raw supporting evidence remains recoverable through
content-addressed references.

## Knowledge versus task memory

Task memory answers “what is true and pending in this task?” Knowledge answers “what previously
verified pattern may help?” Importing knowledge creates a task-local hypothesis until current probes
confirm compatibility. This prevents a once-valid recipe from silently becoming current environment
truth.

## Audits

Scheduled audits sample entries for citation resolution, evidence replay, scope correctness, stale
versions, contradictory outcomes, and downstream usage. A knowledge-quality dashboard reports counts
by tier, age, backend, evidence health, successful reuse, false reuse, and retraction rate. Volume is
not a success metric.

## Rejected alternatives

- Store raw transcripts as reusable knowledge: they are noisy, unscoped, and difficult to invalidate.
- Learn only from successful runs: failed routes and boundary conditions prevent repeated waste.
- Let agents auto-promote self-judged lessons: memory would inherit the generator's blind spots.
- Repeatedly rewrite one playbook: concise updates can erase provenance and counterexamples.

## Verification plan

- Entries without evidence cannot pass T0.
- Promotion fixtures reject missing scope, self-judged success, stale receipts, and hidden failures.
- Version changes invalidate matching capabilities until probes rerun.
- Contradictory evidence marks entries and their dependent procedures for re-audit.
- Retrieval never returns an out-of-scope entry as automatically applicable.
- Removing all summaries still leaves enough provenance to inspect and replay the source evidence.
- Negative knowledge prevents identical retries unless the new attempt records a justified delta.

## Research and implementation basis

- [ReasoningBank](https://arxiv.org/abs/2509.25140) shows the value of distilling both successful and
  failed experience into reusable reasoning memory. AlloyPort replaces self-judged success with
  independent verdicts before promotion.
- [Agentic Context Engineering](https://arxiv.org/abs/2510.04618) identifies brevity bias and context
  collapse during iterative rewriting, motivating structured incremental updates rather than a
  repeatedly overwritten playbook.
- [ARC](https://arxiv.org/abs/2601.12030) motivates active repair of working context. AlloyPort keeps
  such working memory separate from canonical knowledge and evidence.
- [SAM](https://arxiv.org/abs/2605.24468) motivates compact cues that retain access to raw trajectory
  pages and documents how memory can amplify an incorrect early frame.
