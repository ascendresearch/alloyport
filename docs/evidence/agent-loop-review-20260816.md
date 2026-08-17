# Agent loop review: what five failed migrations taught, and what mature loops do

- Date: 2026-08-16
- Scope reviewed: the Agent Episode reducer, the tool gateway, the provider gateway and transport,
  and the driver that advances the loop. **Not reviewed**: the worker/executor plane, the artifact
  store, the gRPC services, the CUDA/Ascend harnesses.
- Evidence: [`fatal-harness-defects-20260816.md`](fatal-harness-defects-20260816.md),
  [`first-real-migration-20260816.md`](first-real-migration-20260816.md)

Five paid migrations died. **None died because the model wrote a bad kernel.** Every one died inside
the harness. This review asks what the loop is missing that mature loops have, and it is anchored on
observed failures rather than on taste.

## The one sentence

> Everything that killed a run was a condition the model could not see, could not satisfy, or would
> have complied with had it been told — enforced by a layer that had no business ending anything.

## What mature loops do, and where AlloyPort stood

The Claude Agent SDK's rule is that a tool handler returning `is_error` keeps the loop alive so the
model can adapt, while throwing kills the loop and the model never sees it. A denied tool "receives a
rejection message as the tool result"; a `PreToolUse` hook that rejects a call "prevents it from
executing, and Claude receives the rejection message instead". LangGraph states the same thing as a
four-way classification — transient (retry with backoff), LLM-recoverable (return the error to the
model), user-fixable (interrupt), unexpected (crash loudly) — and calls the classification matrix
"the whole strategy".

AlloyPort had the right design and the wrong wiring.

| Concern | Mature pattern | AlloyPort before | Now |
|---|---|---|---|
| Model-fixable tool error | returned to the model | `ToolGatewayError::Adapter` → run dies | `Citation` → correction turn |
| Pre-execution validation | hook rejects, model sees it | `validate_call` existed, decorator swallowed it | forwarded; no default to inherit |
| Transient failure | retry with backoff | immediate retry, forever | hint honoured, bounded backoff |
| Deterministic failure | do not retry | retried until budget gone | `Never` and repeat-detection stop it |
| Termination | typed result subtype | some paths raised a state-machine error | budget is a verdict |
| Failure evidence | error text returned | digest of bytes nobody stored | published before recording |

The gap was never conceptual. `validate_call`'s doc comment already described the mature rule
precisely; `ModelTransportRetryHint` already parsed `Retry-After`. Both were unreachable.

## Fixed in this review

Each was verified red against the code exactly as it shipped.

1. **Citation vs infrastructure.** One `Adapter` variant covered "wrong digest" and "broken store".
   Split, with one chokepoint, and tested in *both* directions — because after the change no test in
   the crate asserted a fatal error at all.
2. **`validate_call` reached production.** The decorator forwarded three methods and not this one;
   its default was `Ok(())`. The default is deleted, so omission is a compile error.
3. **Rejections bind to their call.** A rejected call still owes the protocol a result; without it
   every later dispatch failed identically. This was introduced by fix 2 and found by fix 4.
4. **Diagnostics are published, not hashed.** Six sites hashed a string or a literal label and stored
   the hash. It is what made run 5 undiagnosable.
5. **Retry guidance is honoured.** `Never` stops, `AfterMillis` waits, everything else gets bounded
   exponential backoff, and a byte-identical repeat is treated as deterministic.
6. **Budget exhaustion is a verdict.** `TurnRecorded` never listed `BudgetExhausted`, so both
   conditions it guarded were dead code that could only raise. A too-wide turn now costs a turn
   rather than being misrecorded as an exhausted budget.
7. **Instruments cannot end a migration**, and the four-calls-per-turn bound is stated in the prompt
   because it is invisible from the tools.

## Not fixed, in the order I would take them

**1. There is no cost budget.** `AgentLoopPolicy` bounds turns, attempts, ambiguous attempts, calls
per turn, operations, and stop-feedback turns — and nothing about tokens or money. The Agent SDK has
`max_budget_usd` alongside `max_turns` and treats hitting it as a normal typed result. This project
bills real money on every turn and has now spent five runs; the absence is conspicuous.

**2. A failed episode cannot be resumed.** `migrate --retry` mints a new task and a new episode, so
runs 2 through 5 each re-read the same corpus documents from scratch before doing anything. The Agent
SDK resumes from `session_id` and explicitly suggests it after `error_max_turns`. The episode store
here is already durable and content-addressed; resume is a missing entry point, not a missing
capability.

**3. There is no context compaction.** Run 1 grew 4 130 → 74 373 input tokens across 12 turns, about
10k per corpus read, against a 128 000 ceiling. Nothing summarises, nothing warns, and no behaviour
is defined for reaching it. The Agent SDK compacts automatically and emits a boundary event.

**4. Explaining a failed run means reading SQLite and the CAS by hand.** That is how every diagnosis
in this session was made. With diagnostics now published there is something worth reading; there is
still no command that reads it.

**5. `read_reference` serves 127 of 1099 vendored files.** No longer theoretical: run 4 followed
citations inside the cards to `references/vf-declaration.md` and five more, and was refused six
times. The ledger has one row per card, so serving sub-files is a ledger decision.

**6. Smaller, verified, unfixed.** `read_bounded` on a digest the model named that does not exist is
still `Adapter` — the one citation-class site not converted, because the same helper serves internal
reads. `Created` and `CancellationPending` still do not permit `Failed`; no runtime path was found
that attempts it, which is an unverified absence rather than a cleared one. `Pending`'s
`diagnostic_digest` is computed by two attempt ports and discarded by the reducer.

## What this review is not

It did not read the worker plane, the artifact store, or the gRPC services, so it says nothing about
their robustness. It found no defect in the *judgement* parts of the system — gates, oracle,
tolerance — because it did not look there and because those had already been corrected by 0040
and 0041. And it establishes nothing about whether any generated kernel is correct: no Ascend C has
been compiled yet.

## Sources

- [How the agent loop works — Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk/agent-loop)
- [Handling Tool Calls in the Claude Agent SDK](https://team400.ai/blog/2026-04-claude-agent-sdk-handle-tool-calls)
- [LangGraph agent error handling in production](https://focused.io/lab/langgraph-agent-error-handling-production)
- [LangGraph error handling: retries and fallback strategies](https://machinelearningplus.com/gen-ai/langgraph-error-handling-retries-fallback-strategies/)
