# 0043 — A spending allowance is not part of what an Episode means

- Status: Accepted and implemented
- Date: 2026-08-16
- Corrects: [0033](0033-durable-agent-episode-repository.md), and the resumption shipped earlier
  the same day
- Evidence: [`agent-loop-review-20260816.md`](../evidence/agent-loop-review-20260816.md)

## Context

Resumption shipped able to reopen a `Failed` Episode and unable to reopen a `BudgetExhausted` one.
That is inverted on its face: `Failed` is a defect, while `BudgetExhausted` is the operator's own
cap working exactly as designed. The Agent SDK's documentation for the equivalent outcome says
plainly, "Agent ran out of turns. Resume with a higher limit" — budget exhaustion is the *canonical*
resumable state, and this project had made it the only unresumable one.

The reason was real. `loop_policy_digest` is inside `AgentEpisodeRecord::matches_immutable`, so the
budget an Episode ran under is part of its identity, and continuing past a spent budget means
running under a policy the record does not describe. The first implementation accepted that and
wrote a justification: continuing is a *fork*, not a resumption.

That justification defended a defect.

## The distinction that was missing

`AgentLoopPolicy` held six knobs that are two different kinds of thing. The test that separates them
is whether changing the value changes what the **already recorded** turns mean.

- `max_model_turns` 20 → 30 changes nothing about turn 7. Same model, same prompt, same tools, same
  gates. The only thing that moved is how much the operator is willing to pay.
- `max_tool_calls_per_turn` 4 → 6 does change it. A six-call turn was illegal before and legal
  after, so the shape of a valid turn moved.

The first is an **allowance**; the second is a **rule**. Only rules belong in identity.

The codebase already contained the intent. `AgentEpisodeRecord` carries two separate identity
fields, `loop_policy_digest` and `budget_snapshot_digest`, the latter derived under the schema name
`alloyport-episode-budget-v1` — two slots for exactly this distinction, both fed from the same
`AgentLoopPolicy`. The split was designed and never realised, the same shape as 0025's
`ModelVisibleToolResult`, whose name survived while its contents did not.

## Decision

`EpisodeRules` — `max_tool_calls_per_turn`, `max_ambiguous_model_attempts`,
`max_stop_feedback_turns` — is what `loop_policy_digest` now covers and stays inside
`matches_immutable`.

`max_ambiguous_model_attempts` is a rule, not an allowance, because raising it changes what a
finished Episode means: it may now conclude carrying more external effects nobody could confirm.

`EpisodeAllowance` — `max_model_turns`, `max_model_attempts`, `max_total_tool_operations` — is
outside identity. `matches_runtime_spec` compares rules and no longer compares the allowance, and
`budget_snapshot_digest` leaves `matches_immutable` because it records the allowance the Episode was
*created* under, which a later grant is expected to exceed.

Every finished-but-continuable Episode is reopenable, and each reopening records an
`AllowanceGrant { resumed_from, previous, granted }`. `Succeeded` and `Cancelled` stay closed: one
has nothing to continue, the other was stopped on purpose.

A configuration edit never re-budgets a run in flight. The allowance is applied only when reopening
a finished Episode, so recovery of a live Episode keeps the allowance it was running under.

## Why this is more honest, not less

Before, a raised budget was unrepresentable, so an operator who wanted to keep going had to abandon
the Episode and start another. Five attempts left five unrelated records and five re-read corpora
instead of one audit trail. The immutability bought nothing it claimed: it did not prevent the
spending, it only prevented the spending from being written down.

## Consequences

- `loop_policy_digest` changes meaning, so **existing Episode databases become unrecoverable**. All
  five archived Episodes are failures nobody would resume, so the cost is near zero now and rises
  with every future run. That is the argument for doing it immediately rather than later.
- `durable_episode_state` gains `grants`, defaulted, so older states still load.

## What this does not do

- **No cost or token budget exists yet.** The allowance still counts turns, attempts, and
  operations, not money, while every turn bills. That gap is recorded in the loop review and
  untouched here.
- **Nothing forks.** An Episode whose *rules* need to change is still a new Episode, correctly.
- **The grant is not authorised against anything.** Any operator who can resume can raise the
  allowance arbitrarily; there is no ceiling, no approval, and no per-owner limit.
