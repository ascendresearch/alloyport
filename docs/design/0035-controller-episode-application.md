# Design 0035: Controller Episode application

- Status: Implemented application slice
- Date: 2026-08-12
- Extends: Designs 0025, 0033, and 0034
- Scope: model resolution, durable recovery identity, bounded HTTPS composition, and runner ownership

## Context

The Episode reducer, provider SDK, Candidate tools, Episode repository, and provider context store
were individually connected only in tests or through low-level ports. There was no controller-owned
application object that created their matching identities, recovered an existing Episode, and drove
one durable external action. Constructing those pieces ad hoc would risk selecting a changed model,
prompt, tool catalog, budget, or deployment while resuming old state.

## Decisions

`ControllerEpisodeApplication` is the controller use case for one model-pinned Episode. Its typed
spec includes the Task/Search/Episode identities, prompt and context facts, model alias, exact tool
definitions, Agent loop policy, and independent budget/data-boundary identities. Construction
resolves the runtime-model catalog and derives the complete model, deployment, profile, tool
catalog, and loop-policy digests rather than accepting those derived identities from an operator or
model.

The application creates or exactly recovers both `SQLite` stores, pins one provider gateway,
decorates the supplied narrow `AgentToolGateway` with durable result recording, and owns the
stateless runner and fault boundary. `advance` performs at most one externally visible action;
scheduling and delay policy remain outside the reducer. `open_https` selects the bounded Reqwest
transport whose redirect, proxy, and retry restrictions remain deployment policy.

Recovery compares every immutable Episode and runtime binding. The durable-state schema is revised
to version 2 so it retains the initial input digest separately from the evolving next-input digest;
older snapshots fail closed. Model, deployment, profile, and loop policy now expose canonical
digest functions used by the composition root. Matching context creation is idempotent, while a
changed prompt or tool catalog conflicts.

## Scope boundary

The application accepts a typed tool gateway so production can supply the existing
`CandidateToolGateway` with Build and Correctness attempt ports, while tests use deterministic
adapters. This slice does not add an operator command, choose worker IDs/images, read a live catalog
path, or authorize a billable provider call. Those are bootstrap/configuration responsibilities and
the remaining prerequisites for the first real candidate run.
