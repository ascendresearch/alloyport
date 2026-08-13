# Design 0036: Gated Candidate Episode composition

- Status: Implemented composition slice
- Date: 2026-08-12
- Extends: Designs 0026 through 0029 and 0035
- Scope: production Candidate tool injection and fixed model-visible tool schemas

## Context

The controller Episode application accepted a narrow `AgentToolGateway`, but the production assembly
of that port still required callers to connect four Candidate tools, three independent Gate stages,
worker-control attempt adapters, reference policy, and local materialization consistently. A caller
could also supply a model-visible tool catalog that did not match the actual gateway.

## Decisions

`open_candidate_episode_https` is the reduction-specific composition root. It derives
`CandidateToolConfig` from the Task, MigrationSpec, and generation strategy; binds the fixed Ascend
Build worker and distinct CUDA/Ascend Correctness targets through their existing durable
worker-control adapters; installs the frozen reference/correctness policies; and creates the HTTPS
controller Episode over one Candidate workspace and Artifact store.

The composition overwrites any caller-supplied model tool list with four controller-owned strict
schemas: candidate submission, Source Gate, Ascend Build Gate, and paired reduction Correctness.
Schemas reject unknown fields and expose only content/digest identities. Images, devices, worker
IDs, corpus, tolerances, command lines, and resource policy remain outside model arguments. The
tool-catalog digest captured by the Episode therefore matches the callable production gateway.

## Scope boundary

This is a library composition root, not an implicit server-start side effect. A remaining explicit
operator bootstrap must load the specimen, real model catalog, secret-file location, worker/image
targets, state/CAS/workspace paths, and budgets; start or attach the worker-control service; then
schedule `advance` calls with bounded polling. No live or billable model call is performed here.
