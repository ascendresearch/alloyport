# Design 0037: Explicit Candidate Episode bootstrap

- Status: Implemented
- Date: 2026-08-12
- Extends: Design 0036
- Scope: operator configuration, preflight, dispatch authorization, and supervised execution

## Context

The production Candidate composition existed only as a library boundary. Starting the server could
not run it, and adding an implicit startup hook would make ordinary service restarts capable of
triggering external model charges before worker policy was ready.

## Decisions

`candidate-episode validate CONFIG` loads one strict schema-1 operator document. It resolves paths
relative to the document, validates the catalog and selected alias, securely reads the configured
credential without printing it, derives protocol headers, loads the exact MigrationSpec and CUDA
reference files, deterministically appends the exact contract and declared source bytes to the
operator task text, derives both context-projection and input-root identities, bounds prompt/context
bytes, derives the remaining subtask, data-boundary, episode-budget, and request-budget identities
from their exact policies, validates the configured fixed worker/image/resource
targets, and checks durable state/workspace paths. It performs no listener, worker, or provider
network operation.

`candidate-episode run CONFIG --authorize-provider-dispatch` is the only live entry point. The
authorization is a required literal command argument and cannot be persisted in configuration.
The process owns the normal gRPC services and Candidate runner under one bounded supervisor. The
runner waits for connected workers advertising the exact Build/CUDA-correctness/Ascend-correctness
features before its first Episode advance, polls pending Gate work at a configured positive
interval, and shuts the process down when the Episode succeeds or requires operator intervention.

The server assembly now retains the same typed Artifact store used by the Artifact service so the
Episode, Gate adapters, and workers share one CAS. Worker snapshots expose registered features for
read-only readiness checks. Candidate assignment network policy remains hard-coded disabled rather
than configurable.

## Boundary

This slice does not create credentials, authorize a billable call, manufacture image identity
metadata, or deploy accelerator workers. The checked-in example is deliberately invalid until an
operator replaces zero digests/sizes and the example context projection.
