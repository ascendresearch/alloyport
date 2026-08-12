# Documentation

Project documentation is divided by purpose:

- [`HANDOFF.md`](HANDOFF.md): current implementation state, verification baseline, known gaps, and the
  recommended starting point for the next Codex session.
- [`worker-configuration.md`](worker-configuration.md): standalone and remote worker bootstrap,
  registry-optional image identity, and shared device-selection rules.
- [`server-configuration.md`](server-configuration.md): schema-versioned server bootstrap, local
  defaults, environment precedence, TLS, storage, and identity administration.
- [`ARCHITECTURE_EVOLUTION_PLAN.md`](ARCHITECTURE_EVOLUTION_PLAN.md): active incremental layering,
  composition-root, port, API, configuration, and process-lifecycle improvements.
- [`PORT_CONTRACTS.md`](PORT_CONTRACTS.md): prioritized adapter-conformance inventory and the shared
  behavioral-suite rule.
- [`design/`](design/README.md): numbered architecture and system-design documents.
- Future operational guides, user guides, and research notes belong in their own directories and
  must link back to the design decisions they implement.

Design documents define intended behavior. Code, tests, receipts, and release manifests remain the
authority for what a particular revision actually does.
