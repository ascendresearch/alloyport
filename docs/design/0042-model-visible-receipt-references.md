# 0042 — The model-visible result must carry the references its tools require

- Status: Proposed
- Date: 2026-08-16
- Completes an unimplemented part of: [0025](0025-pluggable-llm-provider-architecture.md) §7.3
- Repairs contracts written against it: [0026](0026-ascend-build-gate-connection.md) §2,
  [0041](0041-instruments-and-evidence-domains.md)
- Evidence: [`first-real-migration-20260816.md`](../evidence/first-real-migration-20260816.md)

## Context

`task-c36ab7b63cbf64234498b88b` produced the first model-authored candidate to pass the Source Gate,
then died on `request_ascend_build` with `SourceGateReceiptMismatch`. The model had no way to
satisfy the call: its turn-12 input carried 17 distinct digests and not the one required.

The tempting diagnoses are both wrong.

It is **not** that the model erred. There was no correct value available to it.

It is **not** that the citation is ceremony to be deleted. 0026 §2 requires it deliberately, and
0036 states the principle it serves: model tool schemas "expose only content/digest identities", so
that the model can *point at* immutable content but never *choose* a worker, image, device, command,
corpus, or tolerance. Digests are the model's entire vocabulary. Removing them narrows what the
model can say, it does not fix anything.

The actual defect is upstream of both. 0025 §7.3 specifies the model-visible projection:

```rust
pub struct ModelVisibleToolResult {
    pub operation_id: ToolOperationId,
    pub status: ToolResultStatus,
    pub summary: BoundedText,
    pub observations: Vec<TypedObservation>,
    pub artifacts: Vec<ArtifactRef>,
    pub receipts: Vec<ReceiptRef>,
    pub authority: ResultAuthority,
    pub retry_hint: RetryHint,
    pub omitted: Option<OmissionNotice>,
}
```

What exists is:

```rust
pub struct ModelVisibleToolResult<'a> {
    pub native_call_id: &'a str,
    pub output: &'a str,
}
```

**The name was kept and the contents were not.** `receipts` is precisely the field that would have
made `source_gate_receipt_digest` obtainable. Every gateway call already computes
`receipt_digests` and stores it durably on the tool operation record; it is dropped on the way to
the model. Contracts were then written against the designed projection while the model was handed
the stub.

`submit_candidate_bundle` is the control that proves this reading. It is the only link in the chain
that works, and the only one that does not depend on the projection: it publishes a **wrapper**
result naming a *separate* artifact.

```json
{"candidate_id": "candidate-3fb3ae54…",
 "manifest": {"digest": "sha256:98913b93…", "size_bytes": 2088,
              "media_type": "application/vnd.alloyport.candidate-source-manifest+json"},
 "source_bundle_digest": "sha256:7695909d…"}
```

A result can name a neighbour's digest. It can never name its own — that is circular under a content
hash, which is why the receipt could not simply be given a `receipt_digest` field. Every broken link
asks the model for the result's own digest; the working link asks for a neighbour's.

## Decision

### 1. A gate result is a wrapper that names its receipt

`request_source_gate`, `request_ascend_build`, and `request_reduction_correctness` publish two
artifacts: the receipt, unchanged and still the thing gates and audits read; and a result document
naming it, which is what `result_digest` points at and therefore what the model reads.

```json
{"status": "succeeded",
 "receipt": {"digest": "sha256:…", "size_bytes": N,
             "media_type": "application/vnd.alloyport.source-gate-receipt+json"},
 "passed": true,
 "failures": []}
```

This implements 0025 §7.3's `receipts` for the tools that need it, using the mechanism already
proven by `submit_candidate_bundle`. It requires no change to the codecs, the provider transport,
the pending-result store, or the continuation binding, because the durable input digest binds
artifact digests rather than rendered text.

Receipt *content* does not change. `verify_source_gate_receipt` and `read_build_diagnostics` still
resolve and re-evaluate the inner receipt exactly as they do now.

### 2. Tool argument contracts are unchanged

0026 §2 and 0041 stand as written. The model still supplies only content/digest identities, and
authority still flows from re-evaluating the receipt rather than from the digest the model names.
The change is that the value it is required to name is now a value it has been shown.

### 3. A citation mismatch is recoverable

`verify_source_gate_receipt` returns a recoverable `CandidateFailed` when the Source Gate finds
something blocking, and a fatal `adapter_error` when the gate passes and the citation mismatches —
two branches of one function, and 0040's class fix reached only one of them. The second becomes a
terminal-and-recoverable rejection carrying a readable artifact that names the digest the tree
actually produces.

This is defence in depth, not the fix. With decision 1 the model can cite correctly; decision 3
means that if it cites a real-but-wrong digest anyway, it gets a correction turn instead of a dead
migration.

### 4. The invariant, and the test that enforces it

> **The model may only be required to name a value that some earlier tool result showed it.**

This is checkable and was never checked. The existing test takes its digests from
`let (_, receipt) = execute(&mut gateway, …)` — the gateway hands a *test caller* the result digest
as a return value. The mechanism was verified for a caller who is given the digest, and nobody asked
whether the runtime model is such a caller.

That is this repository's one mistake in its usual costume: **we applied "don't trust, verify" to
the model and exempted the scaffolding that stood in for it.** The scaffolding supplied the one
thing the real caller can never obtain.

The new test drives the full chain — submit, Source Gate, Build Gate, diagnostics, Correctness —
taking **every** argument only from bytes the model would have received, and fails if a required
digest cannot be found there. A test that may not read the gateway's return value is the only kind
that can hold this invariant.

## Consequences

- The tool-catalog digest and prompt revision change, so a new Episode is required. Recovery of the
  archived failed episode is unaffected; its receipts remain readable.
- Two artifacts per gate call instead of one. Both are small and content-addressed.
- `read_build_diagnostics`, added by 0041 so the model could read the compiler's opinion of its own
  source, becomes invocable for the first time.

## What this does not do

- **It does not implement 0025 §7.3.** `operation_id`, `summary`, `observations`, `authority`,
  `retry_hint`, and `omitted` remain unbuilt, and the receipt is still rendered inline rather than
  summarised with the full bytes behind a reference. This decision takes only the slice that
  unblocks the chain, and the remainder stays visibly outstanding rather than quietly assumed.
- **It does not establish that any generated kernel is correct.** No Ascend C has been compiled. The
  failed run says nothing about the candidate's quality, because a Source Gate pass is deliberately
  almost silent about method.
- **It does not address the corpus gap.** `read_reference` serves 127 of 1099 vendored files while
  the cards cite the other 972 by path. The trust ledger has one row per card, so serving sub-files
  is a ledger decision and belongs in its own decision.
