# Vendored reference corpus

AlloyPort's runtime model has to write Ascend C. Without reference material it writes against the
API from its weights alone, and the only signal it gets is whether the result compiles. This
directory is the initial knowledge base shipped alongside the product.

## What is here

- `cannbot-skills/` — a pinned snapshot of the CANNBot skills corpus: 127 `SKILL.md` documents plus
  their `references/`, covering Ascend C, Catlass, PyPTO, TileLang and Triton operator development,
  `torch.compile` graph mode, and NPU inference optimization.
- `cannbot-skills-audit.jsonl` — one row per document, carrying its trust state and content hash.

## Provenance

- Upstream: `https://gitcode.com/cann/cannbot-skills.git`, branch `main`.
- Snapshotted 2026-07-12 by the sibling `ascend-factory` project, from a copy on a shared host.
  Re-vendored here 2026-08-16 unchanged apart from that project's own snapshot note.
- Kernel sources, build assets, and test harnesses were left behind upstream. We navigate these
  documents; we do not run their pipelines.

**Vendored, not referenced, on purpose.** A knowledge base whose contents point at a path on
somebody else's machine is a bet that nothing there is ever deleted, re-permissioned, or re-imaged.

## Licensing — unresolved, and it must be resolved before distribution

AlloyPort is MIT licensed. **This directory is not.** Upstream states `CANN Open Software License
2.0`, and its `README.md` links to a `LICENSE` file — **which the snapshot did not capture**. The
license text is therefore absent here, and nothing in this repository establishes the terms under
which the corpus may be redistributed.

Two obligations follow, neither of them done:

1. Fetch the upstream `LICENSE` into `cannbot-skills/` and verify the terms cover redistribution
   inside an MIT-licensed distribution.
2. If they do not, this directory must be fetched at install time rather than shipped.

Until then, treat this as an internal development corpus.

## Trust — snapshotting freezes the content, it does not make the content true

Every row in `cannbot-skills-audit.jsonl` carries a `status`:

| status | what it means | how it is served |
|---|---|---|
| `unaudited` | nobody has read it | with a caution |
| `reviewed` | somebody read it and recorded a verdict — **a claim, not a verification** | with a caution and the verdict's note |
| `validated` | a probe on AlloyPort's own hardware confirmed a named claim | quietly |
| `refuted` | a probe disproved a claim | never served without its refutation |

**Nothing here is `validated`, because AlloyPort has run no probe.** 41 rows are `reviewed`, and
their verdicts are `imported_review`: they were recorded by the sibling project against this exact
snapshot, and are carried forward with that provenance rather than adopted as ours. An imported
judgement is somebody else's reading; it is worth more than nothing and less than a measurement. The
remaining 86 are `unaudited`.

The two documents an optimization task would reach for first are both `suspect`:

- `ops/ascendc-perf-optimize` — "HOLLOW: 2 of 4 reference dirs empty; skill admits it."
- `ops/ascendc-performance-best-practices` — "numbers are A2-validated; A5 'needs re-verification'."

A2 is the previous generation. AlloyPort targets `Ascend950PR`. Serving those numbers as facts is
exactly the failure the ledger exists to prevent, which is why the corpus and the ledger travel
together and why the reader refuses to serve a document without its status.

`content_sha` binds each row to the bytes it was recorded against. Edit a document and its verdict
dies with it, because a review is a claim about bytes.
