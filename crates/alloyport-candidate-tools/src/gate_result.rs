//! The model-visible gate result: a document that names the receipt a later tool must cite.
//!
//! [Design 0025](../../../docs/design/0025-pluggable-llm-provider-architecture.md) §7.3 specifies a
//! model-visible projection carrying `artifacts` and `receipts`. What exists is
//! `ModelVisibleToolResult { native_call_id, output }`, so `receipt_digests` — which every gateway
//! call already computes and stores durably — never reached the model. Contracts were then written
//! against the designed projection: `request_ascend_build` requires the Source Gate receipt digest
//! and `read_build_diagnostics` requires the Build Gate receipt digest, and on the first real
//! migration the model had neither and could not have had either.
//!
//! A result cannot name its own digest — that is circular under a content hash — which is why the
//! receipt could not simply gain a `receipt_digest` field. So each gate publishes two artifacts:
//! the receipt, unchanged and still what gates and audits re-evaluate, and a result document naming
//! it, which is what `result_digest` points at and therefore what the model reads. That is the
//! shape `submit_candidate_bundle` already proved by naming its manifest as a separate artifact —
//! the one link in the chain that worked, and the only one that never depended on the projection.

use crate::gateway::{adapter_error, ingest_bytes};
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{Sha256Digest, ToolGatewayError};
use serde_json::{Value, json};

pub(crate) const SOURCE_GATE_RECEIPT_MEDIA_TYPE: &str =
    "application/vnd.alloyport.source-gate-receipt+json";
pub(crate) const ASCEND_BUILD_RECEIPT_MEDIA_TYPE: &str =
    "application/vnd.alloyport.ascend-build-receipt+json";
pub(crate) const CORRECTNESS_RECEIPT_MEDIA_TYPE: &str =
    "application/vnd.alloyport.reduction-correctness-receipt+json";
pub(crate) const CALIBRATION_RECEIPT_MEDIA_TYPE: &str =
    "application/vnd.alloyport.reduction-calibration-receipt+json";

/// Both digests one gate call produces: the wrapper the model reads, and the receipt it may cite.
pub(crate) struct PublishedGateResult {
    pub result_digest: Sha256Digest,
    pub receipt_digest: Sha256Digest,
}

/// Publishes a receipt and the result document that names it.
///
/// The receipt keeps its exact bytes, because `verify_source_gate_receipt` and
/// `read_build_diagnostics` re-evaluate or reparse those bytes and a re-encoding would change the
/// digest they compare against.
///
/// # Errors
///
/// Returns an adapter error when the receipt is not valid JSON or either artifact cannot be stored.
pub(crate) fn publish_gate_result(
    artifacts: &dyn ArtifactStore,
    receipt_bytes: &[u8],
    media_type: &str,
) -> Result<PublishedGateResult, ToolGatewayError> {
    publish_gate_result_with(artifacts, receipt_bytes, media_type, Vec::new())
}

/// Publishes a receipt and a result document naming it plus any sibling receipts.
///
/// # Errors
///
/// Returns an adapter error when the receipt is not valid JSON or either artifact cannot be stored.
pub(crate) fn publish_gate_result_with(
    artifacts: &dyn ArtifactStore,
    receipt_bytes: &[u8],
    media_type: &str,
    also: Vec<(&str, Sha256Digest, u64, &str)>,
) -> Result<PublishedGateResult, ToolGatewayError> {
    let receipt = ingest_bytes(artifacts, receipt_bytes)?;
    let document: Value = serde_json::from_slice(receipt_bytes)
        .map_err(|error| adapter_error(format!("cannot reread gate receipt: {error}")))?;
    let mut wrapper = json!({
        "receipt": descriptor(receipt.digest, receipt.size_bytes, media_type),
        "result": document,
    });
    for (name, digest, size_bytes, sibling_media_type) in also {
        wrapper[name] = descriptor(digest, size_bytes, sibling_media_type);
    }
    let bytes = serde_json::to_vec(&wrapper)
        .map_err(|error| adapter_error(format!("cannot encode gate result: {error}")))?;
    let result = ingest_bytes(artifacts, &bytes)?;
    Ok(PublishedGateResult {
        result_digest: result.digest,
        receipt_digest: receipt.digest,
    })
}

fn descriptor(digest: Sha256Digest, size_bytes: u64, media_type: &str) -> Value {
    json!({
        "digest": digest,
        "size_bytes": size_bytes,
        "media_type": media_type,
    })
}

/// Publishes a readable explanation for a citation the tree does not produce.
///
/// A model that names a real artifact which is not the receipt for this candidate has made a defect
/// it can read and fix. Design 0040 established that such a defect is terminal-and-recoverable
/// rather than fatal, and applied it to the branch where the Source Gate finds something blocking;
/// the branch where the gate passes and the citation disagrees kept an adapter error one line away.
/// The explanation names the digest this tree actually produces, so the correction is a reissue
/// rather than a guess.
///
/// # Errors
///
/// Returns an adapter error when the explanation cannot be stored.
pub(crate) fn publish_citation_rejection(
    artifacts: &dyn ArtifactStore,
    tool: &str,
    argument: &str,
    cited: Sha256Digest,
    expected: Sha256Digest,
    media_type: &str,
) -> Result<Sha256Digest, ToolGatewayError> {
    let explanation = json!({
        "rejected": true,
        "tool": tool,
        "reason": format!(
            "{argument} does not name the receipt this candidate produces"
        ),
        "cited": cited,
        // No size is stated here: this explanation knows the digest the tree produces, and a
        // fabricated length beside it would be an assertion nobody measured.
        "expected_receipt": json!({"digest": expected, "media_type": media_type}),
        "recoverable": true,
        "guidance": "no candidate, build, or run was affected. Reissue this call with \
                     expected_receipt.digest, which is the receipt the gate published for this \
                     exact candidate.",
    });
    let bytes = serde_json::to_vec(&explanation)
        .map_err(|error| adapter_error(format!("cannot encode citation rejection: {error}")))?;
    ingest_bytes(artifacts, &bytes).map(|artifact| artifact.digest)
}

/// Publishes a rejection for a citation that does not resolve to the expected kind of receipt.
///
/// Unlike [`publish_citation_rejection`] this one does not know which artifact the model meant —
/// several builds may exist — so it names the shape to cite instead of inventing a digest. Saying
/// "expected: the value you already sent" would be a correction that corrects nothing.
///
/// # Errors
///
/// Returns an adapter error when the explanation cannot be stored.
pub(crate) fn publish_unresolved_citation(
    artifacts: &dyn ArtifactStore,
    tool: &str,
    argument: &str,
    cited: Sha256Digest,
    expected_media_type: &str,
) -> Result<Sha256Digest, ToolGatewayError> {
    let explanation = json!({
        "rejected": true,
        "tool": tool,
        "reason": format!("{argument} does not name a {expected_media_type} document"),
        "cited": cited,
        "recoverable": true,
        "guidance": "nothing was affected. The gate result that published this receipt carries a \
                     `receipt` object; reissue this call with that object's `digest`, not the \
                     digest of the result document itself.",
    });
    let bytes = serde_json::to_vec(&explanation)
        .map_err(|error| adapter_error(format!("cannot encode citation rejection: {error}")))?;
    ingest_bytes(artifacts, &bytes).map(|artifact| artifact.digest)
}
