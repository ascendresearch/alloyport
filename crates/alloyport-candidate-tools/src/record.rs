//! Reads one Episode's candidate lineage out of the record it already wrote.
//!
//! [Design 0044](../../../docs/design/0044-git-as-the-candidate-record.md) motivates this with a
//! question that was answered by eye several times in one session: *what changed between the build
//! that failed on `acl/acl.h` and the one that failed on `kernel_operator.h`?* Everything needed to
//! answer it was already durable — the Episode records every tool operation in order with the
//! digests it produced, and the CAS holds the manifests, the sources, and the receipts — and there
//! was no way to read it but by hand.
//!
//! Two boundaries are deliberate.
//!
//! **This is a reader, not a gate.** Receipts are parsed as JSON, never deserialized into the
//! domain types that carry gate authority — `SourceGateReceipt` is `Serialize` only, on purpose, and
//! reconstructing one here would be a second thing that can claim to know a verdict. Verdict text
//! goes into a commit message and nothing else. The manifest stays authoritative.
//!
//! **Sources are verified against the manifest, not trusted from the store.** Every file's bytes are
//! rehashed and its length checked before they can enter a commit. The record may be rebuilt at any
//! time, so a projection that silently disagreed with the manifest would be a lie that survives.

use crate::gateway::read_bounded;
use crate::record_stream::{
    RecordStreamError, RecordedCandidate, RecordedFile, RecordedGate, RecordedGateOutcome,
};
use alloyport_core::{
    AttemptOutcome, CandidateId, CandidateSourceManifest, DurableEpisodeState, Sha256Digest,
    ToolOperationRecord, ToolOperationStatus,
};
use serde_json::Value;
use std::collections::BTreeMap;

const MAX_RESULT_BYTES: u64 = 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_SOURCE_BYTES: u64 = 4 * 1024 * 1024;
/// Same bound `read_build_diagnostics` reads compiler output under; measured non-binding at 1 945 B.
const MAX_DIAGNOSTIC_BYTES: u64 = 64 * 1024;

/// Collects every candidate this Episode submitted, in submission order, with what the gates said.
///
/// # Errors
///
/// Returns an error when a digest the Episode recorded does not resolve, when a document it names
/// is not the shape that tool publishes, or when a source file's bytes disagree with the manifest.
/// None of those is tolerable quietly: the projection is rebuildable, so a record that dropped what
/// it could not read would look complete and be wrong.
pub fn collect_candidate_record(
    state: &DurableEpisodeState,
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
) -> Result<Vec<RecordedCandidate>, CandidateRecordError> {
    collect_from_operations(state.tool_operations(), artifacts)
}

/// The projection over an operation sequence, separated so both halves can be exercised directly.
///
/// An Episode only ever grows through the loop, so a defect that needs a particular operation shape
/// — a manifest whose descriptor disagrees with its object, a rejection where a receipt is expected
/// — cannot be reached through a real run. A verification that cannot be provoked is decoration.
pub(crate) fn collect_from_operations<'a>(
    operations: impl Iterator<Item = &'a ToolOperationRecord>,
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
) -> Result<Vec<RecordedCandidate>, CandidateRecordError> {
    let mut candidates: Vec<RecordedCandidate> = Vec::new();
    let mut position: BTreeMap<CandidateId, usize> = BTreeMap::new();
    for operation in operations {
        match operation.tool_name() {
            crate::SUBMIT_CANDIDATE_BUNDLE_TOOL => {
                if let Some(candidate) = read_submission(operation, artifacts)? {
                    // A byte-identical resubmission produces the same content-derived identity, so
                    // the first occurrence keeps the sequence rather than the tree gaining a twin.
                    if !position.contains_key(&candidate.candidate_id) {
                        position.insert(candidate.candidate_id.clone(), candidates.len());
                        candidates.push(candidate);
                    }
                }
            }
            tool => {
                let Some(gate) = gate_of(tool) else { continue };
                // A gate naming a candidate this Episode never submitted has nothing to attach to.
                if let Some((candidate_id, outcome)) =
                    read_gate_outcome(gate, operation, artifacts)?
                    && let Some(index) = position.get(&candidate_id).copied()
                {
                    candidates[index].outcomes.push(outcome);
                }
            }
        }
    }
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.sequence = u32::try_from(index + 1).unwrap_or(u32::MAX);
    }
    Ok(candidates)
}

fn gate_of(tool: &str) -> Option<RecordedGate> {
    match tool {
        crate::REQUEST_SOURCE_GATE_TOOL => Some(RecordedGate::Source),
        crate::REQUEST_ASCEND_BUILD_TOOL => Some(RecordedGate::AscendBuild),
        crate::REQUEST_REDUCTION_CORRECTNESS_TOOL => Some(RecordedGate::ReductionCorrectness),
        _ => None,
    }
}

/// Turns one successful submission into a candidate, or nothing when the call did not produce one.
fn read_submission(
    operation: &ToolOperationRecord,
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
) -> Result<Option<RecordedCandidate>, CandidateRecordError> {
    if operation.status() != ToolOperationStatus::Succeeded {
        return Ok(None);
    }
    let Some(result_digest) = operation.result_digest() else {
        return Ok(None);
    };
    let result = read_json(artifacts, result_digest, MAX_RESULT_BYTES)?;
    // A rejected or malformed submission publishes an explanation rather than a manifest reference.
    let Some(manifest_digest) = result
        .get("manifest")
        .and_then(|manifest| manifest.get("digest"))
        .and_then(Value::as_str)
        .map(str::parse::<Sha256Digest>)
        .transpose()
        .map_err(|_| CandidateRecordError::Malformed {
            digest: result_digest,
            expected: "manifest.digest",
        })?
    else {
        return Ok(None);
    };
    let manifest_bytes = read_bytes(artifacts, manifest_digest, MAX_MANIFEST_BYTES)?;
    let manifest: CandidateSourceManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|_| CandidateRecordError::Malformed {
            digest: manifest_digest,
            expected: "candidate source manifest",
        })?;
    let mut files = Vec::with_capacity(manifest.files().len());
    for file in manifest.files() {
        let descriptor = file.artifact();
        let bytes = read_bytes(artifacts, descriptor.digest, MAX_SOURCE_BYTES)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != descriptor.size_bytes
            || Sha256Digest::digest_bytes(&bytes) != descriptor.digest
        {
            return Err(CandidateRecordError::SourceIdentity {
                path: file.path().as_str().to_owned(),
                digest: descriptor.digest,
            });
        }
        files.push(RecordedFile {
            path: file.path().clone(),
            digest: descriptor.digest,
            bytes,
        });
    }
    Ok(Some(RecordedCandidate {
        // Assigned once the whole list is known, so it counts candidates rather than operations.
        sequence: 0,
        candidate_id: manifest.candidate_id().clone(),
        parent_candidate_id: manifest.parent_candidate_id().cloned(),
        manifest_digest,
        source_bundle_digest: manifest.source_bundle_digest(),
        files,
        outcomes: Vec::new(),
    }))
}

/// Finds the receipt that names a candidate among everything one gate call published.
///
/// A gate operation's receipts are not one document of one kind. The Correctness Gate publishes its
/// calibration receipt beside its verdict, and a build call whose citation was refused publishes a
/// rejection instead of a receipt. Selecting by shape rather than by position is what keeps a
/// calibration report from being read as a verdict.
fn read_gate_outcome(
    gate: RecordedGate,
    operation: &ToolOperationRecord,
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
) -> Result<Option<(CandidateId, RecordedGateOutcome)>, CandidateRecordError> {
    for receipt_digest in operation.receipt_digests() {
        let document = read_json(artifacts, *receipt_digest, MAX_RECEIPT_BYTES)?;
        if document.get("rejected").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let Some(candidate_id) = candidate_of(gate, &document) else {
            continue;
        };
        let Some((verdict, detail)) = verdict_of(gate, &document, artifacts)? else {
            continue;
        };
        return Ok(Some((
            candidate_id,
            RecordedGateOutcome {
                gate,
                verdict,
                detail,
                receipt_digest: *receipt_digest,
            },
        )));
    }
    Ok(None)
}

fn candidate_of(gate: RecordedGate, document: &Value) -> Option<CandidateId> {
    let raw = match gate {
        RecordedGate::Source | RecordedGate::AscendBuild => document.get("candidate_id"),
        RecordedGate::ReductionCorrectness => document
            .get("experiment")
            .and_then(|experiment| experiment.get("candidate_id")),
    }?;
    CandidateId::try_from(raw.as_str()?).ok()
}

/// Reads the gate's own word for what happened, and one line of why when the receipt carries one.
fn verdict_of(
    gate: RecordedGate,
    document: &Value,
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
) -> Result<Option<(String, Option<String>)>, CandidateRecordError> {
    match gate {
        RecordedGate::Source => {
            let Some(passed) = document.get("passed").and_then(Value::as_bool) else {
                return Ok(None);
            };
            Ok(Some((
                if passed { "passed" } else { "failed" }.to_owned(),
                blocking_findings(document),
            )))
        }
        RecordedGate::AscendBuild => {
            // A build receipt is the only document here carrying the source-gate citation it was
            // authorized by; a rejection or a Source Gate receipt does not.
            if document.get("source_gate_receipt_digest").is_none() {
                return Ok(None);
            }
            let passed = document.get("passed").and_then(Value::as_bool);
            let exit = document.get("exit_code").and_then(Value::as_i64);
            let verdict = match (passed, exit) {
                (Some(true), _) => "passed".to_owned(),
                (_, Some(code)) => format!("exit {code}"),
                _ => outcome_name(document).unwrap_or_else(|| "failed".to_owned()),
            };
            Ok(Some((verdict, build_detail(document, artifacts)?)))
        }
        RecordedGate::ReductionCorrectness => {
            let Some(verdict) = document.get("verdict").and_then(Value::as_str) else {
                return Ok(None);
            };
            let detail = document
                .get("failures")
                .and_then(Value::as_array)
                .filter(|failures| !failures.is_empty())
                .map(|failures| format!("{} oracle failure(s)", failures.len()));
            Ok(Some((verdict.to_owned(), detail)))
        }
    }
}

/// Names the findings that stopped a candidate. Advisories are deliberately not in the subject line.
fn blocking_findings(document: &Value) -> Option<String> {
    let kinds: Vec<&str> = document
        .get("failures")?
        .as_array()?
        .iter()
        .filter(|failure| failure.get("severity").and_then(Value::as_str) == Some("blocking"))
        .filter_map(|failure| failure.get("kind").and_then(Value::as_str))
        .collect();
    (!kinds.is_empty()).then(|| kinds.join(", "))
}

/// The compiler's first complaint, which is the line the next candidate has to answer.
fn build_detail(
    document: &Value,
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
) -> Result<Option<String>, CandidateRecordError> {
    let classification = document
        .get("detail")
        .and_then(Value::as_str)
        .filter(|detail| !detail.is_empty())
        .map(str::to_owned);
    let Some(digest) = document
        .get("stderr")
        .and_then(|stderr| stderr.get("digest"))
        .and_then(Value::as_str)
        .and_then(|text| text.parse::<Sha256Digest>().ok())
    else {
        return Ok(classification);
    };
    let bytes = read_bytes(artifacts, digest, MAX_DIAGNOSTIC_BYTES)?;
    let text = String::from_utf8_lossy(&bytes);
    let compiler = text
        .lines()
        .map(str::trim)
        .find(|line| line.contains("error:") || line.contains("Error:"))
        .or_else(|| text.lines().map(str::trim).find(|line| !line.is_empty()));
    Ok(match (classification, compiler) {
        (Some(classification), Some(compiler)) => Some(format!("{compiler}\n{classification}")),
        (classification, compiler) => classification.or_else(|| compiler.map(str::to_owned)),
    })
}

/// Turns the wire integer back into the name the rest of the system uses for that outcome.
fn outcome_name(document: &Value) -> Option<String> {
    let raw = document.get("outcome")?.as_i64()?;
    let raw = i32::try_from(raw).ok()?;
    AttemptOutcome::try_from(raw)
        .ok()
        .map(|outcome| outcome.as_str_name().to_owned())
}

fn read_json(
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
    digest: Sha256Digest,
    maximum: u64,
) -> Result<Value, CandidateRecordError> {
    let bytes = read_bytes(artifacts, digest, maximum)?;
    serde_json::from_slice(&bytes).map_err(|_| CandidateRecordError::Malformed {
        digest,
        expected: "JSON document",
    })
}

fn read_bytes(
    artifacts: &dyn alloyport_artifacts::ArtifactStore,
    digest: Sha256Digest,
    maximum: u64,
) -> Result<Vec<u8>, CandidateRecordError> {
    read_bounded(artifacts, digest, maximum)
        .map_err(|error| CandidateRecordError::Unreadable { digest, error })
}

#[derive(Debug)]
pub enum CandidateRecordError {
    Unreadable {
        digest: Sha256Digest,
        error: alloyport_core::ToolGatewayError,
    },
    Malformed {
        digest: Sha256Digest,
        expected: &'static str,
    },
    SourceIdentity {
        path: String,
        digest: Sha256Digest,
    },
    Stream(RecordStreamError),
    /// The destination already holds something. The record is rebuildable, never overwritten.
    Occupied(std::path::PathBuf),
    /// `git` could not be run at all. A record that cannot be built must say so, not print nothing.
    GitUnavailable(String),
    Git {
        operation: &'static str,
        message: String,
    },
    /// The imported repository disagrees with the manifests it was built from.
    Projection(String),
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
}

impl std::fmt::Display for CandidateRecordError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreadable { digest, error } => {
                write!(
                    formatter,
                    "the record cites {digest}, which cannot be read: {error}"
                )
            }
            Self::Malformed { digest, expected } => {
                write!(formatter, "{digest} is not the expected {expected}")
            }
            Self::SourceIdentity { path, digest } => write!(
                formatter,
                "the bytes stored for {path} do not match the manifest identity {digest}"
            ),
            Self::Stream(error) => std::fmt::Display::fmt(error, formatter),
            Self::Occupied(path) => write!(
                formatter,
                "{} already holds something; the record is rebuilt into an empty directory",
                path.display()
            ),
            Self::GitUnavailable(reason) => {
                write!(
                    formatter,
                    "the candidate record needs git, which cannot run: {reason}"
                )
            }
            Self::Git { operation, message } => write!(formatter, "{operation}: {message}"),
            Self::Projection(reason) => write!(
                formatter,
                "the written record disagrees with the manifests it projects: {reason}"
            ),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
        }
    }
}

impl std::error::Error for CandidateRecordError {}

impl From<RecordStreamError> for CandidateRecordError {
    fn from(error: RecordStreamError) -> Self {
        Self::Stream(error)
    }
}

#[cfg(test)]
#[path = "record_tests.rs"]
mod tests;
