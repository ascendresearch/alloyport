//! Read-only access to the vendored reference corpus, with each document's trust state attached.
//!
//! The runtime model writes Ascend C against an API it knows only from its weights. This gives it
//! the vendor's own documents — and gives it their trust state in the same breath, because the two
//! documents an optimization task reaches for first carry numbers validated on the previous
//! hardware generation. A corpus served without its ledger would present those as facts.

use crate::gateway::{adapter_error, ingest_bytes};
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    Sha256Digest, ToolEffectClass, ToolGatewayError, ToolGatewayOutcome, ToolInvocation,
    ToolOperationStatus, ToolResultAuthority,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const READ_REFERENCE_TOOL: &str = "read_reference";
const MAX_DOCUMENT_BYTES: u64 = 128 * 1024;
const MAX_LEDGER_BYTES: u64 = 4 * 1024 * 1024;
const CARD: &str = "SKILL.md";

pub(crate) const READ_REFERENCE_ARGUMENT_CONTRACT: &str =
    r#"{"document": "ops/ascendc-api-best-practices (omit to list the corpus)"}"#;

/// Decodes the arguments without any effect, so a malformed call can be returned to the model.
pub(crate) fn check_read_reference_arguments(raw: &[u8]) -> Result<(), serde_json::Error> {
    serde_json::from_slice::<ReadReferenceArguments>(raw).map(|_| ())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadReferenceArguments {
    #[serde(default)]
    pub(crate) document: Option<String>,
}

/// How far a document has been checked. Only a probe on our own hardware reaches `Validated`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceStatus {
    Unaudited,
    Reviewed,
    Validated,
    Refuted,
}

impl ReferenceStatus {
    /// What the model is told before it reads a word of the document.
    ///
    /// Silence means trusted. Anything not yet verified on this hardware says so, and says what
    /// remains usable anyway: a vendor document is authoritative about its own supported path long
    /// before any of its numbers have been reproduced here.
    const fn caution(self) -> Option<&'static str> {
        match self {
            Self::Validated => None,
            Self::Unaudited => Some(
                "UNAUDITED: nobody has checked this document. Follow its supported path; treat \
                 every number in it as a hypothesis, not a fact.",
            ),
            Self::Reviewed => Some(
                "REVIEWED, NOT VERIFIED: somebody read this and recorded a verdict, which is a \
                 claim rather than a measurement. No probe has run on this hardware. Follow its \
                 supported path; treat every number as a hypothesis.",
            ),
            Self::Refuted => Some(
                "REFUTED: a probe disproved a claim in this document. Do not act on it without \
                 reading the refutation.",
            ),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct LedgerRow {
    id: String,
    family: String,
    content_sha: String,
    status: ReferenceStatus,
    #[serde(default)]
    verdict: Option<String>,
    #[serde(default)]
    note: Option<String>,
}

/// The vendored corpus plus the ledger that says how far each document has been checked.
#[derive(Clone, Debug)]
pub struct ReferenceCorpus {
    root: PathBuf,
    documents: BTreeMap<String, LedgerRow>,
}

impl ReferenceCorpus {
    /// Loads a corpus root and its ledger.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is not a directory, the ledger is missing or malformed, or
    /// the ledger and the corpus disagree about which documents exist. A ledger that has drifted
    /// from the corpus cannot say what is trusted, so it is refused rather than half-applied.
    pub fn load(root: impl AsRef<Path>, ledger: impl AsRef<Path>) -> Result<Self, String> {
        let root = root.as_ref();
        if !root.is_dir() {
            return Err(format!(
                "reference corpus root {} is not a directory",
                root.display()
            ));
        }
        let metadata = std::fs::metadata(ledger.as_ref())
            .map_err(|error| format!("cannot read reference ledger: {error}"))?;
        if metadata.len() > MAX_LEDGER_BYTES {
            return Err("reference ledger exceeds its bound".to_owned());
        }
        let text = std::fs::read_to_string(ledger.as_ref())
            .map_err(|error| format!("cannot read reference ledger: {error}"))?;
        let mut documents = BTreeMap::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let row: LedgerRow = serde_json::from_str(line)
                .map_err(|error| format!("invalid reference ledger row: {error}"))?;
            if !root.join(&row.id).join(CARD).is_file() {
                return Err(format!(
                    "reference ledger names a missing document: {}",
                    row.id
                ));
            }
            if documents.insert(row.id.clone(), row).is_some() {
                return Err("reference ledger contains a duplicate document".to_owned());
            }
        }
        let mut found = 0_usize;
        for card in walk_cards(root) {
            found += 1;
            let id = card
                .strip_prefix(root)
                .map_err(|_| "reference document escaped the corpus root".to_owned())?
                .parent()
                .and_then(|parent| parent.to_str())
                .ok_or_else(|| "reference document has no readable path".to_owned())?
                .to_owned();
            if !documents.contains_key(&id) {
                // A document nobody can see the trust state of would be served as though it had
                // none, which is the same as serving it as trusted.
                return Err(format!(
                    "reference corpus contains an unlisted document: {id}"
                ));
            }
        }
        if found != documents.len() {
            return Err("reference ledger and corpus disagree on document count".to_owned());
        }
        Ok(Self {
            root: root.to_owned(),
            documents,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Whether the corpus holds this document, for a side-effect-free argument check.
    ///
    /// Naming a document that does not exist is a defect the model can see and correct — it
    /// misremembers an id, or infers one from a card that cites it — so it belongs in
    /// `validate_call`, not in an adapter error that ends the migration. A live run died on
    /// `ops/ascendc-register-invoke-template` when the corpus holds `ascendc-registry-invoke-template`.
    #[must_use]
    pub fn contains(&self, document: &str) -> bool {
        self.documents.contains_key(document)
    }

    /// Every document id, so a rejection can name what the model may actually ask for.
    #[must_use]
    pub fn document_ids(&self) -> Vec<&str> {
        self.documents.keys().map(String::as_str).collect()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    fn index(&self) -> ReferenceIndex {
        ReferenceIndex {
            documents: self
                .documents
                .values()
                .map(|row| ReferenceEntry {
                    document: row.id.clone(),
                    family: row.family.clone(),
                    status: row.status,
                    verdict: row.verdict.clone(),
                    note: row.note.clone(),
                })
                .collect(),
        }
    }

    fn read(&self, id: &str) -> Result<ReferenceDocument, String> {
        let row = self
            .documents
            .get(id)
            .ok_or_else(|| format!("no reference document named {id}"))?;
        // The id came from the ledger, not from the caller, so no caller-supplied path segment ever
        // reaches the filesystem.
        let path = self.root.join(&row.id).join(CARD);
        let bytes = std::fs::read(&path).map_err(|error| format!("cannot read {id}: {error}"))?;
        let sha = Sha256Digest::digest_bytes(&bytes).to_string();
        let total_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let kept = usize::try_from(MAX_DOCUMENT_BYTES).unwrap_or(usize::MAX);
        let truncated = bytes.len() > kept;
        let text = String::from_utf8_lossy(&bytes[..bytes.len().min(kept)]).into_owned();
        Ok(ReferenceDocument {
            document: row.id.clone(),
            family: row.family.clone(),
            status: row.status,
            verdict: row.verdict.clone(),
            note: row.note.clone(),
            caution: row.status.caution(),
            // A review is a claim about bytes. If the document has been edited since, the verdict
            // above describes something that no longer exists, and the reader is told so.
            verdict_matches_current_bytes: sha == row.content_sha,
            returned_bytes: u64::try_from(text.len()).unwrap_or(u64::MAX),
            total_bytes,
            truncated,
            text,
        })
    }
}

fn walk_cards(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == CARD) {
                found.push(path);
            }
        }
    }
    found
}

#[derive(Serialize)]
struct ReferenceEntry {
    document: String,
    family: String,
    status: ReferenceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    verdict: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

#[derive(Serialize)]
struct ReferenceIndex {
    documents: Vec<ReferenceEntry>,
}

#[derive(Serialize)]
struct ReferenceDocument {
    document: String,
    family: String,
    status: ReferenceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    verdict: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caution: Option<&'static str>,
    verdict_matches_current_bytes: bool,
    text: String,
    returned_bytes: u64,
    total_bytes: u64,
    truncated: bool,
}

/// Lists the corpus, or returns one document with its trust state attached.
pub(crate) fn read_reference(
    corpus: &ReferenceCorpus,
    artifacts: &dyn ArtifactStore,
    request: &ToolInvocation,
) -> Result<ToolGatewayOutcome, ToolGatewayError> {
    let arguments: ReadReferenceArguments = serde_json::from_slice(&request.call.raw_arguments)
        .map_err(|error| adapter_error(format!("invalid reference request: {error}")))?;
    let bytes = match arguments.document {
        None => serde_json::to_vec(&corpus.index()),
        Some(id) => serde_json::to_vec(&corpus.read(&id).map_err(adapter_error)?),
    }
    .map_err(|error| adapter_error(format!("cannot encode reference result: {error}")))?;
    let stored = ingest_bytes(artifacts, &bytes)?;
    Ok(ToolGatewayOutcome::Completed {
        status: ToolOperationStatus::Succeeded,
        result_digest: stored.digest,
        receipt_digests: Vec::new(),
        satisfies_subtask: false,
    })
}

/// Reference reading is an instrument: it grants no authority and cannot satisfy a subtask.
pub(crate) fn descriptor(name: &str) -> alloyport_core::RuntimeToolDescriptor {
    alloyport_core::RuntimeToolDescriptor {
        name: name.to_owned(),
        version: "1".to_owned(),
        effect_class: ToolEffectClass::ReadOnly,
        result_authority: ToolResultAuthority::Observed,
    }
}

#[cfg(test)]
#[path = "reference_tests.rs"]
mod tests;
