//! The candidate record as a deterministic `git fast-import` stream.
//!
//! [Design 0044](../../../docs/design/0044-git-as-the-candidate-record.md) adopts git as the
//! *record* of candidate lineage — not the store and not the model-facing interface. This module is
//! the whole projection: a pure function from recorded candidates to stream bytes, with no clock, no
//! filesystem, and no subprocess. Everything that could make the same history produce two different
//! repositories is therefore visible here rather than inherited from the host.
//!
//! Two things are pinned deliberately.
//!
//! **Identity and date.** A commit embeds an author, a committer, and two timestamps, so the same
//! tree yields a different commit id at a different second. 0044 rejected git as the store for
//! exactly this reason. The identity is a fixed non-address and the date is the submission's
//! sequence number in seconds after the epoch, so a replayed history is byte-identical and
//! `git log` shows submissions in the order they happened. A 1970 date is meant to be conspicuous:
//! it is a position, not a time, and the record holds no time because the Episode records none.
//!
//! **Paths are always C-quoted.** `BundlePath` forbids NUL, backslashes and traversal, but permits
//! spaces, quotes, and newlines — and the paths in a candidate are authored by the model. An
//! unquoted newline would end the `M` line early and the rest of the path would be read as a
//! command, so the model would be authoring stream commands. Quoting every path removes the class
//! rather than the instance.

use alloyport_core::{BundlePath, CandidateId, Sha256Digest};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// Fixed record author. `.invalid` is reserved by RFC 2606, so it cannot become a real address.
const RECORD_IDENTITY: &str = "AlloyPort candidate record <record@alloyport.invalid>";
/// Regular non-executable blob. Generated sources are text; nothing here is a mode bit decision.
const FILE_MODE: &str = "100644";
/// Wide enough for a real compiler line. The container path alone eats 46 bytes of
/// `/alloyport/bundle/generated/op_host/reduce_sum_launch.cpp:1:10: fatal error: acl/acl.h: No such
/// file or directory`, and the part worth reading is at the end, so a tighter budget cuts the answer
/// off and leaves the path.
const SUBJECT_DETAIL_BYTES: usize = 160;
const BODY_DETAIL_BYTES: usize = 4 * 1024;

/// One candidate exactly as the record projects it: its tree, its lineage, and what the gates said.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedCandidate {
    /// 1-based submission order within the Episode. Also the commit's author date in seconds.
    pub sequence: u32,
    pub candidate_id: CandidateId,
    pub parent_candidate_id: Option<CandidateId>,
    pub manifest_digest: Sha256Digest,
    pub source_bundle_digest: Sha256Digest,
    pub files: Vec<RecordedFile>,
    /// Gate outcomes in the order they were recorded, which is the order they ran.
    pub outcomes: Vec<RecordedGateOutcome>,
}

/// One generated file, with the digest the manifest binds it to beside the bytes it names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedFile {
    pub path: BundlePath,
    pub digest: Sha256Digest,
    pub bytes: Vec<u8>,
}

/// What one gate said about one candidate, read from the receipt the Episode recorded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedGateOutcome {
    pub gate: RecordedGate,
    /// The gate's own word for the outcome: `passed`, `failed`, `exit 1`, `pass`, `fail`.
    pub verdict: String,
    /// One line of why, when the receipt carries one. Never a verdict.
    pub detail: Option<String>,
    pub receipt_digest: Sha256Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordedGate {
    Source,
    AscendBuild,
    ReductionCorrectness,
}

impl RecordedGate {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Source => "source_gate",
            Self::AscendBuild => "ascend_build",
            Self::ReductionCorrectness => "correctness",
        }
    }
}

/// The ref one candidate is reachable by: a lightweight tag, sequence first so `git tag` sorts.
///
/// Every candidate gets its own ref rather than sharing a branch, because lineage forks whenever a
/// submission inherits from something other than the newest manifest and a shared branch would
/// either rewind or refuse. A tag rather than a branch for two reasons: it is immutable, which is
/// what a candidate is, and `git log --all --decorate` names it — refs outside the standard
/// namespaces are silently undecorated, so `refs/candidates/…` produced a history whose commits
/// could not be told apart.
#[must_use]
pub fn candidate_ref(candidate: &RecordedCandidate) -> String {
    // Only the identity's own alphanumerics reach a ref name. A `CandidateId` is validated as
    // non-empty and nothing more, so a `~` or a space in one would otherwise make git refuse the
    // whole import over a label.
    let mut short: Vec<char> = candidate
        .candidate_id
        .as_str()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .rev()
        .take(12)
        .collect();
    short.reverse();
    let short: String = short.into_iter().collect();
    if short.is_empty() {
        return format!("refs/tags/c{:03}", candidate.sequence);
    }
    format!("refs/tags/c{:03}-{short}", candidate.sequence)
}

/// The branch a reader lands on: the last candidate the Episode submitted.
pub const RECORD_BRANCH: &str = "refs/heads/main";

/// Renders the complete import stream for one task's candidates, in submission order.
///
/// # Errors
///
/// Returns an error when a candidate has no files, since a commit with an empty tree would record a
/// candidate that cannot exist, or when two candidates claim the same sequence.
pub fn fast_import_stream(candidates: &[RecordedCandidate]) -> Result<Vec<u8>, RecordStreamError> {
    if candidates.is_empty() {
        return Err(RecordStreamError::NoCandidates);
    }
    let mut stream: Vec<u8> = Vec::new();
    let mut next_mark: u32 = 0;
    let mut blob_marks: BTreeMap<Sha256Digest, u32> = BTreeMap::new();
    let mut commit_marks: BTreeMap<CandidateId, u32> = BTreeMap::new();
    let mut seen_sequences: BTreeSet<u32> = BTreeSet::new();

    for candidate in candidates {
        if candidate.files.is_empty() {
            return Err(RecordStreamError::EmptyCandidate(
                candidate.candidate_id.clone(),
            ));
        }
        if !seen_sequences.insert(candidate.sequence) {
            return Err(RecordStreamError::DuplicateSequence(candidate.sequence));
        }
        // One blob per distinct content, which is the CAS mapping 0044 names. A correction that
        // changes one file re-uses every inherited blob, exactly as the manifest re-uses artifacts.
        let mut files: Vec<&RecordedFile> = candidate.files.iter().collect();
        files.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));
        for file in &files {
            if blob_marks.contains_key(&file.digest) {
                continue;
            }
            next_mark += 1;
            blob_marks.insert(file.digest, next_mark);
            stream.extend_from_slice(format!("blob\nmark :{next_mark}\n").as_bytes());
            append_data(&mut stream, &file.bytes);
        }

        next_mark += 1;
        let commit_mark = next_mark;
        commit_marks.insert(candidate.candidate_id.clone(), commit_mark);
        let parent_mark = candidate
            .parent_candidate_id
            .as_ref()
            .and_then(|parent| commit_marks.get(parent).copied());
        let reference = candidate_ref(candidate);
        stream.extend_from_slice(format!("commit {reference}\nmark :{commit_mark}\n").as_bytes());
        let when = format!("{} +0000", candidate.sequence);
        stream.extend_from_slice(format!("author {RECORD_IDENTITY} {when}\n").as_bytes());
        stream.extend_from_slice(format!("committer {RECORD_IDENTITY} {when}\n").as_bytes());
        append_data(
            &mut stream,
            commit_message(candidate, &files, parent_mark.is_some()).as_bytes(),
        );
        if let Some(mark) = parent_mark {
            stream.extend_from_slice(format!("from :{mark}\n").as_bytes());
        }
        // The tree is exactly this candidate, never the previous one plus a change. Inheritance is
        // already resolved in the manifest; carrying it again here could record a file the candidate
        // does not contain.
        stream.extend_from_slice(b"deleteall\n");
        for file in &files {
            let mark = blob_marks
                .get(&file.digest)
                .copied()
                .ok_or(RecordStreamError::UnmarkedBlob(file.digest))?;
            stream.extend_from_slice(
                format!("M {FILE_MODE} :{mark} {}\n", quote_path(file.path.as_str())).as_bytes(),
            );
        }
        stream.extend_from_slice(b"\n");
    }

    let last = commit_marks
        .get(&candidates[candidates.len() - 1].candidate_id)
        .copied()
        .ok_or(RecordStreamError::NoCandidates)?;
    stream.extend_from_slice(format!("reset {RECORD_BRANCH}\nfrom :{last}\n\n").as_bytes());
    stream.extend_from_slice(b"done\n");
    Ok(stream)
}

/// Writes one length-prefixed payload. The count is bytes, so no payload needs escaping.
fn append_data(stream: &mut Vec<u8>, payload: &[u8]) {
    stream.extend_from_slice(format!("data {}\n", payload.len()).as_bytes());
    stream.extend_from_slice(payload);
    stream.extend_from_slice(b"\n");
}

/// C-quotes a path the way `git` unquotes it, escaping per byte rather than per character.
fn quote_path(path: &str) -> String {
    let mut quoted = String::with_capacity(path.len() + 2);
    quoted.push('"');
    for byte in path.as_bytes() {
        match byte {
            b'"' => quoted.push_str("\\\""),
            b'\\' => quoted.push_str("\\\\"),
            b'\n' => quoted.push_str("\\n"),
            b'\r' => quoted.push_str("\\r"),
            b'\t' => quoted.push_str("\\t"),
            0x20..=0x7e => quoted.push(char::from(*byte)),
            other => {
                let _ = write!(quoted, "\\{other:03o}");
            }
        }
    }
    quoted.push('"');
    quoted
}

/// Builds the message: a subject that answers "what happened to this candidate" in one line.
fn commit_message(
    candidate: &RecordedCandidate,
    files: &[&RecordedFile],
    parent_recorded: bool,
) -> String {
    let mut message = format!(
        "c{:03} {}\n\n",
        candidate.sequence,
        subject_outcome(candidate)
    );
    let _ = writeln!(message, "candidate: {}", candidate.candidate_id.as_str());
    let _ = writeln!(message, "manifest:  {}", candidate.manifest_digest);
    let _ = writeln!(message, "bundle:    {}", candidate.source_bundle_digest);
    match candidate.parent_candidate_id.as_ref() {
        None => {
            let _ = writeln!(message, "parent:    none");
        }
        Some(parent) if parent_recorded => {
            let _ = writeln!(message, "parent:    {}", parent.as_str());
        }
        Some(parent) => {
            // Naming it and saying it is absent is the honest form. A silent root commit would
            // claim this candidate had no parent, which the manifest contradicts.
            let _ = writeln!(
                message,
                "parent:    {} (not recorded in this Episode)",
                parent.as_str()
            );
        }
    }
    let _ = writeln!(message, "files:     {}", files.len());
    if candidate.outcomes.is_empty() {
        let _ = writeln!(message, "\nno gate ran on this candidate");
    }
    for outcome in &candidate.outcomes {
        let _ = writeln!(
            message,
            "\n{} {}\n  receipt {}",
            outcome.gate.label(),
            outcome.verdict,
            outcome.receipt_digest
        );
        if let Some(detail) = outcome.detail.as_deref() {
            for line in clamp(detail, BODY_DETAIL_BYTES).lines() {
                let _ = writeln!(message, "  {line}");
            }
        }
    }
    message.push_str(
        "\nThis commit is a projection of the candidate manifest, not evidence. The manifest is \
         authoritative\nfor identity and every gate; nothing in the trust path reads this \
         repository. The author date is\nthe submission's sequence number, not a time.\n",
    );
    message
}

/// The one line `git log --oneline` shows. Ordered so the newest gate a candidate reached is last.
fn subject_outcome(candidate: &RecordedCandidate) -> String {
    if candidate.outcomes.is_empty() {
        return "submitted; no gate ran".to_owned();
    }
    candidate
        .outcomes
        .iter()
        .map(|outcome| {
            let head = format!("{} {}", outcome.gate.label(), outcome.verdict);
            match outcome.detail.as_deref().map(first_line) {
                Some(detail) if !detail.is_empty() => {
                    let shown = clamp(detail, SUBJECT_DETAIL_BYTES);
                    // A cut line says so. The full text is in the body either way, but a subject
                    // that ends mid-sentence without a mark reads as the whole message.
                    let elision = if shown.len() < detail.len() {
                        "…"
                    } else {
                        ""
                    };
                    format!("{head}: {shown}{elision}")
                }
                _ => head,
            }
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or("").trim()
}

/// Truncates on a character boundary so a clamped detail is still valid UTF-8.
fn clamp(text: &str, maximum: usize) -> &str {
    if text.len() <= maximum {
        return text;
    }
    let mut end = maximum;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[derive(Debug, Eq, PartialEq)]
pub enum RecordStreamError {
    NoCandidates,
    EmptyCandidate(CandidateId),
    DuplicateSequence(u32),
    UnmarkedBlob(Sha256Digest),
}

impl std::fmt::Display for RecordStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCandidates => write!(formatter, "no candidate was recorded for this task"),
            Self::EmptyCandidate(candidate) => {
                write!(formatter, "candidate {candidate} has no files")
            }
            Self::DuplicateSequence(sequence) => {
                write!(formatter, "two candidates claim sequence {sequence}")
            }
            Self::UnmarkedBlob(digest) => {
                write!(formatter, "no blob was written for {digest}")
            }
        }
    }
}

impl std::error::Error for RecordStreamError {}
