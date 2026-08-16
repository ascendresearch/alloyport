//! Admission for evidence-backed knowledge: what an entry must cite before anything may reuse it.
//!
//! [Design 0008](../../../docs/design/0008-evidence-backed-knowledge-lifecycle.md) describes the
//! full lifecycle. This is its load-bearing core and nothing more, because no migration has
//! completed yet and a store built ahead of the first entry would be shelving for an empty room.
//!
//! The one property that cannot be added later is that **an entry is never promoted on its own
//! say-so**. Everything else — retrieval, staleness, dashboards — is elaboration on top of a gate
//! that reads the record. So this decides admission and leaves the rest unbuilt and unclaimed.
//!
//! Four rules carry the weight:
//!
//! - **A claim that cannot be traced to a receipt is a belief.** An entry with no citation stays
//!   proposed however confident its prose.
//! - **A citation is checked, not counted.** The gate resolves each one and reads what the receipt
//!   actually says. A field the author fills in is a claim; the strictest-sounding schema field is
//!   the one that ends up emptiest.
//! - **Negative knowledge has its own evidence shape.** A failed route is supported by a receipt
//!   that failed. A gate that only accepts passing receipts makes the most valuable kind of entry
//!   unfilable, and an honest path that does not exist gets routed around.
//! - **A retraction is a claim.** It carries evidence of the same weight as the promotion it kills,
//!   or it is a delete button that a budget-pressured agent will use on whatever is in the way.

use crate::{CandidateId, CorrectnessVerdict, PerformanceVerdict, Sha256Digest, TaskId};
use serde::{Deserialize, Serialize};

pub const KNOWLEDGE_ENTRY_SCHEMA_V1: u16 = 1;

/// What an entry is about. The kinds differ in what evidence can support them.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeKind {
    /// A source change that carried a candidate through the gates.
    Transformation,
    /// An observed, scoped property of the platform or toolchain.
    Fact,
    /// A route that was tried and did not work, with what would justify trying it again.
    FailedRoute,
}

/// Where an entry applies. Retrieval filters on this before anything else.
///
/// An entry without a scope is a claim about everything, which is a claim about nothing. The
/// sibling project audited its own facts and found that not one of forty-five could say what it had
/// been measured against.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeScope {
    pub soc: String,
    pub cann: String,
    pub operator_family: String,
}

impl KnowledgeScope {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        [&self.soc, &self.cann, &self.operator_family]
            .into_iter()
            .all(|field| !field.trim().is_empty())
    }
}

/// A pointer to a receipt this entry rests on.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "receipt", content = "digest")]
pub enum Citation {
    Correctness(Sha256Digest),
    Performance(Sha256Digest),
    Build(Sha256Digest),
}

impl Citation {
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        match self {
            Self::Correctness(digest) | Self::Performance(digest) | Self::Build(digest) => *digest,
        }
    }
}

/// What a citation resolved to. Produced by whoever can open artifacts, never by the author.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedCitation {
    /// The digest named nothing, or named something that would not parse.
    Unresolved(Sha256Digest),
    Correctness {
        digest: Sha256Digest,
        task_id: TaskId,
        candidate_id: CandidateId,
        verdict: CorrectnessVerdict,
    },
    Performance {
        digest: Sha256Digest,
        verdict: PerformanceVerdict,
    },
    Build {
        digest: Sha256Digest,
        task_id: TaskId,
        candidate_id: CandidateId,
        passed: bool,
    },
}

impl ResolvedCitation {
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        match self {
            Self::Unresolved(digest)
            | Self::Correctness { digest, .. }
            | Self::Performance { digest, .. }
            | Self::Build { digest, .. } => *digest,
        }
    }

    /// Whether this receipt reports the outcome succeeding.
    const fn succeeded(&self) -> Option<bool> {
        match self {
            Self::Unresolved(_) => None,
            Self::Correctness { verdict, .. } => Some(matches!(verdict, CorrectnessVerdict::Pass)),
            Self::Performance { verdict, .. } => {
                Some(matches!(verdict, PerformanceVerdict::Improved))
            }
            Self::Build { passed, .. } => Some(*passed),
        }
    }

    fn task_id(&self) -> Option<&TaskId> {
        match self {
            Self::Correctness { task_id, .. } | Self::Build { task_id, .. } => Some(task_id),
            Self::Unresolved(_) | Self::Performance { .. } => None,
        }
    }
}

/// How far an entry has been checked.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeStatus {
    /// Recorded and immediately retrievable, with a caution. Nobody has checked it.
    ///
    /// It enters usable on purpose: a gate that cannot be satisfied until after the knowledge has
    /// been used cannot be satisfied at all, and a gate like that gets bypassed rather than met.
    Proposed,
    /// Its citations resolve and report what it claims.
    Supported,
    /// Later evidence contradicts it. Still retrievable, as a warning, never as procedure.
    Contested,
    /// Adjudicated removal, kept with its reason and its own evidence.
    Retracted,
}

/// Why an entry could not be supported.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionRefusal {
    NoCitations,
    InvalidScope,
    UnresolvedCitation,
    /// The receipt reports an outcome the entry's kind cannot rest on.
    EvidenceContradictsKind,
    /// The receipt belongs to a different task than the entry claims.
    ForeignEvidence,
    /// A retraction with no evidence of its own is a delete button.
    UnevidencedRetraction,
}

/// An entry as proposed. Status is decided by [`admit`], never by the author.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeEntry {
    pub schema_version: u16,
    pub id: String,
    pub kind: KnowledgeKind,
    pub scope: KnowledgeScope,
    pub task_id: TaskId,
    pub claim: String,
    pub citations: Vec<Citation>,
    /// Present only for a proposed retraction, and adjudicated like any other claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retracts: Option<Retraction>,
}

/// A proposed removal, which is itself a claim needing evidence.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Retraction {
    pub entry_id: String,
    pub reason: String,
    pub citations: Vec<Citation>,
}

/// The gate's verdict on one entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Admission {
    pub status: KnowledgeStatus,
    pub refusals: Vec<AdmissionRefusal>,
}

/// Decides an entry's status from resolved citations rather than from the entry.
///
/// `resolved` must come from whoever can open artifacts. Passing the author's own description of
/// its evidence would restore exactly the hole this exists to close.
#[must_use]
pub fn admit(entry: &KnowledgeEntry, resolved: &[ResolvedCitation]) -> Admission {
    let mut refusals = Vec::new();
    if !entry.scope.is_valid() || entry.claim.trim().is_empty() || entry.id.trim().is_empty() {
        refusals.push(AdmissionRefusal::InvalidScope);
    }

    if let Some(retraction) = &entry.retracts {
        // A retraction kills something another run is relying on, so it needs evidence of the same
        // weight as the promotion it reverses.
        if retraction.citations.is_empty() || retraction.reason.trim().is_empty() {
            refusals.push(AdmissionRefusal::UnevidencedRetraction);
        }
        for citation in &retraction.citations {
            check_citation(
                entry,
                citation,
                resolved,
                KnowledgeKind::FailedRoute,
                &mut refusals,
            );
        }
        refusals.sort_unstable();
        refusals.dedup();
        return Admission {
            status: if refusals.is_empty() {
                KnowledgeStatus::Retracted
            } else {
                KnowledgeStatus::Proposed
            },
            refusals,
        };
    }

    if entry.citations.is_empty() {
        refusals.push(AdmissionRefusal::NoCitations);
    }
    for citation in &entry.citations {
        check_citation(entry, citation, resolved, entry.kind, &mut refusals);
    }

    refusals.sort_unstable();
    refusals.dedup();
    Admission {
        status: if refusals.is_empty() {
            KnowledgeStatus::Supported
        } else {
            KnowledgeStatus::Proposed
        },
        refusals,
    }
}

fn check_citation(
    entry: &KnowledgeEntry,
    citation: &Citation,
    resolved: &[ResolvedCitation],
    kind: KnowledgeKind,
    refusals: &mut Vec<AdmissionRefusal>,
) {
    let Some(found) = resolved
        .iter()
        .find(|candidate| candidate.digest() == citation.digest())
    else {
        refusals.push(AdmissionRefusal::UnresolvedCitation);
        return;
    };
    let Some(succeeded) = found.succeeded() else {
        refusals.push(AdmissionRefusal::UnresolvedCitation);
        return;
    };
    if found
        .task_id()
        .is_some_and(|task_id| task_id != &entry.task_id)
    {
        refusals.push(AdmissionRefusal::ForeignEvidence);
    }
    // A transformation or a fact rests on something that worked; a failed route rests on something
    // that did not. Requiring a passing receipt for both would leave the most valuable kind of
    // entry with no honest way in.
    let expected = !matches!(kind, KnowledgeKind::FailedRoute);
    if succeeded != expected {
        refusals.push(AdmissionRefusal::EvidenceContradictsKind);
    }
}

/// Re-runs the gate over entries already stored, and reports those whose status it no longer grants.
///
/// A gate only ever sees what arrives; it says nothing about what is already inside. Every time the
/// sibling project ran one of these backwards it found something, and the worst findings were its
/// own entries.
#[must_use]
pub fn audit<'a>(
    stored: impl IntoIterator<Item = (&'a KnowledgeEntry, KnowledgeStatus, &'a [ResolvedCitation])>,
) -> Vec<AuditFinding> {
    stored
        .into_iter()
        .filter_map(|(entry, recorded, resolved)| {
            let admission = admit(entry, resolved);
            (admission.status != recorded).then(|| AuditFinding {
                id: entry.id.clone(),
                recorded,
                granted: admission.status,
                refusals: admission.refusals,
            })
        })
        .collect()
}

/// One stored entry whose recorded status the gate would not grant today.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditFinding {
    pub id: String,
    pub recorded: KnowledgeStatus,
    pub granted: KnowledgeStatus,
    pub refusals: Vec<AdmissionRefusal>,
}

#[cfg(test)]
#[path = "knowledge_tests.rs"]
mod tests;
