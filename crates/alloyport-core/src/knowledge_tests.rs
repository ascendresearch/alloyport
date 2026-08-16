use super::*;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn task() -> TaskId {
    TaskId::try_from("task-knowledge").expect("task")
}

fn candidate() -> CandidateId {
    CandidateId::try_from("candidate-knowledge").expect("candidate")
}

fn scope() -> KnowledgeScope {
    KnowledgeScope {
        soc: "Ascend950PR".to_owned(),
        cann: "9.1.0-beta.1".to_owned(),
        operator_family: "reduction".to_owned(),
    }
}

fn entry(kind: KnowledgeKind, citations: Vec<Citation>) -> KnowledgeEntry {
    KnowledgeEntry {
        schema_version: KNOWLEDGE_ENTRY_SCHEMA_V1,
        id: "entry-1".to_owned(),
        kind,
        scope: scope(),
        task_id: task(),
        claim: "a block reduction over UB tiles carried the candidate through Correctness"
            .to_owned(),
        citations,
        retracts: None,
    }
}

fn passing_correctness(label: &str) -> ResolvedCitation {
    ResolvedCitation::Correctness {
        digest: digest(label),
        task_id: task(),
        candidate_id: candidate(),
        verdict: CorrectnessVerdict::Pass,
    }
}

fn failing_correctness(label: &str) -> ResolvedCitation {
    ResolvedCitation::Correctness {
        digest: digest(label),
        task_id: task(),
        candidate_id: candidate(),
        verdict: CorrectnessVerdict::Fail,
    }
}

#[test]
fn a_claim_that_cites_nothing_stays_proposed_however_confident_it_sounds() {
    let admission = admit(&entry(KnowledgeKind::Transformation, Vec::new()), &[]);
    assert_eq!(admission.status, KnowledgeStatus::Proposed);
    assert!(admission.refusals.contains(&AdmissionRefusal::NoCitations));
}

#[test]
fn a_citation_is_resolved_and_read_rather_than_counted() {
    let cited = Citation::Correctness(digest("receipt"));
    // Named but never published: the strictest-sounding field is the one that ends up emptiest.
    let dangling = admit(&entry(KnowledgeKind::Transformation, vec![cited]), &[]);
    assert_eq!(dangling.status, KnowledgeStatus::Proposed);
    assert!(
        dangling
            .refusals
            .contains(&AdmissionRefusal::UnresolvedCitation)
    );

    let supported = admit(
        &entry(KnowledgeKind::Transformation, vec![cited]),
        &[passing_correctness("receipt")],
    );
    assert_eq!(supported.status, KnowledgeStatus::Supported);
    assert!(supported.refusals.is_empty());
}

#[test]
fn a_transformation_cannot_rest_on_a_receipt_that_failed() {
    let admission = admit(
        &entry(
            KnowledgeKind::Transformation,
            vec![Citation::Correctness(digest("receipt"))],
        ),
        &[failing_correctness("receipt")],
    );
    assert_eq!(admission.status, KnowledgeStatus::Proposed);
    assert!(
        admission
            .refusals
            .contains(&AdmissionRefusal::EvidenceContradictsKind)
    );
}

#[test]
fn negative_knowledge_is_supported_by_the_receipt_that_failed() {
    // The most valuable entry a run can leave is where not to go. A gate that only accepts passing
    // receipts leaves it with no honest way in, and a gate with no honest path gets routed around.
    let failed_route = admit(
        &entry(
            KnowledgeKind::FailedRoute,
            vec![Citation::Correctness(digest("receipt"))],
        ),
        &[failing_correctness("receipt")],
    );
    assert_eq!(failed_route.status, KnowledgeStatus::Supported);
    assert!(failed_route.refusals.is_empty());

    // And the reverse: a route cannot be recorded as failed by citing a run that passed.
    let contradicted = admit(
        &entry(
            KnowledgeKind::FailedRoute,
            vec![Citation::Correctness(digest("receipt"))],
        ),
        &[passing_correctness("receipt")],
    );
    assert_eq!(contradicted.status, KnowledgeStatus::Proposed);
}

#[test]
fn evidence_from_another_task_supports_nothing() {
    let admission = admit(
        &entry(
            KnowledgeKind::Transformation,
            vec![Citation::Correctness(digest("receipt"))],
        ),
        &[ResolvedCitation::Correctness {
            digest: digest("receipt"),
            task_id: TaskId::try_from("task-somebody-else").expect("task"),
            candidate_id: candidate(),
            verdict: CorrectnessVerdict::Pass,
        }],
    );
    assert_eq!(admission.status, KnowledgeStatus::Proposed);
    assert!(
        admission
            .refusals
            .contains(&AdmissionRefusal::ForeignEvidence)
    );
}

#[test]
fn an_entry_without_a_scope_is_a_claim_about_everything() {
    let mut unscoped = entry(
        KnowledgeKind::Fact,
        vec![Citation::Correctness(digest("receipt"))],
    );
    unscoped.scope.soc = "  ".to_owned();
    let admission = admit(&unscoped, &[passing_correctness("receipt")]);
    assert_eq!(admission.status, KnowledgeStatus::Proposed);
    assert!(admission.refusals.contains(&AdmissionRefusal::InvalidScope));
}

#[test]
fn a_retraction_with_no_evidence_is_a_delete_button() {
    let mut bare = entry(KnowledgeKind::FailedRoute, Vec::new());
    bare.retracts = Some(Retraction {
        entry_id: "entry-1".to_owned(),
        reason: "it is in my way".to_owned(),
        citations: Vec::new(),
    });
    let admission = admit(&bare, &[]);
    assert_eq!(admission.status, KnowledgeStatus::Proposed);
    assert!(
        admission
            .refusals
            .contains(&AdmissionRefusal::UnevidencedRetraction)
    );

    let mut evidenced = entry(KnowledgeKind::FailedRoute, Vec::new());
    evidenced.retracts = Some(Retraction {
        entry_id: "entry-1".to_owned(),
        reason: "the transformation it recorded now fails Correctness".to_owned(),
        citations: vec![Citation::Correctness(digest("later"))],
    });
    let admission = admit(&evidenced, &[failing_correctness("later")]);
    assert_eq!(admission.status, KnowledgeStatus::Retracted);
    assert!(admission.refusals.is_empty());
}

#[test]
fn a_proxy_backed_speedup_never_reaches_supported() {
    // The performance module already refuses a proxy comparison. What matters here is that the
    // knowledge gate reads the receipt's verdict rather than the entry's prose about it.
    let claim = entry(
        KnowledgeKind::Fact,
        vec![Citation::Performance(digest("perf"))],
    );
    let unverifiable = admit(
        &claim,
        &[ResolvedCitation::Performance {
            digest: digest("perf"),
            verdict: PerformanceVerdict::Unverifiable,
        }],
    );
    assert_eq!(unverifiable.status, KnowledgeStatus::Proposed);

    let no_result = admit(
        &claim,
        &[ResolvedCitation::Performance {
            digest: digest("perf"),
            verdict: PerformanceVerdict::NoResult,
        }],
    );
    assert_eq!(
        no_result.status,
        KnowledgeStatus::Proposed,
        "an effect inside the noise supports no fact about speed"
    );
}

#[test]
fn the_gate_run_backwards_reports_what_it_would_no_longer_grant() {
    // A gate only ever sees what arrives. This is the only thing that looks at what is already in.
    let good = entry(
        KnowledgeKind::Transformation,
        vec![Citation::Correctness(digest("kept"))],
    );
    let mut stale = entry(
        KnowledgeKind::Transformation,
        vec![Citation::Correctness(digest("gone"))],
    );
    stale.id = "entry-2".to_owned();
    let kept = [passing_correctness("kept")];
    let findings = audit([
        (&good, KnowledgeStatus::Supported, kept.as_slice()),
        (&stale, KnowledgeStatus::Supported, [].as_slice()),
    ]);
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].id, "entry-2");
    assert_eq!(findings[0].recorded, KnowledgeStatus::Supported);
    assert_eq!(findings[0].granted, KnowledgeStatus::Proposed);
    assert!(
        findings[0]
            .refusals
            .contains(&AdmissionRefusal::UnresolvedCitation)
    );
}
