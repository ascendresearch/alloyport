use super::*;

fn corpus() -> ReferenceCorpus {
    ReferenceCorpus::load(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../vendor/cannbot-skills"),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../vendor/cannbot-skills-audit.jsonl"
        ),
    )
    .expect("vendored corpus")
}

#[test]
fn every_vendored_document_is_listed_with_a_trust_state() {
    let corpus = corpus();
    assert_eq!(corpus.len(), 127);
    let index = corpus.index();
    assert_eq!(index.documents.len(), 127);
    assert!(
        index
            .documents
            .iter()
            .all(|entry| entry.status != ReferenceStatus::Validated),
        "no probe has run on this hardware, so nothing may claim validation"
    );
}

#[test]
fn the_documents_an_optimization_task_reaches_for_first_arrive_with_their_caveats() {
    let corpus = corpus();
    for id in [
        "ops/ascendc-perf-optimize",
        "ops/ascendc-performance-best-practices",
    ] {
        let document = corpus.read(id).expect("perf document");
        assert_eq!(document.status, ReferenceStatus::Reviewed);
        assert_eq!(document.verdict.as_deref(), Some("suspect"));
        assert!(
            document
                .caution
                .is_some_and(|caution| caution.contains("hypothesis")),
            "a document whose numbers were validated on other hardware must not read as fact"
        );
        assert!(document.verdict_matches_current_bytes);
        assert!(!document.note.as_deref().unwrap_or_default().is_empty());
    }
}

#[test]
fn a_document_the_ledger_does_not_cover_cannot_be_read() {
    let corpus = corpus();
    assert!(corpus.read("ops/../../etc/passwd").is_err());
    assert!(corpus.read("ops/not-a-real-skill").is_err());
}

#[test]
fn an_edited_document_no_longer_carries_its_verdict() {
    let root = tempfile::tempdir().expect("root");
    let skill = root.path().join("ops/example");
    std::fs::create_dir_all(&skill).expect("skill dir");
    std::fs::write(skill.join("SKILL.md"), "edited after review").expect("card");
    let ledger = root.path().join("audit.jsonl");
    std::fs::write(
        &ledger,
        concat!(
            r#"{"id":"ops/example","family":"ops","content_sha":"sha256:0000000000000000000"#,
            r#"000000000000000000000000000000000000000000000","status":"reviewed",
"#,
        )
        .replace(
            ",\n",
            r#","verdict":"authoritative","note":"recorded against other bytes"}
"#,
        ),
    )
    .expect("ledger");
    let corpus = ReferenceCorpus::load(root.path(), &ledger).expect("corpus");
    let document = corpus.read("ops/example").expect("document");
    assert!(
        !document.verdict_matches_current_bytes,
        "a review is a claim about bytes; editing them must retire it"
    );
}

#[test]
fn a_corpus_the_ledger_does_not_cover_is_refused_rather_than_half_applied() {
    let root = tempfile::tempdir().expect("root");
    let skill = root.path().join("ops/unlisted");
    std::fs::create_dir_all(&skill).expect("skill dir");
    std::fs::write(skill.join("SKILL.md"), "body").expect("card");
    let ledger = root.path().join("audit.jsonl");
    std::fs::write(&ledger, "").expect("ledger");
    let error = ReferenceCorpus::load(root.path(), &ledger).expect_err("must refuse");
    assert!(error.contains("unlisted document"));
}
