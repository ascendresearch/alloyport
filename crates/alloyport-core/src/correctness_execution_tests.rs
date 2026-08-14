use super::*;
use crate::{REDUCTION_CORPUS_REVISION_V1, ReductionTolerancePlan, TaskId};

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn experiment(
    candidate_id: CandidateId,
    corpus: &ReductionCorpus,
) -> ReductionCorrectnessExperiment {
    ReductionCorrectnessExperiment::new(
        TaskId::try_from("task-reduction-execution").expect("task ID"),
        candidate_id,
        digest("spec"),
        digest("manifest"),
        digest("source"),
        digest("build"),
        corpus.digest().expect("corpus digest"),
        ReductionTolerancePlan::fixture_v1()
            .digest()
            .expect("tolerance plan digest"),
    )
}

#[test]
fn role_bundles_keep_reference_and_candidate_source_roots_separate() {
    let corpus = ReductionCorpus::fixture_v1();
    assert_eq!(corpus.cases().len(), 24);
    let candidate_id = CandidateId::try_from("candidate-reduction-execution").expect("ID");
    let experiment = experiment(candidate_id.clone(), &corpus);
    let reference = ReductionExecutionBundle::new(
        experiment.clone(),
        ReductionRunRole::CudaReference,
        corpus.clone(),
        vec![
            ReductionExecutionFile::new(
                BundlePath::try_from("input/reduce.cu").expect("path"),
                "cuda source",
            )
            .expect("file"),
        ],
    )
    .expect("reference bundle");
    let candidate = ReductionExecutionBundle::new(
        experiment,
        ReductionRunRole::AscendCandidate,
        corpus,
        vec![
            ReductionExecutionFile::new(
                BundlePath::try_from("generated/reduce.cpp").expect("path"),
                "ascend source",
            )
            .expect("file"),
        ],
    )
    .expect("candidate bundle");
    assert_eq!(reference.role(), ReductionRunRole::CudaReference);
    assert_eq!(candidate.role(), ReductionRunRole::AscendCandidate);
    assert_ne!(
        reference.implementation_digest(),
        candidate.implementation_digest()
    );
    assert_eq!(
        serde_json::from_slice::<ReductionExecutionBundle>(
            &serde_json::to_vec(&candidate).expect("serialize")
        )
        .expect("round trip"),
        candidate
    );
}

#[test]
fn bundle_rejects_cross_role_paths_and_authored_identity() {
    let corpus = ReductionCorpus::fixture_v1();
    let experiment = experiment(
        CandidateId::try_from("candidate-reduction-execution").expect("ID"),
        &corpus,
    );
    assert!(
        ReductionExecutionBundle::new(
            experiment.clone(),
            ReductionRunRole::CudaReference,
            corpus.clone(),
            vec![
                ReductionExecutionFile::new(
                    BundlePath::try_from("generated/reduce.cpp").expect("path"),
                    "wrong side",
                )
                .expect("file")
            ],
        )
        .is_err()
    );
    let bundle = ReductionExecutionBundle::new(
        experiment,
        ReductionRunRole::AscendCandidate,
        corpus,
        vec![
            ReductionExecutionFile::new(
                BundlePath::try_from("generated/reduce.cpp").expect("path"),
                "ascend source",
            )
            .expect("file"),
        ],
    )
    .expect("bundle");
    let mut value = serde_json::to_value(bundle).expect("serialize");
    value["implementation_digest"] = serde_json::to_value(digest("forged")).expect("digest");
    assert!(serde_json::from_value::<ReductionExecutionBundle>(value).is_err());
    assert_eq!(REDUCTION_CORPUS_REVISION_V1, "cuda-reduction-corpus-v1");
}
