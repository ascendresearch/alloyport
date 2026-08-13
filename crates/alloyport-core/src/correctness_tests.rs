use super::*;
use crate::ReductionCaseKind;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn observations(corpus: &ReductionCorpus) -> Vec<ReductionObservation> {
    corpus
        .cases()
        .iter()
        .map(|case| {
            let (status, output) = match case.kind {
                ReductionCaseKind::Valid => (
                    0,
                    Some(if case.elements == 0 {
                        0.0
                    } else {
                        f32::from(u16::try_from(case.elements % 997).expect("bounded remainder"))
                            + f32::from(u16::try_from(case.seed % 97).expect("bounded seed"))
                                / 1_000.0
                    }),
                ),
                ReductionCaseKind::NullInput | ReductionCaseKind::NullOutput => (1, None),
                ReductionCaseKind::UnsupportedSize => (3, None),
            };
            ReductionObservation {
                case_id: case.case_id.clone(),
                repetition: case.repetition,
                elements: case.elements,
                input_digest: case.input_digest(),
                status,
                output_bits: output.map(f32::to_bits),
            }
        })
        .collect()
}

fn experiment(
    candidate_id: CandidateId,
    corpus: &ReductionCorpus,
) -> ReductionCorrectnessExperiment {
    let policy = ReductionOraclePolicy::fixture_v1();
    ReductionCorrectnessExperiment::new(
        TaskId::try_from("task-reduction-correctness").expect("task ID"),
        candidate_id,
        digest("migration-spec"),
        digest("manifest"),
        digest("source-gate"),
        digest("build-gate"),
        corpus.digest().expect("corpus digest"),
        policy.digest().expect("policy digest"),
    )
}

fn runs() -> (
    ReductionCorrectnessExperiment,
    ReductionRunReceipt,
    ReductionRunReceipt,
    ReductionCorpus,
) {
    let corpus = ReductionCorpus::fixture_v1();
    let candidate_id = CandidateId::try_from("candidate-reduction-correctness").expect("ID");
    let experiment = experiment(candidate_id.clone(), &corpus);
    let reference = ReductionRunReceipt::new(
        experiment.experiment_digest(),
        ReductionRunRole::CudaReference,
        None,
        digest("cuda-source"),
        experiment.corpus_digest(),
        digest("cuda-environment"),
        true,
        true,
        observations(&corpus),
    )
    .expect("reference run");
    let candidate = ReductionRunReceipt::new(
        experiment.experiment_digest(),
        ReductionRunRole::AscendCandidate,
        Some(candidate_id),
        digest("ascend-source"),
        experiment.corpus_digest(),
        digest("ascend-environment"),
        true,
        true,
        observations(&corpus),
    )
    .expect("candidate run");
    (experiment, reference, candidate, corpus)
}

#[test]
fn calibrated_oracle_passes_the_exact_independent_run_pair() {
    let policy = ReductionOraclePolicy::fixture_v1();
    let (experiment, reference, candidate, corpus) = runs();
    let calibration =
        calibrate_reduction_oracle(&reference, &policy, &corpus).expect("calibration receipt");
    assert!(calibration.passed());
    assert_eq!(
        calibration.detections().len(),
        ReductionMutantKind::ALL.len()
    );
    assert!(calibration.detections().iter().all(|item| item.detected));

    let receipt = evaluate_reduction_correctness(
        experiment,
        &reference,
        &candidate,
        &policy,
        &corpus,
        &calibration,
    )
    .expect("correctness receipt");
    assert_eq!(receipt.verdict(), CorrectnessVerdict::Pass);
    assert!(receipt.failures().is_empty());
}

#[test]
fn semantic_mismatch_fails_but_uncalibrated_policy_is_unverifiable() {
    let policy = ReductionOraclePolicy::fixture_v1();
    let (experiment, reference, mut candidate, corpus) = runs();
    candidate.observations[2].output_bits = Some(9.0_f32.to_bits());
    let calibration =
        calibrate_reduction_oracle(&reference, &policy, &corpus).expect("calibration");
    let failed = evaluate_reduction_correctness(
        experiment.clone(),
        &reference,
        &candidate,
        &policy,
        &corpus,
        &calibration,
    )
    .expect("failed receipt");
    assert_eq!(failed.verdict(), CorrectnessVerdict::Fail);
    assert!(
        failed
            .failures()
            .iter()
            .any(|item| item.kind == ReductionOracleFailureKind::NumericMismatch)
    );

    let weak_policy = ReductionOraclePolicy {
        absolute_tolerance_nanos: u32::MAX,
        relative_tolerance_ppb: u32::MAX,
        required_repetitions: 2,
    };
    let weak_experiment = ReductionCorrectnessExperiment::new(
        TaskId::try_from("task-reduction-correctness").expect("task ID"),
        candidate.candidate_id().expect("candidate").clone(),
        digest("migration-spec"),
        digest("manifest"),
        digest("source-gate"),
        digest("build-gate"),
        corpus.digest().expect("corpus digest"),
        weak_policy.digest().expect("policy digest"),
    );
    let weak_reference = ReductionRunReceipt::new(
        weak_experiment.experiment_digest(),
        ReductionRunRole::CudaReference,
        None,
        digest("cuda-source"),
        weak_experiment.corpus_digest(),
        digest("cuda-environment"),
        true,
        true,
        observations(&corpus),
    )
    .expect("weak reference");
    let weak_candidate = ReductionRunReceipt::new(
        weak_experiment.experiment_digest(),
        ReductionRunRole::AscendCandidate,
        candidate.candidate_id().cloned(),
        digest("ascend-source"),
        weak_experiment.corpus_digest(),
        digest("ascend-environment"),
        true,
        true,
        observations(&corpus),
    )
    .expect("weak candidate");
    let weak_calibration = calibrate_reduction_oracle(&weak_reference, &weak_policy, &corpus)
        .expect("weak calibration");
    assert!(!weak_calibration.passed());
    let unverifiable = evaluate_reduction_correctness(
        weak_experiment,
        &weak_reference,
        &weak_candidate,
        &weak_policy,
        &corpus,
        &weak_calibration,
    )
    .expect("unverifiable receipt");
    assert_eq!(unverifiable.verdict(), CorrectnessVerdict::Unverifiable);
}

#[test]
fn run_receipt_deserialization_cannot_bypass_identity_validation() {
    let (_, reference, _, _) = runs();
    let mut value = serde_json::to_value(reference).expect("serialize");
    value["observations"][1]["repetition"] = value["observations"][0]["repetition"].clone();
    assert!(serde_json::from_value::<ReductionRunReceipt>(value).is_err());
}

#[test]
fn calibration_rejects_a_reference_that_omits_one_frozen_case() {
    let policy = ReductionOraclePolicy::fixture_v1();
    let (_, mut reference, _, corpus) = runs();
    reference.observations.pop();
    let calibration =
        calibrate_reduction_oracle(&reference, &policy, &corpus).expect("calibration receipt");
    assert!(!calibration.passed());
}
