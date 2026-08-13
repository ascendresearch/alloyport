use super::*;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn observations() -> Vec<ReductionObservation> {
    let cases = [
        ("zero", 0, 0, Some(0.0_f32)),
        ("one", 1, 0, Some(1.25_f32)),
        ("tail-257", 257, 0, Some(31.5_f32)),
        ("maximum", 1_048_576, 0, Some(-2048.25_f32)),
        ("invalid-null", 1, 1, None),
        ("unsupported", 1_048_577, 3, None),
    ];
    cases
        .into_iter()
        .flat_map(|(case_id, elements, status, output)| {
            (1..=2).map(move |repetition| ReductionObservation {
                case_id: case_id.to_owned(),
                repetition,
                elements,
                input_digest: digest(&format!("input-{case_id}")),
                status,
                output_bits: output.map(f32::to_bits),
            })
        })
        .collect()
}

fn experiment(candidate_id: CandidateId) -> ReductionCorrectnessExperiment {
    let policy = ReductionOraclePolicy::fixture_v1();
    ReductionCorrectnessExperiment::new(
        TaskId::try_from("task-reduction-correctness").expect("task ID"),
        candidate_id,
        digest("migration-spec"),
        digest("manifest"),
        digest("source-gate"),
        digest("build-gate"),
        digest(REDUCTION_CORPUS_REVISION_V1),
        policy.digest().expect("policy digest"),
    )
}

fn runs() -> (
    ReductionCorrectnessExperiment,
    ReductionRunReceipt,
    ReductionRunReceipt,
) {
    let candidate_id = CandidateId::try_from("candidate-reduction-correctness").expect("ID");
    let experiment = experiment(candidate_id.clone());
    let reference = ReductionRunReceipt::new(
        experiment.experiment_digest(),
        ReductionRunRole::CudaReference,
        None,
        digest("cuda-source"),
        experiment.corpus_digest(),
        digest("cuda-environment"),
        true,
        true,
        observations(),
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
        observations(),
    )
    .expect("candidate run");
    (experiment, reference, candidate)
}

#[test]
fn calibrated_oracle_passes_the_exact_independent_run_pair() {
    let policy = ReductionOraclePolicy::fixture_v1();
    let (experiment, reference, candidate) = runs();
    let calibration = calibrate_reduction_oracle(&reference, &policy).expect("calibration receipt");
    assert!(calibration.passed());
    assert_eq!(
        calibration.detections().len(),
        ReductionMutantKind::ALL.len()
    );
    assert!(calibration.detections().iter().all(|item| item.detected));

    let receipt =
        evaluate_reduction_correctness(experiment, &reference, &candidate, &policy, &calibration)
            .expect("correctness receipt");
    assert_eq!(receipt.verdict(), CorrectnessVerdict::Pass);
    assert!(receipt.failures().is_empty());
}

#[test]
fn semantic_mismatch_fails_but_uncalibrated_policy_is_unverifiable() {
    let policy = ReductionOraclePolicy::fixture_v1();
    let (experiment, reference, mut candidate) = runs();
    candidate.observations[2].output_bits = Some(9.0_f32.to_bits());
    let calibration = calibrate_reduction_oracle(&reference, &policy).expect("calibration");
    let failed = evaluate_reduction_correctness(
        experiment.clone(),
        &reference,
        &candidate,
        &policy,
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
        digest(REDUCTION_CORPUS_REVISION_V1),
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
        observations(),
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
        observations(),
    )
    .expect("weak candidate");
    let weak_calibration =
        calibrate_reduction_oracle(&weak_reference, &weak_policy).expect("weak calibration");
    assert!(!weak_calibration.passed());
    let unverifiable = evaluate_reduction_correctness(
        weak_experiment,
        &weak_reference,
        &weak_candidate,
        &weak_policy,
        &weak_calibration,
    )
    .expect("unverifiable receipt");
    assert_eq!(unverifiable.verdict(), CorrectnessVerdict::Unverifiable);
}

#[test]
fn run_receipt_deserialization_cannot_bypass_identity_validation() {
    let (_, reference, _) = runs();
    let mut value = serde_json::to_value(reference).expect("serialize");
    value["observations"][1]["repetition"] = value["observations"][0]["repetition"].clone();
    assert!(serde_json::from_value::<ReductionRunReceipt>(value).is_err());
}
