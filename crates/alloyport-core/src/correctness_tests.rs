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
                reorder_output_bits: None,
            }
        })
        .collect()
}

/// Replaces every successful output so a test can state the authority's spread exactly.
fn with_outputs(
    mut observations: Vec<ReductionObservation>,
    first: f32,
    later: f32,
) -> Vec<ReductionObservation> {
    for observation in &mut observations {
        if observation.output_bits.is_some() {
            observation.output_bits = Some(if observation.repetition == 1 {
                first.to_bits()
            } else {
                later.to_bits()
            });
        }
    }
    observations
}

/// Drifts one repetition of every case, the way a block reduction that ends in `atomicAdd` does.
fn with_repetition_drift(
    mut observations: Vec<ReductionObservation>,
    drifted_repetition: u16,
    relative_drift: f32,
) -> Vec<ReductionObservation> {
    for observation in &mut observations {
        if observation.repetition != drifted_repetition {
            continue;
        }
        if let Some(bits) = observation.output_bits {
            let value = f32::from_bits(bits);
            observation.output_bits = Some((value * (1.0 + relative_drift)).to_bits());
        }
    }
    observations
}

fn experiment(
    candidate_id: CandidateId,
    corpus: &ReductionCorpus,
) -> ReductionCorrectnessExperiment {
    let plan = ReductionTolerancePlan::fixture_v1();
    ReductionCorrectnessExperiment::new(
        TaskId::try_from("task-reduction-correctness").expect("task ID"),
        candidate_id,
        digest("migration-spec"),
        digest("manifest"),
        digest("source-gate"),
        digest("build-gate"),
        corpus.digest().expect("corpus digest"),
        plan.digest().expect("tolerance plan digest"),
    )
}

fn pair(
    corpus: &ReductionCorpus,
    reference_observations: Vec<ReductionObservation>,
    candidate_observations: Vec<ReductionObservation>,
) -> (
    ReductionCorrectnessExperiment,
    ReductionRunReceipt,
    ReductionRunReceipt,
) {
    let candidate_id = CandidateId::try_from("candidate-reduction-correctness").expect("ID");
    let experiment = experiment(candidate_id.clone(), corpus);
    let reference = ReductionRunReceipt::new(
        experiment.experiment_digest(),
        ReductionRunRole::CudaReference,
        None,
        digest("cuda-source"),
        experiment.corpus_digest(),
        digest("cuda-environment"),
        true,
        true,
        reference_observations,
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
        candidate_observations,
    )
    .expect("candidate run");
    (experiment, reference, candidate)
}

fn runs() -> (
    ReductionCorrectnessExperiment,
    ReductionRunReceipt,
    ReductionRunReceipt,
    ReductionCorpus,
) {
    let corpus = ReductionCorpus::fixture_v1();
    let (experiment, reference, candidate) =
        pair(&corpus, observations(&corpus), observations(&corpus));
    (experiment, reference, candidate, corpus)
}

#[test]
fn calibrated_oracle_passes_the_exact_independent_run_pair() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let (experiment, reference, candidate, corpus) = runs();
    let calibration =
        calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration receipt");
    assert!(calibration.passed());
    assert!(calibration.undetected().is_empty());
    assert_eq!(
        calibration.detections().len(),
        ReductionMutantKind::ALL.len()
    );
    let floor = calibration.noise_floor().expect("a measured floor");
    assert!(floor.deterministic(), "this authority reproduces itself");
    assert_eq!(floor.observed_absolute_nanos(), 0);
    assert!(floor.repetition_pairs() > 0, "repetitions were compared");
    assert_eq!(
        calibration.battery_scope(),
        ReductionBatteryScope::ComparatorOnly,
        "no known-bad implementation has ever been compiled and run through this gate"
    );

    let receipt = evaluate_reduction_correctness(
        experiment,
        &reference,
        &candidate,
        &plan,
        &corpus,
        &calibration,
    )
    .expect("correctness receipt");
    assert_eq!(receipt.verdict(), CorrectnessVerdict::Pass);
    assert!(receipt.failures().is_empty());
}

#[test]
fn semantic_mismatch_fails_the_calibrated_gate() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let (experiment, reference, mut candidate, corpus) = runs();
    candidate.observations[2].output_bits = Some(9.0_f32.to_bits());
    let calibration = calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration");
    let failed = evaluate_reduction_correctness(
        experiment,
        &reference,
        &candidate,
        &plan,
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
}

#[test]
fn a_tolerance_nobody_measured_cannot_calibrate() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let corpus = ReductionCorpus::fixture_v1();
    // One observation per case and no second summation order: nothing in this run says how far a
    // correct implementation may legitimately land from it.
    let single: Vec<_> = observations(&corpus)
        .into_iter()
        .filter(|observation| observation.repetition == 1)
        .collect();
    let (experiment, reference, candidate) = pair(&corpus, single.clone(), single);
    assert!(measure_reduction_noise_floor(&reference, &corpus).is_err());
    let calibration = calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration");
    assert!(!calibration.passed());
    assert!(!calibration.tolerance_measured());
    assert!(calibration.noise_floor().is_none());
    let receipt = evaluate_reduction_correctness(
        experiment,
        &reference,
        &candidate,
        &plan,
        &corpus,
        &calibration,
    )
    .expect("receipt");
    assert_eq!(
        receipt.verdict(),
        CorrectnessVerdict::Unverifiable,
        "identical runs must not read as PASS when no tolerance was ever measured"
    );
}

#[test]
fn the_measured_floor_admits_a_port_that_sums_in_a_different_order() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let corpus = ReductionCorpus::fixture_v1();
    // The authority does not reproduce itself: its blocks retire in whatever order they finish.
    let reference_observations = with_repetition_drift(observations(&corpus), 2, 2e-7);
    // A correct port lands within that same spread, but never on the reference's exact bits.
    let candidate_observations = with_repetition_drift(observations(&corpus), 1, 2e-7);
    let (experiment, reference, candidate) = pair(
        &corpus,
        reference_observations.clone(),
        candidate_observations,
    );
    let floor = measure_reduction_noise_floor(&reference, &corpus).expect("floor");
    assert!(!floor.deterministic());
    assert!(floor.observed_absolute_nanos() > 0);

    let calibration = calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration");
    assert!(
        calibration.reordered_authority_admitted(),
        "a gate tighter than the task's own spread would reject a correct port"
    );
    assert!(calibration.passed(), "{:?}", calibration.undetected());
    let receipt = evaluate_reduction_correctness(
        experiment,
        &reference,
        &candidate,
        &plan,
        &corpus,
        &calibration,
    )
    .expect("receipt");
    assert_eq!(receipt.verdict(), CorrectnessVerdict::Pass);
}

#[test]
fn a_battery_of_sledgehammers_cannot_bound_a_tolerance_that_swallowed_a_real_defect() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let corpus = ReductionCorpus::fixture_v1();
    // A floor this wide is what a badly behaved authority would produce. The gate derived from it
    // still catches every mutant sized by the policy, and still admits the reordered authority —
    // and it must nonetheless refuse, because a defect it should separate now survives.
    let reference_observations = with_outputs(observations(&corpus), 1.0, 1.5);
    let (_, reference, _) = pair(
        &corpus,
        reference_observations.clone(),
        reference_observations,
    );
    let calibration = calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration");
    assert!(calibration.reordered_authority_admitted());
    assert!(
        calibration
            .undetected()
            .contains(&ReductionMutantKind::ArithmeticScale),
        "a ten percent arithmetic error must be reported as missed, not silently tolerated"
    );
    assert!(!calibration.passed());
    assert!(
        calibration
            .detections()
            .iter()
            .any(|item| item.mutant == ReductionMutantKind::JustOutsideTolerance && item.detected),
        "the policy-sized mutant is always caught, which is exactly why it cannot be the only one"
    );
}

#[test]
fn run_receipt_deserialization_cannot_bypass_identity_validation() {
    let (_, reference, _, _) = runs();
    let mut value = serde_json::to_value(reference).expect("serialize");
    value["observations"][1]["repetition"] = value["observations"][0]["repetition"].clone();
    assert!(serde_json::from_value::<ReductionRunReceipt>(value).is_err());
}

#[test]
fn a_calibration_receipt_is_revalidated_on_read_rather_than_believed() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let (_, reference, _, corpus) = runs();
    let calibration = calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration");
    assert!(calibration.passed());
    let mut value = serde_json::to_value(&calibration).expect("serialize");
    // The shape a calibration written under the earlier rules has: it says it passed, and it
    // cannot say what tolerance it measured.
    value["checks"]["tolerance_measured"] = serde_json::Value::Bool(false);
    value["noise_floor"] = serde_json::Value::Null;
    let legacy: ReductionCalibrationReceipt = serde_json::from_value(value).expect("deserialize");
    assert!(
        !legacy.passed(),
        "a stored pass verdict is the applicant's own word about a gate it never measured"
    );
}

#[test]
fn calibration_rejects_a_reference_that_omits_one_frozen_case() {
    let plan = ReductionTolerancePlan::fixture_v1();
    let (_, mut reference, _, corpus) = runs();
    reference.observations.pop();
    let calibration =
        calibrate_reduction_oracle(&reference, &plan, &corpus).expect("calibration receipt");
    assert!(!calibration.passed());
}
