//! The differential comparison itself: one candidate run judged against one authority run.
//!
//! Split out of `correctness.rs` for the module-size limit. It stays a child module so the verdict
//! can be assembled from the receipt's own private fields instead of a public constructor that
//! anything could call.

use super::calibration::measure_reduction_noise_floor;
use super::{
    BTreeMap, BTreeSet, CorrectnessVerdict, REDUCTION_CORRECTNESS_RECEIPT_SCHEMA_V1,
    REDUCTION_ORACLE_REVISION_V1, ReductionCalibrationReceipt, ReductionCorpus,
    ReductionCorrectnessError, ReductionCorrectnessExperiment, ReductionCorrectnessReceipt,
    ReductionOracleFailure, ReductionOracleFailureKind, ReductionOraclePolicy, ReductionRunReceipt,
    ReductionRunRole, ReductionTolerancePlan, unverified_assumptions,
};

/// Judge an Ascend run only after calibration of this exact reference and policy.
///
/// # Errors
///
/// Returns an error for crossed experiment/run identities or serialization failure.
pub fn evaluate_reduction_correctness(
    experiment: ReductionCorrectnessExperiment,
    reference: &ReductionRunReceipt,
    candidate: &ReductionRunReceipt,
    plan: &ReductionTolerancePlan,
    corpus: &ReductionCorpus,
    calibration: &ReductionCalibrationReceipt,
) -> Result<ReductionCorrectnessReceipt, ReductionCorrectnessError> {
    validate_experiment_runs(&experiment, reference, candidate, plan, corpus)?;
    let reference_run_digest = reference.digest()?;
    let candidate_run_digest = candidate.digest()?;
    let calibration_receipt_digest = calibration.digest()?;
    // Derived from the reference run again rather than carried from calibration, so the two agree
    // by construction or the identity checks below refuse the pair.
    let policy = measure_reduction_noise_floor(reference, corpus)
        .ok()
        .map(|floor| plan.derive(&floor))
        .transpose()?;
    let derived_policy_digest = policy
        .as_ref()
        .map(ReductionOraclePolicy::digest)
        .transpose()?;
    let calibrated = calibration.passed()
        && calibration.oracle_revision == REDUCTION_ORACLE_REVISION_V1
        && calibration.corpus_digest == experiment.corpus_digest
        && calibration.reference_run_digest == reference_run_digest
        && derived_policy_digest.is_some_and(|derived| calibration.policy_digest == derived);
    let (verdict, failures) = if let (true, Some(policy)) = (calibrated, policy.as_ref()) {
        let failures = compare_runs(reference, candidate, policy, corpus);
        (
            if failures.is_empty() {
                CorrectnessVerdict::Pass
            } else {
                CorrectnessVerdict::Fail
            },
            failures,
        )
    } else {
        (
            CorrectnessVerdict::Unverifiable,
            vec![failure(
                ReductionOracleFailureKind::CalibrationUnavailable,
                None,
                None,
                "the exact oracle, tolerance plan, corpus, and reference run were not calibrated \
                 against a measured floor",
            )],
        )
    };
    Ok(ReductionCorrectnessReceipt {
        schema_version: REDUCTION_CORRECTNESS_RECEIPT_SCHEMA_V1,
        oracle_revision: REDUCTION_ORACLE_REVISION_V1.to_owned(),
        experiment,
        reference_run_digest,
        candidate_run_digest,
        calibration_receipt_digest,
        verdict,
        unverified: unverified_assumptions(),
        failures,
    })
}

fn validate_experiment_runs(
    experiment: &ReductionCorrectnessExperiment,
    reference: &ReductionRunReceipt,
    candidate: &ReductionRunReceipt,
    plan: &ReductionTolerancePlan,
    corpus: &ReductionCorpus,
) -> Result<(), ReductionCorrectnessError> {
    if reference.role != ReductionRunRole::CudaReference
        || candidate.role != ReductionRunRole::AscendCandidate
        || candidate.candidate_id.as_ref() != Some(&experiment.candidate_id)
        || reference.experiment_digest != experiment.experiment_digest
        || candidate.experiment_digest != experiment.experiment_digest
        || reference.corpus_digest != experiment.corpus_digest
        || candidate.corpus_digest != experiment.corpus_digest
        || plan.digest()? != experiment.tolerance_plan_digest
        || corpus.digest()? != experiment.corpus_digest
    {
        return Err(ReductionCorrectnessError::ExperimentIdentityMismatch);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn compare_runs(
    reference: &ReductionRunReceipt,
    candidate: &ReductionRunReceipt,
    policy: &ReductionOraclePolicy,
    corpus: &ReductionCorpus,
) -> Vec<ReductionOracleFailure> {
    let mut failures = Vec::new();
    if !reference.implementation_invoked || !reference.synchronized {
        failures.push(failure(
            ReductionOracleFailureKind::ReferenceNotAuthoritative,
            None,
            None,
            "the CUDA authority path was not invoked and synchronized",
        ));
        return failures;
    }
    if !candidate.implementation_invoked {
        failures.push(failure(
            ReductionOracleFailureKind::CandidatePathNotInvoked,
            None,
            None,
            "the generated Ascend implementation was not observed",
        ));
    }
    if !candidate.synchronized {
        failures.push(failure(
            ReductionOracleFailureKind::MissingSynchronization,
            None,
            None,
            "device completion was not synchronized before observation",
        ));
    }
    let reference_by_key: BTreeMap<_, _> = reference
        .observations
        .iter()
        .map(|item| ((item.case_id.as_str(), item.repetition), item))
        .collect();
    let candidate_by_key: BTreeMap<_, _> = candidate
        .observations
        .iter()
        .map(|item| ((item.case_id.as_str(), item.repetition), item))
        .collect();
    let corpus_by_key: BTreeMap<_, _> = corpus
        .cases()
        .iter()
        .map(|item| ((item.case_id.as_str(), item.repetition), item))
        .collect();
    if reference_by_key.keys().ne(corpus_by_key.keys()) {
        failures.push(failure(
            ReductionOracleFailureKind::ObservationSetMismatch,
            None,
            None,
            "reference observations do not cover the exact frozen corpus",
        ));
        return failures;
    }
    if reference_by_key.keys().ne(candidate_by_key.keys()) {
        failures.push(failure(
            ReductionOracleFailureKind::ObservationSetMismatch,
            None,
            None,
            "reference and candidate observation keys differ",
        ));
        return failures;
    }
    let repetitions: BTreeMap<&str, BTreeSet<u16>> =
        reference
            .observations
            .iter()
            .fold(BTreeMap::new(), |mut repetitions, item| {
                repetitions
                    .entry(item.case_id.as_str())
                    .or_default()
                    .insert(item.repetition);
                repetitions
            });
    let required_repetitions: BTreeSet<_> = (1..=policy.required_repetitions).collect();
    if repetitions
        .values()
        .any(|actual| actual != &required_repetitions)
    {
        failures.push(failure(
            ReductionOracleFailureKind::ObservationSetMismatch,
            None,
            None,
            "the required repetition count is absent",
        ));
    }
    for (key, expected) in reference_by_key {
        let actual = candidate_by_key[&key];
        let corpus_case = corpus_by_key[&key];
        if expected.elements != corpus_case.elements
            || expected.input_digest != corpus_case.input_digest()
            || actual.elements != expected.elements
            || actual.input_digest != expected.input_digest
        {
            failures.push(failure(
                ReductionOracleFailureKind::ObservationSetMismatch,
                Some(expected.case_id.clone()),
                Some(expected.repetition),
                "input identity differs",
            ));
            continue;
        }
        if actual.status != expected.status {
            failures.push(failure(
                ReductionOracleFailureKind::StatusMismatch,
                Some(expected.case_id.clone()),
                Some(expected.repetition),
                "public API status differs",
            ));
            continue;
        }
        let (Some(expected_bits), Some(actual_bits)) = (expected.output_bits, actual.output_bits)
        else {
            if expected.status == 0 {
                failures.push(failure(
                    ReductionOracleFailureKind::MissingOutput,
                    Some(expected.case_id.clone()),
                    Some(expected.repetition),
                    "successful case lacks an fp32 output",
                ));
            }
            continue;
        };
        let expected_value = f32::from_bits(expected_bits);
        let actual_value = f32::from_bits(actual_bits);
        if !expected_value.is_finite() || !actual_value.is_finite() {
            failures.push(failure(
                ReductionOracleFailureKind::NonFiniteOutput,
                Some(expected.case_id.clone()),
                Some(expected.repetition),
                "comparison contains a non-finite value",
            ));
        } else if expected_bits == 0 && actual_bits != 0 {
            failures.push(failure(
                ReductionOracleFailureKind::SignedZeroMismatch,
                Some(expected.case_id.clone()),
                Some(expected.repetition),
                "zero-element contract requires positive zero",
            ));
        } else if !within_tolerance(expected_value, actual_value, policy) {
            failures.push(failure(
                ReductionOracleFailureKind::NumericMismatch,
                Some(expected.case_id.clone()),
                Some(expected.repetition),
                "candidate output exceeds the frozen absolute/relative tolerance",
            ));
        }
    }
    for case_id in repetitions.keys() {
        let mut values = candidate
            .observations
            .iter()
            .filter(|item| item.case_id == *case_id && item.status == 0)
            .filter_map(|item| item.output_bits.map(f32::from_bits));
        if let Some(first) = values.next()
            && values.any(|value| !within_tolerance(first, value, policy))
        {
            failures.push(failure(
                ReductionOracleFailureKind::NondeterministicOutput,
                Some((*case_id).to_owned()),
                None,
                "repeated candidate observations disagree beyond tolerance",
            ));
        }
    }
    failures
}

fn within_tolerance(expected: f32, actual: f32, policy: &ReductionOraclePolicy) -> bool {
    let expected = f64::from(expected);
    let actual = f64::from(actual);
    (actual - expected).abs() <= tolerance_bound(expected, policy)
}

/// The largest error this policy admits at `expected`.
pub(crate) fn tolerance_bound(expected: f64, policy: &ReductionOraclePolicy) -> f64 {
    let absolute = f64::from(policy.absolute_tolerance_nanos) / 1_000_000_000.0;
    let relative = expected.abs() * f64::from(policy.relative_tolerance_ppb) / 1_000_000_000.0;
    absolute.max(relative)
}

fn failure(
    kind: ReductionOracleFailureKind,
    case_id: Option<String>,
    repetition: Option<u16>,
    detail: impl Into<String>,
) -> ReductionOracleFailure {
    ReductionOracleFailure {
        kind,
        case_id,
        repetition,
        detail: detail.into(),
    }
}
