//! Measuring this task's numeric spread, and proving the oracle still catches defects at it.
//!
//! Split out of `correctness.rs` for the module-size limit. These two belong together: the floor is
//! what makes the tolerance honest, and the battery is what shows the honest tolerance still
//! separates correct from broken.

use super::evaluation::compare_runs;
use super::mutation;
use super::{
    BTreeMap, CandidateId, REDUCTION_CALIBRATION_RECEIPT_SCHEMA_V1,
    REDUCTION_NOISE_FLOOR_SCHEMA_V1, REDUCTION_ORACLE_REVISION_V1, ReductionBatteryScope,
    ReductionCalibrationChecks, ReductionCalibrationReceipt, ReductionCorpus,
    ReductionCorrectnessError, ReductionMutantKind, ReductionMutationDetection,
    ReductionNoiseFloor, ReductionOraclePolicy, ReductionRunReceipt, ReductionRunRole,
    ReductionTolerancePlan, Sha256Digest,
};

/// Measures this task's numeric spread from the authority run alone.
///
/// # Errors
///
/// Returns an error for a non-reference run, a corpus that does not match, a reference whose second
/// summation order covers only some cases, or one that offers neither repetitions nor that order —
/// in which case no floor exists and none may be invented.
pub fn measure_reduction_noise_floor(
    reference: &ReductionRunReceipt,
    corpus: &ReductionCorpus,
) -> Result<ReductionNoiseFloor, ReductionCorrectnessError> {
    if reference.role != ReductionRunRole::CudaReference {
        return Err(ReductionCorrectnessError::ReferenceRoleRequired);
    }
    if corpus.digest()? != reference.corpus_digest {
        return Err(ReductionCorrectnessError::ExperimentIdentityMismatch);
    }
    let mut absolute = 0.0_f64;
    let mut relative = 0.0_f64;
    let mut repetition_pairs = 0_u32;
    let mut reorder_pairs = 0_u32;
    let mut deterministic = true;
    let mut successful = 0_u32;
    let mut by_case: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
    for observation in &reference.observations {
        let Some(bits) = observation.output_bits else {
            continue;
        };
        successful = successful.saturating_add(1);
        by_case
            .entry(observation.case_id.as_str())
            .or_default()
            .push(bits);
        if let Some(reorder_bits) = observation.reorder_output_bits {
            reorder_pairs = reorder_pairs.saturating_add(1);
            let (error, ratio) = spread(bits, reorder_bits)?;
            absolute = absolute.max(error);
            relative = relative.max(ratio);
            deterministic &= bits == reorder_bits;
        }
    }
    // Partial coverage is worse than none: it would report a floor measured on the cases that
    // happened to carry a second order, and silently omit the ones that did not.
    if reorder_pairs != 0 && reorder_pairs != successful {
        return Err(ReductionCorrectnessError::NoiseFloorUnavailable);
    }
    for outputs in by_case.values() {
        for window in outputs.windows(2) {
            repetition_pairs = repetition_pairs.saturating_add(1);
            let (error, ratio) = spread(window[0], window[1])?;
            absolute = absolute.max(error);
            relative = relative.max(ratio);
            deterministic &= window[0] == window[1];
        }
    }
    if repetition_pairs == 0 && reorder_pairs == 0 {
        return Err(ReductionCorrectnessError::NoiseFloorUnavailable);
    }
    Ok(ReductionNoiseFloor {
        schema_version: REDUCTION_NOISE_FLOOR_SCHEMA_V1,
        corpus_digest: reference.corpus_digest,
        reference_run_digest: reference.digest()?,
        observed_absolute_nanos: scale_to_units(absolute, 1_000_000_000.0),
        observed_relative_ppb: scale_to_units(relative, 1_000_000_000.0),
        repetition_pairs,
        reorder_pairs,
        deterministic,
    })
}

/// Absolute and relative distance between two fp32 results of the same mathematics.
fn spread(left: u32, right: u32) -> Result<(f64, f64), ReductionCorrectnessError> {
    let left = f64::from(f32::from_bits(left));
    let right = f64::from(f32::from_bits(right));
    if !left.is_finite() || !right.is_finite() {
        return Err(ReductionCorrectnessError::InvalidObservation);
    }
    let error = (right - left).abs();
    let relative = if left == 0.0 { 0.0 } else { error / left.abs() };
    Ok((error, relative))
}

/// Rounds a measured quantity up into fixed-point units without ever rounding a spread to nothing.
fn scale_to_units(value: f64, units_per_one: f64) -> u64 {
    let scaled = (value * units_per_one).ceil();
    if !scaled.is_finite() || scaled <= 0.0 {
        return 0;
    }
    // `u64::MAX as f64` rounds up, so compare against the first power of two above the range.
    if scaled >= 18_446_744_073_709_551_616.0 {
        return u64::MAX;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    {
        scaled as u64
    }
}

/// Run the complete mutation battery against one exact reference and policy.
///
/// Calibration answers two questions that a mutation battery alone cannot. **Is the tolerance too
/// tight** — would this gate reject a correct implementation that merely sums in a different order?
/// That is `reordered_authority_admitted`, checked against the authority's own second summation
/// order rather than against the authority compared to itself, which is true by construction and
/// verifies nothing. **Is the tolerance too loose** — is the boundary it claims the boundary it
/// enforces? That is `JustOutsideTolerance`, the only mutant sized by the policy instead of chosen
/// to be obviously wrong.
///
/// A policy whose tolerance was asserted rather than measured cannot pass: neither question can be
/// answered without a floor, and shipping the gate anyway is how a guessed number becomes evidence.
///
/// # Errors
///
/// Returns an error for a non-reference input or evidence that cannot be serialized.
pub fn calibrate_reduction_oracle(
    reference: &ReductionRunReceipt,
    plan: &ReductionTolerancePlan,
    corpus: &ReductionCorpus,
) -> Result<ReductionCalibrationReceipt, ReductionCorrectnessError> {
    if reference.role != ReductionRunRole::CudaReference {
        return Err(ReductionCorrectnessError::ReferenceRoleRequired);
    }
    if corpus.digest()? != reference.corpus_digest {
        return Err(ReductionCorrectnessError::ExperimentIdentityMismatch);
    }
    // The tolerance is computed here from the reference run itself. No caller supplies it, so no
    // caller can widen it to make a candidate pass.
    let floor = measure_reduction_noise_floor(reference, corpus).ok();
    let policy = floor
        .as_ref()
        .map(|measured| plan.derive(measured))
        .transpose()?;
    let (tolerance_measured, reordered_authority_admitted, detections) = match policy.as_ref() {
        Some(policy) => {
            let admitted = reordered_authority(reference).is_some_and(|reordered| {
                compare_runs(reference, &reordered, policy, corpus).is_empty()
            });
            let detections = ReductionMutantKind::ALL
                .into_iter()
                .map(|mutant| ReductionMutationDetection {
                    mutant,
                    detected: mutation::apply_mutant(reference.clone(), mutant, policy)
                        .is_some_and(|candidate| {
                            !compare_runs(reference, &candidate, policy, corpus).is_empty()
                        }),
                })
                .collect::<Vec<_>>();
            (true, admitted, detections)
        }
        None => (
            false,
            false,
            ReductionMutantKind::ALL
                .into_iter()
                .map(|mutant| ReductionMutationDetection {
                    mutant,
                    detected: false,
                })
                .collect(),
        ),
    };
    let undetected = detections
        .iter()
        .filter(|item| !item.detected)
        .map(|item| item.mutant)
        .collect::<Vec<_>>();
    let passed = tolerance_measured && reordered_authority_admitted && undetected.is_empty();
    Ok(ReductionCalibrationReceipt {
        schema_version: REDUCTION_CALIBRATION_RECEIPT_SCHEMA_V1,
        oracle_revision: REDUCTION_ORACLE_REVISION_V1.to_owned(),
        policy_digest: policy
            .as_ref()
            .map(ReductionOraclePolicy::digest)
            .transpose()?
            .unwrap_or_else(|| {
                Sha256Digest::digest_bytes(b"alloyport-reduction-no-derived-policy")
            }),
        corpus_digest: reference.corpus_digest,
        reference_run_digest: reference.digest()?,
        identity_passed: false,
        battery_scope: ReductionBatteryScope::ComparatorOnly,
        noise_floor: floor,
        checks: ReductionCalibrationChecks {
            tolerance_measured,
            reordered_authority_admitted,
        },
        detections,
        undetected,
        passed,
    })
}

/// The authority's own second summation order, presented as a candidate run.
///
/// This is a correct implementation by construction — same mathematics, same inputs, different
/// legitimate order — so a gate that fails it is a gate that would fail a correct port.
fn reordered_authority(reference: &ReductionRunReceipt) -> Option<ReductionRunReceipt> {
    let mut reordered = reference.clone();
    reordered.role = ReductionRunRole::AscendCandidate;
    reordered.candidate_id = CandidateId::try_from("candidate-reordered-authority").ok();
    if reordered
        .observations
        .iter()
        .any(|observation| observation.reorder_output_bits.is_some())
    {
        for observation in &mut reordered.observations {
            if let Some(bits) = observation.reorder_output_bits {
                observation.output_bits = Some(bits);
            }
        }
        return Some(reordered);
    }
    // Without a second summation order, the authority's own repetitions are the next best correct
    // implementation available: rotating them within each case yields results a correct candidate
    // could legitimately return, because the authority itself returned them.
    let mut by_case: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    for observation in &reference.observations {
        if let Some(bits) = observation.output_bits {
            by_case
                .entry(observation.case_id.clone())
                .or_default()
                .push(bits);
        }
    }
    if by_case.values().all(|outputs| outputs.len() < 2) {
        return None;
    }
    let mut consumed: BTreeMap<String, usize> = BTreeMap::new();
    for observation in &mut reordered.observations {
        if observation.output_bits.is_none() {
            continue;
        }
        let outputs = &by_case[&observation.case_id];
        let index = consumed.entry(observation.case_id.clone()).or_insert(0);
        observation.output_bits = Some(outputs[(*index + 1) % outputs.len()]);
        *index += 1;
    }
    Some(reordered)
}
