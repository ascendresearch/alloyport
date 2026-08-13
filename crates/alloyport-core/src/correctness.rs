//! Independent reduction correctness, oracle calibration, and execution-port contracts.

use crate::correctness_attempt::ReductionCorrectnessError;
use crate::{CandidateId, ReductionCorpus, Sha256Digest, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[path = "correctness_mutation.rs"]
mod mutation;

pub const REDUCTION_RUN_RECEIPT_SCHEMA_V1: u16 = 1;
pub const REDUCTION_CALIBRATION_RECEIPT_SCHEMA_V1: u16 = 1;
pub const REDUCTION_CORRECTNESS_RECEIPT_SCHEMA_V1: u16 = 1;
pub const REDUCTION_ORACLE_REVISION_V1: &str = "cuda-reduction-differential-oracle-v1";
pub const REDUCTION_CORPUS_REVISION_V1: &str = "cuda-reduction-corpus-v1";

/// Whether a run is the original CUDA authority path or the generated Ascend path under test.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReductionRunRole {
    CudaReference,
    AscendCandidate,
}

/// One externally observed public-API result. Output is stored as exact fp32 bits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionObservation {
    pub case_id: String,
    pub repetition: u16,
    pub elements: u64,
    pub input_digest: Sha256Digest,
    pub status: i32,
    pub output_bits: Option<u32>,
}

impl ReductionObservation {
    fn validate(&self) -> Result<(), ReductionCorrectnessError> {
        if self.case_id.is_empty()
            || self.case_id.len() > 128
            || !self
                .case_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            || self.repetition == 0
        {
            return Err(ReductionCorrectnessError::InvalidObservation);
        }
        if (self.status == 0) != self.output_bits.is_some() {
            return Err(ReductionCorrectnessError::InvalidObservation);
        }
        Ok(())
    }
}

/// Immutable structured output authored by a trusted runner, not by the candidate process.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReductionRunReceipt {
    schema_version: u16,
    experiment_digest: Sha256Digest,
    role: ReductionRunRole,
    candidate_id: Option<CandidateId>,
    implementation_digest: Sha256Digest,
    corpus_digest: Sha256Digest,
    environment_digest: Sha256Digest,
    implementation_invoked: bool,
    synchronized: bool,
    observations: Vec<ReductionObservation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReductionRunReceiptDocument {
    schema_version: u16,
    experiment_digest: Sha256Digest,
    role: ReductionRunRole,
    candidate_id: Option<CandidateId>,
    implementation_digest: Sha256Digest,
    corpus_digest: Sha256Digest,
    environment_digest: Sha256Digest,
    implementation_invoked: bool,
    synchronized: bool,
    observations: Vec<ReductionObservation>,
}

impl<'de> Deserialize<'de> for ReductionRunReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let document = ReductionRunReceiptDocument::deserialize(deserializer)?;
        if document.schema_version != REDUCTION_RUN_RECEIPT_SCHEMA_V1 {
            return Err(serde::de::Error::custom(
                "unsupported reduction run receipt schema",
            ));
        }
        Self::new(
            document.experiment_digest,
            document.role,
            document.candidate_id,
            document.implementation_digest,
            document.corpus_digest,
            document.environment_digest,
            document.implementation_invoked,
            document.synchronized,
            document.observations,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ReductionRunReceipt {
    /// Validates a complete, uniquely keyed observation set.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid role/candidate combinations, observations, or duplicate keys.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        experiment_digest: Sha256Digest,
        role: ReductionRunRole,
        candidate_id: Option<CandidateId>,
        implementation_digest: Sha256Digest,
        corpus_digest: Sha256Digest,
        environment_digest: Sha256Digest,
        implementation_invoked: bool,
        synchronized: bool,
        observations: Vec<ReductionObservation>,
    ) -> Result<Self, ReductionCorrectnessError> {
        if observations.is_empty()
            || matches!(role, ReductionRunRole::CudaReference) && candidate_id.is_some()
            || matches!(role, ReductionRunRole::AscendCandidate) && candidate_id.is_none()
        {
            return Err(ReductionCorrectnessError::InvalidRunContext);
        }
        let mut keys = BTreeSet::new();
        for observation in &observations {
            observation.validate()?;
            if !keys.insert((observation.case_id.clone(), observation.repetition)) {
                return Err(ReductionCorrectnessError::DuplicateObservation);
            }
        }
        Ok(Self {
            schema_version: REDUCTION_RUN_RECEIPT_SCHEMA_V1,
            experiment_digest,
            role,
            candidate_id,
            implementation_digest,
            corpus_digest,
            environment_digest,
            implementation_invoked,
            synchronized,
            observations,
        })
    }

    #[must_use]
    pub const fn experiment_digest(&self) -> Sha256Digest {
        self.experiment_digest
    }

    #[must_use]
    pub const fn role(&self) -> ReductionRunRole {
        self.role
    }

    #[must_use]
    pub const fn corpus_digest(&self) -> Sha256Digest {
        self.corpus_digest
    }

    #[must_use]
    pub const fn implementation_digest(&self) -> Sha256Digest {
        self.implementation_digest
    }

    #[must_use]
    pub const fn candidate_id(&self) -> Option<&CandidateId> {
        self.candidate_id.as_ref()
    }

    #[must_use]
    pub fn observations(&self) -> &[ReductionObservation] {
        &self.observations
    }

    /// Computes the canonical serialized receipt identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Fixed numeric and repetition policy captured by every calibration and verdict.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionOraclePolicy {
    pub absolute_tolerance_nanos: u32,
    pub relative_tolerance_ppb: u32,
    pub required_repetitions: u16,
}

impl ReductionOraclePolicy {
    /// Policy used by the first reduction specimen: 1e-4 absolute and 2e-5 relative.
    #[must_use]
    pub const fn fixture_v1() -> Self {
        Self {
            absolute_tolerance_nanos: 100_000,
            relative_tolerance_ppb: 20_000,
            required_repetitions: 2,
        }
    }

    /// Computes the canonical policy identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Deliberate non-equivalent defects the reduction oracle must reject before use.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReductionMutantKind {
    ArithmeticScale,
    IndexingSwap,
    BoundaryMask,
    AccumulationError,
    InvalidStatus,
    SignedZero,
    NonFinite,
    FallbackBypass,
    MissingSynchronization,
    Nondeterminism,
}

impl ReductionMutantKind {
    pub const ALL: [Self; 10] = [
        Self::ArithmeticScale,
        Self::IndexingSwap,
        Self::BoundaryMask,
        Self::AccumulationError,
        Self::InvalidStatus,
        Self::SignedZero,
        Self::NonFinite,
        Self::FallbackBypass,
        Self::MissingSynchronization,
        Self::Nondeterminism,
    ];
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionMutationDetection {
    pub mutant: ReductionMutantKind,
    pub detected: bool,
}

/// Evidence that the exact oracle/policy/reference combination catches every required mutant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionCalibrationReceipt {
    schema_version: u16,
    oracle_revision: String,
    policy_digest: Sha256Digest,
    corpus_digest: Sha256Digest,
    reference_run_digest: Sha256Digest,
    identity_passed: bool,
    detections: Vec<ReductionMutationDetection>,
    passed: bool,
}

impl ReductionCalibrationReceipt {
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.passed
    }

    #[must_use]
    pub fn detections(&self) -> &[ReductionMutationDetection] {
        &self.detections
    }

    /// Computes the canonical calibration identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Stable identity joining the independent CUDA and Ascend executions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionCorrectnessExperiment {
    experiment_digest: Sha256Digest,
    task_id: TaskId,
    candidate_id: CandidateId,
    migration_spec_digest: Sha256Digest,
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
    build_gate_receipt_digest: Sha256Digest,
    corpus_digest: Sha256Digest,
    policy_digest: Sha256Digest,
}

impl ReductionCorrectnessExperiment {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        candidate_id: CandidateId,
        migration_spec_digest: Sha256Digest,
        manifest_digest: Sha256Digest,
        source_gate_receipt_digest: Sha256Digest,
        build_gate_receipt_digest: Sha256Digest,
        corpus_digest: Sha256Digest,
        policy_digest: Sha256Digest,
    ) -> Self {
        let mut identity = b"alloyport-reduction-correctness-experiment-v1\0".to_vec();
        identity.extend_from_slice(task_id.as_str().as_bytes());
        identity.extend_from_slice(candidate_id.as_str().as_bytes());
        for digest in [
            migration_spec_digest,
            manifest_digest,
            source_gate_receipt_digest,
            build_gate_receipt_digest,
            corpus_digest,
            policy_digest,
        ] {
            identity.extend_from_slice(&digest.bytes());
        }
        Self {
            experiment_digest: Sha256Digest::digest_bytes(&identity),
            task_id,
            candidate_id,
            migration_spec_digest,
            manifest_digest,
            source_gate_receipt_digest,
            build_gate_receipt_digest,
            corpus_digest,
            policy_digest,
        }
    }

    #[must_use]
    pub const fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    #[must_use]
    pub const fn experiment_digest(&self) -> Sha256Digest {
        self.experiment_digest
    }

    #[must_use]
    pub const fn candidate_id(&self) -> &CandidateId {
        &self.candidate_id
    }

    #[must_use]
    pub const fn corpus_digest(&self) -> Sha256Digest {
        self.corpus_digest
    }

    #[must_use]
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }
}

/// Why an otherwise terminal pair cannot pass the correctness gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReductionOracleFailureKind {
    CalibrationUnavailable,
    ObservationSetMismatch,
    ReferenceNotAuthoritative,
    CandidatePathNotInvoked,
    MissingSynchronization,
    StatusMismatch,
    MissingOutput,
    NonFiniteOutput,
    SignedZeroMismatch,
    NumericMismatch,
    NondeterministicOutput,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionOracleFailure {
    pub kind: ReductionOracleFailureKind,
    pub case_id: Option<String>,
    pub repetition: Option<u16>,
    pub detail: String,
}

/// Four-way correctness semantics. Only `Pass` promotes a candidate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CorrectnessVerdict {
    Pass,
    Fail,
    Unverifiable,
    InfraError,
}

/// Independent oracle result over exact run and calibration identities.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionCorrectnessReceipt {
    schema_version: u16,
    oracle_revision: String,
    experiment: ReductionCorrectnessExperiment,
    reference_run_digest: Sha256Digest,
    candidate_run_digest: Sha256Digest,
    calibration_receipt_digest: Sha256Digest,
    verdict: CorrectnessVerdict,
    failures: Vec<ReductionOracleFailure>,
}

impl ReductionCorrectnessReceipt {
    #[must_use]
    pub const fn verdict(&self) -> CorrectnessVerdict {
        self.verdict
    }

    #[must_use]
    pub fn failures(&self) -> &[ReductionOracleFailure] {
        &self.failures
    }

    /// Computes the canonical correctness-receipt identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Run the complete mutation battery against one exact reference and policy.
///
/// # Errors
///
/// Returns an error for a non-reference input or evidence that cannot be serialized.
pub fn calibrate_reduction_oracle(
    reference: &ReductionRunReceipt,
    policy: &ReductionOraclePolicy,
    corpus: &ReductionCorpus,
) -> Result<ReductionCalibrationReceipt, ReductionCorrectnessError> {
    if reference.role != ReductionRunRole::CudaReference {
        return Err(ReductionCorrectnessError::ReferenceRoleRequired);
    }
    if corpus.digest()? != reference.corpus_digest {
        return Err(ReductionCorrectnessError::ExperimentIdentityMismatch);
    }
    let identity_passed = compare_runs(reference, reference, policy, corpus).is_empty();
    let mut detections = Vec::with_capacity(ReductionMutantKind::ALL.len());
    for mutant in ReductionMutantKind::ALL {
        let detected = mutation::apply_mutant(reference.clone(), mutant).is_some_and(|candidate| {
            !compare_runs(reference, &candidate, policy, corpus).is_empty()
        });
        detections.push(ReductionMutationDetection { mutant, detected });
    }
    let passed = identity_passed && detections.iter().all(|item| item.detected);
    Ok(ReductionCalibrationReceipt {
        schema_version: REDUCTION_CALIBRATION_RECEIPT_SCHEMA_V1,
        oracle_revision: REDUCTION_ORACLE_REVISION_V1.to_owned(),
        policy_digest: policy.digest()?,
        corpus_digest: reference.corpus_digest,
        reference_run_digest: reference.digest()?,
        identity_passed,
        detections,
        passed,
    })
}

/// Judge an Ascend run only after calibration of this exact reference and policy.
///
/// # Errors
///
/// Returns an error for crossed experiment/run identities or serialization failure.
pub fn evaluate_reduction_correctness(
    experiment: ReductionCorrectnessExperiment,
    reference: &ReductionRunReceipt,
    candidate: &ReductionRunReceipt,
    policy: &ReductionOraclePolicy,
    corpus: &ReductionCorpus,
    calibration: &ReductionCalibrationReceipt,
) -> Result<ReductionCorrectnessReceipt, ReductionCorrectnessError> {
    validate_experiment_runs(&experiment, reference, candidate, policy, corpus)?;
    let reference_run_digest = reference.digest()?;
    let candidate_run_digest = candidate.digest()?;
    let calibration_receipt_digest = calibration.digest()?;
    let calibrated = calibration.passed
        && calibration.oracle_revision == REDUCTION_ORACLE_REVISION_V1
        && calibration.policy_digest == experiment.policy_digest
        && calibration.corpus_digest == experiment.corpus_digest
        && calibration.reference_run_digest == reference_run_digest;
    let (verdict, failures) = if calibrated {
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
                "the exact oracle, policy, corpus, and reference run were not calibrated",
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
        failures,
    })
}

fn validate_experiment_runs(
    experiment: &ReductionCorrectnessExperiment,
    reference: &ReductionRunReceipt,
    candidate: &ReductionRunReceipt,
    policy: &ReductionOraclePolicy,
    corpus: &ReductionCorpus,
) -> Result<(), ReductionCorrectnessError> {
    if reference.role != ReductionRunRole::CudaReference
        || candidate.role != ReductionRunRole::AscendCandidate
        || candidate.candidate_id.as_ref() != Some(&experiment.candidate_id)
        || reference.experiment_digest != experiment.experiment_digest
        || candidate.experiment_digest != experiment.experiment_digest
        || reference.corpus_digest != experiment.corpus_digest
        || candidate.corpus_digest != experiment.corpus_digest
        || policy.digest()? != experiment.policy_digest
        || corpus.digest()? != experiment.corpus_digest
    {
        return Err(ReductionCorrectnessError::ExperimentIdentityMismatch);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn compare_runs(
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
    let error = (actual - expected).abs();
    let absolute = f64::from(policy.absolute_tolerance_nanos) / 1_000_000_000.0;
    let relative = expected.abs() * f64::from(policy.relative_tolerance_ppb) / 1_000_000_000.0;
    error <= absolute.max(relative)
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

#[cfg(test)]
#[path = "correctness_tests.rs"]
mod tests;
