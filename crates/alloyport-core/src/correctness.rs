//! Independent reduction correctness, oracle calibration, and execution-port contracts.

use crate::correctness_attempt::ReductionCorrectnessError;
use crate::{CandidateId, ReductionCorpus, Sha256Digest, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[path = "correctness_mutation.rs"]
mod mutation;

pub const REDUCTION_RUN_RECEIPT_SCHEMA_V1: u16 = 1;
pub const REDUCTION_NOISE_FLOOR_SCHEMA_V1: u16 = 1;
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
///
/// `reorder_output_bits` is the same mathematics over the same input under a different legitimate
/// fp32 summation order, computed by the trusted harness beside the authority itself. It exists
/// because a reduction has no single correct fp32 answer: a candidate that sums in a different
/// order is still correct, and the only honest bound on that difference is a measured one. A
/// tolerance chosen without it is an assertion, and an assertion that is too tight rejects correct
/// ports while one that is too loose admits broken ones.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionObservation {
    pub case_id: String,
    pub repetition: u16,
    pub elements: u64,
    pub input_digest: Sha256Digest,
    pub status: i32,
    pub output_bits: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorder_output_bits: Option<u32>,
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
        if self.reorder_output_bits.is_some() && self.output_bits.is_none() {
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

/// This task's own numeric spread, measured rather than assumed.
///
/// The floor is a property of the task — fp32 summation of exactly these inputs — not of the device
/// under test, and not of anyone's judgement. It is measured from the authority alone, across two
/// sources a correct implementation is equally entitled to differ by:
///
/// - **repetitions**: the reference sums the same input more than once. A block reduction that
///   finishes with `atomicAdd` accumulates its partial sums in whatever order the blocks retire, so
///   the authority does not reproduce itself bit for bit. That spread is already present in every
///   run the corpus mandates.
/// - **order**: `reorder_output_bits`, a second legitimate summation order of the same mathematics
///   over the same bytes, when the trusted harness emits one. This is the component that bounds a
///   candidate whose reduction tree simply differs from the reference's.
///
/// A tolerance below this floor rejects correct ports. A tolerance far above it stops separating
/// correct from broken — which is why [`calibrate_reduction_oracle`] must then show that real
/// defects are still caught at whatever the floor produced.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionNoiseFloor {
    schema_version: u16,
    corpus_digest: Sha256Digest,
    reference_run_digest: Sha256Digest,
    observed_absolute_nanos: u64,
    observed_relative_ppb: u64,
    repetition_pairs: u32,
    reorder_pairs: u32,
    deterministic: bool,
}

impl ReductionNoiseFloor {
    #[must_use]
    pub const fn observed_absolute_nanos(&self) -> u64 {
        self.observed_absolute_nanos
    }

    #[must_use]
    pub const fn observed_relative_ppb(&self) -> u64 {
        self.observed_relative_ppb
    }

    /// Same-case repetition pairs of the authority that were compared.
    #[must_use]
    pub const fn repetition_pairs(&self) -> u32 {
        self.repetition_pairs
    }

    /// Authority-versus-second-order pairs compared; zero until the harness emits that column.
    #[must_use]
    pub const fn reorder_pairs(&self) -> u32 {
        self.reorder_pairs
    }

    /// True when every compared pair agreed bit for bit, so the measured floor is exactly zero.
    #[must_use]
    pub const fn deterministic(&self) -> bool {
        self.deterministic
    }

    /// Computes the canonical floor identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

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

/// Where a tolerance came from. A number nobody measured is an assertion, not a floor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToleranceProvenance {
    /// Derived from a `ReductionNoiseFloor` measured on the exact reference run being calibrated.
    MeasuredFloor {
        floor_digest: Sha256Digest,
        slack_percent: u16,
    },
    /// Typed by a person. Readable, reproducible, and deliberately unable to calibrate: a gate
    /// whose tolerance nobody measured cannot say whether it would reject a correct port.
    Asserted { justification: String },
}

/// How the tolerance will be derived once the authority has run.
///
/// This, not a number, is what an experiment can bind before dispatch: the tolerance depends on a
/// measurement that has not happened yet. Freezing a number here instead would be freezing a guess,
/// and the experiment identity would vouch for it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionTolerancePlan {
    pub slack_percent: u16,
    pub required_repetitions: u16,
}

impl ReductionTolerancePlan {
    /// The first specimen's plan: admit half again the task's own measured spread.
    #[must_use]
    pub const fn fixture_v1() -> Self {
        Self {
            slack_percent: 50,
            required_repetitions: 2,
        }
    }

    /// Derives the tolerance this plan produces for one measured floor.
    ///
    /// # Errors
    ///
    /// Returns an error when the measured floor does not fit the tolerance representation.
    pub fn derive(
        &self,
        floor: &ReductionNoiseFloor,
    ) -> Result<ReductionOraclePolicy, ReductionCorrectnessError> {
        ReductionOraclePolicy::derive_from_floor(
            floor,
            self.slack_percent,
            self.required_repetitions,
        )
    }

    /// Computes the canonical plan identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| Sha256Digest::digest_bytes(&bytes))
    }
}

/// Numeric and repetition tolerance, derived from a measured floor by a [`ReductionTolerancePlan`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionOraclePolicy {
    pub absolute_tolerance_nanos: u32,
    pub relative_tolerance_ppb: u32,
    pub required_repetitions: u16,
    #[serde(default = "ToleranceProvenance::legacy_assertion")]
    pub provenance: ToleranceProvenance,
}

impl ToleranceProvenance {
    fn legacy_assertion() -> Self {
        Self::Asserted {
            justification: "recorded before tolerance provenance existed".to_owned(),
        }
    }
}

impl ReductionOraclePolicy {
    /// The tolerance the first specimen shipped with: 1e-4 absolute and 2e-5 relative.
    ///
    /// Nothing measured these numbers. They are retained so existing receipts stay readable and so
    /// a caller can state a tolerance deliberately, but calibration refuses them — see
    /// [`calibrate_reduction_oracle`].
    #[must_use]
    pub fn asserted_v1() -> Self {
        Self {
            absolute_tolerance_nanos: 100_000,
            relative_tolerance_ppb: 20_000,
            required_repetitions: 2,
            provenance: ToleranceProvenance::Asserted {
                justification: "first reduction specimen, chosen before any floor was measured"
                    .to_owned(),
            },
        }
    }

    /// Derives a tolerance from a measured floor plus explicit slack.
    ///
    /// # Errors
    ///
    /// Returns an error when the measured floor does not fit the tolerance representation.
    pub fn derive_from_floor(
        floor: &ReductionNoiseFloor,
        slack_percent: u16,
        required_repetitions: u16,
    ) -> Result<Self, ReductionCorrectnessError> {
        let widen = |observed: u64| -> Option<u32> {
            let widened = u128::from(observed) * u128::from(100_u16.checked_add(slack_percent)?)
                / u128::from(100_u8);
            u32::try_from(widened).ok()
        };
        let absolute_tolerance_nanos =
            widen(floor.observed_absolute_nanos).ok_or(ReductionCorrectnessError::InvalidCorpus)?;
        let relative_tolerance_ppb =
            widen(floor.observed_relative_ppb).ok_or(ReductionCorrectnessError::InvalidCorpus)?;
        Ok(Self {
            absolute_tolerance_nanos,
            relative_tolerance_ppb,
            required_repetitions,
            provenance: ToleranceProvenance::MeasuredFloor {
                floor_digest: floor.digest()?,
                slack_percent,
            },
        })
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
///
/// Every kind here perturbs a **run receipt**, not an implementation. That bounds what the battery
/// can prove: it exercises the comparator's own arithmetic and bookkeeping, and it says nothing
/// about whether the surrounding pipeline would catch a genuinely broken kernel. `FallbackBypass`
/// and `MissingSynchronization` are the clearest case — the trusted harness emits those two flags
/// as literals, so no real candidate can set them false and no real bypass would be caught here.
/// See [`ReductionBatteryScope`], which records that limitation inside the receipt.
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
    /// A perturbation just above the configured tolerance. The battery is worthless if this
    /// survives: every other mutant is orders of magnitude larger, so ten of them can pass at a
    /// tolerance a hundred times too loose.
    JustOutsideTolerance,
}

impl ReductionMutantKind {
    pub const ALL: [Self; 11] = [
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
        Self::JustOutsideTolerance,
    ];
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionMutationDetection {
    pub mutant: ReductionMutantKind,
    pub detected: bool,
}

/// What the battery actually exercised, recorded so a reader cannot over-read `passed`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReductionBatteryScope {
    /// Mutated receipts only: this calibrates the comparator, not the execution pipeline.
    ComparatorOnly,
    /// Known-bad implementations were compiled and run through the real worker path.
    ComparatorAndImplementation,
}

/// The two questions a mutation battery cannot answer about its own tolerance.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReductionCalibrationChecks {
    /// The tolerance was derived from a floor measured on this exact reference run.
    pub tolerance_measured: bool,
    /// A second legitimate summation order of the authority still passes that tolerance, so the
    /// gate is not tighter than the task's own numeric spread.
    pub reordered_authority_admitted: bool,
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
    /// Retained only so archived receipts still parse. It recorded the reference compared against
    /// itself, which is true by construction; nothing reads it and nothing writes it.
    #[serde(default, skip_serializing)]
    identity_passed: bool,
    #[serde(default = "ReductionBatteryScope::legacy")]
    battery_scope: ReductionBatteryScope,
    #[serde(default)]
    noise_floor: Option<ReductionNoiseFloor>,
    #[serde(default)]
    checks: ReductionCalibrationChecks,
    detections: Vec<ReductionMutationDetection>,
    #[serde(default)]
    undetected: Vec<ReductionMutantKind>,
    passed: bool,
}

impl ReductionBatteryScope {
    fn legacy() -> Self {
        Self::ComparatorOnly
    }
}

impl ReductionCalibrationReceipt {
    /// Recomputes the verdict from the facts the receipt records.
    ///
    /// A stored `passed: true` is the applicant's own word. Reading it back and believing it is how
    /// a calibration written under weaker rules keeps vouching for a gate those rules no longer
    /// justify, so the conjunction is evaluated on every read instead.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.passed
            && self.checks.tolerance_measured
            && self.checks.reordered_authority_admitted
            && self.undetected.is_empty()
    }

    #[must_use]
    pub fn detections(&self) -> &[ReductionMutationDetection] {
        &self.detections
    }

    /// Mutants this exact oracle, policy, and reference did **not** catch.
    ///
    /// A battery that reports nothing it misses has not found its own edge.
    #[must_use]
    pub fn undetected(&self) -> &[ReductionMutantKind] {
        &self.undetected
    }

    #[must_use]
    pub const fn battery_scope(&self) -> ReductionBatteryScope {
        self.battery_scope
    }

    #[must_use]
    pub const fn noise_floor(&self) -> Option<&ReductionNoiseFloor> {
        self.noise_floor.as_ref()
    }

    /// True when a second legitimate summation order of the authority passes this tolerance.
    ///
    /// False means the gate is tighter than the task's own numeric spread, and would reject a
    /// correct port for summing in a different order.
    #[must_use]
    pub const fn reordered_authority_admitted(&self) -> bool {
        self.checks.reordered_authority_admitted
    }

    /// True when the tolerance came from a floor measured on this exact reference run.
    #[must_use]
    pub const fn tolerance_measured(&self) -> bool {
        self.checks.tolerance_measured
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
    tolerance_plan_digest: Sha256Digest,
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
        tolerance_plan_digest: Sha256Digest,
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
            tolerance_plan_digest,
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
            tolerance_plan_digest,
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
    pub const fn tolerance_plan_digest(&self) -> Sha256Digest {
        self.tolerance_plan_digest
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

#[cfg(test)]
#[path = "correctness_tests.rs"]
mod tests;
