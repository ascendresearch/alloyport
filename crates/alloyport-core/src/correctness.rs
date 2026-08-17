//! Independent reduction correctness, oracle calibration, and execution-port contracts.

use crate::correctness_attempt::ReductionCorrectnessError;
use crate::{CandidateId, ReductionCorpus, Sha256Digest, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[path = "correctness_calibration.rs"]
mod calibration;
#[path = "correctness_evaluation.rs"]
mod evaluation;
#[path = "correctness_mutation.rs"]
mod mutation;
#[path = "correctness_tolerance.rs"]
mod tolerance;

pub use calibration::{calibrate_reduction_oracle, measure_reduction_noise_floor};
pub use evaluation::evaluate_reduction_correctness;
pub use tolerance::{ReductionOraclePolicy, ReductionTolerancePlan, ToleranceProvenance};

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
///
/// `implementation_invoked` and `synchronized` are **runner attestations, not observations**: the
/// trusted harness emits both as literals, so no real candidate can move them and a candidate that
/// bypassed its own implementation would report them `true`. The oracle still refuses a receipt
/// that admits either is false — an admission is worth acting on — but their being `true` proves
/// nothing, and [`ReductionCorrectnessReceipt::unverified`] says so on every verdict.
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

/// Deliberate non-equivalent defects the reduction oracle must reject before use.
///
/// Every kind here perturbs a **run receipt**, not an implementation. That bounds what the battery
/// can prove: it exercises the comparator's own arithmetic and bookkeeping, and it says nothing
/// about whether the surrounding pipeline would catch a genuinely broken kernel. See
/// [`ReductionBatteryScope`], which records that limitation inside the receipt.
///
/// Two kinds were removed rather than kept: they flipped `implementation_invoked` and
/// `synchronized`, which the trusted runner emits as literals. No real candidate can move either,
/// so the comparator caught them every time and the battery counted two guaranteed detections it
/// had not earned. A mutant that cannot occur inflates the pass rate of the battery that contains
/// it. The comparator's handling of those admissions is still covered, by a unit test that does not
/// claim to be calibration.
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
    Nondeterminism,
    /// Retired. Kept so archived calibration receipts still parse; never produced. These flipped
    /// `implementation_invoked` and `synchronized`, which the trusted runner emits as literals.
    FallbackBypass,
    /// Retired for the same reason as [`Self::FallbackBypass`].
    MissingSynchronization,
    /// A perturbation just above the configured tolerance. The battery is worthless if this
    /// survives: every other mutant is orders of magnitude larger, so ten of them can pass at a
    /// tolerance a hundred times too loose.
    JustOutsideTolerance,
}

impl ReductionMutantKind {
    pub const ALL: [Self; 9] = [
        Self::ArithmeticScale,
        Self::IndexingSwap,
        Self::BoundaryMask,
        Self::AccumulationError,
        Self::InvalidStatus,
        Self::SignedZero,
        Self::NonFinite,
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

/// Something a verdict rests on and did not check.
///
/// A gate that cannot state its own blind spots invites every reader to assume it has none. These
/// travel with the verdict rather than living in a design document nobody opens while reading a
/// receipt, and the list shrinks when an observation replaces an assumption.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnverifiedAssumption {
    /// Nothing observed that the candidate executed on the accelerator. Plain host C++ that sums
    /// the input compiles on the Ascend build worker, links, is called through the same ABI, and
    /// matches the authority exactly. The Source Gate blocks delegating to a framework or a
    /// prebuilt operator; it does not, and by itself cannot, establish that a device was used.
    DeviceExecution,
    /// Invocation and synchronization are attested by the trusted runner rather than observed.
    RunnerAttestation,
}

/// What no run through this oracle has yet verified.
///
/// Constant until something observes it. It is derived rather than written by hand so that adding
/// the observation is what removes the entry.
fn unverified_assumptions() -> Vec<UnverifiedAssumption> {
    vec![
        UnverifiedAssumption::DeviceExecution,
        UnverifiedAssumption::RunnerAttestation,
    ]
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
    #[serde(default)]
    unverified: Vec<UnverifiedAssumption>,
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

    /// What this verdict rests on and did not check. A `Pass` carries these too.
    #[must_use]
    pub fn unverified(&self) -> &[UnverifiedAssumption] {
        &self.unverified
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

#[cfg(test)]
#[path = "correctness_tests.rs"]
mod tests;
