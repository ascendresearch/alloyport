//! How a reduction tolerance is derived, and where it came from.
//!
//! Split out of `correctness.rs` for the module-size limit. It stays a child module so the
//! derivation can read a measured floor's own fields rather than widening them to public.

use super::{ReductionCorrectnessError, ReductionNoiseFloor, Sha256Digest};
use serde::{Deserialize, Serialize};

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
