//! Performance evidence: what a timing claim must carry before anything may act on it.
//!
//! This is the domain, not an execution path. Nothing here runs a workload; it decides whether a
//! set of timings said anything. That order is deliberate — the rules below each exist because a
//! measurement without them has already been believed somewhere and turned out to be empty.
//!
//! Three rules do the work:
//!
//! - **A number without a spread is not a measurement.** The correctness oracle in this repository
//!   shipped a tolerance nobody had measured, and the authority it judged against turned out to
//!   disagree with itself by twenty times that tolerance's absolute term. Timing is far noisier
//!   than fp32 accumulation.
//! - **When the noise is larger than the effect, there is no result.** Not a small result — no
//!   result. A gate that reports the difference of two medians without their spread will call
//!   scheduler jitter an optimization.
//! - **A proxy is only evidence where the thing it proxies for is the bottleneck.** Launch counts,
//!   instruction counts, and occupancy are claims about a mechanism. They can support a diagnosis;
//!   they cannot establish that anything got faster, and nothing here lets them try.

use crate::Sha256Digest;
use serde::{Deserialize, Serialize};

pub const PERFORMANCE_RECEIPT_SCHEMA_V1: u16 = 1;

/// Below this a summary describes the scheduler, not the implementation.
pub const MINIMUM_TIMED_SAMPLES: usize = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PerformanceError {
    TooFewSamples,
    EmptySample,
    Serialization,
}

impl std::fmt::Display for PerformanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooFewSamples => write!(
                formatter,
                "a timing summary needs at least {MINIMUM_TIMED_SAMPLES} samples to have a spread"
            ),
            Self::EmptySample => write!(formatter, "a timed sample cannot be zero"),
            Self::Serialization => write!(formatter, "cannot encode performance evidence"),
        }
    }
}

impl std::error::Error for PerformanceError {}

/// Timings for one implementation, kept with their spread and with the raw samples.
///
/// The samples are retained rather than reduced away: a summary that discards them cannot be
/// re-judged under a different rule, and every rule here has already been rewritten once.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredDuration {
    samples_nanos: Vec<u64>,
    discarded_warmup_samples: u32,
    min_nanos: u64,
    median_nanos: u64,
    max_nanos: u64,
    relative_spread_ppb: u64,
}

impl MeasuredDuration {
    /// Summarizes timed samples taken after the recorded warmup was discarded.
    ///
    /// # Errors
    ///
    /// Returns an error for fewer than [`MINIMUM_TIMED_SAMPLES`] samples or a zero sample.
    pub fn from_samples(
        samples_nanos: impl IntoIterator<Item = u64>,
        discarded_warmup_samples: u32,
    ) -> Result<Self, PerformanceError> {
        let mut samples: Vec<u64> = samples_nanos.into_iter().collect();
        if samples.len() < MINIMUM_TIMED_SAMPLES {
            return Err(PerformanceError::TooFewSamples);
        }
        if samples.contains(&0) {
            return Err(PerformanceError::EmptySample);
        }
        let raw = samples.clone();
        samples.sort_unstable();
        let min_nanos = samples[0];
        let max_nanos = samples[samples.len() - 1];
        let median_nanos = samples[samples.len() / 2];
        let relative_spread_ppb = u64::try_from(
            u128::from(max_nanos - min_nanos) * 1_000_000_000 / u128::from(median_nanos),
        )
        .unwrap_or(u64::MAX);
        Ok(Self {
            samples_nanos: raw,
            discarded_warmup_samples,
            min_nanos,
            median_nanos,
            max_nanos,
            relative_spread_ppb,
        })
    }

    #[must_use]
    pub const fn median_nanos(&self) -> u64 {
        self.median_nanos
    }

    /// Half the observed range: what this measurement cannot distinguish.
    #[must_use]
    pub const fn noise_band_nanos(&self) -> u64 {
        (self.max_nanos - self.min_nanos) / 2
    }

    #[must_use]
    pub const fn relative_spread_ppb(&self) -> u64 {
        self.relative_spread_ppb
    }

    #[must_use]
    pub fn samples(&self) -> &[u64] {
        &self.samples_nanos
    }
}

/// What a timing claim was measured against.
///
/// A comparison that cannot say this is not a comparison. The variants are ordered by what they can
/// establish, and [`Self::Proxy`] can establish nothing about speed at all.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceBaseline {
    /// An earlier revision of this implementation, on the same worker and the same device.
    PreviousRevision,
    /// Another candidate measured in the same session on the same device.
    SiblingCandidate,
    /// A ceiling derived from a probe run on this hardware. None exists yet.
    MeasuredRoofline,
    /// A mechanism counter — launches, instructions, occupancy — rather than elapsed time.
    Proxy,
}

/// Why a comparison could not be judged.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceRefusal {
    /// The two sides ran in different environments, so the difference includes the hardware.
    CrossEnvironmentComparison,
    /// A mechanism counter cannot establish that elapsed time changed.
    ProxyCannotEstablishSpeed,
}

/// Four-way performance semantics. Only `Improved` may promote anything.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PerformanceVerdict {
    Improved,
    Regressed,
    /// The difference did not clear the noise. This is not a small improvement; it is nothing.
    NoResult,
    Unverifiable,
}

/// One judged comparison, carrying both sides and the rule that decided it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceReceipt {
    schema_version: u16,
    baseline: PerformanceBaseline,
    baseline_environment_digest: Sha256Digest,
    candidate_environment_digest: Sha256Digest,
    baseline_duration: MeasuredDuration,
    candidate_duration: MeasuredDuration,
    verdict: PerformanceVerdict,
    /// Change in the median, as parts per billion of the baseline median. Negative is faster.
    median_change_ppb: i64,
    /// What the two measurements together cannot distinguish, in the same units.
    combined_noise_ppb: u64,
    refusals: Vec<PerformanceRefusal>,
}

impl PerformanceReceipt {
    #[must_use]
    pub const fn verdict(&self) -> PerformanceVerdict {
        self.verdict
    }

    #[must_use]
    pub const fn median_change_ppb(&self) -> i64 {
        self.median_change_ppb
    }

    #[must_use]
    pub const fn combined_noise_ppb(&self) -> u64 {
        self.combined_noise_ppb
    }

    #[must_use]
    pub fn refusals(&self) -> &[PerformanceRefusal] {
        &self.refusals
    }

    /// Computes the canonical receipt identity.
    ///
    /// # Errors
    ///
    /// Returns an error only if serialization fails.
    pub fn digest(&self) -> Result<Sha256Digest, PerformanceError> {
        serde_json::to_vec(self)
            .map(|bytes| Sha256Digest::digest_bytes(&bytes))
            .map_err(|_| PerformanceError::Serialization)
    }
}

/// Inputs for one judged comparison.
#[derive(Clone, Debug)]
pub struct PerformanceComparison {
    pub baseline: PerformanceBaseline,
    pub baseline_environment_digest: Sha256Digest,
    pub candidate_environment_digest: Sha256Digest,
    pub baseline_duration: MeasuredDuration,
    pub candidate_duration: MeasuredDuration,
}

/// Judges one comparison against the three rules in this module's header.
#[must_use]
pub fn judge_performance(comparison: PerformanceComparison) -> PerformanceReceipt {
    let mut refusals = Vec::new();
    if comparison.baseline == PerformanceBaseline::Proxy {
        refusals.push(PerformanceRefusal::ProxyCannotEstablishSpeed);
    }
    if comparison.baseline_environment_digest != comparison.candidate_environment_digest {
        // The difference between a kernel on one accelerator and a kernel on another is mostly the
        // accelerators. A migration's speedup is not measured across the migration.
        refusals.push(PerformanceRefusal::CrossEnvironmentComparison);
    }

    let baseline_median = comparison.baseline_duration.median_nanos();
    let candidate_median = comparison.candidate_duration.median_nanos();
    let median_change_ppb =
        i64::try_from(ratio_ppb(candidate_median, baseline_median).saturating_sub(1_000_000_000))
            .unwrap_or(i64::MAX);
    // What the pair cannot distinguish: each side's own range, expressed against the baseline.
    let combined_noise_ppb = scale_ppb(
        comparison
            .baseline_duration
            .noise_band_nanos()
            .saturating_add(comparison.candidate_duration.noise_band_nanos()),
        baseline_median,
    );

    let verdict = if refusals.is_empty() {
        let effect = median_change_ppb.unsigned_abs();
        if effect <= combined_noise_ppb {
            PerformanceVerdict::NoResult
        } else if median_change_ppb < 0 {
            PerformanceVerdict::Improved
        } else {
            PerformanceVerdict::Regressed
        }
    } else {
        PerformanceVerdict::Unverifiable
    };

    refusals.sort_unstable();
    PerformanceReceipt {
        schema_version: PERFORMANCE_RECEIPT_SCHEMA_V1,
        baseline: comparison.baseline,
        baseline_environment_digest: comparison.baseline_environment_digest,
        candidate_environment_digest: comparison.candidate_environment_digest,
        baseline_duration: comparison.baseline_duration,
        candidate_duration: comparison.candidate_duration,
        verdict,
        median_change_ppb,
        combined_noise_ppb,
        refusals,
    }
}

fn ratio_ppb(value: u64, reference: u64) -> i128 {
    if reference == 0 {
        return 0;
    }
    i128::from(value) * 1_000_000_000 / i128::from(reference)
}

fn scale_ppb(value: u64, reference: u64) -> u64 {
    if reference == 0 {
        return u64::MAX;
    }
    u64::try_from(u128::from(value) * 1_000_000_000 / u128::from(reference)).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "performance_tests.rs"]
mod tests;
