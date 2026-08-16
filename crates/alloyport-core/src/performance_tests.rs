use super::*;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn duration(samples: impl IntoIterator<Item = u64>) -> MeasuredDuration {
    MeasuredDuration::from_samples(samples, 3).expect("summary")
}

fn same_environment(
    baseline: MeasuredDuration,
    candidate: MeasuredDuration,
) -> PerformanceComparison {
    PerformanceComparison {
        baseline: PerformanceBaseline::PreviousRevision,
        baseline_environment_digest: digest("ascend-worker-1"),
        candidate_environment_digest: digest("ascend-worker-1"),
        baseline_duration: baseline,
        candidate_duration: candidate,
    }
}

#[test]
fn a_single_timing_is_not_a_measurement() {
    assert_eq!(
        MeasuredDuration::from_samples([1_000_000], 0),
        Err(PerformanceError::TooFewSamples)
    );
    assert_eq!(
        MeasuredDuration::from_samples([1, 2, 3, 4], 0),
        Err(PerformanceError::TooFewSamples)
    );
    assert!(MeasuredDuration::from_samples([1, 2, 3, 4, 5], 0).is_ok());
}

#[test]
fn a_summary_keeps_its_samples_so_a_later_rule_can_rejudge_it() {
    let measured = duration([120, 100, 110, 130, 105]);
    assert_eq!(measured.samples(), &[120, 100, 110, 130, 105]);
    assert_eq!(measured.median_nanos(), 110);
    assert_eq!(measured.noise_band_nanos(), 15);
}

#[test]
fn an_effect_inside_the_noise_is_no_result_rather_than_a_small_win() {
    // Two percent faster in the medians, with each side ranging over ten percent. A gate that
    // subtracts medians and stops there would report this as an optimization.
    let baseline = duration([1_000, 1_050, 1_100, 950, 1_020]);
    let candidate = duration([980, 1_030, 1_080, 930, 1_000]);
    let receipt = judge_performance(same_environment(baseline, candidate));
    assert_eq!(receipt.verdict(), PerformanceVerdict::NoResult);
    assert!(
        receipt.median_change_ppb().unsigned_abs() <= receipt.combined_noise_ppb(),
        "the rule is that the effect did not clear the noise, and the receipt must show both"
    );
}

#[test]
fn an_effect_that_clears_the_noise_is_a_result() {
    let baseline = duration([1_000, 1_010, 1_005, 995, 1_002]);
    let candidate = duration([500, 505, 502, 498, 501]);
    let receipt = judge_performance(same_environment(baseline, candidate));
    assert_eq!(receipt.verdict(), PerformanceVerdict::Improved);
    assert!(receipt.median_change_ppb() < 0);

    let regressed = judge_performance(same_environment(
        duration([500, 505, 502, 498, 501]),
        duration([1_000, 1_010, 1_005, 995, 1_002]),
    ));
    assert_eq!(regressed.verdict(), PerformanceVerdict::Regressed);
    assert!(regressed.median_change_ppb() > 0);
}

#[test]
fn a_proxy_can_never_establish_that_anything_got_faster() {
    // Ninety percent fewer launches, measured with no noise at all. The mechanism moved; whether
    // the wall moved is a different question, and this one is not evidence for it.
    let mut comparison = same_environment(
        duration([1_000, 1_000, 1_000, 1_000, 1_000]),
        duration([100, 100, 100, 100, 100]),
    );
    comparison.baseline = PerformanceBaseline::Proxy;
    let receipt = judge_performance(comparison);
    assert_eq!(receipt.verdict(), PerformanceVerdict::Unverifiable);
    assert!(
        receipt
            .refusals()
            .contains(&PerformanceRefusal::ProxyCannotEstablishSpeed)
    );
}

#[test]
fn a_migration_speedup_is_not_measured_across_the_migration() {
    // The candidate on the accelerator against the reference on the GPU. Most of that difference is
    // the two chips, and none of it says the port is good.
    let comparison = PerformanceComparison {
        baseline: PerformanceBaseline::PreviousRevision,
        baseline_environment_digest: digest("cuda-worker-gb10"),
        candidate_environment_digest: digest("ascend-worker-950pr"),
        baseline_duration: duration([1_000, 1_010, 1_005, 995, 1_002]),
        candidate_duration: duration([500, 505, 502, 498, 501]),
    };
    let receipt = judge_performance(comparison);
    assert_eq!(receipt.verdict(), PerformanceVerdict::Unverifiable);
    assert!(
        receipt
            .refusals()
            .contains(&PerformanceRefusal::CrossEnvironmentComparison)
    );
}

#[test]
fn a_zero_sample_is_a_broken_clock_not_an_instant_kernel() {
    assert_eq!(
        MeasuredDuration::from_samples([1_000, 0, 1_000, 1_000, 1_000], 0),
        Err(PerformanceError::EmptySample)
    );
}
