//! Calibrates the frozen reduction oracle against one exact CUDA reference receipt.

use alloyport_core::{
    ReductionCorpus, ReductionRunReceipt, ReductionTolerancePlan, calibrate_reduction_oracle,
    measure_reduction_noise_floor,
};
use std::error::Error;
use std::fs;

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let [receipt_path, output_path] = arguments.as_slice() else {
        return Err("usage: calibrate_reduction_receipt RUN_RECEIPT OUTPUT_JSON".into());
    };
    let receipt: ReductionRunReceipt = serde_json::from_slice(&fs::read(receipt_path)?)?;
    let calibration = calibrate_reduction_oracle(
        &receipt,
        &ReductionTolerancePlan::fixture_v1(),
        &ReductionCorpus::fixture_v1(),
    )?;
    let bytes = serde_json::to_vec(&calibration)?;
    fs::write(output_path, &bytes)?;
    println!("calibration_digest={}", calibration.digest()?);
    println!("passed={}", calibration.passed());
    match measure_reduction_noise_floor(&receipt, &ReductionCorpus::fixture_v1()) {
        Ok(floor) => println!(
            "measured_floor absolute_nanos={} relative_ppb={} repetition_pairs={} \
             reorder_pairs={} deterministic={}",
            floor.observed_absolute_nanos(),
            floor.observed_relative_ppb(),
            floor.repetition_pairs(),
            floor.reorder_pairs(),
            floor.deterministic(),
        ),
        Err(error) => println!("measured_floor unavailable: {error}"),
    }
    if !calibration.undetected().is_empty() {
        println!("undetected={:?}", calibration.undetected());
    }
    Ok(())
}
