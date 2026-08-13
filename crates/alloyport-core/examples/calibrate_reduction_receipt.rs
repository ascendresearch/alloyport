//! Calibrates the frozen reduction oracle against one exact CUDA reference receipt.

use alloyport_core::{
    ReductionCorpus, ReductionOraclePolicy, ReductionRunReceipt, calibrate_reduction_oracle,
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
        &ReductionOraclePolicy::fixture_v1(),
        &ReductionCorpus::fixture_v1(),
    )?;
    let bytes = serde_json::to_vec(&calibration)?;
    fs::write(output_path, &bytes)?;
    println!("calibration_digest={}", calibration.digest()?);
    println!("passed={}", calibration.passed());
    Ok(())
}
