//! Emits a domain-validated CUDA reference bundle for direct hardware diagnostics.
//!
//! This example does not create Build Gate authority and must not be used as release evidence.

use alloyport_core::{
    BundlePath, CandidateId, ReductionCorpus, ReductionCorrectnessExperiment,
    ReductionExecutionBundle, ReductionExecutionFile, ReductionOraclePolicy, ReductionRunRole,
    Sha256Digest, TaskId,
};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let [source_root, output] = arguments.as_slice() else {
        return Err("usage: reduction_reference_bundle SOURCE_ROOT OUTPUT_JSON".into());
    };
    let source_root = PathBuf::from(source_root);
    let corpus = ReductionCorpus::fixture_v1();
    let experiment = ReductionCorrectnessExperiment::new(
        TaskId::try_from("diagnostic-cuda-reference")?,
        CandidateId::try_from("diagnostic-cuda-reference")?,
        digest("diagnostic-migration-spec"),
        digest("diagnostic-candidate-manifest"),
        digest("diagnostic-source-gate"),
        digest("diagnostic-build-gate"),
        corpus.digest()?,
        ReductionOraclePolicy::fixture_v1().digest()?,
    );
    let mut paths = Vec::new();
    collect_files(&source_root, &source_root, &mut paths)?;
    paths.sort();
    let files = paths
        .into_iter()
        .map(|path| {
            let relative = path.strip_prefix(&source_root)?;
            let bundle_path = format!("input/{}", relative.to_string_lossy());
            Ok(ReductionExecutionFile::new(
                BundlePath::try_from(bundle_path)?,
                fs::read_to_string(path)?,
            )?)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let bundle =
        ReductionExecutionBundle::new(experiment, ReductionRunRole::CudaReference, corpus, files)?;
    let bytes = serde_json::to_vec(&bundle)?;
    fs::write(output, &bytes)?;
    println!("bundle_digest={}", Sha256Digest::digest_bytes(&bytes));
    println!("implementation_digest={}", bundle.implementation_digest());
    Ok(())
}

fn collect_files(
    root: &Path,
    directory: &Path,
    paths: &mut Vec<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(format!("source tree contains symlink: {}", entry.path().display()).into());
        }
        if file_type.is_dir() {
            collect_files(root, &entry.path(), paths)?;
        } else if file_type.is_file() {
            let path = entry.path();
            path.strip_prefix(root)?;
            paths.push(path);
        } else {
            return Err(format!(
                "source tree contains special file: {}",
                entry.path().display()
            )
            .into());
        }
    }
    Ok(())
}

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}
