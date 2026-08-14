//! Deterministic source and build inspection for a Phase-1 migration intake.

use crate::{BundlePath, MigrationSpec, Sha256Digest};
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Stable evidence categories emitted by source inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionEvidenceKind {
    CudaKernel,
    CudaIndexing,
    BlockSynchronization,
    AtomicOperation,
    HostLaunch,
    RuntimeErrorPropagation,
    PublicDeclaration,
    PublicImplementation,
    CudaBuild,
    BuildSourceReference,
}

/// Stable failure categories emitted before candidate generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionFailureKind {
    MissingDeclaredFile,
    MissingCudaKernel,
    MissingCudaIndexing,
    MissingHostLaunch,
    MissingRuntimeErrorPropagation,
    MissingPublicDeclaration,
    MissingPublicImplementation,
    MissingCudaBuild,
    MissingBuildSourceReference,
    UnsupportedCudaConstruct,
}

/// One source location supporting an intake conclusion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InspectionEvidence {
    pub kind: InspectionEvidenceKind,
    pub path: BundlePath,
    pub line: usize,
    pub detail: String,
}

/// One deterministic reason why intake cannot advance to generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InspectionFailure {
    pub kind: InspectionFailureKind,
    pub path: Option<BundlePath>,
    pub line: Option<usize>,
    pub detail: String,
}

/// Complete deterministic result for one `MigrationSpec` and source bundle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationInspection {
    pub passed: bool,
    pub migration_spec_digest: Sha256Digest,
    pub declared_source_digest: Sha256Digest,
    pub inspected_files: usize,
    pub evidence: Vec<InspectionEvidence>,
    pub failures: Vec<InspectionFailure>,
}

/// Inspects the declared CUDA extension without compiling it or invoking an LLM.
///
/// The caller owns bundle materialization and supplies text under validated [`BundlePath`] keys.
/// Undeclared files may be present but cannot satisfy evidence required from the spec's declared
/// device, host, and build boundaries.
#[must_use]
pub fn inspect_migration_source(
    spec: &MigrationSpec,
    files: &BTreeMap<BundlePath, String>,
) -> MigrationInspection {
    let mut evidence = Vec::new();
    let mut failures = Vec::new();
    let declared = declared_paths(spec);

    for path in &declared {
        if !files.contains_key(path) {
            failures.push(InspectionFailure {
                kind: InspectionFailureKind::MissingDeclaredFile,
                path: Some(path.clone()),
                line: None,
                detail: "file declared by MigrationSpec is absent from the bundle".to_owned(),
            });
        }
    }

    let device_files = existing_files(spec.sources().device_sources(), files);
    let host_files = existing_files(spec.sources().host_sources(), files);
    let build_files = existing_files(spec.sources().build_files(), files);

    require_pattern(
        &device_files,
        &["__global__", "__device__"],
        InspectionEvidenceKind::CudaKernel,
        InspectionFailureKind::MissingCudaKernel,
        "CUDA device source declares no __global__ or __device__ function",
        &mut evidence,
        &mut failures,
    );
    require_pattern(
        &device_files,
        &["threadIdx", "blockIdx", "blockDim", "gridDim"],
        InspectionEvidenceKind::CudaIndexing,
        InspectionFailureKind::MissingCudaIndexing,
        "CUDA device source exposes no thread or block indexing semantics",
        &mut evidence,
        &mut failures,
    );
    optional_pattern(
        &device_files,
        &["__syncthreads", "__syncwarp"],
        InspectionEvidenceKind::BlockSynchronization,
        &mut evidence,
    );
    optional_pattern(
        &device_files,
        &["atomicAdd", "atomicMax", "atomicMin", "atomicCAS"],
        InspectionEvidenceKind::AtomicOperation,
        &mut evidence,
    );
    require_pattern(
        &host_files,
        &["<<<"],
        InspectionEvidenceKind::HostLaunch,
        InspectionFailureKind::MissingHostLaunch,
        "host source does not launch a CUDA kernel",
        &mut evidence,
        &mut failures,
    );
    require_pattern(
        &host_files,
        &[
            "cudaGetLastError",
            "cudaPeekAtLastError",
            "cudaDeviceSynchronize",
            "cudaStreamSynchronize",
        ],
        InspectionEvidenceKind::RuntimeErrorPropagation,
        InspectionFailureKind::MissingRuntimeErrorPropagation,
        "host launch path does not observe CUDA launch or synchronization errors",
        &mut evidence,
        &mut failures,
    );

    inspect_public_entry(spec, &host_files, &mut evidence, &mut failures);
    inspect_build(spec, &build_files, &mut evidence, &mut failures);
    inspect_unsupported_constructs(&device_files, &host_files, &mut failures);

    evidence.sort_by(|left, right| {
        (&left.kind, &left.path, left.line).cmp(&(&right.kind, &right.path, right.line))
    });
    failures.sort_by(|left, right| {
        (&left.kind, &left.path, left.line).cmp(&(&right.kind, &right.path, right.line))
    });

    MigrationInspection {
        passed: failures.is_empty(),
        migration_spec_digest: spec.digest(),
        declared_source_digest: digest_declared_sources(&declared, files),
        inspected_files: declared
            .iter()
            .filter(|path| files.contains_key(*path))
            .count(),
        evidence,
        failures,
    }
}

fn digest_declared_sources(
    declared: &BTreeSet<BundlePath>,
    files: &BTreeMap<BundlePath, String>,
) -> Sha256Digest {
    let mut context = Context::new(&SHA256);
    context.update(b"alloyport-declared-source-v1\0");
    context.update(&(declared.len() as u64).to_be_bytes());
    for path in declared {
        context.update(&(path.as_str().len() as u64).to_be_bytes());
        context.update(path.as_str().as_bytes());
        if let Some(contents) = files.get(path) {
            context.update(&[1]);
            context.update(&(contents.len() as u64).to_be_bytes());
            context.update(contents.as_bytes());
        } else {
            context.update(&[0]);
        }
    }
    let digest = context.finish();
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(digest.as_ref());
    Sha256Digest::from_bytes(bytes)
}

fn declared_paths(spec: &MigrationSpec) -> BTreeSet<BundlePath> {
    spec.sources()
        .device_sources()
        .iter()
        .chain(spec.sources().host_sources())
        .chain(spec.sources().build_files())
        .cloned()
        .collect()
}

fn existing_files<'a>(
    paths: &'a BTreeSet<BundlePath>,
    files: &'a BTreeMap<BundlePath, String>,
) -> Vec<(&'a BundlePath, &'a str)> {
    paths
        .iter()
        .filter_map(|path| files.get(path).map(|contents| (path, contents.as_str())))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn require_pattern(
    files: &[(&BundlePath, &str)],
    patterns: &[&str],
    evidence_kind: InspectionEvidenceKind,
    failure_kind: InspectionFailureKind,
    failure_detail: &str,
    evidence: &mut Vec<InspectionEvidence>,
    failures: &mut Vec<InspectionFailure>,
) {
    if let Some(found) = find_first(files, patterns) {
        evidence.push(InspectionEvidence {
            kind: evidence_kind,
            path: found.path.clone(),
            line: found.line,
            detail: format!("found `{}`", found.pattern),
        });
    } else {
        failures.push(InspectionFailure {
            kind: failure_kind,
            path: None,
            line: None,
            detail: failure_detail.to_owned(),
        });
    }
}

fn optional_pattern(
    files: &[(&BundlePath, &str)],
    patterns: &[&str],
    evidence_kind: InspectionEvidenceKind,
    evidence: &mut Vec<InspectionEvidence>,
) {
    if let Some(found) = find_first(files, patterns) {
        evidence.push(InspectionEvidence {
            kind: evidence_kind,
            path: found.path.clone(),
            line: found.line,
            detail: format!("found `{}`", found.pattern),
        });
    }
}

fn inspect_public_entry(
    spec: &MigrationSpec,
    host_files: &[(&BundlePath, &str)],
    evidence: &mut Vec<InspectionEvidence>,
    failures: &mut Vec<InspectionFailure>,
) {
    let symbol = spec.public_entry().symbol();
    let header_files: Vec<_> = host_files
        .iter()
        .copied()
        .filter(|(path, _)| is_header(path.as_str()))
        .collect();
    let occurrences = find_all(&header_files, symbol);
    if let Some(found) = occurrences.first() {
        evidence.push(InspectionEvidence {
            kind: InspectionEvidenceKind::PublicDeclaration,
            path: found.path.clone(),
            line: found.line,
            detail: format!("public symbol `{symbol}` is declared"),
        });
    } else {
        failures.push(InspectionFailure {
            kind: InspectionFailureKind::MissingPublicDeclaration,
            path: None,
            line: None,
            detail: format!("public symbol `{symbol}` is absent from host sources"),
        });
        return;
    }

    let implementation = host_files.iter().find_map(|(path, contents)| {
        if is_header(path.as_str()) {
            return None;
        }
        let has_symbol = contents.contains(symbol);
        let has_launch = contents.contains("<<<");
        (has_symbol && has_launch).then(|| PatternMatch {
            path,
            line: line_number(contents, symbol),
            pattern: symbol,
        })
    });
    if let Some(found) = implementation {
        evidence.push(InspectionEvidence {
            kind: InspectionEvidenceKind::PublicImplementation,
            path: found.path.clone(),
            line: found.line,
            detail: format!("public symbol `{symbol}` owns the CUDA launch path"),
        });
    } else {
        failures.push(InspectionFailure {
            kind: InspectionFailureKind::MissingPublicImplementation,
            path: None,
            line: None,
            detail: format!("public symbol `{symbol}` is not connected to a CUDA launch"),
        });
    }
}

fn inspect_build(
    spec: &MigrationSpec,
    build_files: &[(&BundlePath, &str)],
    evidence: &mut Vec<InspectionEvidence>,
    failures: &mut Vec<InspectionFailure>,
) {
    require_pattern(
        build_files,
        &[
            "LANGUAGES CXX CUDA",
            "LANGUAGES CUDA",
            "enable_language(CUDA)",
        ],
        InspectionEvidenceKind::CudaBuild,
        InspectionFailureKind::MissingCudaBuild,
        "build files do not enable the CUDA language",
        evidence,
        failures,
    );

    for path in spec
        .sources()
        .device_sources()
        .iter()
        .chain(spec.sources().host_sources())
        .filter(|path| is_compilation_unit(path.as_str()))
    {
        let file_name = path.as_str().rsplit('/').next().unwrap_or(path.as_str());
        if let Some(found) = find_first(build_files, &[file_name]) {
            evidence.push(InspectionEvidence {
                kind: InspectionEvidenceKind::BuildSourceReference,
                path: found.path.clone(),
                line: found.line,
                detail: format!("build references `{}`", path.as_str()),
            });
        } else {
            failures.push(InspectionFailure {
                kind: InspectionFailureKind::MissingBuildSourceReference,
                path: Some(path.clone()),
                line: None,
                detail: format!(
                    "no build file references compilation unit `{}`",
                    path.as_str()
                ),
            });
        }
    }
}

fn is_compilation_unit(path: &str) -> bool {
    [".cu", ".c", ".cc", ".cpp", ".cxx"]
        .iter()
        .any(|extension| path.ends_with(extension))
}

fn is_header(path: &str) -> bool {
    [".h", ".hh", ".hpp", ".cuh"]
        .iter()
        .any(|extension| path.ends_with(extension))
}

fn inspect_unsupported_constructs(
    device_files: &[(&BundlePath, &str)],
    host_files: &[(&BundlePath, &str)],
    failures: &mut Vec<InspectionFailure>,
) {
    const UNSUPPORTED: [(&str, &str); 10] = [
        ("cooperative_groups", "cooperative groups"),
        ("cudaGraph", "CUDA Graphs"),
        ("asm(", "inline PTX or assembly"),
        ("asm volatile", "inline PTX or assembly"),
        ("texture<", "CUDA texture references"),
        ("surface<", "CUDA surface references"),
        ("nvrtc", "runtime compilation"),
        ("cudaStreamCreate", "owned non-default streams"),
        ("cudaEventCreate", "CUDA events"),
        ("cudaLaunchCooperativeKernel", "cooperative launch"),
    ];

    let all_files: Vec<_> = device_files.iter().chain(host_files).copied().collect();
    for (pattern, construct) in UNSUPPORTED {
        for found in find_all(&all_files, pattern) {
            failures.push(InspectionFailure {
                kind: InspectionFailureKind::UnsupportedCudaConstruct,
                path: Some(found.path.clone()),
                line: Some(found.line),
                detail: format!("Phase-1 intake contains unsupported {construct}: `{pattern}`"),
            });
        }
    }
}

struct PatternMatch<'a> {
    path: &'a BundlePath,
    line: usize,
    pattern: &'a str,
}

fn find_first<'a>(
    files: &[(&'a BundlePath, &'a str)],
    patterns: &'a [&'a str],
) -> Option<PatternMatch<'a>> {
    files.iter().find_map(|(path, contents)| {
        patterns.iter().find_map(|pattern| {
            contents.contains(pattern).then(|| PatternMatch {
                path,
                line: line_number(contents, pattern),
                pattern,
            })
        })
    })
}

fn find_all<'a>(files: &[(&'a BundlePath, &'a str)], pattern: &'a str) -> Vec<PatternMatch<'a>> {
    files
        .iter()
        .filter(|(_, contents)| contents.contains(pattern))
        .map(|(path, contents)| PatternMatch {
            path,
            line: line_number(contents, pattern),
            pattern,
        })
        .collect()
}

fn line_number(contents: &str, pattern: &str) -> usize {
    contents.find(pattern).map_or(1, |offset| {
        contents[..offset]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AscendTarget, CudaSourceSet, PublicEntryPoint, ReferenceWorkload};

    fn path(value: &str) -> BundlePath {
        BundlePath::try_from(value).expect("valid path")
    }

    fn spec() -> MigrationSpec {
        MigrationSpec::new_v1(
            "source-sha",
            CudaSourceSet::new(
                [path("src/kernel.cu")],
                [path("include/api.h"), path("src/launch.cu")],
                [path("CMakeLists.txt")],
            )
            .expect("source set"),
            PublicEntryPoint::new("reduce_sum", "sum contiguous fp32", "reduce_sum_candidate")
                .expect("public entry"),
            ReferenceWorkload::new(path("."), ["./build/test".to_owned()]).expect("reference"),
            AscendTarget::new("Ascend950PR", "9.1", "ccec", "25.7", "acl").expect("target"),
            "contiguous fp32",
            Vec::<String>::new(),
            "return unsupported",
        )
        .expect("spec")
    }

    fn valid_files() -> BTreeMap<BundlePath, String> {
        BTreeMap::from([
            (
                path("src/kernel.cu"),
                "__global__ void kernel(float *x) { auto i = blockIdx.x * blockDim.x + threadIdx.x; __syncthreads(); atomicAdd(x, i); }".to_owned(),
            ),
            (
                path("include/api.h"),
                "extern \"C\" int reduce_sum(const float *, unsigned long, float *);".to_owned(),
            ),
            (
                path("src/launch.cu"),
                "extern \"C\" int reduce_sum(const float *x, unsigned long n, float *y) { kernel<<<1, 32>>>(y); return cudaDeviceSynchronize(); }".to_owned(),
            ),
            (
                path("CMakeLists.txt"),
                "project(reduce LANGUAGES CXX CUDA)\nadd_library(reduce src/kernel.cu src/launch.cu)".to_owned(),
            ),
        ])
    }

    #[test]
    fn valid_cuda_extension_emits_structural_evidence() {
        let report = inspect_migration_source(&spec(), &valid_files());
        assert!(report.passed, "{:?}", report.failures);
        assert_eq!(report.inspected_files, 4);
        assert!(
            report
                .evidence
                .iter()
                .any(|item| item.kind == InspectionEvidenceKind::BlockSynchronization)
        );
        assert!(
            report
                .evidence
                .iter()
                .any(|item| item.kind == InspectionEvidenceKind::PublicImplementation)
        );
    }

    #[test]
    fn missing_launch_and_build_reference_fail_before_generation() {
        let mut files = valid_files();
        files.insert(
            path("src/launch.cu"),
            "int reduce_sum() { return 0; }".to_owned(),
        );
        files.insert(
            path("CMakeLists.txt"),
            "project(reduce LANGUAGES CXX CUDA)\nadd_library(reduce src/kernel.cu)".to_owned(),
        );

        let report = inspect_migration_source(&spec(), &files);
        assert!(!report.passed);
        assert!(has_failure(
            &report,
            InspectionFailureKind::MissingHostLaunch
        ));
        assert!(has_failure(
            &report,
            InspectionFailureKind::MissingPublicImplementation
        ));
        assert!(has_failure(
            &report,
            InspectionFailureKind::MissingBuildSourceReference
        ));
    }

    #[test]
    fn unsupported_construct_is_an_explicit_intake_failure() {
        let mut files = valid_files();
        files
            .get_mut(&path("src/kernel.cu"))
            .expect("kernel")
            .push_str("\n#include <cooperative_groups.h>\n");

        let report = inspect_migration_source(&spec(), &files);
        assert!(!report.passed);
        assert!(has_failure(
            &report,
            InspectionFailureKind::UnsupportedCudaConstruct
        ));
    }

    #[test]
    fn inspection_identity_changes_when_declared_source_changes() {
        let files = valid_files();
        let first = inspect_migration_source(&spec(), &files);
        let mut changed = files;
        changed
            .get_mut(&path("src/kernel.cu"))
            .expect("kernel")
            .push('\n');
        let second = inspect_migration_source(&spec(), &changed);

        assert_eq!(first.migration_spec_digest, second.migration_spec_digest);
        assert_ne!(first.declared_source_digest, second.declared_source_digest);
    }

    #[test]
    fn absent_declared_file_cannot_be_satisfied_by_an_undeclared_copy() {
        let mut files = valid_files();
        let kernel = files.remove(&path("src/kernel.cu")).expect("kernel");
        files.insert(path("src/other.cu"), kernel);

        let report = inspect_migration_source(&spec(), &files);
        assert!(!report.passed);
        assert!(has_failure(
            &report,
            InspectionFailureKind::MissingDeclaredFile
        ));
        assert!(has_failure(
            &report,
            InspectionFailureKind::MissingCudaKernel
        ));
    }

    fn has_failure(report: &MigrationInspection, kind: InspectionFailureKind) -> bool {
        report.failures.iter().any(|failure| failure.kind == kind)
    }
}
