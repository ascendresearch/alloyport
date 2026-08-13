use super::*;

fn file(path: &str, kind: GeneratedSourceKind, contents: &str) -> CandidateSourceFile {
    CandidateSourceFile::new(
        BundlePath::try_from(path).expect("path"),
        kind,
        ArtifactDescriptor {
            digest: Sha256Digest::digest_bytes(contents.as_bytes()),
            size_bytes: contents.len() as u64,
            media_type: "text/plain; charset=utf-8".to_owned(),
        },
    )
    .expect("source file")
}

fn source_set(device: &str) -> (CandidateSourceManifest, BTreeMap<BundlePath, Vec<u8>>) {
    let host = "extern \"C\" int alloyport_reduce_sum_f32(const float *input, size_t elements, float *output) { ACLRT_LAUNCH_KERNEL(reduce_sum); }";
    let build = "add_library(alloyport_reduction_candidate reduce.cpp host.cpp)";
    let mapping = "input/kernel.cu -> generated/reduce.cpp\ninput/host.cu -> generated/host.cpp\ngenerated/CMakeLists.txt";
    let files = vec![
        file(
            "generated/reduce.cpp",
            GeneratedSourceKind::AscendCDevice,
            device,
        ),
        file("generated/host.cpp", GeneratedSourceKind::AscendHost, host),
        file(
            "generated/CMakeLists.txt",
            GeneratedSourceKind::BuildIntegration,
            build,
        ),
        file(
            "generated/component-map.txt",
            GeneratedSourceKind::ComponentMapping,
            mapping,
        ),
    ];
    let manifest = CandidateSourceManifest::new(CandidateSourceManifestSpec {
        candidate_id: CandidateId::try_from("candidate-source-1").expect("candidate ID"),
        task_id: TaskId::try_from("task-source-1").expect("task ID"),
        parent_candidate_id: None,
        migration_spec_digest: Sha256Digest::digest_bytes(b"spec"),
        generation_strategy: GenerationStrategy::DirectAscendC,
        public_symbol: "alloyport_reduce_sum_f32".to_owned(),
        input_source_paths: ["input/kernel.cu", "input/host.cu"]
            .into_iter()
            .map(|path| BundlePath::try_from(path).expect("input path"))
            .collect(),
        source_bundle_digest: Sha256Digest::digest_bytes(b"bundle"),
        files,
    })
    .expect("manifest");
    let sources = manifest
        .files()
        .iter()
        .map(|file| {
            let bytes = match file.kind() {
                GeneratedSourceKind::AscendCDevice => device.as_bytes(),
                GeneratedSourceKind::AscendHost => host.as_bytes(),
                GeneratedSourceKind::BuildIntegration => build.as_bytes(),
                GeneratedSourceKind::ComponentMapping => mapping.as_bytes(),
            };
            (file.path().clone(), bytes.to_vec())
        })
        .collect();
    (manifest, sources)
}

#[test]
fn source_gate_accepts_structural_ascend_c_and_complete_mapping() {
    let device = "#include <kernel_operator.h>\nextern \"C\" __global__ __aicore__ void reduce_sum(GM_ADDR x) {}";
    let (manifest, sources) = source_set(device);
    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(receipt.passed(), "{:?}", receipt.failures());
}

#[test]
fn source_gate_independently_rejects_framework_fallback_and_missing_kernel() {
    let device = "#include <torch/extension.h>\nauto reduce() { return at::sum(input); }";
    let (manifest, sources) = source_set(device);
    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(!receipt.passed());
    let kinds: BTreeSet<_> = receipt
        .failures()
        .iter()
        .map(|failure| failure.kind)
        .collect();
    assert!(kinds.contains(&SourceGateFailureKind::ForbiddenFallback));
    assert!(kinds.contains(&SourceGateFailureKind::MissingAscendCKernel));
}
