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
    source_set_with_host(
        device,
        "extern \"C\" int alloyport_reduce_sum_f32(const float *input, size_t elements, float *output) { ACLRT_LAUNCH_KERNEL(reduce_sum); }",
    )
}

fn source_set_with_host(
    device: &str,
    host: &str,
) -> (CandidateSourceManifest, BTreeMap<BundlePath, Vec<u8>>) {
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
        build_target: "alloyport_reduction_candidate".to_owned(),
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
fn source_gate_independently_rejects_a_framework_fallback() {
    let device = "#include <torch/extension.h>\nauto reduce() { return at::sum(input); }";
    let (manifest, sources) = source_set(device);
    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(!receipt.passed());
    let kinds: BTreeSet<_> = receipt.blocking().map(|failure| failure.kind).collect();
    assert!(kinds.contains(&SourceGateFailureKind::ForbiddenFallback));
}

#[test]
fn an_unfamiliar_ascend_c_surface_is_reported_and_still_allowed_through() {
    // The low-level `Te` tensor API is the only route proven to work for a direct-launch Cube
    // kernel, and it carries none of the markers the earlier gate demanded. Refusing it here would
    // reject a correct port for spelling, before a compiler ever saw it.
    let device = "#include \"mm_te_common.h\"\nextern \"C\" __global__ void reduce(uint8_t *x) {}";
    let (manifest, sources) = source_set(device);
    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(
        receipt.passed(),
        "{:?}",
        receipt.blocking().collect::<Vec<_>>()
    );
    let advisories: Vec<_> = receipt.advisories().map(|item| item.kind).collect();
    assert_eq!(
        advisories,
        vec![SourceGateFailureKind::UnrecognizedKernelStructure],
        "the finding is reported to the model, it just does not decide anything"
    );
}

#[test]
fn a_bundle_with_no_device_source_cannot_pass() {
    let (manifest, sources) = source_set("   \n");
    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(!receipt.passed());
    let kinds: BTreeSet<_> = receipt.blocking().map(|failure| failure.kind).collect();
    assert!(kinds.contains(&SourceGateFailureKind::MissingDeviceSource));
}

#[test]
fn dropping_the_public_symbol_blocks_the_candidate() {
    let host = "extern \"C\" int something_else(const float *input) { return 0; }";
    let (manifest, sources) = source_set_with_host(
        "#include <kernel_operator.h>\n__aicore__ void reduce(GM_ADDR x) {}",
        host,
    );
    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(!receipt.passed());
    let kinds: BTreeSet<_> = receipt.blocking().map(|failure| failure.kind).collect();
    assert!(kinds.contains(&SourceGateFailureKind::MissingHostEntryPoint));
}

/// A blocking failure must name what is wrong, not only that something is.
///
/// Both migrations that reached this gate lost turns here. The message said the component mapping
/// "does not cover every input and generated implementation source" and stopped, so the model went
/// looking through the reference corpus for a mapping *format* — which is not documented anywhere,
/// because the gate only looks for the path text. The check already knows exactly which paths are
/// absent; withholding them was the whole cost.
#[test]
fn a_blocking_failure_names_the_paths_it_is_missing() {
    let (_, sources) = source_set(
        "#include <kernel_operator.h>\nextern \"C\" __global__ __aicore__ void reduce_sum(GM_ADDR x) {}",
    );
    // A mapping that mentions nothing, and build files that reference nothing.
    let blank = "nothing useful here";
    let files = vec![
        file(
            "generated/reduce.cpp",
            GeneratedSourceKind::AscendCDevice,
            "#include <kernel_operator.h>\nextern \"C\" __global__ __aicore__ void reduce_sum(GM_ADDR x) {}",
        ),
        file(
            "generated/host.cpp",
            GeneratedSourceKind::AscendHost,
            "extern \"C\" int alloyport_reduce_sum_f32(const float *input, size_t elements, float *output) { ACLRT_LAUNCH_KERNEL(reduce_sum); }",
        ),
        file(
            "generated/CMakeLists.txt",
            GeneratedSourceKind::BuildIntegration,
            "add_library(alloyport_reduction_candidate)",
        ),
        file(
            "generated/component-map.txt",
            GeneratedSourceKind::ComponentMapping,
            blank,
        ),
    ];
    let manifest = CandidateSourceManifest::new(CandidateSourceManifestSpec {
        candidate_id: CandidateId::try_from("candidate-source-2").expect("candidate ID"),
        task_id: TaskId::try_from("task-source-1").expect("task ID"),
        parent_candidate_id: None,
        migration_spec_digest: Sha256Digest::digest_bytes(b"spec"),
        generation_strategy: GenerationStrategy::DirectAscendC,
        public_symbol: "alloyport_reduce_sum_f32".to_owned(),
        build_target: "alloyport_reduction_candidate".to_owned(),
        input_source_paths: ["input/kernel.cu", "input/host.cu"]
            .into_iter()
            .map(|path| BundlePath::try_from(path).expect("input path"))
            .collect(),
        source_bundle_digest: Sha256Digest::digest_bytes(b"bundle"),
        files,
    })
    .expect("manifest");
    let sources = sources
        .into_iter()
        .map(|(path, bytes)| {
            if path.as_str().ends_with("component-map.txt") {
                (path, blank.as_bytes().to_vec())
            } else if path.as_str().ends_with("CMakeLists.txt") {
                (path, b"add_library(alloyport_reduction_candidate)".to_vec())
            } else {
                (path, bytes)
            }
        })
        .collect();

    let receipt =
        evaluate_source_gate(&manifest, Sha256Digest::digest_bytes(b"manifest"), &sources);
    assert!(!receipt.passed());
    let details: String = receipt
        .failures()
        .iter()
        .map(|failure| failure.detail.clone())
        .collect::<Vec<_>>()
        .join(" | ");
    for path in [
        "input/kernel.cu",
        "input/host.cu",
        "generated/reduce.cpp",
        "generated/host.cpp",
    ] {
        assert!(
            details.contains(path),
            "a failure must name {path}; the model cannot fix what it is not told: {details}"
        );
    }
}
