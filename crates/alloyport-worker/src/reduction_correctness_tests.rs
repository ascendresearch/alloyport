use super::*;
use alloyport_artifacts::{ArtifactStore, InMemoryArtifactStore, IngestRequest};
use alloyport_core::{
    ArtifactDescriptor, AssignmentContract, AssignmentId, AttemptId, BundlePath, CandidateId,
    ExecutionContract, ReductionCalibrationReceipt, ReductionCorpus,
    ReductionCorrectnessExperiment, ReductionExecutionFile, ReductionRunReceipt, ResourceContract,
    TaskId,
};
use std::io::Cursor;
use std::str::FromStr;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn image(reference: &str, manifest: Sha256Digest, image_id: Sha256Digest) -> ArtifactDescriptor {
    ArtifactDescriptor {
        digest: manifest,
        size_bytes: 512,
        media_type: crate::container_engine::image_artifact_media_type(
            reference, manifest, image_id,
        )
        .expect("image media type")
        .to_owned(),
    }
}

fn ceilings() -> CorrectnessResourceCeilings {
    CorrectnessResourceCeilings {
        timeout_ms: 120_000,
        cpu_millis: 2_000,
        memory_bytes: 2 * 1024 * 1024 * 1024,
        disk_bytes: 512 * 1024 * 1024,
        process_count: 64,
        output_bytes: 8 * 1024 * 1024,
    }
}

fn resources() -> ResourceContract {
    let ceilings = ceilings();
    ResourceContract {
        cpu_millis: ceilings.cpu_millis,
        memory_bytes: ceilings.memory_bytes,
        disk_bytes: ceilings.disk_bytes,
        process_count: ceilings.process_count,
        output_bytes: ceilings.output_bytes,
        device_count: 1,
        network: NetworkPolicy::Disabled,
    }
}

fn bundle(role: ReductionRunRole) -> ReductionExecutionBundle {
    let corpus = ReductionCorpus::fixture_v1();
    let experiment = ReductionCorrectnessExperiment::new(
        TaskId::try_from("task-worker-correctness").expect("task ID"),
        CandidateId::try_from("candidate-worker-correctness").expect("candidate ID"),
        digest("migration"),
        digest("manifest"),
        digest("source"),
        digest("build"),
        corpus.digest().expect("corpus digest"),
        digest("policy"),
    );
    let path = match role {
        ReductionRunRole::CudaReference => "input/CMakeLists.txt",
        ReductionRunRole::AscendCandidate => "generated/CMakeLists.txt",
    };
    ReductionExecutionBundle::new(
        experiment,
        role,
        corpus,
        vec![
            ReductionExecutionFile::new(
                BundlePath::try_from(path).expect("bundle path"),
                "add_library(reduce_sum STATIC source.cpp)",
            )
            .expect("execution file"),
        ],
    )
    .expect("execution bundle")
}

fn ingest_bundle(
    artifacts: &dyn ArtifactStore,
    bundle: &ReductionExecutionBundle,
) -> ArtifactDescriptor {
    let bytes = serde_json::to_vec(bundle).expect("serialize bundle");
    let digest = Sha256Digest::digest_bytes(&bytes);
    let size_bytes = u64::try_from(bytes.len()).expect("bundle size");
    artifacts
        .ingest(
            &mut Cursor::new(bytes),
            IngestRequest {
                expected_digest: Some(digest),
                expected_size_bytes: Some(size_bytes),
            },
        )
        .expect("ingest bundle");
    ArtifactDescriptor {
        digest,
        size_bytes,
        media_type: REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE.to_owned(),
    }
}

fn assignment(
    bundle: &ReductionExecutionBundle,
    bundle_artifact: ArtifactDescriptor,
    image: ArtifactDescriptor,
) -> AssignmentContract {
    let (role, executor, feature) = match bundle.role() {
        ReductionRunRole::CudaReference => (
            "cuda",
            ExecutionKind::CudaCorrectness,
            CUDA_REDUCTION_CORRECTNESS_FEATURE,
        ),
        ReductionRunRole::AscendCandidate => (
            "ascend",
            ExecutionKind::AscendCorrectness,
            ASCEND_REDUCTION_CORRECTNESS_FEATURE,
        ),
    };
    AssignmentContract {
        assignment_id: AssignmentId::try_from(format!("assignment-{role}-correctness"))
            .expect("assignment ID"),
        attempt_id: AttemptId::try_from(format!("attempt-{role}-correctness")).expect("attempt ID"),
        attempt_number: 1,
        idempotency_key: format!("correctness:{role}"),
        task_id: bundle.experiment().task_id().clone(),
        candidate_id: bundle.experiment().candidate_id().clone(),
        execution: ExecutionContract {
            executor_kind: executor,
            argv: vec!["reduction-correctness-v1".to_owned()],
            working_directory: ".".to_owned(),
            environment: Vec::new(),
            timeout_ms: ceilings().timeout_ms,
            bundle: bundle_artifact,
            image,
            limits: Some(resources()),
        },
        required_features: vec![feature.to_owned()],
    }
}

#[test]
fn cuda_policy_materializes_exact_bundle_and_derives_fixed_plan() -> Result<(), Box<dyn Error>> {
    let artifacts = InMemoryArtifactStore::new(64 * 1024 * 1024);
    let bundle = bundle(ReductionRunRole::CudaReference);
    let bundle_artifact = ingest_bundle(&artifacts, &bundle);
    let root = tempfile::tempdir()?;
    let manifest = digest("cuda-manifest");
    let image_id = digest("cuda-image");
    let reference = format!("example.invalid/cuda@{manifest}");
    let environment = crate::cuda_runtime::CudaEnvironmentFacts::new("sm_90", "580", "13.0")?;
    let policy = ReductionCorrectnessPolicy::new_cuda(
        manifest,
        &reference,
        image_id,
        "0",
        root.path().join("sandboxes"),
        ceilings(),
        &environment,
    )?;
    let assignment = assignment(
        &bundle,
        bundle_artifact,
        image(&reference, manifest, image_id),
    );

    let sandbox = policy.materialize_bundle(&assignment, &artifacts)?;
    assert_eq!(
        sandbox.implementation_digest(),
        bundle.implementation_digest()
    );
    assert!(sandbox.directory().join("input/CMakeLists.txt").is_file());
    assert!(sandbox.directory().join(RUNNER_FILENAME).is_file());
    let plan = policy.cuda_docker_create_plan(&assignment, &sandbox)?;
    assert!(
        plan.argv
            .windows(2)
            .any(|item| item == ["--network", "none"])
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|item| item == ["--gpus", "device=0"])
    );
    assert!(!plan.argv.iter().any(|item| item == "sh" || item == "bash"));
    Ok(())
}

#[test]
fn ascend_policy_materializes_candidate_and_derives_local_device_plan() -> Result<(), Box<dyn Error>>
{
    let artifacts = InMemoryArtifactStore::new(64 * 1024 * 1024);
    let bundle = bundle(ReductionRunRole::AscendCandidate);
    let bundle_artifact = ingest_bundle(&artifacts, &bundle);
    let root = tempfile::tempdir()?;
    let manifest = digest("ascend-manifest");
    let image_id = digest("ascend-image");
    let reference = format!("example.invalid/ascend@{manifest}");
    let environment = AscendEnvironmentFacts {
        architecture: "Ascend950PR".to_owned(),
        cann_version: "9.1.0".to_owned(),
        driver_version: "25.7".to_owned(),
        firmware_version: "9.0".to_owned(),
    };
    let device = AcceleratorDevice {
        device_id: "3".to_owned(),
        product_name: environment.architecture.clone(),
        serial_number: "serial-3".to_owned(),
        firmware_version: environment.firmware_version.clone(),
    };
    let policy = ReductionCorrectnessPolicy::new_ascend(
        manifest,
        &reference,
        image_id,
        device,
        vec![
            "/dev/davinci3".into(),
            "/dev/davinci_manager".into(),
            "/dev/hisi_hdc".into(),
        ],
        DRIVER_PATH,
        root.path().join("sandboxes"),
        ceilings(),
        &environment,
    )?;
    let assignment = assignment(
        &bundle,
        bundle_artifact,
        image(&reference, manifest, image_id),
    );

    let sandbox = policy.materialize_bundle(&assignment, &artifacts)?;
    let plan = policy.ascend_docker_create_plan(&assignment, &sandbox)?;
    assert_eq!(plan.device.device_id, "3");
    assert!(
        plan.argv
            .windows(2)
            .any(|item| { item == ["--env", "ASCEND_RT_VISIBLE_DEVICES=3"] })
    );
    assert!(plan.argv.iter().any(|item| item.contains("/dev/davinci3")));
    Ok(())
}

#[test]
fn role_and_network_cannot_cross_local_policy() -> Result<(), Box<dyn Error>> {
    let artifacts = InMemoryArtifactStore::new(64 * 1024 * 1024);
    let bundle = bundle(ReductionRunRole::CudaReference);
    let bundle_artifact = ingest_bundle(&artifacts, &bundle);
    let root = tempfile::tempdir()?;
    let manifest = digest("cuda-manifest");
    let image_id = digest("cuda-image");
    let reference = format!("example.invalid/cuda@{manifest}");
    let environment = crate::cuda_runtime::CudaEnvironmentFacts::new("sm_90", "580", "13.0")?;
    let policy = ReductionCorrectnessPolicy::new_cuda(
        manifest,
        &reference,
        image_id,
        "0",
        root.path().join("sandboxes"),
        ceilings(),
        &environment,
    )?;
    let mut assignment = assignment(
        &bundle,
        bundle_artifact,
        image(&reference, manifest, image_id),
    );
    assignment.execution.executor_kind = ExecutionKind::AscendCorrectness;
    assert!(policy.validate_assignment(&assignment).is_err());
    assignment.execution.executor_kind = ExecutionKind::CudaCorrectness;
    assignment
        .execution
        .limits
        .as_mut()
        .expect("limits")
        .network = NetworkPolicy::DependencyFetch;
    assert!(policy.validate_assignment(&assignment).is_err());
    Ok(())
}

#[test]
fn trusted_images_declare_the_complete_runner_toolchain() {
    let cuda = include_str!("../../../fixtures/reduction-correctness-v1/cuda-image/Dockerfile");
    for required in ["python3", "cmake", "g++", "make", "/usr/local/cuda/bin"] {
        assert!(cuda.contains(required), "CUDA image omits {required}");
    }
    let ascend = include_str!("../../../fixtures/reduction-correctness-v1/ascend-image/Dockerfile");
    for required in [
        "python3",
        "ASCEND_TOOLKIT_HOME",
        "ccec_compiler",
        "/usr/local/Ascend/driver/lib64",
    ] {
        assert!(ascend.contains(required), "Ascend image omits {required}");
    }
    assert!(!cuda.contains("COPY "));
    assert!(!ascend.contains("COPY "));
}

#[test]
fn real_cuda_diagnostic_evidence_remains_schema_valid_and_complete() -> Result<(), Box<dyn Error>> {
    let receipt: ReductionRunReceipt = serde_json::from_slice(include_bytes!(
        "../../../docs/evidence/cuda-reduction-correctness-diagnostic-20260812.json"
    ))?;
    assert_eq!(receipt.role(), ReductionRunRole::CudaReference);
    assert_eq!(receipt.observations().len(), 24);
    assert_eq!(
        receipt.implementation_digest(),
        Sha256Digest::from_str(
            "sha256:b495ea483e83b074eb71a559a85a6d5c1644c271b144625c5ca430d6a24579ed"
        )?
    );
    // The real run is genuine evidence and stays. What it can now also do is state this task's own
    // numeric spread, because the corpus already makes the authority sum every input twice.
    let floor = alloyport_core::measure_reduction_noise_floor(
        &receipt,
        &alloyport_core::ReductionCorpus::fixture_v1(),
    )?;
    assert!(floor.repetition_pairs() > 0);

    let calibration: ReductionCalibrationReceipt = serde_json::from_slice(include_bytes!(
        "../../../docs/evidence/cuda-reduction-calibration-diagnostic-20260812.json"
    ))?;
    assert_eq!(calibration.detections().len(), 10);
    assert!(calibration.detections().iter().all(|item| item.detected));
    // It caught all ten of its mutants and it still does not pass. Every one of them is orders of
    // magnitude larger than the tolerance it ran under, and that tolerance was asserted rather than
    // measured, so the receipt cannot say whether the gate would have rejected a correct port.
    assert!(
        !calibration.passed(),
        "an archived calibration must not keep vouching for a tolerance nobody measured"
    );
    Ok(())
}
