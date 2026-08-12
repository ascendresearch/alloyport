//! Behavioral tests for the fixed CUDA execution contract.

use super::*;
use crate::journal::{StoredArtifact, StoredEnvironment, StoredExecution};
use alloyport_artifacts::{FilesystemArtifactStore, IngestRequest, Sha256Digest};
use alloyport_core::{AssignmentId, AttemptId, CandidateId, ExecutionKind, TaskId};
use std::io::Cursor;

#[test]
fn fixed_contract_materializes_idempotently_and_never_builds_a_shell_command()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = FilesystemArtifactStore::open(directory.path().join("cas"), 64 * 1024)?;
    let bundle = CudaFixtureBundle::vector_add("__global__ void vector_add() {}\n");
    let bundle_bytes = serde_json::to_vec(&bundle)?;
    let stored = artifacts.ingest(&mut Cursor::new(&bundle_bytes), IngestRequest::unverified())?;
    let image_manifest = Sha256Digest::digest_bytes(b"image manifest");
    let image_id = Sha256Digest::digest_bytes(b"local image filesystem");
    let policy = policy(
        directory.path().join("sandboxes"),
        stored.artifact.digest,
        image_manifest,
        image_id,
    )?;
    let assignment = assignment(
        stored.artifact.digest,
        stored.artifact.size_bytes,
        image_manifest,
    );

    policy.validate_assignment(&assignment)?;
    let sandbox = policy.materialize_bundle(&assignment, &artifacts)?;
    assert_eq!(
        fs::read_to_string(sandbox.directory().join(SOURCE_FILENAME))?,
        bundle.source
    );
    assert_eq!(
        policy.materialize_bundle(&assignment, &artifacts)?,
        sandbox,
        "restart materialization must preserve identical bytes"
    );
    let plan = policy.docker_create_plan(&assignment, &sandbox)?;
    assert_eq!(plan.container_name, "alloyport-attempt-1");
    assert_eq!(
        plan.image_reference,
        format!("example.invalid/alloyport/cuda@{image_manifest}")
    );
    assert_eq!(plan.expected_image_id, image_id);
    assert_eq!(plan.argv.first().map(String::as_str), Some("create"));
    assert!(!plan.argv.iter().any(|part| part == "sh" || part == "-c"));
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--network", "none"])
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--gpus", "device=0"])
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--entrypoint", "python3"])
    );
    assert_eq!(
        &plan.argv[plan.argv.len() - 2..],
        [
            format!("example.invalid/alloyport/cuda@{image_manifest}"),
            "/alloyport/bundle/run_fixture.py".into(),
        ]
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--log-opt", "max-size=65536"])
    );
    let tmpfs = plan
        .argv
        .windows(2)
        .filter(|pair| pair[0] == "--tmpfs")
        .map(|pair| pair[1].as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        tmpfs,
        [
            "/alloyport/work:rw,exec,size=201326592",
            "/tmp:rw,exec,size=67108864",
        ]
    );

    fs::write(sandbox.directory().join(SOURCE_FILENAME), b"changed\n")?;
    assert!(matches!(
        policy.materialize_bundle(&assignment, &artifacts),
        Err(CudaContractError::Bundle(detail)) if detail.contains("conflicting bytes")
    ));

    let mut changed = assignment.clone();
    changed.execution.environment.push(StoredEnvironment {
        name: "LD_PRELOAD".into(),
        value: "/host/inject.so".into(),
    });
    assert!(matches!(
        policy.validate_assignment(&changed),
        Err(CudaContractError::Assignment(_))
    ));
    Ok(())
}

#[test]
fn bundle_rejects_a_source_digest_mismatch() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = FilesystemArtifactStore::open(directory.path().join("cas"), 64 * 1024)?;
    let mut bundle = CudaFixtureBundle::vector_add("source\n");
    bundle.source_sha256 = Sha256Digest::digest_bytes(b"other").to_string();
    let bytes = serde_json::to_vec(&bundle)?;
    let stored = artifacts.ingest(&mut Cursor::new(bytes), IngestRequest::unverified())?;
    let image_manifest = Sha256Digest::digest_bytes(b"image manifest");
    let policy = policy(
        directory.path().join("sandboxes"),
        stored.artifact.digest,
        image_manifest,
        Sha256Digest::digest_bytes(b"image id"),
    )?;
    let assignment = assignment(
        stored.artifact.digest,
        stored.artifact.size_bytes,
        image_manifest,
    );
    assert!(matches!(
        policy.materialize_bundle(&assignment, &artifacts),
        Err(CudaContractError::Bundle(detail)) if detail.contains("source digest")
    ));
    Ok(())
}

#[test]
fn standalone_local_image_id_is_an_immutable_assignment_identity() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let bundle = Sha256Digest::digest_bytes(b"bundle");
    let image_id = Sha256Digest::digest_bytes(b"local image config");
    let policy = CudaFixturePolicy::new(
        VECTOR_ADD_FIXTURE_ID,
        bundle,
        image_id,
        "alloyport-cuda-vectoradd-v1:local",
        image_id,
        "0",
        directory.path(),
        CudaResourceCeilings {
            cpu_millis: 2_000,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            disk_bytes: 512 * 1024 * 1024,
            process_count: 64,
            output_bytes: 1024 * 1024,
        },
    )?;
    let mut assignment = assignment(bundle, 1, image_id);
    assignment.execution.image.media_type = OCI_IMAGE_CONFIG_MEDIA_TYPE.into();
    policy.validate_assignment(&assignment)?;

    assignment.execution.image.media_type = OCI_IMAGE_MANIFEST_MEDIA_TYPE.into();
    assert!(policy.validate_assignment(&assignment).is_err());
    Ok(())
}

fn policy(
    root: PathBuf,
    bundle: Sha256Digest,
    image_manifest: Sha256Digest,
    image_id: Sha256Digest,
) -> Result<CudaFixturePolicy, CudaContractError> {
    CudaFixturePolicy::new(
        VECTOR_ADD_FIXTURE_ID,
        bundle,
        image_manifest,
        format!("example.invalid/alloyport/cuda@{image_manifest}"),
        image_id,
        "0",
        root,
        CudaResourceCeilings {
            cpu_millis: 2_000,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            disk_bytes: 512 * 1024 * 1024,
            process_count: 64,
            output_bytes: 1024 * 1024,
        },
    )
}

fn assignment(
    bundle_digest: Sha256Digest,
    bundle_size: u64,
    image_digest: Sha256Digest,
) -> StoredAssignment {
    StoredAssignment {
        assignment_id: AssignmentId::try_from("assignment-1").expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from("attempt-1").expect("valid fixture attempt ID"),
        attempt_number: 1,
        idempotency_key: "cuda-vectoradd-v1".into(),
        task_id: TaskId::try_from("task-1").expect("valid fixture task ID"),
        candidate_id: CandidateId::try_from("candidate-1").expect("valid fixture candidate ID"),
        execution: StoredExecution {
            executor_kind: ExecutionKind::CudaFixture,
            argv: vec![VECTOR_ADD_FIXTURE_ID.into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 30_000,
            bundle: StoredArtifact {
                digest: bundle_digest,
                size_bytes: bundle_size,
                media_type: CUDA_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
            },
            image: StoredArtifact {
                digest: image_digest,
                size_bytes: 0,
                media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.into(),
            },
            limits: Some(StoredLimits {
                cpu_millis: 1_000,
                memory_bytes: 1024 * 1024 * 1024,
                disk_bytes: 256 * 1024 * 1024,
                process_count: 32,
                output_bytes: 64 * 1024,
                device_count: 1,
                network: NetworkPolicy::Disabled,
            }),
        },
        required_features: vec![CUDA_FIXTURE_FEATURE.into()],
    }
}
