//! Behavioral tests for the fixed Ascend execution contract.

use super::*;
use crate::journal::{StoredArtifact, StoredEnvironment, StoredExecution};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, IngestRequest};
use alloyport_core::{AssignmentId, AttemptId, CandidateId, TaskId};
use std::io::Cursor;

#[test]
fn bundle_materialization_is_digest_verified_write_once_and_idempotent()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let artifacts = FilesystemArtifactStore::open(directory.path().join("cas"), 64 * 1024)?;
    let source = "extern \"C\" __global__ __aicore__ void add_custom() {}\n";
    let bytes = serde_json::to_vec(&AscendFixtureBundle::add(source))?;
    let stored = artifacts.ingest(&mut Cursor::new(bytes), IngestRequest::unverified())?;
    let manifest = Sha256Digest::digest_bytes(b"ascend image manifest");
    let policy = AscendFixturePolicy::new(
        ASCEND_ADD_FIXTURE_ID,
        stored.artifact.digest,
        manifest,
        format!("example.invalid/alloyport/ascend@{manifest}"),
        Sha256Digest::digest_bytes(b"image"),
        device(),
        device_nodes(),
        DRIVER_PATH,
        directory.path().join("sandboxes"),
        ceilings(),
        environment()?,
    )?;
    let mut assignment = assignment(stored.artifact.digest, manifest);
    assignment.execution.bundle.size_bytes = stored.artifact.size_bytes;

    let sandbox = policy.materialize_bundle(&assignment, &artifacts)?;
    assert_eq!(
        std::fs::read_to_string(sandbox.directory().join(SOURCE_FILENAME))?,
        source
    );
    assert_eq!(
        sandbox.source_digest(),
        Sha256Digest::digest_bytes(source.as_bytes()).to_string()
    );
    assert_eq!(policy.materialize_bundle(&assignment, &artifacts)?, sandbox);
    assert!(
        std::fs::read_to_string(sandbox.directory().join(RUNNER_FILENAME))?
            .contains("/opt/alloyport/fixtures/ascend-add-v1/run_fixture.py")
    );

    std::fs::write(sandbox.directory().join(SOURCE_FILENAME), b"changed\n")?;
    assert!(matches!(
        policy.materialize_bundle(&assignment, &artifacts),
        Err(AscendContractError::Bundle(detail)) if detail.contains("conflicting bytes")
    ));
    Ok(())
}

#[test]
fn fixed_contract_derives_enumerated_nodes_driver_and_visible_device_without_a_shell()
-> Result<(), Box<dyn Error>> {
    let bundle = Sha256Digest::digest_bytes(b"ascend bundle");
    let manifest = Sha256Digest::digest_bytes(b"ascend image manifest");
    let image_id = Sha256Digest::digest_bytes(b"ascend local image");
    let policy = policy(bundle, manifest, image_id)?;
    let assignment = assignment(bundle, manifest);

    policy.validate_assignment(&assignment)?;
    let sandbox = AscendSandbox {
        directory: PathBuf::from("/var/lib/alloyport/ascend-sandboxes/attempt-ascend-1"),
        source_digest: Sha256Digest::digest_bytes(b"source").to_string(),
    };
    let plan = policy.docker_create_plan(&assignment, &sandbox)?;
    assert_eq!(plan.container_name, "alloyport-attempt-ascend-1");
    assert_eq!(plan.expected_image_id, image_id);
    assert_eq!(plan.device.device_id, "3");
    assert_eq!(plan.environment.cann_version, "9.1.0-beta.1");
    assert!(!plan.argv.iter().any(|part| part == "sh" || part == "-c"));
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--cap-drop", "ALL"])
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--cap-add", "DAC_OVERRIDE"])
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| pair == ["--network", "none"])
    );
    assert!(plan.argv.windows(2).any(|pair| {
        pair == [
            "--mount",
            "type=bind,src=/usr/local/Ascend/driver,dst=/usr/local/Ascend/driver,readonly",
        ]
    }));
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| { pair == ["--env", "ASCEND_RT_VISIBLE_DEVICES=3"] })
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| { pair == ["--env", "TMPDIR=/alloyport/work/tmp"] })
    );
    assert!(
        plan.argv
            .windows(2)
            .any(|pair| { pair == ["--env", "ASCEND_PROCESS_LOG_PATH=/alloyport/work/log",] })
    );
    let devices = plan
        .argv
        .windows(2)
        .filter(|pair| pair[0] == "--device")
        .map(|pair| pair[1].as_str())
        .collect::<Vec<_>>();
    assert_eq!(devices.len(), 9);
    assert!(devices.contains(&"/dev/davinci0:/dev/davinci0:rwm"));
    assert!(devices.contains(&"/dev/davinci6:/dev/davinci6:rwm"));
    assert!(devices.contains(&"/dev/davinci_manager:/dev/davinci_manager:rwm"));
    assert!(devices.contains(&"/dev/hisi_hdc:/dev/hisi_hdc:rwm"));
    Ok(())
}

#[test]
fn assignment_cannot_select_environment_and_policy_requires_manager_nodes()
-> Result<(), Box<dyn Error>> {
    let bundle = Sha256Digest::digest_bytes(b"ascend bundle");
    let manifest = Sha256Digest::digest_bytes(b"ascend image manifest");
    let policy = policy(
        bundle,
        manifest,
        Sha256Digest::digest_bytes(b"ascend local image"),
    )?;
    let mut changed = assignment(bundle, manifest);
    changed.execution.environment.push(StoredEnvironment {
        name: "ASCEND_RT_VISIBLE_DEVICES".to_owned(),
        value: "0".to_owned(),
    });
    assert!(matches!(
        policy.validate_assignment(&changed),
        Err(AscendContractError::Assignment(_))
    ));

    let mut excessive_timeout = assignment(bundle, manifest);
    excessive_timeout.execution.timeout_ms = 60_001;
    assert!(matches!(
        policy.validate_assignment(&excessive_timeout),
        Err(AscendContractError::Assignment(_))
    ));

    let mut unsafe_attempt = assignment(bundle, manifest);
    unsafe_attempt.attempt_id = AttemptId::try_from("attempt/escape")?;
    assert!(matches!(
        policy.validate_assignment(&unsafe_attempt),
        Err(AscendContractError::Assignment(_))
    ));

    let mut nodes = device_nodes();
    nodes.retain(|path| path != Path::new("/dev/davinci_manager"));
    assert!(matches!(
        AscendFixturePolicy::new(
            ASCEND_ADD_FIXTURE_ID,
            bundle,
            manifest,
            format!("example.invalid/alloyport/ascend@{manifest}"),
            Sha256Digest::digest_bytes(b"image"),
            device(),
            nodes,
            DRIVER_PATH,
            "/var/lib/alloyport/ascend-sandboxes",
            ceilings(),
            environment()?,
        ),
        Err(AscendContractError::InvalidPolicy(_))
    ));
    Ok(())
}

#[test]
fn standalone_local_image_id_is_an_immutable_assignment_identity() -> Result<(), Box<dyn Error>> {
    let bundle = Sha256Digest::digest_bytes(b"ascend bundle");
    let image_id = Sha256Digest::digest_bytes(b"ascend local image config");
    let policy = AscendFixturePolicy::new(
        ASCEND_ADD_FIXTURE_ID,
        bundle,
        image_id,
        "alloyport-ascend-add-v1:local",
        image_id,
        device(),
        device_nodes(),
        DRIVER_PATH,
        "/var/lib/alloyport/ascend-sandboxes",
        ceilings(),
        environment()?,
    )?;
    let mut assignment = assignment(bundle, image_id);
    assignment.execution.image.media_type = OCI_IMAGE_CONFIG_MEDIA_TYPE.into();
    policy.validate_assignment(&assignment)?;

    assignment.execution.image.media_type = OCI_IMAGE_MANIFEST_MEDIA_TYPE.into();
    assert!(policy.validate_assignment(&assignment).is_err());
    Ok(())
}

fn policy(
    bundle: Sha256Digest,
    manifest: Sha256Digest,
    image_id: Sha256Digest,
) -> Result<AscendFixturePolicy, AscendContractError> {
    AscendFixturePolicy::new(
        ASCEND_ADD_FIXTURE_ID,
        bundle,
        manifest,
        format!("example.invalid/alloyport/ascend@{manifest}"),
        image_id,
        device(),
        device_nodes(),
        DRIVER_PATH,
        "/var/lib/alloyport/ascend-sandboxes",
        ceilings(),
        environment()?,
    )
}

fn device() -> AcceleratorDevice {
    AcceleratorDevice {
        device_id: "3".to_owned(),
        product_name: "Ascend950PR".to_owned(),
        serial_number: "fixture-serial-3".to_owned(),
        firmware_version: "9.0.0.105.229".to_owned(),
    }
}

fn device_nodes() -> Vec<PathBuf> {
    (0..7)
        .map(|index| PathBuf::from(format!("/dev/davinci{index}")))
        .chain([
            PathBuf::from("/dev/davinci_manager"),
            PathBuf::from("/dev/hisi_hdc"),
        ])
        .collect()
}

fn environment() -> Result<AscendEnvironmentFacts, AscendContractError> {
    AscendEnvironmentFacts::new("Ascend950PR", "9.1.0-beta.1", "25.7.rc1.6", "9.0.0.105.229")
}

const fn ceilings() -> AscendResourceCeilings {
    AscendResourceCeilings {
        timeout_ms: 60_000,
        cpu_millis: 4_000,
        memory_bytes: 8 * 1024 * 1024 * 1024,
        disk_bytes: 1024 * 1024 * 1024,
        process_count: 128,
        output_bytes: 1024 * 1024,
    }
}

fn assignment(bundle: Sha256Digest, manifest: Sha256Digest) -> StoredAssignment {
    StoredAssignment {
        assignment_id: AssignmentId::try_from("assignment-ascend-1")
            .expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from("attempt-ascend-1").expect("valid fixture attempt ID"),
        attempt_number: 1,
        idempotency_key: ASCEND_ADD_FIXTURE_ID.to_owned(),
        task_id: TaskId::try_from("task-ascend-1").expect("valid fixture task ID"),
        candidate_id: CandidateId::try_from("candidate-ascend-1")
            .expect("valid fixture candidate ID"),
        execution: StoredExecution {
            executor_kind: ExecutionKind::AscendFixture,
            argv: vec![ASCEND_ADD_FIXTURE_ID.to_owned()],
            working_directory: ".".to_owned(),
            environment: Vec::new(),
            timeout_ms: 30_000,
            bundle: StoredArtifact {
                digest: bundle,
                size_bytes: 1,
                media_type: ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE.to_owned(),
            },
            image: StoredArtifact {
                digest: manifest,
                size_bytes: 0,
                media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.to_owned(),
            },
            limits: Some(StoredLimits {
                cpu_millis: 2_000,
                memory_bytes: 4 * 1024 * 1024 * 1024,
                disk_bytes: 512 * 1024 * 1024,
                process_count: 64,
                output_bytes: 64 * 1024,
                device_count: 1,
                network: NetworkPolicy::Disabled,
            }),
        },
        required_features: vec![ASCEND_FIXTURE_FEATURE.to_owned()],
    }
}
