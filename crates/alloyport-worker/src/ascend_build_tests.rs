use super::*;
use crate::container_engine::OCI_IMAGE_MANIFEST_MEDIA_TYPE;
use crate::journal::{StoredArtifact, StoredExecution, StoredLimits};
use alloyport_artifacts::{InMemoryArtifactStore, IngestRequest};
use alloyport_core::{AssignmentId, AttemptId, CandidateId, GenerationStrategy, TaskId};
use serde_json::json;
use std::error::Error;
use std::io::Cursor;
use std::sync::Arc;

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn source(path: &str, kind: &str, contents: &str) -> serde_json::Value {
    json!({
        "path": path,
        "kind": kind,
        "digest": Sha256Digest::digest_bytes(contents.as_bytes()),
        "size_bytes": contents.len(),
        "contents": contents,
    })
}

fn build_bundle() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "candidate_id": "candidate-build-1",
        "task_id": "task-build-1",
        "manifest_digest": digest("manifest"),
        "source_gate_receipt_digest": digest("source-receipt"),
        "migration_spec_digest": digest("migration-spec"),
        "generation_strategy": GenerationStrategy::DirectAscendC,
        "public_symbol": "alloyport_reduce_sum_f32",
        "target_architecture": "Ascend950PR",
        "files": [
            source("generated/reduce.cpp", "ascend_c_device", "device"),
            source("generated/host.cpp", "ascend_host", "host"),
            source("generated/CMakeLists.txt", "build_integration", "build"),
            source("generated/component-map.txt", "component_mapping", "mapping"),
        ]
    })
}

#[test]
fn build_policy_materializes_exact_candidate_and_rejects_command_or_network_changes()
-> Result<(), Box<dyn Error>> {
    let artifacts = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let bytes = serde_json::to_vec(&build_bundle())?;
    let stored = artifacts
        .ingest(&mut Cursor::new(bytes), IngestRequest::unverified())?
        .artifact;
    let directory = tempfile::tempdir()?;
    let image_manifest = digest("image-manifest");
    let policy = AscendBuildPolicy::new(
        image_manifest,
        format!("example.invalid/alloyport/build@{image_manifest}"),
        digest("image-id"),
        device(),
        device_nodes(),
        DRIVER_PATH,
        directory.path().join("sandboxes"),
        ceilings(),
        environment()?,
    )?;
    let mut assignment = assignment(stored.digest, stored.size_bytes, image_manifest);
    let sandbox = policy.materialize_bundle(&assignment, artifacts.as_ref())?;
    assert_eq!(
        fs::read_to_string(sandbox.directory().join("generated/reduce.cpp"))?,
        "device"
    );
    assert!(sandbox.directory().join(RUNNER_FILENAME).is_file());
    assert_eq!(
        policy.materialize_bundle(&assignment, artifacts.as_ref())?,
        sandbox
    );
    let plan = policy.docker_create_plan(&assignment, &sandbox)?;
    assert!(
        plan.argv
            .windows(2)
            .any(|item| item == ["--network", "none"])
    );
    assert!(plan.argv.iter().any(|item| item == "--read-only"));
    assert!(
        plan.argv
            .windows(2)
            .any(|item| item == ["--entrypoint", "python3"])
    );
    assert_eq!(
        plan.argv.last().map(String::as_str),
        Some("/alloyport/bundle/run_build.py")
    );
    assert!(!plan.argv.iter().any(|item| item == "sh" || item == "bash"));

    assignment.execution.argv = vec!["sh".to_owned()];
    assert!(policy.validate_assignment(&assignment).is_err());
    assignment.execution.argv = vec!["build-v1".to_owned()];
    assignment
        .execution
        .limits
        .as_mut()
        .expect("limits")
        .network = NetworkPolicy::DependencyFetch;
    assert!(policy.validate_assignment(&assignment).is_err());
    Ok(())
}

fn assignment(bundle: Sha256Digest, size: u64, image: Sha256Digest) -> StoredAssignment {
    StoredAssignment {
        assignment_id: AssignmentId::try_from("assignment-build-1").expect("assignment"),
        attempt_id: AttemptId::try_from("attempt-build-1").expect("attempt"),
        attempt_number: 1,
        idempotency_key: "build-1".to_owned(),
        task_id: TaskId::try_from("task-build-1").expect("task"),
        candidate_id: CandidateId::try_from("candidate-build-1").expect("candidate"),
        execution: StoredExecution {
            executor_kind: ExecutionKind::AscendBuild,
            argv: vec!["build-v1".to_owned()],
            working_directory: ".".to_owned(),
            environment: Vec::new(),
            timeout_ms: 30_000,
            bundle: StoredArtifact {
                digest: bundle,
                size_bytes: size,
                media_type: ASCEND_BUILD_BUNDLE_MEDIA_TYPE.to_owned(),
            },
            image: StoredArtifact {
                digest: image,
                size_bytes: 1,
                media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.to_owned(),
            },
            limits: Some(StoredLimits {
                cpu_millis: 2_000,
                memory_bytes: 1024 * 1024 * 1024,
                disk_bytes: 256 * 1024 * 1024,
                process_count: 64,
                output_bytes: 1024 * 1024,
                device_count: 0,
                network: NetworkPolicy::Disabled,
            }),
        },
        required_features: vec![ASCEND_BUILD_FEATURE.to_owned()],
    }
}

fn device() -> AcceleratorDevice {
    AcceleratorDevice {
        device_id: "3".to_owned(),
        product_name: "Ascend950PR".to_owned(),
        serial_number: "serial-3".to_owned(),
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

fn environment() -> Result<AscendEnvironmentFacts, crate::ascend::AscendContractError> {
    AscendEnvironmentFacts::new("Ascend950PR", "9.1.0-beta.1", "25.7.rc1.6", "9.0.0.105.229")
}

const fn ceilings() -> AscendResourceCeilings {
    AscendResourceCeilings {
        timeout_ms: 60_000,
        cpu_millis: 4_000,
        memory_bytes: 8 * 1024 * 1024 * 1024,
        disk_bytes: 1024 * 1024 * 1024,
        process_count: 128,
        output_bytes: 2 * 1024 * 1024,
    }
}

/// A build asks for no accelerator, and its container mounts none.
///
/// The runner is two `cmake` calls and never opens a device; `fixtures/ascend-add-v1` compiles and
/// links in the pinned image with none attached. Requiring one made every build queue behind other
/// users' processes on the shared host and blocked the pipeline for a day, buying nothing: the
/// build receipt names no device, and the architecture and firmware it attests are configuration
/// cross-checked at policy time, not read from a card.
#[test]
fn a_build_neither_requests_nor_mounts_an_accelerator() -> Result<(), Box<dyn Error>> {
    let artifacts = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let bytes = serde_json::to_vec(&build_bundle())?;
    let stored = artifacts
        .ingest(&mut Cursor::new(bytes), IngestRequest::unverified())?
        .artifact;
    let directory = tempfile::tempdir()?;
    let image_manifest = digest("image-manifest");
    let policy = AscendBuildPolicy::new(
        image_manifest,
        format!("example.invalid/alloyport/build@{image_manifest}"),
        digest("image-id"),
        device(),
        device_nodes(),
        DRIVER_PATH,
        directory.path().join("sandboxes"),
        ceilings(),
        environment()?,
    )?;
    let mut assignment = assignment(stored.digest, stored.size_bytes, image_manifest);
    policy.validate_assignment(&assignment)?;

    let sandbox = policy.materialize_bundle(&assignment, artifacts.as_ref())?;
    let plan = policy.docker_create_plan(&assignment, &sandbox)?;
    assert!(
        !plan.argv.iter().any(|item| item == "--device"),
        "a build container must mount no device: {:?}",
        plan.argv
    );
    assert!(
        !plan
            .argv
            .iter()
            .any(|item| item.starts_with("ASCEND_RT_VISIBLE_DEVICES")),
        "a build container must not be told which device to use: {:?}",
        plan.argv
    );

    assignment
        .execution
        .limits
        .as_mut()
        .expect("limits")
        .device_count = 1;
    assert!(
        policy.validate_assignment(&assignment).is_err(),
        "a build that asks for a card must be refused"
    );
    Ok(())
}
