//! Unit tests for server control orchestration.

use super::*;
use crate::storage::{ArtifactIdentity, AssignmentRepository, ExecutionContract};
use alloyport_artifacts::upload::BeginUpload;
use alloyport_artifacts::{FilesystemArtifactStore, SqliteUploadStore};
use alloyport_core::{AssignmentId, AttemptId, AttemptOutcome, CandidateId, ExecutionKind, TaskId};
use alloyport_proto::v1::{
    ArtifactRef, ExecutionSpec, ExecutorKind as WireExecutorKind, ResourceLimits,
};

#[test]
fn worker_acknowledgement_must_be_monotonic_and_not_future() {
    assert!(validate_worker_acknowledgement(3, 2, 3).is_ok());
    assert_eq!(
        validate_worker_acknowledgement(1, 2, 3)
            .expect_err("regression is rejected")
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        validate_worker_acknowledgement(4, 2, 3)
            .expect_err("future acknowledgement is rejected")
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn reconciliation_recovers_restart_residue_without_blocking_on_another_attempt()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("control.sqlite3");
    {
        let repository = SqliteControlRepository::open(&database)?;
        repository.store_assignment(
            "worker-1",
            &stored_contract("fake-attempt", ExecutionKind::Process),
            1_000,
        )?;
        repository.store_assignment(
            "worker-1",
            &stored_contract("cuda-attempt", ExecutionKind::CudaFixture),
            1_001,
        )?;
    }

    let service = WorkerControlService::open_sqlite(&database)?;
    let report = service.reconcile_preparing_assignments().await?;
    assert_eq!(report.scanned, 2);
    assert_eq!(report.recovered, 1);
    assert_eq!(report.pending_delivery, 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].attempt_id, "cuda-attempt");
    assert_eq!(
        service.assignment_state("fake-attempt")?,
        Some(AssignmentState::Dispatchable)
    );
    assert_eq!(
        service.assignment_state("cuda-attempt")?,
        Some(AssignmentState::Preparing)
    );
    assert_eq!(service.interaction_events("task-fake-attempt")?.len(), 1);

    let second = service.reconcile_preparing_assignments().await?;
    assert_eq!(second.scanned, 1);
    assert_eq!(second.recovered, 0);
    assert_eq!(second.failures.len(), 1);
    assert_eq!(service.interaction_events("task-fake-attempt")?.len(), 1);
    Ok(())
}

#[test]
fn terminal_artifacts_must_be_finalized_by_the_reporting_worker() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("uploads"),
        1_024,
        8,
    )?;
    let artifact = ArtifactRef {
        digest: format!("sha256:{}", "a".repeat(64)),
        size_bytes: 1,
        media_type: "application/octet-stream".into(),
    };
    let error = validate_and_grant_finished_artifacts(
        &uploads,
        "worker-1",
        "attempt-1",
        &ExecutionFinished {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            outcome: AttemptOutcome::Succeeded.into(),
            exit_code: Some(0),
            elapsed_ms: 1,
            receipt: Some(artifact.clone()),
            stdout: Some(artifact.clone()),
            stderr: Some(artifact),
            detail: "untrusted terminal".into(),
        },
        1,
    )
    .expect_err("a wire digest cannot manufacture remote Artifact evidence");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    Ok(())
}

#[tokio::test]
async fn cuda_assignment_grants_only_a_published_size_matched_input_bundle()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = Arc::new(SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("upload-data"),
        1_024,
        1_024,
    )?);
    let cas = FilesystemArtifactStore::open(directory.path().join("cas"), 1_024)?;
    let bytes = b"fixture bundle";
    let digest = Sha256Digest::digest_bytes(bytes);
    let session = uploads.begin(&BeginUpload {
        owner_id: "controller".into(),
        upload_key: "fixture:cuda-vectoradd-v1".into(),
        expected_digest: digest,
        expected_size_bytes: u64::try_from(bytes.len())?,
        media_type: "application/vnd.alloyport.cuda-fixture.v1+json".into(),
        now_ms: 1,
        expires_at_ms: 1_001,
    })?;
    uploads.append("controller", &session.upload_id, 0, bytes, 2)?;
    uploads.finalize("controller", &session.upload_id, &cas, 3)?;

    let assignment = Assignment {
        assignment_id: "assignment-1".into(),
        attempt_id: "attempt-1".into(),
        attempt_number: 1,
        idempotency_key: "cuda-vectoradd-v1".into(),
        task_id: "task-1".into(),
        candidate_id: "candidate-1".into(),
        execution: Some(ExecutionSpec {
            executor_kind: WireExecutorKind::CudaFixture.into(),
            argv: vec!["cuda-vectoradd-v1".into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 30_000,
            bundle: Some(ArtifactRef {
                digest: digest.to_string(),
                size_bytes: u64::try_from(bytes.len())?,
                media_type: "application/vnd.alloyport.cuda-fixture.v1+json".into(),
            }),
            image: Some(ArtifactRef {
                digest: format!("sha256:{}", "b".repeat(64)),
                size_bytes: 0,
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            }),
            limits: Some(ResourceLimits {
                cpu_millis: 1_000,
                memory_bytes: 1_024,
                disk_bytes: 1_024,
                process_count: 1,
                output_bytes: 1_024,
                device_count: 1,
                network: alloyport_proto::v1::NetworkPolicy::Disabled.into(),
            }),
        }),
        required_features: vec!["cuda-fixture-v1".into()],
    };
    assert!(matches!(
        WorkerControlService::new()
            .enqueue_assignment("cuda-1", assignment.clone())
            .await,
        Err(EnqueueError::Artifact(_))
    ));
    let service = WorkerControlService::new().with_artifact_metadata(uploads.clone());
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment).await?,
        EnqueueOutcome::Pending
    );
    let reference = uploads.reference("cuda-1", "input:attempt-1:bundle")?;
    assert_eq!(reference.digest, digest);
    assert_eq!(reference.kind, ArtifactReferenceKind::AssignmentInput);
    Ok(())
}

fn stored_contract(attempt_id: &str, executor_kind: ExecutionKind) -> AssignmentContract {
    AssignmentContract {
        assignment_id: AssignmentId::try_from(format!("assignment-{attempt_id}"))
            .expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from(attempt_id).expect("valid fixture attempt ID"),
        attempt_number: 1,
        idempotency_key: format!("key-{attempt_id}"),
        task_id: TaskId::try_from(format!("task-{attempt_id}")).expect("valid fixture task ID"),
        candidate_id: CandidateId::try_from("candidate-1").expect("valid fixture candidate ID"),
        execution: ExecutionContract {
            executor_kind,
            argv: vec!["fixture".into()],
            working_directory: ".".into(),
            environment: Vec::new(),
            timeout_ms: 1_000,
            bundle: ArtifactIdentity {
                digest: format!("sha256:{}", "a".repeat(64)),
                size_bytes: 1,
                media_type: "application/octet-stream".into(),
            },
            image: ArtifactIdentity {
                digest: format!("sha256:{}", "b".repeat(64)),
                size_bytes: 0,
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            },
            limits: None,
        },
        required_features: Vec::new(),
    }
}
