//! Unit tests for server control orchestration.

use super::*;
use crate::storage::{ArtifactIdentity, AssignmentWriteRepository, ExecutionContract};
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

#[test]
fn service_accepts_independently_supplied_repository_ports() -> Result<(), Box<dyn Error>> {
    let connections: Arc<dyn WorkerConnectionRepository> =
        Arc::new(SqliteControlRepository::in_memory()?);
    let assignment_reads: Arc<dyn AssignmentReadRepository> =
        Arc::new(SqliteControlRepository::in_memory()?);
    let assignment_writes: Arc<dyn AssignmentWriteRepository> =
        Arc::new(SqliteControlRepository::in_memory()?);
    let attempts: Arc<dyn AttemptLifecycleRepository> =
        Arc::new(SqliteControlRepository::in_memory()?);
    let outbox: Arc<dyn ServerOutboxRepository> = Arc::new(SqliteControlRepository::in_memory()?);
    let interactions: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);

    let service = WorkerControlService::with_repository_capabilities(
        connections,
        assignment_reads,
        assignment_writes,
        attempts,
        outbox,
        interactions,
        Arc::new(ManualClock::new(1)),
    );

    assert_eq!(service.assignment_state("missing-attempt")?, None);
    Ok(())
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

    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let reaper_service = service.clone();
    let reaper_receiver = receiver.clone();
    let reaper =
        tokio::spawn(async move { reaper_service.run_lease_reaper_until(reaper_receiver).await });
    let reconciler =
        tokio::spawn(async move { service.run_preparation_reconciler_until(receiver).await });
    tokio::task::yield_now().await;
    shutdown.send(true)?;
    tokio::time::timeout(std::time::Duration::from_secs(1), reaper).await???;
    tokio::time::timeout(std::time::Duration::from_secs(1), reconciler).await???;
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
async fn fixed_device_assignments_grant_only_a_published_size_matched_input_bundle()
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
    let mut ascend_assignment = assignment.clone();
    ascend_assignment.assignment_id = "assignment-ascend-1".into();
    ascend_assignment.attempt_id = "attempt-ascend-1".into();
    ascend_assignment.idempotency_key = "ascend-add-v1".into();
    ascend_assignment.task_id = "task-ascend-1".into();
    ascend_assignment.candidate_id = "candidate-ascend-1".into();
    ascend_assignment
        .execution
        .as_mut()
        .expect("fixture execution")
        .executor_kind = WireExecutorKind::AscendFixture.into();
    ascend_assignment
        .execution
        .as_mut()
        .expect("fixture execution")
        .argv = vec!["ascend-add-v1".into()];
    ascend_assignment.required_features = vec!["ascend-fixture-v1".into()];
    assert_eq!(
        service.enqueue_assignment("cuda-1", assignment).await?,
        EnqueueOutcome::Pending
    );
    let reference = uploads.reference("cuda-1", "input:attempt-1:bundle")?;
    assert_eq!(reference.digest, digest);
    assert_eq!(reference.kind, ArtifactReferenceKind::AssignmentInput);
    assert_eq!(
        service
            .enqueue_assignment("ascend-1", ascend_assignment)
            .await?,
        EnqueueOutcome::Pending
    );
    let ascend_reference = uploads.reference("ascend-1", "input:attempt-ascend-1:bundle")?;
    assert_eq!(ascend_reference.digest, digest);
    assert_eq!(
        ascend_reference.kind,
        ArtifactReferenceKind::AssignmentInput
    );
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
                digest: format!("sha256:{}", "a".repeat(64))
                    .parse()
                    .expect("valid fixture digest"),
                size_bytes: 1,
                media_type: "application/octet-stream".into(),
            },
            image: ArtifactIdentity {
                digest: format!("sha256:{}", "b".repeat(64))
                    .parse()
                    .expect("valid fixture digest"),
                size_bytes: 0,
                media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            },
            limits: None,
        },
        required_features: Vec::new(),
    }
}
