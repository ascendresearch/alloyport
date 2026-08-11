use super::*;
use crate::storage::{
    AssignmentContract, AssignmentDeliveryPreparation, AssignmentReadRepository,
    AssignmentWriteRepository, AttemptLifecycleRepository, AttemptState, CancellationStoreOutcome,
    ConnectionRegistration, ObservationDisposition, ObservedAttempt, RepositoryError,
    ServerOutboxFrame, ServerOutboxRepository, StoreAssignmentOutcome, WorkerConnectionRepository,
    WorkerRegistration,
};
use alloyport_core::{AssignmentId, AttemptId, AttemptOutcome, CandidateId, ExecutionKind, TaskId};
use std::error::Error;

#[test]
fn immutable_assignment_is_idempotent_and_survives_reopen() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("control.sqlite3");
    let contract = contract();
    {
        let repository = SqliteControlRepository::open(&database)?;
        assert_eq!(
            repository.store_assignment("worker-1", &contract, 1_000)?,
            StoreAssignmentOutcome::Inserted
        );
        assert_eq!(
            repository.store_assignment("worker-1", &contract, 1_001)?,
            StoreAssignmentOutcome::Duplicate
        );
        let mut changed = contract.clone();
        changed.execution.argv = vec!["different".to_owned()];
        assert!(matches!(
            repository.store_assignment("worker-1", &changed, 1_002),
            Err(RepositoryError::ConflictingAttempt(attempt)) if attempt == "attempt-1"
        ));
    }

    let reopened = SqliteControlRepository::open(&database)?;
    let recovered = reopened
        .assignment("attempt-1")?
        .expect("stored assignment is recovered");
    assert_eq!(recovered.contract, contract);
    assert_eq!(recovered.state, AttemptState::Preparing);
    Ok(())
}

#[test]
fn preparing_assignment_is_not_replayable_until_side_effects_complete() -> Result<(), Box<dyn Error>>
{
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;

    assert!(repository.replayable_assignments("worker-1")?.is_empty());
    assert!(repository.mark_assignment_dispatchable("attempt-1", "worker-1", 1_001)?);
    assert!(!repository.mark_assignment_dispatchable("attempt-1", "worker-1", 1_002)?);
    assert_eq!(repository.replayable_assignments("worker-1")?.len(), 1);
    Ok(())
}

#[test]
fn deferred_preparation_rotates_behind_newer_work() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;
    let mut second = contract();
    second.assignment_id = AssignmentId::try_from("assignment-2")?;
    second.attempt_id = AttemptId::try_from("attempt-2")?;
    repository.store_assignment("worker-1", &second, 1_001)?;

    assert_eq!(
        repository.preparing_assignments(1)?[0]
            .contract
            .attempt_id
            .as_str(),
        "attempt-1"
    );
    assert!(repository.defer_assignment_preparation("attempt-1", "worker-1", 2_000)?);
    assert_eq!(
        repository.preparing_assignments(1)?[0]
            .contract
            .attempt_id
            .as_str(),
        "attempt-2"
    );
    Ok(())
}

#[test]
fn assignment_delivery_rolls_back_lease_and_state_when_outbox_insert_fails()
-> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;
    prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;

    let mut second = contract();
    second.assignment_id = AssignmentId::try_from("assignment-2")?;
    second.attempt_id = AttemptId::try_from("attempt-2")?;
    repository.store_assignment("worker-1", &second, 1_001)?;
    repository.mark_assignment_dispatchable("attempt-2", "worker-1", 1_001)?;
    let failed = repository.prepare_assignment_delivery(&AssignmentDeliveryPreparation {
        frame: ServerOutboxFrame {
            connection_id: "test-connection".into(),
            sequence: 1_000,
            message_id: "assignment:attempt-2".into(),
            worker_id: "worker-1".into(),
            kind: ServerFrameKind::Assignment,
            attempt_id: Some("attempt-2".into()),
        },
        lease_id: "lease:attempt-2".into(),
        last_worker_sequence: 1,
        last_server_acknowledged_by_worker: 0,
        now_ms: 1_001,
        lease_duration_ms: 100,
    });
    assert!(failed.is_err());
    assert_eq!(
        repository
            .assignment("attempt-2")?
            .expect("failed preparation keeps the assignment")
            .state,
        AttemptState::Dispatchable
    );
    assert!(repository.lease("attempt-2")?.is_none());
    let persisted_sequence = repository.connection()?.query_row(
        "SELECT last_server_sequence FROM worker_connections WHERE connection_id = ?1",
        ["test-connection"],
        |row| row.get::<_, i64>(0),
    )?;
    assert_eq!(persisted_sequence, 1_000);
    Ok(())
}

#[test]
fn migration_upgrades_the_initial_server_schema() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("old.sqlite3");
    let connection = Connection::open(&database)?;
    connection.execute_batch(
        "CREATE TABLE worker_connections (
                 connection_id TEXT PRIMARY KEY,
                 worker_id TEXT NOT NULL,
                 instance_id TEXT NOT NULL,
                 connected_at_ms INTEGER NOT NULL,
                 disconnected_at_ms INTEGER,
                 last_worker_sequence INTEGER NOT NULL,
                 last_server_sequence INTEGER NOT NULL
             );
             CREATE TABLE assignments (
                 attempt_id TEXT PRIMARY KEY,
                 assignment_id TEXT NOT NULL,
                 worker_id TEXT NOT NULL,
                 contract_json TEXT NOT NULL,
                 state INTEGER NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 last_sent_at_ms INTEGER
             );",
    )?;
    drop(connection);

    let repository = SqliteControlRepository::open(&database)?;
    let database = repository.connection()?;
    let version_two: i64 = database.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = 2",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(version_two, 1);
    assert!(column_exists_raw(
        &database,
        "worker_connections",
        "last_server_acknowledged_by_worker"
    )?);
    assert!(column_exists_raw(
        &database,
        "assignments",
        "cancellation_reason"
    )?);
    assert!(column_exists_raw(
        &database,
        "server_outbox_frames",
        "message_id"
    )?);
    let version_three: i64 = database.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = 3",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(version_three, 1);
    Ok(())
}

#[test]
fn lease_expiry_retains_a_late_result_as_stale() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    let contract = contract();
    repository.store_assignment("worker-1", &contract, 1_000)?;
    prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
    assert_eq!(
        repository.observe_attempt(&observation(
            1_001,
            AttemptObservation::Accepted {
                already_known: false,
            },
        ))?,
        ObservationDisposition::Applied
    );

    assert_eq!(repository.expire_leases(1_100)?, vec!["attempt-1"]);
    assert_eq!(
        repository
            .assignment("attempt-1")?
            .expect("attempt remains stored")
            .state,
        AttemptState::LeaseExpired
    );
    let disposition = repository.observe_attempt(&observation(
        1_101,
        AttemptObservation::Finished(Box::new(FinishedObservation {
            outcome: AttemptOutcome::Succeeded,
            exit_code: Some(0),
            elapsed_ms: 90,
            receipt: Some(artifact('c')),
            stdout: None,
            stderr: None,
            detail: "late success".to_owned(),
        })),
    ))?;
    assert_eq!(disposition, ObservationDisposition::Stale);
    assert_eq!(
        repository
            .assignment("attempt-1")?
            .expect("stale result cannot remove attempt")
            .state,
        AttemptState::LeaseExpired
    );
    let observation_count: i64 = repository.connection()?.query_row(
        "SELECT COUNT(*) FROM attempt_observations WHERE attempt_id = 'attempt-1'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(observation_count, 2);
    Ok(())
}

#[test]
fn expired_attempt_reassignment_creates_a_fresh_linked_contract() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;
    prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
    assert!(matches!(
        repository.reassign_expired("attempt-1", "worker-2", "attempt-2", 1_050),
        Err(RepositoryError::InvalidTransition {
            from: AttemptState::Sent,
            to: AttemptState::Dispatchable,
        })
    ));
    assert_eq!(repository.expire_leases(1_100)?, vec!["attempt-1"]);
    assert!(matches!(
        repository.reassign_expired("attempt-1", "worker-2", "  ", 1_101),
        Err(RepositoryError::InvalidIdentity(_))
    ));

    let reassigned = repository.reassign_expired("attempt-1", "worker-2", "attempt-2", 1_102)?;
    assert_eq!(reassigned.outcome, StoreAssignmentOutcome::Inserted);
    assert_eq!(reassigned.assignment.worker_id, "worker-2");
    assert_eq!(
        reassigned.assignment.contract.attempt_id.as_str(),
        "attempt-2"
    );
    assert_eq!(reassigned.assignment.contract.attempt_number, 2);
    assert_eq!(reassigned.assignment.state, AttemptState::Preparing);
    assert_eq!(
        repository
            .assignment("attempt-1")?
            .expect("expired source remains auditable")
            .state,
        AttemptState::LeaseExpired
    );
    assert_eq!(
        repository
            .reassign_expired("attempt-1", "worker-2", "attempt-2", 1_102)?
            .outcome,
        StoreAssignmentOutcome::Duplicate
    );

    assert_eq!(
        repository.observe_attempt(&observation(
            1_103,
            AttemptObservation::Finished(Box::new(FinishedObservation {
                outcome: AttemptOutcome::Succeeded,
                exit_code: Some(0),
                elapsed_ms: 100,
                receipt: None,
                stdout: None,
                stderr: None,
                detail: "late old result".to_owned(),
            })),
        ))?,
        ObservationDisposition::Stale
    );
    Ok(())
}

#[test]
fn active_heartbeat_renews_the_lease() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;
    prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
    repository.renew_active_leases("worker-1", &["attempt-1".to_owned()], 1_050, 100)?;
    assert!(repository.expire_leases(1_100)?.is_empty());
    assert_eq!(repository.expire_leases(1_150)?, vec!["attempt-1"]);
    Ok(())
}

#[test]
fn heartbeat_after_expiry_cannot_resurrect_a_lease() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;
    prepare_test_assignment(&repository, "attempt-1", 1_000, 100)?;
    repository.renew_active_leases("worker-1", &["attempt-1".to_owned()], 1_101, 100)?;
    assert_eq!(
        repository
            .assignment("attempt-1")?
            .expect("attempt remains durable")
            .state,
        AttemptState::LeaseExpired
    );
    assert_eq!(
        repository
            .lease("attempt-1")?
            .expect("lease remains auditable")
            .expired_at_ms,
        Some(1_101)
    );
    Ok(())
}

#[test]
fn cancellation_cannot_resurrect_expired_work() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.store_assignment("worker-1", &contract(), 1_000)?;
    assert_eq!(
        repository
            .request_cancellation("attempt-1", "cancel queued", 1_001)?
            .outcome,
        CancellationStoreOutcome::CancelledBeforeSend
    );
    assert_eq!(
        repository
            .assignment("attempt-1")?
            .expect("cancelled assignment remains auditable")
            .state,
        AttemptState::Cancelled
    );

    let mut second = contract();
    second.assignment_id = AssignmentId::try_from("assignment-2")?;
    second.attempt_id = AttemptId::try_from("attempt-2")?;
    repository.store_assignment("worker-1", &second, 2_000)?;
    prepare_test_assignment(&repository, "attempt-2", 2_000, 100)?;
    assert_eq!(repository.expire_leases(2_100)?, vec!["attempt-2"]);
    assert_eq!(
        repository
            .request_cancellation("attempt-2", "too late", 2_101)?
            .outcome,
        CancellationStoreOutcome::AlreadyTerminal
    );
    assert_eq!(
        repository
            .assignment("attempt-2")?
            .expect("expired attempt remains auditable")
            .state,
        AttemptState::LeaseExpired
    );
    Ok(())
}

#[test]
fn server_outbox_compacts_only_cumulatively_acknowledged_frames() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    for (sequence, kind) in [
        (2, ServerFrameKind::Assignment),
        (3, ServerFrameKind::Cancel),
    ] {
        repository.record_server_frame(
            &ServerOutboxFrame {
                connection_id: "connection-1".to_owned(),
                sequence,
                message_id: format!("fixture:{sequence}"),
                worker_id: "worker-1".to_owned(),
                kind,
                attempt_id: Some("attempt-1".to_owned()),
            },
            1_000 + sequence,
        )?;
    }
    assert_eq!(repository.server_outbox_len("connection-1")?, 2);
    assert_eq!(
        repository.compact_server_frames("connection-1", 2, 1_010)?,
        1
    );
    assert_eq!(repository.server_outbox_len("connection-1")?, 1);
    assert_eq!(
        repository.compact_server_frames("connection-1", 3, 1_011)?,
        1
    );
    assert_eq!(repository.server_outbox_len("connection-1")?, 0);
    Ok(())
}

#[test]
fn orphaned_server_frames_are_retained_until_the_policy_cutoff() -> Result<(), Box<dyn Error>> {
    let repository = SqliteControlRepository::in_memory()?;
    repository.register_worker(
        &WorkerRegistration {
            protocol_major: 1,
            protocol_minor: 2,
            worker_id: "worker-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            worker_version: "test".to_owned(),
            features: Vec::new(),
            capabilities: WorkerCapabilities {
                backend: 1,
                architecture: "test".to_owned(),
                device_count: 1,
                max_concurrency: 1,
                driver_version: "test".to_owned(),
                toolkit_version: "test".to_owned(),
                container_runtime: "test".to_owned(),
            },
        },
        &ConnectionRegistration {
            connection_id: "connection-1".to_owned(),
            worker_id: "worker-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            connected_at_ms: 1_000,
        },
    )?;
    repository.record_server_frame(
        &ServerOutboxFrame {
            connection_id: "connection-1".to_owned(),
            sequence: 2,
            message_id: "assignment:attempt-1".to_owned(),
            worker_id: "worker-1".to_owned(),
            kind: ServerFrameKind::Assignment,
            attempt_id: Some("attempt-1".to_owned()),
        },
        1_001,
    )?;
    repository.disconnect("connection-1", 1_010)?;

    assert_eq!(repository.prune_orphaned_server_frames(1_010)?, 0);
    assert_eq!(repository.server_outbox_len("connection-1")?, 1);
    assert_eq!(repository.prune_orphaned_server_frames(1_011)?, 1);
    assert_eq!(repository.server_outbox_len("connection-1")?, 0);
    Ok(())
}

fn observation(at_ms: u64, observation: AttemptObservation) -> ObservedAttempt {
    ObservedAttempt {
        assignment_id: AssignmentId::try_from("assignment-1").expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from("attempt-1").expect("valid fixture attempt ID"),
        worker_id: "worker-1".to_owned(),
        observed_at_ms: at_ms,
        observation,
    }
}

fn prepare_test_assignment(
    repository: &SqliteControlRepository,
    attempt_id: &str,
    now_ms: u64,
    lease_duration_ms: u64,
) -> Result<(), RepositoryError> {
    let connection_id = "test-connection";
    let connection_exists = repository.connection()?.query_row(
        "SELECT EXISTS(SELECT 1 FROM worker_connections WHERE connection_id = ?1)",
        [connection_id],
        |row| row.get::<_, bool>(0),
    )?;
    if !connection_exists {
        repository.register_worker(
            &WorkerRegistration {
                protocol_major: 1,
                protocol_minor: 0,
                worker_id: "worker-1".into(),
                instance_id: "test-instance".into(),
                worker_version: "test".into(),
                features: Vec::new(),
                capabilities: WorkerCapabilities {
                    backend: 1,
                    architecture: "test".into(),
                    device_count: 1,
                    max_concurrency: 1,
                    driver_version: "test".into(),
                    toolkit_version: "test".into(),
                    container_runtime: "test".into(),
                },
            },
            &ConnectionRegistration {
                connection_id: connection_id.into(),
                worker_id: "worker-1".into(),
                instance_id: "test-instance".into(),
                connected_at_ms: now_ms,
            },
        )?;
    }
    repository.mark_assignment_dispatchable(attempt_id, "worker-1", now_ms)?;
    repository.prepare_assignment_delivery(&AssignmentDeliveryPreparation {
        frame: ServerOutboxFrame {
            connection_id: connection_id.into(),
            sequence: now_ms,
            message_id: format!("assignment:{attempt_id}"),
            worker_id: "worker-1".into(),
            kind: ServerFrameKind::Assignment,
            attempt_id: Some(attempt_id.into()),
        },
        lease_id: format!("lease:{attempt_id}"),
        last_worker_sequence: 1,
        last_server_acknowledged_by_worker: 0,
        now_ms,
        lease_duration_ms,
    })?;
    Ok(())
}

fn contract() -> AssignmentContract {
    AssignmentContract {
        assignment_id: AssignmentId::try_from("assignment-1").expect("valid fixture assignment ID"),
        attempt_id: AttemptId::try_from("attempt-1").expect("valid fixture attempt ID"),
        attempt_number: 1,
        idempotency_key: "task-1:build".to_owned(),
        task_id: TaskId::try_from("task-1").expect("valid fixture task ID"),
        candidate_id: CandidateId::try_from("candidate-1").expect("valid fixture candidate ID"),
        execution: ExecutionContract {
            executor_kind: ExecutionKind::Container,
            argv: vec!["true".to_owned()],
            working_directory: "source".to_owned(),
            environment: Vec::new(),
            timeout_ms: 30_000,
            bundle: artifact('a'),
            image: artifact('b'),
            limits: None,
        },
        required_features: Vec::new(),
    }
}

fn artifact(byte: char) -> ArtifactIdentity {
    ArtifactIdentity {
        digest: format!("sha256:{}", byte.to_string().repeat(64))
            .parse()
            .expect("valid fixture digest"),
        size_bytes: 1,
        media_type: "application/octet-stream".to_owned(),
    }
}

fn column_exists_raw(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, rusqlite::Error> {
    let query = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    connection
        .query_row(&query, [column], |row| row.get::<_, i64>(0))
        .map(|count| count == 1)
}
