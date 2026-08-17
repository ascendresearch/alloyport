use super::*;

fn submission<'a>(
    owner_id: &'a str,
    request_id: &'a str,
    task_id: &'a str,
    project_name: &'a str,
    project_digest: Sha256Digest,
    created_at_ms: u64,
) -> MigrationTaskSubmission<'a> {
    MigrationTaskSubmission {
        owner_id,
        request_id,
        task_id,
        project_name,
        project_digest,
        project_size_bytes: 7,
        file_count: 1,
        created_at_ms,
    }
}

/// Only a failed migration returns to the queue, and it keeps its identity when it does.
///
/// The identity is the point: the Episode is derived from the task id, so a resumed task
/// recovers the Episode it already built instead of starting a new one. A retry mints a new id
/// and throws that work away, which is what four consecutive live runs did.
#[test]
fn only_a_failed_migration_resumes_and_it_keeps_its_task_identity() -> Result<(), MigrationTaskError>
{
    let store = SqliteMigrationTaskStore::in_memory()?;
    let digest = Sha256Digest::digest_bytes(b"project");
    store.submit(submission(
        "owner-a",
        "request-a",
        "task-a",
        "project-a",
        digest,
        10,
    ))?;
    store.claim_next()?.expect("captured task");

    // Running is not finished; there is nothing to resume yet.
    assert!(store.resume("owner-a", "task-a").is_err());

    store.finish("task-a", MigrationTaskState::Failed)?;
    assert!(
        store.claim_next()?.is_none(),
        "a failed task stays out of the queue"
    );

    let resumed = store.resume("owner-a", "task-a")?;
    assert_eq!(
        resumed.task_id, "task-a",
        "resuming must not mint a new identity"
    );
    assert_eq!(resumed.state, MigrationTaskState::Captured);
    assert_eq!(
        store
            .claim_next()?
            .expect("resumed task is queued again")
            .task_id,
        "task-a"
    );

    // Another owner cannot resume it, and a completed task is not resumable at all.
    store.finish("task-a", MigrationTaskState::Failed)?;
    assert!(store.resume("owner-b", "task-a").is_err());
    store.resume("owner-a", "task-a")?;
    store.claim_next()?;
    store.finish("task-a", MigrationTaskState::Completed)?;
    assert!(store.resume("owner-a", "task-a").is_err());
    Ok(())
}

#[test]
fn claim_resume_cancel_and_finish_are_durable_state_transitions() -> Result<(), MigrationTaskError>
{
    let store = SqliteMigrationTaskStore::in_memory()?;
    let digest = Sha256Digest::digest_bytes(b"project");
    let submitted = store.submit(submission(
        "owner-a",
        "request-a",
        "task-a",
        "project-a",
        digest,
        10,
    ))?;
    assert_eq!(submitted.state, MigrationTaskState::Captured);

    let claimed = store.claim_next()?.expect("captured task");
    assert_eq!(claimed.owner_id, "owner-a");
    assert_eq!(claimed.state, MigrationTaskState::Running);
    let resumed = store.claim_next()?.expect("running task resumes first");
    assert_eq!(resumed.task_id, "task-a");
    store.finish("task-a", MigrationTaskState::Completed)?;
    assert!(store.claim_next()?.is_none());

    store.submit(submission(
        "owner-a",
        "request-b",
        "task-b",
        "project-b",
        digest,
        11,
    ))?;
    store.cancel("owner-a", "task-b")?;
    assert!(store.is_cancelled("task-b")?);
    assert!(store.claim_next()?.is_none());
    Ok(())
}

/// A retry with the same request identity is the same task; changed bytes are a conflict.
#[test]
fn one_request_identity_is_idempotent_and_changed_bytes_conflict() -> Result<(), MigrationTaskError>
{
    let store = SqliteMigrationTaskStore::in_memory()?;
    let digest = Sha256Digest::digest_bytes(b"project");
    let first = store.submit(submission(
        "owner-a",
        "request-a",
        "task-a",
        "project-a",
        digest,
        10,
    ))?;
    let repeat = store.submit(submission(
        "owner-a",
        "request-a",
        "task-a",
        "project-a",
        digest,
        99,
    ))?;
    assert_eq!(first, repeat, "a repeat must not mint a second task");
    let conflict = store.submit(submission(
        "owner-a",
        "request-a",
        "task-a",
        "project-a",
        Sha256Digest::digest_bytes(b"different project"),
        10,
    ));
    assert!(matches!(conflict, Err(MigrationTaskError::Conflict)));
    Ok(())
}
