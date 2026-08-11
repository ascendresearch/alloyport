
use super::*;
use alloyport_events::{Authority, OutputStream, Producer, Visibility};
use std::error::Error;
use std::sync::Arc;

#[test]
fn durable_sequence_dedup_conflict_gap_and_restart() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("events.sqlite3");
    let store = SqliteInteractionStore::open(&database)?;
    let start = frame(Event::RunStarted {
        task: "fixture".into(),
    });
    let mut replayed_start = start.clone();
    replayed_start.emitted_at_unix_ms = 99;
    replayed_start.producer.instance = "restarted-server".into();
    assert!(matches!(
        store.append("run:start", &replayed_start)?,
        AppendOutcome::Inserted(_)
    ));
    assert!(matches!(
        store.append("run:start", &start)?,
        AppendOutcome::Duplicate(_)
    ));
    let conflicting = frame(Event::RunStarted {
        task: "changed".into(),
    });
    assert!(matches!(
        store.append("run:start", &conflicting),
        Err(InteractionError::ConflictingDedupKey(_))
    ));

    let output = output_frame(3, "abc");
    let appended = store.append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?;
    assert_eq!(appended.missing_bytes_before, 3);
    assert!(matches!(
        store
            .append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?
            .outcome,
        AppendOutcome::Duplicate(_)
    ));
    assert!(matches!(
        store.append_output("output:3", "attempt-1", 1, 3, b"xyz", &output),
        Err(InteractionError::ConflictingOutput { .. })
    ));
    let overlap = output_frame(4, "overlap");
    assert!(matches!(
        store.append_output("output:4", "attempt-1", 1, 4, b"overlap", &overlap),
        Err(InteractionError::ConflictingOutput { .. })
    ));
    drop(store);

    let reopened = SqliteInteractionStore::open(&database)?;
    assert!(matches!(
        reopened
            .append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?
            .outcome,
        AppendOutcome::Duplicate(_)
    ));
    let events = reopened.events("task-1")?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[1].sequence, 2);
    assert_eq!(reopened.latest_sequence("task-1")?, Some(2));
    assert_eq!(
        reopened
            .events_after("task-1", 1, 1)?
            .into_iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert!(reopened.events_after("task-1", 0, 0)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn subscription_replays_then_crosses_to_live_without_a_gap() -> Result<(), Box<dyn Error>> {
    let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
    let hub = InteractionHub::new(durable, 4, 1)?;
    hub.append(
        "run:start",
        &frame(Event::RunStarted {
            task: "fixture".into(),
        }),
    )?;

    let mut subscription = hub.subscribe("task-1", 0)?;
    hub.append(
        "warning:live",
        &frame(Event::Warning {
            message: "arrived while replay was pending".into(),
        }),
    )?;

    assert_eq!(subscription.recv().await?.sequence, 1);
    assert_eq!(subscription.recv().await?.sequence, 2);
    assert_eq!(subscription.last_sequence(), 2);
    Ok(())
}

#[tokio::test]
async fn slow_subscriber_terminates_and_reconnects_from_durable_cursor()
-> Result<(), Box<dyn Error>> {
    let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
    let hub = InteractionHub::new(durable, 2, 1)?;
    let mut slow = hub.subscribe("task-1", 0)?;
    for sequence in 1..=3 {
        hub.append(
            &format!("warning:{sequence}"),
            &frame(Event::Warning {
                message: format!("warning {sequence}"),
            }),
        )?;
    }

    assert!(matches!(
        slow.recv().await,
        Err(SubscriptionError::SlowConsumer {
            last_sequence: 0,
            skipped_notifications: 1
        })
    ));
    assert!(matches!(slow.recv().await, Err(SubscriptionError::Closed)));

    let mut resumed = hub.subscribe("task-1", slow.last_sequence())?;
    for expected in 1..=3 {
        assert_eq!(resumed.recv().await?.sequence, expected);
    }
    Ok(())
}

#[tokio::test]
async fn unrelated_run_pressure_does_not_lag_subscriber() -> Result<(), Box<dyn Error>> {
    let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
    let hub = InteractionHub::new(durable, 1, 1)?;
    let mut subscription = hub.subscribe("task-1", 0)?;
    for sequence in 1..=3 {
        hub.append(
            &format!("other:{sequence}"),
            &frame_for(
                "task-2",
                Event::Warning {
                    message: format!("unrelated {sequence}"),
                },
            ),
        )?;
    }
    hub.append(
        "task-1:warning",
        &frame(Event::Warning {
            message: "relevant".into(),
        }),
    )?;

    let envelope = subscription.recv().await?;
    assert_eq!(envelope.run_id, "task-1");
    assert_eq!(envelope.sequence, 1);
    Ok(())
}

#[test]
fn subscription_rejects_cursor_beyond_durable_high_water() -> Result<(), Box<dyn Error>> {
    let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
    let hub = InteractionHub::new(durable, 2, 1)?;
    assert!(matches!(
        hub.subscribe("task-1", 1),
        Err(InteractionError::InvalidCursor {
            after_sequence: 1,
            latest_sequence: 0,
            ..
        })
    ));
    Ok(())
}

#[test]
fn run_grants_are_durable_idempotent_and_revocation_is_terminal() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("events.sqlite3");
    let store = SqliteInteractionStore::open(&database)?;
    assert_eq!(
        store.grant_run_access("task-1", "owner-a", 1)?,
        RunGrantOutcome::Granted
    );
    assert_eq!(
        store.grant_run_access("task-1", "owner-a", 99)?,
        RunGrantOutcome::Duplicate
    );
    assert!(store.can_read_run("task-1", "owner-a")?);
    assert!(!store.can_read_run("task-1", "owner-b")?);
    assert_eq!(
        store.revoke_run_access("task-1", "owner-a", 2)?,
        RunRevokeOutcome::Revoked
    );
    assert_eq!(
        store.revoke_run_access("task-1", "owner-a", 3)?,
        RunRevokeOutcome::Duplicate
    );
    assert!(!store.can_read_run("task-1", "owner-a")?);
    assert!(matches!(
        store.grant_run_access("task-1", "owner-a", 4),
        Err(InteractionError::RevokedRunGrant { .. })
    ));
    drop(store);

    let reopened = SqliteInteractionStore::open(database)?;
    assert!(!reopened.can_read_run("task-1", "owner-a")?);
    assert_eq!(
        reopened.grant_run_access("task-1", "owner-b", 5)?,
        RunGrantOutcome::Granted
    );
    assert!(reopened.can_read_run("task-1", "owner-b")?);
    Ok(())
}

#[test]
fn controller_redaction_strips_terminal_controls_and_common_credentials() {
    let mut event = Event::CommandOutput {
        stream: OutputStream::Stdout,
        byte_offset: 0,
        text: "\u{1b}[31mTOKEN=top-secret\u{1b}[0m\nBearer credential\nordinary secret text".into(),
        display_sanitized: false,
    };
    redact_worker_event(&mut event);
    assert_eq!(
        event,
        Event::CommandOutput {
            stream: OutputStream::Stdout,
            byte_offset: 0,
            text: "TOKEN=[REDACTED]\nBearer [REDACTED]\nordinary secret text".into(),
            display_sanitized: true,
        }
    );
}

fn frame(event: Event) -> ProducerEvent {
    frame_for("task-1", event)
}

fn frame_for(run_id: &str, event: Event) -> ProducerEvent {
    let mut frame = ProducerEvent::new(run_id, Producer::new("controller", "server"), event);
    frame.task_id = Some(run_id.into());
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame.emitted_at_unix_ms = 1;
    frame
}

fn output_frame(byte_offset: u64, text: &str) -> ProducerEvent {
    let mut frame = frame(Event::CommandOutput {
        stream: OutputStream::Stdout,
        byte_offset,
        text: text.into(),
        display_sanitized: false,
    });
    frame.operation_id = Some("attempt-1".into());
    frame
}
