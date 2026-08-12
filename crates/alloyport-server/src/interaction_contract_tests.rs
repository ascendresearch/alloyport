//! Reusable behavioral contract for canonical Interaction persistence ports.

use crate::adapters::sqlite::SqliteInteractionStore;
use crate::interaction::{
    AppendOutcome, InteractionError, InteractionEventReader, InteractionEventWriter,
    InteractionRunAccessStore, InteractionStore, OutputAppend, RunGrantOutcome, RunRevokeOutcome,
};
use alloyport_events::{
    Authority, Event, EventEnvelope, OutputStream, Producer, ProducerEvent, SCHEMA_VERSION,
    Visibility,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Mutex, MutexGuard};

#[test]
fn sqlite_interaction_store_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    interaction_persistence_port_contract(&SqliteInteractionStore::in_memory()?)
}

#[test]
fn memory_interaction_store_satisfies_shared_port_contract() -> Result<(), Box<dyn Error>> {
    interaction_persistence_port_contract(&MemoryInteractionStore::default())
}

fn interaction_persistence_port_contract(
    store: &dyn InteractionStore,
) -> Result<(), Box<dyn Error>> {
    event_identity_contract(store)?;
    output_and_cursor_contract(store)?;
    run_access_contract(store)
}

fn event_identity_contract(store: &dyn InteractionStore) -> Result<(), Box<dyn Error>> {
    assert!(store.events("run-1")?.is_empty());
    assert_eq!(store.latest_sequence("run-1")?, None);

    let start = frame(
        "run-1",
        Event::RunStarted {
            task: "fixture".into(),
        },
    );
    let inserted = store.append("run:start", &start)?;
    let first = match inserted {
        AppendOutcome::Inserted(envelope) => envelope,
        AppendOutcome::Duplicate(_) => panic!("first append must insert"),
    };
    assert_eq!(first.sequence, 1);
    assert_eq!(first.event_id, "run-1:00000000000000000001");

    let mut replay = start.clone();
    replay.emitted_at_unix_ms = 99;
    replay.producer.instance = "restarted-server".into();
    replay.producer_sequence = Some(77);
    assert_eq!(
        store.append("run:start", &replay)?,
        AppendOutcome::Duplicate(first.clone())
    );
    assert!(matches!(
        store.append(
            "run:start",
            &frame("run-1", Event::RunStarted { task: "changed".into() })
        ),
        Err(InteractionError::ConflictingDedupKey(key)) if key == "run:start"
    ));

    let other = store.append(
        "run:start",
        &frame(
            "run-2",
            Event::RunStarted {
                task: "other".into(),
            },
        ),
    )?;
    assert_eq!(other.envelope().sequence, 1);
    Ok(())
}

fn output_and_cursor_contract(store: &dyn InteractionStore) -> Result<(), Box<dyn Error>> {
    let output = output_frame(3, "abc");
    let appended = store.append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?;
    assert_eq!(appended.missing_bytes_before, 3);
    assert_eq!(appended.outcome.envelope().sequence, 2);
    let duplicate = store.append_output("output:3", "attempt-1", 1, 3, b"abc", &output)?;
    assert!(matches!(duplicate.outcome, AppendOutcome::Duplicate(_)));
    assert_eq!(duplicate.missing_bytes_before, 0);
    assert!(matches!(
        store.append_output("output:3", "attempt-1", 1, 3, b"xyz", &output),
        Err(InteractionError::ConflictingOutput {
            attempt_id,
            stream: 1,
            byte_offset: 3,
        }) if attempt_id == "attempt-1"
    ));
    assert!(matches!(
        store.append_output(
            "output:overlap",
            "attempt-1",
            1,
            4,
            b"overlap",
            &output_frame(4, "overlap")
        ),
        Err(InteractionError::ConflictingOutput { byte_offset: 4, .. })
    ));
    let contiguous =
        store.append_output("output:6", "attempt-1", 1, 6, b"de", &output_frame(6, "de"))?;
    assert_eq!(contiguous.missing_bytes_before, 0);
    assert_eq!(contiguous.outcome.envelope().sequence, 3);
    let other_stream = store.append_output(
        "stderr:2",
        "attempt-1",
        2,
        2,
        b"err",
        &stderr_frame(2, "err"),
    )?;
    assert_eq!(other_stream.missing_bytes_before, 2);
    assert_eq!(other_stream.outcome.envelope().sequence, 4);

    assert_eq!(
        store
            .events("run-1")?
            .into_iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(store.latest_sequence("run-1")?, Some(4));
    assert_eq!(
        store
            .events_after("run-1", 1, 2)?
            .into_iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(store.events_after("run-1", 0, 0)?.is_empty());
    assert!(store.events_after("run-1", 4, 10)?.is_empty());
    Ok(())
}

fn run_access_contract(store: &dyn InteractionStore) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        store.grant_run_access("run-1", "owner-a", 1)?,
        RunGrantOutcome::Granted
    );
    assert_eq!(
        store.grant_run_access("run-1", "owner-a", 99)?,
        RunGrantOutcome::Duplicate
    );
    assert!(store.can_read_run("run-1", "owner-a")?);
    assert!(!store.can_read_run("run-1", "owner-b")?);
    assert!(matches!(
        store.revoke_run_access("run-1", "missing", 2),
        Err(InteractionError::MissingRunGrant { .. })
    ));
    assert_eq!(
        store.revoke_run_access("run-1", "owner-a", 2)?,
        RunRevokeOutcome::Revoked
    );
    assert_eq!(
        store.revoke_run_access("run-1", "owner-a", 3)?,
        RunRevokeOutcome::Duplicate
    );
    assert!(!store.can_read_run("run-1", "owner-a")?);
    assert!(matches!(
        store.grant_run_access("run-1", "owner-a", 4),
        Err(InteractionError::RevokedRunGrant { .. })
    ));
    assert_eq!(
        store.grant_run_access("run-1", "owner-b", 5)?,
        RunGrantOutcome::Granted
    );
    assert!(store.can_read_run("run-1", "owner-b")?);
    Ok(())
}

#[derive(Default)]
struct MemoryInteractionStore {
    state: Mutex<MemoryInteractionState>,
}

#[derive(Default)]
struct MemoryInteractionState {
    events: BTreeMap<String, Vec<StoredEvent>>,
    outputs: BTreeMap<(String, i32, u64), StoredOutput>,
    output_offsets: BTreeMap<(String, i32), u64>,
    grants: BTreeMap<(String, String), GrantState>,
}

struct StoredEvent {
    dedup_key: String,
    frame: ProducerEvent,
    envelope: EventEnvelope,
}

struct StoredOutput {
    payload: Vec<u8>,
    frame: ProducerEvent,
    envelope: EventEnvelope,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum GrantState {
    Active,
    Revoked,
}

impl Debug for MemoryInteractionStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryInteractionStore")
            .finish_non_exhaustive()
    }
}

impl MemoryInteractionStore {
    fn state(&self) -> Result<MutexGuard<'_, MemoryInteractionState>, InteractionError> {
        self.state
            .lock()
            .map_err(|_| InteractionError::LockPoisoned)
    }
}

impl InteractionEventWriter for MemoryInteractionStore {
    fn append(
        &self,
        dedup_key: &str,
        frame: &ProducerEvent,
    ) -> Result<AppendOutcome, InteractionError> {
        validate_event(dedup_key, frame)?;
        let mut state = self.state()?;
        append_memory_event(&mut state, dedup_key, frame)
    }

    fn append_output(
        &self,
        dedup_key: &str,
        attempt_id: &str,
        stream: i32,
        byte_offset: u64,
        payload: &[u8],
        frame: &ProducerEvent,
    ) -> Result<OutputAppend, InteractionError> {
        validate_event(dedup_key, frame)?;
        if attempt_id.trim().is_empty() {
            return Err(InteractionError::InvalidFrame(
                "output attempt identity is missing".into(),
            ));
        }
        let mut state = self.state()?;
        let output_key = (attempt_id.to_owned(), stream, byte_offset);
        if let Some(stored) = state.outputs.get(&output_key) {
            if stored.payload != payload || !same_canonical_frame(&stored.frame, frame) {
                return Err(InteractionError::ConflictingOutput {
                    attempt_id: attempt_id.into(),
                    stream,
                    byte_offset,
                });
            }
            return Ok(OutputAppend {
                outcome: AppendOutcome::Duplicate(stored.envelope.clone()),
                missing_bytes_before: 0,
            });
        }
        let stream_key = (attempt_id.to_owned(), stream);
        let expected = state.output_offsets.get(&stream_key).copied().unwrap_or(0);
        if byte_offset < expected {
            return Err(InteractionError::ConflictingOutput {
                attempt_id: attempt_id.into(),
                stream,
                byte_offset,
            });
        }
        let missing_bytes_before = byte_offset.saturating_sub(expected);
        let outcome = append_memory_event(&mut state, dedup_key, frame)?;
        let envelope = outcome.envelope().clone();
        state.outputs.insert(
            output_key,
            StoredOutput {
                payload: payload.to_vec(),
                frame: frame.clone(),
                envelope,
            },
        );
        state.output_offsets.insert(
            stream_key,
            byte_offset.saturating_add(u64::try_from(payload.len()).unwrap_or(u64::MAX)),
        );
        Ok(OutputAppend {
            outcome,
            missing_bytes_before,
        })
    }
}

impl InteractionEventReader for MemoryInteractionStore {
    fn events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError> {
        Ok(self
            .state()?
            .events
            .get(run_id)
            .into_iter()
            .flatten()
            .map(|event| event.envelope.clone())
            .collect())
    }

    fn events_after(
        &self,
        run_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>, InteractionError> {
        Ok(self
            .state()?
            .events
            .get(run_id)
            .into_iter()
            .flatten()
            .filter(|event| event.envelope.sequence > after_sequence)
            .take(limit)
            .map(|event| event.envelope.clone())
            .collect())
    }

    fn latest_sequence(&self, run_id: &str) -> Result<Option<u64>, InteractionError> {
        Ok(self
            .state()?
            .events
            .get(run_id)
            .and_then(|events| events.last())
            .map(|event| event.envelope.sequence))
    }
}

impl InteractionRunAccessStore for MemoryInteractionStore {
    fn grant_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        _now_ms: u64,
    ) -> Result<RunGrantOutcome, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let mut state = self.state()?;
        let key = (run_id.into(), owner_id.into());
        match state.grants.get(&key) {
            Some(GrantState::Active) => Ok(RunGrantOutcome::Duplicate),
            Some(GrantState::Revoked) => Err(InteractionError::RevokedRunGrant {
                run_id: run_id.into(),
                owner_id: owner_id.into(),
            }),
            None => {
                state.grants.insert(key, GrantState::Active);
                Ok(RunGrantOutcome::Granted)
            }
        }
    }

    fn revoke_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        _now_ms: u64,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        let mut state = self.state()?;
        let key = (run_id.into(), owner_id.into());
        match state.grants.get_mut(&key) {
            Some(grant @ GrantState::Active) => {
                *grant = GrantState::Revoked;
                Ok(RunRevokeOutcome::Revoked)
            }
            Some(GrantState::Revoked) => Ok(RunRevokeOutcome::Duplicate),
            None => Err(InteractionError::MissingRunGrant {
                run_id: run_id.into(),
                owner_id: owner_id.into(),
            }),
        }
    }

    fn can_read_run(&self, run_id: &str, owner_id: &str) -> Result<bool, InteractionError> {
        validate_run_owner(run_id, owner_id)?;
        Ok(
            self.state()?.grants.get(&(run_id.into(), owner_id.into()))
                == Some(&GrantState::Active),
        )
    }
}

fn append_memory_event(
    state: &mut MemoryInteractionState,
    dedup_key: &str,
    frame: &ProducerEvent,
) -> Result<AppendOutcome, InteractionError> {
    let events = state.events.entry(frame.run_id.clone()).or_default();
    if let Some(stored) = events.iter().find(|event| event.dedup_key == dedup_key) {
        if same_canonical_frame(&stored.frame, frame) {
            return Ok(AppendOutcome::Duplicate(stored.envelope.clone()));
        }
        return Err(InteractionError::ConflictingDedupKey(dedup_key.into()));
    }
    let sequence = u64::try_from(events.len())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let envelope = EventEnvelope {
        schema_version: SCHEMA_VERSION,
        event_id: format!("{}:{sequence:020}", frame.run_id),
        run_id: frame.run_id.clone(),
        task_id: frame.task_id.clone(),
        turn_id: frame.turn_id.clone(),
        operation_id: frame.operation_id.clone(),
        parent_operation_id: frame.parent_operation_id.clone(),
        producer_sequence: frame.producer_sequence,
        sequence,
        emitted_at_unix_ms: frame.emitted_at_unix_ms,
        producer: frame.producer.clone(),
        authority: frame.authority,
        visibility: frame.visibility,
        event: frame.event.clone(),
    };
    events.push(StoredEvent {
        dedup_key: dedup_key.into(),
        frame: frame.clone(),
        envelope: envelope.clone(),
    });
    Ok(AppendOutcome::Inserted(envelope))
}

fn same_canonical_frame(left: &ProducerEvent, right: &ProducerEvent) -> bool {
    left.schema_version == right.schema_version
        && left.run_id == right.run_id
        && left.task_id == right.task_id
        && left.turn_id == right.turn_id
        && left.operation_id == right.operation_id
        && left.parent_operation_id == right.parent_operation_id
        && left.producer.component == right.producer.component
        && left.authority == right.authority
        && left.visibility == right.visibility
        && left.event == right.event
}

fn validate_event(dedup_key: &str, frame: &ProducerEvent) -> Result<(), InteractionError> {
    if dedup_key.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "deduplication key is missing".into(),
        ));
    }
    if frame.schema_version != SCHEMA_VERSION {
        return Err(InteractionError::InvalidFrame(format!(
            "unsupported schema {}",
            frame.schema_version
        )));
    }
    if frame.run_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run identity is missing".into(),
        ));
    }
    Ok(())
}

fn validate_run_owner(run_id: &str, owner_id: &str) -> Result<(), InteractionError> {
    if run_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run grant identity is missing".into(),
        ));
    }
    if owner_id.trim().is_empty() {
        return Err(InteractionError::InvalidFrame(
            "run grant owner is missing".into(),
        ));
    }
    Ok(())
}

fn frame(run_id: &str, event: Event) -> ProducerEvent {
    let mut frame = ProducerEvent::new(run_id, Producer::new("controller", "server"), event);
    frame.task_id = Some(run_id.into());
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame.emitted_at_unix_ms = 1;
    frame
}

fn output_frame(byte_offset: u64, text: &str) -> ProducerEvent {
    let mut frame = frame(
        "run-1",
        Event::CommandOutput {
            stream: OutputStream::Stdout,
            byte_offset,
            text: text.into(),
            display_sanitized: false,
        },
    );
    frame.operation_id = Some("attempt-1".into());
    frame
}

fn stderr_frame(byte_offset: u64, text: &str) -> ProducerEvent {
    let mut frame = output_frame(byte_offset, text);
    frame.event = Event::CommandOutput {
        stream: OutputStream::Stderr,
        byte_offset,
        text: text.into(),
        display_sanitized: false,
    };
    frame
}
