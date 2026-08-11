//! Durable replay-to-live interaction delivery decorator.

use super::{
    AppendOutcome, InteractionError, InteractionEventReader, InteractionEventWriter,
    InteractionRunAccessStore, InteractionStore, OutputAppend, RunGrantOutcome, RunRevokeOutcome,
};
use alloyport_events::{EventEnvelope, ProducerEvent};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Append-through store that publishes only newly inserted canonical envelopes.
#[derive(Debug)]
pub struct InteractionHub {
    store: Arc<dyn InteractionStore>,
    live: Mutex<BTreeMap<String, broadcast::Sender<EventEnvelope>>>,
    live_capacity: usize,
    replay_batch_size: usize,
}

impl InteractionHub {
    /// Creates a bounded notification hub over one authoritative durable store.
    ///
    /// # Errors
    ///
    /// Returns an error when either bound is zero.
    pub fn new(
        store: Arc<dyn InteractionStore>,
        live_capacity: usize,
        replay_batch_size: usize,
    ) -> Result<Self, InteractionError> {
        if live_capacity == 0 || replay_batch_size == 0 {
            return Err(InteractionError::InvalidSubscriptionCapacity);
        }
        Ok(Self {
            store,
            live: Mutex::new(BTreeMap::new()),
            live_capacity,
            replay_batch_size,
        })
    }

    /// Opens a run-scoped stream after the client's last applied canonical sequence.
    ///
    /// The receiver is attached before the durable high-water mark is read, closing the race
    /// between historical replay and live notification delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable high-water mark cannot be read.
    pub fn subscribe(
        &self,
        run_id: impl Into<String>,
        after_sequence: u64,
    ) -> Result<InteractionSubscription, InteractionError> {
        let run_id = run_id.into();
        let receiver = self.receiver(&run_id);
        let replay_through = self.store.latest_sequence(&run_id)?.unwrap_or(0);
        if after_sequence > replay_through {
            return Err(InteractionError::InvalidCursor {
                run_id,
                after_sequence,
                latest_sequence: replay_through,
            });
        }
        Ok(InteractionSubscription {
            store: Arc::clone(&self.store),
            receiver,
            run_id,
            last_sequence: after_sequence,
            replay_through,
            replay_batch_size: self.replay_batch_size,
            replay: VecDeque::new(),
            terminated: false,
        })
    }

    fn publish(&self, outcome: &AppendOutcome) {
        if let AppendOutcome::Inserted(envelope) = outcome {
            let live = self
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(sender) = live.get(&envelope.run_id) {
                let _ = sender.send(envelope.clone());
            }
        }
    }

    fn receiver(&self, run_id: &str) -> broadcast::Receiver<EventEnvelope> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.entry(run_id.to_owned())
            .or_insert_with(|| broadcast::channel(self.live_capacity).0)
            .subscribe()
    }
}

impl InteractionEventWriter for InteractionHub {
    fn append(
        &self,
        dedup_key: &str,
        frame: &ProducerEvent,
    ) -> Result<AppendOutcome, InteractionError> {
        let outcome = self.store.append(dedup_key, frame)?;
        self.publish(&outcome);
        Ok(outcome)
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
        let appended =
            self.store
                .append_output(dedup_key, attempt_id, stream, byte_offset, payload, frame)?;
        self.publish(&appended.outcome);
        Ok(appended)
    }
}

impl InteractionEventReader for InteractionHub {
    fn events(&self, run_id: &str) -> Result<Vec<EventEnvelope>, InteractionError> {
        self.store.events(run_id)
    }

    fn events_after(
        &self,
        run_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>, InteractionError> {
        self.store.events_after(run_id, after_sequence, limit)
    }

    fn latest_sequence(&self, run_id: &str) -> Result<Option<u64>, InteractionError> {
        self.store.latest_sequence(run_id)
    }
}

impl InteractionRunAccessStore for InteractionHub {
    fn grant_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunGrantOutcome, InteractionError> {
        self.store.grant_run_access(run_id, owner_id, now_ms)
    }

    fn revoke_run_access(
        &self,
        run_id: &str,
        owner_id: &str,
        now_ms: u64,
    ) -> Result<RunRevokeOutcome, InteractionError> {
        self.store.revoke_run_access(run_id, owner_id, now_ms)
    }

    fn can_read_run(&self, run_id: &str, owner_id: &str) -> Result<bool, InteractionError> {
        self.store.can_read_run(run_id, owner_id)
    }
}

/// Terminal reason for a live subscription.
#[derive(Debug)]
pub enum SubscriptionError {
    Store(InteractionError),
    SlowConsumer {
        last_sequence: u64,
        skipped_notifications: u64,
    },
    SequenceGap {
        expected_sequence: u64,
        observed_sequence: u64,
    },
    Closed,
}

impl Display for SubscriptionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => Display::fmt(error, formatter),
            Self::SlowConsumer {
                last_sequence,
                skipped_notifications,
            } => write!(
                formatter,
                "interaction subscriber after sequence {last_sequence} fell behind by {skipped_notifications} notifications"
            ),
            Self::SequenceGap {
                expected_sequence,
                observed_sequence,
            } => write!(
                formatter,
                "interaction subscriber expected sequence {expected_sequence} but observed {observed_sequence}"
            ),
            Self::Closed => write!(formatter, "interaction subscription is closed"),
        }
    }
}

impl Error for SubscriptionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::SlowConsumer { .. } | Self::SequenceGap { .. } | Self::Closed => None,
        }
    }
}

/// One run-scoped replay-to-live cursor.
#[derive(Debug)]
pub struct InteractionSubscription {
    store: Arc<dyn InteractionStore>,
    receiver: broadcast::Receiver<EventEnvelope>,
    run_id: String,
    last_sequence: u64,
    replay_through: u64,
    replay_batch_size: usize,
    replay: VecDeque<EventEnvelope>,
    terminated: bool,
}

impl InteractionSubscription {
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Returns the next canonical envelope, replaying the durable snapshot before live delivery.
    ///
    /// # Errors
    ///
    /// Terminates explicitly on durable gaps, storage failure, or bounded-channel lag. A client can
    /// reconnect after [`Self::last_sequence`] to resume from authoritative storage.
    pub async fn recv(&mut self) -> Result<EventEnvelope, SubscriptionError> {
        if self.terminated {
            return Err(SubscriptionError::Closed);
        }
        loop {
            if let Some(envelope) = self.replay.pop_front() {
                return self.accept(envelope);
            }
            if self.last_sequence < self.replay_through {
                let events = self
                    .store
                    .events_after(&self.run_id, self.last_sequence, self.replay_batch_size)
                    .map_err(|error| self.terminate(SubscriptionError::Store(error)))?;
                self.replay.extend(
                    events
                        .into_iter()
                        .take_while(|event| event.sequence <= self.replay_through),
                );
                if self.replay.is_empty() {
                    let expected_sequence = self.last_sequence.saturating_add(1);
                    return Err(self.terminate(SubscriptionError::SequenceGap {
                        expected_sequence,
                        observed_sequence: self.replay_through,
                    }));
                }
                continue;
            }
            match self.receiver.recv().await {
                Ok(envelope) if envelope.run_id != self.run_id => {}
                Ok(envelope) if envelope.sequence <= self.last_sequence => {}
                Ok(envelope) => return self.accept(envelope),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(self.terminate(SubscriptionError::SlowConsumer {
                        last_sequence: self.last_sequence,
                        skipped_notifications: skipped,
                    }));
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(self.terminate(SubscriptionError::Closed));
                }
            }
        }
    }

    fn accept(&mut self, envelope: EventEnvelope) -> Result<EventEnvelope, SubscriptionError> {
        let expected_sequence = self.last_sequence.saturating_add(1);
        if envelope.sequence != expected_sequence {
            return Err(self.terminate(SubscriptionError::SequenceGap {
                expected_sequence,
                observed_sequence: envelope.sequence,
            }));
        }
        self.last_sequence = envelope.sequence;
        Ok(envelope)
    }

    fn terminate(&mut self, error: SubscriptionError) -> SubscriptionError {
        self.terminated = true;
        error
    }
}
