//! Durable outbox delivery, ephemeral framing, and terminal Artifact publication.

use super::{OutboundWorker, WorkerError};
use crate::executor::terminal_reference_intents;
use crate::journal::WorkerOutboxPayload;
use crate::wire_mapping::outbox_to_wire;
use alloyport_proto::v1::{WorkerToServer, worker_to_server};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

impl OutboundWorker {
    pub(super) async fn send_ephemeral(
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        message: worker_to_server::Message,
    ) -> Result<(), WorkerError> {
        let sequence = *next_worker_sequence;
        *next_worker_sequence += 1;
        outbound
            .send(WorkerToServer {
                sequence,
                acknowledges_server_through,
                message_id: String::new(),
                message: Some(message),
            })
            .await
            .map_err(|_| WorkerError::StreamClosed)
    }

    pub(super) async fn publish_pending_terminal_artifacts(&self) -> Result<(), WorkerError> {
        let Some(publisher) = self.artifact_publisher.as_ref() else {
            return Ok(());
        };
        let pending = self.state.pending_outbox_async().await?;
        for entry in pending {
            let WorkerOutboxPayload::ExecutionFinished {
                attempt_id,
                finished,
                ..
            } = entry.payload
            else {
                continue;
            };
            publisher
                .publish(&terminal_reference_intents(&attempt_id, &finished))
                .await?;
        }
        Ok(())
    }

    pub(super) async fn send_pending_outbox(
        &self,
        connection_id: &str,
        outbound: &mpsc::Sender<WorkerToServer>,
        next_worker_sequence: &mut u64,
        acknowledges_server_through: u64,
        delivered_message_ids: &mut BTreeSet<String>,
    ) -> Result<(), WorkerError> {
        let pending = self.state.pending_outbox_async().await?;
        for entry in pending {
            if delivered_message_ids.contains(&entry.message_id) {
                continue;
            }
            let sequence = *next_worker_sequence;
            self.state
                .record_delivery_async(connection_id.to_owned(), sequence, entry.message_id.clone())
                .await?;
            *next_worker_sequence += 1;
            delivered_message_ids.insert(entry.message_id.clone());
            outbound
                .send(WorkerToServer {
                    sequence,
                    acknowledges_server_through,
                    message_id: entry.message_id,
                    message: Some(outbox_to_wire(entry.payload)),
                })
                .await
                .map_err(|_| WorkerError::StreamClosed)?;
        }
        Ok(())
    }

    pub(super) async fn available_slots(&self) -> Result<u32, WorkerError> {
        let active = u32::try_from(self.state.attempt_count_async().await?).unwrap_or(u32::MAX);
        Ok(self.hello.capabilities.as_ref().map_or(0, |capabilities| {
            capabilities.max_concurrency.saturating_sub(active)
        }))
    }
}
