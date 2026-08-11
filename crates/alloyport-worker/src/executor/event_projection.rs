//! Projection of typed execution observations into canonical interaction events.

use super::{ExecutionChunk, ExecutionStream, ExecutorInput};
use crate::journal::StoredArtifact;
use alloyport_events::{
    ArtifactRef as EventArtifactRef, Authority, Event, OutputStream as EventOutputStream, Producer,
    ProducerEvent, Visibility,
};

pub(crate) fn producer_event(
    worker_id: &str,
    input: &ExecutorInput,
    event: Event,
) -> ProducerEvent {
    let mut frame = ProducerEvent::new(
        input.task_id.clone(),
        Producer::new("alloyport-worker", worker_id),
        event,
    );
    frame.task_id = Some(input.task_id.clone());
    frame.operation_id = Some(input.attempt_id.clone());
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame
}

pub(crate) fn output_event(
    worker_id: &str,
    input: &ExecutorInput,
    chunk: &ExecutionChunk,
) -> ProducerEvent {
    let text = String::from_utf8_lossy(&chunk.bytes);
    let display_sanitized = matches!(text, std::borrow::Cow::Owned(_));
    producer_event(
        worker_id,
        input,
        Event::CommandOutput {
            stream: match chunk.stream {
                ExecutionStream::Stdout => EventOutputStream::Stdout,
                ExecutionStream::Stderr => EventOutputStream::Stderr,
            },
            byte_offset: chunk.byte_offset,
            text: text.into_owned(),
            display_sanitized,
        },
    )
}

pub(crate) fn event_artifact(artifact: &StoredArtifact, reference: &str) -> EventArtifactRef {
    EventArtifactRef {
        digest: artifact.digest.clone(),
        media_type: artifact.media_type.clone(),
        size_bytes: artifact.size_bytes,
        reference: reference.into(),
    }
}
