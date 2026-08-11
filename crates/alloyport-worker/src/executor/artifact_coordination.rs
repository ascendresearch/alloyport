//! Execution Artifact identity, local spooling, and remote publication boundary.

use super::ExecutionRuntimeError;
use crate::journal::{StoredArtifact, StoredFinished};
use alloyport_artifacts::upload::ArtifactReferenceKind;
use alloyport_artifacts::{ArtifactStore, IngestRequest};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) const STDOUT_MEDIA_TYPE: &str = "application/vnd.alloyport.stdout";
pub(crate) const STDERR_MEDIA_TYPE: &str = "application/vnd.alloyport.stderr";
pub(crate) const RECEIPT_MEDIA_TYPE: &str = "application/vnd.alloyport.run-receipt+json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactReferenceIntent {
    pub reference_key: String,
    pub kind: ArtifactReferenceKind,
    pub purpose: String,
    pub artifact: StoredArtifact,
}

/// Stable failure categories exposed by pluggable Artifact publishers.
///
/// Adapter-specific errors stay behind the publisher boundary. Callers may make retry and
/// observability decisions from these variants without parsing an implementation's error text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactPublicationError {
    LocalArtifact(String),
    Unavailable(String),
    Rejected(String),
    Internal(String),
}

impl Display for ArtifactPublicationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalArtifact(detail) => write!(formatter, "local Artifact failure: {detail}"),
            Self::Unavailable(detail) => write!(formatter, "publisher unavailable: {detail}"),
            Self::Rejected(detail) => write!(formatter, "publication rejected: {detail}"),
            Self::Internal(detail) => write!(formatter, "publisher internal failure: {detail}"),
        }
    }
}

impl Error for ArtifactPublicationError {}

/// Publishes worker-local execution artifacts before terminal lifecycle state becomes reportable.
pub trait ArtifactPublisher: Debug + Send + Sync {
    /// Publishes every reference intent, idempotently resuming any prior partial publication.
    fn publish<'a>(
        &'a self,
        references: &'a [ArtifactReferenceIntent],
    ) -> Pin<Box<dyn Future<Output = Result<(), ArtifactPublicationError>> + Send + 'a>>;
}

pub(crate) async fn store_artifact(
    artifacts: Arc<dyn ArtifactStore>,
    bytes: Vec<u8>,
    media_type: &'static str,
) -> Result<StoredArtifact, ExecutionRuntimeError> {
    let result = tokio::task::spawn_blocking(move || {
        let mut source = Cursor::new(bytes);
        artifacts.ingest(&mut source, IngestRequest::unverified())
    })
    .await
    .map_err(ExecutionRuntimeError::TaskJoin)??;
    Ok(StoredArtifact {
        digest: result.artifact.digest,
        size_bytes: result.artifact.size_bytes,
        media_type: media_type.into(),
    })
}

pub(crate) fn terminal_reference_intents(
    attempt_id: &str,
    finished: &StoredFinished,
) -> Vec<ArtifactReferenceIntent> {
    let mut references = Vec::new();
    for (artifact, suffix, purpose) in [
        (
            finished.stdout.as_ref(),
            "stdout",
            "complete attempt stdout",
        ),
        (
            finished.stderr.as_ref(),
            "stderr",
            "complete attempt stderr",
        ),
    ] {
        if let Some(artifact) = artifact {
            references.push(ArtifactReferenceIntent {
                reference_key: format!("output:{attempt_id}:{suffix}"),
                kind: ArtifactReferenceKind::AssignmentOutput,
                purpose: purpose.into(),
                artifact: artifact.clone(),
            });
        }
    }
    if let Some(receipt) = finished.receipt.as_ref() {
        references.push(ArtifactReferenceIntent {
            reference_key: format!("receipt:{attempt_id}"),
            kind: ArtifactReferenceKind::Receipt,
            purpose: "attempt run receipt".into(),
            artifact: receipt.clone(),
        });
    }
    references
}
