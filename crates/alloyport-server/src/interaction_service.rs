//! Authorized public replay and live subscription over canonical interaction envelopes.

use crate::identity::{ConnectionIdentityResolver, ResolvedConnectionIdentity};
use crate::interaction::{InteractionError, InteractionHub, InteractionStore, SubscriptionError};
use alloyport_events::EventEnvelope;
use alloyport_proto::interaction_v1::interaction_service_server::InteractionService;
use alloyport_proto::interaction_v1::{CanonicalEvent, ReplayRunRequest, SubscribeRunRequest};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::metadata::MetadataMap;
use tonic::{Extensions, Request, Response, Status};

const DEFAULT_REPLAY_LIMIT: usize = 256;
const MAX_REPLAY_LIMIT: usize = 4_096;
const DEFAULT_DELIVERY_CAPACITY: usize = 32;
const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Authorization retained for the lifetime of a replay or subscription request.
#[derive(Clone, Debug)]
pub struct RunAuthorization {
    owner_id: String,
    identity: Option<ResolvedConnectionIdentity>,
}

impl RunAuthorization {
    #[must_use]
    pub fn local(owner_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            identity: None,
        }
    }

    #[must_use]
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }
}

/// Resolves request identity and checks run visibility without trusting request body ownership.
pub trait InteractionAccessPolicy: Debug + Send + Sync {
    /// Authorizes one request and returns state suitable for later revalidation.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when identity is absent, inactive, or lacks run access.
    fn authorize(
        &self,
        metadata: &MetadataMap,
        extensions: &Extensions,
        run_id: &str,
    ) -> Result<RunAuthorization, Status>;

    /// Revalidates a live stream against credential and grant revocation.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when access is no longer active.
    fn revalidate(&self, authorization: &RunAuthorization, run_id: &str) -> Result<(), Status>;
}

/// Production policy backed by verified certificate enrollment and durable run grants.
#[derive(Clone, Debug)]
pub struct EnrolledInteractionAccessPolicy {
    interactions: Arc<dyn InteractionStore>,
    identities: Arc<dyn ConnectionIdentityResolver>,
}

impl EnrolledInteractionAccessPolicy {
    #[must_use]
    pub fn new(
        interactions: Arc<dyn InteractionStore>,
        identities: Arc<dyn ConnectionIdentityResolver>,
    ) -> Self {
        Self {
            interactions,
            identities,
        }
    }

    fn authorize_owner(&self, owner_id: &str, run_id: &str) -> Result<(), Status> {
        match self.interactions.can_read_run(run_id, owner_id) {
            Ok(true) => Ok(()),
            Ok(false) => Err(Status::permission_denied(
                "run is not visible to this owner",
            )),
            Err(error) => Err(interaction_status(&error)),
        }
    }
}

impl InteractionAccessPolicy for EnrolledInteractionAccessPolicy {
    fn authorize(
        &self,
        _metadata: &MetadataMap,
        extensions: &Extensions,
        run_id: &str,
    ) -> Result<RunAuthorization, Status> {
        let identity = self.identities.resolve_identity(extensions)?;
        self.authorize_owner(&identity.owner_id, run_id)?;
        Ok(RunAuthorization {
            owner_id: identity.owner_id.clone(),
            identity: Some(identity),
        })
    }

    fn revalidate(&self, authorization: &RunAuthorization, run_id: &str) -> Result<(), Status> {
        let identity = authorization
            .identity
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("verified interaction identity is missing"))?;
        self.identities.revalidate(identity)?;
        self.authorize_owner(&authorization.owner_id, run_id)
    }
}

#[derive(Clone, Debug)]
pub struct InteractionServiceImpl {
    hub: Arc<InteractionHub>,
    access: Arc<dyn InteractionAccessPolicy>,
    replay_default: usize,
    replay_max: usize,
    delivery_capacity: usize,
    delivery_timeout: Duration,
}

impl InteractionServiceImpl {
    #[must_use]
    pub fn new(hub: Arc<InteractionHub>, access: Arc<dyn InteractionAccessPolicy>) -> Self {
        Self {
            hub,
            access,
            replay_default: DEFAULT_REPLAY_LIMIT,
            replay_max: MAX_REPLAY_LIMIT,
            delivery_capacity: DEFAULT_DELIVERY_CAPACITY,
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_limits(
        hub: Arc<InteractionHub>,
        access: Arc<dyn InteractionAccessPolicy>,
        replay_default: usize,
        replay_max: usize,
        delivery_capacity: usize,
        delivery_timeout: Duration,
    ) -> Self {
        Self {
            hub,
            access,
            replay_default,
            replay_max,
            delivery_capacity,
            delivery_timeout,
        }
    }

    fn authorize<T>(&self, request: &Request<T>, run_id: &str) -> Result<RunAuthorization, Status> {
        validate_run_id(run_id)?;
        self.access
            .authorize(request.metadata(), request.extensions(), run_id)
    }
}

#[tonic::async_trait]
impl InteractionService for InteractionServiceImpl {
    type ReplayRunStream =
        Pin<Box<dyn Stream<Item = Result<CanonicalEvent, Status>> + Send + 'static>>;
    type SubscribeRunStream =
        Pin<Box<dyn Stream<Item = Result<CanonicalEvent, Status>> + Send + 'static>>;

    async fn replay_run(
        &self,
        request: Request<ReplayRunRequest>,
    ) -> Result<Response<Self::ReplayRunStream>, Status> {
        let run_id = request.get_ref().run_id.clone();
        let authorization = self.authorize(&request, &run_id)?;
        self.access.revalidate(&authorization, &run_id)?;
        let after_sequence = request.get_ref().after_sequence;
        let limit = replay_limit(
            request.get_ref().limit,
            self.replay_default,
            self.replay_max,
        )?;
        let hub = Arc::clone(&self.hub);
        let events = tokio::task::spawn_blocking(move || {
            let latest = hub.latest_sequence(&run_id)?.unwrap_or(0);
            if after_sequence > latest {
                return Err(InteractionError::InvalidCursor {
                    run_id,
                    after_sequence,
                    latest_sequence: latest,
                });
            }
            hub.events_after(&run_id, after_sequence, limit)
        })
        .await
        .map_err(|error| Status::internal(format!("interaction replay task failed: {error}")))?
        .map_err(|error| interaction_status(&error))?;
        self.access
            .revalidate(&authorization, &request.get_ref().run_id)?;
        let stream = tokio_stream::iter(
            events
                .into_iter()
                .map(|envelope| canonical_event(&envelope)),
        );
        Ok(Response::new(Box::pin(stream)))
    }

    async fn subscribe_run(
        &self,
        request: Request<SubscribeRunRequest>,
    ) -> Result<Response<Self::SubscribeRunStream>, Status> {
        let run_id = request.get_ref().run_id.clone();
        let authorization = self.authorize(&request, &run_id)?;
        let after_sequence = request.get_ref().after_sequence;
        let hub = Arc::clone(&self.hub);
        let subscription_run_id = run_id.clone();
        let mut subscription =
            tokio::task::spawn_blocking(move || hub.subscribe(subscription_run_id, after_sequence))
                .await
                .map_err(|error| {
                    Status::internal(format!("interaction subscribe task failed: {error}"))
                })?
                .map_err(|error| interaction_status(&error))?;
        let (sender, receiver) = mpsc::channel(self.delivery_capacity);
        let access = Arc::clone(&self.access);
        let delivery_timeout = self.delivery_timeout;
        tokio::spawn(async move {
            loop {
                if let Err(status) = revalidate_access(&access, &authorization, &run_id).await {
                    let _ = sender.send(Err(status)).await;
                    break;
                }
                let envelope = match subscription.recv().await {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        let _ = sender.send(Err(subscription_status(error))).await;
                        break;
                    }
                };
                if let Err(status) = revalidate_access(&access, &authorization, &run_id).await {
                    let _ = sender.send(Err(status)).await;
                    break;
                }
                let cursor = envelope.sequence;
                let event = canonical_event(&envelope);
                match tokio::time::timeout(delivery_timeout, sender.send(event)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => break,
                    Err(_) => {
                        let _ = sender
                            .send(Err(Status::resource_exhausted(format!(
                                "interaction client is too slow; reconnect with after_sequence={}",
                                cursor.saturating_sub(1)
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

async fn revalidate_access(
    access: &Arc<dyn InteractionAccessPolicy>,
    authorization: &RunAuthorization,
    run_id: &str,
) -> Result<(), Status> {
    let access = Arc::clone(access);
    let authorization = authorization.clone();
    let run_id = run_id.to_owned();
    tokio::task::spawn_blocking(move || access.revalidate(&authorization, &run_id))
        .await
        .map_err(|error| {
            Status::internal(format!("interaction authorization task failed: {error}"))
        })?
}

fn replay_limit(requested: u32, default: usize, maximum: usize) -> Result<usize, Status> {
    let requested = if requested == 0 {
        default
    } else {
        usize::try_from(requested)
            .map_err(|_| Status::invalid_argument("replay limit exceeds this platform"))?
    };
    if requested > maximum {
        return Err(Status::invalid_argument(format!(
            "replay limit exceeds server maximum {maximum}"
        )));
    }
    Ok(requested)
}

fn validate_run_id(run_id: &str) -> Result<(), Status> {
    if run_id.trim().is_empty() {
        Err(Status::invalid_argument("run ID is missing"))
    } else {
        Ok(())
    }
}

fn canonical_event(envelope: &EventEnvelope) -> Result<CanonicalEvent, Status> {
    Ok(CanonicalEvent {
        envelope_json: serde_json::to_vec(&envelope)
            .map_err(|error| Status::internal(format!("serialize canonical event: {error}")))?,
    })
}

fn subscription_status(error: SubscriptionError) -> Status {
    match error {
        SubscriptionError::Store(error) => interaction_status(&error),
        SubscriptionError::SlowConsumer {
            last_sequence,
            skipped_notifications,
        } => Status::resource_exhausted(format!(
            "interaction subscriber fell behind by {skipped_notifications} notifications; reconnect with after_sequence={last_sequence}"
        )),
        SubscriptionError::SequenceGap {
            expected_sequence,
            observed_sequence,
        } => Status::data_loss(format!(
            "interaction sequence gap: expected {expected_sequence}, observed {observed_sequence}"
        )),
        SubscriptionError::Closed => Status::unavailable("interaction subscription closed"),
    }
}

fn interaction_status(error: &InteractionError) -> Status {
    let detail = error.to_string();
    match error {
        InteractionError::InvalidFrame(_)
        | InteractionError::ConflictingDedupKey(_)
        | InteractionError::ConflictingOutput { .. }
        | InteractionError::InvalidCursor { .. }
        | InteractionError::RevokedRunGrant { .. }
        | InteractionError::MissingRunGrant { .. }
        | InteractionError::ValueOutOfRange(_) => Status::invalid_argument(detail),
        InteractionError::Sqlite(_)
        | InteractionError::Serialization(_)
        | InteractionError::InvalidSubscriptionCapacity
        | InteractionError::LockPoisoned => Status::internal(detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interaction::SqliteInteractionStore;
    use alloyport_events::{Event, Producer, ProducerEvent};
    use tokio_stream::StreamExt;
    use tonic::Code;
    use tonic::metadata::MetadataValue;

    #[tokio::test]
    async fn authorized_replay_is_bounded_and_returns_exact_canonical_json()
    -> Result<(), Box<dyn std::error::Error>> {
        let (hub, access) = fixture()?;
        hub.grant_run_access("run-1", "owner-a", 1)?;
        append_warning(&hub, "run-1", 1)?;
        append_warning(&hub, "run-1", 2)?;
        let service = InteractionServiceImpl::with_limits(
            Arc::clone(&hub),
            access,
            1,
            2,
            2,
            Duration::from_secs(1),
        );

        let mut replay = service
            .replay_run(authorized(
                "owner-a",
                ReplayRunRequest {
                    run_id: "run-1".into(),
                    after_sequence: 0,
                    limit: 1,
                },
            ))
            .await?
            .into_inner();
        let wire = replay.next().await.transpose()?.expect("one replay event");
        let envelope: EventEnvelope = serde_json::from_slice(&wire.envelope_json)?;
        assert_eq!(envelope.sequence, 1);
        assert!(replay.next().await.is_none());

        let Err(denied) = service
            .replay_run(authorized(
                "owner-b",
                ReplayRunRequest {
                    run_id: "run-1".into(),
                    after_sequence: 0,
                    limit: 1,
                },
            ))
            .await
        else {
            panic!("another owner must not see whether the run has events");
        };
        assert_eq!(denied.code(), Code::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn subscription_crosses_to_live_and_stops_after_grant_revocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (hub, access) = fixture()?;
        hub.grant_run_access("run-1", "owner-a", 1)?;
        append_warning(&hub, "run-1", 1)?;
        let service = InteractionServiceImpl::new(Arc::clone(&hub), access);
        let mut stream = service
            .subscribe_run(authorized(
                "owner-a",
                SubscribeRunRequest {
                    run_id: "run-1".into(),
                    after_sequence: 0,
                },
            ))
            .await?
            .into_inner();

        assert_eq!(next_sequence(&mut stream).await?, 1);
        append_warning(&hub, "run-1", 2)?;
        assert_eq!(next_sequence(&mut stream).await?, 2);
        hub.revoke_run_access("run-1", "owner-a", 3)?;
        append_warning(&hub, "run-1", 3)?;
        let revoked = stream
            .next()
            .await
            .expect("revocation terminates the stream")
            .expect_err("revoked grant must fail closed");
        assert_eq!(revoked.code(), Code::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn public_delivery_timeout_reports_a_resumable_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let (hub, access) = fixture()?;
        hub.grant_run_access("run-1", "owner-a", 1)?;
        let service = InteractionServiceImpl::with_limits(
            Arc::clone(&hub),
            access,
            1,
            2,
            1,
            Duration::from_millis(10),
        );
        let mut stream = service
            .subscribe_run(authorized(
                "owner-a",
                SubscribeRunRequest {
                    run_id: "run-1".into(),
                    after_sequence: 0,
                },
            ))
            .await?
            .into_inner();
        for sequence in 1..=3 {
            append_warning(&hub, "run-1", sequence)?;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;

        assert_eq!(next_sequence(&mut stream).await?, 1);
        let lagged = stream
            .next()
            .await
            .expect("slow-consumer status follows queued data")
            .expect_err("slow consumer must terminate explicitly");
        assert_eq!(lagged.code(), Code::ResourceExhausted);
        assert!(lagged.message().contains("after_sequence=1"));
        Ok(())
    }

    fn fixture() -> Result<Fixture, InteractionError> {
        let durable: Arc<dyn InteractionStore> = Arc::new(SqliteInteractionStore::in_memory()?);
        let hub = Arc::new(InteractionHub::new(Arc::clone(&durable), 8, 1)?);
        let access: Arc<dyn InteractionAccessPolicy> =
            Arc::new(TestAccessPolicy { store: durable });
        Ok((hub, access))
    }

    type Fixture = (Arc<InteractionHub>, Arc<dyn InteractionAccessPolicy>);

    #[derive(Debug)]
    struct TestAccessPolicy {
        store: Arc<dyn InteractionStore>,
    }

    impl InteractionAccessPolicy for TestAccessPolicy {
        fn authorize(
            &self,
            metadata: &MetadataMap,
            _extensions: &Extensions,
            run_id: &str,
        ) -> Result<RunAuthorization, Status> {
            let owner_id = metadata
                .get("x-test-owner")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Status::unauthenticated("test owner is missing"))?;
            let authorization = RunAuthorization::local(owner_id);
            self.revalidate(&authorization, run_id)?;
            Ok(authorization)
        }

        fn revalidate(&self, authorization: &RunAuthorization, run_id: &str) -> Result<(), Status> {
            match self.store.can_read_run(run_id, authorization.owner_id()) {
                Ok(true) => Ok(()),
                Ok(false) => Err(Status::permission_denied("run is not visible")),
                Err(error) => Err(interaction_status(&error)),
            }
        }
    }

    fn authorized<T>(owner_id: &'static str, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("x-test-owner", MetadataValue::from_static(owner_id));
        request
    }

    fn append_warning(
        hub: &InteractionHub,
        run_id: &str,
        sequence: u64,
    ) -> Result<(), InteractionError> {
        let mut frame = ProducerEvent::new(
            run_id,
            Producer::new("controller", "test"),
            Event::Warning {
                message: format!("warning {sequence}"),
            },
        );
        frame.task_id = Some(run_id.into());
        hub.append(&format!("warning:{sequence}"), &frame)?;
        Ok(())
    }

    async fn next_sequence(
        stream: &mut (impl Stream<Item = Result<CanonicalEvent, Status>> + Unpin),
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let wire = stream.next().await.transpose()?.expect("next event");
        let envelope: EventEnvelope = serde_json::from_slice(&wire.envelope_json)?;
        Ok(envelope.sequence)
    }
}
