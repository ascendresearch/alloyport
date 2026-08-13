//! Authorized public replay and live subscription over canonical interaction envelopes.

mod access;

pub use access::{
    EnrolledInteractionAccessPolicy, InteractionAccessPolicy, LocalInteractionAccessPolicy,
    RunAuthorization,
};

use crate::grpc_status::interaction_status;
use crate::interaction::{
    InteractionError, InteractionEventReader, InteractionHub, SubscriptionError,
};
use crate::persistence::ServerPersistence;
use alloyport_events::EventEnvelope;
use alloyport_proto::interaction_v1::interaction_service_server::InteractionService;
use alloyport_proto::interaction_v1::{CanonicalEvent, ReplayRunRequest, SubscribeRunRequest};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

const DEFAULT_REPLAY_LIMIT: usize = 256;
const MAX_REPLAY_LIMIT: usize = 4_096;
const DEFAULT_DELIVERY_CAPACITY: usize = 32;
const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

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

    async fn authorize<T>(
        &self,
        request: &Request<T>,
        run_id: &str,
    ) -> Result<RunAuthorization, Status> {
        validate_run_id(run_id)?;
        self.access
            .authorize(request.metadata(), request.extensions(), run_id)
            .await
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
        let authorization = self.authorize(&request, &run_id).await?;
        self.access.revalidate(&authorization, &run_id).await?;
        let after_sequence = request.get_ref().after_sequence;
        let limit = replay_limit(
            request.get_ref().limit,
            self.replay_default,
            self.replay_max,
        )?;
        let hub = Arc::clone(&self.hub);
        let events = ServerPersistence::default()
            .run(move || {
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
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| interaction_status(&error))?;
        self.access
            .revalidate(&authorization, &request.get_ref().run_id)
            .await?;
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
        let authorization = self.authorize(&request, &run_id).await?;
        let after_sequence = request.get_ref().after_sequence;
        let hub = Arc::clone(&self.hub);
        let subscription_run_id = run_id.clone();
        let mut subscription = ServerPersistence::default()
            .run(move || hub.subscribe(subscription_run_id, after_sequence))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
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
    access.revalidate(authorization, run_id).await
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

#[cfg(test)]
#[path = "interaction_service_tests.rs"]
mod tests;
