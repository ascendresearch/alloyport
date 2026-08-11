//! Behavioral tests for the authorized Interaction gRPC service.

use super::*;
use crate::adapters::sqlite::SqliteInteractionStore;
use crate::interaction::{InteractionEventWriter, InteractionRunAccessStore, InteractionStore};
use alloyport_events::{Event, Producer, ProducerEvent};
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::Extensions;
use tonic::metadata::{MetadataMap, MetadataValue};

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
    let access: Arc<dyn InteractionAccessPolicy> = Arc::new(TestAccessPolicy { store: durable });
    Ok((hub, access))
}

type Fixture = (Arc<InteractionHub>, Arc<dyn InteractionAccessPolicy>);

#[derive(Debug)]
struct TestAccessPolicy {
    store: Arc<dyn InteractionStore>,
}

#[tonic::async_trait]
impl InteractionAccessPolicy for TestAccessPolicy {
    async fn authorize(
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
        self.revalidate(&authorization, run_id).await?;
        Ok(authorization)
    }

    async fn revalidate(
        &self,
        authorization: &RunAuthorization,
        run_id: &str,
    ) -> Result<(), Status> {
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
