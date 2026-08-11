//! gRPC control-stream adapter and wire/domain mappings.

use super::WorkerControlService;
use crate::interaction::{self, InteractionError};
use crate::storage::{
    ArtifactIdentity, AssignmentContract, EnvironmentEntry, ExecutionContract, RepositoryError,
    ResourceContract, WorkerCapabilities, WorkerRegistration,
};
use alloyport_core::{AttemptId, ExecutionKind, NetworkPolicy};
use alloyport_events::{
    ArtifactRef as EventArtifactRef, Authority, Event, Producer, ProducerEvent, Visibility,
};
use alloyport_proto::v1::worker_control_server::WorkerControl;
use alloyport_proto::v1::{
    ArtifactRef, Assignment, EnvironmentVariable, ExecutionSpec, ResourceLimits, ServerToWorker,
    WorkerHello, WorkerToServer, worker_to_server,
};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    type OpenControlStreamStream =
        Pin<Box<dyn Stream<Item = Result<ServerToWorker, Status>> + Send + 'static>>;

    async fn open_control_stream(
        &self,
        request: Request<Streaming<WorkerToServer>>,
    ) -> Result<Response<Self::OpenControlStreamStream>, Status> {
        let authenticated_identity = match self.identity_resolver.as_ref() {
            Some(resolver) => Some(resolver.resolve_identity(request.extensions()).await?),
            None => None,
        };
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("worker stream ended before hello"))?;
        if first.sequence != 1 {
            return Err(Status::invalid_argument("hello must have sequence 1"));
        }
        if first.acknowledges_server_through != 0 {
            return Err(Status::invalid_argument(
                "hello cannot acknowledge a server connection that is not open",
            ));
        }
        if !first.message_id.is_empty() {
            return Err(Status::invalid_argument("hello cannot carry a message ID"));
        }
        let Some(worker_to_server::Message::Hello(hello)) = first.message else {
            return Err(Status::invalid_argument(
                "first worker message must be hello",
            ));
        };
        alloyport_proto::validate_worker_hello(&hello)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if authenticated_identity
            .as_ref()
            .is_some_and(|identity| identity.owner_id != hello.worker_id)
        {
            return Err(Status::permission_denied(
                "worker hello identity does not match the enrolled client certificate",
            ));
        }
        if let Some(identity) = authenticated_identity.as_ref()
            && let Some(resolver) = self.identity_resolver.as_ref()
        {
            resolver.revalidate(identity).await?;
        }

        let worker_id = hello.worker_id.clone();
        let (outbound, receiver) = mpsc::channel(64);
        let (connection_id, initial_messages) = self
            .register(hello, outbound.clone())
            .await
            .map_err(repository_status)?;
        for message in initial_messages {
            outbound
                .send(Ok(message))
                .await
                .map_err(|_| Status::unavailable("worker response stream closed"))?;
        }

        tokio::spawn(self.clone().consume_stream(
            worker_id,
            connection_id,
            authenticated_identity,
            inbound,
            outbound,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

pub(super) fn repository_status(error: RepositoryError) -> Status {
    match error {
        RepositoryError::NotFound(detail) => Status::not_found(detail),
        RepositoryError::IdentityMismatch(detail) => Status::permission_denied(detail),
        RepositoryError::InvalidIdentity(detail) => Status::invalid_argument(detail),
        RepositoryError::InvalidTransition { .. } => Status::failed_precondition(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

pub(super) fn expected_worker_message_id(
    message: Option<&worker_to_server::Message>,
) -> Option<String> {
    let (kind, attempt_id) = match message? {
        worker_to_server::Message::AssignmentAccepted(accepted) => {
            ("assignment-accepted", accepted.attempt_id.as_str())
        }
        worker_to_server::Message::AssignmentRejected(rejected) => {
            ("assignment-rejected", rejected.attempt_id.as_str())
        }
        worker_to_server::Message::ExecutionStarted(started) => {
            ("execution-started", started.attempt_id.as_str())
        }
        worker_to_server::Message::ExecutionFinished(finished) => {
            ("execution-finished", finished.attempt_id.as_str())
        }
        worker_to_server::Message::CancellationAcknowledged(acknowledged) => (
            "cancellation-acknowledged",
            acknowledged.attempt_id.as_str(),
        ),
        worker_to_server::Message::Hello(_)
        | worker_to_server::Message::Heartbeat(_)
        | worker_to_server::Message::OutputChunk(_)
        | worker_to_server::Message::Status(_) => return None,
    };
    Some(format!("{kind}:{attempt_id}"))
}

pub(super) fn hello_to_registration(hello: &WorkerHello) -> WorkerRegistration {
    let capabilities = hello
        .capabilities
        .as_ref()
        .expect("validated worker hello contains capabilities");
    WorkerRegistration {
        protocol_major: hello.protocol_major,
        protocol_minor: hello.protocol_minor,
        worker_id: hello.worker_id.clone(),
        instance_id: hello.instance_id.clone(),
        worker_version: hello.worker_version.clone(),
        features: hello.features.clone(),
        capabilities: WorkerCapabilities {
            backend: capabilities.backend,
            architecture: capabilities.architecture.clone(),
            device_count: capabilities.device_count,
            max_concurrency: capabilities.max_concurrency,
            driver_version: capabilities.driver_version.clone(),
            toolkit_version: capabilities.toolkit_version.clone(),
            container_runtime: capabilities.container_runtime.clone(),
        },
    }
}

pub(super) fn assignment_to_contract(assignment: &Assignment) -> AssignmentContract {
    let execution = assignment
        .execution
        .as_ref()
        .expect("validated assignment contains execution");
    AssignmentContract {
        assignment_id: assignment.assignment_id.clone(),
        attempt_id: AttemptId::try_from(assignment.attempt_id.clone())
            .expect("validated assignment contains a non-empty attempt ID"),
        attempt_number: assignment.attempt_number,
        idempotency_key: assignment.idempotency_key.clone(),
        task_id: assignment.task_id.clone(),
        candidate_id: assignment.candidate_id.clone(),
        execution: ExecutionContract {
            executor_kind: ExecutionKind::try_from(execution.executor_kind)
                .expect("validated assignment contains a known executor kind"),
            argv: execution.argv.clone(),
            working_directory: execution.working_directory.clone(),
            environment: execution
                .environment
                .iter()
                .map(|entry| EnvironmentEntry {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: execution.timeout_ms,
            bundle: artifact_to_identity(
                execution
                    .bundle
                    .as_ref()
                    .expect("validated assignment contains bundle"),
            ),
            image: artifact_to_identity(
                execution
                    .image
                    .as_ref()
                    .expect("validated assignment contains image"),
            ),
            limits: execution.limits.as_ref().map(|limits| ResourceContract {
                cpu_millis: limits.cpu_millis,
                memory_bytes: limits.memory_bytes,
                disk_bytes: limits.disk_bytes,
                process_count: limits.process_count,
                output_bytes: limits.output_bytes,
                device_count: limits.device_count,
                network: NetworkPolicy::try_from(limits.network)
                    .expect("validated assignment contains a known network policy"),
            }),
        },
        required_features: assignment.required_features.clone(),
    }
}

pub(super) fn contract_to_assignment(contract: &AssignmentContract) -> Assignment {
    Assignment {
        assignment_id: contract.assignment_id.clone(),
        attempt_id: contract.attempt_id.to_string(),
        attempt_number: contract.attempt_number,
        idempotency_key: contract.idempotency_key.clone(),
        task_id: contract.task_id.clone(),
        candidate_id: contract.candidate_id.clone(),
        execution: Some(ExecutionSpec {
            executor_kind: contract.execution.executor_kind.into(),
            argv: contract.execution.argv.clone(),
            working_directory: contract.execution.working_directory.clone(),
            environment: contract
                .execution
                .environment
                .iter()
                .map(|entry| EnvironmentVariable {
                    name: entry.name.clone(),
                    value: entry.value.clone(),
                })
                .collect(),
            timeout_ms: contract.execution.timeout_ms,
            bundle: Some(identity_to_artifact(&contract.execution.bundle)),
            image: Some(identity_to_artifact(&contract.execution.image)),
            limits: contract
                .execution
                .limits
                .as_ref()
                .map(|limits| ResourceLimits {
                    cpu_millis: limits.cpu_millis,
                    memory_bytes: limits.memory_bytes,
                    disk_bytes: limits.disk_bytes,
                    process_count: limits.process_count,
                    output_bytes: limits.output_bytes,
                    device_count: limits.device_count,
                    network: limits.network.into(),
                }),
        }),
        required_features: contract.required_features.clone(),
    }
}

pub(super) fn artifact_to_identity(artifact: &ArtifactRef) -> ArtifactIdentity {
    ArtifactIdentity {
        digest: artifact.digest.clone(),
        size_bytes: artifact.size_bytes,
        media_type: artifact.media_type.clone(),
    }
}

pub(super) fn identity_to_artifact(identity: &ArtifactIdentity) -> ArtifactRef {
    ArtifactRef {
        digest: identity.digest.clone(),
        size_bytes: identity.size_bytes,
        media_type: identity.media_type.clone(),
    }
}

pub(super) fn event_artifact(artifact: &ArtifactRef, reference: &str) -> EventArtifactRef {
    EventArtifactRef {
        digest: artifact.digest.clone(),
        media_type: artifact.media_type.clone(),
        size_bytes: artifact.size_bytes,
        reference: reference.into(),
    }
}

pub(super) fn worker_event(
    contract: &AssignmentContract,
    worker_id: &str,
    emitted_at_unix_ms: u64,
    mut event: Event,
) -> ProducerEvent {
    interaction::redact_worker_event(&mut event);
    let mut frame = ProducerEvent::new(
        contract.task_id.clone(),
        Producer::new("alloyport-worker", worker_id),
        event,
    );
    frame.task_id = Some(contract.task_id.clone());
    frame.operation_id = Some(contract.attempt_id.to_string());
    frame.emitted_at_unix_ms = emitted_at_unix_ms;
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame
}

pub(super) fn interaction_status(error: &InteractionError) -> Status {
    let detail = error.to_string();
    match error {
        InteractionError::InvalidFrame(_)
        | InteractionError::ConflictingDedupKey(_)
        | InteractionError::ConflictingOutput { .. }
        | InteractionError::InvalidCursor { .. }
        | InteractionError::RevokedRunGrant { .. }
        | InteractionError::MissingRunGrant { .. }
        | InteractionError::ValueOutOfRange(_) => Status::invalid_argument(detail),
        InteractionError::Storage(_)
        | InteractionError::Encoding(_)
        | InteractionError::InvalidSubscriptionCapacity
        | InteractionError::LockPoisoned => Status::internal(detail),
    }
}
