//! Operator API consumed by `alloyport-cli`.

use crate::WorkerControlService;
use crate::identity::ConnectionIdentityResolver;
use crate::migration_task::{
    MigrationTaskError, MigrationTaskRecord, MigrationTaskState as StoredTaskState,
    SqliteMigrationTaskStore,
};
use crate::persistence::ServerPersistence;
use crate::storage::{Clock, SystemClock};
use alloyport_artifacts::{ArtifactStore, IngestRequest};
use alloyport_core::{BundlePath, Sha256Digest};
use alloyport_events::{Authority, Event, Producer, ProducerEvent, Visibility};
use alloyport_proto::management_v1::management_service_server::ManagementService;
use alloyport_proto::management_v1::{
    CancelMigrationRequest, GetMigrationRequest, GetServerStatusRequest, ListMigrationsRequest,
    ListMigrationsResponse, ListWorkersRequest, ListWorkersResponse, MigrationProjectBundle,
    MigrationTask, MigrationTaskState, ResumeMigrationRequest, ServerStatus,
    SubmitMigrationRequest, Worker, WorkerDevice,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use prost::Message;
use std::io::Cursor;
use std::sync::Arc;
use tonic::{Request, Response, Status};

const LOCAL_OWNER_ID: &str = "local-cli";
const DEFAULT_MIGRATION_LIMIT: usize = 50;
const MAX_MIGRATION_LIMIT: usize = 1_000;
const MAX_PROJECT_FILES: usize = 4_096;
const MAX_PROJECT_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROJECT_BYTES: u64 = 60 * 1024 * 1024;

#[derive(Clone)]
pub struct ManagementServiceImpl {
    control: WorkerControlService,
    migrations: Option<Arc<SqliteMigrationTaskStore>>,
    artifacts: Option<Arc<dyn ArtifactStore>>,
    identities: Option<Arc<dyn ConnectionIdentityResolver>>,
    clock: Arc<dyn Clock>,
    persistence: ServerPersistence,
}

impl ManagementServiceImpl {
    #[must_use]
    pub fn new(control: WorkerControlService) -> Self {
        Self {
            control,
            migrations: None,
            artifacts: None,
            identities: None,
            clock: Arc::new(SystemClock),
            persistence: ServerPersistence::default(),
        }
    }

    #[must_use]
    pub fn with_migration_intake(
        mut self,
        migrations: Arc<SqliteMigrationTaskStore>,
        artifacts: Arc<dyn ArtifactStore>,
        identities: Option<Arc<dyn ConnectionIdentityResolver>>,
    ) -> Self {
        self.migrations = Some(migrations);
        self.artifacts = Some(artifacts);
        self.identities = identities;
        self
    }

    async fn owner<T>(&self, request: &Request<T>) -> Result<String, Status> {
        match &self.identities {
            Some(identities) => identities.resolve_owner(request.extensions()).await,
            None => Ok(LOCAL_OWNER_ID.to_owned()),
        }
    }

    fn intake(&self) -> Result<(&Arc<SqliteMigrationTaskStore>, &Arc<dyn ArtifactStore>), Status> {
        match (&self.migrations, &self.artifacts) {
            (Some(migrations), Some(artifacts)) => Ok((migrations, artifacts)),
            _ => Err(Status::unavailable("migration intake is not configured")),
        }
    }
}

#[tonic::async_trait]
impl ManagementService for ManagementServiceImpl {
    async fn get_server_status(
        &self,
        _request: Request<GetServerStatusRequest>,
    ) -> Result<Response<ServerStatus>, Status> {
        let workers = self.control.worker_snapshots().await;
        Ok(Response::new(ServerStatus {
            server_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            worker_count: workers.len() as u64,
            connected_worker_count: workers.iter().filter(|worker| worker.connected).count() as u64,
        }))
    }

    async fn list_workers(
        &self,
        _request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        let workers = self
            .control
            .worker_snapshots()
            .await
            .into_iter()
            .map(|snapshot| Worker {
                health: snapshot
                    .heartbeat
                    .as_ref()
                    .map_or(0, |heartbeat| heartbeat.health),
                available_slots: snapshot
                    .heartbeat
                    .as_ref()
                    .map_or(0, |heartbeat| heartbeat.available_slots),
                devices: snapshot.heartbeat.map_or_else(Vec::new, |heartbeat| {
                    heartbeat
                        .devices
                        .into_iter()
                        .map(|device| WorkerDevice {
                            device_id: device.device_id,
                            health: device.health,
                            process_count: device.process_count,
                        })
                        .collect()
                }),
                worker_id: snapshot.worker_id,
                instance_id: snapshot.instance_id,
                connected: snapshot.connected,
                last_worker_sequence: snapshot.last_worker_sequence,
                backend: snapshot.backend,
                features: snapshot.features,
            })
            .collect();
        Ok(Response::new(ListWorkersResponse { workers }))
    }

    async fn submit_migration(
        &self,
        request: Request<SubmitMigrationRequest>,
    ) -> Result<Response<MigrationTask>, Status> {
        let owner_id = self.owner(&request).await?;
        let event_owner = owner_id.clone();
        let request = request.into_inner();
        validate_request_id(&request.request_id)?;
        let project = request
            .project
            .ok_or_else(|| Status::invalid_argument("project bundle is missing"))?;
        let (project_size_bytes, file_count) = validate_project(&project)?;
        let project_bytes = project.encode_to_vec();
        let project_digest = Sha256Digest::digest_bytes(&project_bytes);
        let task_id = task_id(&owner_id, &request.request_id, project_digest);
        let (migrations, artifacts) = self.intake()?;
        let artifacts = Arc::clone(artifacts);
        let bytes = project_bytes;
        self.persistence
            .run(move || {
                artifacts.ingest(
                    &mut Cursor::new(bytes),
                    IngestRequest {
                        expected_digest: Some(project_digest),
                        expected_size_bytes: None,
                    },
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| Status::internal(format!("store project bundle: {error}")))?;
        let migrations = Arc::clone(migrations);
        let request_id = request.request_id;
        let project_name = project.name;
        let created_at_ms = self.clock.now_unix_ms();
        let record = self
            .persistence
            .run(move || {
                migrations.submit(
                    &owner_id,
                    &request_id,
                    &task_id,
                    &project_name,
                    project_digest,
                    project_size_bytes,
                    file_count,
                    created_at_ms,
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(task_status)?;
        let control = self.control.clone();
        let task_id = record.task_id.clone();
        let project_name = record.project_name.clone();
        let emitted_at_unix_ms = record.created_at_ms;
        self.persistence
            .run(move || {
                control.grant_interaction_access(&task_id, &event_owner)?;
                let mut frame = ProducerEvent::new(
                    task_id.clone(),
                    Producer::new("alloyport-server", "management"),
                    Event::RunStarted {
                        task: format!("migrate CUDA project {project_name}"),
                    },
                );
                frame.task_id = Some(task_id);
                frame.emitted_at_unix_ms = emitted_at_unix_ms;
                frame.authority = Authority::Observed;
                frame.visibility = Visibility::User;
                control.append_interaction_event("migration-captured", &frame)?;
                Ok::<(), crate::interaction::InteractionError>(())
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| Status::internal(format!("publish migration event: {error}")))?;
        Ok(Response::new(task_to_proto(record)))
    }

    async fn get_migration(
        &self,
        request: Request<GetMigrationRequest>,
    ) -> Result<Response<MigrationTask>, Status> {
        let owner_id = self.owner(&request).await?;
        let task_id = required_text(&request.get_ref().task_id, "task_id")?.to_owned();
        let (migrations, _) = self.intake()?;
        let migrations = Arc::clone(migrations);
        let record = self
            .persistence
            .run(move || migrations.get(&owner_id, &task_id))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(task_status)?;
        Ok(Response::new(task_to_proto(record)))
    }

    async fn list_migrations(
        &self,
        request: Request<ListMigrationsRequest>,
    ) -> Result<Response<ListMigrationsResponse>, Status> {
        let owner_id = self.owner(&request).await?;
        let requested = request.get_ref().limit as usize;
        let limit = if requested == 0 {
            DEFAULT_MIGRATION_LIMIT
        } else if requested <= MAX_MIGRATION_LIMIT {
            requested
        } else {
            return Err(Status::invalid_argument(
                "migration list limit is too large",
            ));
        };
        let (migrations, _) = self.intake()?;
        let migrations = Arc::clone(migrations);
        let tasks = self
            .persistence
            .run(move || migrations.list(&owner_id, limit))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(task_status)?
            .into_iter()
            .map(task_to_proto)
            .collect();
        Ok(Response::new(ListMigrationsResponse { tasks }))
    }

    async fn resume_migration(
        &self,
        request: Request<ResumeMigrationRequest>,
    ) -> Result<Response<MigrationTask>, Status> {
        let owner_id = self.owner(&request).await?;
        let task_id = required_text(&request.get_ref().task_id, "task_id")?.to_owned();
        let (migrations, _) = self.intake()?;
        let migrations = Arc::clone(migrations);
        let record = self
            .persistence
            .run(move || migrations.resume(&owner_id, &task_id))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(task_status)?;
        Ok(Response::new(task_to_proto(record)))
    }

    async fn cancel_migration(
        &self,
        request: Request<CancelMigrationRequest>,
    ) -> Result<Response<MigrationTask>, Status> {
        let owner_id = self.owner(&request).await?;
        let task_id = required_text(&request.get_ref().task_id, "task_id")?.to_owned();
        let (migrations, _) = self.intake()?;
        let migrations = Arc::clone(migrations);
        let record = self
            .persistence
            .run(move || migrations.cancel(&owner_id, &task_id))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(task_status)?;
        Ok(Response::new(task_to_proto(record)))
    }
}

fn validate_request_id(request_id: &str) -> Result<(), Status> {
    let request_id = required_text(request_id, "request_id")?;
    if request_id.len() > 128 {
        return Err(Status::invalid_argument("request_id is too long"));
    }
    Ok(())
}

fn validate_project(project: &MigrationProjectBundle) -> Result<(u64, u64), Status> {
    let name = required_text(&project.name, "project.name")?;
    if name.len() > 255 {
        return Err(Status::invalid_argument("project.name is too long"));
    }
    if project.files.is_empty() || project.files.len() > MAX_PROJECT_FILES {
        return Err(Status::invalid_argument(
            "project file count is outside the supported range",
        ));
    }
    let mut previous: Option<&str> = None;
    let mut total = 0_u64;
    let mut has_cuda = false;
    for file in &project.files {
        let path = BundlePath::try_from(file.path.as_str())
            .map_err(|error| Status::invalid_argument(format!("invalid project path: {error}")))?;
        if previous.is_some_and(|previous| previous >= file.path.as_str()) {
            return Err(Status::invalid_argument(
                "project files must be unique and sorted by path",
            ));
        }
        previous = Some(file.path.as_str());
        if file.contents.len() > MAX_PROJECT_FILE_BYTES {
            return Err(Status::invalid_argument(format!(
                "project file {} exceeds the per-file limit",
                path.as_str()
            )));
        }
        total = total
            .checked_add(file.contents.len() as u64)
            .ok_or_else(|| Status::invalid_argument("project size overflow"))?;
        if total > MAX_PROJECT_BYTES {
            return Err(Status::invalid_argument(
                "project exceeds the total byte limit",
            ));
        }
        has_cuda |= std::path::Path::new(path.as_str())
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("cu"));
    }
    if !has_cuda {
        return Err(Status::invalid_argument(
            "project does not contain a CUDA .cu source file",
        ));
    }
    Ok((total, project.files.len() as u64))
}

fn required_text<'a>(value: &'a str, field: &'static str) -> Result<&'a str, Status> {
    if value.trim().is_empty() {
        Err(Status::invalid_argument(format!("{field} is required")))
    } else {
        Ok(value)
    }
}

fn task_id(owner_id: &str, request_id: &str, project_digest: Sha256Digest) -> String {
    let identity = format!("{owner_id}\0{request_id}\0{project_digest}");
    let digest = Sha256Digest::digest_bytes(identity.as_bytes()).hexadecimal();
    format!("task-{}", &digest[..24])
}

#[allow(clippy::needless_pass_by_value)]
fn task_status(error: MigrationTaskError) -> Status {
    match error {
        MigrationTaskError::Conflict => Status::already_exists(error.to_string()),
        MigrationTaskError::NotFound => Status::not_found(error.to_string()),
        MigrationTaskError::Storage(_) | MigrationTaskError::Corrupt(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn task_to_proto(record: MigrationTaskRecord) -> MigrationTask {
    MigrationTask {
        task_id: record.task_id,
        project_name: record.project_name,
        project_digest: record.project_digest.to_string(),
        project_size_bytes: record.project_size_bytes,
        file_count: record.file_count,
        state: match record.state {
            StoredTaskState::Captured => MigrationTaskState::Captured,
            StoredTaskState::Running => MigrationTaskState::Running,
            StoredTaskState::Completed => MigrationTaskState::Completed,
            StoredTaskState::Failed => MigrationTaskState::Failed,
            StoredTaskState::Cancelled => MigrationTaskState::Cancelled,
        }
        .into(),
        created_at_ms: record.created_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_artifacts::InMemoryArtifactStore;
    use alloyport_proto::management_v1::ProjectFile;

    #[tokio::test]
    async fn reports_an_empty_but_healthy_daemon() -> Result<(), Status> {
        let service = ManagementServiceImpl::new(WorkerControlService::new());

        let status = service
            .get_server_status(Request::new(GetServerStatusRequest {}))
            .await?
            .into_inner();
        let workers = service
            .list_workers(Request::new(ListWorkersRequest {}))
            .await?
            .into_inner();

        assert_eq!(status.server_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(status.protocol_major, PROTOCOL_MAJOR);
        assert_eq!(status.protocol_minor, PROTOCOL_MINOR);
        assert_eq!(status.worker_count, 0);
        assert_eq!(status.connected_worker_count, 0);
        assert!(workers.workers.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn submission_is_durable_queryable_and_cancellable()
    -> Result<(), Box<dyn std::error::Error>> {
        let migrations = Arc::new(SqliteMigrationTaskStore::in_memory()?);
        let artifacts: Arc<dyn ArtifactStore> =
            Arc::new(InMemoryArtifactStore::new(64 * 1024 * 1024));
        let control = WorkerControlService::new();
        let service = ManagementServiceImpl::new(control.clone()).with_migration_intake(
            migrations,
            Arc::clone(&artifacts),
            None,
        );
        let request = SubmitMigrationRequest {
            request_id: "request-1".to_owned(),
            project: Some(MigrationProjectBundle {
                name: "vector-add".to_owned(),
                files: vec![ProjectFile {
                    path: "src/vector_add.cu".to_owned(),
                    contents: b"__global__ void add() {}".to_vec(),
                }],
            }),
        };

        let submitted = service
            .submit_migration(Request::new(request.clone()))
            .await?
            .into_inner();
        let retried = service
            .submit_migration(Request::new(request))
            .await?
            .into_inner();
        assert_eq!(submitted, retried);
        assert_eq!(submitted.state, MigrationTaskState::Captured as i32);
        assert!(artifacts.contains(submitted.project_digest.parse()?)?);
        let events = control.interaction_events(&submitted.task_id)?;
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].event, Event::RunStarted { .. }));
        assert_eq!(
            control.grant_interaction_access(&submitted.task_id, LOCAL_OWNER_ID)?,
            crate::interaction::RunGrantOutcome::Duplicate
        );

        let listed = service
            .list_migrations(Request::new(ListMigrationsRequest { limit: 0 }))
            .await?
            .into_inner();
        assert_eq!(listed.tasks, vec![submitted.clone()]);

        let cancelled = service
            .cancel_migration(Request::new(CancelMigrationRequest {
                task_id: submitted.task_id.clone(),
            }))
            .await?
            .into_inner();
        assert_eq!(cancelled.state, MigrationTaskState::Cancelled as i32);
        let fetched = service
            .get_migration(Request::new(GetMigrationRequest {
                task_id: submitted.task_id,
            }))
            .await?
            .into_inner();
        assert_eq!(fetched, cancelled);
        Ok(())
    }
}
