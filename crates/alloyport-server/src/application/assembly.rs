//! Concrete server dependency assembly.

use super::config::{ArtifactConfig, ServerConfig, ServerTlsPaths};
use super::migration_dispatcher::MigrationDispatcher;
use crate::WorkerControlService;
use crate::adapters::sqlite::SqliteIdentityRegistry;
use crate::artifact::{ArtifactServiceImpl, EnrolledArtifactAccessPolicy};
use crate::identity::{
    ConnectionIdentityResolver, IdentityRegistry, MtlsConnectionIdentityResolver,
};
use crate::interaction::InteractionStore;
use crate::interaction_service::{EnrolledInteractionAccessPolicy, InteractionServiceImpl};
use crate::interaction_service::{InteractionAccessPolicy, LocalInteractionAccessPolicy};
use crate::management_service::ManagementServiceImpl;
use crate::migration_task::MigrationTaskStore;
use crate::storage::SystemClock;
use alloyport_artifacts::upload::UploadQuotas;
use alloyport_artifacts::{FilesystemArtifactStore, SqliteUploadStore};
use alloyport_proto::PROTOBUF_MESSAGE_OVERHEAD_BYTES;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

pub(super) struct ServerApplication {
    pub(super) address: SocketAddr,
    pub(super) tls: Option<ServerTlsConfig>,
    pub(super) shutdown_timeout: Duration,
    pub(super) control: WorkerControlService,
    pub(super) artifacts: Arc<dyn alloyport_artifacts::ArtifactStore>,
    pub(super) artifact: ArtifactServiceImpl,
    pub(super) artifact_max_decoding_message_bytes: usize,
    pub(super) interaction: InteractionServiceImpl,
    pub(super) management: ManagementServiceImpl,
    pub(super) migration_dispatcher: Option<MigrationDispatcher>,
}

pub(super) async fn assemble(
    mut config: ServerConfig,
) -> Result<ServerApplication, Box<dyn Error>> {
    let require_enrollment = config.tls.is_some();
    let tls = config.tls.map(load_tls).transpose()?;
    let identities = Arc::new(SqliteIdentityRegistry::open(config.identity_database)?);
    let identity_registry: Arc<dyn IdentityRegistry> = identities;
    let identity_resolver: Arc<dyn ConnectionIdentityResolver> =
        Arc::new(MtlsConnectionIdentityResolver::new(identity_registry));
    let artifact = assemble_artifact(&config.artifact, Arc::clone(&identity_resolver))?;
    let migrations: Arc<dyn MigrationTaskStore> = Arc::new(
        crate::adapters::sqlite::SqliteMigrationTaskStore::open(&config.database)?,
    );
    let (control, interaction_hub) =
        WorkerControlService::open_sqlite_with_interaction_hub(config.database)?;
    let mut control = control
        .with_artifact_metadata(artifact.uploads.clone())
        .with_artifact_store(artifact.artifacts.clone());
    if require_enrollment {
        control = control.require_identity_resolver(Arc::clone(&identity_resolver));
    }
    let initial_reconciliation = control.reconcile_preparing_assignments_at_startup().await?;
    if !initial_reconciliation.failures.is_empty() {
        eprintln!(
            "deferred {} of {} preparing assignments during startup reconciliation",
            initial_reconciliation.failures.len(),
            initial_reconciliation.scanned
        );
    }
    let interaction_store: Arc<dyn InteractionStore> = interaction_hub.clone();
    let interaction_access: Arc<dyn InteractionAccessPolicy> = if require_enrollment {
        Arc::new(EnrolledInteractionAccessPolicy::new(
            interaction_store,
            Arc::clone(&identity_resolver),
        ))
    } else {
        Arc::new(LocalInteractionAccessPolicy::new(
            interaction_store,
            "local-cli",
        ))
    };
    let interaction = InteractionServiceImpl::new(interaction_hub, interaction_access);
    let management = ManagementServiceImpl::new(control.clone()).with_migration_intake(
        Arc::clone(&migrations),
        artifact.artifacts.clone(),
        require_enrollment.then_some(identity_resolver),
    );
    let migration_dispatcher = config
        .migration_runtime
        .take()
        .map(|runtime| {
            MigrationDispatcher::new(
                runtime,
                Arc::clone(&migrations),
                artifact.artifacts.clone(),
                control.clone(),
            )
        })
        .transpose()?;
    Ok(ServerApplication {
        address: config.address,
        tls,
        shutdown_timeout: config.shutdown_timeout,
        control,
        artifacts: artifact.artifacts,
        artifact: artifact.service,
        artifact_max_decoding_message_bytes: artifact.max_decoding_message_bytes,
        interaction,
        management,
        migration_dispatcher,
    })
}

struct ArtifactAssembly {
    service: ArtifactServiceImpl,
    artifacts: Arc<dyn alloyport_artifacts::ArtifactStore>,
    uploads: Arc<SqliteUploadStore>,
    max_decoding_message_bytes: usize,
}

fn assemble_artifact(
    config: &ArtifactConfig,
    identity_resolver: Arc<dyn ConnectionIdentityResolver>,
) -> Result<ArtifactAssembly, Box<dyn Error>> {
    let artifacts: Arc<dyn alloyport_artifacts::ArtifactStore> = Arc::new(
        FilesystemArtifactStore::open(config.root.join("cas"), config.max_artifact_bytes)?,
    );
    let uploads = Arc::new(SqliteUploadStore::open_with_quotas(
        config.root.join("uploads.sqlite3"),
        config.root.join("upload-data"),
        config.max_artifact_bytes,
        config.max_chunk_bytes,
        UploadQuotas {
            total_bytes: config.total_quota_bytes,
            per_owner_bytes: config.per_owner_quota_bytes,
        },
    )?);
    let access = Arc::new(EnrolledArtifactAccessPolicy::new(
        uploads.clone(),
        identity_resolver,
    ));
    Ok(ArtifactAssembly {
        service: ArtifactServiceImpl::new(
            uploads.clone(),
            artifacts.clone(),
            access,
            Arc::new(SystemClock),
        ),
        artifacts,
        uploads,
        max_decoding_message_bytes: config
            .max_chunk_bytes
            .saturating_add(PROTOBUF_MESSAGE_OVERHEAD_BYTES),
    })
}

fn load_tls(paths: ServerTlsPaths) -> Result<ServerTlsConfig, Box<dyn Error>> {
    let identity = Identity::from_pem(fs::read(paths.certificate)?, fs::read(paths.private_key)?);
    let client_ca = Certificate::from_pem(fs::read(paths.client_ca)?);
    Ok(ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(client_ca))
}
