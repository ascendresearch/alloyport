use alloyport_artifacts::FilesystemArtifactStore;
use alloyport_artifacts::upload::{SqliteUploadStore, UploadQuotas};
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::interaction_v1::interaction_service_server::InteractionServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_server::WorkerControlService;
use alloyport_server::adapters::sqlite::SqliteIdentityRegistry;
use alloyport_server::artifact::{ArtifactServiceImpl, EnrolledArtifactAccessPolicy};
use alloyport_server::identity::{
    ConnectionIdentityResolver, IdentityRegistry, MtlsConnectionIdentityResolver,
    certificate_fingerprint_from_pem,
};
use alloyport_server::interaction::InteractionStore;
use alloyport_server::interaction_service::{
    EnrolledInteractionAccessPolicy, InteractionServiceImpl,
};
use alloyport_server::storage::{Clock, SystemClock};
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const DEFAULT_MAX_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_MAX_UPLOAD_CHUNK_BYTES: u64 = 1024 * 1024;
const DEFAULT_TOTAL_ARTIFACT_QUOTA_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_OWNER_ARTIFACT_QUOTA_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const PROTOBUF_CHUNK_OVERHEAD_BYTES: usize = 64 * 1024;

struct ArtifactRuntime {
    service: ArtifactServiceImpl,
    uploads: Arc<SqliteUploadStore>,
    max_decoding_message_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if run_identity_command()? {
        return Ok(());
    }
    let address: SocketAddr = env::var("ALLOYPORT_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_owned())
        .parse()?;
    let tls = tls_config()?;
    let require_enrollment = tls.is_some();
    if tls.is_none() && !address.ip().is_loopback() {
        return Err("plaintext worker control is restricted to loopback".into());
    }

    let database =
        env::var_os("ALLOYPORT_DATABASE").unwrap_or_else(|| "alloyport-control.sqlite3".into());
    let artifact_root = artifact_root();
    let identities = Arc::new(SqliteIdentityRegistry::open(identity_database(
        &artifact_root,
    ))?);
    let identity_registry: Arc<dyn IdentityRegistry> = identities.clone();
    let identity_resolver: Arc<dyn ConnectionIdentityResolver> =
        Arc::new(MtlsConnectionIdentityResolver::new(identity_registry));
    let artifact = artifact_runtime(&artifact_root, Arc::clone(&identity_resolver))?;
    let (control_service, interaction_hub) =
        WorkerControlService::open_sqlite_with_interaction_hub(database)?;
    let mut control_service = control_service.with_artifact_metadata(Arc::clone(&artifact.uploads));
    if require_enrollment {
        control_service = control_service.require_identity_resolver(Arc::clone(&identity_resolver));
    }
    let initial_reconciliation = control_service
        .reconcile_preparing_assignments_at_startup()
        .await?;
    if !initial_reconciliation.failures.is_empty() {
        eprintln!(
            "deferred {} of {} preparing assignments during startup reconciliation",
            initial_reconciliation.failures.len(),
            initial_reconciliation.scanned
        );
    }
    let interaction_store: Arc<dyn InteractionStore> = interaction_hub.clone();
    let interaction_service = InteractionServiceImpl::new(
        interaction_hub,
        Arc::new(EnrolledInteractionAccessPolicy::new(
            interaction_store,
            Arc::clone(&identity_resolver),
        )),
    );
    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    println!("AlloyPort worker control, artifact, and interaction services listening on {address}");
    let reaper_service = control_service.clone();
    let mut lease_reaper = tokio::spawn(async move { reaper_service.run_lease_reaper().await });
    let reconciler_service = control_service.clone();
    let mut preparation_reconciler =
        tokio::spawn(async move { reconciler_service.run_preparation_reconciler().await });
    let serve = server
        .add_service(WorkerControlServer::new(control_service))
        .add_service(
            ArtifactServiceServer::new(artifact.service)
                .max_decoding_message_size(artifact.max_decoding_message_bytes),
        )
        .add_service(InteractionServiceServer::new(interaction_service))
        .serve(address);
    tokio::select! {
        serve_result = serve => {
            lease_reaper.abort();
            preparation_reconciler.abort();
            serve_result?;
        }
        reaper_result = &mut lease_reaper => {
            preparation_reconciler.abort();
            reaper_result??;
            return Err("lease reaper stopped unexpectedly".into());
        }
        reconciler_result = &mut preparation_reconciler => {
            lease_reaper.abort();
            reconciler_result??;
            return Err("assignment preparation reconciler stopped unexpectedly".into());
        }
    }
    Ok(())
}

fn artifact_runtime(
    root: &Path,
    identity_resolver: Arc<dyn ConnectionIdentityResolver>,
) -> Result<ArtifactRuntime, Box<dyn Error>> {
    let max_artifact_bytes =
        positive_environment_u64("ALLOYPORT_ARTIFACT_MAX_BYTES", DEFAULT_MAX_ARTIFACT_BYTES)?;
    let max_chunk_bytes = positive_environment_u64(
        "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES",
        DEFAULT_MAX_UPLOAD_CHUNK_BYTES,
    )?;
    let total_quota_bytes = positive_environment_u64(
        "ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES",
        DEFAULT_TOTAL_ARTIFACT_QUOTA_BYTES,
    )?;
    let per_owner_quota_bytes = positive_environment_u64(
        "ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES",
        DEFAULT_OWNER_ARTIFACT_QUOTA_BYTES,
    )?;
    let max_chunk_bytes = usize::try_from(max_chunk_bytes)
        .map_err(|_| "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES exceeds this platform's usize")?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        root.join("cas"),
        max_artifact_bytes,
    )?);
    let uploads = Arc::new(SqliteUploadStore::open_with_quotas(
        root.join("uploads.sqlite3"),
        root.join("upload-data"),
        max_artifact_bytes,
        max_chunk_bytes,
        UploadQuotas {
            total_bytes: total_quota_bytes,
            per_owner_bytes: per_owner_quota_bytes,
        },
    )?);
    let access = Arc::new(EnrolledArtifactAccessPolicy::new(
        Arc::clone(&uploads),
        identity_resolver,
    ));
    Ok(ArtifactRuntime {
        service: ArtifactServiceImpl::new(
            Arc::clone(&uploads),
            artifacts,
            access,
            Arc::new(SystemClock),
        ),
        uploads: Arc::clone(&uploads),
        max_decoding_message_bytes: max_chunk_bytes.saturating_add(PROTOBUF_CHUNK_OVERHEAD_BYTES),
    })
}

fn artifact_root() -> PathBuf {
    PathBuf::from(
        env::var_os("ALLOYPORT_ARTIFACT_ROOT").unwrap_or_else(|| "alloyport-artifacts".into()),
    )
}

fn identity_database(artifact_root: &Path) -> PathBuf {
    env::var_os("ALLOYPORT_IDENTITY_DATABASE")
        .map_or_else(|| artifact_root.join("identities.sqlite3"), PathBuf::from)
}

fn run_identity_command() -> Result<bool, Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("identity")) {
        return Ok(false);
    }
    let action = required_argument(&mut arguments, "identity action")?;
    let root = artifact_root();
    let registry = SqliteIdentityRegistry::open(identity_database(&root))?;
    let now_ms = SystemClock.now_unix_ms();
    match action.to_str() {
        Some("enroll") => {
            let owner = required_utf8_argument(&mut arguments, "owner ID")?;
            let certificate = required_argument(&mut arguments, "certificate PEM path")?;
            ensure_no_more_arguments(&mut arguments)?;
            let fingerprint = certificate_fingerprint_from_pem(&fs::read(certificate)?)?;
            registry.enroll(&owner, fingerprint, now_ms)?;
            println!("enrolled {fingerprint} as {owner}");
        }
        Some("rotate") => {
            let owner = required_utf8_argument(&mut arguments, "owner ID")?;
            let old_certificate = required_argument(&mut arguments, "old certificate PEM path")?;
            let new_certificate = required_argument(&mut arguments, "new certificate PEM path")?;
            ensure_no_more_arguments(&mut arguments)?;
            let old_fingerprint = certificate_fingerprint_from_pem(&fs::read(old_certificate)?)?;
            let new_fingerprint = certificate_fingerprint_from_pem(&fs::read(new_certificate)?)?;
            registry.rotate(&owner, old_fingerprint, new_fingerprint, now_ms)?;
            println!("rotated {owner} from {old_fingerprint} to {new_fingerprint}");
        }
        Some("revoke") => {
            let certificate = required_argument(&mut arguments, "certificate PEM path")?;
            ensure_no_more_arguments(&mut arguments)?;
            let fingerprint = certificate_fingerprint_from_pem(&fs::read(certificate)?)?;
            let enrollment = registry.revoke(fingerprint, now_ms)?;
            println!("revoked {fingerprint} for {}", enrollment.owner_id);
        }
        _ => {
            return Err(
                "identity action must be enroll, rotate, or revoke; see docs/HANDOFF.md".into(),
            );
        }
    }
    Ok(true)
}

fn required_argument(
    arguments: &mut impl Iterator<Item = OsString>,
    name: &str,
) -> Result<OsString, Box<dyn Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("missing {name}").into())
}

fn required_utf8_argument(
    arguments: &mut impl Iterator<Item = OsString>,
    name: &str,
) -> Result<String, Box<dyn Error>> {
    required_argument(arguments, name)?
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8").into())
}

fn ensure_no_more_arguments(
    arguments: &mut impl Iterator<Item = OsString>,
) -> Result<(), Box<dyn Error>> {
    if arguments.next().is_some() {
        Err("unexpected extra identity command arguments".into())
    } else {
        Ok(())
    }
}

fn positive_environment_u64(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<u64>()?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(value)
}

fn tls_config() -> Result<Option<ServerTlsConfig>, Box<dyn Error>> {
    let certificate = env::var_os("ALLOYPORT_TLS_CERT");
    let key = env::var_os("ALLOYPORT_TLS_KEY");
    let client_ca = env::var_os("ALLOYPORT_TLS_CLIENT_CA");
    match (certificate, key, client_ca) {
        (None, None, None) => Ok(None),
        (Some(certificate), Some(key), Some(client_ca)) => {
            let identity = Identity::from_pem(fs::read(certificate)?, fs::read(key)?);
            let client_ca = Certificate::from_pem(fs::read(client_ca)?);
            Ok(Some(
                ServerTlsConfig::new()
                    .identity(identity)
                    .client_ca_root(client_ca),
            ))
        }
        _ => Err("ALLOYPORT_TLS_CERT, ALLOYPORT_TLS_KEY and ALLOYPORT_TLS_CLIENT_CA must be set together"
            .into()),
    }
}
