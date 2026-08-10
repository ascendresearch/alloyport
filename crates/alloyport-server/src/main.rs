use alloyport_artifacts::FilesystemArtifactStore;
use alloyport_artifacts::upload::SqliteUploadStore;
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_server::WorkerControlService;
use alloyport_server::artifact::{ArtifactServiceImpl, MtlsArtifactAccessPolicy};
use alloyport_server::storage::SystemClock;
use std::env;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const DEFAULT_MAX_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_MAX_UPLOAD_CHUNK_BYTES: u64 = 1024 * 1024;
const PROTOBUF_CHUNK_OVERHEAD_BYTES: usize = 64 * 1024;

struct ArtifactRuntime {
    service: ArtifactServiceImpl,
    max_decoding_message_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = env::var("ALLOYPORT_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_owned())
        .parse()?;
    let tls = tls_config()?;
    if tls.is_none() && !address.ip().is_loopback() {
        return Err("plaintext worker control is restricted to loopback".into());
    }

    let database =
        env::var_os("ALLOYPORT_DATABASE").unwrap_or_else(|| "alloyport-control.sqlite3".into());
    let control_service = WorkerControlService::open_sqlite(database)?;
    let artifact = artifact_runtime()?;
    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    println!("AlloyPort worker control and artifact services listening on {address}");
    let reaper_service = control_service.clone();
    let mut lease_reaper = tokio::spawn(async move { reaper_service.run_lease_reaper().await });
    let serve = server
        .add_service(WorkerControlServer::new(control_service))
        .add_service(
            ArtifactServiceServer::new(artifact.service)
                .max_decoding_message_size(artifact.max_decoding_message_bytes),
        )
        .serve(address);
    tokio::select! {
        serve_result = serve => {
            lease_reaper.abort();
            serve_result?;
        }
        reaper_result = &mut lease_reaper => {
            reaper_result??;
            return Err("lease reaper stopped unexpectedly".into());
        }
    }
    Ok(())
}

fn artifact_runtime() -> Result<ArtifactRuntime, Box<dyn Error>> {
    let root = PathBuf::from(
        env::var_os("ALLOYPORT_ARTIFACT_ROOT").unwrap_or_else(|| "alloyport-artifacts".into()),
    );
    let max_artifact_bytes =
        positive_environment_u64("ALLOYPORT_ARTIFACT_MAX_BYTES", DEFAULT_MAX_ARTIFACT_BYTES)?;
    let max_chunk_bytes = positive_environment_u64(
        "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES",
        DEFAULT_MAX_UPLOAD_CHUNK_BYTES,
    )?;
    let max_chunk_bytes = usize::try_from(max_chunk_bytes)
        .map_err(|_| "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES exceeds this platform's usize")?;
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        root.join("cas"),
        max_artifact_bytes,
    )?);
    let uploads = Arc::new(SqliteUploadStore::open(
        root.join("uploads.sqlite3"),
        root.join("upload-data"),
        max_artifact_bytes,
        max_chunk_bytes,
    )?);
    let access = Arc::new(MtlsArtifactAccessPolicy::new(Arc::clone(&uploads)));
    Ok(ArtifactRuntime {
        service: ArtifactServiceImpl::new(uploads, artifacts, access, Arc::new(SystemClock)),
        max_decoding_message_bytes: max_chunk_bytes.saturating_add(PROTOBUF_CHUNK_OVERHEAD_BYTES),
    })
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
