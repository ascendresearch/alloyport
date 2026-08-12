//! Process configuration for the `AlloyPort` server.

use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_MAX_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_MAX_UPLOAD_CHUNK_BYTES: u64 = 1024 * 1024;
const DEFAULT_TOTAL_ARTIFACT_QUOTA_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_OWNER_ARTIFACT_QUOTA_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_SHUTDOWN_TIMEOUT_SECONDS: u64 = 10;

#[derive(Debug)]
pub(super) struct ServerConfig {
    pub(super) address: SocketAddr,
    pub(super) database: PathBuf,
    pub(super) artifact: ArtifactConfig,
    pub(super) identity_database: PathBuf,
    pub(super) tls: Option<ServerTlsPaths>,
    pub(super) shutdown_timeout: Duration,
}

#[derive(Debug)]
pub(super) struct ArtifactConfig {
    pub(super) root: PathBuf,
    pub(super) max_artifact_bytes: u64,
    pub(super) max_chunk_bytes: usize,
    pub(super) total_quota_bytes: u64,
    pub(super) per_owner_quota_bytes: u64,
}

#[derive(Debug)]
pub(super) struct ServerTlsPaths {
    pub(super) certificate: PathBuf,
    pub(super) private_key: PathBuf,
    pub(super) client_ca: PathBuf,
}

impl ServerConfig {
    pub(super) fn from_environment() -> Result<Self, Box<dyn Error>> {
        let address = env::var("ALLOYPORT_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_owned())
            .parse::<SocketAddr>()?;
        let tls = tls_paths_from_environment()?;
        if tls.is_none() && !address.ip().is_loopback() {
            return Err("plaintext worker control is restricted to loopback".into());
        }

        let root = artifact_root();
        let max_chunk_bytes = positive_environment_u64(
            "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES",
            DEFAULT_MAX_UPLOAD_CHUNK_BYTES,
        )?;
        let max_chunk_bytes = usize::try_from(max_chunk_bytes)
            .map_err(|_| "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES exceeds this platform's usize")?;
        let identity_database = identity_database(&root);
        Ok(Self {
            address,
            database: env::var_os("ALLOYPORT_DATABASE")
                .map_or_else(|| "alloyport-control.sqlite3".into(), PathBuf::from),
            artifact: ArtifactConfig {
                root,
                max_artifact_bytes: positive_environment_u64(
                    "ALLOYPORT_ARTIFACT_MAX_BYTES",
                    DEFAULT_MAX_ARTIFACT_BYTES,
                )?,
                max_chunk_bytes,
                total_quota_bytes: positive_environment_u64(
                    "ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES",
                    DEFAULT_TOTAL_ARTIFACT_QUOTA_BYTES,
                )?,
                per_owner_quota_bytes: positive_environment_u64(
                    "ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES",
                    DEFAULT_OWNER_ARTIFACT_QUOTA_BYTES,
                )?,
            },
            identity_database,
            tls,
            shutdown_timeout: Duration::from_secs(positive_environment_u64(
                "ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS",
                DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
            )?),
        })
    }
}

pub(super) fn artifact_root() -> PathBuf {
    env::var_os("ALLOYPORT_ARTIFACT_ROOT")
        .map_or_else(|| "alloyport-artifacts".into(), PathBuf::from)
}

pub(super) fn identity_database(artifact_root: &std::path::Path) -> PathBuf {
    env::var_os("ALLOYPORT_IDENTITY_DATABASE")
        .map_or_else(|| artifact_root.join("identities.sqlite3"), PathBuf::from)
}

fn tls_paths_from_environment() -> Result<Option<ServerTlsPaths>, Box<dyn Error>> {
    let certificate = env::var_os("ALLOYPORT_TLS_CERT");
    let private_key = env::var_os("ALLOYPORT_TLS_KEY");
    let client_ca = env::var_os("ALLOYPORT_TLS_CLIENT_CA");
    match (certificate, private_key, client_ca) {
        (None, None, None) => Ok(None),
        (Some(certificate), Some(private_key), Some(client_ca)) => Ok(Some(ServerTlsPaths {
            certificate: certificate.into(),
            private_key: private_key.into(),
            client_ca: client_ca.into(),
        })),
        _ => Err(
            "ALLOYPORT_TLS_CERT, ALLOYPORT_TLS_KEY and ALLOYPORT_TLS_CLIENT_CA must be set together"
                .into(),
        ),
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
