//! Versioned process configuration for the `AlloyPort` server.

use serde::Deserialize;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerFileConfig {
    schema_version: u16,
    listen: Option<String>,
    database: Option<PathBuf>,
    artifact: Option<ArtifactFileConfig>,
    identity_database: Option<PathBuf>,
    tls: Option<TlsFileConfig>,
    shutdown_timeout_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ArtifactFileConfig {
    root: Option<PathBuf>,
    max_bytes: Option<u64>,
    max_chunk_bytes: Option<u64>,
    total_quota_bytes: Option<u64>,
    owner_quota_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsFileConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    client_ca: PathBuf,
}

impl ServerConfig {
    pub(super) fn load(explicit_path: Option<PathBuf>) -> Result<Self, Box<dyn Error>> {
        Self::load_with(explicit_path, |name| env::var_os(name))
    }

    fn load_with(
        explicit_path: Option<PathBuf>,
        environment: impl Fn(&str) -> Option<OsString>,
    ) -> Result<Self, Box<dyn Error>> {
        let configured_path = match explicit_path {
            Some(path) => Some(path),
            None => environment("ALLOYPORT_SERVER_CONFIG").map(PathBuf::from),
        };
        let (file, base) = load_file(configured_path.as_deref())?;
        let artifact_file = file.as_ref().and_then(|config| config.artifact.as_ref());
        let address = text_environment(&environment, "ALLOYPORT_LISTEN")?
            .or_else(|| file.as_ref().and_then(|config| config.listen.clone()))
            .unwrap_or_else(|| "127.0.0.1:50051".to_owned())
            .parse::<SocketAddr>()?;
        let tls = resolve_tls(&environment, file.as_ref(), base.as_deref())?;
        if tls.is_none() && !address.ip().is_loopback() {
            return Err("plaintext worker control is restricted to loopback".into());
        }
        let artifact_root = environment("ALLOYPORT_ARTIFACT_ROOT").map_or_else(
            || {
                Ok(resolve_file_path(
                    artifact_file.and_then(|artifact| artifact.root.as_ref()),
                    base.as_deref(),
                    Path::new("alloyport-artifacts"),
                ))
            },
            |path| Ok::<PathBuf, Box<dyn Error>>(path.into()),
        )?;
        let identity_database = environment("ALLOYPORT_IDENTITY_DATABASE").map_or_else(
            || {
                Ok(resolve_file_path(
                    file.as_ref()
                        .and_then(|config| config.identity_database.as_ref()),
                    base.as_deref(),
                    &artifact_root.join("identities.sqlite3"),
                ))
            },
            |path| Ok::<PathBuf, Box<dyn Error>>(path.into()),
        )?;
        let max_chunk_bytes = positive_value(
            &environment,
            "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES",
            artifact_file.and_then(|artifact| artifact.max_chunk_bytes),
            DEFAULT_MAX_UPLOAD_CHUNK_BYTES,
        )?;
        Ok(Self {
            address,
            database: resolve_environment_or_file_path(
                &environment,
                "ALLOYPORT_DATABASE",
                file.as_ref().and_then(|config| config.database.as_ref()),
                base.as_deref(),
                Path::new("alloyport-control.sqlite3"),
            ),
            artifact: ArtifactConfig {
                root: artifact_root,
                max_artifact_bytes: positive_value(
                    &environment,
                    "ALLOYPORT_ARTIFACT_MAX_BYTES",
                    artifact_file.and_then(|artifact| artifact.max_bytes),
                    DEFAULT_MAX_ARTIFACT_BYTES,
                )?,
                max_chunk_bytes: usize::try_from(max_chunk_bytes).map_err(
                    |_| "ALLOYPORT_ARTIFACT_MAX_CHUNK_BYTES exceeds this platform's usize",
                )?,
                total_quota_bytes: positive_value(
                    &environment,
                    "ALLOYPORT_ARTIFACT_TOTAL_QUOTA_BYTES",
                    artifact_file.and_then(|artifact| artifact.total_quota_bytes),
                    DEFAULT_TOTAL_ARTIFACT_QUOTA_BYTES,
                )?,
                per_owner_quota_bytes: positive_value(
                    &environment,
                    "ALLOYPORT_ARTIFACT_OWNER_QUOTA_BYTES",
                    artifact_file.and_then(|artifact| artifact.owner_quota_bytes),
                    DEFAULT_OWNER_ARTIFACT_QUOTA_BYTES,
                )?,
            },
            identity_database,
            tls,
            shutdown_timeout: Duration::from_secs(positive_value(
                &environment,
                "ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS",
                file.as_ref()
                    .and_then(|config| config.shutdown_timeout_seconds),
                DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
            )?),
        })
    }
}

fn load_file(
    path: Option<&Path>,
) -> Result<(Option<ServerFileConfig>, Option<PathBuf>), Box<dyn Error>> {
    let Some(path) = path else {
        return Ok((None, None));
    };
    let absolute = fs::canonicalize(path)?;
    let config: ServerFileConfig = serde_json::from_slice(&fs::read(&absolute)?)?;
    if config.schema_version != 1 {
        return Err(format!(
            "unsupported server config schema {}; expected 1",
            config.schema_version
        )
        .into());
    }
    Ok((config.into(), absolute.parent().map(Path::to_path_buf)))
}

fn resolve_tls(
    environment: &impl Fn(&str) -> Option<OsString>,
    file: Option<&ServerFileConfig>,
    base: Option<&Path>,
) -> Result<Option<ServerTlsPaths>, Box<dyn Error>> {
    let certificate = environment("ALLOYPORT_TLS_CERT");
    let private_key = environment("ALLOYPORT_TLS_KEY");
    let client_ca = environment("ALLOYPORT_TLS_CLIENT_CA");
    match (certificate, private_key, client_ca) {
        (None, None, None) => Ok(file.and_then(|config| config.tls.as_ref()).map(|tls| {
            ServerTlsPaths {
                certificate: resolve_file_path(Some(&tls.certificate), base, &tls.certificate),
                private_key: resolve_file_path(Some(&tls.private_key), base, &tls.private_key),
                client_ca: resolve_file_path(Some(&tls.client_ca), base, &tls.client_ca),
            }
        })),
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

fn resolve_environment_or_file_path(
    environment: &impl Fn(&str) -> Option<OsString>,
    name: &str,
    file: Option<&PathBuf>,
    base: Option<&Path>,
    default: &Path,
) -> PathBuf {
    environment(name).map_or_else(|| resolve_file_path(file, base, default), PathBuf::from)
}

fn resolve_file_path(file: Option<&PathBuf>, base: Option<&Path>, default: &Path) -> PathBuf {
    let path = file.map_or_else(|| default.to_path_buf(), Clone::clone);
    if file.is_some() && path.is_relative() {
        base.map_or(path.clone(), |base| base.join(path))
    } else {
        path
    }
}

fn positive_value(
    environment: &impl Fn(&str) -> Option<OsString>,
    name: &str,
    file: Option<u64>,
    default: u64,
) -> Result<u64, Box<dyn Error>> {
    let value = match text_environment(environment, name)? {
        Some(value) => value.parse::<u64>()?,
        None => file.unwrap_or(default),
    };
    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(value)
}

fn text_environment(
    environment: &impl Fn(&str) -> Option<OsString>,
    name: &str,
) -> Result<Option<String>, Box<dyn Error>> {
    environment(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| format!("{name} must be UTF-8").into())
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn file_paths_are_relative_to_file_and_environment_wins() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("server.json");
        fs::write(
            &path,
            br#"{
              "schema_version": 1,
              "listen": "127.0.0.1:6000",
              "database": "state/control.sqlite3",
              "artifact": {"root": "state/artifacts", "max_bytes": 2},
              "identity_database": "state/identities.sqlite3",
              "shutdown_timeout_seconds": 3
            }"#,
        )?;
        let environment = BTreeMap::from([
            ("ALLOYPORT_LISTEN", OsString::from("127.0.0.1:7000")),
            ("ALLOYPORT_SHUTDOWN_TIMEOUT_SECONDS", OsString::from("5")),
        ]);
        let config = ServerConfig::load_with(Some(path), |name| environment.get(name).cloned())?;
        assert_eq!(config.address.port(), 7_000);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(5));
        assert_eq!(
            config.database,
            directory.path().join("state/control.sqlite3")
        );
        assert_eq!(
            config.artifact.root,
            directory.path().join("state/artifacts")
        );
        assert_eq!(
            config.identity_database,
            directory.path().join("state/identities.sqlite3")
        );
        Ok(())
    }

    #[test]
    fn unknown_fields_versions_and_remote_plaintext_fail_closed() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        for (name, json) in [
            ("unknown.json", r#"{"schema_version":1,"surprise":true}"#),
            (
                "unknown-artifact.json",
                r#"{"schema_version":1,"artifact":{"surprise":true}}"#,
            ),
            (
                "unknown-tls.json",
                r#"{"schema_version":1,"tls":{"certificate":"server.pem","private_key":"server-key.pem","client_ca":"ca.pem","surprise":true}}"#,
            ),
            ("version.json", r#"{"schema_version":2}"#),
            (
                "plaintext.json",
                r#"{"schema_version":1,"listen":"0.0.0.0:50051"}"#,
            ),
        ] {
            let path = directory.path().join(name);
            fs::write(&path, json)?;
            assert!(ServerConfig::load_with(Some(path), |_| None).is_err());
        }
        Ok(())
    }

    #[test]
    fn explicit_locator_wins_over_environment_locator() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let explicit = directory.path().join("explicit.json");
        let environment = directory.path().join("environment.json");
        fs::write(&explicit, r#"{"schema_version":1}"#)?;
        fs::write(&environment, r#"{"schema_version":2}"#)?;
        let config = ServerConfig::load_with(Some(explicit), |name| {
            (name == "ALLOYPORT_SERVER_CONFIG").then(|| environment.clone().into_os_string())
        })?;
        assert_eq!(config.address.port(), 50_051);
        Ok(())
    }
}
