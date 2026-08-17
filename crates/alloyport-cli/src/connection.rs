//! Where the CLI connects and how it proves who it is.
//!
//! Split out of `main.rs` for the module-size limit. The endpoint and its mTLS material are read
//! once, from an explicit locator, so no command can quietly reach a different server than another.

use alloyport_proto::interaction_v1::interaction_service_client::InteractionServiceClient;
use alloyport_proto::management_v1::management_service_client::ManagementServiceClient;
use alloyport_proto::{
    MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES, MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES,
};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

const DEFAULT_SERVER_ENDPOINT: &str = "http://127.0.0.1:50051";
const SIBLING_CONFIG_NAME: &str = "alloyport-cli.json";
const SYSTEM_CONFIG_PATH: &str = "/etc/alloyport-cli/client.json";
pub(crate) static CONNECTION: OnceLock<CliConnectionConfig> = OnceLock::new();

#[derive(Clone, Debug)]
pub(crate) struct CliConnectionConfig {
    endpoint: String,
    tls: Option<CliTlsConfig>,
}

#[derive(Clone, Debug)]
struct CliTlsConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    server_ca: PathBuf,
    server_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliFileConfig {
    schema_version: u16,
    server: CliServerFileConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliServerFileConfig {
    endpoint: String,
    tls: Option<CliTlsFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliTlsFileConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    server_ca: PathBuf,
    server_name: String,
}

impl CliConnectionConfig {
    pub(crate) fn load(explicit: Option<PathBuf>) -> Result<Self, String> {
        let path = explicit
            .or_else(|| env::var_os("ALLOYPORT_CLI_CONFIG").map(PathBuf::from))
            .or_else(|| {
                env::current_exe()
                    .ok()
                    .and_then(|executable| executable.parent().map(Path::to_path_buf))
                    .map(|directory| directory.join(SIBLING_CONFIG_NAME))
                    .filter(|path| path.is_file())
            })
            .or_else(|| {
                let path = PathBuf::from(SYSTEM_CONFIG_PATH);
                path.is_file().then_some(path)
            });
        let Some(path) = path else {
            return Ok(Self {
                endpoint: DEFAULT_SERVER_ENDPOINT.to_owned(),
                tls: None,
            });
        };
        let path = fs::canonicalize(&path)
            .map_err(|error| format!("cannot open CLI config {}: {error}", path.display()))?;
        let base = path
            .parent()
            .ok_or_else(|| "CLI config has no parent directory".to_owned())?;
        let file: CliFileConfig = serde_json::from_slice(
            &fs::read(&path)
                .map_err(|error| format!("cannot read CLI config {}: {error}", path.display()))?,
        )
        .map_err(|error| format!("invalid CLI config {}: {error}", path.display()))?;
        if file.schema_version != 1 {
            return Err(format!(
                "unsupported CLI config schema {}; expected 1",
                file.schema_version
            ));
        }
        if file.server.endpoint.trim().is_empty() {
            return Err("CLI server endpoint is required".to_owned());
        }
        let tls = file.server.tls.map(|tls| CliTlsConfig {
            certificate: resolve_config_path(base, tls.certificate),
            private_key: resolve_config_path(base, tls.private_key),
            server_ca: resolve_config_path(base, tls.server_ca),
            server_name: tls.server_name,
        });
        if tls
            .as_ref()
            .is_some_and(|tls| tls.server_name.trim().is_empty())
        {
            return Err("CLI TLS server_name is required".to_owned());
        }
        Ok(Self {
            endpoint: file.server.endpoint,
            tls,
        })
    }
}

fn resolve_config_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_relative() {
        base.join(path)
    } else {
        path
    }
}

pub(crate) fn server_endpoint() -> String {
    env::var("ALLOYPORT_SERVER_ENDPOINT").unwrap_or_else(|_| {
        CONNECTION.get().map_or_else(
            || DEFAULT_SERVER_ENDPOINT.to_owned(),
            |config| config.endpoint.clone(),
        )
    })
}

async fn server_channel() -> Result<Channel, String> {
    let endpoint_uri = server_endpoint();
    let mut endpoint = Endpoint::from_shared(endpoint_uri.clone())
        .map_err(|error| format!("invalid AlloyPort server endpoint {endpoint_uri}: {error}"))?;
    if let Some(tls) = CONNECTION.get().and_then(|config| config.tls.as_ref()) {
        let identity = Identity::from_pem(
            fs::read(&tls.certificate).map_err(|error| {
                format!(
                    "cannot read client certificate {}: {error}",
                    tls.certificate.display()
                )
            })?,
            fs::read(&tls.private_key).map_err(|error| {
                format!(
                    "cannot read client private key {}: {error}",
                    tls.private_key.display()
                )
            })?,
        );
        let ca = Certificate::from_pem(fs::read(&tls.server_ca).map_err(|error| {
            format!("cannot read server CA {}: {error}", tls.server_ca.display())
        })?);
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(ca)
                    .domain_name(tls.server_name.clone()),
            )
            .map_err(|error| format!("invalid CLI TLS configuration: {error}"))?;
    }
    endpoint
        .connect()
        .await
        .map_err(|error| format!("cannot connect to AlloyPort server at {endpoint_uri}: {error}"))
}

pub(crate) async fn management_client() -> Result<ManagementServiceClient<Channel>, String> {
    server_channel()
        .await
        .map(ManagementServiceClient::new)
        .map(|client| {
            client
                .max_encoding_message_size(MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES)
        })
}

pub(crate) async fn interaction_client() -> Result<InteractionServiceClient<Channel>, String> {
    server_channel()
        .await
        .map(InteractionServiceClient::new)
        .map(|client| {
            client
                .max_encoding_message_size(alloyport_proto::MAX_INTERACTION_REQUEST_MESSAGE_BYTES)
                .max_decoding_message_size(alloyport_proto::MAX_INTERACTION_EVENT_MESSAGE_BYTES)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_cli_config_resolves_tls_files_relative_to_it() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("client.json");
        fs::write(
            &path,
            r#"{
              "schema_version": 1,
              "server": {
                "endpoint": "https://controller.example:50051",
                "tls": {
                  "certificate": "pki/client.pem",
                  "private_key": "pki/client-key.pem",
                  "server_ca": "pki/ca.pem",
                  "server_name": "alloyport-server"
                }
              }
            }"#,
        )
        .map_err(|error| error.to_string())?;

        let config = CliConnectionConfig::load(Some(path))?;
        let tls = config.tls.expect("TLS config");
        assert_eq!(config.endpoint, "https://controller.example:50051");
        assert_eq!(tls.certificate, directory.path().join("pki/client.pem"));
        assert_eq!(tls.server_name, "alloyport-server");
        Ok(())
    }
}
