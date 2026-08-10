use alloyport_proto::v1::{Backend, WorkerCapabilities, WorkerHello};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_worker::OutboundWorker;
use std::env;
use std::error::Error;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint_uri =
        env::var("ALLOYPORT_SERVER").unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
    let endpoint = endpoint(&endpoint_uri)?;
    let worker_id = env::var("ALLOYPORT_WORKER_ID")?;
    let backend = parse_backend(&env::var("ALLOYPORT_BACKEND")?)?;
    let instance_id = instance_id(&worker_id)?;
    let worker = OutboundWorker::new(
        endpoint,
        WorkerHello {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            worker_id,
            instance_id,
            worker_version: env!("CARGO_PKG_VERSION").to_owned(),
            features: Vec::new(),
            capabilities: Some(WorkerCapabilities {
                backend: backend.into(),
                architecture: env::var("ALLOYPORT_ARCH").unwrap_or_default(),
                device_count: parse_u32("ALLOYPORT_DEVICE_COUNT", 1)?,
                max_concurrency: parse_u32("ALLOYPORT_MAX_CONCURRENCY", 1)?,
                driver_version: env::var("ALLOYPORT_DRIVER_VERSION").unwrap_or_default(),
                toolkit_version: env::var("ALLOYPORT_TOOLKIT_VERSION").unwrap_or_default(),
                container_runtime: env::var("ALLOYPORT_CONTAINER_RUNTIME")
                    .unwrap_or_else(|_| "docker".to_owned()),
            }),
            active_attempts: Vec::new(),
        },
    )?;

    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::select! {
            result = worker.run_session() => {
                if let Err(error) = result {
                    eprintln!("worker session ended: {error}; reconnecting in {backoff:?}");
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn endpoint(uri: &str) -> Result<Endpoint, Box<dyn Error>> {
    let mut endpoint = Endpoint::from_shared(uri.to_owned())?;
    let certificate = env::var_os("ALLOYPORT_TLS_CERT");
    let key = env::var_os("ALLOYPORT_TLS_KEY");
    let server_ca = env::var_os("ALLOYPORT_TLS_SERVER_CA");
    let server_name = env::var("ALLOYPORT_TLS_SERVER_NAME").ok();
    match (certificate, key, server_ca, server_name) {
        (None, None, None, None) if is_loopback_uri(uri) => {}
        (Some(certificate), Some(key), Some(server_ca), Some(server_name)) => {
            let identity = Identity::from_pem(fs::read(certificate)?, fs::read(key)?);
            let server_ca = Certificate::from_pem(fs::read(server_ca)?);
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(server_ca)
                    .domain_name(server_name),
            )?;
        }
        _ => {
            return Err("remote workers require ALLOYPORT_TLS_CERT, ALLOYPORT_TLS_KEY, ALLOYPORT_TLS_SERVER_CA and ALLOYPORT_TLS_SERVER_NAME"
                .into());
        }
    }
    Ok(endpoint)
}

fn is_loopback_uri(uri: &str) -> bool {
    uri.starts_with("http://127.0.0.1:")
        || uri.starts_with("http://localhost:")
        || uri.starts_with("http://[::1]:")
}

fn parse_backend(value: &str) -> Result<Backend, Box<dyn Error>> {
    match value.to_ascii_lowercase().as_str() {
        "cuda" => Ok(Backend::Cuda),
        "ascend" | "npu" => Ok(Backend::Ascend),
        _ => {
            Err(format!("unsupported ALLOYPORT_BACKEND {value:?}; expected cuda or ascend").into())
        }
    }
}

fn parse_u32(name: &str, default: u32) -> Result<u32, Box<dyn Error>> {
    Ok(env::var(name).map_or(Ok(default), |value| value.parse())?)
}

fn instance_id(worker_id: &str) -> Result<String, Box<dyn Error>> {
    let unix_nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{worker_id}-{}-{unix_nanos}", std::process::id()))
}
