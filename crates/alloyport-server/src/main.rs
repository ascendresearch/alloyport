use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_server::WorkerControlService;
use std::env;
use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

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
    let service = WorkerControlService::open_sqlite(database)?;
    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }
    println!("AlloyPort worker control listening on {address}");
    let reaper_service = service.clone();
    let mut lease_reaper = tokio::spawn(async move { reaper_service.run_lease_reaper().await });
    let serve = server
        .add_service(WorkerControlServer::new(service))
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
