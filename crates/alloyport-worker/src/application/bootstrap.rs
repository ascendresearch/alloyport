//! Create-only executor bootstrap from a server-issued enrollment bundle.

use alloyport_proto::management_v1::GetServerStatusRequest;
use alloyport_proto::management_v1::management_service_client::ManagementServiceClient;
use serde_json::{Value, json};
use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

const SERVER_NAME: &str = "alloyport-server";

#[derive(Clone, Copy, Debug)]
enum WorkerRole {
    AscendBuild,
    AscendCandidate,
    CudaCorrectness,
    AscendCorrectness,
}

impl WorkerRole {
    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "ascend-build" => Ok(Self::AscendBuild),
            "ascend-candidate" => Ok(Self::AscendCandidate),
            "cuda-correctness" => Ok(Self::CudaCorrectness),
            "ascend-correctness" => Ok(Self::AscendCorrectness),
            _ => Err(format!(
                "unsupported worker role {value}; expected ascend-candidate, cuda-correctness, ascend-build, or ascend-correctness"
            )
            .into()),
        }
    }

    const fn worker_id(self) -> &'static str {
        match self {
            Self::AscendBuild => "ascend-build-worker-1",
            Self::AscendCandidate => "ascend-worker-1",
            Self::CudaCorrectness => "cuda-correctness-worker-1",
            Self::AscendCorrectness => "ascend-correctness-worker-1",
        }
    }

    fn template(self) -> &'static str {
        match self {
            Self::AscendBuild => {
                include_str!("../../../../docs/ascend-build-worker-config.example.json")
            }
            Self::AscendCandidate => {
                include_str!("../../../../docs/ascend-candidate-worker-config.example.json")
            }
            Self::CudaCorrectness => {
                include_str!("../../../../docs/cuda-correctness-worker-config.example.json")
            }
            Self::AscendCorrectness => {
                include_str!("../../../../docs/ascend-correctness-worker-config.example.json")
            }
        }
    }
}

pub(super) async fn run(arguments: &[OsString]) -> Result<(), Box<dyn Error>> {
    let [role, bundle, destination, endpoint] = arguments else {
        return Err(usage().into());
    };
    let role = WorkerRole::parse(role.to_str().ok_or("worker role must be UTF-8")?)?;
    let endpoint = endpoint
        .to_str()
        .ok_or("server endpoint must be UTF-8")?
        .to_owned();
    let root = prepare_root(Path::new(destination))?;
    let bundle = fs::canonicalize(bundle)?;
    if !bundle.is_dir() {
        return Err("worker enrollment bundle is not a directory".into());
    }
    for relative in ["pki", "state", "state/sandboxes", "state/artifacts"] {
        fs::create_dir_all(root.join(relative))?;
    }
    copy_regular_create_only(
        &bundle.join("worker.pem"),
        &root.join("pki/worker.pem"),
        0o644,
    )?;
    copy_regular_create_only(
        &bundle.join("worker-key.pem"),
        &root.join("pki/worker-key.pem"),
        0o600,
    )?;
    copy_regular_create_only(&bundle.join("ca.pem"), &root.join("pki/ca.pem"), 0o644)?;

    let config = worker_config(role, &root, &endpoint)?;
    write_json(&root.join("worker.json"), &config)?;
    println!(
        "AlloyPort {} bootstrap created {}",
        role.worker_id(),
        root.display()
    );

    match test_connectivity(&root, &endpoint).await {
        Ok(version) => println!("Server connectivity: ok (AlloyPort {version})"),
        Err(error) => println!("Server connectivity: failed ({error})"),
    }
    println!(
        "Fill the REPLACE_WITH_* and all-zero image fields in {}, then start:",
        root.join("worker.json").display()
    );
    println!(
        "  alloyport-worker --config {}",
        root.join("worker.json").display()
    );
    Ok(())
}

fn usage() -> &'static str {
    "usage: alloyport-worker bootstrap ROLE ENROLLMENT_BUNDLE DIRECTORY SERVER_ENDPOINT"
}

fn prepare_root(directory: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if directory.exists() {
        if !directory.is_dir() {
            return Err("bootstrap destination exists and is not a directory".into());
        }
        if fs::read_dir(directory)?.next().is_some() {
            return Err("bootstrap destination must be empty".into());
        }
    } else {
        fs::create_dir_all(directory)?;
    }
    Ok(fs::canonicalize(directory)?)
}

fn worker_config(role: WorkerRole, root: &Path, endpoint: &str) -> Result<Value, Box<dyn Error>> {
    let mut config: Value = serde_json::from_str(role.template())?;
    config["server"] = json!({
        "endpoint": endpoint,
        "tls": {
            "certificate": root.join("pki/worker.pem"),
            "private_key": root.join("pki/worker-key.pem"),
            "server_ca": root.join("pki/ca.pem"),
            "server_name": SERVER_NAME
        }
    });
    config["worker"]["id"] = Value::String(role.worker_id().to_owned());
    config["worker"]["journal"] = path_value(root.join("state/worker.sqlite3"))?;
    config["runtime"]["policy"]["sandbox_root"] = path_value(root.join("state/sandboxes"))?;
    config["runtime"]["policy"]["local_artifact_root"] = path_value(root.join("state/artifacts"))?;
    Ok(config)
}

fn path_value(path: PathBuf) -> Result<Value, Box<dyn Error>> {
    Ok(Value::String(
        path.into_os_string()
            .into_string()
            .map_err(|_| "bootstrap path must be UTF-8")?,
    ))
}

async fn test_connectivity(root: &Path, endpoint: &str) -> Result<String, Box<dyn Error>> {
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .tls_config(
            ClientTlsConfig::new()
                .identity(Identity::from_pem(
                    fs::read(root.join("pki/worker.pem"))?,
                    fs::read(root.join("pki/worker-key.pem"))?,
                ))
                .ca_certificate(Certificate::from_pem(fs::read(root.join("pki/ca.pem"))?))
                .domain_name(SERVER_NAME),
        )?
        .connect()
        .await?;
    let response = ManagementServiceClient::new(channel)
        .get_server_status(GetServerStatusRequest {})
        .await?
        .into_inner();
    Ok(response.server_version)
}

fn copy_regular_create_only(source: &Path, target: &Path, mode: u32) -> Result<(), Box<dyn Error>> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("enrollment file {} is not a regular file", source.display()).into());
    }
    write_create_only(target, &fs::read(source)?, mode)
}

fn write_json(path: &Path, value: &Value) -> Result<(), Box<dyn Error>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_create_only(path, &bytes, 0o644)
}

fn write_create_only(path: &Path, bytes: &[u8], mode: u32) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_cuda_config_has_local_absolute_state_and_tls_paths() -> Result<(), Box<dyn Error>>
    {
        let directory = tempfile::tempdir()?;
        let root = fs::canonicalize(directory.path())?;
        let config = worker_config(
            WorkerRole::CudaCorrectness,
            &root,
            "https://controller.example:50051",
        )?;
        assert_eq!(
            config["worker"]["id"],
            Value::String("cuda-correctness-worker-1".to_owned())
        );
        assert_eq!(
            config["server"]["tls"]["server_name"],
            Value::String(SERVER_NAME.to_owned())
        );
        assert!(
            config["worker"]["journal"]
                .as_str()
                .is_some_and(|path| Path::new(path).is_absolute())
        );
        Ok(())
    }
}
