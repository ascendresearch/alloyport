//! Create-only deployment bootstrap for one `AlloyPort` server and its initial clients.

use crate::adapters::sqlite::SqliteIdentityRegistry;
use crate::identity::{IdentityRegistry, certificate_fingerprint_from_pem};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SERVER_NAME: &str = "alloyport-server";
const WORKER_IDS: [&str; 3] = [
    "ascend-build-worker-1",
    "cuda-correctness-worker-1",
    "ascend-correctness-worker-1",
];

struct PemIdentity {
    certificate: String,
    private_key: String,
}

pub(super) fn run(directory: &Path) -> Result<(), Box<dyn Error>> {
    let root = prepare_root(directory)?;
    for relative in [
        "pki",
        "clients/admin",
        "workers/ascend-build-worker-1",
        "workers/cuda-correctness-worker-1",
        "workers/ascend-correctness-worker-1",
        "state/artifacts",
        "state/migrations",
    ] {
        fs::create_dir_all(root.join(relative))?;
    }

    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "AlloyPort deployment CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = ca_params.self_signed(&ca_key)?;
    let ca_pem = ca.pem();
    write_public(&root.join("pki/ca.pem"), ca_pem.as_bytes())?;
    write_secret(
        &root.join("pki/ca-key.pem"),
        ca_key.serialize_pem().as_bytes(),
    )?;

    let server = signed_identity(
        SERVER_NAME,
        vec![
            SERVER_NAME.to_owned(),
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
            "::1".to_owned(),
        ],
        ExtendedKeyUsagePurpose::ServerAuth,
        &ca,
        &ca_key,
    )?;
    write_identity(&root.join("pki"), "server", &server)?;

    let admin = signed_identity(
        "alloyport-admin",
        Vec::new(),
        ExtendedKeyUsagePurpose::ClientAuth,
        &ca,
        &ca_key,
    )?;
    write_identity(&root.join("clients/admin"), "admin", &admin)?;
    write_public(&root.join("clients/admin/ca.pem"), ca_pem.as_bytes())?;

    let identity_database = root.join("state/identities.sqlite3");
    let identities = SqliteIdentityRegistry::open(&identity_database)?;
    enroll(&identities, "alloyport-admin", &admin.certificate)?;
    for worker_id in WORKER_IDS {
        let worker = signed_identity(
            worker_id,
            Vec::new(),
            ExtendedKeyUsagePurpose::ClientAuth,
            &ca,
            &ca_key,
        )?;
        let worker_root = root.join("workers").join(worker_id);
        write_identity(&worker_root, "worker", &worker)?;
        write_public(&worker_root.join("ca.pem"), ca_pem.as_bytes())?;
        enroll(&identities, worker_id, &worker.certificate)?;
    }

    write_json(&root.join("server.json"), &server_config())?;
    write_json(&root.join("clients/admin/client.json"), &cli_config())?;
    write_runtime_assets(&root)?;

    println!("AlloyPort server bootstrap created {}", root.display());
    println!("Start the daemon:");
    println!(
        "  alloyport-server --config {}",
        root.join("server.json").display()
    );
    println!("Check it from this host:");
    println!(
        "  alloyport-cli --config {} server status",
        root.join("clients/admin/client.json").display()
    );
    println!("On each executor host, copy its enrollment bundle and run:");
    for (role, worker_id) in [
        ("ascend-build", "ascend-build-worker-1"),
        ("cuda-correctness", "cuda-correctness-worker-1"),
        ("ascend-correctness", "ascend-correctness-worker-1"),
    ] {
        println!(
            "  alloyport-worker bootstrap {role} {} ./alloyport-worker https://SERVER_ADDRESS:50051",
            root.join("workers").join(worker_id).display()
        );
    }
    println!(
        "Before submitting a real migration, configure provider.json and replace image placeholders in candidate.json and each worker.json."
    );
    Ok(())
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

fn signed_identity(
    common_name: &str,
    subject_alt_names: Vec<String>,
    purpose: ExtendedKeyUsagePurpose,
    ca: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<PemIdentity, rcgen::Error> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(subject_alt_names)?;
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![purpose];
    let certificate = params.signed_by(&key, ca, ca_key)?;
    Ok(PemIdentity {
        certificate: certificate.pem(),
        private_key: key.serialize_pem(),
    })
}

fn write_identity(root: &Path, stem: &str, identity: &PemIdentity) -> Result<(), Box<dyn Error>> {
    write_public(
        &root.join(format!("{stem}.pem")),
        identity.certificate.as_bytes(),
    )?;
    write_secret(
        &root.join(format!("{stem}-key.pem")),
        identity.private_key.as_bytes(),
    )
}

fn enroll(
    identities: &dyn IdentityRegistry,
    owner_id: &str,
    certificate: &str,
) -> Result<(), Box<dyn Error>> {
    let fingerprint = certificate_fingerprint_from_pem(certificate.as_bytes())?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    identities.enroll(owner_id, fingerprint, now_ms)?;
    Ok(())
}

fn server_config() -> Value {
    json!({
        "schema_version": 1,
        "listen": "0.0.0.0:50051",
        "database": "state/control.sqlite3",
        "artifact": { "root": "state/artifacts" },
        "identity_database": "state/identities.sqlite3",
        "tls": {
            "certificate": "pki/server.pem",
            "private_key": "pki/server-key.pem",
            "client_ca": "pki/ca.pem"
        },
        "shutdown_timeout_seconds": 10,
        "migration_runtime": {
            "candidate_template": "candidate.json",
            "root": "state/migrations"
        }
    })
}

fn cli_config() -> Value {
    json!({
        "schema_version": 1,
        "server": {
            "endpoint": "https://127.0.0.1:50051",
            "tls": {
                "certificate": "admin.pem",
                "private_key": "admin-key.pem",
                "server_ca": "ca.pem",
                "server_name": SERVER_NAME
            }
        }
    })
}

fn write_runtime_assets(root: &Path) -> Result<(), Box<dyn Error>> {
    let mut candidate: Value = serde_json::from_str(include_str!(
        "../../../../docs/candidate-episode-config.example.json"
    ))?;
    candidate["model_catalog"] = Value::String("provider.json".to_owned());
    candidate["episode"]["system_prompt"] = Value::String("system-prompt.md".to_owned());
    candidate["episode"]["initial_user_text"] = Value::String("user-prompt.md".to_owned());
    write_json(&root.join("candidate.json"), &candidate)?;
    let provider: Value = serde_json::from_str(include_str!(
        "../../../../docs/runtime-model-catalog.example.json"
    ))?;
    write_json(&root.join("provider.json"), &provider)?;
    write_public(
        &root.join("system-prompt.md"),
        include_bytes!("../../../../docs/candidate-system-prompt.example.md"),
    )?;
    write_public(
        &root.join("user-prompt.md"),
        include_bytes!("../../../../docs/candidate-user-prompt.example.md"),
    )?;
    Ok(())
}

fn write_json(path: &Path, value: &Value) -> Result<(), Box<dyn Error>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_public(path, &bytes)
}

fn write_public(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    write_create_only(path, bytes, 0o644)
}

fn write_secret(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    write_create_only(path, bytes, 0o600)
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
    use crate::application::config::ServerConfig;
    use crate::identity::certificate_fingerprint_from_pem;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn bootstrap_creates_loadable_config_pki_and_enrollments() -> Result<(), Box<dyn Error>> {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("deployment");
        run(&root)?;

        let config = ServerConfig::load(Some(root.join("server.json")))?;
        assert!(config.tls.is_some());
        assert!(config.migration_runtime.is_some());
        assert_eq!(
            fs::metadata(root.join("pki/server-key.pem"))?
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let registry = SqliteIdentityRegistry::open(root.join("state/identities.sqlite3"))?;
        let worker_certificate =
            fs::read(root.join("workers/cuda-correctness-worker-1/worker.pem"))?;
        let fingerprint = certificate_fingerprint_from_pem(&worker_certificate)?;
        assert_eq!(
            registry.resolve_fingerprint(fingerprint)?,
            "cuda-correctness-worker-1"
        );
        assert!(run(&root).is_err());
        Ok(())
    }
}
