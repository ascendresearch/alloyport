use alloyport_server::identity::{
    EnrollmentState, IdentityError, SqliteIdentityRegistry, certificate_fingerprint_from_pem,
};
use rcgen::generate_simple_self_signed;
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

#[test]
fn identity_cli_enrolls_rotates_revokes_and_rejects_conflicts() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("identities.sqlite3");
    let old_certificate = certificate(directory.path(), "old")?;
    let new_certificate = certificate(directory.path(), "new")?;

    assert_success(&run_identity(
        &database,
        [
            "enroll".as_ref(),
            "worker-1".as_ref(),
            old_certificate.as_os_str(),
        ],
    )?);
    let conflict = run_identity(
        &database,
        [
            "enroll".as_ref(),
            "worker-2".as_ref(),
            old_certificate.as_os_str(),
        ],
    )?;
    assert!(!conflict.status.success());
    assert_success(&run_identity(
        &database,
        [
            "rotate".as_ref(),
            "worker-1".as_ref(),
            old_certificate.as_os_str(),
            new_certificate.as_os_str(),
        ],
    )?);
    assert_success(&run_identity(
        &database,
        ["revoke".as_ref(), new_certificate.as_os_str()],
    )?);

    let registry = SqliteIdentityRegistry::open(database)?;
    let old_fingerprint = certificate_fingerprint_from_pem(&fs::read(old_certificate)?)?;
    let new_fingerprint = certificate_fingerprint_from_pem(&fs::read(new_certificate)?)?;
    assert!(matches!(
        registry.resolve_fingerprint(old_fingerprint),
        Err(IdentityError::Replaced(_))
    ));
    assert_eq!(
        registry.revoke(new_fingerprint, 10)?.state,
        EnrollmentState::Revoked
    );
    Ok(())
}

fn certificate(directory: &Path, name: &str) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let certified = generate_simple_self_signed(vec![name.to_owned()])?;
    let path = directory.join(format!("{name}.pem"));
    fs::write(&path, certified.cert.pem())?;
    Ok(path)
}

fn run_identity<const N: usize>(
    database: &Path,
    arguments: [&OsStr; N],
) -> Result<Output, Box<dyn Error>> {
    Ok(Command::new(env!("CARGO_BIN_EXE_alloyport-server"))
        .env("ALLOYPORT_IDENTITY_DATABASE", database)
        .arg("identity")
        .args(arguments)
        .output()?)
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "identity command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
