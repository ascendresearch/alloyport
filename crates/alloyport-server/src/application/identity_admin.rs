//! Offline certificate-enrollment administration.

use super::config::{artifact_root, identity_database};
use crate::adapters::sqlite::SqliteIdentityRegistry;
use crate::identity::{IdentityRegistry, certificate_fingerprint_from_pem};
use crate::storage::{Clock, SystemClock};
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs;

pub(super) fn try_run_from_args() -> Result<bool, Box<dyn Error>> {
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
