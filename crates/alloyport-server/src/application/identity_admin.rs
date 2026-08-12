//! Offline certificate-enrollment administration.

use super::command::IdentityAction;
use crate::adapters::sqlite::SqliteIdentityRegistry;
use crate::identity::{IdentityRegistry, certificate_fingerprint_from_pem};
use crate::storage::{Clock, SystemClock};
use std::error::Error;
use std::fs;
use std::path::Path;

pub(super) fn run(action: IdentityAction, identity_database: &Path) -> Result<(), Box<dyn Error>> {
    let registry = SqliteIdentityRegistry::open(identity_database)?;
    let now_ms = SystemClock.now_unix_ms();
    match action {
        IdentityAction::Enroll { owner, certificate } => {
            let fingerprint = certificate_fingerprint_from_pem(&fs::read(certificate)?)?;
            registry.enroll(&owner, fingerprint, now_ms)?;
            println!("enrolled {fingerprint} as {owner}");
        }
        IdentityAction::Rotate {
            owner,
            old_certificate,
            new_certificate,
        } => {
            let old_fingerprint = certificate_fingerprint_from_pem(&fs::read(old_certificate)?)?;
            let new_fingerprint = certificate_fingerprint_from_pem(&fs::read(new_certificate)?)?;
            registry.rotate(&owner, old_fingerprint, new_fingerprint, now_ms)?;
            println!("rotated {owner} from {old_fingerprint} to {new_fingerprint}");
        }
        IdentityAction::Revoke { certificate } => {
            let fingerprint = certificate_fingerprint_from_pem(&fs::read(certificate)?)?;
            let enrollment = registry.revoke(fingerprint, now_ms)?;
            println!("revoked {fingerprint} for {}", enrollment.owner_id);
        }
    }
    Ok(())
}
