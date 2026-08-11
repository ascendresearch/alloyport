//! `SQLite` implementation of the durable identity-registry port.

use crate::identity::{
    CertificateEnrollment, EnrollmentState, IdentityError, IdentityRegistry, validate_owner,
};
use alloyport_artifacts::Sha256Digest;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::fmt::{self, Debug, Formatter};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

const SCHEMA: &str = r"
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS certificate_enrollments (
    fingerprint TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    state INTEGER NOT NULL,
    enrolled_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    replacement_fingerprint TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS certificate_enrollments_active_owner
    ON certificate_enrollments(owner_id) WHERE state = 1;
COMMIT;
";

pub struct SqliteIdentityRegistry {
    connection: Mutex<Connection>,
}

impl Debug for SqliteIdentityRegistry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteIdentityRegistry")
            .finish_non_exhaustive()
    }
}

impl From<rusqlite::Error> for IdentityError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl SqliteIdentityRegistry {
    /// Opens or creates the identity database and applies its schema.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the directory, database, or schema cannot be initialized.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IdentityError> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| IdentityError::Storage(Box::new(error)))?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Creates an in-memory identity database using the production schema.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the database or schema cannot be initialized.
    pub fn in_memory() -> Result<Self, IdentityError> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, IdentityError> {
        self.connection
            .lock()
            .map_err(|_| IdentityError::Corrupt("identity registry lock poisoned".into()))
    }
}

impl IdentityRegistry for SqliteIdentityRegistry {
    fn enroll(
        &self,
        owner_id: &str,
        fingerprint: Sha256Digest,
        now_ms: u64,
    ) -> Result<CertificateEnrollment, IdentityError> {
        validate_owner(owner_id)?;
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = enrollment_by_fingerprint(&transaction, fingerprint)? {
            if existing.owner_id == owner_id && existing.state == EnrollmentState::Active {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(IdentityError::Conflict(format!(
                "certificate {fingerprint} already belongs to {} in {:?} state",
                existing.owner_id, existing.state
            )));
        }
        if let Some(active) = active_enrollment_by_owner(&transaction, owner_id)? {
            return Err(IdentityError::Conflict(format!(
                "owner {owner_id} already uses active certificate {}",
                active.fingerprint
            )));
        }
        transaction.execute(
            "INSERT INTO certificate_enrollments(
                fingerprint, owner_id, state, enrolled_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?4)",
            params![
                fingerprint.to_string(),
                owner_id,
                EnrollmentState::Active as i64,
                to_i64(now_ms)?
            ],
        )?;
        let enrollment = enrollment_by_fingerprint(&transaction, fingerprint)?
            .ok_or_else(|| IdentityError::Corrupt("inserted enrollment disappeared".to_owned()))?;
        transaction.commit()?;
        Ok(enrollment)
    }

    fn rotate(
        &self,
        owner_id: &str,
        old_fingerprint: Sha256Digest,
        new_fingerprint: Sha256Digest,
        now_ms: u64,
    ) -> Result<CertificateEnrollment, IdentityError> {
        validate_owner(owner_id)?;
        if old_fingerprint == new_fingerprint {
            return Err(IdentityError::Invalid(
                "replacement certificate must be different",
            ));
        }
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old = enrollment_by_fingerprint(&transaction, old_fingerprint)?
            .ok_or(IdentityError::NotEnrolled(old_fingerprint))?;
        if old.owner_id == owner_id
            && old.state == EnrollmentState::Replaced
            && old.replacement_fingerprint == Some(new_fingerprint)
        {
            let replacement = enrollment_by_fingerprint(&transaction, new_fingerprint)?
                .ok_or_else(|| {
                    IdentityError::Corrupt("replacement enrollment is missing".into())
                })?;
            if replacement.owner_id == owner_id && replacement.state == EnrollmentState::Active {
                transaction.commit()?;
                return Ok(replacement);
            }
            return Err(IdentityError::Corrupt(
                "replacement enrollment is not the active owner certificate".into(),
            ));
        }
        if old.owner_id != owner_id || old.state != EnrollmentState::Active {
            return Err(IdentityError::Conflict(format!(
                "certificate {old_fingerprint} is not active for owner {owner_id}"
            )));
        }
        if let Some(existing) = enrollment_by_fingerprint(&transaction, new_fingerprint)? {
            return Err(IdentityError::Conflict(format!(
                "replacement certificate {new_fingerprint} already belongs to {} in {:?} state",
                existing.owner_id, existing.state
            )));
        }
        transaction.execute(
            "UPDATE certificate_enrollments
             SET state = ?2, updated_at_ms = ?3, replacement_fingerprint = ?4
             WHERE fingerprint = ?1",
            params![
                old_fingerprint.to_string(),
                EnrollmentState::Replaced as i64,
                to_i64(now_ms)?,
                new_fingerprint.to_string()
            ],
        )?;
        transaction.execute(
            "INSERT INTO certificate_enrollments(
                fingerprint, owner_id, state, enrolled_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?4)",
            params![
                new_fingerprint.to_string(),
                owner_id,
                EnrollmentState::Active as i64,
                to_i64(now_ms)?
            ],
        )?;
        let replacement = enrollment_by_fingerprint(&transaction, new_fingerprint)?
            .ok_or_else(|| IdentityError::Corrupt("replacement enrollment disappeared".into()))?;
        transaction.commit()?;
        Ok(replacement)
    }

    fn revoke(
        &self,
        fingerprint: Sha256Digest,
        now_ms: u64,
    ) -> Result<CertificateEnrollment, IdentityError> {
        let mut database = self.connection()?;
        let transaction = database.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enrollment = enrollment_by_fingerprint(&transaction, fingerprint)?
            .ok_or(IdentityError::NotEnrolled(fingerprint))?;
        if enrollment.state == EnrollmentState::Revoked {
            transaction.commit()?;
            return Ok(enrollment);
        }
        if enrollment.state == EnrollmentState::Replaced {
            return Err(IdentityError::Conflict(format!(
                "replaced certificate {fingerprint} is already inactive"
            )));
        }
        transaction.execute(
            "UPDATE certificate_enrollments
             SET state = ?2, updated_at_ms = ?3 WHERE fingerprint = ?1",
            params![
                fingerprint.to_string(),
                EnrollmentState::Revoked as i64,
                to_i64(now_ms)?
            ],
        )?;
        let revoked = enrollment_by_fingerprint(&transaction, fingerprint)?
            .ok_or_else(|| IdentityError::Corrupt("revoked enrollment disappeared".into()))?;
        transaction.commit()?;
        Ok(revoked)
    }

    fn resolve_fingerprint(&self, fingerprint: Sha256Digest) -> Result<String, IdentityError> {
        let database = self.connection()?;
        let enrollment = enrollment_by_fingerprint(&database, fingerprint)?
            .ok_or(IdentityError::NotEnrolled(fingerprint))?;
        match enrollment.state {
            EnrollmentState::Active => Ok(enrollment.owner_id),
            EnrollmentState::Replaced => Err(IdentityError::Replaced(fingerprint)),
            EnrollmentState::Revoked => Err(IdentityError::Revoked(fingerprint)),
        }
    }
}

fn enrollment_by_fingerprint(
    connection: &Connection,
    fingerprint: Sha256Digest,
) -> Result<Option<CertificateEnrollment>, IdentityError> {
    query_enrollment(connection, "fingerprint = ?1", [fingerprint.to_string()])
}

fn active_enrollment_by_owner(
    connection: &Connection,
    owner_id: &str,
) -> Result<Option<CertificateEnrollment>, IdentityError> {
    query_enrollment(
        connection,
        "owner_id = ?1 AND state = ?2",
        params![owner_id, EnrollmentState::Active as i64],
    )
}

fn query_enrollment(
    connection: &Connection,
    predicate: &str,
    parameters: impl rusqlite::Params,
) -> Result<Option<CertificateEnrollment>, IdentityError> {
    let sql = format!(
        "SELECT fingerprint, owner_id, state, replacement_fingerprint
         FROM certificate_enrollments WHERE {predicate}"
    );
    let row = connection
        .query_row(&sql, parameters, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .optional()?;
    row.map(|(fingerprint, owner_id, state, replacement)| {
        Ok(CertificateEnrollment {
            fingerprint: Sha256Digest::from_str(&fingerprint)
                .map_err(|error| IdentityError::Corrupt(error.to_string()))?,
            owner_id,
            state: EnrollmentState::from_i64(state)?,
            replacement_fingerprint: replacement
                .map(|fingerprint| {
                    Sha256Digest::from_str(&fingerprint)
                        .map_err(|error| IdentityError::Corrupt(error.to_string()))
                })
                .transpose()?,
        })
    })
    .transpose()
}

fn to_i64(value: u64) -> Result<i64, IdentityError> {
    i64::try_from(value)
        .map_err(|_| IdentityError::Invalid("timestamp exceeds storage integer range"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn rotation_is_atomic_idempotent_and_survives_restart() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("identities.sqlite3");
        let old = Sha256Digest::digest_bytes(b"old");
        let new = Sha256Digest::digest_bytes(b"new");
        {
            let registry = SqliteIdentityRegistry::open(&path)?;
            assert_eq!(
                registry.enroll("worker-1", old, 1)?.state,
                EnrollmentState::Active
            );
            assert_eq!(registry.enroll("worker-1", old, 2)?.fingerprint, old);
            assert_eq!(registry.rotate("worker-1", old, new, 3)?.fingerprint, new);
            assert_eq!(registry.rotate("worker-1", old, new, 4)?.fingerprint, new);
            assert!(matches!(
                registry.resolve_fingerprint(old),
                Err(IdentityError::Replaced(fingerprint)) if fingerprint == old
            ));
        }
        let registry = SqliteIdentityRegistry::open(path)?;
        assert_eq!(registry.resolve_fingerprint(new)?, "worker-1");
        Ok(())
    }

    #[test]
    fn conflict_and_revocation_never_reactivate_a_certificate() -> Result<(), Box<dyn Error>> {
        let registry = SqliteIdentityRegistry::in_memory()?;
        let first = Sha256Digest::digest_bytes(b"first");
        let second = Sha256Digest::digest_bytes(b"second");
        registry.enroll("worker-1", first, 1)?;
        assert!(matches!(
            registry.enroll("worker-2", first, 2),
            Err(IdentityError::Conflict(_))
        ));
        assert!(matches!(
            registry.enroll("worker-1", second, 2),
            Err(IdentityError::Conflict(_))
        ));
        assert_eq!(registry.revoke(first, 3)?.state, EnrollmentState::Revoked);
        assert_eq!(registry.revoke(first, 4)?.state, EnrollmentState::Revoked);
        assert!(matches!(
            registry.resolve_fingerprint(first),
            Err(IdentityError::Revoked(fingerprint)) if fingerprint == first
        ));
        assert!(matches!(
            registry.enroll("worker-1", first, 5),
            Err(IdentityError::Conflict(_))
        ));
        Ok(())
    }
}
