//! Durable mapping from verified client certificates to stable logical worker identities.

use alloyport_artifacts::Sha256Digest;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::fs;
use std::io::{self, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::{Extensions, Status};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum EnrollmentState {
    Active = 1,
    Replaced = 2,
    Revoked = 3,
}

impl EnrollmentState {
    fn from_i64(value: i64) -> Result<Self, IdentityError> {
        match value {
            1 => Ok(Self::Active),
            2 => Ok(Self::Replaced),
            3 => Ok(Self::Revoked),
            _ => Err(IdentityError::Corrupt(format!(
                "unknown certificate enrollment state {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertificateEnrollment {
    pub fingerprint: Sha256Digest,
    pub owner_id: String,
    pub state: EnrollmentState,
    pub replacement_fingerprint: Option<Sha256Digest>,
}

#[derive(Debug)]
pub enum IdentityError {
    Sqlite(rusqlite::Error),
    Io(io::Error),
    Certificate(String),
    Invalid(&'static str),
    NotEnrolled(Sha256Digest),
    Revoked(Sha256Digest),
    Replaced(Sha256Digest),
    Conflict(String),
    Corrupt(String),
}

impl Display for IdentityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => Display::fmt(error, formatter),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Certificate(detail) => write!(formatter, "invalid certificate PEM: {detail}"),
            Self::Invalid(detail) => write!(formatter, "invalid identity enrollment: {detail}"),
            Self::NotEnrolled(fingerprint) => {
                write!(formatter, "certificate {fingerprint} is not enrolled")
            }
            Self::Revoked(fingerprint) => write!(formatter, "certificate {fingerprint} is revoked"),
            Self::Replaced(fingerprint) => {
                write!(formatter, "certificate {fingerprint} has been replaced")
            }
            Self::Conflict(detail) => write!(formatter, "identity enrollment conflict: {detail}"),
            Self::Corrupt(detail) => write!(formatter, "corrupt identity registry: {detail}"),
        }
    }
}

impl Error for IdentityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for IdentityError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedConnectionIdentity {
    pub owner_id: String,
    pub fingerprint: Sha256Digest,
}

/// Resolves one authenticated connection to a stable logical owner.
pub trait ConnectionIdentityResolver: Debug + Send + Sync {
    /// Resolves the verified peer certificate attached by tonic transport.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status for absent TLS identity, inactive enrollment, or storage failure.
    fn resolve_identity(
        &self,
        extensions: &Extensions,
    ) -> Result<ResolvedConnectionIdentity, Status>;

    /// Confirms that a previously resolved credential remains active.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status after certificate replacement, revocation, or registry failure.
    fn revalidate(&self, identity: &ResolvedConnectionIdentity) -> Result<(), Status>;

    /// Resolves only the stable owner for request/response services.
    ///
    /// # Errors
    ///
    /// Returns the same status as [`Self::resolve_identity`].
    fn resolve_owner(&self, extensions: &Extensions) -> Result<String, Status> {
        self.resolve_identity(extensions)
            .map(|identity| identity.owner_id)
    }
}

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

#[allow(clippy::missing_errors_doc)]
impl SqliteIdentityRegistry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IdentityError> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(IdentityError::Io)?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn in_memory() -> Result<Self, IdentityError> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn enroll(
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

    pub fn rotate(
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

    pub fn revoke(
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

    pub fn resolve_fingerprint(&self, fingerprint: Sha256Digest) -> Result<String, IdentityError> {
        let database = self.connection()?;
        let enrollment = enrollment_by_fingerprint(&database, fingerprint)?
            .ok_or(IdentityError::NotEnrolled(fingerprint))?;
        match enrollment.state {
            EnrollmentState::Active => Ok(enrollment.owner_id),
            EnrollmentState::Replaced => Err(IdentityError::Replaced(fingerprint)),
            EnrollmentState::Revoked => Err(IdentityError::Revoked(fingerprint)),
        }
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, IdentityError> {
        self.connection
            .lock()
            .map_err(|_| IdentityError::Corrupt("identity registry lock poisoned".into()))
    }
}

impl ConnectionIdentityResolver for SqliteIdentityRegistry {
    fn resolve_identity(
        &self,
        extensions: &Extensions,
    ) -> Result<ResolvedConnectionIdentity, Status> {
        let fingerprint =
            peer_certificate_fingerprint(extensions).map_err(|error| identity_status(&error))?;
        let owner_id = self
            .resolve_fingerprint(fingerprint)
            .map_err(|error| identity_status(&error))?;
        Ok(ResolvedConnectionIdentity {
            owner_id,
            fingerprint,
        })
    }

    fn revalidate(&self, identity: &ResolvedConnectionIdentity) -> Result<(), Status> {
        let owner_id = self
            .resolve_fingerprint(identity.fingerprint)
            .map_err(|error| identity_status(&error))?;
        if owner_id == identity.owner_id {
            Ok(())
        } else {
            Err(Status::permission_denied(
                "certificate enrollment owner changed unexpectedly",
            ))
        }
    }
}

/// Hashes the verified client leaf certificate from tonic's TLS connection information.
///
/// # Errors
///
/// Returns an identity error when the request is not mutually authenticated or has no leaf cert.
pub fn peer_certificate_fingerprint(
    extensions: &Extensions,
) -> Result<Sha256Digest, IdentityError> {
    let connection = extensions
        .get::<TlsConnectInfo<TcpConnectInfo>>()
        .ok_or_else(|| IdentityError::Certificate("mutual TLS connection is missing".into()))?;
    let certificates = connection
        .peer_certs()
        .ok_or_else(|| IdentityError::Certificate("client certificate is missing".into()))?;
    let leaf = certificates
        .first()
        .ok_or_else(|| IdentityError::Certificate("client certificate chain is empty".into()))?;
    Ok(Sha256Digest::digest_bytes(leaf.as_ref()))
}

/// Hashes the first certificate in a PEM certificate chain.
///
/// # Errors
///
/// Returns an identity error when PEM parsing fails or no certificate is present.
pub fn certificate_fingerprint_from_pem(pem: &[u8]) -> Result<Sha256Digest, IdentityError> {
    let mut reader = BufReader::new(pem);
    let certificate = rustls_pemfile::certs(&mut reader)
        .next()
        .transpose()
        .map_err(|error| IdentityError::Certificate(error.to_string()))?
        .ok_or_else(|| IdentityError::Certificate("no certificate was found".into()))?;
    Ok(Sha256Digest::digest_bytes(certificate.as_ref()))
}

fn identity_status(error: &IdentityError) -> Status {
    match error {
        IdentityError::NotEnrolled(_) | IdentityError::Certificate(_) => {
            Status::unauthenticated(error.to_string())
        }
        IdentityError::Revoked(_)
        | IdentityError::Replaced(_)
        | IdentityError::Conflict(_)
        | IdentityError::Invalid(_) => Status::permission_denied(error.to_string()),
        IdentityError::Sqlite(_) | IdentityError::Io(_) | IdentityError::Corrupt(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn validate_owner(owner_id: &str) -> Result<(), IdentityError> {
    if owner_id.trim().is_empty() {
        Err(IdentityError::Invalid("owner ID is missing"))
    } else {
        Ok(())
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
        .map_err(|_| IdentityError::Invalid("timestamp exceeds SQLite integer range"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
