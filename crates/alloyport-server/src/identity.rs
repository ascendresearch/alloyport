//! Certificate identity domain, persistence port, and mTLS transport resolver.

use alloyport_artifacts::Sha256Digest;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::BufReader;
use std::sync::Arc;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::{Extensions, Status};

use crate::grpc_status::identity_status;
use crate::persistence::ServerPersistence;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum EnrollmentState {
    Active = 1,
    Replaced = 2,
    Revoked = 3,
}

impl EnrollmentState {
    pub(crate) fn from_i64(value: i64) -> Result<Self, IdentityError> {
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
    Storage(Box<dyn Error + Send + Sync>),
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
            Self::Storage(error) => Display::fmt(error, formatter),
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
            Self::Storage(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Durable certificate-enrollment capabilities required by the application layer.
pub trait IdentityRegistry: Debug + Send + Sync {
    /// Enrolls an active certificate for an owner idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, conflicting enrollment, or storage failure.
    fn enroll(
        &self,
        owner_id: &str,
        fingerprint: Sha256Digest,
        now_ms: u64,
    ) -> Result<CertificateEnrollment, IdentityError>;

    /// Atomically replaces an owner's active certificate.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, conflicting state, or storage failure.
    fn rotate(
        &self,
        owner_id: &str,
        old_fingerprint: Sha256Digest,
        new_fingerprint: Sha256Digest,
        now_ms: u64,
    ) -> Result<CertificateEnrollment, IdentityError>;

    /// Revokes a certificate idempotently without permitting reactivation.
    ///
    /// # Errors
    ///
    /// Returns an error when the certificate is unknown, replaced, or cannot be persisted.
    fn revoke(
        &self,
        fingerprint: Sha256Digest,
        now_ms: u64,
    ) -> Result<CertificateEnrollment, IdentityError>;

    /// Resolves an active certificate to its stable owner.
    ///
    /// # Errors
    ///
    /// Returns an error when the certificate is unknown, inactive, or cannot be read.
    fn resolve_fingerprint(&self, fingerprint: Sha256Digest) -> Result<String, IdentityError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedConnectionIdentity {
    pub owner_id: String,
    pub fingerprint: Sha256Digest,
}

/// Authenticated principal retained for the lifetime of one gRPC request or stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedRequestContext {
    owner_id: String,
    identity: Option<ResolvedConnectionIdentity>,
}

impl AuthenticatedRequestContext {
    /// Creates an explicitly trusted local context for tests and in-process adapters.
    #[must_use]
    pub fn local(owner_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            identity: None,
        }
    }

    /// Creates a context backed by a verified transport credential.
    #[must_use]
    pub fn verified(identity: ResolvedConnectionIdentity) -> Self {
        Self {
            owner_id: identity.owner_id.clone(),
            identity: Some(identity),
        }
    }

    #[must_use]
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    #[must_use]
    pub fn connection_identity(&self) -> Option<&ResolvedConnectionIdentity> {
        self.identity.as_ref()
    }
}

/// Resolves one authenticated transport connection to a stable logical owner.
#[tonic::async_trait]
pub trait ConnectionIdentityResolver: Debug + Send + Sync {
    /// Resolves the verified peer certificate carried by transport extensions.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when authentication or registry resolution fails.
    async fn resolve_identity(
        &self,
        extensions: &Extensions,
    ) -> Result<ResolvedConnectionIdentity, Status>;

    /// Confirms that a previously resolved credential remains active for the same owner.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status after replacement, revocation, or registry failure.
    async fn revalidate(&self, identity: &ResolvedConnectionIdentity) -> Result<(), Status>;

    /// Resolves a transport credential into request-lifetime authentication state.
    ///
    /// # Errors
    ///
    /// Returns the same status classifications as [`Self::resolve_identity`].
    async fn resolve_context(
        &self,
        extensions: &Extensions,
    ) -> Result<AuthenticatedRequestContext, Status> {
        self.resolve_identity(extensions)
            .await
            .map(AuthenticatedRequestContext::verified)
    }

    /// Revalidates the verified credential retained by a request context.
    ///
    /// # Errors
    ///
    /// Returns unauthenticated for a local context or the status from [`Self::revalidate`].
    async fn revalidate_context(
        &self,
        context: &AuthenticatedRequestContext,
    ) -> Result<(), Status> {
        let identity = context
            .connection_identity()
            .ok_or_else(|| Status::unauthenticated("verified connection identity is missing"))?;
        self.revalidate(identity).await
    }

    /// Resolves only the stable owner for request/response services.
    ///
    /// # Errors
    ///
    /// Returns the same status classifications as [`Self::resolve_identity`].
    async fn resolve_owner(&self, extensions: &Extensions) -> Result<String, Status> {
        self.resolve_context(extensions)
            .await
            .map(|context| context.owner_id().to_owned())
    }
}

/// mTLS transport adapter over an implementation-independent identity registry.
#[derive(Debug)]
pub struct MtlsConnectionIdentityResolver {
    registry: Arc<dyn IdentityRegistry>,
    persistence: ServerPersistence,
}

impl MtlsConnectionIdentityResolver {
    #[must_use]
    pub fn new(registry: Arc<dyn IdentityRegistry>) -> Self {
        Self {
            registry,
            persistence: ServerPersistence::default(),
        }
    }
}

#[tonic::async_trait]
impl ConnectionIdentityResolver for MtlsConnectionIdentityResolver {
    async fn resolve_identity(
        &self,
        extensions: &Extensions,
    ) -> Result<ResolvedConnectionIdentity, Status> {
        let fingerprint =
            peer_certificate_fingerprint(extensions).map_err(|error| identity_status(&error))?;
        let registry = Arc::clone(&self.registry);
        let owner_id = self
            .persistence
            .run(move || registry.resolve_fingerprint(fingerprint))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| identity_status(&error))?;
        Ok(ResolvedConnectionIdentity {
            owner_id,
            fingerprint,
        })
    }

    async fn revalidate(&self, identity: &ResolvedConnectionIdentity) -> Result<(), Status> {
        let registry = Arc::clone(&self.registry);
        let fingerprint = identity.fingerprint;
        let owner_id = self
            .persistence
            .run(move || registry.resolve_fingerprint(fingerprint))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
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

/// Hashes the verified client leaf certificate attached by tonic transport.
///
/// # Errors
///
/// Returns an error when the connection is not mutually authenticated or has no leaf certificate.
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
/// Returns an error when the PEM cannot be parsed or contains no certificate.
pub fn certificate_fingerprint_from_pem(pem: &[u8]) -> Result<Sha256Digest, IdentityError> {
    let mut reader = BufReader::new(pem);
    let certificate = rustls_pemfile::certs(&mut reader)
        .next()
        .transpose()
        .map_err(|error| IdentityError::Certificate(error.to_string()))?
        .ok_or_else(|| IdentityError::Certificate("no certificate was found".into()))?;
    Ok(Sha256Digest::digest_bytes(certificate.as_ref()))
}

pub(crate) fn validate_owner(owner_id: &str) -> Result<(), IdentityError> {
    if owner_id.trim().is_empty() {
        Err(IdentityError::Invalid("owner ID is missing"))
    } else {
        Ok(())
    }
}
