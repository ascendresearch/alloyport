//! Identity resolution and durable run-access authorization policy.

use super::interaction_status;
use crate::identity::{ConnectionIdentityResolver, ResolvedConnectionIdentity};
use crate::interaction::InteractionStore;
use crate::persistence::ServerPersistence;
use std::fmt::Debug;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Extensions, Status};

/// Authorization retained for the lifetime of a replay or subscription request.
#[derive(Clone, Debug)]
pub struct RunAuthorization {
    owner_id: String,
    identity: Option<ResolvedConnectionIdentity>,
}

impl RunAuthorization {
    #[must_use]
    pub fn local(owner_id: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            identity: None,
        }
    }

    #[must_use]
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }
}

/// Resolves request identity and checks run visibility without trusting request body ownership.
#[tonic::async_trait]
pub trait InteractionAccessPolicy: Debug + Send + Sync {
    /// Authorizes one request and returns state suitable for later revalidation.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when identity is absent, inactive, or lacks run access.
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        extensions: &Extensions,
        run_id: &str,
    ) -> Result<RunAuthorization, Status>;

    /// Revalidates a live stream against credential and grant revocation.
    ///
    /// # Errors
    ///
    /// Returns a gRPC status when access is no longer active.
    async fn revalidate(
        &self,
        authorization: &RunAuthorization,
        run_id: &str,
    ) -> Result<(), Status>;
}

/// Production policy backed by verified certificate enrollment and durable run grants.
#[derive(Clone, Debug)]
pub struct EnrolledInteractionAccessPolicy {
    interactions: Arc<dyn InteractionStore>,
    identities: Arc<dyn ConnectionIdentityResolver>,
}

impl EnrolledInteractionAccessPolicy {
    #[must_use]
    pub fn new(
        interactions: Arc<dyn InteractionStore>,
        identities: Arc<dyn ConnectionIdentityResolver>,
    ) -> Self {
        Self {
            interactions,
            identities,
        }
    }

    async fn authorize_owner(&self, owner_id: &str, run_id: &str) -> Result<(), Status> {
        let interactions = Arc::clone(&self.interactions);
        let owner_id = owner_id.to_owned();
        let run_id = run_id.to_owned();
        let can_read = ServerPersistence::default()
            .run(move || interactions.can_read_run(&run_id, &owner_id))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        match can_read {
            Ok(true) => Ok(()),
            Ok(false) => Err(Status::permission_denied(
                "run is not visible to this owner",
            )),
            Err(error) => Err(interaction_status(&error)),
        }
    }
}

#[tonic::async_trait]
impl InteractionAccessPolicy for EnrolledInteractionAccessPolicy {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        extensions: &Extensions,
        run_id: &str,
    ) -> Result<RunAuthorization, Status> {
        let identity = self.identities.resolve_identity(extensions).await?;
        self.authorize_owner(&identity.owner_id, run_id).await?;
        Ok(RunAuthorization {
            owner_id: identity.owner_id.clone(),
            identity: Some(identity),
        })
    }

    async fn revalidate(
        &self,
        authorization: &RunAuthorization,
        run_id: &str,
    ) -> Result<(), Status> {
        let identity = authorization
            .identity
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("verified interaction identity is missing"))?;
        self.identities.revalidate(identity).await?;
        self.authorize_owner(&authorization.owner_id, run_id).await
    }
}
