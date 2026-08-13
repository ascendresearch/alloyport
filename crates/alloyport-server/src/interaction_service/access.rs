//! Identity resolution and durable run-access authorization policy.

use crate::grpc_status::interaction_status;
use crate::identity::{AuthenticatedRequestContext, ConnectionIdentityResolver};
use crate::interaction::InteractionStore;
use crate::persistence::ServerPersistence;
use std::fmt::Debug;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Extensions, Status};

/// Authentication state retained for run authorization and stream revalidation.
pub type RunAuthorization = AuthenticatedRequestContext;

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

/// Loopback-only policy used when the server itself is configured without TLS.
#[derive(Clone, Debug)]
pub struct LocalInteractionAccessPolicy {
    interactions: Arc<dyn InteractionStore>,
    owner_id: String,
}

impl LocalInteractionAccessPolicy {
    #[must_use]
    pub fn new(interactions: Arc<dyn InteractionStore>, owner_id: impl Into<String>) -> Self {
        Self {
            interactions,
            owner_id: owner_id.into(),
        }
    }

    async fn authorize_owner(&self, run_id: &str) -> Result<(), Status> {
        authorize_owner(
            Arc::clone(&self.interactions),
            self.owner_id.clone(),
            run_id.to_owned(),
        )
        .await
    }
}

#[tonic::async_trait]
impl InteractionAccessPolicy for LocalInteractionAccessPolicy {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        _extensions: &Extensions,
        run_id: &str,
    ) -> Result<RunAuthorization, Status> {
        self.authorize_owner(run_id).await?;
        Ok(AuthenticatedRequestContext::local(self.owner_id.clone()))
    }

    async fn revalidate(
        &self,
        _authorization: &RunAuthorization,
        run_id: &str,
    ) -> Result<(), Status> {
        self.authorize_owner(run_id).await
    }
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
        authorize_owner(
            Arc::clone(&self.interactions),
            owner_id.to_owned(),
            run_id.to_owned(),
        )
        .await
    }
}

async fn authorize_owner(
    interactions: Arc<dyn InteractionStore>,
    owner_id: String,
    run_id: String,
) -> Result<(), Status> {
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

#[tonic::async_trait]
impl InteractionAccessPolicy for EnrolledInteractionAccessPolicy {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        extensions: &Extensions,
        run_id: &str,
    ) -> Result<RunAuthorization, Status> {
        let context = self.identities.resolve_context(extensions).await?;
        self.authorize_owner(context.owner_id(), run_id).await?;
        Ok(context)
    }

    async fn revalidate(
        &self,
        authorization: &RunAuthorization,
        run_id: &str,
    ) -> Result<(), Status> {
        self.identities.revalidate_context(authorization).await?;
        self.authorize_owner(authorization.owner_id(), run_id).await
    }
}
