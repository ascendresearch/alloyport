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
