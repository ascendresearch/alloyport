//! Single audited mapping from domain failure categories to gRPC transport codes.

use crate::identity::IdentityError;
use crate::interaction::InteractionError;
use crate::storage::RepositoryError;
use alloyport_artifacts::ArtifactStoreError;
use alloyport_artifacts::upload::UploadError;
use tonic::Status;

pub(crate) fn repository_status(error: RepositoryError) -> Status {
    match error {
        RepositoryError::NotFound(detail) => Status::not_found(detail),
        RepositoryError::IdentityMismatch(detail) => Status::permission_denied(detail),
        RepositoryError::InvalidIdentity(detail) => Status::invalid_argument(detail),
        RepositoryError::InvalidTransition { .. } => Status::failed_precondition(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

pub(crate) fn interaction_status(error: &InteractionError) -> Status {
    let detail = error.to_string();
    match error {
        InteractionError::InvalidFrame(_)
        | InteractionError::ConflictingDedupKey(_)
        | InteractionError::ConflictingOutput { .. }
        | InteractionError::InvalidCursor { .. }
        | InteractionError::RevokedRunGrant { .. }
        | InteractionError::MissingRunGrant { .. }
        | InteractionError::ValueOutOfRange(_) => Status::invalid_argument(detail),
        InteractionError::Storage(_)
        | InteractionError::Encoding(_)
        | InteractionError::InvalidSubscriptionCapacity
        | InteractionError::LockPoisoned => Status::internal(detail),
    }
}

pub(crate) fn upload_status(error: UploadError) -> Status {
    match error {
        UploadError::NotFound(_) => Status::not_found(error.to_string()),
        UploadError::OwnerMismatch => Status::permission_denied(error.to_string()),
        UploadError::OffsetConflict { .. } => Status::aborted(error.to_string()),
        UploadError::ChunkTooLarge { .. }
        | UploadError::SizeLimitExceeded { .. }
        | UploadError::QuotaExceeded { .. } => Status::resource_exhausted(error.to_string()),
        UploadError::InvalidRequest(_)
        | UploadError::ConflictingUploadKey
        | UploadError::ConflictingReferenceKey => Status::invalid_argument(error.to_string()),
        UploadError::ReferenceRevoked
        | UploadError::GarbageCollectionPending(_)
        | UploadError::Expired
        | UploadError::Incomplete { .. }
        | UploadError::InvalidState(_) => Status::failed_precondition(error.to_string()),
        UploadError::Artifact(error) => artifact_status(&error),
        UploadError::Storage(_) | UploadError::Io { .. } | UploadError::Corrupt(_) => {
            Status::internal(error.to_string())
        }
    }
}

pub(crate) fn identity_status(error: &IdentityError) -> Status {
    match error {
        IdentityError::NotEnrolled(_) | IdentityError::Certificate(_) => {
            Status::unauthenticated(error.to_string())
        }
        IdentityError::Revoked(_)
        | IdentityError::Replaced(_)
        | IdentityError::Conflict(_)
        | IdentityError::Invalid(_) => Status::permission_denied(error.to_string()),
        IdentityError::Storage(_) | IdentityError::Corrupt(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn artifact_status(error: &ArtifactStoreError) -> Status {
    match error {
        ArtifactStoreError::DigestMismatch { .. }
        | ArtifactStoreError::SizeMismatch { .. }
        | ArtifactStoreError::IntegrityViolation { .. } => Status::data_loss(error.to_string()),
        ArtifactStoreError::SizeLimitExceeded { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        ArtifactStoreError::Io { .. } => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::AttemptState;
    use alloyport_artifacts::Sha256Digest;
    use alloyport_artifacts::upload::{QuotaScope, UploadState};
    use std::io;
    use tonic::Code;

    #[test]
    fn repository_error_codes_are_stable() {
        assert_eq!(
            repository_status(RepositoryError::NotFound("a-1".into())).code(),
            Code::NotFound
        );
        assert_eq!(
            repository_status(RepositoryError::IdentityMismatch("a-1".into())).code(),
            Code::PermissionDenied
        );
        assert_eq!(
            repository_status(RepositoryError::InvalidIdentity("bad".into())).code(),
            Code::InvalidArgument
        );
        assert_eq!(
            repository_status(RepositoryError::InvalidTransition {
                from: AttemptState::Sent,
                to: AttemptState::Preparing,
            })
            .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            repository_status(RepositoryError::Corrupt("bad row".into())).code(),
            Code::Internal
        );
    }

    #[test]
    fn interaction_error_codes_are_stable() {
        for error in [
            InteractionError::InvalidFrame("bad".into()),
            InteractionError::ConflictingDedupKey("key".into()),
            InteractionError::InvalidCursor {
                run_id: "run-1".into(),
                after_sequence: 2,
                latest_sequence: 1,
            },
            InteractionError::RevokedRunGrant {
                run_id: "run-1".into(),
                owner_id: "owner-1".into(),
            },
        ] {
            assert_eq!(interaction_status(&error).code(), Code::InvalidArgument);
        }
        assert_eq!(
            interaction_status(&InteractionError::LockPoisoned).code(),
            Code::Internal
        );
    }

    #[test]
    fn upload_and_artifact_error_codes_are_stable() {
        let digest = Sha256Digest::digest_bytes(b"artifact");
        let cases = [
            (UploadError::NotFound("upload-1".into()), Code::NotFound),
            (UploadError::OwnerMismatch, Code::PermissionDenied),
            (
                UploadError::OffsetConflict {
                    expected: 1,
                    received: 2,
                },
                Code::Aborted,
            ),
            (
                UploadError::QuotaExceeded {
                    scope: QuotaScope::Owner,
                    limit: 1,
                    used: 1,
                    requested: 1,
                },
                Code::ResourceExhausted,
            ),
            (
                UploadError::InvalidRequest("bad request"),
                Code::InvalidArgument,
            ),
            (
                UploadError::InvalidState(UploadState::Failed),
                Code::FailedPrecondition,
            ),
            (
                UploadError::Artifact(ArtifactStoreError::DigestMismatch {
                    expected: digest,
                    actual: Sha256Digest::digest_bytes(b"other"),
                }),
                Code::DataLoss,
            ),
            (
                UploadError::Io {
                    operation: "fixture",
                    source: io::Error::other("fixture"),
                },
                Code::Internal,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(upload_status(error).code(), expected);
        }
    }

    #[test]
    fn identity_error_codes_are_stable() {
        let digest = Sha256Digest::digest_bytes(b"certificate");
        assert_eq!(
            identity_status(&IdentityError::NotEnrolled(digest)).code(),
            Code::Unauthenticated
        );
        assert_eq!(
            identity_status(&IdentityError::Revoked(digest)).code(),
            Code::PermissionDenied
        );
        assert_eq!(
            identity_status(&IdentityError::Corrupt("bad row".into())).code(),
            Code::Internal
        );
    }
}
