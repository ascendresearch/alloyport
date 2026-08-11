//! Contract tests for the remote publisher's application-facing error taxonomy.

use super::*;

#[test]
fn adapter_failures_map_to_stable_publication_categories() {
    assert!(matches!(
        ArtifactPublicationError::from(RemoteArtifactUploadError::LocalRead(
            std::io::Error::other("disk")
        )),
        ArtifactPublicationError::LocalArtifact(_)
    ));
    assert!(matches!(
        ArtifactPublicationError::from(RemoteArtifactUploadError::Rpc(tonic::Status::unavailable(
            "retry"
        ))),
        ArtifactPublicationError::Unavailable(_)
    ));
    assert!(matches!(
        ArtifactPublicationError::from(RemoteArtifactUploadError::Rpc(
            tonic::Status::permission_denied("forbidden")
        )),
        ArtifactPublicationError::Rejected(_)
    ));
    assert!(matches!(
        ArtifactPublicationError::from(RemoteArtifactUploadError::Protocol("mismatch".into())),
        ArtifactPublicationError::Rejected(_)
    ));
    assert!(matches!(
        ArtifactPublicationError::from(RemoteArtifactUploadError::InvalidConfiguration("bad")),
        ArtifactPublicationError::Internal(_)
    ));
}
