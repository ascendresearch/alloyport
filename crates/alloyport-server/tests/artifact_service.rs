use alloyport_artifacts::upload::SqliteUploadStore;
use alloyport_artifacts::{FilesystemArtifactStore, Sha256Digest};
use alloyport_proto::artifact_v1::artifact_service_client::ArtifactServiceClient;
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::artifact_v1::{
    BeginUploadRequest, DownloadRequest, FinalizeUploadRequest, GetUploadRequest, UploadChunk,
    UploadState,
};
use alloyport_server::ManualClock;
use alloyport_server::artifact::{ArtifactAccessPolicy, ArtifactServiceImpl};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::{Endpoint, Server};
use tonic::{Extensions, Request, Status};

#[tokio::test]
async fn upload_resumes_across_streams_then_downloads_in_chunks() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let uploads = Arc::new(SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("uploads"),
        1_024,
        8,
    )?);
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("cas"),
        1_024,
    )?);
    let service = ArtifactServiceImpl::new(
        uploads,
        artifacts,
        Arc::new(TestAccessPolicy),
        Arc::new(ManualClock::new(1_000)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        Server::builder()
            .add_service(ArtifactServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let mut client =
        ArtifactServiceClient::connect(Endpoint::from_shared(format!("http://{address}"))?).await?;
    let digest = "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    let session = client
        .begin_upload(authorized(BeginUploadRequest {
            upload_key: "attempt-1:stdout".to_owned(),
            expected_digest: digest.to_owned(),
            expected_size_bytes: 11,
            media_type: "text/plain".to_owned(),
            ttl_ms: 60_000,
        }))
        .await?
        .into_inner();

    let first = client
        .upload(authorized(tokio_stream::iter([UploadChunk {
            upload_id: session.upload_id.clone(),
            offset: 0,
            data: b"hello ".to_vec(),
        }])))
        .await?
        .into_inner();
    assert_eq!(first.committed_offset, 6);

    let resumed = client
        .upload(authorized(tokio_stream::iter([UploadChunk {
            upload_id: session.upload_id.clone(),
            offset: 6,
            data: b"world".to_vec(),
        }])))
        .await?
        .into_inner();
    assert_eq!(resumed.committed_offset, 11);
    let finalized = client
        .finalize_upload(authorized(FinalizeUploadRequest {
            upload_id: session.upload_id.clone(),
        }))
        .await?
        .into_inner();
    assert_eq!(finalized.digest, digest);

    let status = client
        .get_upload(authorized(GetUploadRequest {
            upload_id: session.upload_id,
        }))
        .await?
        .into_inner();
    assert_eq!(status.state, i32::from(UploadState::Completed));

    let mut download = client
        .download(authorized(DownloadRequest {
            digest: digest.to_owned(),
            offset: 6,
            max_bytes: 5,
        }))
        .await?
        .into_inner();
    let mut downloaded = Vec::new();
    while let Some(chunk) = download.next().await.transpose()? {
        assert_eq!(chunk.offset, 6 + u64::try_from(downloaded.len())?);
        downloaded.extend_from_slice(&chunk.data);
    }
    assert_eq!(downloaded, b"world");

    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-test-owner", MetadataValue::from_static("worker-1"));
    request
}

#[derive(Debug)]
struct TestAccessPolicy;

impl ArtifactAccessPolicy for TestAccessPolicy {
    fn resolve_owner(
        &self,
        metadata: &MetadataMap,
        _extensions: &Extensions,
    ) -> Result<String, Status> {
        metadata
            .get("x-test-owner")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| Status::unauthenticated("fixture owner metadata is missing"))
    }

    fn authorize_download(&self, owner_id: &str, digest: Sha256Digest) -> Result<(), Status> {
        let expected = Sha256Digest::from_str(
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
        )
        .expect("fixture digest is valid");
        if owner_id == "worker-1" && digest == expected {
            Ok(())
        } else {
            Err(Status::permission_denied("artifact is not authorized"))
        }
    }
}
