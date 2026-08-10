use alloyport_artifacts::FilesystemArtifactStore;
use alloyport_artifacts::upload::SqliteUploadStore;
use alloyport_proto::artifact_v1::artifact_service_client::ArtifactServiceClient;
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::artifact_v1::{
    BeginUploadRequest, DownloadRequest, FinalizeUploadRequest, GetUploadRequest, UploadChunk,
};
use alloyport_server::ManualClock;
use alloyport_server::artifact::{ArtifactServiceImpl, MtlsArtifactAccessPolicy};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};
use tonic::{Code, Request};

#[tokio::test]
async fn client_certificate_owns_sessions_and_completed_artifacts() -> Result<(), Box<dyn Error>> {
    let pki = test_pki()?;
    let directory = tempfile::tempdir()?;
    let uploads = Arc::new(SqliteUploadStore::open(
        directory.path().join("uploads.sqlite3"),
        directory.path().join("uploads"),
        1_024,
        64,
    )?);
    let artifacts = Arc::new(FilesystemArtifactStore::open(
        directory.path().join("cas"),
        1_024,
    )?);
    let service = ArtifactServiceImpl::new(
        Arc::clone(&uploads),
        artifacts,
        Arc::new(MtlsArtifactAccessPolicy::new(uploads)),
        Arc::new(ManualClock::new(1_000)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_tls = ServerTlsConfig::new()
        .identity(pki.server.tonic_identity())
        .client_ca_root(Certificate::from_pem(pki.ca_certificate.clone()));
    let server_task = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)?
            .add_service(ArtifactServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let mut client_a = artifact_client(address, &pki.ca_certificate, &pki.client_a).await?;
    let mut client_b = artifact_client(address, &pki.ca_certificate, &pki.client_b).await?;
    let digest = "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    let session = client_a
        .begin_upload(forged_owner(BeginUploadRequest {
            upload_key: "attempt-1:stdout".to_owned(),
            expected_digest: digest.to_owned(),
            expected_size_bytes: 11,
            media_type: "text/plain".to_owned(),
            ttl_ms: 60_000,
        }))
        .await?
        .into_inner();
    client_a
        .upload(forged_owner(tokio_stream::iter([UploadChunk {
            upload_id: session.upload_id.clone(),
            offset: 0,
            data: b"hello world".to_vec(),
        }])))
        .await?;
    client_a
        .finalize_upload(forged_owner(FinalizeUploadRequest {
            upload_id: session.upload_id.clone(),
        }))
        .await?;

    let session_error = client_b
        .get_upload(forged_owner(GetUploadRequest {
            upload_id: session.upload_id,
        }))
        .await
        .expect_err("a second certificate cannot claim the first certificate's session");
    assert_eq!(session_error.code(), Code::PermissionDenied);
    let download_error = client_b
        .download(forged_owner(DownloadRequest {
            digest: digest.to_owned(),
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .expect_err("a second certificate cannot read the first certificate's artifact");
    assert_eq!(download_error.code(), Code::PermissionDenied);

    let mut download = client_a
        .download(forged_owner(DownloadRequest {
            digest: digest.to_owned(),
            offset: 0,
            max_bytes: 0,
        }))
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = download.next().await.transpose()? {
        bytes.extend_from_slice(&chunk.data);
    }
    assert_eq!(bytes, b"hello world");

    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

async fn artifact_client(
    address: std::net::SocketAddr,
    ca_certificate: &str,
    identity: &PemIdentity,
) -> Result<ArtifactServiceClient<Channel>, Box<dyn Error>> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_certificate))
        .identity(identity.tonic_identity())
        .domain_name("localhost");
    let channel = Endpoint::from_shared(format!("https://{address}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(ArtifactServiceClient::new(channel))
}

fn forged_owner<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-artifact-owner",
        "forged-client-owner".parse().expect("static metadata"),
    );
    request
}

struct TestPki {
    ca_certificate: String,
    server: PemIdentity,
    client_a: PemIdentity,
    client_b: PemIdentity,
}

struct PemIdentity {
    certificate: String,
    private_key: String,
}

impl PemIdentity {
    fn tonic_identity(&self) -> Identity {
        Identity::from_pem(self.certificate.clone(), self.private_key.clone())
    }
}

fn test_pki() -> Result<TestPki, rcgen::Error> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "AlloyPort test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = ca_params.self_signed(&ca_key)?;
    Ok(TestPki {
        ca_certificate: ca.pem(),
        server: signed_identity(
            "localhost",
            vec!["localhost".to_owned()],
            ExtendedKeyUsagePurpose::ServerAuth,
            &ca,
            &ca_key,
        )?,
        client_a: signed_identity(
            "worker-a",
            Vec::new(),
            ExtendedKeyUsagePurpose::ClientAuth,
            &ca,
            &ca_key,
        )?,
        client_b: signed_identity(
            "worker-b",
            Vec::new(),
            ExtendedKeyUsagePurpose::ClientAuth,
            &ca,
            &ca_key,
        )?,
    })
}

fn signed_identity(
    common_name: &str,
    subject_alt_names: Vec<String>,
    purpose: ExtendedKeyUsagePurpose,
    ca: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Result<PemIdentity, rcgen::Error> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(subject_alt_names)?;
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![purpose];
    let certificate = params.signed_by(&key, ca, ca_key)?;
    Ok(PemIdentity {
        certificate: certificate.pem(),
        private_key: key.serialize_pem(),
    })
}
