use alloyport_artifacts::upload::{
    ArtifactReferenceKind, GrantArtifactReference, SqliteUploadStore,
};
use alloyport_artifacts::{FilesystemArtifactStore, Sha256Digest};
use alloyport_proto::artifact_v1::artifact_service_client::ArtifactServiceClient;
use alloyport_proto::artifact_v1::artifact_service_server::ArtifactServiceServer;
use alloyport_proto::artifact_v1::{
    BeginUploadRequest, DownloadRequest, FinalizeUploadRequest, GetUploadRequest, UploadChunk,
};
use alloyport_proto::v1::worker_control_client::WorkerControlClient;
use alloyport_proto::v1::worker_control_server::WorkerControlServer;
use alloyport_proto::v1::{
    Backend, Heartbeat, ServerToWorker, WorkerCapabilities, WorkerHealth, WorkerHello,
    WorkerToServer, worker_to_server,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use alloyport_server::artifact::{ArtifactServiceImpl, EnrolledArtifactAccessPolicy};
use alloyport_server::identity::{
    ConnectionIdentityResolver, SqliteIdentityRegistry, certificate_fingerprint_from_pem,
};
use alloyport_server::{ManualClock, WorkerControlService};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};
use tonic::{Code, Request, Streaming};

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
    let identities = Arc::new(SqliteIdentityRegistry::open(
        directory.path().join("identities.sqlite3"),
    )?);
    let fingerprint_a = certificate_fingerprint_from_pem(pki.client_a.certificate.as_bytes())?;
    let fingerprint_b = certificate_fingerprint_from_pem(pki.client_b.certificate.as_bytes())?;
    let fingerprint_c = certificate_fingerprint_from_pem(pki.client_c.certificate.as_bytes())?;
    identities.enroll("worker-a", fingerprint_a, 1)?;
    identities.enroll("worker-b", fingerprint_b, 1)?;
    let artifact_resolver: Arc<dyn ConnectionIdentityResolver> = identities.clone();
    let artifact_service = ArtifactServiceImpl::new(
        Arc::clone(&uploads),
        artifacts,
        Arc::new(EnrolledArtifactAccessPolicy::new(
            Arc::clone(&uploads),
            artifact_resolver,
        )),
        Arc::new(ManualClock::new(1_000)),
    );
    let control_resolver: Arc<dyn ConnectionIdentityResolver> = identities.clone();
    let control_service = WorkerControlService::new().require_identity_resolver(control_resolver);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_send, shutdown_receive) = oneshot::channel();
    let server_tls = ServerTlsConfig::new()
        .identity(pki.server.tonic_identity())
        .client_ca_root(Certificate::from_pem(pki.ca_certificate.clone()));
    let server_task = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)?
            .add_service(ArtifactServiceServer::new(artifact_service))
            .add_service(WorkerControlServer::new(control_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receive.await;
            })
            .await
    });

    let mut client_a = artifact_client(address, &pki.ca_certificate, &pki.client_a).await?;
    let mut client_b = artifact_client(address, &pki.ca_certificate, &pki.client_b).await?;
    let forged_worker_channel = tls_channel(address, &pki.ca_certificate, &pki.client_a).await?;
    assert_forged_worker_hello_denied(forged_worker_channel).await?;
    let active_worker_channel = tls_channel(address, &pki.ca_certificate, &pki.client_a).await?;
    let mut active_worker = open_worker_stream(active_worker_channel, "worker-a").await?;
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
    assert_download_denied(&mut client_b, digest).await?;
    assert_eq!(download_bytes(&mut client_a, digest).await?, b"hello world");

    assert_controller_grant_and_revoke(&uploads, &mut client_b, digest).await?;

    identities.rotate("worker-a", fingerprint_a, fingerprint_c, 2)?;
    assert_rotated_worker_stream_closes(&mut active_worker).await?;
    assert_download_denied(&mut client_a, digest).await?;
    let mut client_c = artifact_client(address, &pki.ca_certificate, &pki.client_c).await?;
    assert_eq!(download_bytes(&mut client_c, digest).await?, b"hello world");
    identities.revoke(fingerprint_c, 3)?;
    assert_download_denied(&mut client_c, digest).await?;

    let _ = shutdown_send.send(());
    server_task.await??;
    Ok(())
}

async fn assert_controller_grant_and_revoke(
    uploads: &SqliteUploadStore,
    client: &mut ArtifactServiceClient<Channel>,
    digest: &str,
) -> Result<(), Box<dyn Error>> {
    let assignment_reference = GrantArtifactReference {
        owner_id: "worker-b".into(),
        reference_key: "assignment:attempt-2:input".into(),
        digest: Sha256Digest::from_str(digest)?,
        kind: ArtifactReferenceKind::AssignmentInput,
        purpose: "controller-granted assignment input".into(),
        now_ms: 1_001,
        retained_until_ms: None,
    };
    uploads.grant_reference(&assignment_reference)?;
    assert_eq!(download_bytes(client, digest).await?, b"hello world");
    uploads.revoke_reference(
        &assignment_reference.owner_id,
        &assignment_reference.reference_key,
        1_002,
    )?;
    assert_download_denied(client, digest).await
}

async fn artifact_client(
    address: std::net::SocketAddr,
    ca_certificate: &str,
    identity: &PemIdentity,
) -> Result<ArtifactServiceClient<Channel>, Box<dyn Error>> {
    Ok(ArtifactServiceClient::new(
        tls_channel(address, ca_certificate, identity).await?,
    ))
}

async fn tls_channel(
    address: std::net::SocketAddr,
    ca_certificate: &str,
    identity: &PemIdentity,
) -> Result<Channel, Box<dyn Error>> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_certificate))
        .identity(identity.tonic_identity())
        .domain_name("localhost");
    Ok(Endpoint::from_shared(format!("https://{address}"))?
        .tls_config(tls)?
        .connect()
        .await?)
}

async fn download_bytes(
    client: &mut ArtifactServiceClient<Channel>,
    digest: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut download = client
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
    Ok(bytes)
}

async fn assert_download_denied(
    client: &mut ArtifactServiceClient<Channel>,
    digest: &str,
) -> Result<(), Box<dyn Error>> {
    let error = client
        .download(forged_owner(DownloadRequest {
            digest: digest.to_owned(),
            offset: 0,
            max_bytes: 0,
        }))
        .await
        .expect_err("an inactive or different identity cannot read the artifact");
    assert_eq!(error.code(), Code::PermissionDenied);
    Ok(())
}

async fn assert_forged_worker_hello_denied(channel: Channel) -> Result<(), Box<dyn Error>> {
    let mut client = WorkerControlClient::new(channel);
    let frame = WorkerToServer {
        sequence: 1,
        acknowledges_server_through: 0,
        message_id: String::new(),
        message: Some(worker_to_server::Message::Hello(worker_hello("worker-b"))),
    };
    let error = client
        .open_control_stream(Request::new(tokio_stream::iter([frame])))
        .await
        .expect_err("certificate owner cannot forge another worker ID");
    assert_eq!(error.code(), Code::PermissionDenied);
    Ok(())
}

struct ActiveWorkerStream {
    outbound: mpsc::Sender<WorkerToServer>,
    inbound: Streaming<ServerToWorker>,
}

async fn open_worker_stream(
    channel: Channel,
    worker_id: &str,
) -> Result<ActiveWorkerStream, Box<dyn Error>> {
    let mut client = WorkerControlClient::new(channel);
    let (outbound, receiver) = mpsc::channel(4);
    outbound
        .send(WorkerToServer {
            sequence: 1,
            acknowledges_server_through: 0,
            message_id: String::new(),
            message: Some(worker_to_server::Message::Hello(worker_hello(worker_id))),
        })
        .await?;
    let mut inbound = client
        .open_control_stream(Request::new(ReceiverStream::new(receiver)))
        .await?
        .into_inner();
    inbound
        .message()
        .await?
        .ok_or("worker stream ended before welcome")?;
    Ok(ActiveWorkerStream { outbound, inbound })
}

async fn assert_rotated_worker_stream_closes(
    stream: &mut ActiveWorkerStream,
) -> Result<(), Box<dyn Error>> {
    stream
        .outbound
        .send(WorkerToServer {
            sequence: 2,
            acknowledges_server_through: 0,
            message_id: String::new(),
            message: Some(worker_to_server::Message::Heartbeat(Heartbeat {
                active_attempts: Vec::new(),
                available_slots: 1,
                health: WorkerHealth::Ready.into(),
            })),
        })
        .await?;
    let error = stream
        .inbound
        .message()
        .await
        .expect_err("a replaced certificate must terminate an existing worker stream");
    assert_eq!(error.code(), Code::PermissionDenied);
    Ok(())
}

fn worker_hello(worker_id: &str) -> WorkerHello {
    WorkerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        worker_id: worker_id.to_owned(),
        instance_id: format!("{worker_id}-process"),
        worker_version: "test".to_owned(),
        features: Vec::new(),
        capabilities: Some(WorkerCapabilities {
            backend: Backend::Cuda.into(),
            architecture: "sm_80".to_owned(),
            device_count: 1,
            max_concurrency: 1,
            driver_version: "test".to_owned(),
            toolkit_version: "test".to_owned(),
            container_runtime: "test".to_owned(),
        }),
        active_attempts: Vec::new(),
    }
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
    client_c: PemIdentity,
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
        client_c: signed_identity(
            "worker-a-rotated",
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
