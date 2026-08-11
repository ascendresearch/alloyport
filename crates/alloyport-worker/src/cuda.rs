//! Policy-bound contract for the first fixed CUDA container vertical slice.

use crate::journal::{StoredAssignment, StoredLimits};
use alloyport_artifacts::{ArtifactStore, FilesystemArtifactStore, Sha256Digest};
use alloyport_proto::v1::{ExecutorKind, NetworkPolicy};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub const CUDA_FIXTURE_FEATURE: &str = "cuda-fixture-v1";
pub const CUDA_FIXTURE_BUNDLE_MEDIA_TYPE: &str = "application/vnd.alloyport.cuda-fixture.v1+json";
pub const OCI_IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
pub const VECTOR_ADD_FIXTURE_ID: &str = "cuda-vectoradd-v1";

const SOURCE_FILENAME: &str = "vector_add.cu";
const RUNNER_FILENAME: &str = "run_fixture.py";
const CONTAINER_BUNDLE_PATH: &str = "/alloyport/bundle";
const CONTAINER_WORK_PATH: &str = "/alloyport/work";
const MINIMUM_DISK_BYTES: u64 = 128 * 1024 * 1024;
const CUDA_TEMP_BYTES: u64 = 64 * 1024 * 1024;

const RUNNER: &str = include_str!("../../../fixtures/cuda-vectoradd-v1/run_fixture.py");

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CudaFixtureBundle {
    pub schema_version: u16,
    pub fixture_id: String,
    pub source_sha256: String,
    pub source: String,
}

impl CudaFixtureBundle {
    #[must_use]
    pub fn vector_add(source: impl Into<String>) -> Self {
        let source = source.into();
        Self {
            schema_version: 1,
            fixture_id: VECTOR_ADD_FIXTURE_ID.into(),
            source_sha256: Sha256Digest::digest_bytes(source.as_bytes()).to_string(),
            source,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CudaResourceCeilings {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaFixturePolicy {
    fixture_id: String,
    bundle_digest: Sha256Digest,
    image_manifest_digest: Sha256Digest,
    image_reference: String,
    image_id: Sha256Digest,
    device_id: String,
    sandbox_root: PathBuf,
    ceilings: CudaResourceCeilings,
}

impl CudaFixturePolicy {
    /// Creates a local allowlist for exactly one fixture, bundle, image, and CUDA device.
    ///
    /// # Errors
    ///
    /// Returns an error for an unpinned image, unsafe local path, empty identity, or zero ceiling.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fixture_id: impl Into<String>,
        bundle_digest: Sha256Digest,
        image_manifest_digest: Sha256Digest,
        image_reference: impl Into<String>,
        image_id: Sha256Digest,
        device_id: impl Into<String>,
        sandbox_root: impl Into<PathBuf>,
        ceilings: CudaResourceCeilings,
    ) -> Result<Self, CudaContractError> {
        let fixture_id = fixture_id.into();
        let image_reference = image_reference.into();
        let device_id = device_id.into();
        let sandbox_root = sandbox_root.into();
        if fixture_id.trim().is_empty() {
            return Err(CudaContractError::InvalidPolicy("fixture ID is empty"));
        }
        if !image_reference.ends_with(&format!("@{image_manifest_digest}")) {
            return Err(CudaContractError::InvalidPolicy(
                "image reference is not pinned to the allowed manifest digest",
            ));
        }
        if device_id.trim().is_empty() || device_id.contains(',') {
            return Err(CudaContractError::InvalidPolicy(
                "CUDA device identity is empty or contains a separator",
            ));
        }
        if !sandbox_root.is_absolute() || sandbox_root.to_string_lossy().contains(',') {
            return Err(CudaContractError::InvalidPolicy(
                "sandbox root must be an absolute path without commas",
            ));
        }
        if ceilings.cpu_millis == 0
            || ceilings.memory_bytes == 0
            || ceilings.disk_bytes == 0
            || ceilings.process_count == 0
            || ceilings.output_bytes == 0
        {
            return Err(CudaContractError::InvalidPolicy(
                "resource ceilings must all be nonzero",
            ));
        }
        Ok(Self {
            fixture_id,
            bundle_digest,
            image_manifest_digest,
            image_reference,
            image_id,
            device_id,
            sandbox_root,
            ceilings,
        })
    }

    /// Validates an admitted storage-domain assignment against the complete local allowlist.
    ///
    /// # Errors
    ///
    /// Returns an error if any server-selected execution field exceeds or differs from policy.
    pub fn validate_assignment(
        &self,
        assignment: &StoredAssignment,
    ) -> Result<(), CudaContractError> {
        let execution = &assignment.execution;
        if ExecutorKind::try_from(execution.executor_kind).unwrap_or(ExecutorKind::Unspecified)
            != ExecutorKind::CudaFixture
        {
            return Err(CudaContractError::Assignment(
                "executor kind is not CUDA fixture",
            ));
        }
        if assignment.required_features != [CUDA_FIXTURE_FEATURE] {
            return Err(CudaContractError::Assignment(
                "required features must contain only cuda-fixture-v1",
            ));
        }
        if execution.argv != [self.fixture_id.as_str()] {
            return Err(CudaContractError::Assignment(
                "argv does not name the allowed fixture",
            ));
        }
        if execution.working_directory != "." {
            return Err(CudaContractError::Assignment(
                "working directory must be the fixture root",
            ));
        }
        if !execution.environment.is_empty() {
            return Err(CudaContractError::Assignment(
                "CUDA fixture environment must be empty",
            ));
        }
        validate_artifact(
            &execution.bundle,
            self.bundle_digest,
            CUDA_FIXTURE_BUNDLE_MEDIA_TYPE,
            "bundle",
        )?;
        validate_artifact(
            &execution.image,
            self.image_manifest_digest,
            OCI_IMAGE_MANIFEST_MEDIA_TYPE,
            "image",
        )?;
        validate_limits(execution.limits.as_ref(), self.ceilings)?;
        Ok(())
    }

    /// Materializes the already verified bundle into an attempt-owned immutable source directory.
    ///
    /// Existing identical files make recovery idempotent; changed bytes are an integrity failure.
    ///
    /// # Errors
    ///
    /// Returns an error for policy mismatch, missing/tampered Artifacts, malformed bundles, unsafe
    /// attempt identities, or local filesystem failure.
    pub fn materialize_bundle(
        &self,
        assignment: &StoredAssignment,
        artifacts: &FilesystemArtifactStore,
    ) -> Result<CudaSandbox, CudaContractError> {
        self.validate_assignment(assignment)?;
        validate_attempt_id(&assignment.attempt_id)?;
        let digest = Sha256Digest::from_str(&assignment.execution.bundle.digest)
            .map_err(|error| CudaContractError::Digest(error.to_string()))?;
        let mut reader = artifacts
            .open(digest)
            .map_err(|error| CudaContractError::Artifact(error.to_string()))?;
        let declared = assignment.execution.bundle.size_bytes;
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(declared.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != declared {
            return Err(CudaContractError::Bundle(
                "bundle bytes do not match the assignment size".into(),
            ));
        }
        let bundle: CudaFixtureBundle = serde_json::from_slice(&bytes)?;
        if bundle.schema_version != 1 || bundle.fixture_id != self.fixture_id {
            return Err(CudaContractError::Bundle(
                "bundle schema or fixture identity is not allowed".into(),
            ));
        }
        let source_digest = Sha256Digest::digest_bytes(bundle.source.as_bytes()).to_string();
        if source_digest != bundle.source_sha256 {
            return Err(CudaContractError::Bundle(
                "source digest does not match the bundle manifest".into(),
            ));
        }
        let directory = self.sandbox_root.join(&assignment.attempt_id);
        fs::create_dir_all(&directory)?;
        write_once(&directory.join(SOURCE_FILENAME), bundle.source.as_bytes())?;
        write_once(&directory.join(RUNNER_FILENAME), RUNNER.as_bytes())?;
        Ok(CudaSandbox { directory })
    }

    /// Produces the exact Docker CLI argv for a new durable attempt container.
    ///
    /// No assignment field can add a host path, mount, device, environment variable, shell, or
    /// container option. The returned plan is data and does not start a process.
    ///
    /// # Errors
    ///
    /// Returns an error if the assignment no longer matches this policy.
    pub fn docker_create_plan(
        &self,
        assignment: &StoredAssignment,
        sandbox: &CudaSandbox,
    ) -> Result<DockerCreatePlan, CudaContractError> {
        self.validate_assignment(assignment)?;
        let limits = assignment
            .execution
            .limits
            .as_ref()
            .ok_or(CudaContractError::Assignment(
                "resource limits are required",
            ))?;
        let container_name = format!("alloyport-{}", assignment.attempt_id);
        let cpu_quota = limits.cpu_millis.saturating_mul(100);
        let work_bytes = limits.disk_bytes.saturating_sub(CUDA_TEMP_BYTES);
        let mount = format!(
            "type=bind,src={},dst={CONTAINER_BUNDLE_PATH},readonly",
            sandbox.directory.display()
        );
        Ok(DockerCreatePlan {
            container_name: container_name.clone(),
            expected_image_id: self.image_id,
            argv: vec![
                "create".into(),
                "--name".into(),
                container_name,
                "--label".into(),
                format!("alloyport.attempt={}", assignment.attempt_id),
                "--label".into(),
                format!("alloyport.bundle={}", assignment.execution.bundle.digest),
                "--label".into(),
                format!("alloyport.image={}", assignment.execution.image.digest),
                "--network".into(),
                "none".into(),
                "--read-only".into(),
                "--cap-drop".into(),
                "ALL".into(),
                "--security-opt".into(),
                "no-new-privileges".into(),
                "--cpu-period".into(),
                "100000".into(),
                "--cpu-quota".into(),
                cpu_quota.to_string(),
                "--memory".into(),
                limits.memory_bytes.to_string(),
                "--pids-limit".into(),
                limits.process_count.to_string(),
                "--gpus".into(),
                format!("device={}", self.device_id),
                "--mount".into(),
                mount,
                "--tmpfs".into(),
                format!("{CONTAINER_WORK_PATH}:rw,exec,size={work_bytes}"),
                "--tmpfs".into(),
                format!("/tmp:rw,exec,size={CUDA_TEMP_BYTES}"),
                "--workdir".into(),
                CONTAINER_WORK_PATH.into(),
                self.image_reference.clone(),
                "python3".into(),
                format!("{CONTAINER_BUNDLE_PATH}/{RUNNER_FILENAME}"),
            ],
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaSandbox {
    directory: PathBuf,
}

impl CudaSandbox {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerCreatePlan {
    pub container_name: String,
    pub expected_image_id: Sha256Digest,
    pub argv: Vec<String>,
}

#[derive(Debug)]
pub enum CudaContractError {
    InvalidPolicy(&'static str),
    Assignment(&'static str),
    Digest(String),
    Artifact(String),
    Bundle(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl Display for CudaContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(detail) => write!(formatter, "invalid CUDA policy: {detail}"),
            Self::Assignment(detail) => write!(formatter, "CUDA assignment rejected: {detail}"),
            Self::Digest(detail) => write!(formatter, "invalid CUDA digest: {detail}"),
            Self::Artifact(detail) => write!(formatter, "CUDA input Artifact error: {detail}"),
            Self::Bundle(detail) => write!(formatter, "invalid CUDA fixture bundle: {detail}"),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Json(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for CudaContractError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidPolicy(_)
            | Self::Assignment(_)
            | Self::Digest(_)
            | Self::Artifact(_)
            | Self::Bundle(_) => None,
        }
    }
}

impl From<std::io::Error> for CudaContractError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for CudaContractError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

fn validate_artifact(
    artifact: &crate::journal::StoredArtifact,
    expected_digest: Sha256Digest,
    expected_media_type: &str,
    role: &'static str,
) -> Result<(), CudaContractError> {
    if artifact.digest != expected_digest.to_string() || artifact.media_type != expected_media_type
    {
        return Err(CudaContractError::Assignment(match role {
            "bundle" => "bundle identity is not locally allowed",
            _ => "image identity is not locally allowed",
        }));
    }
    Ok(())
}

fn validate_limits(
    limits: Option<&StoredLimits>,
    ceilings: CudaResourceCeilings,
) -> Result<(), CudaContractError> {
    let limits = limits.ok_or(CudaContractError::Assignment(
        "resource limits are required",
    ))?;
    if limits.cpu_millis == 0
        || limits.cpu_millis > ceilings.cpu_millis
        || limits.memory_bytes == 0
        || limits.memory_bytes > ceilings.memory_bytes
        || limits.disk_bytes < MINIMUM_DISK_BYTES
        || limits.disk_bytes > ceilings.disk_bytes
        || limits.process_count == 0
        || limits.process_count > ceilings.process_count
        || limits.output_bytes == 0
        || limits.output_bytes > ceilings.output_bytes
        || limits.device_count != 1
        || NetworkPolicy::try_from(limits.network).unwrap_or(NetworkPolicy::Unspecified)
            != NetworkPolicy::Disabled
    {
        return Err(CudaContractError::Assignment(
            "resource limits exceed policy or permit an unsafe mode",
        ));
    }
    Ok(())
}

fn validate_attempt_id(attempt_id: &str) -> Result<(), CudaContractError> {
    if attempt_id.is_empty()
        || attempt_id.len() > 64
        || !attempt_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CudaContractError::Assignment(
            "attempt ID is unsafe for local process identity",
        ));
    }
    Ok(())
}

fn write_once(path: &Path, bytes: &[u8]) -> Result<(), CudaContractError> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(path)? == bytes {
                Ok(())
            } else {
                Err(CudaContractError::Bundle(format!(
                    "existing sandbox file {} has conflicting bytes",
                    path.display()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{StoredArtifact, StoredEnvironment, StoredExecution};
    use alloyport_artifacts::{IngestRequest, Sha256Digest};
    use std::io::Cursor;

    #[test]
    fn fixed_contract_materializes_idempotently_and_never_builds_a_shell_command()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let artifacts = FilesystemArtifactStore::open(directory.path().join("cas"), 64 * 1024)?;
        let bundle = CudaFixtureBundle::vector_add("__global__ void vector_add() {}\n");
        let bundle_bytes = serde_json::to_vec(&bundle)?;
        let stored =
            artifacts.ingest(&mut Cursor::new(&bundle_bytes), IngestRequest::unverified())?;
        let image_manifest = Sha256Digest::digest_bytes(b"image manifest");
        let image_id = Sha256Digest::digest_bytes(b"local image filesystem");
        let policy = policy(
            directory.path().join("sandboxes"),
            stored.artifact.digest,
            image_manifest,
            image_id,
        )?;
        let assignment = assignment(
            stored.artifact.digest,
            stored.artifact.size_bytes,
            image_manifest,
        );

        policy.validate_assignment(&assignment)?;
        let sandbox = policy.materialize_bundle(&assignment, &artifacts)?;
        assert_eq!(
            fs::read_to_string(sandbox.directory().join(SOURCE_FILENAME))?,
            bundle.source
        );
        assert_eq!(
            policy.materialize_bundle(&assignment, &artifacts)?,
            sandbox,
            "restart materialization must preserve identical bytes"
        );
        let plan = policy.docker_create_plan(&assignment, &sandbox)?;
        assert_eq!(plan.container_name, "alloyport-attempt-1");
        assert_eq!(plan.expected_image_id, image_id);
        assert_eq!(plan.argv.first().map(String::as_str), Some("create"));
        assert!(!plan.argv.iter().any(|part| part == "sh" || part == "-c"));
        assert!(
            plan.argv
                .windows(2)
                .any(|pair| pair == ["--network", "none"])
        );
        assert!(
            plan.argv
                .windows(2)
                .any(|pair| pair == ["--gpus", "device=0"])
        );
        let tmpfs = plan
            .argv
            .windows(2)
            .filter(|pair| pair[0] == "--tmpfs")
            .map(|pair| pair[1].as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            tmpfs,
            [
                "/alloyport/work:rw,exec,size=201326592",
                "/tmp:rw,exec,size=67108864",
            ]
        );

        fs::write(sandbox.directory().join(SOURCE_FILENAME), b"changed\n")?;
        assert!(matches!(
            policy.materialize_bundle(&assignment, &artifacts),
            Err(CudaContractError::Bundle(detail)) if detail.contains("conflicting bytes")
        ));

        let mut changed = assignment.clone();
        changed.execution.environment.push(StoredEnvironment {
            name: "LD_PRELOAD".into(),
            value: "/host/inject.so".into(),
        });
        assert!(matches!(
            policy.validate_assignment(&changed),
            Err(CudaContractError::Assignment(_))
        ));
        Ok(())
    }

    #[test]
    fn bundle_rejects_a_source_digest_mismatch() -> Result<(), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let artifacts = FilesystemArtifactStore::open(directory.path().join("cas"), 64 * 1024)?;
        let mut bundle = CudaFixtureBundle::vector_add("source\n");
        bundle.source_sha256 = Sha256Digest::digest_bytes(b"other").to_string();
        let bytes = serde_json::to_vec(&bundle)?;
        let stored = artifacts.ingest(&mut Cursor::new(bytes), IngestRequest::unverified())?;
        let image_manifest = Sha256Digest::digest_bytes(b"image manifest");
        let policy = policy(
            directory.path().join("sandboxes"),
            stored.artifact.digest,
            image_manifest,
            Sha256Digest::digest_bytes(b"image id"),
        )?;
        let assignment = assignment(
            stored.artifact.digest,
            stored.artifact.size_bytes,
            image_manifest,
        );
        assert!(matches!(
            policy.materialize_bundle(&assignment, &artifacts),
            Err(CudaContractError::Bundle(detail)) if detail.contains("source digest")
        ));
        Ok(())
    }

    fn policy(
        root: PathBuf,
        bundle: Sha256Digest,
        image_manifest: Sha256Digest,
        image_id: Sha256Digest,
    ) -> Result<CudaFixturePolicy, CudaContractError> {
        CudaFixturePolicy::new(
            VECTOR_ADD_FIXTURE_ID,
            bundle,
            image_manifest,
            format!("example.invalid/alloyport/cuda@{image_manifest}"),
            image_id,
            "0",
            root,
            CudaResourceCeilings {
                cpu_millis: 2_000,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: 512 * 1024 * 1024,
                process_count: 64,
                output_bytes: 1024 * 1024,
            },
        )
    }

    fn assignment(
        bundle_digest: Sha256Digest,
        bundle_size: u64,
        image_digest: Sha256Digest,
    ) -> StoredAssignment {
        StoredAssignment {
            assignment_id: "assignment-1".into(),
            attempt_id: "attempt-1".into(),
            attempt_number: 1,
            idempotency_key: "cuda-vectoradd-v1".into(),
            task_id: "task-1".into(),
            candidate_id: "candidate-1".into(),
            execution: StoredExecution {
                executor_kind: ExecutorKind::CudaFixture.into(),
                argv: vec![VECTOR_ADD_FIXTURE_ID.into()],
                working_directory: ".".into(),
                environment: Vec::new(),
                timeout_ms: 30_000,
                bundle: StoredArtifact {
                    digest: bundle_digest.to_string(),
                    size_bytes: bundle_size,
                    media_type: CUDA_FIXTURE_BUNDLE_MEDIA_TYPE.into(),
                },
                image: StoredArtifact {
                    digest: image_digest.to_string(),
                    size_bytes: 0,
                    media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.into(),
                },
                limits: Some(StoredLimits {
                    cpu_millis: 1_000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 256 * 1024 * 1024,
                    process_count: 32,
                    output_bytes: 64 * 1024,
                    device_count: 1,
                    network: NetworkPolicy::Disabled.into(),
                }),
            },
            required_features: vec![CUDA_FIXTURE_FEATURE.into()],
        }
    }
}
