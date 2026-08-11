//! Policy-bound contract for the first fixed CUDA container vertical slice.

use crate::journal::{StoredAssignment, StoredLimits};
use alloyport_artifacts::{ArtifactStore, Sha256Digest};
use alloyport_core::{ExecutionKind, NetworkPolicy};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

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
        if execution.executor_kind != ExecutionKind::CudaFixture {
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
        artifacts: &dyn ArtifactStore,
    ) -> Result<CudaSandbox, CudaContractError> {
        self.validate_assignment(assignment)?;
        validate_attempt_id(&assignment.attempt_id)?;
        let digest = assignment.execution.bundle.digest;
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
        let directory = self.sandbox_root.join(assignment.attempt_id.as_str());
        fs::create_dir_all(&directory)?;
        write_once(&directory.join(SOURCE_FILENAME), bundle.source.as_bytes())?;
        write_once(&directory.join(RUNNER_FILENAME), RUNNER.as_bytes())?;
        Ok(CudaSandbox {
            directory,
            source_digest,
        })
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
            image_reference: self.image_reference.clone(),
            expected_image_id: self.image_id,
            device_id: self.device_id.clone(),
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
                "--log-driver".into(),
                "json-file".into(),
                "--log-opt".into(),
                format!("max-size={}", limits.output_bytes),
                "--log-opt".into(),
                "max-file=2".into(),
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
                "--entrypoint".into(),
                "python3".into(),
                self.image_reference.clone(),
                format!("{CONTAINER_BUNDLE_PATH}/{RUNNER_FILENAME}"),
            ],
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaSandbox {
    directory: PathBuf,
    source_digest: String,
}

impl CudaSandbox {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub fn source_digest(&self) -> &str {
        &self.source_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerCreatePlan {
    pub container_name: String,
    pub image_reference: String,
    pub expected_image_id: Sha256Digest,
    pub device_id: String,
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
    if artifact.digest != expected_digest || artifact.media_type != expected_media_type {
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
        || limits.network != NetworkPolicy::Disabled
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
#[path = "cuda_tests.rs"]
mod tests;
