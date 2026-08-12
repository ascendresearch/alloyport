//! Policy-bound contract for the first fixed Ascend container vertical slice.

use crate::container_engine::image_artifact_media_type;
use crate::journal::{StoredAssignment, StoredLimits};
use alloyport_artifacts::{ArtifactStore, Sha256Digest};
use alloyport_core::{AcceleratorDevice, ExecutionKind, NetworkPolicy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const ASCEND_FIXTURE_FEATURE: &str = "ascend-fixture-v1";
pub const ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE: &str =
    "application/vnd.alloyport.ascend-fixture.v1+json";
pub const ASCEND_ADD_FIXTURE_ID: &str = "ascend-add-v1";
pub use crate::container_engine::{OCI_IMAGE_CONFIG_MEDIA_TYPE, OCI_IMAGE_MANIFEST_MEDIA_TYPE};
const DRIVER_PATH: &str = "/usr/local/Ascend/driver";
const CONTAINER_BUNDLE_PATH: &str = "/alloyport/bundle";
const CONTAINER_WORK_PATH: &str = "/alloyport/work";
const RUNNER_FILENAME: &str = "run_fixture.py";
const SOURCE_FILENAME: &str = "add_custom.cpp";
const MINIMUM_DISK_BYTES: u64 = 128 * 1024 * 1024;
const RUNNER: &str = include_str!("../../../fixtures/ascend-add-v1/run_fixture.py");

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AscendFixtureBundle {
    pub schema_version: u16,
    pub fixture_id: String,
    pub source_sha256: String,
    pub source: String,
}

impl AscendFixtureBundle {
    #[must_use]
    pub fn add(source: impl Into<String>) -> Self {
        let source = source.into();
        Self {
            schema_version: 1,
            fixture_id: ASCEND_ADD_FIXTURE_ID.into(),
            source_sha256: Sha256Digest::digest_bytes(source.as_bytes()).to_string(),
            source,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AscendEnvironmentFacts {
    pub architecture: String,
    pub cann_version: String,
    pub driver_version: String,
    pub firmware_version: String,
}

impl AscendEnvironmentFacts {
    /// Creates exact environment facts which will eventually be bound into the run receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity component is empty.
    pub fn new(
        architecture: impl Into<String>,
        cann_version: impl Into<String>,
        driver_version: impl Into<String>,
        firmware_version: impl Into<String>,
    ) -> Result<Self, AscendContractError> {
        let facts = Self {
            architecture: architecture.into(),
            cann_version: cann_version.into(),
            driver_version: driver_version.into(),
            firmware_version: firmware_version.into(),
        };
        if [
            facts.architecture.as_str(),
            facts.cann_version.as_str(),
            facts.driver_version.as_str(),
            facts.firmware_version.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(AscendContractError::InvalidPolicy(
                "CANN, driver, firmware, and architecture identities must be nonempty",
            ));
        }
        Ok(facts)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AscendResourceCeilings {
    pub timeout_ms: u64,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AscendFixturePolicy {
    fixture_id: String,
    bundle_digest: Sha256Digest,
    image_manifest_digest: Sha256Digest,
    image_media_type: &'static str,
    image_reference: String,
    image_id: Sha256Digest,
    device: AcceleratorDevice,
    device_nodes: Vec<PathBuf>,
    driver_path: PathBuf,
    sandbox_root: PathBuf,
    ceilings: AscendResourceCeilings,
    environment: AscendEnvironmentFacts,
}

impl AscendFixturePolicy {
    #[must_use]
    pub const fn device(&self) -> &AcceleratorDevice {
        &self.device
    }

    #[must_use]
    pub const fn environment(&self) -> &AscendEnvironmentFacts {
        &self.environment
    }

    /// Creates one complete worker-local allowlist. No path in this policy comes from an assignment.
    ///
    /// # Errors
    ///
    /// Returns an error for unpinned images, incomplete identity, unsafe paths, missing manager
    /// nodes, a selected device not present in the node inventory, or zero resource ceilings.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fixture_id: impl Into<String>,
        bundle_digest: Sha256Digest,
        image_manifest_digest: Sha256Digest,
        image_reference: impl Into<String>,
        image_id: Sha256Digest,
        device: AcceleratorDevice,
        mut device_nodes: Vec<PathBuf>,
        driver_path: impl Into<PathBuf>,
        sandbox_root: impl Into<PathBuf>,
        ceilings: AscendResourceCeilings,
        environment: AscendEnvironmentFacts,
    ) -> Result<Self, AscendContractError> {
        let fixture_id = fixture_id.into();
        let image_reference = image_reference.into();
        let driver_path = driver_path.into();
        let sandbox_root = sandbox_root.into();
        if fixture_id.trim().is_empty() {
            return Err(AscendContractError::InvalidPolicy("fixture ID is empty"));
        }
        let image_media_type =
            image_artifact_media_type(&image_reference, image_manifest_digest, image_id)
                .map_err(AscendContractError::InvalidPolicy)?;
        validate_device_identity(&device)?;
        validate_device_nodes(&device.device_id, &device_nodes)?;
        device_nodes.sort();
        if driver_path != Path::new(DRIVER_PATH) {
            return Err(AscendContractError::InvalidPolicy(
                "driver mount must use the fixed /usr/local/Ascend/driver path",
            ));
        }
        if !sandbox_root.is_absolute() || sandbox_root.to_string_lossy().contains(',') {
            return Err(AscendContractError::InvalidPolicy(
                "sandbox root must be an absolute path without commas",
            ));
        }
        if ceilings.timeout_ms == 0
            || ceilings.cpu_millis == 0
            || ceilings.memory_bytes == 0
            || ceilings.disk_bytes == 0
            || ceilings.process_count == 0
            || ceilings.output_bytes == 0
        {
            return Err(AscendContractError::InvalidPolicy(
                "resource ceilings must all be nonzero",
            ));
        }
        if device.firmware_version != environment.firmware_version
            || device.product_name != environment.architecture
        {
            return Err(AscendContractError::InvalidPolicy(
                "device identity does not match environment facts",
            ));
        }
        Ok(Self {
            fixture_id,
            bundle_digest,
            image_manifest_digest,
            image_media_type,
            image_reference,
            image_id,
            device,
            device_nodes,
            driver_path,
            sandbox_root,
            ceilings,
            environment,
        })
    }

    /// Validates the immutable assignment against the complete local Ascend allowlist.
    ///
    /// # Errors
    ///
    /// Returns an error when a server-controlled field differs from or exceeds local policy.
    pub fn validate_assignment(
        &self,
        assignment: &StoredAssignment,
    ) -> Result<(), AscendContractError> {
        let execution = &assignment.execution;
        if execution.executor_kind != ExecutionKind::AscendFixture {
            return Err(AscendContractError::Assignment(
                "executor kind is not Ascend fixture",
            ));
        }
        if assignment.required_features != [ASCEND_FIXTURE_FEATURE] {
            return Err(AscendContractError::Assignment(
                "required features must contain only ascend-fixture-v1",
            ));
        }
        if execution.argv != [self.fixture_id.as_str()] {
            return Err(AscendContractError::Assignment(
                "argv does not name the allowed fixture",
            ));
        }
        if execution.working_directory != "." {
            return Err(AscendContractError::Assignment(
                "working directory must be the fixture root",
            ));
        }
        if !execution.environment.is_empty() {
            return Err(AscendContractError::Assignment(
                "Ascend fixture environment must be empty",
            ));
        }
        validate_attempt_id(assignment.attempt_id.as_str())?;
        if execution.bundle.digest != self.bundle_digest
            || execution.bundle.media_type != ASCEND_FIXTURE_BUNDLE_MEDIA_TYPE
        {
            return Err(AscendContractError::Assignment(
                "bundle identity is not locally allowed",
            ));
        }
        if execution.image.digest != self.image_manifest_digest
            || execution.image.media_type != self.image_media_type
        {
            return Err(AscendContractError::Assignment(
                "image identity is not locally allowed",
            ));
        }
        validate_limits(
            execution.timeout_ms,
            execution.limits.as_ref(),
            self.ceilings,
        )
    }

    /// Materializes a verified fixture bundle into one attempt-owned, write-once sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error for policy mismatch, missing/tampered bytes, malformed bundle data,
    /// conflicting recovery files, or local filesystem failure.
    pub fn materialize_bundle(
        &self,
        assignment: &StoredAssignment,
        artifacts: &dyn ArtifactStore,
    ) -> Result<AscendSandbox, AscendContractError> {
        self.validate_assignment(assignment)?;
        let mut reader = artifacts
            .open(assignment.execution.bundle.digest)
            .map_err(|error| AscendContractError::Artifact(error.to_string()))?;
        let declared = assignment.execution.bundle.size_bytes;
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(declared.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != declared {
            return Err(AscendContractError::Bundle(
                "bundle bytes do not match the assignment size".into(),
            ));
        }
        let bundle: AscendFixtureBundle = serde_json::from_slice(&bytes)?;
        if bundle.schema_version != 1 || bundle.fixture_id != self.fixture_id {
            return Err(AscendContractError::Bundle(
                "bundle schema or fixture identity is not allowed".into(),
            ));
        }
        let source_digest = Sha256Digest::digest_bytes(bundle.source.as_bytes()).to_string();
        if source_digest != bundle.source_sha256 {
            return Err(AscendContractError::Bundle(
                "source digest does not match the bundle manifest".into(),
            ));
        }
        let directory = self.sandbox_root.join(assignment.attempt_id.as_str());
        fs::create_dir_all(&directory)?;
        write_once(&directory.join(SOURCE_FILENAME), bundle.source.as_bytes())?;
        write_once(&directory.join(RUNNER_FILENAME), RUNNER.as_bytes())?;
        Ok(AscendSandbox {
            directory,
            source_digest,
        })
    }

    /// Derives a Docker create plan without accepting host paths, devices, mounts, env, or shell.
    ///
    /// # Errors
    ///
    /// Returns an error if the assignment no longer matches local policy.
    pub fn docker_create_plan(
        &self,
        assignment: &StoredAssignment,
        sandbox: &AscendSandbox,
    ) -> Result<AscendDockerCreatePlan, AscendContractError> {
        self.validate_assignment(assignment)?;
        let limits =
            assignment
                .execution
                .limits
                .as_ref()
                .ok_or(AscendContractError::Assignment(
                    "resource limits are required",
                ))?;
        let container_name = format!("alloyport-{}", assignment.attempt_id);
        let mut argv = vec![
            "create".to_owned(),
            "--name".to_owned(),
            container_name.clone(),
            "--label".to_owned(),
            format!("alloyport.attempt={}", assignment.attempt_id),
            "--label".to_owned(),
            format!("alloyport.bundle={}", assignment.execution.bundle.digest),
            "--label".to_owned(),
            format!("alloyport.image={}", assignment.execution.image.digest),
            "--network".to_owned(),
            "none".to_owned(),
            "--read-only".to_owned(),
            "--cap-drop".to_owned(),
            "ALL".to_owned(),
            "--cap-add".to_owned(),
            "DAC_OVERRIDE".to_owned(),
            "--security-opt".to_owned(),
            "no-new-privileges".to_owned(),
            "--cpu-period".to_owned(),
            "100000".to_owned(),
            "--cpu-quota".to_owned(),
            limits.cpu_millis.saturating_mul(100).to_string(),
            "--memory".to_owned(),
            limits.memory_bytes.to_string(),
            "--pids-limit".to_owned(),
            limits.process_count.to_string(),
            "--log-driver".to_owned(),
            "json-file".to_owned(),
            "--log-opt".to_owned(),
            format!("max-size={}", limits.output_bytes),
            "--log-opt".to_owned(),
            "max-file=2".to_owned(),
            "--mount".to_owned(),
            format!(
                "type=bind,src={},dst={CONTAINER_BUNDLE_PATH},readonly",
                sandbox.directory.display()
            ),
            "--mount".to_owned(),
            format!(
                "type=bind,src={},dst={DRIVER_PATH},readonly",
                self.driver_path.display()
            ),
        ];
        for device_node in &self.device_nodes {
            argv.push("--device".to_owned());
            argv.push(format!("{path}:{path}:rwm", path = device_node.display()));
        }
        argv.extend([
            "--tmpfs".to_owned(),
            format!("{CONTAINER_WORK_PATH}:rw,exec,size={}", limits.disk_bytes),
            "--workdir".to_owned(),
            CONTAINER_WORK_PATH.to_owned(),
            "--env".to_owned(),
            format!("ASCEND_RT_VISIBLE_DEVICES={}", self.device.device_id),
            "--env".to_owned(),
            format!("TMPDIR={CONTAINER_WORK_PATH}/tmp"),
            "--env".to_owned(),
            format!("HOME={CONTAINER_WORK_PATH}/home"),
            "--env".to_owned(),
            format!("ASCEND_PROCESS_LOG_PATH={CONTAINER_WORK_PATH}/log"),
            "--entrypoint".to_owned(),
            "python3".to_owned(),
            self.image_reference.clone(),
            format!("{CONTAINER_BUNDLE_PATH}/{RUNNER_FILENAME}"),
        ]);
        Ok(AscendDockerCreatePlan {
            container_name,
            image_reference: self.image_reference.clone(),
            expected_image_id: self.image_id,
            device: self.device.clone(),
            environment: self.environment.clone(),
            argv,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AscendSandbox {
    directory: PathBuf,
    source_digest: String,
}

impl AscendSandbox {
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
pub struct AscendDockerCreatePlan {
    pub container_name: String,
    pub image_reference: String,
    pub expected_image_id: Sha256Digest,
    pub device: AcceleratorDevice,
    pub environment: AscendEnvironmentFacts,
    pub argv: Vec<String>,
}

#[derive(Debug)]
pub enum AscendContractError {
    InvalidPolicy(&'static str),
    Assignment(&'static str),
    Artifact(String),
    Bundle(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl Display for AscendContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(detail) => write!(formatter, "invalid Ascend policy: {detail}"),
            Self::Assignment(detail) => write!(formatter, "Ascend assignment rejected: {detail}"),
            Self::Artifact(detail) => write!(formatter, "Ascend input Artifact error: {detail}"),
            Self::Bundle(detail) => write!(formatter, "invalid Ascend fixture bundle: {detail}"),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Json(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for AscendContractError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidPolicy(_) | Self::Assignment(_) | Self::Artifact(_) | Self::Bundle(_) => {
                None
            }
        }
    }
}

impl From<std::io::Error> for AscendContractError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for AscendContractError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

fn validate_device_identity(device: &AcceleratorDevice) -> Result<(), AscendContractError> {
    if device.device_id.is_empty()
        || !device.device_id.bytes().all(|byte| byte.is_ascii_digit())
        || device.product_name.trim().is_empty()
        || device.serial_number.trim().is_empty()
        || device.firmware_version.trim().is_empty()
    {
        return Err(AscendContractError::InvalidPolicy(
            "device identity is incomplete or the device ID is not numeric",
        ));
    }
    Ok(())
}

fn validate_attempt_id(attempt_id: &str) -> Result<(), AscendContractError> {
    if attempt_id.is_empty()
        || attempt_id.len() > 64
        || !attempt_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(AscendContractError::Assignment(
            "attempt ID is unsafe for local process identity",
        ));
    }
    Ok(())
}

fn validate_device_nodes(
    selected_device_id: &str,
    device_nodes: &[PathBuf],
) -> Result<(), AscendContractError> {
    let mut unique = BTreeSet::new();
    for path in device_nodes {
        let value = path.to_string_lossy();
        let is_device = value.strip_prefix("/dev/davinci").is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        }) || value == "/dev/davinci_manager"
            || value == "/dev/hisi_hdc";
        if !path.is_absolute() || !is_device || !unique.insert(value.into_owned()) {
            return Err(AscendContractError::InvalidPolicy(
                "device nodes must be unique enumerated davinci, manager, or hisi_hdc paths",
            ));
        }
    }
    for required in [
        format!("/dev/davinci{selected_device_id}"),
        "/dev/davinci_manager".to_owned(),
        "/dev/hisi_hdc".to_owned(),
    ] {
        if !unique.contains(&required) {
            return Err(AscendContractError::InvalidPolicy(
                "selected device, davinci_manager, and hisi_hdc nodes are required",
            ));
        }
    }
    Ok(())
}

fn validate_limits(
    timeout_ms: u64,
    limits: Option<&StoredLimits>,
    ceilings: AscendResourceCeilings,
) -> Result<(), AscendContractError> {
    let limits = limits.ok_or(AscendContractError::Assignment(
        "resource limits are required",
    ))?;
    if timeout_ms == 0
        || timeout_ms > ceilings.timeout_ms
        || limits.cpu_millis == 0
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
        return Err(AscendContractError::Assignment(
            "resource limits exceed policy or permit an unsafe mode",
        ));
    }
    Ok(())
}

fn write_once(path: &Path, bytes: &[u8]) -> Result<(), AscendContractError> {
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
                Err(AscendContractError::Bundle(format!(
                    "existing sandbox file {} has conflicting bytes",
                    path.display()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
#[path = "ascend_tests.rs"]
mod tests;
