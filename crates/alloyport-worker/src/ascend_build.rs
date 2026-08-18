//! Policy-bound worker contract for controller-authored Ascend candidate build bundles.

use crate::ascend::{AscendDockerCreatePlan, AscendEnvironmentFacts, AscendResourceCeilings};
use crate::container_engine::image_artifact_media_type;
use crate::journal::StoredAssignment;
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    ASCEND_BUILD_BUNDLE_MEDIA_TYPE, ASCEND_BUILD_FEATURE, AcceleratorDevice, CandidateBuildBundle,
    ExecutionKind, NetworkPolicy, Sha256Digest,
};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const DRIVER_PATH: &str = "/usr/local/Ascend/driver";
const CONTAINER_BUNDLE_PATH: &str = "/alloyport/bundle";
const CONTAINER_WORK_PATH: &str = "/alloyport/work";
const RUNNER_FILENAME: &str = "run_build.py";
const MINIMUM_DISK_BYTES: u64 = 128 * 1024 * 1024;
const MAX_BUILD_BUNDLE_BYTES: u64 = 12 * 1024 * 1024;
const RUNNER: &str = include_str!("../../../fixtures/ascend-build-v1/run_build.py");

/// Worker-local allowlist for dynamic candidate bytes and a fixed build environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AscendBuildPolicy {
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

impl AscendBuildPolicy {
    /// Creates a build policy from worker-local identities and ceilings.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete device/image facts, unsafe roots, or zero ceilings.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        image_manifest_digest: Sha256Digest,
        image_reference: impl Into<String>,
        image_id: Sha256Digest,
        device: AcceleratorDevice,
        mut device_nodes: Vec<PathBuf>,
        driver_path: impl Into<PathBuf>,
        sandbox_root: impl Into<PathBuf>,
        ceilings: AscendResourceCeilings,
        environment: AscendEnvironmentFacts,
    ) -> Result<Self, AscendBuildContractError> {
        let image_reference = image_reference.into();
        let image_media_type =
            image_artifact_media_type(&image_reference, image_manifest_digest, image_id)
                .map_err(AscendBuildContractError::InvalidPolicy)?;
        let driver_path = driver_path.into();
        let sandbox_root = sandbox_root.into();
        validate_device(&device, &environment)?;
        validate_device_nodes(&device.device_id, &device_nodes)?;
        device_nodes.sort();
        if driver_path != Path::new(DRIVER_PATH) {
            return Err(AscendBuildContractError::InvalidPolicy(
                "driver mount must use the fixed /usr/local/Ascend/driver path",
            ));
        }
        if !sandbox_root.is_absolute() || sandbox_root.to_string_lossy().contains(',') {
            return Err(AscendBuildContractError::InvalidPolicy(
                "sandbox root must be an absolute path without commas",
            ));
        }
        if ceilings.timeout_ms == 0
            || ceilings.cpu_millis == 0
            || ceilings.memory_bytes == 0
            || ceilings.disk_bytes < MINIMUM_DISK_BYTES
            || ceilings.process_count == 0
            || ceilings.output_bytes == 0
        {
            return Err(AscendBuildContractError::InvalidPolicy(
                "build resource ceilings are incomplete",
            ));
        }
        Ok(Self {
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

    #[must_use]
    pub const fn device(&self) -> &AcceleratorDevice {
        &self.device
    }

    #[must_use]
    pub const fn environment(&self) -> &AscendEnvironmentFacts {
        &self.environment
    }

    /// Validates every server-controlled assignment field against local policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the assignment can select anything except the build bundle.
    pub fn validate_assignment(
        &self,
        assignment: &StoredAssignment,
    ) -> Result<(), AscendBuildContractError> {
        let execution = &assignment.execution;
        validate_attempt_id(assignment.attempt_id.as_str())?;
        if execution.executor_kind != ExecutionKind::AscendBuild
            || assignment.required_features != [ASCEND_BUILD_FEATURE]
            || execution.argv != ["build-v1"]
            || execution.working_directory != "."
            || !execution.environment.is_empty()
        {
            return Err(AscendBuildContractError::Assignment(
                "executor, feature, argv, cwd, or environment is not the fixed build contract",
            ));
        }
        if execution.bundle.media_type != ASCEND_BUILD_BUNDLE_MEDIA_TYPE
            || execution.bundle.size_bytes == 0
            || execution.bundle.size_bytes > MAX_BUILD_BUNDLE_BYTES
        {
            return Err(AscendBuildContractError::Assignment(
                "build bundle identity or size is not allowed",
            ));
        }
        if execution.image.digest != self.image_manifest_digest
            || execution.image.media_type != self.image_media_type
        {
            return Err(AscendBuildContractError::Assignment(
                "build image identity is not locally allowed",
            ));
        }
        validate_limits(assignment, self.ceilings)
    }

    /// Reads, validates, and create-only materializes an exact candidate build bundle.
    ///
    /// # Errors
    ///
    /// Returns an error for policy mismatch, malformed/tampered bytes, or conflicting files.
    pub fn materialize_bundle(
        &self,
        assignment: &StoredAssignment,
        artifacts: &dyn ArtifactStore,
    ) -> Result<AscendBuildSandbox, AscendBuildContractError> {
        self.validate_assignment(assignment)?;
        let mut reader = artifacts
            .open(assignment.execution.bundle.digest)
            .map_err(|error| AscendBuildContractError::Artifact(error.to_string()))?;
        let declared = assignment.execution.bundle.size_bytes;
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(declared.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != declared
            || Sha256Digest::digest_bytes(&bytes) != assignment.execution.bundle.digest
        {
            return Err(AscendBuildContractError::Bundle(
                "build bundle bytes do not match the assignment identity".to_owned(),
            ));
        }
        let bundle: CandidateBuildBundle = serde_json::from_slice(&bytes)?;
        if bundle.candidate_id() != &assignment.candidate_id
            || bundle.task_id() != &assignment.task_id
            || bundle.target_architecture() != self.environment.architecture
        {
            return Err(AscendBuildContractError::Bundle(
                "build bundle lineage or target does not match the assignment".to_owned(),
            ));
        }
        let directory = self.sandbox_root.join(assignment.attempt_id.as_str());
        fs::create_dir_all(&self.sandbox_root)?;
        let root_metadata = fs::symlink_metadata(&self.sandbox_root)?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(AscendBuildContractError::UnsafePath);
        }
        create_real_directory(&directory)?;
        for file in bundle.files() {
            let target = directory.join(file.path().as_str());
            create_real_parents(&directory, &target)?;
            write_once(&target, file.contents().as_bytes())?;
            verify_file(&target, file.digest(), file.size_bytes())?;
        }
        write_once(&directory.join(RUNNER_FILENAME), RUNNER.as_bytes())?;
        verify_exact_tree(&directory, &bundle)?;
        Ok(AscendBuildSandbox {
            directory,
            bundle_digest: assignment.execution.bundle.digest,
        })
    }

    /// Derives a shell-free Docker plan from local policy and the verified build sandbox.
    ///
    /// # Errors
    ///
    /// Returns an error if the assignment no longer matches local policy.
    pub fn docker_create_plan(
        &self,
        assignment: &StoredAssignment,
        sandbox: &AscendBuildSandbox,
    ) -> Result<AscendDockerCreatePlan, AscendBuildContractError> {
        self.validate_assignment(assignment)?;
        let limits =
            assignment
                .execution
                .limits
                .as_ref()
                .ok_or(AscendBuildContractError::Assignment(
                    "build resource limits are required",
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
        // No `--device` and no `ASCEND_RT_VISIBLE_DEVICES`. The build runner is two `cmake` calls
        // and never opens an accelerator; `fixtures/ascend-add-v1` compiles and links in this image
        // with none attached. Mounting one only made every build queue behind other users'
        // processes on a shared host, which is what blocked 2026-08-17.
        argv.extend([
            "--tmpfs".to_owned(),
            format!("{CONTAINER_WORK_PATH}:rw,exec,size={}", limits.disk_bytes),
            "--workdir".to_owned(),
            CONTAINER_WORK_PATH.to_owned(),
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

fn validate_attempt_id(value: &str) -> Result<(), AscendBuildContractError> {
    if value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(AscendBuildContractError::Assignment(
            "attempt ID is unsafe for a build sandbox path",
        ));
    }
    Ok(())
}

fn verify_exact_tree(
    root: &Path,
    bundle: &CandidateBuildBundle,
) -> Result<(), AscendBuildContractError> {
    let mut expected: BTreeSet<String> = bundle
        .files()
        .iter()
        .map(|file| file.path().as_str().to_owned())
        .collect();
    expected.insert(RUNNER_FILENAME.to_owned());
    let mut actual = BTreeSet::new();
    scan_tree(root, root, &mut actual)?;
    if actual != expected {
        return Err(AscendBuildContractError::UnsafePath);
    }
    Ok(())
}

fn scan_tree(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<String>,
) -> Result<(), AscendBuildContractError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err(AscendBuildContractError::UnsafePath);
        }
        if kind.is_dir() {
            scan_tree(root, &entry.path(), files)?;
        } else if kind.is_file() {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| AscendBuildContractError::UnsafePath)?
                .to_string_lossy()
                .into_owned();
            files.insert(relative);
        } else {
            return Err(AscendBuildContractError::UnsafePath);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AscendBuildSandbox {
    directory: PathBuf,
    bundle_digest: Sha256Digest,
}

impl AscendBuildSandbox {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub const fn bundle_digest(&self) -> Sha256Digest {
        self.bundle_digest
    }
}

fn validate_device(
    device: &AcceleratorDevice,
    environment: &AscendEnvironmentFacts,
) -> Result<(), AscendBuildContractError> {
    if device.device_id.trim().is_empty()
        || device.product_name.trim().is_empty()
        || device.serial_number.trim().is_empty()
        || device.firmware_version.trim().is_empty()
        || device.product_name != environment.architecture
        || device.firmware_version != environment.firmware_version
    {
        return Err(AscendBuildContractError::InvalidPolicy(
            "device identity does not match the build environment",
        ));
    }
    Ok(())
}

fn validate_device_nodes(
    device_id: &str,
    nodes: &[PathBuf],
) -> Result<(), AscendBuildContractError> {
    let mut unique = BTreeSet::new();
    for path in nodes {
        let value = path.to_string_lossy();
        let allowed = value.strip_prefix("/dev/davinci").is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        }) || value == "/dev/davinci_manager"
            || value == "/dev/hisi_hdc";
        if !path.is_absolute() || !allowed || !unique.insert(value.into_owned()) {
            return Err(AscendBuildContractError::InvalidPolicy(
                "device nodes are not a unique fixed Ascend inventory",
            ));
        }
    }
    for required in [
        format!("/dev/davinci{device_id}"),
        "/dev/davinci_manager".to_owned(),
        "/dev/hisi_hdc".to_owned(),
    ] {
        if !unique.contains(&required) {
            return Err(AscendBuildContractError::InvalidPolicy(
                "selected device and manager nodes are required",
            ));
        }
    }
    Ok(())
}

fn validate_limits(
    assignment: &StoredAssignment,
    ceilings: AscendResourceCeilings,
) -> Result<(), AscendBuildContractError> {
    let execution = &assignment.execution;
    let limits = execution
        .limits
        .as_ref()
        .ok_or(AscendBuildContractError::Assignment(
            "build resource limits are required",
        ))?;
    if execution.timeout_ms == 0
        || execution.timeout_ms > ceilings.timeout_ms
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
        || limits.device_count != 0
        || limits.network != NetworkPolicy::Disabled
    {
        return Err(AscendBuildContractError::Assignment(
            "build limits exceed local policy or enable network access",
        ));
    }
    Ok(())
}

fn create_real_directory(path: &Path) -> Result<(), AscendBuildContractError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(AscendBuildContractError::UnsafePath);
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn create_real_parents(root: &Path, target: &Path) -> Result<(), AscendBuildContractError> {
    let parent = target
        .parent()
        .ok_or(AscendBuildContractError::UnsafePath)?;
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| AscendBuildContractError::UnsafePath)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        create_real_directory(&current)?;
    }
    Ok(())
}

fn write_once(path: &Path, bytes: &[u8]) -> Result<(), AscendBuildContractError> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::symlink_metadata(path)?.file_type().is_symlink() || fs::read(path)? != bytes {
                return Err(AscendBuildContractError::UnsafePath);
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_file(
    path: &Path,
    digest: Sha256Digest,
    size: u64,
) -> Result<(), AscendBuildContractError> {
    let metadata = fs::symlink_metadata(path)?;
    let bytes = fs::read(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || u64::try_from(bytes.len()).unwrap_or(u64::MAX) != size
        || Sha256Digest::digest_bytes(&bytes) != digest
    {
        return Err(AscendBuildContractError::UnsafePath);
    }
    Ok(())
}

#[derive(Debug)]
pub enum AscendBuildContractError {
    InvalidPolicy(&'static str),
    Assignment(&'static str),
    Artifact(String),
    Bundle(String),
    UnsafePath,
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl Display for AscendBuildContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(detail) => {
                write!(formatter, "invalid Ascend build policy: {detail}")
            }
            Self::Assignment(detail) => {
                write!(formatter, "Ascend build assignment rejected: {detail}")
            }
            Self::Artifact(detail) => write!(formatter, "Ascend build Artifact error: {detail}"),
            Self::Bundle(detail) => write!(formatter, "invalid Ascend build bundle: {detail}"),
            Self::UnsafePath => write!(formatter, "Ascend build sandbox path is unsafe"),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Json(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for AscendBuildContractError {}

impl From<std::io::Error> for AscendBuildContractError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for AscendBuildContractError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[cfg(test)]
#[path = "ascend_build_tests.rs"]
mod tests;
