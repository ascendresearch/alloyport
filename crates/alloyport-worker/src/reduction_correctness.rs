//! Worker-local policy for trusted reduction correctness harnesses.

use crate::ascend::{AscendDockerCreatePlan, AscendEnvironmentFacts};
use crate::container_engine::image_artifact_media_type;
use crate::cuda::DockerCreatePlan;
use crate::journal::StoredAssignment;
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    ASCEND_REDUCTION_CORRECTNESS_FEATURE, AcceleratorDevice, CUDA_REDUCTION_CORRECTNESS_FEATURE,
    ExecutionKind, NetworkPolicy, REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE, ReductionExecutionBundle,
    ReductionRunRole, Sha256Digest,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const DRIVER_PATH: &str = "/usr/local/Ascend/driver";
const CONTAINER_BUNDLE_PATH: &str = "/alloyport/bundle";
const CONTAINER_WORK_PATH: &str = "/alloyport/work";
const BUNDLE_FILENAME: &str = "execution-bundle.json";
const CONFIG_FILENAME: &str = "runner-config.json";
const RUNNER_FILENAME: &str = "run_correctness.py";
const MINIMUM_DISK_BYTES: u64 = 128 * 1024 * 1024;
const CUDA_TEMP_BYTES: u64 = 64 * 1024 * 1024;
const MAX_EXECUTION_BUNDLE_BYTES: u64 = 32 * 1024 * 1024;
const RUNNER: &str = include_str!("../../../fixtures/reduction-correctness-v1/run_correctness.py");

/// Worker-local maximums shared by both correctness roles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorrectnessResourceCeilings {
    pub timeout_ms: u64,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub process_count: u32,
    pub output_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BackendPolicy {
    Cuda { device_id: String },
    Ascend(Box<AscendBackendPolicy>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AscendBackendPolicy {
    device: AcceleratorDevice,
    device_nodes: Vec<PathBuf>,
    driver_path: PathBuf,
    environment: AscendEnvironmentFacts,
}

/// Exact image, device, filesystem, and role policy for one trusted correctness runner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReductionCorrectnessPolicy {
    backend: BackendPolicy,
    image_manifest_digest: Sha256Digest,
    image_media_type: &'static str,
    image_reference: String,
    image_id: Sha256Digest,
    sandbox_root: PathBuf,
    ceilings: CorrectnessResourceCeilings,
    environment_digest: Sha256Digest,
}

impl ReductionCorrectnessPolicy {
    /// Creates a CUDA-authority policy from worker-local facts.
    /// # Errors
    /// Returns an error for unsafe image, device, path, resource, or environment facts.
    #[allow(clippy::too_many_arguments)]
    pub fn new_cuda(
        image_manifest_digest: Sha256Digest,
        image_reference: impl Into<String>,
        image_id: Sha256Digest,
        device_id: impl Into<String>,
        sandbox_root: impl Into<PathBuf>,
        ceilings: CorrectnessResourceCeilings,
        environment: &crate::cuda_runtime::CudaEnvironmentFacts,
    ) -> Result<Self, CorrectnessContractError> {
        let device_id = device_id.into();
        if device_id.trim().is_empty() || device_id.contains(',') {
            return Err(CorrectnessContractError::InvalidPolicy(
                "CUDA device identity is empty or contains a separator",
            ));
        }
        Self::new(
            BackendPolicy::Cuda { device_id },
            image_manifest_digest,
            image_reference,
            image_id,
            sandbox_root,
            ceilings,
            environment,
        )
    }

    /// Creates an Ascend-DUT policy from worker-local facts.
    /// # Errors
    /// Returns an error for unsafe image, device inventory, path, resource, or environment facts.
    #[allow(clippy::too_many_arguments)]
    pub fn new_ascend(
        image_manifest_digest: Sha256Digest,
        image_reference: impl Into<String>,
        image_id: Sha256Digest,
        device: AcceleratorDevice,
        mut device_nodes: Vec<PathBuf>,
        driver_path: impl Into<PathBuf>,
        sandbox_root: impl Into<PathBuf>,
        ceilings: CorrectnessResourceCeilings,
        environment: &AscendEnvironmentFacts,
    ) -> Result<Self, CorrectnessContractError> {
        validate_ascend_device(&device, environment)?;
        validate_ascend_nodes(&device.device_id, &device_nodes)?;
        device_nodes.sort();
        let driver_path = driver_path.into();
        if driver_path != Path::new(DRIVER_PATH) {
            return Err(CorrectnessContractError::InvalidPolicy(
                "driver mount must use the fixed /usr/local/Ascend/driver path",
            ));
        }
        Self::new(
            BackendPolicy::Ascend(Box::new(AscendBackendPolicy {
                device,
                device_nodes,
                driver_path,
                environment: environment.clone(),
            })),
            image_manifest_digest,
            image_reference,
            image_id,
            sandbox_root,
            ceilings,
            environment,
        )
    }

    fn new(
        backend: BackendPolicy,
        image_manifest_digest: Sha256Digest,
        image_reference: impl Into<String>,
        image_id: Sha256Digest,
        sandbox_root: impl Into<PathBuf>,
        ceilings: CorrectnessResourceCeilings,
        environment: &impl Serialize,
    ) -> Result<Self, CorrectnessContractError> {
        let image_reference = image_reference.into();
        let image_media_type =
            image_artifact_media_type(&image_reference, image_manifest_digest, image_id)
                .map_err(CorrectnessContractError::InvalidPolicy)?;
        let sandbox_root = sandbox_root.into();
        if !sandbox_root.is_absolute() || sandbox_root.to_string_lossy().contains(',') {
            return Err(CorrectnessContractError::InvalidPolicy(
                "sandbox root must be an absolute path without commas",
            ));
        }
        validate_ceilings(ceilings)?;
        let environment_digest = Sha256Digest::digest_bytes(&serde_json::to_vec(environment)?);
        Ok(Self {
            backend,
            image_manifest_digest,
            image_media_type,
            image_reference,
            image_id,
            sandbox_root,
            ceilings,
            environment_digest,
        })
    }

    #[must_use]
    pub const fn role(&self) -> ReductionRunRole {
        match self.backend {
            BackendPolicy::Cuda { .. } => ReductionRunRole::CudaReference,
            BackendPolicy::Ascend(_) => ReductionRunRole::AscendCandidate,
        }
    }

    #[must_use]
    pub const fn executor_kind(&self) -> ExecutionKind {
        match self.backend {
            BackendPolicy::Cuda { .. } => ExecutionKind::CudaCorrectness,
            BackendPolicy::Ascend(_) => ExecutionKind::AscendCorrectness,
        }
    }

    #[must_use]
    pub fn device_id(&self) -> &str {
        match &self.backend {
            BackendPolicy::Cuda { device_id } => device_id,
            BackendPolicy::Ascend(policy) => &policy.device.device_id,
        }
    }

    #[must_use]
    pub const fn ascend_device(&self) -> Option<&AcceleratorDevice> {
        match &self.backend {
            BackendPolicy::Cuda { .. } => None,
            BackendPolicy::Ascend(policy) => Some(&policy.device),
        }
    }

    #[must_use]
    pub const fn ascend_environment(&self) -> Option<&AscendEnvironmentFacts> {
        match &self.backend {
            BackendPolicy::Cuda { .. } => None,
            BackendPolicy::Ascend(policy) => Some(&policy.environment),
        }
    }

    /// Validates every controller-selected field before bundle bytes are read.
    /// # Errors
    /// Returns an error unless the assignment is the exact fixed contract for this role.
    pub fn validate_assignment(
        &self,
        assignment: &StoredAssignment,
    ) -> Result<(), CorrectnessContractError> {
        let (feature, executor) = match self.role() {
            ReductionRunRole::CudaReference => (
                CUDA_REDUCTION_CORRECTNESS_FEATURE,
                ExecutionKind::CudaCorrectness,
            ),
            ReductionRunRole::AscendCandidate => (
                ASCEND_REDUCTION_CORRECTNESS_FEATURE,
                ExecutionKind::AscendCorrectness,
            ),
        };
        let execution = &assignment.execution;
        validate_attempt_id(assignment.attempt_id.as_str())?;
        if execution.executor_kind != executor
            || assignment.required_features != [feature]
            || execution.argv != ["reduction-correctness-v1"]
            || execution.working_directory != "."
            || !execution.environment.is_empty()
        {
            return Err(CorrectnessContractError::Assignment(
                "executor, feature, argv, cwd, or environment is not the fixed correctness contract",
            ));
        }
        if execution.bundle.media_type != REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE
            || execution.bundle.size_bytes == 0
            || execution.bundle.size_bytes > MAX_EXECUTION_BUNDLE_BYTES
        {
            return Err(CorrectnessContractError::Assignment(
                "execution bundle descriptor is not allowed",
            ));
        }
        if execution.image.digest != self.image_manifest_digest
            || execution.image.media_type != self.image_media_type
        {
            return Err(CorrectnessContractError::Assignment(
                "correctness image identity is not locally allowed",
            ));
        }
        validate_limits(assignment, self.ceilings)
    }

    /// Reads, validates, and create-only materializes one role-separated execution bundle.
    ///
    /// # Errors
    ///
    /// Returns an error for assignment, Artifact, bundle, lineage, or filesystem violations.
    pub fn materialize_bundle(
        &self,
        assignment: &StoredAssignment,
        artifacts: &dyn ArtifactStore,
    ) -> Result<CorrectnessSandbox, CorrectnessContractError> {
        self.validate_assignment(assignment)?;
        let mut reader = artifacts
            .open(assignment.execution.bundle.digest)
            .map_err(|error| CorrectnessContractError::Artifact(error.to_string()))?;
        let declared = assignment.execution.bundle.size_bytes;
        let mut bytes = Vec::new();
        reader
            .by_ref()
            .take(declared.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != declared
            || Sha256Digest::digest_bytes(&bytes) != assignment.execution.bundle.digest
        {
            return Err(CorrectnessContractError::Bundle(
                "execution bundle bytes do not match the assignment identity".to_owned(),
            ));
        }
        let bundle: ReductionExecutionBundle = serde_json::from_slice(&bytes)?;
        if bundle.role() != self.role()
            || bundle.experiment().task_id() != &assignment.task_id
            || bundle.experiment().candidate_id() != &assignment.candidate_id
        {
            return Err(CorrectnessContractError::Bundle(
                "execution bundle role or lineage does not match the assignment".to_owned(),
            ));
        }
        let directory = self.sandbox_root.join(assignment.attempt_id.as_str());
        create_real_directory(&self.sandbox_root)?;
        create_real_directory(&directory)?;
        for file in bundle.files() {
            let target = directory.join(file.path().as_str());
            create_real_parents(&directory, &target)?;
            write_once(&target, file.contents().as_bytes())?;
        }
        write_once(&directory.join(BUNDLE_FILENAME), &bytes)?;
        let config = serde_json::to_vec(&RunnerConfig {
            environment_digest: self.environment_digest,
        })?;
        write_once(&directory.join(CONFIG_FILENAME), &config)?;
        write_once(&directory.join(RUNNER_FILENAME), RUNNER.as_bytes())?;
        verify_exact_tree(&directory, &bundle)?;
        Ok(CorrectnessSandbox {
            directory,
            implementation_digest: bundle.implementation_digest(),
        })
    }

    /// Derives the fixed CUDA Docker plan.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-CUDA policy or changed assignment.
    pub fn cuda_docker_create_plan(
        &self,
        assignment: &StoredAssignment,
        sandbox: &CorrectnessSandbox,
    ) -> Result<DockerCreatePlan, CorrectnessContractError> {
        self.validate_assignment(assignment)?;
        let BackendPolicy::Cuda { device_id } = &self.backend else {
            return Err(CorrectnessContractError::WrongBackend);
        };
        let limits =
            assignment
                .execution
                .limits
                .as_ref()
                .ok_or(CorrectnessContractError::Assignment(
                    "correctness resource limits are required",
                ))?;
        let container_name = format!("alloyport-{}", assignment.attempt_id);
        let work_bytes = limits.disk_bytes.saturating_sub(CUDA_TEMP_BYTES);
        Ok(DockerCreatePlan {
            container_name: container_name.clone(),
            image_reference: self.image_reference.clone(),
            expected_image_id: self.image_id,
            device_id: device_id.clone(),
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
                limits.cpu_millis.saturating_mul(100).to_string(),
                "--memory".into(),
                limits.memory_bytes.to_string(),
                "--pids-limit".into(),
                limits.process_count.to_string(),
                "--gpus".into(),
                format!("device={device_id}"),
                "--mount".into(),
                sandbox_mount(sandbox),
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

    /// Derives the fixed Ascend Docker plan.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-Ascend policy or changed assignment.
    pub fn ascend_docker_create_plan(
        &self,
        assignment: &StoredAssignment,
        sandbox: &CorrectnessSandbox,
    ) -> Result<AscendDockerCreatePlan, CorrectnessContractError> {
        self.validate_assignment(assignment)?;
        let BackendPolicy::Ascend(policy) = &self.backend else {
            return Err(CorrectnessContractError::WrongBackend);
        };
        let AscendBackendPolicy {
            device,
            device_nodes,
            driver_path,
            environment,
        } = policy.as_ref();
        let limits =
            assignment
                .execution
                .limits
                .as_ref()
                .ok_or(CorrectnessContractError::Assignment(
                    "correctness resource limits are required",
                ))?;
        let container_name = format!("alloyport-{}", assignment.attempt_id);
        let mut argv =
            common_ascend_argv(assignment, sandbox, &container_name, limits, driver_path);
        for device_node in device_nodes {
            argv.push("--device".to_owned());
            argv.push(format!("{path}:{path}:rwm", path = device_node.display()));
        }
        argv.extend([
            "--tmpfs".to_owned(),
            format!("{CONTAINER_WORK_PATH}:rw,exec,size={}", limits.disk_bytes),
            "--workdir".to_owned(),
            CONTAINER_WORK_PATH.to_owned(),
            "--env".to_owned(),
            format!("ASCEND_RT_VISIBLE_DEVICES={}", device.device_id),
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
            device: device.clone(),
            environment: environment.clone(),
            argv,
        })
    }
}

#[derive(Serialize)]
struct RunnerConfig {
    environment_digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrectnessSandbox {
    directory: PathBuf,
    implementation_digest: Sha256Digest,
}

impl CorrectnessSandbox {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub const fn implementation_digest(&self) -> Sha256Digest {
        self.implementation_digest
    }
}

fn common_ascend_argv(
    assignment: &StoredAssignment,
    sandbox: &CorrectnessSandbox,
    container_name: &str,
    limits: &alloyport_core::ResourceContract,
    driver_path: &Path,
) -> Vec<String> {
    vec![
        "create".to_owned(),
        "--name".to_owned(),
        container_name.to_owned(),
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
        sandbox_mount(sandbox),
        "--mount".to_owned(),
        format!(
            "type=bind,src={},dst={DRIVER_PATH},readonly",
            driver_path.display()
        ),
    ]
}

fn sandbox_mount(sandbox: &CorrectnessSandbox) -> String {
    format!(
        "type=bind,src={},dst={CONTAINER_BUNDLE_PATH},readonly",
        sandbox.directory.display()
    )
}

fn validate_ceilings(
    ceilings: CorrectnessResourceCeilings,
) -> Result<(), CorrectnessContractError> {
    if ceilings.timeout_ms == 0
        || ceilings.cpu_millis == 0
        || ceilings.memory_bytes == 0
        || ceilings.disk_bytes < MINIMUM_DISK_BYTES
        || ceilings.process_count == 0
        || ceilings.output_bytes == 0
    {
        return Err(CorrectnessContractError::InvalidPolicy(
            "correctness resource ceilings are incomplete",
        ));
    }
    Ok(())
}

fn validate_limits(
    assignment: &StoredAssignment,
    ceilings: CorrectnessResourceCeilings,
) -> Result<(), CorrectnessContractError> {
    let execution = &assignment.execution;
    let limits = execution
        .limits
        .as_ref()
        .ok_or(CorrectnessContractError::Assignment(
            "correctness resource limits are required",
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
        || limits.device_count != 1
        || limits.network != NetworkPolicy::Disabled
    {
        return Err(CorrectnessContractError::Assignment(
            "correctness limits exceed local policy or enable network access",
        ));
    }
    Ok(())
}

fn validate_attempt_id(value: &str) -> Result<(), CorrectnessContractError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CorrectnessContractError::Assignment(
            "attempt ID is unsafe for a correctness sandbox path",
        ));
    }
    Ok(())
}

fn validate_ascend_device(
    device: &AcceleratorDevice,
    environment: &AscendEnvironmentFacts,
) -> Result<(), CorrectnessContractError> {
    if device.device_id.trim().is_empty()
        || device.product_name.trim().is_empty()
        || device.serial_number.trim().is_empty()
        || device.firmware_version.trim().is_empty()
        || device.product_name != environment.architecture
        || device.firmware_version != environment.firmware_version
    {
        return Err(CorrectnessContractError::InvalidPolicy(
            "Ascend device identity does not match the correctness environment",
        ));
    }
    Ok(())
}

fn validate_ascend_nodes(
    device_id: &str,
    nodes: &[PathBuf],
) -> Result<(), CorrectnessContractError> {
    let mut unique = BTreeSet::new();
    for path in nodes {
        let value = path.to_string_lossy();
        let allowed = value.strip_prefix("/dev/davinci").is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        }) || value == "/dev/davinci_manager"
            || value == "/dev/hisi_hdc";
        if !path.is_absolute() || !allowed || !unique.insert(value.into_owned()) {
            return Err(CorrectnessContractError::InvalidPolicy(
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
            return Err(CorrectnessContractError::InvalidPolicy(
                "selected device and manager nodes are required",
            ));
        }
    }
    Ok(())
}

fn create_real_directory(path: &Path) -> Result<(), CorrectnessContractError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(CorrectnessContractError::UnsafePath);
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn create_real_parents(root: &Path, target: &Path) -> Result<(), CorrectnessContractError> {
    let parent = target
        .parent()
        .ok_or(CorrectnessContractError::UnsafePath)?;
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| CorrectnessContractError::UnsafePath)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        create_real_directory(&current)?;
    }
    Ok(())
}

fn write_once(path: &Path, bytes: &[u8]) -> Result<(), CorrectnessContractError> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::symlink_metadata(path)?.file_type().is_symlink() || fs::read(path)? != bytes {
                return Err(CorrectnessContractError::UnsafePath);
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_exact_tree(
    root: &Path,
    bundle: &ReductionExecutionBundle,
) -> Result<(), CorrectnessContractError> {
    let mut expected: BTreeSet<String> = bundle
        .files()
        .iter()
        .map(|file| file.path().as_str().to_owned())
        .collect();
    expected.extend([
        BUNDLE_FILENAME.to_owned(),
        CONFIG_FILENAME.to_owned(),
        RUNNER_FILENAME.to_owned(),
    ]);
    let mut actual = BTreeSet::new();
    scan_tree(root, root, &mut actual)?;
    if actual != expected {
        return Err(CorrectnessContractError::UnsafePath);
    }
    Ok(())
}

fn scan_tree(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<String>,
) -> Result<(), CorrectnessContractError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            return Err(CorrectnessContractError::UnsafePath);
        }
        if kind.is_dir() {
            scan_tree(root, &entry.path(), files)?;
        } else if kind.is_file() {
            files.insert(
                entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| CorrectnessContractError::UnsafePath)?
                    .to_string_lossy()
                    .into_owned(),
            );
        } else {
            return Err(CorrectnessContractError::UnsafePath);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum CorrectnessContractError {
    InvalidPolicy(&'static str),
    Assignment(&'static str),
    Artifact(String),
    Bundle(String),
    WrongBackend,
    UnsafePath,
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl Display for CorrectnessContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(detail) => {
                write!(formatter, "invalid correctness policy: {detail}")
            }
            Self::Assignment(detail) => {
                write!(formatter, "correctness assignment rejected: {detail}")
            }
            Self::Artifact(detail) => write!(formatter, "correctness Artifact error: {detail}"),
            Self::Bundle(detail) => write!(formatter, "invalid correctness bundle: {detail}"),
            Self::WrongBackend => {
                write!(formatter, "correctness policy belongs to another backend")
            }
            Self::UnsafePath => write!(formatter, "correctness sandbox path is unsafe"),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Json(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for CorrectnessContractError {}

impl From<std::io::Error> for CorrectnessContractError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for CorrectnessContractError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[cfg(test)]
#[path = "reduction_correctness_tests.rs"]
mod tests;
