//! Versioned worker-control and artifact protocols plus RPC-boundary validation.

use alloyport_core::Sha256Digest;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::{Component, Path};

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::large_enum_variant,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines
)]
pub mod v1 {
    tonic::include_proto!("alloyport.worker.v1");
}

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::large_enum_variant,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines
)]
pub mod artifact_v1 {
    tonic::include_proto!("alloyport.artifact.v1");
}

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::large_enum_variant,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines
)]
pub mod interaction_v1 {
    tonic::include_proto!("alloyport.interaction.v1");
}

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::large_enum_variant,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines
)]
pub mod management_v1 {
    tonic::include_proto!("alloyport.management.v1");
}

pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 7;
/// Maximum encoded worker-to-server control frame accepted by the service.
pub const MAX_WORKER_TO_SERVER_MESSAGE_BYTES: usize = 128 * 1024;
/// Maximum encoded server-to-worker control frame accepted by the worker.
pub const MAX_SERVER_TO_WORKER_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum raw best-effort output preview carried by one control frame.
pub const MAX_OUTPUT_PREVIEW_CHUNK_BYTES: usize = 64 * 1024;
/// Maximum encoded Interaction request accepted by the service.
pub const MAX_INTERACTION_REQUEST_MESSAGE_BYTES: usize = 64 * 1024;
/// Maximum encoded canonical Interaction event, including worst-case JSON text escaping.
pub const MAX_INTERACTION_EVENT_MESSAGE_BYTES: usize = 512 * 1024;
/// Maximum encoded CLI management request, including one submitted project bundle.
pub const MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum encoded CLI management response.
pub const MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// Fixed byte count read into one Artifact download response.
pub const ARTIFACT_DOWNLOAD_CHUNK_BYTES: usize = 64 * 1024;
/// Conservative framing allowance added to bounded Protobuf byte payloads.
pub const PROTOBUF_MESSAGE_OVERHEAD_BYTES: usize = 64 * 1024;
/// Maximum encoded Artifact download response accepted by a worker.
pub const MAX_ARTIFACT_DOWNLOAD_MESSAGE_BYTES: usize =
    ARTIFACT_DOWNLOAD_CHUNK_BYTES + PROTOBUF_MESSAGE_OVERHEAD_BYTES;

/// Why an incoming wire message cannot enter the `AlloyPort` domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    field: &'static str,
    detail: &'static str,
}

impl ValidationError {
    const fn new(field: &'static str, detail: &'static str) -> Self {
        Self { field, detail }
    }

    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl Display for ValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid {}: {}", self.field, self.detail)
    }
}

impl Error for ValidationError {}

/// Validates bounded payload invariants for one worker-to-server control frame.
///
/// # Errors
///
/// Returns [`ValidationError`] when a best-effort output preview exceeds the wire contract.
pub fn validate_worker_frame(frame: &v1::WorkerToServer) -> Result<(), ValidationError> {
    if let Some(v1::worker_to_server::Message::OutputChunk(output)) = frame.message.as_ref()
        && output.payload.len() > MAX_OUTPUT_PREVIEW_CHUNK_BYTES
    {
        return Err(ValidationError::new(
            "output_chunk.payload",
            "exceeds the per-frame preview limit",
        ));
    }
    Ok(())
}

/// Validates identity, protocol and scheduling capability in the first worker message.
///
/// # Errors
///
/// Returns [`ValidationError`] for unsupported versions, absent identities or unusable capacity.
pub fn validate_worker_hello(hello: &v1::WorkerHello) -> Result<(), ValidationError> {
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err(ValidationError::new(
            "hello.protocol_major",
            "unsupported major version",
        ));
    }
    require_text("hello.worker_id", &hello.worker_id)?;
    require_text("hello.instance_id", &hello.instance_id)?;
    require_text("hello.worker_version", &hello.worker_version)?;
    let capabilities = hello
        .capabilities
        .as_ref()
        .ok_or_else(|| ValidationError::new("hello.capabilities", "missing"))?;
    if v1::Backend::try_from(capabilities.backend).unwrap_or(v1::Backend::Unspecified)
        == v1::Backend::Unspecified
    {
        return Err(ValidationError::new(
            "hello.capabilities.backend",
            "unspecified or unknown",
        ));
    }
    if capabilities.device_count == 0 {
        return Err(ValidationError::new(
            "hello.capabilities.device_count",
            "must be greater than zero",
        ));
    }
    if capabilities.max_concurrency == 0 {
        return Err(ValidationError::new(
            "hello.capabilities.max_concurrency",
            "must be greater than zero",
        ));
    }
    validate_accelerator_devices(capabilities)?;
    if hello.features.iter().any(|feature| {
        matches!(
            feature.as_str(),
            "ascend-fixture-v1" | "ascend-build-v1" | "ascend-reduction-correctness-v1"
        )
    }) {
        if v1::Backend::try_from(capabilities.backend).unwrap_or(v1::Backend::Unspecified)
            != v1::Backend::Ascend
        {
            return Err(ValidationError::new(
                "hello.features",
                "fixed Ascend features require the Ascend backend",
            ));
        }
        if capabilities.devices.len() != capabilities.device_count as usize {
            return Err(ValidationError::new(
                "hello.capabilities.devices",
                "fixed Ascend workers must identify every advertised device",
            ));
        }
    }
    if hello
        .features
        .iter()
        .any(|feature| feature == "cuda-reduction-correctness-v1")
        && v1::Backend::try_from(capabilities.backend).unwrap_or(v1::Backend::Unspecified)
            != v1::Backend::Cuda
    {
        return Err(ValidationError::new(
            "hello.features",
            "cuda-reduction-correctness-v1 requires the CUDA backend",
        ));
    }
    Ok(())
}

fn validate_accelerator_devices(
    capabilities: &v1::WorkerCapabilities,
) -> Result<(), ValidationError> {
    if !capabilities.devices.is_empty()
        && capabilities.devices.len() != capabilities.device_count as usize
    {
        return Err(ValidationError::new(
            "hello.capabilities.devices",
            "must be empty or match device_count",
        ));
    }
    let mut device_ids = BTreeSet::new();
    let mut serial_numbers = BTreeSet::new();
    for device in &capabilities.devices {
        require_text("hello.capabilities.devices.device_id", &device.device_id)?;
        require_text(
            "hello.capabilities.devices.product_name",
            &device.product_name,
        )?;
        require_text(
            "hello.capabilities.devices.serial_number",
            &device.serial_number,
        )?;
        require_text(
            "hello.capabilities.devices.firmware_version",
            &device.firmware_version,
        )?;
        if !device_ids.insert(device.device_id.as_str()) {
            return Err(ValidationError::new(
                "hello.capabilities.devices.device_id",
                "duplicate",
            ));
        }
        if !serial_numbers.insert(device.serial_number.as_str()) {
            return Err(ValidationError::new(
                "hello.capabilities.devices.serial_number",
                "duplicate",
            ));
        }
    }
    Ok(())
}

/// Validates one ephemeral scheduling snapshot before it enters server application code.
///
/// # Errors
///
/// Returns [`ValidationError`] for unknown health, duplicate identity, impossible counters, or an
/// invalid durable lease identity.
pub fn validate_heartbeat(heartbeat: &v1::Heartbeat) -> Result<(), ValidationError> {
    if v1::WorkerHealth::try_from(heartbeat.health).unwrap_or(v1::WorkerHealth::Unspecified)
        == v1::WorkerHealth::Unspecified
    {
        return Err(ValidationError::new(
            "heartbeat.health",
            "unspecified or unknown",
        ));
    }
    let mut observed_devices = BTreeSet::new();
    for device in &heartbeat.devices {
        require_text("heartbeat.devices.device_id", &device.device_id)?;
        if !observed_devices.insert(device.device_id.as_str()) {
            return Err(ValidationError::new(
                "heartbeat.devices.device_id",
                "duplicate",
            ));
        }
        if v1::DeviceHealth::try_from(device.health).unwrap_or(v1::DeviceHealth::Unspecified)
            == v1::DeviceHealth::Unspecified
        {
            return Err(ValidationError::new(
                "heartbeat.devices.health",
                "unspecified or unknown",
            ));
        }
        if device.utilization_percent > 100 {
            return Err(ValidationError::new(
                "heartbeat.devices.utilization_percent",
                "must not exceed 100",
            ));
        }
        if device.memory_used_bytes > device.memory_total_bytes {
            return Err(ValidationError::new(
                "heartbeat.devices.memory_used_bytes",
                "must not exceed total memory",
            ));
        }
        if device.detail.len() > 1_024 {
            return Err(ValidationError::new(
                "heartbeat.devices.detail",
                "exceeds 1024 bytes",
            ));
        }
    }
    let mut leased_attempts = BTreeSet::new();
    let mut leased_devices = BTreeSet::new();
    for lease in &heartbeat.device_leases {
        require_text("heartbeat.device_leases.attempt_id", &lease.attempt_id)?;
        require_text("heartbeat.device_leases.device_id", &lease.device_id)?;
        if !leased_attempts.insert(lease.attempt_id.as_str()) {
            return Err(ValidationError::new(
                "heartbeat.device_leases.attempt_id",
                "duplicate",
            ));
        }
        if !leased_devices.insert(lease.device_id.as_str()) {
            return Err(ValidationError::new(
                "heartbeat.device_leases.device_id",
                "duplicate active device lease",
            ));
        }
    }
    Ok(())
}

/// Validates an assignment before either the server queues it or a worker admits it.
///
/// # Errors
///
/// Returns [`ValidationError`] when identity, executor, sandbox path or artifact requirements fail.
pub fn validate_assignment(assignment: &v1::Assignment) -> Result<(), ValidationError> {
    require_text("assignment.assignment_id", &assignment.assignment_id)?;
    require_text("assignment.attempt_id", &assignment.attempt_id)?;
    require_text("assignment.idempotency_key", &assignment.idempotency_key)?;
    require_text("assignment.task_id", &assignment.task_id)?;
    require_text("assignment.candidate_id", &assignment.candidate_id)?;

    let execution = assignment
        .execution
        .as_ref()
        .ok_or_else(|| ValidationError::new("assignment.execution", "missing"))?;
    if v1::ExecutorKind::try_from(execution.executor_kind).unwrap_or(v1::ExecutorKind::Unspecified)
        == v1::ExecutorKind::Unspecified
    {
        return Err(ValidationError::new(
            "assignment.execution.executor_kind",
            "unspecified or unknown",
        ));
    }
    if execution.argv.is_empty() || execution.argv[0].is_empty() {
        return Err(ValidationError::new(
            "assignment.execution.argv",
            "must contain a non-empty executable",
        ));
    }
    validate_sandbox_path(&execution.working_directory)?;
    validate_artifact("assignment.execution.bundle", execution.bundle.as_ref())?;
    validate_artifact("assignment.execution.image", execution.image.as_ref())?;
    Ok(())
}

fn validate_sandbox_path(path: &str) -> Result<(), ValidationError> {
    if path.is_empty() {
        return Err(ValidationError::new(
            "assignment.execution.working_directory",
            "missing",
        ));
    }
    if Path::new(path).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(ValidationError::new(
            "assignment.execution.working_directory",
            "must stay relative to the sandbox",
        ));
    }
    Ok(())
}

fn validate_artifact(
    field: &'static str,
    artifact: Option<&v1::ArtifactRef>,
) -> Result<(), ValidationError> {
    let artifact = artifact.ok_or_else(|| ValidationError::new(field, "missing"))?;
    artifact
        .digest
        .parse::<Sha256Digest>()
        .map_err(|_| ValidationError::new(field, "digest must be canonical SHA-256"))?;
    Ok(())
}

fn require_text(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        Err(ValidationError::new(field, "missing"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(device_id: &str, serial_number: &str) -> v1::AcceleratorDevice {
        v1::AcceleratorDevice {
            device_id: device_id.to_owned(),
            product_name: "Ascend950PR".to_owned(),
            serial_number: serial_number.to_owned(),
            firmware_version: "9.0.0.105.229".to_owned(),
        }
    }

    fn artifact(byte: char) -> v1::ArtifactRef {
        v1::ArtifactRef {
            digest: format!("sha256:{}", byte.to_string().repeat(64)),
            size_bytes: 1,
            media_type: "application/octet-stream".to_owned(),
        }
    }

    fn assignment() -> v1::Assignment {
        v1::Assignment {
            assignment_id: "assignment-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            attempt_number: 1,
            idempotency_key: "candidate-1:build".to_owned(),
            task_id: "task-1".to_owned(),
            candidate_id: "candidate-1".to_owned(),
            execution: Some(v1::ExecutionSpec {
                executor_kind: v1::ExecutorKind::Container.into(),
                argv: vec!["cmake".to_owned(), "--build".to_owned(), "build".to_owned()],
                working_directory: "source".to_owned(),
                environment: Vec::new(),
                timeout_ms: 30_000,
                bundle: Some(artifact('a')),
                image: Some(artifact('b')),
                limits: None,
            }),
            required_features: Vec::new(),
        }
    }

    #[test]
    fn accepts_typed_sandboxed_assignment() {
        assert_eq!(validate_assignment(&assignment()), Ok(()));
    }

    #[test]
    fn rejects_assignment_without_candidate_identity() {
        let mut assignment = assignment();
        assignment.candidate_id = "  ".to_owned();

        let error = validate_assignment(&assignment).expect_err("candidate identity is required");
        assert_eq!(error.field(), "assignment.candidate_id");
    }

    #[test]
    fn rejects_host_path_escape() {
        let mut assignment = assignment();
        assignment
            .execution
            .as_mut()
            .expect("fixture has execution")
            .working_directory = "../host".to_owned();

        let error = validate_assignment(&assignment).expect_err("parent traversal must fail");
        assert_eq!(error.field(), "assignment.execution.working_directory");
    }

    #[test]
    fn rejects_digest_with_non_hex_bytes() {
        let mut assignment = assignment();
        assignment
            .execution
            .as_mut()
            .expect("fixture has execution")
            .bundle
            .as_mut()
            .expect("fixture has bundle")
            .digest = format!("sha256:{}z", "a".repeat(63));

        let error = validate_assignment(&assignment).expect_err("invalid digest must fail");
        assert_eq!(error.field(), "assignment.execution.bundle");
    }

    #[test]
    fn fixed_ascend_hello_requires_complete_unique_device_identity() {
        let mut hello = v1::WorkerHello {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            worker_id: "ascend-1".to_owned(),
            instance_id: "boot-1".to_owned(),
            worker_version: "0.1.0".to_owned(),
            features: vec!["ascend-fixture-v1".to_owned()],
            capabilities: Some(v1::WorkerCapabilities {
                backend: v1::Backend::Ascend.into(),
                architecture: "Ascend950PR".to_owned(),
                device_count: 2,
                max_concurrency: 2,
                driver_version: "25.7.rc1.6".to_owned(),
                toolkit_version: "9.1.0-beta.1".to_owned(),
                container_runtime: "docker".to_owned(),
                devices: vec![device("0", "serial-0"), device("1", "serial-1")],
            }),
            active_attempts: Vec::new(),
        };
        assert_eq!(validate_worker_hello(&hello), Ok(()));

        hello
            .capabilities
            .as_mut()
            .expect("fixture capabilities")
            .devices[1]
            .device_id = "0".to_owned();
        assert_eq!(
            validate_worker_hello(&hello)
                .expect_err("duplicate device identities must fail")
                .field(),
            "hello.capabilities.devices.device_id"
        );
    }

    #[test]
    fn correctness_features_are_bound_to_their_accelerator_backend() {
        let mut hello = v1::WorkerHello {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            worker_id: "cuda-correctness-1".to_owned(),
            instance_id: "boot-1".to_owned(),
            worker_version: "0.1.0".to_owned(),
            features: vec!["cuda-reduction-correctness-v1".to_owned()],
            capabilities: Some(v1::WorkerCapabilities {
                backend: v1::Backend::Cuda.into(),
                architecture: "sm_90".to_owned(),
                device_count: 1,
                max_concurrency: 1,
                driver_version: "1".to_owned(),
                toolkit_version: "1".to_owned(),
                container_runtime: "docker".to_owned(),
                devices: Vec::new(),
            }),
            active_attempts: Vec::new(),
        };
        assert_eq!(validate_worker_hello(&hello), Ok(()));
        hello.capabilities.as_mut().expect("capabilities").backend = v1::Backend::Ascend.into();
        assert_eq!(
            validate_worker_hello(&hello)
                .expect_err("CUDA correctness cannot be advertised by Ascend")
                .field(),
            "hello.features"
        );
    }

    #[test]
    fn heartbeat_keeps_health_occupancy_and_leases_distinct() {
        let heartbeat = v1::Heartbeat {
            active_attempts: Vec::new(),
            available_slots: 0,
            device_free_slots: 0,
            health: v1::WorkerHealth::Degraded.into(),
            devices: vec![v1::DeviceObservation {
                device_id: "0".to_owned(),
                health: v1::DeviceHealth::Unhealthy.into(),
                process_count: 0,
                utilization_percent: 0,
                memory_used_bytes: 5_249,
                memory_total_bytes: 131_072,
                temperature_millicelsius: 65_000,
                power_milliwatts: 207_600,
                observed_at_ms: 1_000,
                detail: "npu-smi reported Alarm".to_owned(),
            }],
            device_leases: vec![v1::DeviceLease {
                attempt_id: "attempt-1".to_owned(),
                device_id: "0".to_owned(),
                acquired_at_ms: 900,
            }],
        };
        assert_eq!(validate_heartbeat(&heartbeat), Ok(()));

        let mut impossible = heartbeat;
        impossible.devices[0].utilization_percent = 101;
        assert_eq!(
            validate_heartbeat(&impossible)
                .expect_err("impossible utilization must fail")
                .field(),
            "heartbeat.devices.utilization_percent"
        );
    }

    #[test]
    fn worker_output_preview_has_a_protocol_level_chunk_limit() {
        let mut frame = v1::WorkerToServer {
            sequence: 2,
            acknowledges_server_through: 1,
            message_id: String::new(),
            message: Some(v1::worker_to_server::Message::OutputChunk(
                v1::OutputChunk {
                    attempt_id: "attempt-1".to_owned(),
                    stream: v1::OutputStream::Stdout.into(),
                    byte_offset: 0,
                    payload: vec![0; MAX_OUTPUT_PREVIEW_CHUNK_BYTES],
                    display_sanitized: false,
                },
            )),
        };
        assert_eq!(validate_worker_frame(&frame), Ok(()));

        let Some(v1::worker_to_server::Message::OutputChunk(output)) = frame.message.as_mut()
        else {
            panic!("fixture carries output");
        };
        output.payload.push(0);
        assert_eq!(
            validate_worker_frame(&frame)
                .expect_err("oversized preview must fail")
                .field(),
            "output_chunk.payload"
        );
    }
}
