//! Versioned worker-control and artifact protocols plus RPC-boundary validation.

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

pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 3;

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
    if !artifact.digest.starts_with("sha256:") || artifact.digest.len() != 71 {
        return Err(ValidationError::new(
            field,
            "digest must be sha256 followed by 64 hexadecimal characters",
        ));
    }
    if !artifact.digest[7..]
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ValidationError::new(field, "digest contains non-hex data"));
    }
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
}
