//! Controller-owned Ascend build tool over a pluggable durable attempt port.

use crate::gateway::{
    CandidateToolConfig, adapter_error, ingest_bytes, materialization_error, read_bounded,
};
use crate::materialization::CandidateMaterialization;
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    ASCEND_BUILD_BUNDLE_MEDIA_TYPE, ASCEND_BUILD_FEATURE, ArtifactDescriptor,
    AscendBuildAttemptObservation, AscendBuildAttemptPort, AscendBuildReceipt, AssignmentContract,
    AssignmentId, AttemptId, AttemptOutcome, CandidateBuildBundle, CandidateSourceManifest,
    ExecutionContract, ExecutionKind, NetworkPolicy, ResourceContract, Sha256Digest,
    ToolGatewayError, ToolGatewayOutcome, ToolInvocation, ToolOperationStatus,
    evaluate_source_gate,
};
use serde::Deserialize;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::path::Path;

pub const REQUEST_ASCEND_BUILD_TOOL: &str = "request_ascend_build";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_SOURCE_GATE_RECEIPT_BYTES: u64 = 1024 * 1024;

/// Controller-owned image and resource policy for one build tool composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateBuildToolConfig {
    image: ArtifactDescriptor,
    timeout_ms: u64,
    limits: ResourceContract,
}

impl CandidateBuildToolConfig {
    /// Creates a fixed build policy. No field is accepted from model arguments.
    ///
    /// # Errors
    ///
    /// Returns an error for empty image identity, zero ceilings, network access, or no device.
    pub fn new(
        image: ArtifactDescriptor,
        timeout_ms: u64,
        limits: ResourceContract,
    ) -> Result<Self, CandidateBuildToolConfigError> {
        if image.size_bytes == 0 || image.media_type.trim().is_empty() {
            return Err(CandidateBuildToolConfigError::InvalidImage);
        }
        if timeout_ms == 0
            || limits.cpu_millis == 0
            || limits.memory_bytes == 0
            || limits.disk_bytes == 0
            || limits.process_count == 0
            || limits.output_bytes == 0
            || limits.device_count != 1
        {
            return Err(CandidateBuildToolConfigError::InvalidResourceCeiling);
        }
        if limits.network != NetworkPolicy::Disabled {
            return Err(CandidateBuildToolConfigError::NetworkMustBeDisabled);
        }
        Ok(Self {
            image,
            timeout_ms,
            limits,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateBuildToolConfigError {
    InvalidImage,
    InvalidResourceCeiling,
    NetworkMustBeDisabled,
}

impl Display for CandidateBuildToolConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid candidate build tool config: {self:?}")
    }
}

impl Error for CandidateBuildToolConfigError {}

pub(crate) struct CandidateBuildTool {
    config: CandidateBuildToolConfig,
    attempts: Box<dyn AscendBuildAttemptPort>,
}

impl Debug for CandidateBuildTool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CandidateBuildTool")
            .field("config", &self.config)
            .field("attempts", &self.attempts)
            .finish()
    }
}

impl CandidateBuildTool {
    pub(crate) const fn new(
        config: CandidateBuildToolConfig,
        attempts: Box<dyn AscendBuildAttemptPort>,
    ) -> Self {
        Self { config, attempts }
    }

    pub(crate) async fn execute(
        &mut self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        workspace_root: &Path,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let prepared = match self.prepare(context, artifacts, workspace_root, request)? {
            BuildPreparation::Ready(prepared) => *prepared,
            BuildPreparation::CandidateRejected { result_digest } => {
                return Ok(candidate_rejected(result_digest));
            }
        };
        let observation = self
            .attempts
            .dispatch(&prepared.assignment)
            .await
            .map_err(|error| adapter_error(error.to_string()))?;
        Self::observe(artifacts, prepared, observation)
    }

    pub(crate) async fn reconcile(
        &mut self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        workspace_root: &Path,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let prepared = match self.prepare(context, artifacts, workspace_root, request)? {
            BuildPreparation::Ready(prepared) => *prepared,
            BuildPreparation::CandidateRejected { result_digest } => {
                return Ok(candidate_rejected(result_digest));
            }
        };
        let observation = self
            .attempts
            .reconcile(&prepared.assignment)
            .await
            .map_err(|error| adapter_error(error.to_string()))?;
        Self::observe(artifacts, prepared, observation)
    }

    fn prepare(
        &self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        workspace_root: &Path,
        request: &ToolInvocation,
    ) -> Result<BuildPreparation, ToolGatewayError> {
        let arguments: RequestAscendBuildArguments =
            serde_json::from_slice(&request.call.raw_arguments)
                .map_err(|error| adapter_error(format!("invalid Ascend build request: {error}")))?;
        let manifest_bytes =
            read_bounded(artifacts, arguments.manifest_digest, MAX_MANIFEST_BYTES)?;
        let manifest: CandidateSourceManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| adapter_error(format!("invalid candidate manifest: {error}")))?;
        if !context.matches_manifest(&manifest) {
            return Err(adapter_error(
                "candidate manifest does not belong to this migration context",
            ));
        }
        let materialization = CandidateMaterialization::materialize(
            workspace_root,
            artifacts,
            &manifest,
            arguments.manifest_digest,
        )
        .map_err(|error| materialization_error(&error))?;
        let sources = materialization
            .read_verified_sources(&manifest)
            .map_err(|error| materialization_error(&error))?;
        if let Some(result_digest) = verify_source_gate_receipt(
            artifacts,
            &manifest,
            arguments.manifest_digest,
            arguments.source_gate_receipt_digest,
            &sources,
        )? {
            return Ok(BuildPreparation::CandidateRejected { result_digest });
        }
        let bundle = CandidateBuildBundle::new(
            &manifest,
            arguments.manifest_digest,
            arguments.source_gate_receipt_digest,
            context.target_architecture(),
            &sources,
        )
        .map_err(|error| adapter_error(error.to_string()))?;
        let bytes = serde_json::to_vec(&bundle)
            .map_err(|error| adapter_error(format!("cannot encode build bundle: {error}")))?;
        let stored = ingest_bytes(artifacts, &bytes)?;
        let bundle_artifact = ArtifactDescriptor {
            digest: stored.digest,
            size_bytes: stored.size_bytes,
            media_type: ASCEND_BUILD_BUNDLE_MEDIA_TYPE.to_owned(),
        };
        let assignment = self.assignment(request, &bundle, bundle_artifact)?;
        Ok(BuildPreparation::Ready(Box::new(PreparedBuild {
            bundle,
            assignment,
        })))
    }

    fn assignment(
        &self,
        request: &ToolInvocation,
        bundle: &CandidateBuildBundle,
        bundle_artifact: ArtifactDescriptor,
    ) -> Result<AssignmentContract, ToolGatewayError> {
        let mut identity = b"alloyport-ascend-build-attempt-v1\0".to_vec();
        identity.extend_from_slice(request.operation_id.as_str().as_bytes());
        identity.extend_from_slice(&bundle_artifact.digest.bytes());
        identity.extend_from_slice(&self.config.image.digest.bytes());
        let digest = Sha256Digest::digest_bytes(&identity).hexadecimal();
        Ok(AssignmentContract {
            assignment_id: AssignmentId::try_from(format!("assignment-build-{digest}"))
                .map_err(|error| adapter_error(error.to_string()))?,
            attempt_id: AttemptId::try_from(format!("attempt-build-{digest}"))
                .map_err(|error| adapter_error(error.to_string()))?,
            attempt_number: 1,
            idempotency_key: format!("ascend-build:{}", request.operation_id),
            task_id: bundle.task_id().clone(),
            candidate_id: bundle.candidate_id().clone(),
            execution: ExecutionContract {
                executor_kind: ExecutionKind::AscendBuild,
                argv: vec!["build-v1".to_owned()],
                working_directory: ".".to_owned(),
                environment: Vec::new(),
                timeout_ms: self.config.timeout_ms,
                bundle: bundle_artifact,
                image: self.config.image.clone(),
                limits: Some(self.config.limits.clone()),
            },
            required_features: vec![ASCEND_BUILD_FEATURE.to_owned()],
        })
    }

    fn observe(
        artifacts: &dyn ArtifactStore,
        prepared: PreparedBuild,
        observation: AscendBuildAttemptObservation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let terminal = match observation {
            AscendBuildAttemptObservation::Pending { diagnostic_digest } => {
                return Ok(ToolGatewayOutcome::Pending { diagnostic_digest });
            }
            AscendBuildAttemptObservation::Finished(terminal) => terminal,
        };
        let receipt = AscendBuildReceipt::new(&prepared.bundle, prepared.assignment, *terminal)
            .map_err(|error| adapter_error(error.to_string()))?;
        let passed = receipt.passed();
        let status = if passed {
            ToolOperationStatus::Succeeded
        } else {
            match receipt.outcome() {
                AttemptOutcome::CandidateFailed => ToolOperationStatus::CandidateFailed,
                AttemptOutcome::TimedOut => ToolOperationStatus::TimedOut,
                AttemptOutcome::Cancelled => ToolOperationStatus::Cancelled,
                AttemptOutcome::Succeeded
                | AttemptOutcome::InfraError
                | AttemptOutcome::IntegrityViolation => ToolOperationStatus::InfraFailed,
            }
        };
        let bytes = serde_json::to_vec(&receipt)
            .map_err(|error| adapter_error(format!("cannot encode Build Gate receipt: {error}")))?;
        let stored = ingest_bytes(artifacts, &bytes)?;
        Ok(ToolGatewayOutcome::Completed {
            status,
            result_digest: stored.digest,
            receipt_digests: vec![stored.digest],
            satisfies_subtask: passed,
        })
    }
}

/// Confirms the model cited the receipt this exact tree actually produces.
///
/// A candidate whose Source Gate found something blocking is a defect the model can read and fix,
/// not an infrastructure failure. It publishes the receipt and returns its digest so the caller can
/// hand the model a terminal candidate failure and keep the migration alive.
fn verify_source_gate_receipt(
    artifacts: &dyn ArtifactStore,
    manifest: &CandidateSourceManifest,
    manifest_digest: Sha256Digest,
    receipt_digest: Sha256Digest,
    sources: &std::collections::BTreeMap<alloyport_core::BundlePath, Vec<u8>>,
) -> Result<Option<Sha256Digest>, ToolGatewayError> {
    let supplied = read_bounded(artifacts, receipt_digest, MAX_SOURCE_GATE_RECEIPT_BYTES)?;
    let expected = evaluate_source_gate(manifest, manifest_digest, sources);
    if !expected.passed() {
        let bytes = serde_json::to_vec(&expected).map_err(|error| {
            adapter_error(format!("cannot encode Source Gate receipt: {error}"))
        })?;
        return crate::gateway::ingest_bytes(artifacts, &bytes)
            .map(|artifact| Some(artifact.digest));
    }
    let expected = serde_json::to_vec(&expected)
        .map_err(|error| adapter_error(format!("cannot encode Source Gate receipt: {error}")))?;
    if supplied != expected || Sha256Digest::digest_bytes(&supplied) != receipt_digest {
        return Err(adapter_error(
            alloyport_core::CandidateBuildError::SourceGateReceiptMismatch.to_string(),
        ));
    }
    Ok(None)
}

fn candidate_rejected(result_digest: Sha256Digest) -> ToolGatewayOutcome {
    ToolGatewayOutcome::Completed {
        status: ToolOperationStatus::CandidateFailed,
        result_digest,
        receipt_digests: vec![result_digest],
        satisfies_subtask: false,
    }
}

enum BuildPreparation {
    Ready(Box<PreparedBuild>),
    CandidateRejected { result_digest: Sha256Digest },
}

struct PreparedBuild {
    bundle: CandidateBuildBundle,
    assignment: AssignmentContract,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestAscendBuildArguments {
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
}

pub(crate) const REQUEST_ASCEND_BUILD_ARGUMENT_CONTRACT: &str =
    "{\"manifest_digest\": \"sha256:...\", \"source_gate_receipt_digest\": \"sha256:...\"}";

/// Decodes the arguments without any effect, so a malformed call can be returned to the model.
pub(crate) fn check_ascend_build_arguments(raw: &[u8]) -> Result<(), serde_json::Error> {
    serde_json::from_slice::<RequestAscendBuildArguments>(raw).map(|_| ())
}
