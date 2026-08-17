use crate::build_tool::{
    CandidateBuildTool, CandidateBuildToolConfig, READ_BUILD_DIAGNOSTICS_TOOL,
    REQUEST_ASCEND_BUILD_TOOL,
};
use crate::correctness_tool::{
    CandidateCorrectnessTool, CandidateCorrectnessToolConfig, REQUEST_REDUCTION_CORRECTNESS_TOOL,
};
use crate::materialization::{CandidateMaterialization, CandidateMaterializationError};
use crate::reference::{READ_REFERENCE_TOOL, ReferenceCorpus};
use alloyport_artifacts::{ArtifactStore, ArtifactStoreError, IngestRequest};
use alloyport_core::{
    AgentToolGateway, BundlePath, CandidateSourceManifest, GatewayToolCall, GenerationStrategy,
    MigrationSpec, RuntimeToolDescriptor, Sha256Digest, SourceGateReceipt, TaskId, ToolEffectClass,
    ToolGatewayError, ToolGatewayFuture, ToolGatewayOutcome, ToolInputRejection, ToolInvocation,
    ToolOperationStatus, ToolResultAuthority, evaluate_source_gate,
};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[path = "gateway_recovery.rs"]
mod recovery;
#[path = "gateway_submit.rs"]
mod submit;

pub const SUBMIT_CANDIDATE_BUNDLE_TOOL: &str = "submit_candidate_bundle";
pub const REQUEST_SOURCE_GATE_TOOL: &str = "request_source_gate";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;

/// Immutable migration facts injected by the controller, never accepted from model arguments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateToolConfig {
    task_id: TaskId,
    migration_spec_digest: Sha256Digest,
    generation_strategy: GenerationStrategy,
    public_symbol: String,
    build_target: String,
    input_source_paths: BTreeSet<BundlePath>,
    target_architecture: String,
}

impl CandidateToolConfig {
    #[must_use]
    pub fn new(
        task_id: TaskId,
        migration_spec: &MigrationSpec,
        generation_strategy: GenerationStrategy,
    ) -> Self {
        let input_source_paths = migration_spec
            .sources()
            .device_sources()
            .iter()
            .chain(migration_spec.sources().host_sources())
            .chain(migration_spec.sources().build_files())
            .cloned()
            .collect();
        Self {
            task_id,
            migration_spec_digest: migration_spec.digest(),
            generation_strategy,
            public_symbol: migration_spec.public_entry().symbol().to_owned(),
            build_target: migration_spec.public_entry().build_target().to_owned(),
            input_source_paths,
            target_architecture: migration_spec.target().soc().to_owned(),
        }
    }

    pub(crate) fn matches_manifest(&self, manifest: &CandidateSourceManifest) -> bool {
        manifest.matches_context(
            &self.task_id,
            self.migration_spec_digest,
            self.generation_strategy,
            &self.public_symbol,
            &self.build_target,
            &self.input_source_paths,
        )
    }

    pub(crate) fn target_architecture(&self) -> &str {
        &self.target_architecture
    }

    pub(crate) const fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    pub(crate) const fn migration_spec_digest(&self) -> Sha256Digest {
        self.migration_spec_digest
    }
}

/// Real local Agent tool adapter for candidate submission and the structural Source Gate.
pub struct CandidateToolGateway {
    config: CandidateToolConfig,
    artifacts: Arc<dyn ArtifactStore>,
    workspace_root: PathBuf,
    build: Option<CandidateBuildTool>,
    correctness: Option<CandidateCorrectnessTool>,
    reference: Option<ReferenceCorpus>,
}

impl Debug for CandidateToolGateway {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CandidateToolGateway")
            .field("config", &self.config)
            .field("workspace_root", &self.workspace_root)
            .field("build_enabled", &self.build.is_some())
            .field("correctness_enabled", &self.correctness.is_some())
            .finish_non_exhaustive()
    }
}

impl CandidateToolGateway {
    /// Creates a gateway rooted in an existing controller-owned candidate directory.
    ///
    /// # Errors
    ///
    /// Returns an error unless the workspace root is a real absolute directory.
    pub fn new(
        config: CandidateToolConfig,
        artifacts: Arc<dyn ArtifactStore>,
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, ToolGatewayError> {
        let workspace_root = workspace_root.as_ref();
        if !workspace_root.is_absolute() {
            return Err(adapter_error("candidate workspace root must be absolute"));
        }
        let metadata = std::fs::symlink_metadata(workspace_root).map_err(|error| {
            adapter_error(format!("cannot inspect candidate workspace: {error}"))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(adapter_error(
                "candidate workspace root must be a real directory",
            ));
        }
        let workspace_root = std::fs::canonicalize(workspace_root).map_err(|error| {
            adapter_error(format!("cannot resolve candidate workspace: {error}"))
        })?;
        Ok(Self {
            config,
            artifacts,
            workspace_root,
            build: None,
            correctness: None,
            reference: None,
        })
    }

    /// Serves the vendored reference corpus, each document carrying its trust state.
    #[must_use]
    pub fn with_reference(mut self, corpus: ReferenceCorpus) -> Self {
        self.reference = Some(corpus);
        self
    }

    /// Enables the remote Ascend build tool without changing source-tool behavior.
    #[must_use]
    pub fn with_ascend_build(
        mut self,
        config: CandidateBuildToolConfig,
        attempts: Box<dyn alloyport_core::AscendBuildAttemptPort>,
    ) -> Self {
        self.build = Some(CandidateBuildTool::new(config, attempts));
        self
    }

    /// Enables the independent reduction Correctness Gate downstream of Build Gate.
    #[must_use]
    pub fn with_reduction_correctness(
        mut self,
        config: CandidateCorrectnessToolConfig,
        attempts: Box<dyn alloyport_core::ReductionCorrectnessAttemptPort>,
    ) -> Self {
        self.correctness = Some(CandidateCorrectnessTool::new(config, attempts));
        self
    }

    fn source_gate(
        &self,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let arguments: SourceGateArguments = serde_json::from_slice(&request.call.raw_arguments)
            .map_err(|error| adapter_error(format!("invalid Source Gate request: {error}")))?;
        let manifest_bytes = read_bounded(
            self.artifacts.as_ref(),
            arguments.manifest_digest,
            MAX_MANIFEST_BYTES,
        )?;
        let manifest: CandidateSourceManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| citation_error(format!("invalid candidate manifest: {error}")))?;
        if !manifest.matches_context(
            &self.config.task_id,
            self.config.migration_spec_digest,
            self.config.generation_strategy,
            &self.config.public_symbol,
            &self.config.build_target,
            &self.config.input_source_paths,
        ) {
            return Err(citation_error(
                "candidate manifest does not belong to this migration context",
            ));
        }
        let materialization = CandidateMaterialization::materialize(
            &self.workspace_root,
            self.artifacts.as_ref(),
            &manifest,
            arguments.manifest_digest,
        )
        .map_err(|error| materialization_error(&error))?;
        let sources = materialization
            .read_verified_sources(&manifest)
            .map_err(|error| materialization_error(&error))?;
        let receipt = evaluate_source_gate(&manifest, arguments.manifest_digest, &sources);
        self.persist_gate_receipt(&receipt)
    }

    fn persist_gate_receipt(
        &self,
        receipt: &SourceGateReceipt,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let passed = receipt.passed();
        let bytes = serde_json::to_vec(&receipt).map_err(|error| {
            adapter_error(format!("cannot encode Source Gate receipt: {error}"))
        })?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_RECEIPT_BYTES {
            return Err(adapter_error("Source Gate receipt exceeded its bound"));
        }
        // The model reads the wrapper and cites `receipt.digest` on the next call. Publishing the
        // bare receipt as the result would ask it to name this artifact's own digest, which no
        // document can contain.
        let published = crate::gate_result::publish_gate_result(
            self.artifacts.as_ref(),
            &bytes,
            crate::gate_result::SOURCE_GATE_RECEIPT_MEDIA_TYPE,
        )?;
        Ok(ToolGatewayOutcome::Completed {
            status: if passed {
                ToolOperationStatus::Succeeded
            } else {
                ToolOperationStatus::CandidateFailed
            },
            result_digest: published.result_digest,
            receipt_digests: vec![published.receipt_digest],
            satisfies_subtask: passed && self.build.is_none() && self.correctness.is_none(),
        })
    }

    fn invoke(&self, request: &ToolInvocation) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let outcome = match request.call.name.as_str() {
            SUBMIT_CANDIDATE_BUNDLE_TOOL => self.submit(request),
            REQUEST_SOURCE_GATE_TOOL => self.source_gate(request),
            READ_REFERENCE_TOOL => self.as_instrument(
                request,
                crate::reference::read_reference(
                    self.reference
                        .as_ref()
                        .ok_or(ToolGatewayError::UnexpectedRequest)?,
                    self.artifacts.as_ref(),
                    request,
                ),
            ),
            READ_BUILD_DIAGNOSTICS_TOOL => self.as_instrument(
                request,
                crate::build_tool::read_build_diagnostics(
                    &self.config,
                    self.artifacts.as_ref(),
                    request,
                ),
            ),
            _ => Err(ToolGatewayError::UnexpectedRequest),
        };
        self.recover_citations(request, outcome)
    }
}

pub(super) const SOURCE_GATE_ARGUMENT_CONTRACT: &str = r#"{"manifest_digest": "sha256:..."}"#;

impl AgentToolGateway for CandidateToolGateway {
    fn validate_call(&self, call: &GatewayToolCall) -> Result<(), ToolInputRejection> {
        let (reason, contract) = if self.descriptor(&call.name).is_none() {
            (
                format!("tool {} is not available in this migration", call.name),
                "",
            )
        } else {
            match recovery::check_arguments(&call.name, &call.raw_arguments)
                .and_then(|()| self.check_referents(call))
            {
                Ok(()) => return Ok(()),
                Err(rejection) => rejection,
            }
        };
        let diagnostic = format!("{}: {reason}", call.name);
        let explanation = serde_json::json!({
            "rejected": true,
            "tool": call.name,
            "reason": reason,
            "expected_arguments": contract,
            "recoverable": true,
            "guidance": recovery::guidance_for(&call.name, &reason),
        });
        // A rejection the model cannot read is worse than no rejection: the controller opens this
        // digest to build the next model input. If publication fails the store itself is broken,
        // which is an infrastructure failure and not the model's to correct, so the call proceeds
        // and surfaces that failure through the normal adapter path.
        let Ok(bytes) = serde_json::to_vec(&explanation) else {
            return Ok(());
        };
        match ingest_bytes(self.artifacts.as_ref(), &bytes) {
            Ok(artifact) => Err(ToolInputRejection {
                result_digest: artifact.digest,
                diagnostic,
            }),
            Err(_) => Ok(()),
        }
    }

    fn descriptor(&self, name: &str) -> Option<RuntimeToolDescriptor> {
        match name {
            SUBMIT_CANDIDATE_BUNDLE_TOOL => Some(RuntimeToolDescriptor {
                name: name.to_owned(),
                version: "1".to_owned(),
                effect_class: ToolEffectClass::CandidateWrite,
                result_authority: ToolResultAuthority::Observed,
            }),
            REQUEST_SOURCE_GATE_TOOL => Some(RuntimeToolDescriptor {
                name: name.to_owned(),
                version: "1".to_owned(),
                effect_class: ToolEffectClass::ReadOnly,
                result_authority: ToolResultAuthority::VerifiedReference,
            }),
            // An instrument, not a gate: it returns information the pipeline already produced and
            // grants no authority, so its result is `Observed` and cannot satisfy a subtask.
            READ_REFERENCE_TOOL if self.reference.is_some() => {
                Some(crate::reference::descriptor(name))
            }
            READ_BUILD_DIAGNOSTICS_TOOL if self.build.is_some() => Some(RuntimeToolDescriptor {
                name: name.to_owned(),
                version: "1".to_owned(),
                effect_class: ToolEffectClass::ReadOnly,
                result_authority: ToolResultAuthority::Observed,
            }),
            REQUEST_ASCEND_BUILD_TOOL if self.build.is_some() => Some(RuntimeToolDescriptor {
                name: name.to_owned(),
                version: "1".to_owned(),
                effect_class: ToolEffectClass::RemoteExecution,
                result_authority: ToolResultAuthority::VerifiedReference,
            }),
            REQUEST_REDUCTION_CORRECTNESS_TOOL if self.correctness.is_some() => {
                Some(RuntimeToolDescriptor {
                    name: name.to_owned(),
                    version: "1".to_owned(),
                    effect_class: ToolEffectClass::AuthorityRequest,
                    result_authority: ToolResultAuthority::VerifiedReference,
                })
            }
            _ => None,
        }
    }

    fn execute<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
        Box::pin(async move {
            if request.call.name == REQUEST_ASCEND_BUILD_TOOL {
                let has_correctness = self.correctness.is_some();
                let build = self
                    .build
                    .as_mut()
                    .ok_or(ToolGatewayError::UnexpectedRequest)?;
                let outcome = build
                    .execute(
                        &self.config,
                        self.artifacts.as_ref(),
                        &self.workspace_root,
                        request,
                    )
                    .await?;
                return Ok(require_downstream_gate(outcome, has_correctness));
            }
            if request.call.name == REQUEST_REDUCTION_CORRECTNESS_TOOL {
                let correctness = self
                    .correctness
                    .as_mut()
                    .ok_or(ToolGatewayError::UnexpectedRequest)?;
                return correctness
                    .execute(
                        &self.config,
                        self.artifacts.as_ref(),
                        &self.workspace_root,
                        request,
                    )
                    .await;
            }
            self.invoke(request)
        })
    }

    fn reconcile<'a>(&'a mut self, request: &'a ToolInvocation) -> ToolGatewayFuture<'a> {
        Box::pin(async move {
            if request.call.name == REQUEST_ASCEND_BUILD_TOOL {
                let has_correctness = self.correctness.is_some();
                let build = self
                    .build
                    .as_mut()
                    .ok_or(ToolGatewayError::UnexpectedRequest)?;
                let outcome = build
                    .reconcile(
                        &self.config,
                        self.artifacts.as_ref(),
                        &self.workspace_root,
                        request,
                    )
                    .await?;
                return Ok(require_downstream_gate(outcome, has_correctness));
            }
            if request.call.name == REQUEST_REDUCTION_CORRECTNESS_TOOL {
                let correctness = self
                    .correctness
                    .as_mut()
                    .ok_or(ToolGatewayError::UnexpectedRequest)?;
                return correctness
                    .reconcile(
                        &self.config,
                        self.artifacts.as_ref(),
                        &self.workspace_root,
                        request,
                    )
                    .await;
            }
            self.invoke(request)
        })
    }
}

fn require_downstream_gate(
    outcome: ToolGatewayOutcome,
    downstream_enabled: bool,
) -> ToolGatewayOutcome {
    match outcome {
        ToolGatewayOutcome::Completed {
            status,
            result_digest,
            receipt_digests,
            satisfies_subtask,
        } => ToolGatewayOutcome::Completed {
            status,
            result_digest,
            receipt_digests,
            satisfies_subtask: satisfies_subtask && !downstream_enabled,
        },
        pending => pending,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceGateArguments {
    manifest_digest: Sha256Digest,
}

pub(crate) fn ingest_bytes(
    artifacts: &dyn ArtifactStore,
    bytes: &[u8],
) -> Result<alloyport_artifacts::ArtifactIdentity, ToolGatewayError> {
    let digest = Sha256Digest::digest_bytes(bytes);
    let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    artifacts
        .ingest(
            &mut Cursor::new(bytes),
            IngestRequest {
                expected_digest: Some(digest),
                expected_size_bytes: Some(size),
            },
        )
        .map(|result| result.artifact)
        .map_err(|error| artifact_error(&error))
}

pub(crate) fn read_bounded(
    artifacts: &dyn ArtifactStore,
    digest: Sha256Digest,
    maximum: u64,
) -> Result<Vec<u8>, ToolGatewayError> {
    let mut reader = artifacts
        .open(digest)
        .map_err(|error| artifact_error(&error))?;
    if reader.identity().size_bytes > maximum {
        return Err(adapter_error("Artifact exceeds the requested read bound"));
    }
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| adapter_error(format!("cannot read Artifact: {error}")))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != reader.identity().size_bytes {
        return Err(adapter_error("Artifact reader returned an unexpected size"));
    }
    Ok(bytes)
}

/// The model named something this migration cannot resolve. It can name another; the run goes on.
pub(crate) fn citation_error(message: impl Into<String>) -> ToolGatewayError {
    ToolGatewayError::Citation(message.into())
}

pub(crate) fn adapter_error(message: impl Into<String>) -> ToolGatewayError {
    ToolGatewayError::Adapter(message.into())
}

pub(crate) fn artifact_error(error: &ArtifactStoreError) -> ToolGatewayError {
    adapter_error(error.to_string())
}

pub(crate) fn materialization_error(error: &CandidateMaterializationError) -> ToolGatewayError {
    adapter_error(error.to_string())
}
