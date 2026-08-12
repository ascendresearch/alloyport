use crate::materialization::{CandidateMaterialization, CandidateMaterializationError};
use alloyport_artifacts::{ArtifactStore, ArtifactStoreError, IngestRequest};
use alloyport_core::{
    AgentToolGateway, ArtifactDescriptor, BundlePath, CandidateId, CandidateSourceFile,
    CandidateSourceManifest, CandidateSourceManifestSpec, GeneratedSourceBundle,
    GenerationStrategy, MigrationSpec, RuntimeToolDescriptor, Sha256Digest, SourceGateReceipt,
    TaskId, ToolEffectClass, ToolGatewayError, ToolGatewayOutcome, ToolInvocation,
    ToolOperationStatus, ToolResultAuthority, evaluate_source_gate,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    input_source_paths: BTreeSet<BundlePath>,
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
            input_source_paths,
        }
    }
}

/// Real local Agent tool adapter for candidate submission and the structural Source Gate.
pub struct CandidateToolGateway {
    config: CandidateToolConfig,
    artifacts: Arc<dyn ArtifactStore>,
    workspace_root: PathBuf,
}

impl Debug for CandidateToolGateway {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CandidateToolGateway")
            .field("config", &self.config)
            .field("workspace_root", &self.workspace_root)
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
        })
    }

    fn submit(&self, request: &ToolInvocation) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let arguments: SubmitCandidateBundleArguments =
            serde_json::from_slice(&request.call.raw_arguments)
                .map_err(|error| adapter_error(format!("invalid candidate submission: {error}")))?;
        let bundle_digest = arguments.bundle.digest();
        let candidate_id = candidate_id(
            &self.config,
            arguments.parent_candidate_id.as_ref(),
            bundle_digest,
        )?;
        let mut source_files = Vec::with_capacity(arguments.bundle.files().len());
        for file in arguments.bundle.files() {
            let bytes = file.contents().as_bytes();
            let expected_digest = Sha256Digest::digest_bytes(bytes);
            let expected_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let stored = self
                .artifacts
                .ingest(
                    &mut Cursor::new(bytes),
                    IngestRequest {
                        expected_digest: Some(expected_digest),
                        expected_size_bytes: Some(expected_size),
                    },
                )
                .map_err(|error| artifact_error(&error))?;
            source_files.push(
                CandidateSourceFile::new(
                    file.path().clone(),
                    file.kind(),
                    ArtifactDescriptor {
                        digest: stored.artifact.digest,
                        size_bytes: stored.artifact.size_bytes,
                        media_type: "text/plain; charset=utf-8".to_owned(),
                    },
                )
                .map_err(|error| adapter_error(error.to_string()))?,
            );
        }
        let manifest = CandidateSourceManifest::new(CandidateSourceManifestSpec {
            candidate_id: candidate_id.clone(),
            task_id: self.config.task_id.clone(),
            parent_candidate_id: arguments.parent_candidate_id,
            migration_spec_digest: self.config.migration_spec_digest,
            generation_strategy: self.config.generation_strategy,
            public_symbol: self.config.public_symbol.clone(),
            input_source_paths: self.config.input_source_paths.clone(),
            source_bundle_digest: bundle_digest,
            files: source_files,
        })
        .map_err(|error| adapter_error(error.to_string()))?;
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|error| adapter_error(format!("cannot encode candidate manifest: {error}")))?;
        let manifest_artifact = ingest_bytes(self.artifacts.as_ref(), &manifest_bytes)?;
        CandidateMaterialization::materialize(
            &self.workspace_root,
            self.artifacts.as_ref(),
            &manifest,
            manifest_artifact.digest,
        )
        .map_err(|error| materialization_error(&error))?;
        let result = SubmitCandidateBundleResult {
            candidate_id,
            manifest: ArtifactDescriptor {
                digest: manifest_artifact.digest,
                size_bytes: manifest_artifact.size_bytes,
                media_type: "application/vnd.alloyport.candidate-source-manifest+json".to_owned(),
            },
            source_bundle_digest: bundle_digest,
        };
        let result_bytes = serde_json::to_vec(&result)
            .map_err(|error| adapter_error(format!("cannot encode submission result: {error}")))?;
        let result = ingest_bytes(self.artifacts.as_ref(), &result_bytes)?;
        Ok(ToolGatewayOutcome::Completed {
            status: ToolOperationStatus::Succeeded,
            result_digest: result.digest,
            receipt_digests: vec![manifest_artifact.digest],
            satisfies_subtask: false,
        })
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
            .map_err(|error| adapter_error(format!("invalid candidate manifest: {error}")))?;
        if !manifest.matches_context(
            &self.config.task_id,
            self.config.migration_spec_digest,
            self.config.generation_strategy,
            &self.config.public_symbol,
            &self.config.input_source_paths,
        ) {
            return Err(adapter_error(
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
        let artifact = ingest_bytes(self.artifacts.as_ref(), &bytes)?;
        Ok(ToolGatewayOutcome::Completed {
            status: if passed {
                ToolOperationStatus::Succeeded
            } else {
                ToolOperationStatus::CandidateFailed
            },
            result_digest: artifact.digest,
            receipt_digests: vec![artifact.digest],
            satisfies_subtask: passed,
        })
    }

    fn invoke(&self, request: &ToolInvocation) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        match request.call.name.as_str() {
            SUBMIT_CANDIDATE_BUNDLE_TOOL => self.submit(request),
            REQUEST_SOURCE_GATE_TOOL => self.source_gate(request),
            _ => Err(ToolGatewayError::UnexpectedRequest),
        }
    }
}

impl AgentToolGateway for CandidateToolGateway {
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
            _ => None,
        }
    }

    fn execute(
        &mut self,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        self.invoke(request)
    }

    fn reconcile(
        &mut self,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        self.invoke(request)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitCandidateBundleArguments {
    #[serde(default)]
    parent_candidate_id: Option<CandidateId>,
    bundle: GeneratedSourceBundle,
}

#[derive(Serialize)]
struct SubmitCandidateBundleResult {
    candidate_id: CandidateId,
    manifest: ArtifactDescriptor,
    source_bundle_digest: Sha256Digest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceGateArguments {
    manifest_digest: Sha256Digest,
}

fn candidate_id(
    config: &CandidateToolConfig,
    parent: Option<&CandidateId>,
    bundle_digest: Sha256Digest,
) -> Result<CandidateId, ToolGatewayError> {
    let mut input = b"alloyport-candidate-v1\0".to_vec();
    input.extend_from_slice(config.task_id.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(&config.migration_spec_digest.bytes());
    input.push(match config.generation_strategy {
        GenerationStrategy::DirectAscendC => 1,
        GenerationStrategy::AscendSimtBootstrap => 2,
        GenerationStrategy::VerifiedTemplateAdaptation => 3,
        GenerationStrategy::MemoryGuidedSynthesis => 4,
    });
    input.extend_from_slice(&bundle_digest.bytes());
    if let Some(parent) = parent {
        input.extend_from_slice(parent.as_str().as_bytes());
    }
    CandidateId::try_from(format!(
        "candidate-{}",
        Sha256Digest::digest_bytes(&input).hexadecimal()
    ))
    .map_err(|error| adapter_error(error.to_string()))
}

fn ingest_bytes(
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

fn read_bounded(
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

fn adapter_error(message: impl Into<String>) -> ToolGatewayError {
    ToolGatewayError::Adapter(message.into())
}

fn artifact_error(error: &ArtifactStoreError) -> ToolGatewayError {
    adapter_error(error.to_string())
}

fn materialization_error(error: &CandidateMaterializationError) -> ToolGatewayError {
    adapter_error(error.to_string())
}
