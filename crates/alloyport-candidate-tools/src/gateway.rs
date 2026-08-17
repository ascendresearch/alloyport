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
    AgentToolGateway, ArtifactDescriptor, BundlePath, CandidateId, CandidateSourceFile,
    CandidateSourceManifest, CandidateSourceManifestSpec, GatewayToolCall, GeneratedSourceChange,
    GenerationStrategy, MigrationSpec, RuntimeToolDescriptor, Sha256Digest, SourceGateReceipt,
    TaskId, ToolEffectClass, ToolGatewayError, ToolGatewayFuture, ToolGatewayOutcome,
    ToolInputRejection, ToolInvocation, ToolOperationStatus, ToolResultAuthority,
    evaluate_source_gate,
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

    fn submit(&self, request: &ToolInvocation) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let arguments: SubmitCandidateBundleArguments =
            serde_json::from_slice(&request.call.raw_arguments)
                .map_err(|error| adapter_error(format!("invalid candidate submission: {error}")))?;
        let bundle_digest = arguments.bundle.digest();
        // Inheriting keeps the candidate complete and immutable while letting the model retype only
        // what changed. A whole bundle costs 90-100% of one response: on the first migration to
        // reach a compiler, correcting a single CMake line meant re-emitting all four files and the
        // JSON truncated mid-string at exactly the output ceiling.
        let inherited = arguments
            .inherit_from_manifest_digest
            .map(|digest| self.load_inheritable_manifest(digest))
            .transpose()?;
        if inherited.is_none() {
            // Nothing to build on, so this submission must be the whole deliverable. The error is
            // the same one the complete-bundle contract has always produced.
            arguments
                .bundle
                .require_complete()
                .map_err(|error| adapter_error(format!("invalid candidate submission: {error}")))?;
        }
        let parent_candidate_id = inherited.as_ref().map_or_else(
            || arguments.parent_candidate_id.clone(),
            // Lineage is derived from what was actually inherited rather than stated beside it, so
            // the two cannot disagree.
            |manifest| Some(manifest.candidate_id().clone()),
        );
        let candidate_id = candidate_id(&self.config, parent_candidate_id.as_ref(), bundle_digest)?;
        let mut assembled: std::collections::BTreeMap<BundlePath, CandidateSourceFile> = inherited
            .as_ref()
            .map(|manifest| {
                manifest
                    .files()
                    .iter()
                    .map(|file| (file.path().clone(), file.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for file in arguments.bundle.files() {
            assembled.insert(file.path().clone(), self.ingest_source_file(file)?);
        }
        let source_files: Vec<CandidateSourceFile> = assembled.into_values().collect();
        let manifest = CandidateSourceManifest::new(CandidateSourceManifestSpec {
            candidate_id: candidate_id.clone(),
            task_id: self.config.task_id.clone(),
            parent_candidate_id,
            migration_spec_digest: self.config.migration_spec_digest,
            generation_strategy: self.config.generation_strategy,
            public_symbol: self.config.public_symbol.clone(),
            build_target: self.config.build_target.clone(),
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
        let files = manifest
            .files()
            .iter()
            .map(|file| file.path().as_str().to_owned())
            .collect();
        let result = SubmitCandidateBundleResult {
            candidate_id,
            files,
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

    /// Stores one authored file and describes it for the manifest.
    fn ingest_source_file(
        &self,
        file: &alloyport_core::GeneratedSourceFile,
    ) -> Result<CandidateSourceFile, ToolGatewayError> {
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
        CandidateSourceFile::new(
            file.path().clone(),
            file.kind(),
            ArtifactDescriptor {
                digest: stored.artifact.digest,
                size_bytes: stored.artifact.size_bytes,
                media_type: "text/plain; charset=utf-8".to_owned(),
            },
        )
        .map_err(|error| adapter_error(error.to_string()))
    }

    /// Loads a manifest this submission inherits from, refusing one that is not ours.
    ///
    /// The digest must be one an earlier `submit_candidate_bundle` result showed the model, which
    /// is the same rule every other citation follows. A manifest belonging to another migration is
    /// a citation error, so the model is told and the run continues.
    fn load_inheritable_manifest(
        &self,
        digest: Sha256Digest,
    ) -> Result<CandidateSourceManifest, ToolGatewayError> {
        // Every input here is a digest the model named, so a digest that resolves to nothing is a
        // citation it can correct rather than a broken store.
        let bytes =
            read_bounded(self.artifacts.as_ref(), digest, MAX_MANIFEST_BYTES).map_err(|_| {
                citation_error(format!("inherited manifest {digest} does not resolve here"))
            })?;
        let manifest: CandidateSourceManifest = serde_json::from_slice(&bytes)
            .map_err(|error| citation_error(format!("invalid inherited manifest: {error}")))?;
        if !self.config.matches_manifest(&manifest) {
            return Err(citation_error(
                "inherited manifest does not belong to this migration context",
            ));
        }
        Ok(manifest)
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

    /// Checks that what a decodable call names actually exists, without touching anything.
    ///
    /// `check_arguments` only proves the JSON decodes. A live migration then died on
    /// `read_reference` naming a document one letter off from a real one, because a referent that
    /// does not resolve became an adapter error. That is a defect the model can see and correct, so
    /// it belongs here, where the reducer records it as terminal `RejectedAsInvalid` and the
    /// Episode continues.
    fn check_referents(&self, call: &GatewayToolCall) -> Result<(), (String, &'static str)> {
        let (Some(corpus), READ_REFERENCE_TOOL) = (self.reference.as_ref(), call.name.as_str())
        else {
            return Ok(());
        };
        let Ok(arguments) =
            serde_json::from_slice::<crate::reference::ReadReferenceArguments>(&call.raw_arguments)
        else {
            return Ok(());
        };
        let Some(document) = arguments.document else {
            return Ok(());
        };
        if corpus.contains(&document) {
            return Ok(());
        }
        Err((
            format!(
                "no reference document named {document}; the corpus holds {}",
                corpus.document_ids().join(", ")
            ),
            crate::reference::READ_REFERENCE_ARGUMENT_CONTRACT,
        ))
    }

    /// Turns "you named the wrong thing" into a correction turn, for every tool.
    ///
    /// This is the general form of the per-site nets that preceded it. A `Citation` error means the
    /// model pointed at something this migration cannot resolve, which it can fix by pointing
    /// somewhere else; `Adapter` keeps its durable semantics because a broken store is not the
    /// model's to correct. Separating the two is the whole difference between a correction turn and
    /// a dead migration, and they were one variant while three paid runs died on the wrong side of
    /// it.
    fn recover_citations(
        &self,
        request: &ToolInvocation,
        outcome: Result<ToolGatewayOutcome, ToolGatewayError>,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let Err(ToolGatewayError::Citation(message)) = outcome else {
            return outcome;
        };
        self.publish_recoverable(
            &request.call.name,
            &message,
            "nothing was built, run, or changed. Reissue this call naming a value an earlier tool \
             result gave you.",
        )
    }

    /// Keeps an instrument from ever ending a migration.
    ///
    /// An instrument is `ReadOnly` and cannot satisfy a subtask — it grants no authority — so it
    /// must not hold the power to terminate the run either. Design 0040 made model-authored input
    /// defects recoverable and Design 0042 extended that to citations; both fixed the sites they
    /// were looking at. This is the same rule enforced where it cannot be missed again: every
    /// adapter failure reaching a model through an instrument comes back as a readable result.
    fn as_instrument(
        &self,
        request: &ToolInvocation,
        outcome: Result<ToolGatewayOutcome, ToolGatewayError>,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let Err(ToolGatewayError::Adapter(message)) = outcome else {
            return outcome;
        };
        self.publish_recoverable(
            &request.call.name,
            &message,
            "this tool only reports; nothing was built, run, or changed. Correct the arguments \
             and reissue, or continue without it.",
        )
    }

    /// Publishes a readable refusal and returns it as a terminal-but-recoverable tool result.
    fn publish_recoverable(
        &self,
        tool: &str,
        reason: &str,
        guidance: &str,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let explanation = serde_json::json!({
            "rejected": true,
            "tool": tool,
            "reason": reason,
            "recoverable": true,
            "guidance": guidance,
        });
        let bytes = serde_json::to_vec(&explanation)
            .map_err(|error| adapter_error(format!("cannot encode rejection: {error}")))?;
        let artifact = ingest_bytes(self.artifacts.as_ref(), &bytes)?;
        Ok(ToolGatewayOutcome::Completed {
            status: ToolOperationStatus::CandidateFailed,
            result_digest: artifact.digest,
            receipt_digests: Vec::new(),
            satisfies_subtask: false,
        })
    }
}

/// Says what to do about a rejection, not only that there was one.
///
/// A serde message is accurate and useless on its own. "EOF while parsing a string at line 1
/// column 7687" is what a model sees when it ran out of output tokens mid-bundle, and it spent a
/// turn re-sending the same oversized call. The two cases worth naming are the two that happened:
/// output that stopped mid-string, and a field name that is nearly right.
fn guidance_for(tool: &str, reason: &str) -> String {
    let truncated = reason.contains("EOF while parsing");
    if truncated && tool == SUBMIT_CANDIDATE_BUNDLE_TOOL {
        return "your arguments stop part-way through a string, which is what a response that ran \
                out of room looks like. Nothing was affected. Send less in one call: set \
                inherit_from_manifest_digest to a manifest an earlier submission returned and \
                include only the files that change."
            .to_owned();
    }
    if truncated {
        return "your arguments stop part-way through a string, which is what a response that ran \
                out of room looks like. Nothing was affected. Reissue this call with a shorter \
                argument."
            .to_owned();
    }
    if reason.contains("unknown field") {
        return "a field name is not one this tool accepts; the accepted names are listed in \
                expected_arguments. Nothing was affected. Reissue with the exact names."
            .to_owned();
    }
    "the arguments were not decodable; no candidate, build, or run was affected. Reissue this call \
     with corrected arguments."
        .to_owned()
}

/// Decodes the arguments of one call without touching the workspace or any worker.
fn check_arguments(name: &str, raw: &[u8]) -> Result<(), (String, &'static str)> {
    let decoded = match name {
        SUBMIT_CANDIDATE_BUNDLE_TOOL => {
            serde_json::from_slice::<SubmitCandidateBundleArguments>(raw).map(|_| ())
        }
        REQUEST_SOURCE_GATE_TOOL => serde_json::from_slice::<SourceGateArguments>(raw).map(|_| ()),
        REQUEST_ASCEND_BUILD_TOOL => crate::build_tool::check_ascend_build_arguments(raw),
        READ_BUILD_DIAGNOSTICS_TOOL => {
            crate::build_tool::check_read_build_diagnostics_arguments(raw)
        }
        READ_REFERENCE_TOOL => crate::reference::check_read_reference_arguments(raw),
        REQUEST_REDUCTION_CORRECTNESS_TOOL => {
            crate::correctness_tool::check_reduction_correctness_arguments(raw)
        }
        _ => return Err((format!("unknown tool {name}"), "")),
    };
    decoded.map_err(|error| (error.to_string(), argument_contract(name)))
}

fn argument_contract(name: &str) -> &'static str {
    match name {
        SUBMIT_CANDIDATE_BUNDLE_TOOL => SUBMIT_CANDIDATE_BUNDLE_ARGUMENT_CONTRACT,
        REQUEST_SOURCE_GATE_TOOL => SOURCE_GATE_ARGUMENT_CONTRACT,
        REQUEST_ASCEND_BUILD_TOOL => crate::build_tool::REQUEST_ASCEND_BUILD_ARGUMENT_CONTRACT,
        READ_BUILD_DIAGNOSTICS_TOOL => crate::build_tool::READ_BUILD_DIAGNOSTICS_ARGUMENT_CONTRACT,
        READ_REFERENCE_TOOL => crate::reference::READ_REFERENCE_ARGUMENT_CONTRACT,
        REQUEST_REDUCTION_CORRECTNESS_TOOL => {
            crate::correctness_tool::REQUEST_REDUCTION_CORRECTNESS_ARGUMENT_CONTRACT
        }
        _ => "",
    }
}

const SUBMIT_CANDIDATE_BUNDLE_ARGUMENT_CONTRACT: &str = concat!(
    r#"{"parent_candidate_id": "candidate-... (optional)", "#,
    r#""inherit_from_manifest_digest": "sha256:... (optional; send only changed files)", "#,
    r#""bundle": {"files": [{"#,
    r#""path": "generated/...", "kind": "ascend_c_device|ascend_host|build_integration"#,
    r#"|component_mapping", "contents": "..."}]}}"#
);

const SOURCE_GATE_ARGUMENT_CONTRACT: &str = r#"{"manifest_digest": "sha256:..."}"#;

impl AgentToolGateway for CandidateToolGateway {
    fn validate_call(&self, call: &GatewayToolCall) -> Result<(), ToolInputRejection> {
        let (reason, contract) = if self.descriptor(&call.name).is_none() {
            (
                format!("tool {} is not available in this migration", call.name),
                "",
            )
        } else {
            match check_arguments(&call.name, &call.raw_arguments)
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
            "guidance": guidance_for(&call.name, &reason),
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
struct SubmitCandidateBundleArguments {
    #[serde(default)]
    parent_candidate_id: Option<CandidateId>,
    /// The manifest whose files this submission starts from, cited from an earlier result.
    #[serde(default)]
    inherit_from_manifest_digest: Option<Sha256Digest>,
    bundle: GeneratedSourceChange,
}

#[derive(Serialize)]
struct SubmitCandidateBundleResult {
    candidate_id: CandidateId,
    manifest: ArtifactDescriptor,
    source_bundle_digest: Sha256Digest,
    /// Every path in the assembled candidate, so an inheriting model can see what it now has
    /// rather than assume it.
    files: Vec<String>,
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

fn artifact_error(error: &ArtifactStoreError) -> ToolGatewayError {
    adapter_error(error.to_string())
}

pub(crate) fn materialization_error(error: &CandidateMaterializationError) -> ToolGatewayError {
    adapter_error(error.to_string())
}
