//! Production Candidate/Build/Correctness tool composition for a controller Episode.

use super::{ControllerEpisodeApplication, ControllerEpisodeError, ControllerEpisodeSpec};
use crate::{
    CorrectnessWorkerTarget, WorkerBuildAttemptAdapter, WorkerControlService,
    WorkerCorrectnessAttemptAdapter,
};
use alloyport_artifacts::ArtifactStore;
use alloyport_candidate_tools::{
    CandidateBuildToolConfig, CandidateCorrectnessToolConfig, CandidateToolConfig,
    CandidateToolGateway, READ_BUILD_DIAGNOSTICS_TOOL, READ_REFERENCE_TOOL,
    REQUEST_ASCEND_BUILD_TOOL, REQUEST_REDUCTION_CORRECTNESS_TOOL, REQUEST_SOURCE_GATE_TOOL,
    SUBMIT_CANDIDATE_BUNDLE_TOOL,
};
use alloyport_core::{CodecLimits, CodecToolDefinition, GenerationStrategy, MigrationSpec};
use alloyport_llm_provider::ReqwestModelTransport;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Validated controller-owned worker and migration policy for all Candidate tools.
#[derive(Clone, Debug)]
pub struct CandidateEpisodeToolSpec {
    pub migration_spec: MigrationSpec,
    pub generation_strategy: GenerationStrategy,
    pub workspace_root: PathBuf,
    /// Vendored reference corpus. Optional so a deployment can withhold it deliberately; when it is
    /// absent the tool is not offered at all, rather than offered and failing.
    pub reference: Option<alloyport_candidate_tools::ReferenceCorpus>,
    pub build_worker_id: String,
    pub build_policy: CandidateBuildToolConfig,
    pub correctness_policy: CandidateCorrectnessToolConfig,
    pub cuda_correctness: CorrectnessWorkerTarget,
    pub ascend_correctness: CorrectnessWorkerTarget,
}

pub type CandidateEpisodeApplication =
    ControllerEpisodeApplication<ReqwestModelTransport, CandidateToolGateway>;

/// Creates the full production HTTPS Episode with the existing independent Gate adapters.
///
/// # Errors
///
/// Returns an error for any mismatched context, worker target, migration policy, persistence
/// identity, model configuration, or local workspace boundary.
pub fn open_candidate_episode_https(
    mut episode: ControllerEpisodeSpec,
    catalog: alloyport_core::RuntimeModelCatalog,
    codec_limits: CodecLimits,
    artifacts: Arc<dyn ArtifactStore>,
    database_path: impl AsRef<Path>,
    service: WorkerControlService,
    tools: CandidateEpisodeToolSpec,
) -> Result<CandidateEpisodeApplication, ControllerEpisodeError> {
    episode.tools = candidate_episode_tool_definitions();
    let context = CandidateToolConfig::new(
        episode.task_id.clone(),
        &tools.migration_spec,
        tools.generation_strategy,
    );
    let build =
        WorkerBuildAttemptAdapter::new(service.clone(), tools.build_worker_id, artifacts.clone())
            .map_err(controller_error)?;
    let correctness = WorkerCorrectnessAttemptAdapter::new(
        service,
        tools.cuda_correctness,
        tools.ascend_correctness,
        artifacts.clone(),
    )
    .map_err(controller_error)?;
    let mut gateway = CandidateToolGateway::new(context, artifacts.clone(), tools.workspace_root)
        .map_err(controller_error)?
        .with_ascend_build(tools.build_policy, Box::new(build))
        .with_reduction_correctness(tools.correctness_policy, Box::new(correctness));
    if let Some(reference) = tools.reference {
        gateway = gateway.with_reference(reference);
    }
    ControllerEpisodeApplication::open_https(
        episode,
        catalog,
        codec_limits,
        artifacts,
        database_path,
        gateway,
    )
}

/// Exact model-visible tool catalog for the reduction Candidate Episode.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn candidate_episode_tool_definitions() -> Vec<CodecToolDefinition> {
    let digest = digest_schema();
    vec![
        CodecToolDefinition {
            name: SUBMIT_CANDIDATE_BUNDLE_TOOL.to_owned(),
            description: "Submit a generated Ascend C candidate. Send every file to create one \
                          from nothing, or set inherit_from_manifest_digest to the manifest digest \
                          an earlier submission returned and send only the files that change; the \
                          candidate is still complete and immutable, assembled from that manifest \
                          plus what you send."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "parent_candidate_id": {"type": "string", "minLength": 1},
                    "inherit_from_manifest_digest": digest.clone(),
                    "bundle": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "files": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 64,
                                "items": {
                                    "type": "object",
                                    "additionalProperties": false,
                                    "properties": {
                                        "path": {"type": "string", "pattern": "^generated/"},
                                        "kind": {"enum": [
                                            "ascend_c_device", "ascend_host",
                                            "build_integration", "component_mapping"
                                        ]},
                                        "contents": {"type": "string", "minLength": 1}
                                    },
                                    "required": ["path", "kind", "contents"]
                                }
                            },
                            "author_notes": {
                                "type": "array",
                                "items": {"type": "string", "minLength": 1}
                            }
                        },
                        "required": ["files"]
                    }
                },
                "required": ["bundle"]
            }),
            strict: true,
        },
        tool(
            REQUEST_SOURCE_GATE_TOOL,
            "Run the independent structural Source Gate for one submitted manifest.",
            &json!({"manifest_digest": digest.clone()}),
            &["manifest_digest"],
        ),
        tool(
            REQUEST_ASCEND_BUILD_TOOL,
            "Build one exact Source-Gate-passing candidate on the pinned Ascend worker.",
            &json!({
                "manifest_digest": digest.clone(),
                "source_gate_receipt_digest": digest.clone()
            }),
            &["manifest_digest", "source_gate_receipt_digest"],
        ),
        CodecToolDefinition {
            name: READ_REFERENCE_TOOL.to_owned(),
            description: "List the vendored Ascend reference corpus, or read one document. Every \
                          document arrives with its trust state; nothing in it has been verified on \
                          this hardware, so follow a document's supported path and treat its \
                          numbers as hypotheses."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {"document": {"type": "string", "minLength": 1}},
                "required": []
            }),
            strict: true,
        },
        tool(
            READ_BUILD_DIAGNOSTICS_TOOL,
            "Read the compiler output the Build Gate published for one of this migration's build \
             receipts. Returns information only; it approves nothing.",
            &json!({"build_gate_receipt_digest": digest.clone()}),
            &["build_gate_receipt_digest"],
        ),
        tool(
            REQUEST_REDUCTION_CORRECTNESS_TOOL,
            "Run the paired frozen reduction Correctness Gate after Build Gate passes.",
            &json!({
                "candidate_id": {"type": "string", "minLength": 1},
                "manifest_digest": digest.clone(),
                "source_gate_receipt_digest": digest.clone(),
                "build_gate_receipt_digest": digest
            }),
            &[
                "candidate_id",
                "manifest_digest",
                "source_gate_receipt_digest",
                "build_gate_receipt_digest",
            ],
        ),
    ]
}

fn tool(
    name: &str,
    description: &str,
    properties: &Value,
    required: &[&str],
) -> CodecToolDefinition {
    CodecToolDefinition {
        name: name.to_owned(),
        description: description.to_owned(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": properties,
            "required": required
        }),
        strict: true,
    }
}

fn digest_schema() -> Value {
    json!({"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"})
}

fn controller_error(error: impl std::fmt::Display) -> ControllerEpisodeError {
    ControllerEpisodeError::adapter(error)
}

#[cfg(test)]
#[path = "candidate_episode_tests.rs"]
mod tests;
