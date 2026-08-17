//! Assembling one candidate from a submission, and the identity it gets.
//!
//! Split out of `gateway.rs` for the module-size limit. It stays a child module so it can use the
//! gateway's own configuration and store without either becoming public.

use super::{
    CandidateToolConfig, CandidateToolGateway, MAX_MANIFEST_BYTES, adapter_error, artifact_error,
    citation_error, ingest_bytes, materialization_error, read_bounded,
};
use crate::materialization::CandidateMaterialization;
use alloyport_artifacts::IngestRequest;
use alloyport_core::{
    ArtifactDescriptor, BundlePath, CandidateId, CandidateSourceFile, CandidateSourceManifest,
    CandidateSourceManifestSpec, GeneratedSourceChange, GenerationStrategy, Sha256Digest,
    ToolGatewayError, ToolGatewayOutcome, ToolInvocation, ToolOperationStatus,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

impl CandidateToolGateway {
    pub(super) fn submit(
        &self,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubmitCandidateBundleArguments {
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

pub(super) const SUBMIT_CANDIDATE_BUNDLE_ARGUMENT_CONTRACT: &str = concat!(
    r#"{"parent_candidate_id": "candidate-... (optional)", "#,
    r#""inherit_from_manifest_digest": "sha256:... (optional; send only changed files)", "#,
    r#""bundle": {"files": [{"#,
    r#""path": "generated/...", "kind": "ascend_c_device|ascend_host|build_integration"#,
    r#"|component_mapping", "contents": "..."}]}}"#
);
