//! Controller-owned reduction Correctness Gate over independently produced run Artifacts.

use crate::gateway::{
    CandidateToolConfig, adapter_error, citation_error, ingest_bytes, materialization_error,
    read_bounded,
};
use crate::materialization::CandidateMaterialization;
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    AscendBuildReceipt, CandidateSourceManifest, CorrectnessVerdict, ReductionCorpus,
    ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptPort,
    ReductionCorrectnessAttemptSpec, ReductionCorrectnessExperiment, ReductionExecutionBundle,
    ReductionExecutionFile, ReductionRunReceipt, ReductionTolerancePlan, Sha256Digest,
    ToolGatewayError, ToolGatewayOutcome, ToolInvocation, ToolOperationStatus,
    calibrate_reduction_oracle, evaluate_reduction_correctness,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};

pub const REQUEST_REDUCTION_CORRECTNESS_TOOL: &str = "request_reduction_correctness";
const MAX_BUILD_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_RUN_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_EXECUTION_BUNDLE_BYTES: u64 = 32 * 1024 * 1024;

/// Frozen corpus and tolerance plan injected by the controller and hidden from model arguments.
///
/// The plan is frozen; the tolerance is not, because it does not exist until the authority has run.
/// The oracle derives it from that run's own measured spread.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateCorrectnessToolConfig {
    tolerance_plan: ReductionTolerancePlan,
    callable: alloyport_core::CorrectnessCallable,
    corpus: ReductionCorpus,
    reference_files: Vec<ReductionExecutionFile>,
}

impl CandidateCorrectnessToolConfig {
    /// Constructs the frozen reduction configuration from the independently captured CUDA intake.
    ///
    /// # Errors
    ///
    /// Returns an error for non-UTF-8, empty, or invalid reference source files.
    pub fn reduction_fixture_v1(
        migration_spec: &alloyport_core::MigrationSpec,
        reference_sources: BTreeMap<alloyport_core::BundlePath, Vec<u8>>,
    ) -> Result<Self, ToolGatewayError> {
        let reference_files = reference_sources
            .into_iter()
            .map(|(path, bytes)| {
                let contents = String::from_utf8(bytes)
                    .map_err(|_| adapter_error("CUDA reference source is not UTF-8"))?;
                ReductionExecutionFile::new(path, contents)
                    .map_err(|error| adapter_error(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            tolerance_plan: ReductionTolerancePlan::fixture_v1(),
            callable: alloyport_core::CorrectnessCallable {
                public_symbol: migration_spec.public_entry().symbol().to_owned(),
                reference_build_target: migration_spec.reference().library_target().to_owned(),
                candidate_build_target: migration_spec.public_entry().build_target().to_owned(),
            },
            corpus: ReductionCorpus::fixture_v1(),
            reference_files,
        })
    }
}

pub(crate) struct CandidateCorrectnessTool {
    config: CandidateCorrectnessToolConfig,
    attempts: Box<dyn ReductionCorrectnessAttemptPort>,
}

impl Debug for CandidateCorrectnessTool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CandidateCorrectnessTool")
            .field("config", &self.config)
            .field("attempts", &self.attempts)
            .finish()
    }
}

impl CandidateCorrectnessTool {
    pub(crate) const fn new(
        config: CandidateCorrectnessToolConfig,
        attempts: Box<dyn ReductionCorrectnessAttemptPort>,
    ) -> Self {
        Self { config, attempts }
    }

    pub(crate) async fn execute(
        &mut self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        workspace_root: &std::path::Path,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let spec = self.prepare(context, artifacts, workspace_root, request)?;
        let observation = self
            .attempts
            .dispatch(&spec)
            .await
            .map_err(|error| adapter_error(error.to_string()))?;
        self.observe(artifacts, &spec, observation)
    }

    pub(crate) async fn reconcile(
        &mut self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        workspace_root: &std::path::Path,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let spec = self.prepare(context, artifacts, workspace_root, request)?;
        let observation = self
            .attempts
            .reconcile(&spec)
            .await
            .map_err(|error| adapter_error(error.to_string()))?;
        self.observe(artifacts, &spec, observation)
    }

    fn prepare(
        &self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        workspace_root: &std::path::Path,
        request: &ToolInvocation,
    ) -> Result<ReductionCorrectnessAttemptSpec, ToolGatewayError> {
        let arguments: RequestReductionCorrectnessArguments =
            serde_json::from_slice(&request.call.raw_arguments).map_err(|error| {
                adapter_error(format!("invalid reduction correctness request: {error}"))
            })?;
        let bytes = read_bounded(
            artifacts,
            arguments.build_gate_receipt_digest,
            MAX_BUILD_RECEIPT_BYTES,
        )?;
        if Sha256Digest::digest_bytes(&bytes) != arguments.build_gate_receipt_digest {
            return Err(citation_error("Build Gate receipt identity changed"));
        }
        let receipt: AscendBuildReceipt = serde_json::from_slice(&bytes)
            .map_err(|error| citation_error(format!("invalid Build Gate receipt: {error}")))?;
        if !receipt.passed()
            || receipt.task_id() != context.task_id()
            || receipt.candidate_id() != &arguments.candidate_id
            || receipt.manifest_digest() != arguments.manifest_digest
            || receipt.source_gate_receipt_digest() != arguments.source_gate_receipt_digest
        {
            return Err(citation_error(
                "correctness requires the exact passing Build Gate receipt for this candidate",
            ));
        }
        let manifest_bytes =
            read_bounded(artifacts, arguments.manifest_digest, MAX_MANIFEST_BYTES)?;
        let manifest: CandidateSourceManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| citation_error(format!("invalid candidate manifest: {error}")))?;
        if !context.matches_manifest(&manifest)
            || manifest.candidate_id() != &arguments.candidate_id
        {
            return Err(citation_error(
                "candidate manifest does not belong to this correctness request",
            ));
        }
        let materialization = CandidateMaterialization::materialize(
            workspace_root,
            artifacts,
            &manifest,
            arguments.manifest_digest,
        )
        .map_err(|error| materialization_error(&error))?;
        let candidate_sources = materialization
            .read_verified_sources(&manifest)
            .map_err(|error| materialization_error(&error))?;
        let candidate_files = candidate_sources
            .into_iter()
            .map(|(path, bytes)| {
                let contents = String::from_utf8(bytes)
                    .map_err(|_| adapter_error("Ascend candidate source is not UTF-8"))?;
                ReductionExecutionFile::new(path, contents)
                    .map_err(|error| adapter_error(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let tolerance_plan_digest = self
            .config
            .tolerance_plan
            .digest()
            .map_err(|error| adapter_error(error.to_string()))?;
        let corpus_digest = self
            .config
            .corpus
            .digest()
            .map_err(|error| adapter_error(error.to_string()))?;
        let experiment = ReductionCorrectnessExperiment::new(
            context.task_id().clone(),
            arguments.candidate_id,
            context.migration_spec_digest(),
            arguments.manifest_digest,
            arguments.source_gate_receipt_digest,
            arguments.build_gate_receipt_digest,
            corpus_digest,
            tolerance_plan_digest,
        );
        let reference_bundle = ReductionExecutionBundle::new(
            experiment.clone(),
            alloyport_core::ReductionRunRole::CudaReference,
            self.config.callable.clone(),
            self.config.corpus.clone(),
            self.config.reference_files.clone(),
        )
        .map_err(|error| adapter_error(error.to_string()))?;
        let candidate_bundle = ReductionExecutionBundle::new(
            experiment.clone(),
            alloyport_core::ReductionRunRole::AscendCandidate,
            self.config.callable.clone(),
            self.config.corpus.clone(),
            candidate_files,
        )
        .map_err(|error| adapter_error(error.to_string()))?;
        Ok(ReductionCorrectnessAttemptSpec {
            experiment,
            reference_bundle: ingest_execution_bundle(artifacts, &reference_bundle)?,
            candidate_bundle: ingest_execution_bundle(artifacts, &candidate_bundle)?,
        })
    }

    fn observe(
        &self,
        artifacts: &dyn ArtifactStore,
        spec: &ReductionCorrectnessAttemptSpec,
        observation: ReductionCorrectnessAttemptObservation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let (reference_descriptor, candidate_descriptor) = match observation {
            ReductionCorrectnessAttemptObservation::Pending { diagnostic_digest } => {
                return Ok(ToolGatewayOutcome::Pending { diagnostic_digest });
            }
            ReductionCorrectnessAttemptObservation::Finished {
                reference_run,
                candidate_run,
            } => (reference_run, candidate_run),
        };
        let reference: ReductionRunReceipt =
            read_receipt(artifacts, &reference_descriptor, "CUDA reference")?;
        let candidate: ReductionRunReceipt =
            read_receipt(artifacts, &candidate_descriptor, "Ascend candidate")?;
        let reference_bundle = read_execution_bundle(artifacts, &spec.reference_bundle)?;
        let candidate_bundle = read_execution_bundle(artifacts, &spec.candidate_bundle)?;
        if reference_bundle.experiment() != &spec.experiment
            || candidate_bundle.experiment() != &spec.experiment
            || reference_bundle.role() != alloyport_core::ReductionRunRole::CudaReference
            || candidate_bundle.role() != alloyport_core::ReductionRunRole::AscendCandidate
            || reference.implementation_digest() != reference_bundle.implementation_digest()
            || candidate.implementation_digest() != candidate_bundle.implementation_digest()
        {
            return Err(adapter_error(
                "correctness run receipt does not bind the assigned implementation bundle",
            ));
        }
        let calibration = calibrate_reduction_oracle(
            &reference,
            &self.config.tolerance_plan,
            &self.config.corpus,
        )
        .map_err(|error| adapter_error(error.to_string()))?;
        let calibration_bytes = serde_json::to_vec(&calibration).map_err(|error| {
            adapter_error(format!("cannot encode calibration receipt: {error}"))
        })?;
        let calibration_artifact = ingest_bytes(artifacts, &calibration_bytes)?;
        let receipt = evaluate_reduction_correctness(
            spec.experiment.clone(),
            &reference,
            &candidate,
            &self.config.tolerance_plan,
            &self.config.corpus,
            &calibration,
        )
        .map_err(|error| adapter_error(error.to_string()))?;
        let verdict = receipt.verdict();
        let receipt_bytes = serde_json::to_vec(&receipt).map_err(|error| {
            adapter_error(format!("cannot encode Correctness Gate receipt: {error}"))
        })?;
        // The calibration receipt is named beside the verdict rather than folded into it: a reader
        // asking what bounded this verdict should not have to already know the digest.
        let published = crate::gate_result::publish_gate_result_with(
            artifacts,
            &receipt_bytes,
            crate::gate_result::CORRECTNESS_RECEIPT_MEDIA_TYPE,
            vec![(
                "calibration",
                calibration_artifact.digest,
                calibration_artifact.size_bytes,
                crate::gate_result::CALIBRATION_RECEIPT_MEDIA_TYPE,
            )],
        )?;
        Ok(ToolGatewayOutcome::Completed {
            status: match verdict {
                CorrectnessVerdict::Pass => ToolOperationStatus::Succeeded,
                CorrectnessVerdict::Fail => ToolOperationStatus::CandidateFailed,
                CorrectnessVerdict::Unverifiable | CorrectnessVerdict::InfraError => {
                    ToolOperationStatus::InfraFailed
                }
            },
            result_digest: published.result_digest,
            receipt_digests: vec![calibration_artifact.digest, published.receipt_digest],
            satisfies_subtask: verdict == CorrectnessVerdict::Pass,
        })
    }
}

fn ingest_execution_bundle(
    artifacts: &dyn ArtifactStore,
    bundle: &ReductionExecutionBundle,
) -> Result<alloyport_core::ArtifactDescriptor, ToolGatewayError> {
    let bytes = serde_json::to_vec(bundle)
        .map_err(|error| adapter_error(format!("cannot encode correctness bundle: {error}")))?;
    let stored = ingest_bytes(artifacts, &bytes)?;
    Ok(alloyport_core::ArtifactDescriptor {
        digest: stored.digest,
        size_bytes: stored.size_bytes,
        media_type: alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE.to_owned(),
    })
}

fn read_receipt(
    artifacts: &dyn ArtifactStore,
    descriptor: &alloyport_core::ArtifactDescriptor,
    label: &str,
) -> Result<ReductionRunReceipt, ToolGatewayError> {
    if descriptor.size_bytes == 0 || descriptor.size_bytes > MAX_RUN_RECEIPT_BYTES {
        return Err(adapter_error(format!(
            "{label} run receipt size is invalid"
        )));
    }
    let bytes = read_bounded(artifacts, descriptor.digest, MAX_RUN_RECEIPT_BYTES)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != descriptor.size_bytes
        || Sha256Digest::digest_bytes(&bytes) != descriptor.digest
    {
        return Err(adapter_error(format!(
            "{label} run receipt identity changed"
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| adapter_error(format!("invalid {label} run receipt: {error}")))
}

fn read_execution_bundle(
    artifacts: &dyn ArtifactStore,
    descriptor: &alloyport_core::ArtifactDescriptor,
) -> Result<ReductionExecutionBundle, ToolGatewayError> {
    if descriptor.media_type != alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE
        || descriptor.size_bytes == 0
        || descriptor.size_bytes > MAX_EXECUTION_BUNDLE_BYTES
    {
        return Err(adapter_error(
            "invalid reduction execution bundle descriptor",
        ));
    }
    let bytes = read_bounded(artifacts, descriptor.digest, MAX_EXECUTION_BUNDLE_BYTES)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != descriptor.size_bytes
        || Sha256Digest::digest_bytes(&bytes) != descriptor.digest
    {
        return Err(adapter_error("reduction execution bundle identity changed"));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| adapter_error(format!("invalid reduction execution bundle: {error}")))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestReductionCorrectnessArguments {
    candidate_id: alloyport_core::CandidateId,
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
    build_gate_receipt_digest: Sha256Digest,
}

pub(crate) const REQUEST_REDUCTION_CORRECTNESS_ARGUMENT_CONTRACT: &str = concat!(
    r#"{"candidate_id": "candidate-...", "manifest_digest": "sha256:...", "#,
    r#""source_gate_receipt_digest": "sha256:...", "build_gate_receipt_digest": "sha256:..."}"#
);

/// Decodes the arguments without any effect, so a malformed call can be returned to the model.
pub(crate) fn check_reduction_correctness_arguments(raw: &[u8]) -> Result<(), serde_json::Error> {
    serde_json::from_slice::<RequestReductionCorrectnessArguments>(raw).map(|_| ())
}
