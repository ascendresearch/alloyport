//! Controller-owned reduction Correctness Gate over independently produced run Artifacts.

use crate::gateway::{CandidateToolConfig, adapter_error, ingest_bytes, read_bounded};
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    AscendBuildReceipt, CorrectnessVerdict, REDUCTION_CORPUS_REVISION_V1,
    ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptPort,
    ReductionCorrectnessExperiment, ReductionOraclePolicy, ReductionRunReceipt, Sha256Digest,
    ToolGatewayError, ToolGatewayOutcome, ToolInvocation, ToolOperationStatus,
    calibrate_reduction_oracle, evaluate_reduction_correctness,
};
use serde::Deserialize;
use std::fmt::{self, Debug, Formatter};

pub const REQUEST_REDUCTION_CORRECTNESS_TOOL: &str = "request_reduction_correctness";
const MAX_BUILD_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_RUN_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;

/// Frozen oracle/corpus policy injected by the controller and hidden from model arguments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateCorrectnessToolConfig {
    policy: ReductionOraclePolicy,
    corpus_digest: Sha256Digest,
}

impl CandidateCorrectnessToolConfig {
    #[must_use]
    pub fn reduction_fixture_v1() -> Self {
        Self {
            policy: ReductionOraclePolicy::fixture_v1(),
            corpus_digest: Sha256Digest::digest_bytes(REDUCTION_CORPUS_REVISION_V1.as_bytes()),
        }
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
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let experiment = self.prepare(context, artifacts, request)?;
        let observation = self
            .attempts
            .dispatch(&experiment)
            .await
            .map_err(|error| adapter_error(error.to_string()))?;
        self.observe(artifacts, experiment, observation)
    }

    pub(crate) async fn reconcile(
        &mut self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        request: &ToolInvocation,
    ) -> Result<ToolGatewayOutcome, ToolGatewayError> {
        let experiment = self.prepare(context, artifacts, request)?;
        let observation = self
            .attempts
            .reconcile(&experiment)
            .await
            .map_err(|error| adapter_error(error.to_string()))?;
        self.observe(artifacts, experiment, observation)
    }

    fn prepare(
        &self,
        context: &CandidateToolConfig,
        artifacts: &dyn ArtifactStore,
        request: &ToolInvocation,
    ) -> Result<ReductionCorrectnessExperiment, ToolGatewayError> {
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
            return Err(adapter_error("Build Gate receipt identity changed"));
        }
        let receipt: AscendBuildReceipt = serde_json::from_slice(&bytes)
            .map_err(|error| adapter_error(format!("invalid Build Gate receipt: {error}")))?;
        if !receipt.passed()
            || receipt.task_id() != context.task_id()
            || receipt.candidate_id() != &arguments.candidate_id
            || receipt.manifest_digest() != arguments.manifest_digest
            || receipt.source_gate_receipt_digest() != arguments.source_gate_receipt_digest
        {
            return Err(adapter_error(
                "correctness requires the exact passing Build Gate receipt for this candidate",
            ));
        }
        let policy_digest = self
            .config
            .policy
            .digest()
            .map_err(|error| adapter_error(error.to_string()))?;
        Ok(ReductionCorrectnessExperiment::new(
            context.task_id().clone(),
            arguments.candidate_id,
            context.migration_spec_digest(),
            arguments.manifest_digest,
            arguments.source_gate_receipt_digest,
            arguments.build_gate_receipt_digest,
            self.config.corpus_digest,
            policy_digest,
        ))
    }

    fn observe(
        &self,
        artifacts: &dyn ArtifactStore,
        experiment: ReductionCorrectnessExperiment,
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
        let calibration = calibrate_reduction_oracle(&reference, &self.config.policy)
            .map_err(|error| adapter_error(error.to_string()))?;
        let calibration_bytes = serde_json::to_vec(&calibration).map_err(|error| {
            adapter_error(format!("cannot encode calibration receipt: {error}"))
        })?;
        let calibration_artifact = ingest_bytes(artifacts, &calibration_bytes)?;
        let receipt = evaluate_reduction_correctness(
            experiment,
            &reference,
            &candidate,
            &self.config.policy,
            &calibration,
        )
        .map_err(|error| adapter_error(error.to_string()))?;
        let verdict = receipt.verdict();
        let receipt_bytes = serde_json::to_vec(&receipt).map_err(|error| {
            adapter_error(format!("cannot encode Correctness Gate receipt: {error}"))
        })?;
        let receipt_artifact = ingest_bytes(artifacts, &receipt_bytes)?;
        Ok(ToolGatewayOutcome::Completed {
            status: match verdict {
                CorrectnessVerdict::Pass => ToolOperationStatus::Succeeded,
                CorrectnessVerdict::Fail => ToolOperationStatus::CandidateFailed,
                CorrectnessVerdict::Unverifiable | CorrectnessVerdict::InfraError => {
                    ToolOperationStatus::InfraFailed
                }
            },
            result_digest: receipt_artifact.digest,
            receipt_digests: vec![calibration_artifact.digest, receipt_artifact.digest],
            satisfies_subtask: verdict == CorrectnessVerdict::Pass,
        })
    }
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestReductionCorrectnessArguments {
    candidate_id: alloyport_core::CandidateId,
    manifest_digest: Sha256Digest,
    source_gate_receipt_digest: Sha256Digest,
    build_gate_receipt_digest: Sha256Digest,
}
