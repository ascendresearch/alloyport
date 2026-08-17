//! Paired worker-control adapter for independent reduction reference and DUT executions.

use crate::control_transport::contract_to_assignment;
use crate::storage::{AssignmentRecord, AttemptState, FinishedObservation};
use crate::{EnqueueError, WorkerControlService};
use alloyport_artifacts::ArtifactStore;
use alloyport_core::{
    ASCEND_REDUCTION_CORRECTNESS_FEATURE, ArtifactDescriptor, AssignmentContract, AssignmentId,
    AttemptId, AttemptOutcome, CUDA_REDUCTION_CORRECTNESS_FEATURE, ExecutionContract,
    ExecutionKind, ReductionCorrectnessAttemptError, ReductionCorrectnessAttemptFuture,
    ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptPort,
    ReductionCorrectnessAttemptSpec, ReductionExecutionBundle, ReductionRunReceipt,
    ReductionRunRole, ResourceContract, Sha256Digest,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::io::Read;
use std::str::FromStr;
use std::sync::Arc;

const RUNNER_ARGV: &str = "reduction-correctness-v1";
const MAX_WORKER_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_RUN_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_EXECUTION_BUNDLE_BYTES: u64 = 32 * 1024 * 1024;

/// One explicitly selected worker and its immutable local-image assignment facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrectnessWorkerTarget {
    worker_id: String,
    image: ArtifactDescriptor,
    limits: ResourceContract,
    timeout_ms: u64,
}

impl CorrectnessWorkerTarget {
    /// Creates a role-specific target without granting the server arbitrary execution controls.
    ///
    /// # Errors
    ///
    /// Returns an error for empty identities or incomplete resource/image facts.
    pub fn new(
        worker_id: impl Into<String>,
        image: ArtifactDescriptor,
        limits: ResourceContract,
        timeout_ms: u64,
    ) -> Result<Self, ReductionCorrectnessAttemptError> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty()
            || image.size_bytes == 0
            || image.media_type.trim().is_empty()
            || timeout_ms == 0
            || limits.cpu_millis == 0
            || limits.memory_bytes == 0
            || limits.disk_bytes == 0
            || limits.process_count == 0
            || limits.output_bytes == 0
            || limits.device_count != 1
            || limits.network != alloyport_core::NetworkPolicy::Disabled
        {
            return Err(ReductionCorrectnessAttemptError::Rejected(
                "correctness worker target is incomplete or unsafe".to_owned(),
            ));
        }
        Ok(Self {
            worker_id,
            image,
            limits,
            timeout_ms,
        })
    }
}

/// Production control-plane adapter for one immutable CUDA/Ascend assignment pair.
#[derive(Clone, Debug)]
pub struct WorkerCorrectnessAttemptAdapter {
    service: WorkerControlService,
    cuda: CorrectnessWorkerTarget,
    ascend: CorrectnessWorkerTarget,
    artifacts: Arc<dyn ArtifactStore>,
}

impl WorkerCorrectnessAttemptAdapter {
    /// Binds the paired Port to distinct, explicitly configured backend workers.
    ///
    /// # Errors
    ///
    /// Returns an error when both roles name the same worker.
    pub fn new(
        service: WorkerControlService,
        cuda: CorrectnessWorkerTarget,
        ascend: CorrectnessWorkerTarget,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Result<Self, ReductionCorrectnessAttemptError> {
        if cuda.worker_id == ascend.worker_id {
            return Err(ReductionCorrectnessAttemptError::Rejected(
                "CUDA authority and Ascend DUT require distinct workers".to_owned(),
            ));
        }
        Ok(Self {
            service,
            cuda,
            ascend,
            artifacts,
        })
    }

    async fn dispatch_or_observe(
        &self,
        spec: &ReductionCorrectnessAttemptSpec,
    ) -> Result<ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptError> {
        let assignments = self.assignments(spec)?;
        for (target, assignment) in [
            (&self.cuda, &assignments[0]),
            (&self.ascend, &assignments[1]),
        ] {
            // Same defect as the Build Gate, one hop later: the controller writes each execution
            // bundle into its own store, which the Artifact ledger never hears about, so the
            // assignment would name a digest the coordinator cannot grant.
            self.publish_bundle(&target.worker_id, assignment)?;
            self.service
                .enqueue_assignment(target.worker_id.clone(), contract_to_assignment(assignment))
                .await
                .map_err(|error| enqueue_error(&error))?;
        }
        self.observe_pair(spec, &assignments).await
    }

    /// Makes a controller-authored execution bundle downloadable by the worker that needs it.
    ///
    /// The size comes from the stored object rather than from the assignment, because the
    /// assignment is the thing being checked.
    fn publish_bundle(
        &self,
        worker_id: &str,
        assignment: &AssignmentContract,
    ) -> Result<(), ReductionCorrectnessAttemptError> {
        let digest = assignment.execution.bundle.digest;
        let identity = self
            .artifacts
            .open(digest)
            .map_err(|error| {
                ReductionCorrectnessAttemptError::Rejected(format!(
                    "execution bundle {digest} is not readable in the controller store: {error}"
                ))
            })?
            .identity();
        self.service
            .artifact_metadata()
            .ok_or_else(|| {
                ReductionCorrectnessAttemptError::Rejected(
                    "publishing an execution bundle requires the Artifact metadata service"
                        .to_owned(),
                )
            })?
            .record_local_artifact(worker_id, identity)
            .map_err(|error| {
                ReductionCorrectnessAttemptError::Rejected(format!(
                    "cannot publish execution bundle: {error}"
                ))
            })
    }

    async fn observe_pair(
        &self,
        spec: &ReductionCorrectnessAttemptSpec,
        assignments: &[AssignmentContract; 2],
    ) -> Result<ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptError> {
        let reference = self.lookup(&assignments[0], &self.cuda.worker_id).await?;
        let candidate = self.lookup(&assignments[1], &self.ascend.worker_id).await?;
        let (
            Some((reference_record, reference_finished)),
            Some((candidate_record, candidate_finished)),
        ) = (reference, candidate)
        else {
            return Err(ReductionCorrectnessAttemptError::Integrity(
                "dispatched correctness assignment is missing".to_owned(),
            ));
        };
        reject_terminal_state(&reference_record)?;
        reject_terminal_state(&candidate_record)?;
        match (reference_finished, candidate_finished) {
            (Some(reference), Some(candidate))
                if reference_record.state == AttemptState::Finished
                    && candidate_record.state == AttemptState::Finished =>
            {
                let reference_run = self.terminal_run(
                    spec,
                    &assignments[0],
                    ReductionRunRole::CudaReference,
                    &spec.reference_bundle,
                    reference,
                )?;
                let candidate_run = self.terminal_run(
                    spec,
                    &assignments[1],
                    ReductionRunRole::AscendCandidate,
                    &spec.candidate_bundle,
                    candidate,
                )?;
                Ok(ReductionCorrectnessAttemptObservation::Finished {
                    reference_run,
                    candidate_run,
                })
            }
            _ => Ok(ReductionCorrectnessAttemptObservation::Pending {
                diagnostic_digest: pending_digest(&reference_record, &candidate_record),
            }),
        }
    }

    async fn lookup(
        &self,
        assignment: &AssignmentContract,
        worker_id: &str,
    ) -> Result<
        Option<(AssignmentRecord, Option<FinishedObservation>)>,
        ReductionCorrectnessAttemptError,
    > {
        let reads = Arc::clone(&self.service.repositories.assignment_reads);
        let attempt_id = assignment.attempt_id.to_string();
        let result = self
            .service
            .persistence
            .run(move || {
                Ok::<_, crate::storage::RepositoryError>((
                    reads.assignment(&attempt_id)?,
                    reads.finished_observation(&attempt_id)?,
                ))
            })
            .await
            .map_err(|error| ReductionCorrectnessAttemptError::Unavailable(error.to_string()))?
            .map_err(|error| ReductionCorrectnessAttemptError::Unavailable(error.to_string()))?;
        let (record, finished) = result;
        let Some(record) = record else {
            return Ok(None);
        };
        if record.worker_id != worker_id || record.contract != *assignment {
            return Err(ReductionCorrectnessAttemptError::Integrity(
                "correctness assignment identity changed after dispatch".to_owned(),
            ));
        }
        Ok(Some((record, finished)))
    }

    #[allow(clippy::too_many_arguments)]
    fn terminal_run(
        &self,
        spec: &ReductionCorrectnessAttemptSpec,
        assignment: &AssignmentContract,
        role: ReductionRunRole,
        bundle_descriptor: &ArtifactDescriptor,
        finished: FinishedObservation,
    ) -> Result<ArtifactDescriptor, ReductionCorrectnessAttemptError> {
        if finished.outcome != AttemptOutcome::Succeeded || finished.exit_code != Some(0) {
            return Err(match finished.outcome {
                AttemptOutcome::CandidateFailed => {
                    ReductionCorrectnessAttemptError::Rejected(finished.detail)
                }
                _ => ReductionCorrectnessAttemptError::Unavailable(finished.detail),
            });
        }
        let worker_receipt = finished.receipt.as_ref().ok_or_else(|| {
            ReductionCorrectnessAttemptError::Integrity(
                "correctness attempt lacks worker receipt".to_owned(),
            )
        })?;
        let run_descriptor = finished.stdout.as_ref().ok_or_else(|| {
            ReductionCorrectnessAttemptError::Integrity(
                "correctness attempt lacks structured stdout receipt".to_owned(),
            )
        })?;
        let worker: WorkerReceiptProjection = read_json_artifact(
            self.artifacts.as_ref(),
            worker_receipt,
            MAX_WORKER_RECEIPT_BYTES,
        )?;
        validate_worker_receipt(&worker, assignment, &finished, run_descriptor)?;
        let bundle: ReductionExecutionBundle = read_json_artifact(
            self.artifacts.as_ref(),
            bundle_descriptor,
            MAX_EXECUTION_BUNDLE_BYTES,
        )?;
        let run: ReductionRunReceipt = read_json_artifact(
            self.artifacts.as_ref(),
            run_descriptor,
            MAX_RUN_RECEIPT_BYTES,
        )?;
        validate_structured_run(spec, role, &bundle, &run)?;
        Ok(run_descriptor.clone())
    }

    fn assignments(
        &self,
        spec: &ReductionCorrectnessAttemptSpec,
    ) -> Result<[AssignmentContract; 2], ReductionCorrectnessAttemptError> {
        Ok([
            assignment(
                spec,
                ReductionRunRole::CudaReference,
                &spec.reference_bundle,
                &self.cuda,
            )?,
            assignment(
                spec,
                ReductionRunRole::AscendCandidate,
                &spec.candidate_bundle,
                &self.ascend,
            )?,
        ])
    }
}

impl ReductionCorrectnessAttemptPort for WorkerCorrectnessAttemptAdapter {
    fn dispatch<'a>(
        &'a mut self,
        spec: &'a ReductionCorrectnessAttemptSpec,
    ) -> ReductionCorrectnessAttemptFuture<'a> {
        Box::pin(async move { self.dispatch_or_observe(spec).await })
    }

    fn reconcile<'a>(
        &'a mut self,
        spec: &'a ReductionCorrectnessAttemptSpec,
    ) -> ReductionCorrectnessAttemptFuture<'a> {
        Box::pin(async move { self.dispatch_or_observe(spec).await })
    }
}

fn assignment(
    spec: &ReductionCorrectnessAttemptSpec,
    role: ReductionRunRole,
    bundle: &ArtifactDescriptor,
    target: &CorrectnessWorkerTarget,
) -> Result<AssignmentContract, ReductionCorrectnessAttemptError> {
    if bundle.media_type != alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE
        || bundle.size_bytes == 0
        || bundle.size_bytes > MAX_EXECUTION_BUNDLE_BYTES
    {
        return Err(ReductionCorrectnessAttemptError::Rejected(
            "invalid correctness execution bundle descriptor".to_owned(),
        ));
    }
    let (role_name, executor_kind, feature) = match role {
        ReductionRunRole::CudaReference => (
            "cuda-reference",
            ExecutionKind::CudaCorrectness,
            CUDA_REDUCTION_CORRECTNESS_FEATURE,
        ),
        ReductionRunRole::AscendCandidate => (
            "ascend-candidate",
            ExecutionKind::AscendCorrectness,
            ASCEND_REDUCTION_CORRECTNESS_FEATURE,
        ),
    };
    let mut identity = b"alloyport-correctness-assignment-v1\0".to_vec();
    identity.extend_from_slice(&spec.experiment.experiment_digest().bytes());
    identity.extend_from_slice(role_name.as_bytes());
    identity.extend_from_slice(&bundle.digest.bytes());
    identity.extend_from_slice(&target.image.digest.bytes());
    let digest = Sha256Digest::digest_bytes(&identity).hexadecimal();
    Ok(AssignmentContract {
        assignment_id: AssignmentId::try_from(format!("assignment-{role_name}-{digest}"))
            .map_err(|error| ReductionCorrectnessAttemptError::Rejected(error.to_string()))?,
        attempt_id: AttemptId::try_from(format!("attempt-{role_name}-{digest}"))
            .map_err(|error| ReductionCorrectnessAttemptError::Rejected(error.to_string()))?,
        attempt_number: 1,
        idempotency_key: format!(
            "reduction-correctness:{role_name}:{}",
            spec.experiment.experiment_digest()
        ),
        task_id: spec.experiment.task_id().clone(),
        candidate_id: spec.experiment.candidate_id().clone(),
        execution: ExecutionContract {
            executor_kind,
            argv: vec![RUNNER_ARGV.to_owned()],
            working_directory: ".".to_owned(),
            environment: Vec::new(),
            timeout_ms: target.timeout_ms,
            bundle: bundle.clone(),
            image: target.image.clone(),
            limits: Some(target.limits.clone()),
        },
        required_features: vec![feature.to_owned()],
    })
}

fn reject_terminal_state(
    record: &AssignmentRecord,
) -> Result<(), ReductionCorrectnessAttemptError> {
    match record.state {
        AttemptState::Rejected | AttemptState::Cancelled => {
            Err(ReductionCorrectnessAttemptError::Rejected(format!(
                "correctness attempt became {:?}",
                record.state
            )))
        }
        AttemptState::LeaseExpired => Err(ReductionCorrectnessAttemptError::Unavailable(
            "correctness attempt lease expired".to_owned(),
        )),
        _ => Ok(()),
    }
}

fn pending_digest(reference: &AssignmentRecord, candidate: &AssignmentRecord) -> Sha256Digest {
    let mut bytes = b"alloyport-correctness-pending-v1\0".to_vec();
    for record in [reference, candidate] {
        bytes.extend_from_slice(record.contract.attempt_id.as_str().as_bytes());
        bytes.extend_from_slice(&(record.state as i64).to_be_bytes());
        bytes.extend_from_slice(&record.updated_at_ms.to_be_bytes());
    }
    Sha256Digest::digest_bytes(&bytes)
}

fn enqueue_error(error: &EnqueueError) -> ReductionCorrectnessAttemptError {
    ReductionCorrectnessAttemptError::Unavailable(error.to_string())
}

#[derive(Deserialize)]
struct WorkerReceiptProjection {
    schema_version: u16,
    assignment_id: String,
    attempt_id: String,
    task_id: String,
    candidate_id: String,
    bundle_digest: String,
    image_digest: String,
    outcome: String,
    exit_code: Option<i32>,
    elapsed_ms: u64,
    stdout_digest: Sha256Digest,
}

fn validate_worker_receipt(
    receipt: &WorkerReceiptProjection,
    assignment: &AssignmentContract,
    finished: &FinishedObservation,
    stdout: &ArtifactDescriptor,
) -> Result<(), ReductionCorrectnessAttemptError> {
    let expected_schema = match assignment.execution.executor_kind {
        ExecutionKind::CudaCorrectness => 3,
        ExecutionKind::AscendCorrectness => 2,
        _ => {
            return Err(ReductionCorrectnessAttemptError::Integrity(
                "unexpected correctness executor kind".to_owned(),
            ));
        }
    };
    let bundle_digest = Sha256Digest::from_str(&receipt.bundle_digest)
        .map_err(|error| ReductionCorrectnessAttemptError::Integrity(error.to_string()))?;
    let image_digest = Sha256Digest::from_str(&receipt.image_digest)
        .map_err(|error| ReductionCorrectnessAttemptError::Integrity(error.to_string()))?;
    if receipt.schema_version != expected_schema
        || receipt.assignment_id != assignment.assignment_id.as_str()
        || receipt.attempt_id != assignment.attempt_id.as_str()
        || receipt.task_id != assignment.task_id.as_str()
        || receipt.candidate_id != assignment.candidate_id.as_str()
        || bundle_digest != assignment.execution.bundle.digest
        || image_digest != assignment.execution.image.digest
        || receipt.outcome != finished.outcome.as_str_name()
        || receipt.exit_code != finished.exit_code
        || receipt.elapsed_ms != finished.elapsed_ms
        || receipt.stdout_digest != stdout.digest
    {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "worker receipt does not match the terminal correctness observation".to_owned(),
        ));
    }
    Ok(())
}

fn validate_structured_run(
    spec: &ReductionCorrectnessAttemptSpec,
    role: ReductionRunRole,
    bundle: &ReductionExecutionBundle,
    run: &ReductionRunReceipt,
) -> Result<(), ReductionCorrectnessAttemptError> {
    let expected_candidate = match role {
        ReductionRunRole::CudaReference => None,
        ReductionRunRole::AscendCandidate => Some(spec.experiment.candidate_id()),
    };
    if bundle.role() != role
        || bundle.experiment() != &spec.experiment
        || run.role() != role
        || run.experiment_digest() != spec.experiment.experiment_digest()
        || run.candidate_id() != expected_candidate
        || run.corpus_digest() != spec.experiment.corpus_digest()
        || run.implementation_digest() != bundle.implementation_digest()
    {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "structured run receipt does not match its correctness assignment".to_owned(),
        ));
    }
    Ok(())
}

fn read_json_artifact<T: DeserializeOwned>(
    artifacts: &dyn ArtifactStore,
    descriptor: &ArtifactDescriptor,
    max_bytes: u64,
) -> Result<T, ReductionCorrectnessAttemptError> {
    if descriptor.size_bytes == 0 || descriptor.size_bytes > max_bytes {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "structured correctness Artifact descriptor is invalid".to_owned(),
        ));
    }
    let mut reader = artifacts
        .open(descriptor.digest)
        .map_err(|error| ReductionCorrectnessAttemptError::Unavailable(error.to_string()))?;
    if reader.identity().size_bytes != descriptor.size_bytes {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "structured correctness Artifact size changed".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| ReductionCorrectnessAttemptError::Unavailable(error.to_string()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != descriptor.size_bytes
        || Sha256Digest::digest_bytes(&bytes) != descriptor.digest
    {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "structured correctness Artifact identity changed".to_owned(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| ReductionCorrectnessAttemptError::Integrity(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloyport_core::{
        BundlePath, CandidateId, NetworkPolicy, ReductionCorpus, ReductionCorrectnessExperiment,
        ReductionExecutionFile, ReductionObservation, TaskId,
    };

    fn digest(label: &str) -> Sha256Digest {
        Sha256Digest::digest_bytes(label.as_bytes())
    }

    fn descriptor(label: &str, media_type: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            digest: digest(label),
            size_bytes: 128,
            media_type: media_type.to_owned(),
        }
    }

    fn target(worker_id: &str, image: &str) -> CorrectnessWorkerTarget {
        CorrectnessWorkerTarget::new(
            worker_id,
            descriptor(image, "application/vnd.oci.image.manifest.v1+json"),
            ResourceContract {
                cpu_millis: 1_000,
                memory_bytes: 1024 * 1024 * 1024,
                disk_bytes: 1024 * 1024 * 1024,
                process_count: 64,
                output_bytes: 8 * 1024 * 1024,
                device_count: 1,
                network: NetworkPolicy::Disabled,
            },
            60_000,
        )
        .expect("worker target")
    }

    fn spec() -> ReductionCorrectnessAttemptSpec {
        let experiment = ReductionCorrectnessExperiment::new(
            TaskId::try_from("task-correctness-adapter").expect("task ID"),
            CandidateId::try_from("candidate-correctness-adapter").expect("candidate ID"),
            digest("migration"),
            digest("manifest"),
            digest("source-gate"),
            digest("build-gate"),
            digest("corpus"),
            digest("policy"),
        );
        ReductionCorrectnessAttemptSpec {
            experiment,
            reference_bundle: descriptor(
                "reference-bundle",
                alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE,
            ),
            candidate_bundle: descriptor(
                "candidate-bundle",
                alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE,
            ),
        }
    }

    #[test]
    fn paired_assignments_are_stable_role_separated_and_shell_free() {
        let spec = spec();
        let cuda = target("cuda-worker", "cuda-image");
        let ascend = target("ascend-worker", "ascend-image");
        let reference = assignment(
            &spec,
            ReductionRunRole::CudaReference,
            &spec.reference_bundle,
            &cuda,
        )
        .expect("reference assignment");
        let repeated = assignment(
            &spec,
            ReductionRunRole::CudaReference,
            &spec.reference_bundle,
            &cuda,
        )
        .expect("stable reference assignment");
        let candidate = assignment(
            &spec,
            ReductionRunRole::AscendCandidate,
            &spec.candidate_bundle,
            &ascend,
        )
        .expect("candidate assignment");

        assert_eq!(reference, repeated);
        assert_ne!(reference.assignment_id, candidate.assignment_id);
        assert_eq!(
            reference.execution.executor_kind,
            ExecutionKind::CudaCorrectness
        );
        assert_eq!(
            candidate.execution.executor_kind,
            ExecutionKind::AscendCorrectness
        );
        assert_eq!(reference.execution.argv, [RUNNER_ARGV]);
        assert_eq!(candidate.execution.argv, [RUNNER_ARGV]);
        assert!(reference.execution.environment.is_empty());
        assert!(candidate.execution.environment.is_empty());
        assert_eq!(
            reference.required_features,
            [CUDA_REDUCTION_CORRECTNESS_FEATURE]
        );
        assert_eq!(
            candidate.required_features,
            [ASCEND_REDUCTION_CORRECTNESS_FEATURE]
        );
    }

    #[test]
    fn correctness_target_requires_one_offline_device() {
        let mut limits = target("cuda-worker", "cuda-image").limits;
        limits.network = NetworkPolicy::DependencyFetch;
        assert!(
            CorrectnessWorkerTarget::new(
                "cuda-worker",
                descriptor("cuda-image", "application/vnd.oci.image.manifest.v1+json"),
                limits,
                60_000,
            )
            .is_err()
        );
    }

    #[test]
    fn structured_run_must_bind_the_assigned_implementation() {
        let corpus = ReductionCorpus::fixture_v1();
        let experiment = ReductionCorrectnessExperiment::new(
            TaskId::try_from("task-correctness-run").expect("task ID"),
            CandidateId::try_from("candidate-correctness-run").expect("candidate ID"),
            digest("migration"),
            digest("manifest"),
            digest("source-gate"),
            digest("build-gate"),
            corpus.digest().expect("corpus digest"),
            digest("policy"),
        );
        let bundle = ReductionExecutionBundle::new(
            experiment.clone(),
            ReductionRunRole::CudaReference,
            correctness_callable(),
            corpus.clone(),
            vec![
                ReductionExecutionFile::new(
                    BundlePath::try_from("input/reference.cu").expect("bundle path"),
                    "extern \"C\" int reference();",
                )
                .expect("execution file"),
            ],
        )
        .expect("execution bundle");
        let spec = ReductionCorrectnessAttemptSpec {
            experiment: experiment.clone(),
            reference_bundle: descriptor(
                "reference-bundle",
                alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE,
            ),
            candidate_bundle: descriptor(
                "candidate-bundle",
                alloyport_core::REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE,
            ),
        };
        let case = &corpus.cases()[0];
        let observation = ReductionObservation {
            case_id: case.case_id.clone(),
            repetition: case.repetition,
            elements: case.elements,
            input_digest: case.input_digest(),
            status: 0,
            output_bits: Some(0),
            reorder_output_bits: Some(0),
        };
        let valid = ReductionRunReceipt::new(
            experiment.experiment_digest(),
            ReductionRunRole::CudaReference,
            None,
            bundle.implementation_digest(),
            experiment.corpus_digest(),
            digest("environment"),
            true,
            true,
            vec![observation.clone()],
        )
        .expect("valid run receipt");
        validate_structured_run(&spec, ReductionRunRole::CudaReference, &bundle, &valid)
            .expect("matching structured run");

        let forged = ReductionRunReceipt::new(
            experiment.experiment_digest(),
            ReductionRunRole::CudaReference,
            None,
            digest("different-implementation"),
            experiment.corpus_digest(),
            digest("environment"),
            true,
            true,
            vec![observation],
        )
        .expect("forged run receipt shape");
        assert!(
            validate_structured_run(&spec, ReductionRunRole::CudaReference, &bundle, &forged)
                .is_err()
        );
    }

    fn correctness_callable() -> alloyport_core::CorrectnessCallable {
        alloyport_core::CorrectnessCallable {
            public_symbol: "alloyport_reduce_sum_f32".to_owned(),
            reference_build_target: "reduce_sum".to_owned(),
            candidate_build_target: "alloyport_reduction_candidate".to_owned(),
        }
    }
}
