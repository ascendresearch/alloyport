use super::*;
use alloyport_artifacts::{ArtifactStore, IngestRequest};
use alloyport_core::{
    CandidateId, REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE, ReductionCaseKind, ReductionCorpus,
    ReductionCorrectnessAttemptError, ReductionCorrectnessAttemptFuture,
    ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptPort,
    ReductionCorrectnessAttemptSpec, ReductionCorrectnessExperiment, ReductionExecutionBundle,
    ReductionObservation, ReductionRunReceipt, ReductionRunRole,
};
use std::collections::{BTreeMap, VecDeque};
use std::io::{Cursor, Read};

#[derive(Clone, Copy, Debug)]
enum CorrectnessStep {
    Pending,
    Finished,
}

#[derive(Debug)]
struct FakeCorrectnessAttemptPort {
    artifacts: Arc<dyn ArtifactStore>,
    steps: VecDeque<CorrectnessStep>,
    experiments: Arc<Mutex<Vec<ReductionCorrectnessExperiment>>>,
}

impl FakeCorrectnessAttemptPort {
    fn invoke(
        &mut self,
        spec: &ReductionCorrectnessAttemptSpec,
    ) -> Result<ReductionCorrectnessAttemptObservation, ReductionCorrectnessAttemptError> {
        let experiment = &spec.experiment;
        let reference_bundle = read_execution_bundle(
            self.artifacts.as_ref(),
            &spec.reference_bundle,
            ReductionRunRole::CudaReference,
            experiment,
        )?;
        let candidate_bundle = read_execution_bundle(
            self.artifacts.as_ref(),
            &spec.candidate_bundle,
            ReductionRunRole::AscendCandidate,
            experiment,
        )?;
        if reference_bundle.corpus() != candidate_bundle.corpus() {
            return Err(ReductionCorrectnessAttemptError::Integrity(
                "paired runners received different corpora".to_owned(),
            ));
        }
        self.experiments
            .lock()
            .expect("experiment log")
            .push(experiment.clone());
        match self.steps.pop_front().expect("correctness step") {
            CorrectnessStep::Pending => Ok(ReductionCorrectnessAttemptObservation::Pending {
                diagnostic_digest: digest("correctness-pending"),
            }),
            CorrectnessStep::Finished => {
                let observations = observations(reference_bundle.corpus());
                let reference = ReductionRunReceipt::new(
                    experiment.experiment_digest(),
                    ReductionRunRole::CudaReference,
                    None,
                    reference_bundle.implementation_digest(),
                    experiment.corpus_digest(),
                    digest("cuda-environment"),
                    true,
                    true,
                    observations.clone(),
                )
                .map_err(|error| ReductionCorrectnessAttemptError::Integrity(error.to_string()))?;
                let candidate = ReductionRunReceipt::new(
                    experiment.experiment_digest(),
                    ReductionRunRole::AscendCandidate,
                    Some(experiment.candidate_id().clone()),
                    candidate_bundle.implementation_digest(),
                    experiment.corpus_digest(),
                    digest("ascend-environment"),
                    true,
                    true,
                    observations,
                )
                .map_err(|error| ReductionCorrectnessAttemptError::Integrity(error.to_string()))?;
                Ok(ReductionCorrectnessAttemptObservation::Finished {
                    reference_run: ingest_json(self.artifacts.as_ref(), &reference),
                    candidate_run: ingest_json(self.artifacts.as_ref(), &candidate),
                })
            }
        }
    }
}

impl ReductionCorrectnessAttemptPort for FakeCorrectnessAttemptPort {
    fn dispatch<'a>(
        &'a mut self,
        spec: &'a ReductionCorrectnessAttemptSpec,
    ) -> ReductionCorrectnessAttemptFuture<'a> {
        Box::pin(async move { self.invoke(spec) })
    }

    fn reconcile<'a>(
        &'a mut self,
        spec: &'a ReductionCorrectnessAttemptSpec,
    ) -> ReductionCorrectnessAttemptFuture<'a> {
        Box::pin(async move { self.invoke(spec) })
    }
}

fn observations(corpus: &ReductionCorpus) -> Vec<ReductionObservation> {
    corpus
        .cases()
        .iter()
        .map(|case| {
            let (status, output) = match case.kind {
                ReductionCaseKind::Valid => (
                    0,
                    Some(if case.elements == 0 {
                        0.0
                    } else {
                        f32::from(u16::try_from(case.elements % 997).expect("bounded remainder"))
                            + f32::from(u16::try_from(case.seed % 97).expect("bounded seed"))
                                / 1_000.0
                    }),
                ),
                ReductionCaseKind::NullInput | ReductionCaseKind::NullOutput => (1, None),
                ReductionCaseKind::UnsupportedSize => (3, None),
            };
            ReductionObservation {
                case_id: case.case_id.clone(),
                repetition: case.repetition,
                elements: case.elements,
                input_digest: case.input_digest(),
                status,
                output_bits: output.map(f32::to_bits),
                reorder_output_bits: output.map(f32::to_bits),
            }
        })
        .collect()
}

fn read_execution_bundle(
    artifacts: &dyn ArtifactStore,
    descriptor: &ArtifactDescriptor,
    role: ReductionRunRole,
    experiment: &ReductionCorrectnessExperiment,
) -> Result<ReductionExecutionBundle, ReductionCorrectnessAttemptError> {
    if descriptor.media_type != REDUCTION_EXECUTION_BUNDLE_MEDIA_TYPE {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "unexpected execution bundle media type".to_owned(),
        ));
    }
    let mut reader = artifacts
        .open(descriptor.digest)
        .map_err(|error| ReductionCorrectnessAttemptError::Unavailable(error.to_string()))?;
    if reader.identity().size_bytes != descriptor.size_bytes {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "execution bundle size changed".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| ReductionCorrectnessAttemptError::Unavailable(error.to_string()))?;
    if Sha256Digest::digest_bytes(&bytes) != descriptor.digest {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "execution bundle digest changed".to_owned(),
        ));
    }
    let bundle: ReductionExecutionBundle = serde_json::from_slice(&bytes)
        .map_err(|error| ReductionCorrectnessAttemptError::Integrity(error.to_string()))?;
    if bundle.role() != role || bundle.experiment() != experiment {
        return Err(ReductionCorrectnessAttemptError::Integrity(
            "execution bundle assignment identity changed".to_owned(),
        ));
    }
    Ok(bundle)
}

fn ingest_json<T: serde::Serialize>(
    artifacts: &dyn ArtifactStore,
    value: &T,
) -> ArtifactDescriptor {
    let bytes = serde_json::to_vec(value).expect("serialize receipt");
    let digest = Sha256Digest::digest_bytes(&bytes);
    let size_bytes = u64::try_from(bytes.len()).expect("receipt size");
    artifacts
        .ingest(
            &mut Cursor::new(&bytes),
            IngestRequest {
                expected_digest: Some(digest),
                expected_size_bytes: Some(size_bytes),
            },
        )
        .expect("ingest receipt");
    ArtifactDescriptor {
        digest,
        size_bytes,
        media_type: "application/json".to_owned(),
    }
}

fn reference_sources() -> BTreeMap<alloyport_core::BundlePath, Vec<u8>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/migrations/cuda-reduction-v1");
    let spec = migration_spec();
    spec.sources()
        .device_sources()
        .iter()
        .chain(spec.sources().host_sources())
        .chain(spec.sources().build_files())
        .map(|path| {
            (
                path.clone(),
                std::fs::read(root.join(path.as_str())).expect("read reference source"),
            )
        })
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)]
fn passing_build_dispatches_one_stable_calibrated_correctness_experiment()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(32 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let context = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let assignments = Arc::new(Mutex::new(Vec::new()));
    let mut build_gateway =
        CandidateToolGateway::new(context.clone(), artifacts.clone(), workspace.path())?
            .with_ascend_build(
                build_config()?,
                Box::new(FakeBuildAttemptPort::new(
                    [
                        FakeBuildStep::Pending,
                        FakeBuildStep::Finished {
                            outcome: AttemptOutcome::Succeeded,
                            build_completed: true,
                        },
                    ],
                    assignments,
                )),
            );
    let (_, submit_result) = execute(
        &mut build_gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "correctness-submit",
        ),
    );
    let submission = read_json(artifacts.as_ref(), submit_result);
    let candidate_id =
        CandidateId::try_from(submission["candidate_id"].as_str().expect("candidate ID"))?;
    let manifest_digest: Sha256Digest =
        serde_json::from_value(submission["manifest"]["digest"].clone())?;
    let (_, source_gate_receipt_digest) = execute(
        &mut build_gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":manifest_digest}),
            "correctness-source",
        ),
    );
    let build_request = invocation(
        REQUEST_ASCEND_BUILD_TOOL,
        &json!({
            "manifest_digest":manifest_digest,
            "source_gate_receipt_digest":source_gate_receipt_digest
        }),
        "correctness-build",
    );
    assert!(matches!(
        complete_immediate(build_gateway.execute(&build_request))?,
        alloyport_core::ToolGatewayOutcome::Pending { .. }
    ));
    let alloyport_core::ToolGatewayOutcome::Completed {
        result_digest: build_gate_receipt_digest,
        status: ToolOperationStatus::Succeeded,
        ..
    } = complete_immediate(build_gateway.reconcile(&build_request))?
    else {
        panic!("passing Build Gate expected");
    };

    let experiments = Arc::new(Mutex::new(Vec::new()));
    let correctness = FakeCorrectnessAttemptPort {
        artifacts: artifacts.clone(),
        steps: [CorrectnessStep::Pending, CorrectnessStep::Finished]
            .into_iter()
            .collect(),
        experiments: Arc::clone(&experiments),
    };
    let mut gateway = CandidateToolGateway::new(context, artifacts.clone(), workspace.path())?
        .with_reduction_correctness(
            CandidateCorrectnessToolConfig::reduction_fixture_v1(
                &migration_spec(),
                reference_sources(),
            )?,
            Box::new(correctness),
        );
    assert!(
        gateway
            .descriptor(REQUEST_REDUCTION_CORRECTNESS_TOOL)
            .is_some()
    );
    let request = invocation(
        REQUEST_REDUCTION_CORRECTNESS_TOOL,
        &json!({
            "candidate_id":candidate_id,
            "manifest_digest":manifest_digest,
            "source_gate_receipt_digest":source_gate_receipt_digest,
            "build_gate_receipt_digest":build_gate_receipt_digest
        }),
        "correctness-run",
    );
    assert!(matches!(
        complete_immediate(gateway.execute(&request))?,
        alloyport_core::ToolGatewayOutcome::Pending { .. }
    ));
    let alloyport_core::ToolGatewayOutcome::Completed {
        status,
        result_digest,
        receipt_digests,
        satisfies_subtask,
    } = complete_immediate(gateway.reconcile(&request))?
    else {
        panic!("terminal correctness result expected");
    };
    assert_eq!(status, ToolOperationStatus::Succeeded);
    assert!(satisfies_subtask);
    assert_eq!(receipt_digests.len(), 2);
    assert_eq!(
        read_json(artifacts.as_ref(), result_digest)["verdict"],
        "PASS"
    );
    let experiments = experiments.lock().expect("experiment log");
    assert_eq!(experiments.len(), 2);
    assert_eq!(experiments[0], experiments[1]);
    assert_eq!(
        experiments[0].corpus_digest(),
        ReductionCorpus::fixture_v1().digest()?
    );
    Ok(())
}

#[test]
fn correctness_rejects_non_build_evidence_before_dispatch() -> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(8 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let experiments = Arc::new(Mutex::new(Vec::new()));
    let correctness = FakeCorrectnessAttemptPort {
        artifacts: artifacts.clone(),
        steps: [CorrectnessStep::Finished].into_iter().collect(),
        experiments: Arc::clone(&experiments),
    };
    let mut gateway = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-candidate-tools")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        workspace.path(),
    )?
    .with_reduction_correctness(
        CandidateCorrectnessToolConfig::reduction_fixture_v1(
            &migration_spec(),
            reference_sources(),
        )?,
        Box::new(correctness),
    );
    let fake_receipt = ingest_json(
        artifacts.as_ref(),
        &json!({"passed":true,"kind":"not-a-build-receipt"}),
    );
    let request = invocation(
        REQUEST_REDUCTION_CORRECTNESS_TOOL,
        &json!({
            "candidate_id":"candidate-not-built",
            "manifest_digest":digest("manifest"),
            "source_gate_receipt_digest":digest("source"),
            "build_gate_receipt_digest":fake_receipt.digest
        }),
        "correctness-unbuilt",
    );
    assert!(complete_immediate(gateway.execute(&request)).is_err());
    assert!(experiments.lock().expect("experiment log").is_empty());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn durable_episode_completes_only_after_the_calibrated_correctness_gate()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(32 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let context = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut preflight =
        CandidateToolGateway::new(context.clone(), artifacts.clone(), workspace.path())?
            .with_ascend_build(
                build_config()?,
                Box::new(FakeBuildAttemptPort::new(
                    [FakeBuildStep::Finished {
                        outcome: AttemptOutcome::Succeeded,
                        build_completed: true,
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                )),
            );
    let (_, submit_result) = execute(
        &mut preflight,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "episode-correctness-submit",
        ),
    );
    let submission = read_json(artifacts.as_ref(), submit_result);
    let candidate_id =
        CandidateId::try_from(submission["candidate_id"].as_str().expect("candidate ID"))?;
    let manifest_digest: Sha256Digest =
        serde_json::from_value(submission["manifest"]["digest"].clone())?;
    let (_, source_gate_receipt_digest) = execute(
        &mut preflight,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":manifest_digest}),
            "episode-correctness-source",
        ),
    );
    let (_, build_gate_receipt_digest) = execute(
        &mut preflight,
        &invocation(
            REQUEST_ASCEND_BUILD_TOOL,
            &json!({
                "manifest_digest":manifest_digest,
                "source_gate_receipt_digest":source_gate_receipt_digest
            }),
            "episode-correctness-build",
        ),
    );

    let correctness = FakeCorrectnessAttemptPort {
        artifacts: artifacts.clone(),
        steps: [CorrectnessStep::Pending, CorrectnessStep::Finished]
            .into_iter()
            .collect(),
        experiments: Arc::new(Mutex::new(Vec::new())),
    };
    let mut tools = CandidateToolGateway::new(context, artifacts, workspace.path())?
        .with_reduction_correctness(
            CandidateCorrectnessToolConfig::reduction_fixture_v1(
                &migration_spec(),
                reference_sources(),
            )?,
            Box::new(correctness),
        );
    let continuations = [
        digest("correctness-episode-c1"),
        digest("correctness-episode-c2"),
    ];
    let mut models = OrderedModelGateway::new([
        exchange(
            "correctness-episode-request",
            tool_turn(
                "correctness-episode-request",
                REQUEST_REDUCTION_CORRECTNESS_TOOL,
                &json!({
                    "candidate_id":candidate_id,
                    "manifest_digest":manifest_digest,
                    "source_gate_receipt_digest":source_gate_receipt_digest,
                    "build_gate_receipt_digest":build_gate_receipt_digest
                }),
            ),
            continuations[0],
        ),
        exchange(
            "correctness-episode-final",
            GatewayTurn {
                narrative: vec!["candidate passed calibrated differential correctness".to_owned()],
                tool_calls: Vec::new(),
                stop_reason: NormalizedStopReason::Stop,
                usage: None,
            },
            continuations[1],
        ),
    ]);
    let episode_id = EpisodeId::try_from("episode-real-source-gate")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(runtime_state()?)?;
    let runner = AgentLoopRunner::new(episode_id.clone());
    let outcome = complete_immediate(async {
        for _ in 0..48 {
            let outcome = runner
                .advance(
                    &mut repository,
                    &mut models,
                    &mut tools,
                    &mut NoAgentRuntimeFault,
                )
                .await?;
            if matches!(outcome, AgentLoopAdvance::Terminal(_)) {
                return Ok::<_, alloyport_core::AgentLoopRuntimeError>(outcome);
            }
        }
        Ok(AgentLoopAdvance::Progressed(EpisodeStatus::Created))
    })?;
    assert_eq!(
        outcome,
        AgentLoopAdvance::Terminal(EpisodeStatus::Succeeded)
    );
    let state = repository.load(&episode_id)?.state;
    assert_eq!(state.turn_count(), 2);
    assert_eq!(state.tool_statuses(), [ToolOperationStatus::Succeeded]);
    Ok(())
}
