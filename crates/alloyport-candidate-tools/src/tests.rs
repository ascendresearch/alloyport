use super::*;
use crate::materialization::{CandidateMaterialization, CandidateMaterializationError};
use alloyport_artifacts::{ArtifactStore, InMemoryArtifactStore};
use alloyport_core::{
    AgentEpisodeRecord, AgentLoopAdvance, AgentLoopPolicy, AgentLoopRunner, AgentLoopRuntimeSpec,
    AgentToolGateway, ArtifactDescriptor, AscendBuildAttemptFuture, AscendBuildAttemptObservation,
    AscendBuildAttemptPort, AscendBuildEnvironment, AscendBuildTerminal, AssignmentContract,
    AttemptOutcome, DurableEpisodeState, EpisodeId, EpisodeRepository, EpisodeSpec, EpisodeStatus,
    GatewayToolCall, GatewayTurn, GatewayTurnExchange, InMemoryEpisodeRepository, ModelGateway,
    ModelGatewayError, ModelGatewayFuture, ModelGatewayOutcome, ModelTurnRequest, NetworkPolicy,
    NoAgentRuntimeFault, NormalizedStopReason, ResourceContract, ScriptedFakeModelGateway,
    ScriptedGatewayStep, SearchRunId, Sha256Digest, TaskId, ToolGatewayError, ToolInvocation,
    ToolOperationId, ToolOperationStatus,
};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::error::Error;
use std::io::Read;
use std::sync::{Arc, Mutex};

fn digest(label: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(label.as_bytes())
}

fn migration_spec() -> alloyport_core::MigrationSpec {
    serde_json::from_slice(include_bytes!(
        "../../../fixtures/migrations/cuda-reduction-v1/migration-spec-v1.json"
    ))
    .expect("migration spec")
}

fn mapping() -> String {
    [
        "input/src/reduce_sum_kernel.cu",
        "input/include/reduce_sum.h",
        "input/src/reduce_sum_launch.cu",
        "input/tests/reference_main.cpp",
        "input/CMakeLists.txt",
        "generated/reduce_sum.cpp",
        "generated/reduce_sum_host.cpp",
        "generated/CMakeLists.txt",
    ]
    .join(" -> mapped\n")
}

fn bundle(valid: bool, parent: Option<&str>) -> Value {
    let device = if valid {
        "#include <kernel_operator.h>\nextern \"C\" __global__ __aicore__ void reduce_sum(GM_ADDR x) { AscendC::GlobalTensor<float> input; }"
    } else {
        "#include <torch/extension.h>\nauto reduce_sum() { return at::sum(input); }"
    };
    json!({
        "parent_candidate_id": parent,
        "bundle": {
            "files": [
                {"path":"generated/reduce_sum.cpp","kind":"ascend_c_device","contents":device},
                {"path":"generated/reduce_sum_host.cpp","kind":"ascend_host","contents":"extern \"C\" int alloyport_reduce_sum_f32() { ACLRT_LAUNCH_KERNEL(reduce_sum); return 0; }"},
                {"path":"generated/CMakeLists.txt","kind":"build_integration","contents":"add_library(port reduce_sum.cpp reduce_sum_host.cpp)"},
                {"path":"generated/component-map.txt","kind":"component_mapping","contents":mapping()}
            ],
            "author_notes": ["untrusted fixture proposal"]
        }
    })
}

fn invocation(name: &str, arguments: &Value, suffix: &str) -> ToolInvocation {
    ToolInvocation {
        operation_id: ToolOperationId::try_from(format!("operation-{suffix}")).expect("operation"),
        call: GatewayToolCall {
            native_call_id: format!("call-{suffix}"),
            name: name.to_owned(),
            raw_arguments: serde_json::to_vec(&arguments).expect("arguments"),
        },
        input_identity_digest: digest(&format!("input-{suffix}")),
    }
}

fn read_json(artifacts: &dyn ArtifactStore, digest: Sha256Digest) -> Value {
    let mut reader = artifacts.open(digest).expect("open Artifact");
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).expect("read Artifact");
    serde_json::from_slice(&bytes).expect("JSON Artifact")
}

fn execute(
    gateway: &mut CandidateToolGateway,
    request: &ToolInvocation,
) -> (ToolOperationStatus, Sha256Digest) {
    let alloyport_core::ToolGatewayOutcome::Completed {
        status,
        result_digest,
        ..
    } = complete_immediate(gateway.execute(request)).expect("tool execution")
    else {
        panic!("local candidate tools must be determinate");
    };
    (status, result_digest)
}

fn build_config() -> Result<CandidateBuildToolConfig, Box<dyn Error>> {
    Ok(CandidateBuildToolConfig::new(
        ArtifactDescriptor {
            digest: digest("pinned-build-image"),
            size_bytes: 1,
            media_type: "application/vnd.oci.image.manifest.v1+json".to_owned(),
        },
        30_000,
        ResourceContract {
            cpu_millis: 2_000,
            memory_bytes: 1024 * 1024 * 1024,
            disk_bytes: 256 * 1024 * 1024,
            process_count: 64,
            output_bytes: 1024 * 1024,
            device_count: 1,
            network: NetworkPolicy::Disabled,
        },
    )?)
}

#[derive(Clone, Copy, Debug)]
enum FakeBuildStep {
    Pending,
    Finished {
        outcome: AttemptOutcome,
        build_completed: bool,
    },
}

#[derive(Debug)]
struct FakeBuildAttemptPort {
    steps: VecDeque<FakeBuildStep>,
    assignments: Arc<Mutex<Vec<AssignmentContract>>>,
}

impl FakeBuildAttemptPort {
    fn new(
        steps: impl IntoIterator<Item = FakeBuildStep>,
        assignments: Arc<Mutex<Vec<AssignmentContract>>>,
    ) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            assignments,
        }
    }

    fn invoke(&mut self, assignment: &AssignmentContract) -> AscendBuildAttemptObservation {
        self.assignments
            .lock()
            .expect("assignment log")
            .push(assignment.clone());
        match self.steps.pop_front().expect("scripted build step") {
            FakeBuildStep::Pending => AscendBuildAttemptObservation::Pending {
                diagnostic_digest: digest("build-pending"),
            },
            FakeBuildStep::Finished {
                outcome,
                build_completed,
            } => AscendBuildAttemptObservation::Finished(Box::new(AscendBuildTerminal {
                assignment_id: assignment.assignment_id.clone(),
                attempt_id: assignment.attempt_id.clone(),
                outcome,
                exit_code: Some(i32::from(outcome != AttemptOutcome::Succeeded)),
                elapsed_ms: 12,
                detail: "bounded fake compiler result".to_owned(),
                build_completed,
                environment: AscendBuildEnvironment {
                    architecture: "Ascend950PR".to_owned(),
                    cann_version: "9.1.0-beta.1".to_owned(),
                    driver_version: "25.7.rc1.6".to_owned(),
                    firmware_version: "9.0.0.105.229".to_owned(),
                },
                worker_receipt: None,
                stdout: None,
                stderr: None,
            })),
        }
    }
}

impl AscendBuildAttemptPort for FakeBuildAttemptPort {
    fn dispatch<'a>(
        &'a mut self,
        assignment: &'a AssignmentContract,
    ) -> AscendBuildAttemptFuture<'a> {
        Box::pin(async move { Ok(self.invoke(assignment)) })
    }

    fn reconcile<'a>(
        &'a mut self,
        assignment: &'a AssignmentContract,
    ) -> AscendBuildAttemptFuture<'a> {
        Box::pin(async move { Ok(self.invoke(assignment)) })
    }
}

#[test]
fn candidate_submission_is_create_only_idempotent_and_source_gate_is_independent()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut gateway = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?;
    let submit = invocation(
        SUBMIT_CANDIDATE_BUNDLE_TOOL,
        &bundle(false, None),
        "submit-bad",
    );
    let (status, result_digest) = execute(&mut gateway, &submit);
    assert_eq!(status, ToolOperationStatus::Succeeded);
    assert_eq!(execute(&mut gateway, &submit).1, result_digest);
    let result = read_json(artifacts.as_ref(), result_digest);
    let manifest_digest: Sha256Digest =
        serde_json::from_value(result["manifest"]["digest"].clone())?;
    let mut foreign_gateway = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-foreign-context")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        workspace.path(),
    )?;
    let gate = invocation(
        REQUEST_SOURCE_GATE_TOOL,
        &json!({"manifest_digest":manifest_digest}),
        "gate-bad",
    );
    assert!(matches!(
        complete_immediate(foreign_gateway.execute(&gate)),
        Err(ToolGatewayError::Adapter(message))
            if message.contains("does not belong to this migration context")
    ));
    let (status, receipt_digest) = execute(&mut gateway, &gate);
    assert_eq!(status, ToolOperationStatus::CandidateFailed);
    let receipt = read_json(artifacts.as_ref(), receipt_digest);
    assert_eq!(receipt["passed"], false);
    assert!(
        receipt["failures"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn ascend_build_requires_the_exact_source_receipt_and_reconciles_one_stable_assignment()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let assignments = Arc::new(Mutex::new(Vec::new()));
    let attempts = FakeBuildAttemptPort::new(
        [
            FakeBuildStep::Pending,
            FakeBuildStep::Finished {
                outcome: AttemptOutcome::CandidateFailed,
                build_completed: false,
            },
        ],
        Arc::clone(&assignments),
    );
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut gateway = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?
        .with_ascend_build(build_config()?, Box::new(attempts));
    let (_, result_digest) = execute(
        &mut gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "build-submit",
        ),
    );
    let result = read_json(artifacts.as_ref(), result_digest);
    let candidate_id = result["candidate_id"].as_str().expect("candidate ID");
    let manifest_digest: Sha256Digest =
        serde_json::from_value(result["manifest"]["digest"].clone())?;
    let (_, source_receipt_digest) = execute(
        &mut gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":manifest_digest}),
            "build-source-gate",
        ),
    );
    let invalid_build = invocation(
        REQUEST_ASCEND_BUILD_TOOL,
        &json!({
            "manifest_digest":manifest_digest,
            "source_gate_receipt_digest":digest("not-the-source-receipt")
        }),
        "build-invalid-receipt",
    );
    assert!(complete_immediate(gateway.execute(&invalid_build)).is_err());
    assert!(assignments.lock().expect("assignment log").is_empty());

    let (_, child_result_digest) = execute(
        &mut gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, Some(candidate_id)),
            "build-foreign-submit",
        ),
    );
    let child_result = read_json(artifacts.as_ref(), child_result_digest);
    let child_manifest: Sha256Digest =
        serde_json::from_value(child_result["manifest"]["digest"].clone())?;
    let (_, foreign_source_receipt) = execute(
        &mut gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":child_manifest}),
            "build-foreign-source-gate",
        ),
    );
    let foreign_build = invocation(
        REQUEST_ASCEND_BUILD_TOOL,
        &json!({
            "manifest_digest":manifest_digest,
            "source_gate_receipt_digest":foreign_source_receipt
        }),
        "build-foreign-receipt",
    );
    assert!(matches!(
        complete_immediate(gateway.execute(&foreign_build)),
        Err(ToolGatewayError::Adapter(message)) if message.contains("SourceGateReceiptMismatch")
    ));
    assert!(assignments.lock().expect("assignment log").is_empty());

    let (_, failing_result_digest) = execute(
        &mut gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(false, None),
            "build-failing-submit",
        ),
    );
    let failing_result = read_json(artifacts.as_ref(), failing_result_digest);
    let failing_manifest: Sha256Digest =
        serde_json::from_value(failing_result["manifest"]["digest"].clone())?;
    let (_, failing_source_receipt) = execute(
        &mut gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest":failing_manifest}),
            "build-failing-source-gate",
        ),
    );
    let failing_build = invocation(
        REQUEST_ASCEND_BUILD_TOOL,
        &json!({
            "manifest_digest":failing_manifest,
            "source_gate_receipt_digest":failing_source_receipt
        }),
        "build-failing-receipt",
    );
    assert!(matches!(
        complete_immediate(gateway.execute(&failing_build)),
        Err(ToolGatewayError::Adapter(message)) if message.contains("SourceGateDidNotPass")
    ));
    assert!(assignments.lock().expect("assignment log").is_empty());

    let build = invocation(
        REQUEST_ASCEND_BUILD_TOOL,
        &json!({
            "manifest_digest":manifest_digest,
            "source_gate_receipt_digest":source_receipt_digest
        }),
        "build-valid",
    );
    assert!(matches!(
        complete_immediate(gateway.execute(&build))?,
        alloyport_core::ToolGatewayOutcome::Pending { .. }
    ));
    let alloyport_core::ToolGatewayOutcome::Completed {
        status,
        result_digest: build_receipt_digest,
        satisfies_subtask,
        ..
    } = complete_immediate(gateway.reconcile(&build))?
    else {
        panic!("terminal fake build observation expected");
    };
    assert_eq!(status, ToolOperationStatus::CandidateFailed);
    assert!(!satisfies_subtask);
    assert_eq!(
        read_json(artifacts.as_ref(), build_receipt_digest)["passed"],
        false
    );

    let assignments = assignments.lock().expect("assignment log");
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0], assignments[1]);
    assert_eq!(
        assignments[0].execution.executor_kind,
        alloyport_core::ExecutionKind::AscendBuild
    );
    assert_eq!(assignments[0].execution.argv, ["build-v1"]);
    assert!(assignments[0].execution.environment.is_empty());
    assert_eq!(
        assignments[0]
            .execution
            .limits
            .as_ref()
            .expect("limits")
            .network,
        NetworkPolicy::Disabled
    );
    let build_bundle = read_json(artifacts.as_ref(), assignments[0].execution.bundle.digest);
    assert_eq!(build_bundle["files"].as_array().map(Vec::len), Some(4));
    Ok(())
}

#[test]
fn materialization_rejects_a_manifest_candidate_id_that_is_not_one_path_segment()
-> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-path")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut gateway = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?;
    let (_, result_digest) = execute(
        &mut gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "unsafe-path-fixture",
        ),
    );
    let result = read_json(artifacts.as_ref(), result_digest);
    let manifest_digest: Sha256Digest =
        serde_json::from_value(result["manifest"]["digest"].clone())?;
    let mut manifest = read_json(artifacts.as_ref(), manifest_digest);
    manifest["candidate_id"] = json!("../../outside");
    let manifest: alloyport_core::CandidateSourceManifest = serde_json::from_value(manifest)?;

    assert!(matches!(
        CandidateMaterialization::materialize(
            workspace.path(),
            artifacts.as_ref(),
            &manifest,
            manifest_digest,
        ),
        Err(CandidateMaterializationError::UnsafeCandidateId)
    ));
    Ok(())
}

fn tool_turn(call_id: &str, name: &str, arguments: &Value) -> GatewayTurn {
    GatewayTurn {
        narrative: Vec::new(),
        tool_calls: vec![GatewayToolCall {
            native_call_id: call_id.to_owned(),
            name: name.to_owned(),
            raw_arguments: serde_json::to_vec(&arguments).expect("arguments"),
        }],
        stop_reason: NormalizedStopReason::ToolCalls,
        usage: None,
    }
}

fn exchange(label: &str, turn: GatewayTurn, continuation: Sha256Digest) -> GatewayTurnExchange {
    GatewayTurnExchange {
        turn,
        raw_exchange_digest: digest(&format!("{label}-raw")),
        native_continuation_digest: continuation,
    }
}

fn runtime_state() -> Result<DurableEpisodeState, Box<dyn Error>> {
    let episode = AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from("episode-real-source-gate")?,
        task_id: TaskId::try_from("task-candidate-tools")?,
        search_run_id: SearchRunId::try_from("search-real-source-gate")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: "configured-model".to_owned(),
        resolved_model_digest: digest("resolved-model"),
        prompt_revision: "fixture-v1".to_owned(),
        tool_catalog_digest: digest("tools"),
        loop_policy_digest: digest("policy"),
        data_boundary_policy_digest: digest("boundary"),
        budget_snapshot_digest: digest("budget"),
    })?;
    Ok(DurableEpisodeState::new(AgentLoopRuntimeSpec {
        episode,
        policy: AgentLoopPolicy {
            max_model_turns: 6,
            max_model_attempts: 6,
            max_ambiguous_model_attempts: 1,
            max_tool_calls_per_turn: 1,
            max_total_tool_operations: 4,
            max_stop_feedback_turns: 0,
        },
        initial_input_digest: digest("initial-input"),
        resolved_model_digest: digest("resolved-model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
    })?)
}

fn build_runtime_state() -> Result<DurableEpisodeState, Box<dyn Error>> {
    let episode = AgentEpisodeRecord::new(EpisodeSpec {
        id: EpisodeId::try_from("episode-real-build-gate")?,
        task_id: TaskId::try_from("task-candidate-tools")?,
        search_run_id: SearchRunId::try_from("search-real-build-gate")?,
        parent_candidate_id: None,
        subtask_contract_digest: digest("subtask"),
        context_projection_digest: digest("context"),
        input_artifact_root_digest: digest("input-root"),
        runtime_model_alias: "configured-model".to_owned(),
        resolved_model_digest: digest("resolved-model"),
        prompt_revision: "fixture-v1".to_owned(),
        tool_catalog_digest: digest("tools"),
        loop_policy_digest: digest("policy"),
        data_boundary_policy_digest: digest("boundary"),
        budget_snapshot_digest: digest("budget"),
    })?;
    Ok(DurableEpisodeState::new(AgentLoopRuntimeSpec {
        episode,
        policy: AgentLoopPolicy {
            max_model_turns: 8,
            max_model_attempts: 8,
            max_ambiguous_model_attempts: 1,
            max_tool_calls_per_turn: 1,
            max_total_tool_operations: 6,
            max_stop_feedback_turns: 0,
        },
        initial_input_digest: digest("initial-input"),
        resolved_model_digest: digest("resolved-model"),
        deployment_digest: digest("deployment"),
        model_profile_digest: digest("profile"),
        request_budget_digest: digest("request-budget"),
    })?)
}

#[derive(Debug)]
struct OrderedModelGateway {
    turns: VecDeque<GatewayTurnExchange>,
    next_turn_index: u32,
}

impl OrderedModelGateway {
    fn new(turns: impl IntoIterator<Item = GatewayTurnExchange>) -> Self {
        Self {
            turns: turns.into_iter().collect(),
            next_turn_index: 1,
        }
    }
}

impl ModelGateway for OrderedModelGateway {
    fn invoke<'a>(&'a mut self, request: &'a ModelTurnRequest) -> ModelGatewayFuture<'a> {
        Box::pin(async move {
            if request.turn_index != self.next_turn_index {
                return Err(ModelGatewayError::UnexpectedRequest {
                    expected_turn_index: self.next_turn_index,
                    actual_turn_index: request.turn_index,
                });
            }
            let exchange = self
                .turns
                .pop_front()
                .ok_or(ModelGatewayError::ScriptExhausted)?;
            exchange.turn.validate()?;
            self.next_turn_index += 1;
            Ok(ModelGatewayOutcome::Turn(exchange))
        })
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn same_episode_consumes_real_source_failure_and_submits_a_correction() -> Result<(), Box<dyn Error>>
{
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    let mut tools = CandidateToolGateway::new(config, artifacts.clone(), workspace.path())?;

    let bad_submit = invocation(
        SUBMIT_CANDIDATE_BUNDLE_TOOL,
        &bundle(false, None),
        "pre-bad",
    );
    let (_, bad_result) = execute(&mut tools, &bad_submit);
    let bad_json = read_json(artifacts.as_ref(), bad_result);
    let bad_candidate = bad_json["candidate_id"].as_str().expect("candidate ID");
    let bad_manifest: Sha256Digest =
        serde_json::from_value(bad_json["manifest"]["digest"].clone())?;
    let bad_gate = invocation(
        REQUEST_SOURCE_GATE_TOOL,
        &json!({"manifest_digest":bad_manifest}),
        "pre-gate-bad",
    );
    let (_, bad_receipt) = execute(&mut tools, &bad_gate);

    let good_bundle = bundle(true, Some(bad_candidate));
    let good_submit = invocation(SUBMIT_CANDIDATE_BUNDLE_TOOL, &good_bundle, "pre-good");
    let (_, good_result) = execute(&mut tools, &good_submit);
    let good_json = read_json(artifacts.as_ref(), good_result);
    let good_manifest: Sha256Digest =
        serde_json::from_value(good_json["manifest"]["digest"].clone())?;
    let good_gate = invocation(
        REQUEST_SOURCE_GATE_TOOL,
        &json!({"manifest_digest":good_manifest}),
        "pre-gate-good",
    );
    let (_, good_receipt) = execute(&mut tools, &good_gate);

    let continuations = [digest("c1"), digest("c2"), digest("c3"), digest("c4")];
    let input2 =
        alloyport_core::derive_model_continuation_input_digest(continuations[0], [bad_result]);
    let input3 =
        alloyport_core::derive_model_continuation_input_digest(continuations[1], [bad_receipt]);
    let input4 =
        alloyport_core::derive_model_continuation_input_digest(continuations[2], [good_result]);
    let input5 =
        alloyport_core::derive_model_continuation_input_digest(continuations[3], [good_receipt]);
    let mut models = ScriptedFakeModelGateway::new([
        ScriptedGatewayStep {
            expected_turn_index: 1,
            expected_input_digest: digest("initial-input"),
            outcome: ModelGatewayOutcome::Turn(exchange(
                "submit-bad",
                tool_turn(
                    "submit-bad",
                    SUBMIT_CANDIDATE_BUNDLE_TOOL,
                    &bundle(false, None),
                ),
                continuations[0],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 2,
            expected_input_digest: input2,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "gate-bad",
                tool_turn(
                    "gate-bad",
                    REQUEST_SOURCE_GATE_TOOL,
                    &json!({"manifest_digest":bad_manifest}),
                ),
                continuations[1],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 3,
            expected_input_digest: input3,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "submit-good",
                tool_turn("submit-good", SUBMIT_CANDIDATE_BUNDLE_TOOL, &good_bundle),
                continuations[2],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 4,
            expected_input_digest: input4,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "gate-good",
                tool_turn(
                    "gate-good",
                    REQUEST_SOURCE_GATE_TOOL,
                    &json!({"manifest_digest":good_manifest}),
                ),
                continuations[3],
            )),
        },
        ScriptedGatewayStep {
            expected_turn_index: 5,
            expected_input_digest: input5,
            outcome: ModelGatewayOutcome::Turn(exchange(
                "final",
                GatewayTurn {
                    narrative: vec![
                        "corrected candidate passed independent Source Gate".to_owned(),
                    ],
                    tool_calls: Vec::new(),
                    stop_reason: NormalizedStopReason::Stop,
                    usage: None,
                },
                digest("c5"),
            )),
        },
    ]);
    let episode_id = EpisodeId::try_from("episode-real-source-gate")?;
    let mut repository = InMemoryEpisodeRepository::default();
    repository.create(runtime_state()?)?;
    let runner = AgentLoopRunner::new(episode_id.clone());
    let outcome = complete_immediate(async {
        for _ in 0..64 {
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
    assert_eq!(state.turn_count(), 5);
    assert_eq!(state.tool_operation_count(), 4);
    Ok(())
}

#[path = "build_episode_tests.rs"]
mod build_episode_tests;

fn complete_immediate<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("scripted model must complete immediately"),
    }
}
