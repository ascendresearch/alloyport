//! What must never end a migration: an instrument naming nothing, a bad citation, a rejection with
//! no move in it. Each of these ended a paid run before the rule that prevents it existed.

use super::*;

#[test]
fn an_instrument_naming_something_that_does_not_exist_cannot_end_the_migration()
-> Result<(), Box<dyn Error>> {
    // `task-436fe144a291b285ec9547db` died here. The model asked for
    // `ops/ascendc-register-invoke-template`; the corpus holds `ascendc-registry-invoke-template`.
    // One letter, and a read-only instrument that grants no authority ended a paid migration.
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let corpus = crate::reference::ReferenceCorpus::load(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../vendor/cannbot-skills"),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../vendor/cannbot-skills-audit.jsonl"
        ),
    )?;
    let gateway = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-candidate-tools")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        workspace.path(),
    )?
    .with_reference(corpus);

    let call = GatewayToolCall {
        native_call_id: "call-typo".to_owned(),
        name: READ_REFERENCE_TOOL.to_owned(),
        raw_arguments: serde_json::to_vec(
            &json!({"document": "ops/ascendc-register-invoke-template"}),
        )?,
    };
    // Caught before authorization, so the reducer records terminal `RejectedAsInvalid` and the
    // Episode continues. The explanation must name what the model may actually ask for.
    let rejection = gateway
        .validate_call(&call)
        .expect_err("a document that does not exist must be returned to the model");
    let explanation = read_json(artifacts.as_ref(), rejection.result_digest);
    assert_eq!(explanation["recoverable"], json!(true));
    let reason = explanation["reason"].as_str().expect("reason");
    assert!(
        reason.contains("ops/ascendc-registry-invoke-template"),
        "the rejection must name the documents that do exist, or the model can only guess again"
    );

    // A real document still passes, so the check rejects the wrong name rather than the tool.
    let good = GatewayToolCall {
        native_call_id: "call-good".to_owned(),
        name: READ_REFERENCE_TOOL.to_owned(),
        raw_arguments: serde_json::to_vec(
            &json!({"document": "ops/ascendc-registry-invoke-template"}),
        )?,
    };
    assert!(gateway.validate_call(&good).is_ok());
    Ok(())
}

/// The split must cut both ways, or it is not a split.
///
/// Citation failures became recoverable so a wrong digest costs a turn instead of the run. That is
/// only correct if infrastructure failures stayed fatal: a store that cannot hold the candidate is
/// not something the model can fix by naming something else, and quietly handing it back as a
/// correctable rejection would invite it to retry forever against a broken machine.
#[test]
fn a_broken_store_stays_fatal_while_a_bad_citation_does_not() -> Result<(), Box<dyn Error>> {
    let workspace = tempfile::tempdir()?;
    let config = CandidateToolConfig::new(
        TaskId::try_from("task-candidate-tools")?,
        &migration_spec(),
        alloyport_core::GenerationStrategy::DirectAscendC,
    );
    // Far too small to hold the submitted bundle.
    let starved: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(8));
    let mut gateway = CandidateToolGateway::new(config, starved, workspace.path())?;
    let outcome = complete_immediate(gateway.execute(&invocation(
        SUBMIT_CANDIDATE_BUNDLE_TOOL,
        &bundle(true, None),
        "starved-submit",
    )));
    assert!(
        matches!(outcome, Err(ToolGatewayError::Adapter(_))),
        "an unusable artifact store is infrastructure, not a citation the model can correct"
    );
    Ok(())
}

/// Correcting one file must not require retyping the candidate.
///
/// A complete bundle costs 90-100% of one model response. On the first migration to reach a real
/// compiler, fixing a single `CMake` line meant re-emitting all four files, and the JSON truncated
/// mid-string at exactly the 16384-token ceiling: `EOF while parsing a string at line 1 column
/// 7687`. Inheriting keeps the candidate complete and immutable while the model retypes only what
/// changed.
#[test]
fn a_child_candidate_inherits_the_files_it_did_not_resend() -> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(16 * 1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let mut gateway = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-candidate-tools")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        workspace.path(),
    )?;

    let (_, first) = execute(
        &mut gateway,
        &invocation(
            SUBMIT_CANDIDATE_BUNDLE_TOOL,
            &bundle(true, None),
            "inherit-first",
        ),
    );
    let parent = read_json(artifacts.as_ref(), first);
    let parent_manifest: Sha256Digest =
        serde_json::from_value(parent["manifest"]["digest"].clone())?;
    let parent_files = parent["files"].as_array().expect("file list").len();
    assert_eq!(parent_files, 4, "the parent states what it contains");

    // One file, not four. Without inheritance this cannot form a candidate at all.
    let one_file = json!({
        "inherit_from_manifest_digest": parent_manifest,
        "bundle": {"files": [{
            "path": "generated/CMakeLists.txt",
            "kind": "build_integration",
            "contents": "add_library(alloyport_reduction_candidate reduce_sum.cpp reduce_sum_host.cpp)\ntarget_include_directories(alloyport_reduction_candidate PRIVATE $ENV{ASCEND_HOME_PATH}/x86_64-linux/include)"
        }]}
    });
    let (status, second) = execute(
        &mut gateway,
        &invocation(SUBMIT_CANDIDATE_BUNDLE_TOOL, &one_file, "inherit-second"),
    );
    assert_eq!(status, ToolOperationStatus::Succeeded);
    let child = read_json(artifacts.as_ref(), second);
    assert_eq!(
        child["files"].as_array().expect("file list").len(),
        4,
        "the child is a complete candidate, not a fragment"
    );
    assert_ne!(child["candidate_id"], parent["candidate_id"]);

    // The assembled child passes the Source Gate, so inheritance produced a real candidate.
    let child_manifest: Sha256Digest = serde_json::from_value(child["manifest"]["digest"].clone())?;
    let (gate_status, gate) = execute(
        &mut gateway,
        &invocation(
            REQUEST_SOURCE_GATE_TOOL,
            &json!({"manifest_digest": child_manifest}),
            "inherit-gate",
        ),
    );
    assert_eq!(gate_status, ToolOperationStatus::Succeeded);
    assert_eq!(
        gate_payload(artifacts.as_ref(), gate)["passed"],
        json!(true)
    );

    // A manifest from another migration is refused, recoverably.
    let foreign = json!({
        "inherit_from_manifest_digest": digest("not-a-manifest"),
        "bundle": {"files": [{"path":"generated/CMakeLists.txt","kind":"build_integration","contents":"x"}]}
    });
    let (foreign_status, _) = execute(
        &mut gateway,
        &invocation(SUBMIT_CANDIDATE_BUNDLE_TOOL, &foreign, "inherit-foreign"),
    );
    assert_eq!(foreign_status, ToolOperationStatus::CandidateFailed);
    Ok(())
}

/// A rejection must say what to do, not only what the parser saw.
///
/// Two live runs hit `EOF while parsing a string` after their submission ran out of output tokens,
/// and one hit `unknown field 'content'` for a missing letter. Both messages are accurate and tell
/// a model nothing about the move that fixes them.
#[test]
fn a_rejection_names_the_move_that_fixes_it() -> Result<(), Box<dyn Error>> {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(1024 * 1024));
    let workspace = tempfile::tempdir()?;
    let gateway = CandidateToolGateway::new(
        CandidateToolConfig::new(
            TaskId::try_from("task-candidate-tools")?,
            &migration_spec(),
            alloyport_core::GenerationStrategy::DirectAscendC,
        ),
        artifacts.clone(),
        workspace.path(),
    )?;

    // Arguments that stop mid-string, exactly as a response that ran out of room produces.
    let truncated = GatewayToolCall {
        native_call_id: "call-truncated".to_owned(),
        name: SUBMIT_CANDIDATE_BUNDLE_TOOL.to_owned(),
        raw_arguments: br#"{"bundle":{"files":[{"path":"generated/reduce_sum.cpp","contents":"#
            .to_vec(),
    };
    let rejection = gateway
        .validate_call(&truncated)
        .expect_err("a truncated call cannot be dispatched");
    let explanation = read_json(artifacts.as_ref(), rejection.result_digest);
    let guidance = explanation["guidance"].as_str().expect("guidance");
    assert!(
        guidance.contains("inherit_from_manifest_digest"),
        "a truncated submission must be pointed at the move that makes it fit: {guidance}"
    );

    // A field name that is one letter off.
    let misspelled = GatewayToolCall {
        native_call_id: "call-misspelled".to_owned(),
        name: SUBMIT_CANDIDATE_BUNDLE_TOOL.to_owned(),
        raw_arguments: br#"{"bundle":{"files":[{"path":"generated/x.cpp","kind":"ascend_c_device","content":"x"}]}}"#.to_vec(),
    };
    let rejection = gateway
        .validate_call(&misspelled)
        .expect_err("an unknown field cannot be dispatched");
    let explanation = read_json(artifacts.as_ref(), rejection.result_digest);
    assert!(
        explanation["guidance"]
            .as_str()
            .is_some_and(|guidance| guidance.contains("expected_arguments")),
        "a misspelled field must be pointed at the accepted names"
    );
    assert!(
        explanation["expected_arguments"]
            .as_str()
            .is_some_and(|contract| contract.contains("contents")),
        "and those names must actually be listed"
    );
    Ok(())
}
