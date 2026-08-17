use super::*;
use crate::gateway::ingest_bytes;
use crate::record_git::write_candidate_record;
use crate::record_stream::{candidate_ref, fast_import_stream};
use alloyport_artifacts::{ArtifactStore, InMemoryArtifactStore};
use alloyport_core::{
    ArtifactDescriptor, BundlePath, CandidateSourceFile, CandidateSourceManifest,
    CandidateSourceManifestSpec, GeneratedSourceKind, GenerationStrategy, TaskId, ToolEffectClass,
    ToolOperationId, ToolOperationSpec, ToolResultAuthority, TurnId,
};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

fn label(text: &str) -> Sha256Digest {
    Sha256Digest::digest_bytes(text.as_bytes())
}

fn source(path: &str, contents: &str) -> RecordedFile {
    RecordedFile {
        path: BundlePath::try_from(path).expect("bundle path"),
        digest: Sha256Digest::digest_bytes(contents.as_bytes()),
        bytes: contents.as_bytes().to_vec(),
    }
}

fn candidate(
    sequence: u32,
    id: &str,
    parent: Option<&str>,
    files: Vec<RecordedFile>,
) -> RecordedCandidate {
    RecordedCandidate {
        sequence,
        candidate_id: CandidateId::try_from(id).expect("candidate ID"),
        parent_candidate_id: parent.map(|parent| CandidateId::try_from(parent).expect("parent")),
        manifest_digest: label(&format!("{id}-manifest")),
        source_bundle_digest: label(&format!("{id}-bundle")),
        files,
        outcomes: Vec::new(),
    }
}

/// Runs git the same way the record does, so a test reads the repository rather than the writer.
fn git(root: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(arguments)
        .output()
        .expect("the candidate record requires git; a missing git must fail, not skip");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn the_same_history_produces_the_same_stream_twice() {
    let history = vec![
        candidate(
            1,
            "candidate-aaa1",
            None,
            vec![source("generated/a.cpp", "one")],
        ),
        candidate(
            2,
            "candidate-bbb2",
            Some("candidate-aaa1"),
            vec![source("generated/a.cpp", "two")],
        ),
    ];
    let first = fast_import_stream(&history).expect("stream");
    let second = fast_import_stream(&history).expect("stream");
    assert_eq!(first, second);
    // Nothing in the stream may come from the clock or the host, so no year can appear in it.
    let text = String::from_utf8(first).expect("stream is text plus payloads");
    assert!(text.contains("author AlloyPort candidate record <record@alloyport.invalid> 1 +0000"));
    assert!(
        text.contains("committer AlloyPort candidate record <record@alloyport.invalid> 2 +0000")
    );
}

#[test]
fn a_correction_reuses_the_blobs_it_did_not_change() {
    let unchanged = source("generated/CMakeLists.txt", "add_library(x a.cpp)");
    let history = vec![
        candidate(
            1,
            "candidate-aaa1",
            None,
            vec![source("generated/a.cpp", "one"), unchanged.clone()],
        ),
        candidate(
            2,
            "candidate-bbb2",
            Some("candidate-aaa1"),
            vec![source("generated/a.cpp", "two"), unchanged],
        ),
    ];
    let stream = String::from_utf8(fast_import_stream(&history).expect("stream")).expect("text");
    // Three distinct contents across two candidates of two files each.
    assert_eq!(stream.matches("\nmark :").count(), 5, "3 blobs, 2 commits");
    assert_eq!(
        stream.matches("blob\nmark :").count(),
        3,
        "the unchanged build file must be written once"
    );
}

#[test]
fn a_path_a_model_authored_cannot_become_a_stream_command() {
    // `BundlePath` forbids NUL, backslashes and traversal, and permits this. The model writes paths.
    let hostile = "generated/a.cpp\ndeleteall\nM 100644 :1 \"generated/injected\"";
    let history = vec![candidate(
        1,
        "candidate-aaa1",
        None,
        vec![source(hostile, "one")],
    )];
    let root = tempfile::tempdir().expect("record directory");
    let record = write_candidate_record(&root.path().join("record"), &history).expect("record");
    let listing = git(
        &record.root,
        &["ls-tree", "-r", "-z", "--name-only", "HEAD"],
    );
    let paths: BTreeSet<&str> = listing
        .split('\0')
        .filter(|entry| !entry.is_empty())
        .collect();
    // One entry, and it is the whole hostile string. Unquoted, git would have read the tail of the
    // path as two more commands and the tree would hold `generated/injected` beside `generated/a.cpp`.
    assert_eq!(paths, BTreeSet::from([hostile]));
    assert!(!paths.contains("generated/injected"));
    assert!(!paths.contains("generated/a.cpp"));
}

#[test]
fn each_commit_holds_exactly_its_own_candidate() {
    let history = vec![
        candidate(
            1,
            "candidate-aaa1",
            None,
            vec![
                source("generated/a.cpp", "one"),
                source("generated/dropped.cpp", "gone next time"),
            ],
        ),
        candidate(
            2,
            "candidate-bbb2",
            Some("candidate-aaa1"),
            vec![source("generated/a.cpp", "one")],
        ),
    ];
    let root = tempfile::tempdir().expect("record directory");
    let record = write_candidate_record(&root.path().join("record"), &history).expect("record");
    let second = git(
        &record.root,
        &["ls-tree", "-r", "--name-only", &candidate_ref(&history[1])],
    );
    assert_eq!(second.trim(), "generated/a.cpp");
    // The dropped file must be gone from the child and still present in the parent.
    let first = git(
        &record.root,
        &["ls-tree", "-r", "--name-only", &candidate_ref(&history[0])],
    );
    assert!(first.contains("generated/dropped.cpp"));
    let changes = git(
        &record.root,
        &[
            "diff",
            "--name-status",
            &candidate_ref(&history[0]),
            &candidate_ref(&history[1]),
        ],
    );
    assert_eq!(changes.trim(), "D\tgenerated/dropped.cpp");
}

#[test]
fn a_parent_the_record_cannot_reach_is_named_rather_than_dropped() {
    let history = vec![candidate(
        1,
        "candidate-child",
        Some("candidate-from-an-earlier-run"),
        vec![source("generated/a.cpp", "one")],
    )];
    let root = tempfile::tempdir().expect("record directory");
    let record = write_candidate_record(&root.path().join("record"), &history).expect("record");
    let lineage = git(&record.root, &["rev-list", "--parents", "-n", "1", "HEAD"]);
    assert_eq!(
        lineage.split_whitespace().count(),
        1,
        "a parent outside the record is a root commit"
    );
    let message = git(&record.root, &["log", "-1", "--format=%B", "HEAD"]);
    assert!(
        message.contains("candidate-from-an-earlier-run (not recorded in this Episode)"),
        "the commit must name the parent it could not reach: {message}"
    );
}

#[test]
fn a_record_refuses_a_directory_that_already_holds_something() {
    let root = tempfile::tempdir().expect("record directory");
    let occupied = root.path().join("record");
    std::fs::create_dir(&occupied).expect("directory");
    std::fs::write(occupied.join("existing"), b"do not clobber").expect("existing file");
    let history = vec![candidate(
        1,
        "candidate-aaa1",
        None,
        vec![source("generated/a.cpp", "one")],
    )];
    let error = write_candidate_record(&occupied, &history).expect_err("occupied");
    assert!(
        matches!(error, CandidateRecordError::Occupied(_)),
        "unexpected error: {error}"
    );
    assert!(occupied.join("existing").exists());
}

#[test]
fn an_empty_history_is_refused_rather_than_written_as_an_empty_repository() {
    assert!(matches!(
        fast_import_stream(&[]),
        Err(RecordStreamError::NoCandidates)
    ));
}

/// Builds a terminal operation the way the loop does, so a projection can be tested per shape.
fn operation(
    tool: &str,
    suffix: &str,
    result: Sha256Digest,
    receipts: Vec<Sha256Digest>,
) -> ToolOperationRecord {
    let mut record = ToolOperationRecord::new(ToolOperationSpec {
        id: ToolOperationId::try_from(format!("operation-{suffix}")).expect("operation"),
        episode_id: alloyport_core::EpisodeId::try_from("episode-record").expect("episode"),
        turn_id: TurnId::try_from(format!("turn-{suffix}")).expect("turn"),
        native_call_id: format!("call-{suffix}"),
        tool_name: tool.to_owned(),
        tool_version: "1".to_owned(),
        effect_class: ToolEffectClass::CandidateWrite,
        result_authority: ToolResultAuthority::Observed,
        arguments_digest: label(&format!("arguments-{suffix}")),
        input_identity_digest: label(&format!("input-{suffix}")),
    })
    .expect("operation record");
    record
        .transition(ToolOperationStatus::Authorized)
        .expect("authorized");
    record
        .transition(ToolOperationStatus::Dispatching)
        .expect("dispatching");
    record
        .finish(ToolOperationStatus::Succeeded, result, receipts)
        .expect("terminal");
    record
}

/// Stores one authored file and describes it the way a submission does.
fn stored_file(
    artifacts: &dyn ArtifactStore,
    path: &str,
    kind: GeneratedSourceKind,
    contents: &str,
) -> CandidateSourceFile {
    let stored = ingest_bytes(artifacts, contents.as_bytes()).expect("store source");
    CandidateSourceFile::new(
        BundlePath::try_from(path).expect("path"),
        kind,
        ArtifactDescriptor {
            digest: stored.digest,
            size_bytes: stored.size_bytes,
            media_type: "text/plain; charset=utf-8".to_owned(),
        },
    )
    .expect("source file")
}

/// A manifest is only valid with all four categories present, so every fixture supplies them.
fn complete_files(artifacts: &dyn ArtifactStore) -> Vec<CandidateSourceFile> {
    vec![
        stored_file(
            artifacts,
            "generated/reduce_sum.cpp",
            GeneratedSourceKind::AscendCDevice,
            "#include <kernel_operator.h>\n",
        ),
        stored_file(
            artifacts,
            "generated/reduce_sum_host.cpp",
            GeneratedSourceKind::AscendHost,
            "extern \"C\" int alloyport_reduce_sum_f32() { return 0; }\n",
        ),
        stored_file(
            artifacts,
            "generated/CMakeLists.txt",
            GeneratedSourceKind::BuildIntegration,
            "add_library(alloyport_reduction_candidate reduce_sum.cpp)\n",
        ),
        stored_file(
            artifacts,
            "generated/component-map.txt",
            GeneratedSourceKind::ComponentMapping,
            "input/src/reduce_sum_kernel.cu -> generated/reduce_sum.cpp\n",
        ),
    ]
}

/// Stores a manifest and the submission result naming it, returning the result digest.
fn stored_submission(
    artifacts: &dyn ArtifactStore,
    candidate_id: &str,
    files: Vec<CandidateSourceFile>,
) -> Sha256Digest {
    let manifest = CandidateSourceManifest::new(CandidateSourceManifestSpec {
        candidate_id: CandidateId::try_from(candidate_id).expect("candidate"),
        task_id: TaskId::try_from("task-record").expect("task"),
        parent_candidate_id: None,
        migration_spec_digest: label("spec"),
        generation_strategy: GenerationStrategy::DirectAscendC,
        public_symbol: "alloyport_reduce_sum_f32".to_owned(),
        build_target: "alloyport_reduction_candidate".to_owned(),
        input_source_paths: BTreeSet::from([BundlePath::try_from(
            "input/src/reduce_sum_kernel.cu",
        )
        .expect("input path")]),
        source_bundle_digest: label("bundle"),
        files,
    })
    .expect("manifest");
    let manifest_bytes = serde_json::to_vec(&manifest).expect("manifest bytes");
    let stored = ingest_bytes(artifacts, &manifest_bytes).expect("store manifest");
    let result = json!({
        "candidate_id": candidate_id,
        "manifest": {
            "digest": stored.digest,
            "size_bytes": stored.size_bytes,
            "media_type": "application/vnd.alloyport.candidate-source-manifest+json"
        },
        "source_bundle_digest": label("bundle"),
        "files": ["generated/a.cpp"]
    });
    ingest_bytes(
        artifacts,
        &serde_json::to_vec(&result).expect("result bytes"),
    )
    .expect("store result")
    .digest
}

#[test]
fn a_manifest_whose_declared_size_disagrees_with_its_object_is_refused() {
    let artifacts = InMemoryArtifactStore::new(1024 * 1024);
    let mut files = complete_files(&artifacts);
    let real = files[0].clone();
    files[0] = CandidateSourceFile::new(
        real.path().clone(),
        real.kind(),
        ArtifactDescriptor {
            digest: real.artifact().digest,
            // The object holds the real source. Nothing but this check stands between a manifest
            // that says otherwise and a commit that records a candidate nobody submitted.
            size_bytes: 999,
            media_type: real.artifact().media_type.clone(),
        },
    )
    .expect("source file");
    let result = stored_submission(&artifacts, "candidate-mismatch", files);
    let operations = [operation(
        crate::SUBMIT_CANDIDATE_BUNDLE_TOOL,
        "submit",
        result,
        Vec::new(),
    )];
    let error = collect_from_operations(operations.iter(), &artifacts).expect_err("mismatch");
    assert!(
        matches!(error, CandidateRecordError::SourceIdentity { .. }),
        "unexpected error: {error}"
    );
}

#[test]
fn a_rejection_is_not_read_as_a_gate_outcome() {
    let artifacts = InMemoryArtifactStore::new(1024 * 1024);
    let files = complete_files(&artifacts);
    let submission = stored_submission(&artifacts, "candidate-rejected-build", files);
    // What `request_ascend_build` publishes when the model cites the wrong Source Gate receipt: an
    // explanation, carrying the candidate's own identity, and no verdict about it whatsoever.
    let rejection = ingest_bytes(
        &artifacts,
        &serde_json::to_vec(&json!({
            "rejected": true,
            "tool": crate::REQUEST_ASCEND_BUILD_TOOL,
            "reason": "source_gate_receipt_digest does not name the receipt this candidate produces",
            "candidate_id": "candidate-rejected-build",
            "recoverable": true
        }))
        .expect("rejection bytes"),
    )
    .expect("store rejection")
    .digest;
    let operations = [
        operation(
            crate::SUBMIT_CANDIDATE_BUNDLE_TOOL,
            "submit",
            submission,
            Vec::new(),
        ),
        operation(
            crate::REQUEST_ASCEND_BUILD_TOOL,
            "build",
            rejection,
            vec![rejection],
        ),
    ];
    let record = collect_from_operations(operations.iter(), &artifacts).expect("record");
    assert_eq!(record.len(), 1);
    assert!(
        record[0].outcomes.is_empty(),
        "a refused citation is not a build verdict: {:?}",
        record[0].outcomes
    );
}

#[test]
fn a_build_receipt_puts_the_compilers_first_error_in_the_subject_line() {
    let artifacts: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::new(1024 * 1024));
    let files = complete_files(artifacts.as_ref());
    let submission = stored_submission(artifacts.as_ref(), "candidate-built", files);
    // The exact output of the first migration to reach a compiler, including the noise around it.
    let stderr = ingest_bytes(
        artifacts.as_ref(),
        b"gmake: TMPDIR value /alloyport/work/tmp: No such file or directory\n\
          /alloyport/bundle/generated/op_host/reduce_sum_launch.cpp:1:10: fatal error: acl/acl.h: No such file or directory\n",
    )
    .expect("store stderr");
    let receipt = ingest_bytes(
        artifacts.as_ref(),
        &serde_json::to_vec(&json!({
            "candidate_id": "candidate-built",
            "manifest_digest": label("manifest"),
            "source_gate_receipt_digest": label("source-receipt"),
            "passed": false,
            "outcome": 2,
            "exit_code": 1,
            "detail": "bounded compiler result",
            "stderr": {"digest": stderr.digest, "size_bytes": stderr.size_bytes,
                       "media_type": "text/plain; charset=utf-8"}
        }))
        .expect("receipt bytes"),
    )
    .expect("store receipt")
    .digest;
    let operations = [
        operation(
            crate::SUBMIT_CANDIDATE_BUNDLE_TOOL,
            "submit",
            submission,
            Vec::new(),
        ),
        operation(
            crate::REQUEST_ASCEND_BUILD_TOOL,
            "build",
            label("build-result"),
            vec![receipt],
        ),
    ];
    let record = collect_from_operations(operations.iter(), artifacts.as_ref()).expect("record");
    assert_eq!(record.len(), 1);
    assert_eq!(record[0].outcomes.len(), 1);
    assert_eq!(record[0].outcomes[0].verdict, "exit 1");

    let root = tempfile::tempdir().expect("record directory");
    let written = write_candidate_record(&root.path().join("record"), &record).expect("record");
    let subject = git(&written.root, &["log", "-1", "--format=%s", "HEAD"]);
    assert!(
        subject.contains("ascend_build exit 1: ")
            && subject.contains("fatal error: acl/acl.h: No such file or directory"),
        "the subject must answer what the compiler said: {subject}"
    );
    assert!(
        !subject.contains("TMPDIR"),
        "the first error, not the first line of noise: {subject}"
    );
}
