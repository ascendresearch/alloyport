use alloyport_core::{
    BundlePath, Gate, GenerationStrategy, MigrationSpec, TaskState, inspect_migration_source,
};
use alloyport_events::{
    Authority, Event, EventSequencer, FileChange, FileChangeKind, MessageRole, OutputStream,
    Producer, ProducerEvent, RunReducer, Visibility, producer_event_from_json_line, render_plain,
};
use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::process::ExitCode;

const MAX_SPEC_BYTES: u64 = 1024 * 1024;
const MAX_SOURCE_BYTES: u64 = 4 * 1024 * 1024;

fn main() -> ExitCode {
    let mut arguments = env::args().skip(1);
    let result = match arguments.next().as_deref() {
        Some("about") => {
            println!("AlloyPort {}", env!("CARGO_PKG_VERSION"));
            println!("Verified CUDA-to-Ascend-C source migration and optimization.");
            Ok(())
        }
        Some("lifecycle") => {
            println!("states: {:?}", lifecycle());
            println!("generation strategies: {:?}", generation_strategies());
            println!("release gates: {:?}", Gate::ALL);
            Ok(())
        }
        Some("render-events") => render_events(arguments.next().as_deref() == Some("--jsonl")),
        Some("event-demo") => render_demo(arguments.next().as_deref() == Some("--jsonl")),
        Some("inspect-migration") => {
            let spec_path = arguments
                .next()
                .ok_or_else(|| "inspect-migration requires SPEC_PATH and BUNDLE_ROOT".to_owned());
            let bundle_root = arguments
                .next()
                .ok_or_else(|| "inspect-migration requires SPEC_PATH and BUNDLE_ROOT".to_owned());
            match (spec_path, bundle_root, arguments.next()) {
                (Ok(spec_path), Ok(bundle_root), None) => {
                    inspect_migration(Path::new(&spec_path), Path::new(&bundle_root))
                }
                (Ok(_), Ok(_), Some(_)) => {
                    Err("inspect-migration accepts exactly SPEC_PATH and BUNDLE_ROOT".to_owned())
                }
                (Err(error), _, _) | (_, Err(error), _) => Err(error),
            }
        }
        _ => Err(
            "usage: alloyport-cli <about|lifecycle|inspect-migration SPEC_PATH BUNDLE_ROOT|\
             render-events [--jsonl]|event-demo [--jsonl]>"
                .to_owned(),
        ),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn inspect_migration(spec_path: &Path, bundle_root: &Path) -> Result<(), String> {
    let spec_bytes = read_bounded_regular_file(spec_path, MAX_SPEC_BYTES, "MigrationSpec")?;
    let spec: MigrationSpec = serde_json::from_slice(&spec_bytes)
        .map_err(|error| format!("invalid MigrationSpec {}: {error}", spec_path.display()))?;
    let root = fs::canonicalize(bundle_root)
        .map_err(|error| format!("cannot open bundle root {}: {error}", bundle_root.display()))?;
    if !root.is_dir() {
        return Err(format!("bundle root {} is not a directory", root.display()));
    }

    let files = load_declared_sources(&spec, &root)?;
    let report = inspect_migration_source(&spec, &files);
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("cannot render inspection report: {error}"))?
    );
    if report.passed {
        Ok(())
    } else {
        Err("migration intake inspection failed".to_owned())
    }
}

pub(crate) fn load_declared_sources(
    spec: &MigrationSpec,
    bundle_root: &Path,
) -> Result<std::collections::BTreeMap<BundlePath, String>, String> {
    let paths = spec
        .sources()
        .device_sources()
        .iter()
        .chain(spec.sources().host_sources())
        .chain(spec.sources().build_files());
    let mut files = std::collections::BTreeMap::new();

    for relative in paths {
        let candidate = bundle_root.join(relative.as_str());
        let metadata = match fs::symlink_metadata(&candidate) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "cannot inspect declared file {}: {error}",
                    relative.as_str()
                ));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "declared source {} must be a regular non-symlink file",
                relative.as_str()
            ));
        }
        let canonical = fs::canonicalize(&candidate).map_err(|error| {
            format!(
                "cannot resolve declared file {}: {error}",
                relative.as_str()
            )
        })?;
        if !canonical.starts_with(bundle_root) {
            return Err(format!(
                "declared source {} escapes the bundle root",
                relative.as_str()
            ));
        }
        let bytes = read_bounded_regular_file(&canonical, MAX_SOURCE_BYTES, "source file")?;
        let contents = String::from_utf8(bytes)
            .map_err(|_| format!("declared source {} is not UTF-8", relative.as_str()))?;
        files.insert(relative.clone(), contents);
    }
    Ok(files)
}

pub(crate) fn read_bounded_regular_file(
    path: &Path,
    limit: u64,
    kind: &str,
) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect {kind} {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("{kind} {} must be a regular file", path.display()));
    }
    if metadata.len() > limit {
        return Err(format!(
            "{kind} {} is {} bytes; limit is {limit}",
            path.display(),
            metadata.len()
        ));
    }
    fs::read(path).map_err(|error| format!("cannot read {kind} {}: {error}", path.display()))
}

fn render_events(jsonl: bool) -> Result<(), String> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut sequencer: Option<EventSequencer> = None;
    let mut reducer = RunReducer::new();

    for (index, line) in stdin.lock().lines().enumerate() {
        let line_number = index + 1;
        let line = line.map_err(|error| format!("line {line_number}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let frame = producer_event_from_json_line(&line)
            .map_err(|error| format!("line {line_number}: invalid producer event: {error}"))?;
        let sequencer = sequencer.get_or_insert_with(|| EventSequencer::new(frame.run_id.clone()));
        let envelope = sequencer
            .ingest(frame)
            .map_err(|error| format!("line {line_number}: {error}"))?;
        reducer
            .apply(&envelope)
            .map_err(|error| format!("line {line_number}: {error}"))?;
        write_envelope(&mut stdout, &envelope, jsonl)?;
    }
    stdout.flush().map_err(|error| error.to_string())
}

fn render_demo(jsonl: bool) -> Result<(), String> {
    let mut sequencer = EventSequencer::new("demo-run");
    let mut reducer = RunReducer::new();
    let mut stdout = io::stdout().lock();
    for frame in demo_events() {
        let envelope = sequencer.ingest(frame).map_err(|error| error.to_string())?;
        reducer
            .apply(&envelope)
            .map_err(|error| error.to_string())?;
        write_envelope(&mut stdout, &envelope, jsonl)?;
    }
    stdout.flush().map_err(|error| error.to_string())
}

fn write_envelope(
    writer: &mut impl Write,
    envelope: &alloyport_events::EventEnvelope,
    jsonl: bool,
) -> Result<(), String> {
    let output = if jsonl {
        format!(
            "{}\n",
            envelope.to_json_line().map_err(|error| error.to_string())?
        )
    } else {
        render_plain(envelope)
    };
    writer
        .write_all(output.as_bytes())
        .map_err(|error| error.to_string())
}

fn demo_events() -> Vec<ProducerEvent> {
    let run_id = "demo-run";
    let mut events = vec![
        frame(
            run_id,
            Event::RunStarted {
                task: "migrate vector_add.cu to Ascend C".to_owned(),
            },
        ),
        frame(run_id, Event::TurnStarted { turn: 1 }),
        with_operation(
            frame(
                run_id,
                Event::MessageStarted {
                    role: MessageRole::Assistant,
                },
            ),
            "message-1",
            None,
        ),
    ];
    events.extend(demo_tool_events(run_id));
    events.extend([
        frame(
            run_id,
            Event::TurnCompleted {
                turn: 1,
                outcome: "verified".to_owned(),
            },
        ),
        frame(
            run_id,
            Event::RunCompleted {
                result: "demo completed".to_owned(),
            },
        ),
    ]);
    events
}

fn demo_tool_events(run_id: &str) -> Vec<ProducerEvent> {
    vec![
        with_operation(
            frame(
                run_id,
                Event::MessageDelta {
                    text: "我先编译生成的 Ascend C，再运行正确性检查。".to_owned(),
                },
            ),
            "message-1",
            None,
        ),
        with_operation(
            frame(run_id, Event::MessageCompleted {}),
            "message-1",
            None,
        ),
        with_operation(
            frame(
                run_id,
                Event::ToolStarted {
                    name: "project_verify".to_owned(),
                    arguments: serde_json_value("port"),
                },
            ),
            "tool-1",
            None,
        ),
        with_operation(
            frame(
                run_id,
                Event::CommandStarted {
                    command: "cmake --build build && ./build/verify".to_owned(),
                    cwd: Some("/work/vector_add".to_owned()),
                    execution_site: "ascend-worker-0".to_owned(),
                    description: Some("compile and verify generated kernel".to_owned()),
                },
            ),
            "command-1",
            Some("tool-1"),
        ),
        with_operation(
            frame(
                run_id,
                Event::CommandOutput {
                    stream: OutputStream::Stdout,
                    byte_offset: 0,
                    text: "build: ok\nmax_abs_error: 0.0\n".to_owned(),
                    display_sanitized: false,
                },
            ),
            "command-1",
            Some("tool-1"),
        ),
        with_operation(
            frame(
                run_id,
                Event::CommandCompleted {
                    exit_code: 0,
                    elapsed_ms: 842,
                    timed_out: false,
                    output_artifact: None,
                },
            ),
            "command-1",
            Some("tool-1"),
        ),
        with_operation(
            frame(
                run_id,
                Event::WorkspaceDelta {
                    changes: vec![FileChange {
                        path: "src/vector_add.cpp".to_owned(),
                        kind: FileChangeKind::Modified,
                        additions: Some(2),
                        deletions: Some(1),
                        before_digest: None,
                        after_digest: None,
                    }],
                    diff: Some(
                        "@@ -18,1 +18,2 @@\n-constexpr int block = 128;\n+constexpr int tile = 256;\n+constexpr int block = tile;\n"
                            .to_owned(),
                    ),
                    commit: Some("8c2fd71".to_owned()),
                },
            ),
            "tool-1",
            None,
        ),
        with_operation(
            frame(
                run_id,
                Event::ToolCompleted {
                    name: "project_verify".to_owned(),
                    output: "oracle verdict: PASS".to_owned(),
                },
            ),
            "tool-1",
            None,
        ),
    ]
}

fn frame(run_id: &str, event: Event) -> ProducerEvent {
    let mut frame = ProducerEvent::new(run_id, Producer::new("alloyport-cli", "demo"), event);
    frame.task_id = Some("demo-task".to_owned());
    frame.authority = Authority::Observed;
    frame.visibility = Visibility::User;
    frame
}

fn with_operation(
    mut frame: ProducerEvent,
    operation_id: &str,
    parent_operation_id: Option<&str>,
) -> ProducerEvent {
    frame.operation_id = Some(operation_id.to_owned());
    frame.parent_operation_id = parent_operation_id.map(str::to_owned);
    frame
}

fn serde_json_value(variant: &str) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "variant".to_owned(),
        serde_json::Value::String(variant.to_owned()),
    );
    serde_json::Value::Object(value)
}

const fn lifecycle() -> [TaskState; 10] {
    [
        TaskState::Captured,
        TaskState::Specified,
        TaskState::Generating,
        TaskState::Building,
        TaskState::Verifying,
        TaskState::Optimizing,
        TaskState::Integrating,
        TaskState::Releasable,
        TaskState::Released,
        TaskState::Failed,
    ]
}

const fn generation_strategies() -> [GenerationStrategy; 4] {
    [
        GenerationStrategy::DirectAscendC,
        GenerationStrategy::AscendSimtBootstrap,
        GenerationStrategy::VerifiedTemplateAdaptation,
        GenerationStrategy::MemoryGuidedSynthesis,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_product_fixture_passes_filesystem_inspection() -> Result<(), String> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/migrations/cuda-reduction-v1");
        let spec_path = root.join("migration-spec-v1.json");
        let spec_bytes = read_bounded_regular_file(&spec_path, MAX_SPEC_BYTES, "MigrationSpec")?;
        let spec: MigrationSpec = serde_json::from_slice(&spec_bytes)
            .map_err(|error| format!("invalid fixture spec: {error}"))?;
        let root = fs::canonicalize(root).map_err(|error| error.to_string())?;
        let files = load_declared_sources(&spec, &root)?;
        let report = inspect_migration_source(&spec, &files);

        assert!(report.passed, "{:?}", report.failures);
        assert_eq!(report.inspected_files, 5);
        Ok(())
    }
}
