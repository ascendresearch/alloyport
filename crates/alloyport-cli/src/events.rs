//! Rendering canonical interaction events, and the demonstration stream used to check the renderer.
//!
//! Split out of `main.rs` for the module-size limit.

use alloyport_events::{
    Authority, Event, EventSequencer, FileChange, FileChangeKind, MessageRole, OutputStream,
    Producer, ProducerEvent, RunReducer, Visibility, producer_event_from_json_line, render_plain,
};
use std::io::{self, BufRead, Write};

pub(crate) fn render_events(jsonl: bool) -> Result<(), String> {
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

pub(crate) fn render_demo(jsonl: bool) -> Result<(), String> {
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
