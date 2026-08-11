//! Behavioral tests for the event model and reducer module.

use super::*;

fn frame(run_id: &str, event: Event) -> ProducerEvent {
    ProducerEvent::new(run_id, Producer::new("test", "one"), event)
}

fn ingest(
    sequencer: &mut EventSequencer,
    reducer: &mut RunReducer,
    frame: ProducerEvent,
) -> EventEnvelope {
    let envelope = sequencer.ingest(frame).expect("valid producer frame");
    reducer.apply(&envelope).expect("valid event lifecycle");
    envelope
}

#[test]
fn jsonl_uses_stable_type_and_escapes_embedded_newlines() {
    let mut sequencer = EventSequencer::new("run-1");
    let envelope = sequencer
        .ingest(frame(
            "run-1",
            Event::Warning {
                message: "first\nsecond".to_owned(),
            },
        ))
        .expect("frame is accepted");
    let line = envelope.to_json_line().expect("serializable event");

    assert!(line.contains("\"type\":\"warning\""));
    assert!(line.contains("first\\nsecond"));
    assert_eq!(line.lines().count(), 1);
}

#[test]
fn reducer_accepts_nested_tool_and_command_lifecycles() {
    let mut sequencer = EventSequencer::new("run-1");
    let mut reducer = RunReducer::new();
    ingest(
        &mut sequencer,
        &mut reducer,
        frame(
            "run-1",
            Event::RunStarted {
                task: "port extension".to_owned(),
            },
        ),
    );

    let mut tool = frame(
        "run-1",
        Event::ToolStarted {
            name: "verify".to_owned(),
            arguments: Value::Null,
        },
    );
    tool.operation_id = Some("tool-1".to_owned());
    ingest(&mut sequencer, &mut reducer, tool);

    let mut command = frame(
        "run-1",
        Event::CommandStarted {
            command: "cargo test".to_owned(),
            cwd: Some("/work".to_owned()),
            execution_site: "local".to_owned(),
            description: None,
        },
    );
    command.operation_id = Some("command-1".to_owned());
    command.parent_operation_id = Some("tool-1".to_owned());
    ingest(&mut sequencer, &mut reducer, command);

    let mut command_done = frame(
        "run-1",
        Event::CommandCompleted {
            exit_code: 0,
            elapsed_ms: 12,
            timed_out: false,
            output_artifact: None,
        },
    );
    command_done.operation_id = Some("command-1".to_owned());
    ingest(&mut sequencer, &mut reducer, command_done);

    let mut tool_done = frame(
        "run-1",
        Event::ToolCompleted {
            name: "verify".to_owned(),
            output: "PASS".to_owned(),
        },
    );
    tool_done.operation_id = Some("tool-1".to_owned());
    ingest(&mut sequencer, &mut reducer, tool_done);
}

#[test]
fn reducer_rejects_command_output_without_a_start() {
    let mut sequencer = EventSequencer::new("run-1");
    let mut reducer = RunReducer::new();
    ingest(
        &mut sequencer,
        &mut reducer,
        frame(
            "run-1",
            Event::RunStarted {
                task: "port extension".to_owned(),
            },
        ),
    );

    let mut output = frame(
        "run-1",
        Event::CommandOutput {
            stream: OutputStream::Stdout,
            byte_offset: 0,
            text: "oops".to_owned(),
            display_sanitized: false,
        },
    );
    output.operation_id = Some("missing".to_owned());
    let envelope = sequencer.ingest(output).expect("protocol frame is valid");

    assert_eq!(
        reducer.apply(&envelope),
        Err(ReduceError::OperationNotActive("missing".to_owned()))
    );
}

#[test]
fn reducer_rejects_terminal_run_with_an_active_operation() {
    let mut sequencer = EventSequencer::new("run-1");
    let mut reducer = RunReducer::new();
    ingest(
        &mut sequencer,
        &mut reducer,
        frame(
            "run-1",
            Event::RunStarted {
                task: "port extension".to_owned(),
            },
        ),
    );
    let mut message = frame(
        "run-1",
        Event::MessageStarted {
            role: MessageRole::Assistant,
        },
    );
    message.operation_id = Some("message-1".to_owned());
    ingest(&mut sequencer, &mut reducer, message);
    let terminal = sequencer
        .ingest(frame(
            "run-1",
            Event::RunCompleted {
                result: "too early".to_owned(),
            },
        ))
        .expect("protocol frame is valid");

    assert_eq!(
        reducer.apply(&terminal),
        Err(ReduceError::OperationsStillActive(vec![
            "message-1".to_owned()
        ]))
    );
}

#[test]
fn plain_renderer_shows_command_and_diff_evidence() {
    let mut sequencer = EventSequencer::new("run-1");
    let mut command = frame(
        "run-1",
        Event::CommandStarted {
            command: "python verify.py".to_owned(),
            cwd: Some("/work/demo".to_owned()),
            execution_site: "ascend-worker-2".to_owned(),
            description: Some("run correctness oracle".to_owned()),
        },
    );
    command.operation_id = Some("command-1".to_owned());
    let command = sequencer.ingest(command).expect("valid command");
    assert!(render_plain(&command).contains("ascend-worker-2"));

    let delta = sequencer
        .ingest(frame(
            "run-1",
            Event::WorkspaceDelta {
                changes: vec![FileChange {
                    path: "src/kernel.cpp".to_owned(),
                    kind: FileChangeKind::Modified,
                    additions: Some(2),
                    deletions: Some(1),
                    before_digest: None,
                    after_digest: None,
                }],
                diff: Some("@@ -1 +1 @@\n-old\n+new\n".to_owned()),
                commit: Some("abc123".to_owned()),
            },
        ))
        .expect("valid delta");
    let rendered = render_plain(&delta);
    assert!(rendered.contains("src/kernel.cpp +2/-1"));
    assert!(rendered.contains("-old\n+new"));
}
