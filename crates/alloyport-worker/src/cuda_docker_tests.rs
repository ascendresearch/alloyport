//! Behavioral tests for the Docker CLI adapter module.

use super::*;
use crate::ascend::{AscendDockerCreatePlan, AscendEnvironmentFacts};
use crate::ascend_supervisor::AscendContainerEngine;
use crate::container_engine::{ContainerLogChunk, ContainerLogStream, ContainerPhase};
use crate::cuda_docker::protocol::elapsed_ms;
use alloyport_core::{AcceleratorDevice, Sha256Digest};
use std::collections::VecDeque;
use std::io::{self, Cursor};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, mpsc};

#[test]
fn adapter_failures_map_to_stable_engine_categories() {
    assert!(matches!(
        DockerCliEngine::new(""),
        Err(ContainerEngineError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        command_io_error(
            Path::new("/missing/docker"),
            &io::Error::new(io::ErrorKind::NotFound, "missing")
        ),
        ContainerEngineError::Unavailable(_)
    ));
    assert!(matches!(
        require_success("inspect image", &failure(b"daemon rejected request")),
        Err(ContainerEngineError::CommandFailed(_))
    ));
    assert!(matches!(
        parse_image_id(b"not-json"),
        Err(ContainerEngineError::InvalidResponse(_))
    ));
}

#[test]
fn parses_exact_image_and_container_recovery_identity() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        parse_image_id(br#"[{"Id":"sha256:image"}]"#)?,
        "sha256:image"
    );
    let detail = parse_container_inspect(
        br#"[{
            "Name":"/alloyport-attempt-1",
            "Image":"sha256:image",
            "Config":{"Labels":{
                "alloyport.attempt":"attempt-1",
                "alloyport.bundle":"sha256:bundle",
                "alloyport.image":"sha256:manifest"
            }},
            "State":{
                "Status":"exited",
                "ExitCode":0,
                "StartedAt":"2026-08-10T12:00:00.100000000Z",
                "FinishedAt":"2026-08-10T12:00:01.334000000Z"
            }
        }]"#,
    )?;
    assert_eq!(
        detail.snapshot,
        ContainerSnapshot {
            identity: ContainerIdentity {
                name: "alloyport-attempt-1".into(),
                attempt_id: "attempt-1".into(),
                bundle_digest: "sha256:bundle".into(),
                image_manifest_digest: "sha256:manifest".into(),
                image_id: "sha256:image".into(),
            },
            phase: ContainerPhase::Exited,
        }
    );
    assert_eq!(
        detail.exit,
        Some(ContainerExit {
            exit_code: 0,
            elapsed_ms: 1_234,
        })
    );
    Ok(())
}

#[test]
fn inspect_parser_rejects_ambiguous_state_and_negative_elapsed_time() {
    let state = br#"[{
        "Name":"/alloyport-attempt-1",
        "Image":"sha256:image",
        "Config":{"Labels":{}},
        "State":{
            "Status":"paused",
            "ExitCode":0,
            "StartedAt":"2026-08-10T12:00:01Z",
            "FinishedAt":"2026-08-10T12:00:00Z"
        }
    }]"#;
    assert!(parse_container_inspect(state).is_err());
    assert!(elapsed_ms("2026-08-10T12:00:01Z", "2026-08-10T12:00:00Z").is_err());
}

#[test]
fn bounded_reader_drains_but_retains_only_the_declared_limit() -> Result<(), io::Error> {
    let (bytes, exceeded) = read_bounded(Cursor::new(b"abcdefgh"), 5)?;
    assert_eq!(bytes, b"abcde");
    assert!(exceeded);
    assert_eq!(parse_wait_exit_code(b"17\n").expect("valid exit"), 17);
    assert!(parse_wait_exit_code(b"0\n1\n").is_err());
    Ok(())
}

#[test]
fn followed_readers_share_one_combined_output_budget() -> Result<(), io::Error> {
    let total = AtomicU64::new(0);
    let (limit_sender, limit_receiver) = mpsc::sync_channel(1);
    let (preview_sender, mut preview_receiver) = tokio::sync::mpsc::channel(4);
    let (stdout, stdout_exceeded) = read_follow_bounded(
        Cursor::new(b"abc"),
        5,
        &total,
        &limit_sender,
        ContainerLogStream::Stdout,
        Some(&preview_sender),
    )?;
    let (stderr, stderr_exceeded) = read_follow_bounded(
        Cursor::new(b"def"),
        5,
        &total,
        &limit_sender,
        ContainerLogStream::Stderr,
        Some(&preview_sender),
    )?;

    assert_eq!(stdout, b"abc");
    assert_eq!(stderr, b"de");
    assert!(!stdout_exceeded);
    assert!(stderr_exceeded);
    assert!(limit_receiver.try_recv().is_ok());
    assert_eq!(total.load(Ordering::Relaxed), 6);
    assert_eq!(
        preview_receiver.try_recv().expect("stdout preview"),
        ContainerLogChunk {
            stream: ContainerLogStream::Stdout,
            byte_offset: 0,
            bytes: b"abc".to_vec(),
        }
    );
    assert_eq!(
        preview_receiver.try_recv().expect("stderr preview"),
        ContainerLogChunk {
            stream: ContainerLogStream::Stderr,
            byte_offset: 0,
            bytes: b"de".to_vec(),
        }
    );
    Ok(())
}

#[test]
fn slow_preview_consumer_never_blocks_or_changes_authoritative_bytes() -> Result<(), io::Error> {
    let total = AtomicU64::new(0);
    let (limit_sender, _limit_receiver) = mpsc::sync_channel(1);
    let (preview_sender, mut preview_receiver) = tokio::sync::mpsc::channel(1);
    preview_sender
        .try_send(ContainerLogChunk {
            stream: ContainerLogStream::Stdout,
            byte_offset: 99,
            bytes: b"queued".to_vec(),
        })
        .expect("preview queue has capacity");

    let (bytes, exceeded) = read_follow_bounded(
        Cursor::new(b"authoritative"),
        64,
        &total,
        &limit_sender,
        ContainerLogStream::Stdout,
        Some(&preview_sender),
    )?;

    assert_eq!(bytes, b"authoritative");
    assert!(!exceeded);
    assert_eq!(total.load(Ordering::Relaxed), 13);
    assert_eq!(
        preview_receiver.try_recv().expect("preexisting preview"),
        ContainerLogChunk {
            stream: ContainerLogStream::Stdout,
            byte_offset: 99,
            bytes: b"queued".to_vec(),
        }
    );
    assert!(preview_receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn cli_boundary_distinguishes_absence_and_preserves_log_exhaustion()
-> Result<(), Box<dyn std::error::Error>> {
    let runner = Arc::new(ScriptedRunner::new(vec![
        expected(
            &["container", "inspect", "alloyport-attempt-1"],
            METADATA_OUTPUT_LIMIT,
            failure(b"Error: No such container"),
        ),
        expected(
            &[
                "container",
                "list",
                "--all",
                "--filter",
                "name=^/alloyport-attempt-1$",
                "--format",
                "{{.Names}}",
            ],
            METADATA_OUTPUT_LIMIT,
            success(b"", b"", false),
        ),
        expected(
            &["container", "logs", "--follow", "alloyport-attempt-1"],
            5,
            success(b"abcde", b"", true),
        ),
        expected(
            &["container", "rm", "alloyport-attempt-1"],
            METADATA_OUTPUT_LIMIT,
            success(b"alloyport-attempt-1\n", b"", false),
        ),
        expected(
            &["container", "rm", "alloyport-attempt-1"],
            METADATA_OUTPUT_LIMIT,
            failure(b"Error: No such container"),
        ),
        expected(
            &["container", "inspect", "alloyport-attempt-1"],
            METADATA_OUTPUT_LIMIT,
            failure(b"Error: No such container"),
        ),
        expected(
            &[
                "container",
                "list",
                "--all",
                "--filter",
                "name=^/alloyport-attempt-1$",
                "--format",
                "{{.Names}}",
            ],
            METADATA_OUTPUT_LIMIT,
            success(b"", b"", false),
        ),
    ]));
    let engine = DockerCliEngine {
        runner: runner.clone(),
        stop_timeout_seconds: 10,
    };

    assert!(
        CudaContainerEngine::inspect(&engine, "alloyport-attempt-1")
            .await?
            .is_none()
    );
    let logs = CudaContainerEngine::follow_logs(&engine, "alloyport-attempt-1", 5).await?;
    assert_eq!(logs.stdout, b"abcde");
    assert!(logs.output_limit_exceeded);
    CudaContainerEngine::remove(&engine, "alloyport-attempt-1").await?;
    CudaContainerEngine::remove(&engine, "alloyport-attempt-1").await?;
    runner.assert_exhausted();
    Ok(())
}

#[tokio::test]
async fn ascend_port_preserves_the_policy_derived_create_argv()
-> Result<(), Box<dyn std::error::Error>> {
    let image_id = Sha256Digest::digest_bytes(b"ascend image");
    let create_argv = vec![
        "create".into(),
        "--name".into(),
        "alloyport-attempt-ascend-1".into(),
        "pinned-image".into(),
    ];
    let runner = Arc::new(ScriptedRunner::new(vec![
        expected(
            &["image", "inspect", "pinned-image"],
            METADATA_OUTPUT_LIMIT,
            success(format!(r#"[{{"Id":"{image_id}"}}]"#).as_bytes(), b"", false),
        ),
        ExpectedCommand {
            arguments: create_argv.clone(),
            output_limit: METADATA_OUTPUT_LIMIT,
            output: success(b"container\n", b"", false),
        },
    ]));
    let engine = DockerCliEngine {
        runner: runner.clone(),
        stop_timeout_seconds: 10,
    };
    let plan = AscendDockerCreatePlan {
        container_name: "alloyport-attempt-ascend-1".into(),
        image_reference: "pinned-image".into(),
        expected_image_id: image_id,
        device: AcceleratorDevice {
            device_id: "3".into(),
            product_name: "Ascend950PR".into(),
            serial_number: "serial-3".into(),
            firmware_version: "firmware".into(),
        },
        environment: AscendEnvironmentFacts::new("Ascend950PR", "CANN", "driver", "firmware")?,
        argv: create_argv,
    };
    let identity = ContainerIdentity {
        name: plan.container_name.clone(),
        attempt_id: "attempt-ascend-1".into(),
        bundle_digest: "sha256:bundle".into(),
        image_manifest_digest: "sha256:manifest".into(),
        image_id: image_id.to_string(),
    };

    assert_eq!(
        AscendContainerEngine::resolve_image_id(&engine, &plan).await?,
        image_id.to_string()
    );
    AscendContainerEngine::create(&engine, &plan, &identity).await?;
    runner.assert_exhausted();
    Ok(())
}

#[derive(Debug)]
struct ScriptedRunner {
    commands: Mutex<VecDeque<ExpectedCommand>>,
}

impl ScriptedRunner {
    fn new(commands: Vec<ExpectedCommand>) -> Self {
        Self {
            commands: Mutex::new(commands.into()),
        }
    }

    fn assert_exhausted(&self) {
        assert!(
            self.commands.lock().expect("script lock").is_empty(),
            "all scripted Docker commands must be consumed"
        );
    }
}

impl DockerCommandRunner for ScriptedRunner {
    fn run(
        &self,
        arguments: &[String],
        output_limit: u64,
    ) -> Result<DockerCommandOutput, ContainerEngineError> {
        let expected = self
            .commands
            .lock()
            .map_err(|_| ContainerEngineError::Internal("script lock poisoned".into()))?
            .pop_front()
            .ok_or_else(|| {
                ContainerEngineError::Internal(format!("unexpected Docker command: {arguments:?}"))
            })?;
        assert_eq!(arguments, expected.arguments);
        assert_eq!(output_limit, expected.output_limit);
        Ok(expected.output)
    }
}

#[derive(Debug)]
struct ExpectedCommand {
    arguments: Vec<String>,
    output_limit: u64,
    output: DockerCommandOutput,
}

fn expected(arguments: &[&str], output_limit: u64, output: DockerCommandOutput) -> ExpectedCommand {
    ExpectedCommand {
        arguments: arguments.iter().map(ToString::to_string).collect(),
        output_limit,
        output,
    }
}

fn success(stdout: &[u8], stderr: &[u8], exceeded: bool) -> DockerCommandOutput {
    DockerCommandOutput {
        success: true,
        exit_code: Some(0),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
        output_limit_exceeded: exceeded,
    }
}

fn failure(stderr: &[u8]) -> DockerCommandOutput {
    DockerCommandOutput {
        success: false,
        exit_code: Some(1),
        stdout: Vec::new(),
        stderr: stderr.to_vec(),
        output_limit_exceeded: false,
    }
}
