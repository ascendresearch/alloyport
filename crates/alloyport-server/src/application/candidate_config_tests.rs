//! Strict candidate Episode configuration: what a deployment must state before it can run.

use super::*;
use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt as _;

#[tokio::test]
async fn strict_config_preflights_without_provider_dispatch() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let config_path = write_fixture(directory.path(), false)?;
    let config = CandidateEpisodeConfig::load(config_path)?;
    config.preflight_provider().await?;
    assert_eq!(config.required_workers.len(), 3);
    assert_eq!(config.required_workers[0].id, config.required_workers[2].id);
    assert_eq!(
        config.tools.workspace_root,
        directory.path().join("workspace")
    );
    assert!(
        config
            .episode
            .initial_user_text
            .contains("extern \"C\" int alloyport_reduce_sum_f32")
    );
    assert!(
        config
            .episode
            .initial_user_text
            .contains("BEGIN SOURCE input/src/reduce_sum_kernel.cu")
    );
    assert_ne!(
        config.episode.context_projection_digest,
        Sha256Digest::digest_bytes(b"context")
    );
    Ok(())
}

#[test]
fn unknown_fields_and_placeholder_images_fail_closed() -> Result<(), Box<dyn Error>> {
    let unknown = tempfile::tempdir()?;
    let path = write_fixture(unknown.path(), true)?;
    assert!(CandidateEpisodeConfig::load(path).is_err());

    let placeholder = ImageFileConfig {
        digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .parse()?,
        size_bytes: 1,
        media_type: OCI_IMAGE_CONFIG_MEDIA_TYPE.to_owned(),
    };
    assert!(placeholder.into_descriptor().is_err());
    Ok(())
}

fn write_fixture(root: &Path, unknown: bool) -> Result<PathBuf, Box<dyn Error>> {
    fs::create_dir(root.join("workspace"))?;
    fs::write(root.join("system.txt"), "You are a migration agent.")?;
    fs::write(
        root.join("user.txt"),
        "Produce the gated reduction candidate.",
    )?;
    let secret = root.join("model-key");
    fs::write(&secret, "test_secret")?;
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600))?;

    let mut catalog: Value = serde_json::from_slice(include_bytes!(
        "../../../../docs/runtime-model-catalog.example.json"
    ))?;
    catalog["deployments"]["configured-chat-endpoint"]["auth"]["path"] =
        Value::String(secret.to_string_lossy().into_owned());
    fs::write(root.join("catalog.json"), serde_json::to_vec(&catalog)?)?;

    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/migrations/cuda-reduction-v1");
    let digest = |label: &str| Sha256Digest::digest_bytes(label.as_bytes()).to_string();
    let image = |label: &str| {
        json!({
            "digest": digest(label),
            "size_bytes": 1,
            "media_type": OCI_IMAGE_CONFIG_MEDIA_TYPE
        })
    };
    let limits = json!({
        "cpu_millis": 4000,
        "memory_bytes": 8_589_934_592_u64,
        "disk_bytes": 1_073_741_824_u64,
        "process_count": 128,
        "output_bytes": 8_388_608_u64,
        "device_count": 1
    });
    // A build compiles and never opens an accelerator, so it asks for none. Correctness executes
    // and still does.
    let build_limits = json!({
        "cpu_millis": 4000,
        "memory_bytes": 8_589_934_592_u64,
        "disk_bytes": 1_073_741_824_u64,
        "process_count": 128,
        "output_bytes": 8_388_608_u64,
        "device_count": 0
    });
    let mut config = json!({
        "schema_version": 1,
        "model_catalog": "catalog.json",
        "migration_spec": fixture.join("migration-spec-v1.json"),
        "reference_root": fixture,
        "workspace_root": "workspace",
        "episode_database": "episode.sqlite3",
        "generation_strategy": "direct_ascend_c",
        "episode": {
            "episode_id": "episode-test",
            "task_id": "task-test",
            "search_run_id": "search-test",
            "parent_candidate_id": null,
            "runtime_model_alias": null,
            "prompt_revision": "candidate-v1",
            "loop_policy": {
                "max_model_turns": 8,
                "max_model_attempts": 12,
                "max_ambiguous_model_attempts": 1,
                "max_tool_calls_per_turn": 4,
                "max_total_tool_operations": 16,
                "max_stop_feedback_turns": 2
            },
            "system_prompt": "system.txt",
            "initial_user_text": "user.txt"
        },
        "build": {
            "worker_id": "ascend-build-1",
            "image": image("build-image"),
            "timeout_ms": 120_000,
            "limits": build_limits
        },
        "correctness": {
            "cuda": {
                "worker_id": "cuda-correctness-1",
                "image": image("cuda-image"),
                "timeout_ms": 120_000,
                "limits": limits.clone()
            },
            "ascend": {
                "worker_id": "ascend-build-1",
                "image": image("ascend-image"),
                "timeout_ms": 120_000,
                "limits": limits
            }
        },
        "codec_limits": null,
        "worker_poll_interval_ms": 1000,
        "worker_ready_timeout_ms": 10000
    });
    if unknown {
        config["surprise"] = Value::Bool(true);
    }
    let path = root.join("candidate.json");
    fs::write(&path, serde_json::to_vec(&config)?)?;
    Ok(path)
}
