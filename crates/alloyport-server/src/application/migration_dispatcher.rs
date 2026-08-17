//! Persistent daemon loop that turns captured CLI submissions into Candidate Episodes.

use super::candidate_config::CandidateEpisodeConfig;
use super::config::MigrationRuntimeConfig;
use super::{open_candidate_episode_https, runtime};
use crate::WorkerControlService;
use crate::migration_task::{MigrationTaskRecord, MigrationTaskState, SqliteMigrationTaskStore};
use alloyport_artifacts::{ArtifactStore, Sha256Digest};
use alloyport_core::BundlePath;
use alloyport_events::{Authority, Event, Producer, ProducerEvent, Visibility};
use alloyport_proto::management_v1::MigrationProjectBundle;
use prost::Message;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;

const MAX_ENCODED_PROJECT_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct MigrationDispatcher {
    config: MigrationRuntimeConfig,
    tasks: Arc<SqliteMigrationTaskStore>,
    artifacts: Arc<dyn ArtifactStore>,
    control: WorkerControlService,
}

impl MigrationDispatcher {
    pub(super) fn new(
        config: MigrationRuntimeConfig,
        tasks: Arc<SqliteMigrationTaskStore>,
        artifacts: Arc<dyn ArtifactStore>,
        control: WorkerControlService,
    ) -> Result<Self, Box<dyn Error>> {
        fs::create_dir_all(&config.root)?;
        if !fs::canonicalize(&config.root)?.is_dir() {
            return Err("migration runtime root is not a directory".into());
        }
        Ok(Self {
            config,
            tasks,
            artifacts,
            control,
        })
    }

    pub(super) async fn run_until(self, mut shutdown: watch::Receiver<bool>) -> Result<(), String> {
        while !*shutdown.borrow() {
            let tasks = Arc::clone(&self.tasks);
            let task = tokio::task::spawn_blocking(move || tasks.claim_next())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            if let Some(task) = task {
                self.run_task(task).await;
                continue;
            }
            tokio::select! {
                () = tokio::time::sleep(self.config.poll_interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_task(&self, task: MigrationTaskRecord) {
        if let Err(error) = self.publish(
            &task.task_id,
            "migration-running",
            Event::PlanUpdated {
                entries: serde_json::json!([{"step":"prepare migration episode","status":"in_progress"}]),
            },
        ) {
            eprintln!("cannot publish running event for {}: {error}", task.task_id);
        }
        let result = self.execute(&task).await;
        let cancelled = self.tasks.is_cancelled(&task.task_id).unwrap_or(false);
        let (state, dedup_key, event) = if cancelled {
            (
                None,
                "migration-cancelled",
                Event::RunFailed {
                    error: "migration cancelled by user".to_owned(),
                },
            )
        } else {
            match result {
                Ok(()) => (
                    Some(MigrationTaskState::Completed),
                    "migration-completed",
                    Event::RunCompleted {
                        result: "migration Episode completed successfully".to_owned(),
                    },
                ),
                Err(error) => (
                    Some(MigrationTaskState::Failed),
                    "migration-failed",
                    Event::RunFailed { error },
                ),
            }
        };
        if let Some(state) = state
            && let Err(error) = self.tasks.finish(&task.task_id, state)
        {
            eprintln!("cannot finish migration task {}: {error}", task.task_id);
        }
        // Keyed by content, not by outcome name. A fixed "migration-failed" was safe only while a
        // task could fail once; resumption made that false, and the second failure of a resumed
        // task was dropped with "interaction dedup key migration-failed has conflicting content" --
        // losing the very explanation the operator needed. Identical republication after a crash
        // still deduplicates, because identical content keys the same.
        let dedup_key = format!("{dedup_key}:{}", event_content_key(&event));
        if let Err(error) = self.publish(&task.task_id, &dedup_key, event) {
            eprintln!(
                "cannot publish terminal event for {}: {error}",
                task.task_id
            );
        }
    }

    async fn execute(&self, task: &MigrationTaskRecord) -> Result<(), String> {
        let artifacts = Arc::clone(&self.artifacts);
        let digest = task.project_digest;
        let expected_files = task.file_count;
        let task_root = self.config.root.join(&task.task_id);
        let materialize_root = task_root.clone();
        let project_root = tokio::task::spawn_blocking(move || {
            materialize_project(
                artifacts.as_ref(),
                digest,
                expected_files,
                &materialize_root,
            )
        })
        .await
        .map_err(|error| error.to_string())??;
        let candidate = CandidateEpisodeConfig::load_for_task(
            &self.config.candidate_template,
            &task.task_id,
            &project_root,
            &task_root,
        )
        .map_err(|error| error.to_string())?;
        candidate
            .preflight_provider()
            .await
            .map_err(|error| error.to_string())?;
        // Captured before the spec is moved: this is what a resumption may grant.
        let allowance = candidate.episode.loop_policy.allowance();
        let episode = open_candidate_episode_https(
            candidate.episode,
            candidate.catalog,
            candidate.codec_limits,
            Arc::clone(&self.artifacts),
            candidate.database,
            self.control.clone(),
            candidate.tools,
        )
        .map_err(|error| error.to_string())?;
        let runtime = runtime::CandidateRuntime {
            application: episode,
            control: self.control.clone(),
            required_workers: candidate.required_workers,
            poll_interval: candidate.worker_poll_interval,
            ready_timeout: candidate.worker_ready_timeout,
            allowance,
        };
        runtime::drive_candidate_for_task(runtime, Arc::clone(&self.tasks), task.task_id.clone())
            .await
    }

    fn publish(&self, task_id: &str, dedup_key: &str, event: Event) -> Result<(), String> {
        let mut frame = ProducerEvent::new(
            task_id,
            Producer::new("alloyport-server", "migration-dispatcher"),
            event,
        );
        frame.task_id = Some(task_id.to_owned());
        frame.authority = Authority::Observed;
        frame.visibility = Visibility::User;
        self.control
            .append_interaction_event(dedup_key, &frame)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

fn materialize_project(
    artifacts: &dyn ArtifactStore,
    digest: Sha256Digest,
    expected_files: u64,
    task_root: &Path,
) -> Result<PathBuf, String> {
    let mut reader = artifacts
        .open(digest)
        .map_err(|error| format!("open submitted project: {error}"))?;
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(MAX_ENCODED_PROJECT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read submitted project: {error}"))?;
    if bytes.len() as u64 > MAX_ENCODED_PROJECT_BYTES {
        return Err("encoded project exceeds dispatcher limit".to_owned());
    }
    if Sha256Digest::digest_bytes(&bytes) != digest {
        return Err("submitted project digest changed in storage".to_owned());
    }
    let project = MigrationProjectBundle::decode(bytes.as_slice())
        .map_err(|error| format!("decode submitted project: {error}"))?;
    if project.files.len() as u64 != expected_files || project.files.is_empty() {
        return Err("submitted project file count changed".to_owned());
    }
    let input_root = task_root.join("input");
    fs::create_dir_all(&input_root)
        .map_err(|error| format!("create task input directory: {error}"))?;
    let input_root = fs::canonicalize(&input_root)
        .map_err(|error| format!("resolve task input directory: {error}"))?;
    let mut previous: Option<&str> = None;
    for file in &project.files {
        let relative = BundlePath::try_from(file.path.as_str())
            .map_err(|error| format!("invalid stored project path: {error}"))?;
        if previous.is_some_and(|previous| previous >= file.path.as_str()) {
            return Err("stored project paths are not unique and sorted".to_owned());
        }
        previous = Some(file.path.as_str());
        let destination = input_root.join(relative.as_str());
        let parent = destination
            .parent()
            .ok_or_else(|| "stored project path has no parent".to_owned())?;
        fs::create_dir_all(parent).map_err(|error| format!("create project directory: {error}"))?;
        let parent = fs::canonicalize(parent)
            .map_err(|error| format!("resolve project directory: {error}"))?;
        if !parent.starts_with(&input_root) {
            return Err("stored project directory escapes task input root".to_owned());
        }
        write_once(&destination, &file.contents)?;
    }
    if !input_root.join("migration-spec-v1.json").is_file() {
        return Err("project root must contain migration-spec-v1.json".to_owned());
    }
    Ok(input_root)
}

fn write_once(path: &Path, contents: &[u8]) -> Result<(), String> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(contents)
                .map_err(|error| format!("write {}: {error}", path.display()))?;
            file.sync_all()
                .map_err(|error| format!("sync {}: {error}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| format!("inspect {}: {error}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(format!(
                    "existing project path {} is unsafe",
                    path.display()
                ));
            }
            let existing = fs::read(path)
                .map_err(|error| format!("read existing {}: {error}", path.display()))?;
            if existing == contents {
                Ok(())
            } else {
                Err(format!(
                    "existing project file {} has conflicting contents",
                    path.display()
                ))
            }
        }
        Err(error) => Err(format!("create {}: {error}", path.display())),
    }
}

/// Distinguishes two terminal events that mean different things.
fn event_content_key(event: &Event) -> String {
    let text = match event {
        Event::RunFailed { error } => error.as_str(),
        Event::RunCompleted { result } => result.as_str(),
        _ => "",
    };
    alloyport_artifacts::Sha256Digest::digest_bytes(text.as_bytes())
        .hexadecimal()
        .chars()
        .take(16)
        .collect()
}
