use alloyport_core::{
    BundlePath, Gate, GenerationStrategy, MigrationSpec, Sha256Digest, TaskState,
    inspect_migration_source,
};
use alloyport_events::{Event, EventEnvelope, RunReducer, render_plain};
use alloyport_proto::interaction_v1::SubscribeRunRequest;
use alloyport_proto::management_v1::{
    CancelMigrationRequest, GetMigrationRequest, GetServerStatusRequest, ListMigrationsRequest,
    ListWorkersRequest, MigrationProjectBundle, MigrationTask, MigrationTaskState, ProjectFile,
    ResumeMigrationRequest, SubmitMigrationRequest,
};
use alloyport_proto::v1::{Backend, WorkerHealth};
use prost::Message;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

mod connection;
mod events;

use connection::{CONNECTION, CliConnectionConfig, interaction_client, management_client};
use events::{render_demo, render_events};

const MAX_SPEC_BYTES: u64 = 1024 * 1024;
const MAX_SOURCE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_PROJECT_BYTES: u64 = 60 * 1024 * 1024;
const MAX_PROJECT_FILES: usize = 4_096;

#[tokio::main]
async fn main() -> ExitCode {
    let mut arguments = env::args().skip(1);
    let result = run(&mut arguments).await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

async fn run(arguments: &mut impl Iterator<Item = String>) -> Result<(), String> {
    let first = arguments.next();
    let (explicit_config, command) = if first.as_deref() == Some("--config") {
        let path = arguments
            .next()
            .ok_or_else(|| "--config requires PATH".to_owned())?;
        (Some(PathBuf::from(path)), arguments.next())
    } else {
        (None, first)
    };
    CONNECTION
        .set(CliConnectionConfig::load(explicit_config)?)
        .map_err(|_| "CLI connection was initialized twice".to_owned())?;
    match command.as_deref() {
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
        Some("server") if arguments.next().as_deref() == Some("status") => {
            match no_extra_arguments(arguments) {
                Ok(()) => server_status().await,
                Err(error) => Err(error),
            }
        }
        Some("workers") => match arguments.next() {
            None => list_workers().await,
            Some(_) => Err("workers accepts no arguments".to_owned()),
        },
        Some("migrate") => {
            let path = arguments
                .next()
                .ok_or_else(|| "migrate requires PROJECT".to_owned())?;
            let retry = match arguments.next().as_deref() {
                None => false,
                Some("--retry") if arguments.next().is_none() => true,
                Some(_) => return Err("migrate accepts PROJECT and optional --retry".to_owned()),
            };
            submit_migration(Path::new(&path), retry).await
        }
        Some("runs") => match arguments.next() {
            None => list_migrations().await,
            Some(_) => Err("runs accepts no arguments".to_owned()),
        },
        Some("status") => match one_text_argument(arguments, "status", "TASK_ID") {
            Ok(task_id) => get_migration(task_id).await,
            Err(error) => Err(error),
        },
        Some("cancel") => match one_text_argument(arguments, "cancel", "TASK_ID") {
            Ok(task_id) => cancel_migration(task_id).await,
            Err(error) => Err(error),
        },
        Some("resume") => match one_text_argument(arguments, "resume", "TASK_ID") {
            Ok(task_id) => resume_migration(task_id).await,
            Err(error) => Err(error),
        },
        Some("attach") => match one_text_argument(arguments, "attach", "TASK_ID") {
            Ok(task_id) => attach_migration(task_id).await,
            Err(error) => Err(error),
        },
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
            "usage: alloyport-cli [--config PATH] <migrate PROJECT [--retry]|resume TASK_ID|runs|status TASK_ID|attach TASK_ID|\
             cancel TASK_ID|\
             server status|workers|about|lifecycle|\
             inspect-migration SPEC_PATH BUNDLE_ROOT|render-events [--jsonl]|event-demo [--jsonl]>"
                .to_owned(),
        ),
    }
}

fn no_extra_arguments(arguments: &mut impl Iterator<Item = String>) -> Result<(), String> {
    if arguments.next().is_some() {
        Err("server status accepts no arguments".to_owned())
    } else {
        Ok(())
    }
}

fn one_text_argument(
    arguments: &mut impl Iterator<Item = String>,
    command: &str,
    name: &str,
) -> Result<String, String> {
    let value = arguments
        .next()
        .ok_or_else(|| format!("{command} requires {name}"))?;
    if arguments.next().is_some() {
        return Err(format!("{command} accepts exactly one {name}"));
    }
    Ok(value)
}

async fn submit_migration(project_root: &Path, retry: bool) -> Result<(), String> {
    let project = load_project(project_root)?;
    let digest = Sha256Digest::digest_bytes(&project.encode_to_vec()).hexadecimal();
    let request_id = if retry {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "system clock is before the Unix epoch".to_owned())?
            .as_nanos();
        format!("cli-retry-{}-{nonce}", &digest[..32])
    } else {
        format!("cli-{}", &digest[..32])
    };
    let request = SubmitMigrationRequest {
        request_id,
        project: Some(project),
    };
    let task = management_client()
        .await?
        .submit_migration(request)
        .await
        .map_err(|error| format!("migration submission failed: {error}"))?
        .into_inner();
    print_task(&task);
    println!("Attach with: alloyport-cli attach {}", task.task_id);
    Ok(())
}

async fn get_migration(task_id: String) -> Result<(), String> {
    let task = management_client()
        .await?
        .get_migration(GetMigrationRequest { task_id })
        .await
        .map_err(|error| format!("migration status request failed: {error}"))?
        .into_inner();
    print_task(&task);
    Ok(())
}

async fn list_migrations() -> Result<(), String> {
    let tasks = management_client()
        .await?
        .list_migrations(ListMigrationsRequest { limit: 0 })
        .await
        .map_err(|error| format!("migration list request failed: {error}"))?
        .into_inner()
        .tasks;
    if tasks.is_empty() {
        println!("No migration tasks.");
        return Ok(());
    }
    println!("TASK\tSTATE\tPROJECT\tFILES\tBYTES");
    for task in tasks {
        println!(
            "{}\t{}\t{}\t{}\t{}",
            task.task_id,
            task_state(&task),
            task.project_name,
            task.file_count,
            task.project_size_bytes
        );
    }
    Ok(())
}

async fn cancel_migration(task_id: String) -> Result<(), String> {
    let task = management_client()
        .await?
        .cancel_migration(CancelMigrationRequest { task_id })
        .await
        .map_err(|error| format!("migration cancellation failed: {error}"))?
        .into_inner();
    print_task(&task);
    Ok(())
}

/// Continues a failed migration's Episode instead of starting a new one.
///
/// A retry mints a new task and therefore a new Episode, which throws away every turn already
/// taken; four consecutive live retries each re-read the same reference corpus before doing
/// anything. Resuming keeps that work.
async fn resume_migration(task_id: String) -> Result<(), String> {
    let task = management_client()
        .await?
        .resume_migration(ResumeMigrationRequest { task_id })
        .await
        .map_err(|error| format!("migration resumption failed: {error}"))?
        .into_inner();
    print_task(&task);
    Ok(())
}

async fn attach_migration(task_id: String) -> Result<(), String> {
    let mut stream = interaction_client()
        .await?
        .subscribe_run(SubscribeRunRequest {
            run_id: task_id,
            after_sequence: 0,
        })
        .await
        .map_err(|error| format!("run attachment failed: {error}"))?
        .into_inner();
    let mut reducer = RunReducer::new();
    let mut stdout = io::stdout().lock();
    // The reducer checks the run contract; attach shows the run. A stream that violates the
    // contract is exactly when an operator most needs to see it, so a violation is reported once
    // and rendering continues. Aborting instead made every migration unwatchable the moment two
    // producers both published `run.started`, and left eight recorded runs unreadable after it.
    let mut reported = false;
    while let Some(event) = stream
        .message()
        .await
        .map_err(|error| format!("run event stream failed: {error}"))?
    {
        let envelope: EventEnvelope = serde_json::from_slice(&event.envelope_json)
            .map_err(|error| format!("server returned an invalid canonical event: {error}"))?;
        if let Err(error) = reducer.apply(&envelope)
            && !reported
        {
            reported = true;
            eprintln!(
                "warning: this run's event sequence violates the canonical contract ({error}); \
                 showing it anyway, and the lifecycle summary below may be wrong"
            );
        }
        stdout
            .write_all(render_plain(&envelope).as_bytes())
            .map_err(|error| error.to_string())?;
        stdout.flush().map_err(|error| error.to_string())?;
        if matches!(
            envelope.event,
            Event::RunCompleted { .. } | Event::RunFailed { .. }
        ) {
            break;
        }
    }
    Ok(())
}

fn print_task(task: &MigrationTask) {
    println!("task: {}", task.task_id);
    println!("state: {}", task_state(task));
    println!("project: {}", task.project_name);
    println!("files: {}", task.file_count);
    println!("bytes: {}", task.project_size_bytes);
    println!("bundle: {}", task.project_digest);
}

fn task_state(task: &MigrationTask) -> &'static str {
    match MigrationTaskState::try_from(task.state).unwrap_or(MigrationTaskState::Unspecified) {
        MigrationTaskState::Captured => "captured",
        MigrationTaskState::Running => "running",
        MigrationTaskState::Completed => "completed",
        MigrationTaskState::Failed => "failed",
        MigrationTaskState::Cancelled => "cancelled",
        MigrationTaskState::Unspecified => "unknown",
    }
}

fn load_project(project_root: &Path) -> Result<MigrationProjectBundle, String> {
    let root = fs::canonicalize(project_root)
        .map_err(|error| format!("cannot open project {}: {error}", project_root.display()))?;
    if !root.is_dir() {
        return Err(format!("project {} is not a directory", root.display()));
    }
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "project directory name is not UTF-8".to_owned())?
        .to_owned();
    let mut files = BTreeMap::new();
    collect_project_files(&root, &root, &mut files)?;
    if files.is_empty() {
        return Err("project contains no files".to_owned());
    }
    if files.len() > MAX_PROJECT_FILES {
        return Err(format!(
            "project contains more than {MAX_PROJECT_FILES} files"
        ));
    }
    let mut total = 0_u64;
    let files = files
        .into_iter()
        .map(|(path, contents)| {
            total = total.saturating_add(contents.len() as u64);
            ProjectFile { path, contents }
        })
        .collect::<Vec<_>>();
    if total > MAX_PROJECT_BYTES {
        return Err(format!(
            "project exceeds the {MAX_PROJECT_BYTES} byte limit"
        ));
    }
    Ok(MigrationProjectBundle { name, files })
}

fn collect_project_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("cannot read {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("cannot read directory entry: {error}"))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!("project entry {} is a symlink", path.display()));
        }
        if metadata.is_dir() {
            if !ignored_directory(&entry.file_name()) {
                collect_project_files(root, &path, files)?;
            }
            continue;
        }
        if !metadata.is_file() {
            return Err(format!(
                "project entry {} is not a regular file",
                path.display()
            ));
        }
        if metadata.len() > MAX_SOURCE_BYTES {
            return Err(format!(
                "project file {} exceeds the {MAX_SOURCE_BYTES} byte limit",
                path.display()
            ));
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| format!("project entry {} escapes its root", path.display()))?;
        let relative = relative
            .to_str()
            .ok_or_else(|| format!("project path {} is not UTF-8", relative.display()))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        let relative = BundlePath::try_from(relative.as_str())
            .map_err(|error| format!("invalid project path {relative}: {error}"))?;
        let contents = fs::read(&path)
            .map_err(|error| format!("cannot read project file {}: {error}", path.display()))?;
        files.insert(relative.as_str().to_owned(), contents);
    }
    Ok(())
}

fn ignored_directory(name: &std::ffi::OsStr) -> bool {
    matches!(name.to_str(), Some(".git" | "target" | "build" | ".cache"))
}

async fn server_status() -> Result<(), String> {
    let response = management_client()
        .await?
        .get_server_status(GetServerStatusRequest {})
        .await
        .map_err(|error| format!("server status request failed: {error}"))?
        .into_inner();
    println!("server: AlloyPort {}", response.server_version);
    println!(
        "protocol: {}.{}",
        response.protocol_major, response.protocol_minor
    );
    println!(
        "workers: {} connected / {} known",
        response.connected_worker_count, response.worker_count
    );
    Ok(())
}

async fn list_workers() -> Result<(), String> {
    let response = management_client()
        .await?
        .list_workers(ListWorkersRequest {})
        .await
        .map_err(|error| format!("worker list request failed: {error}"))?
        .into_inner();
    if response.workers.is_empty() {
        println!("No workers registered.");
        return Ok(());
    }
    println!("WORKER\tINSTANCE\tSTATE\tHEALTH\tSLOTS\tDEVICE\tBACKEND\tSEQUENCE\tFEATURES");
    for worker in response.workers {
        let backend = Backend::try_from(worker.backend)
            .unwrap_or(Backend::Unspecified)
            .as_str_name();
        let health = WorkerHealth::try_from(worker.health)
            .unwrap_or(WorkerHealth::Unspecified)
            .as_str_name();
        let availability = if !worker.connected {
            "offline"
        } else if worker.health == WorkerHealth::Ready as i32 && worker.available_slots == 0 {
            "busy"
        } else if worker.health == WorkerHealth::Ready as i32 {
            "available"
        } else {
            "unavailable"
        };
        let devices = worker
            .devices
            .iter()
            .map(|device| format!("{}:{}proc", device.device_id, device.process_count))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            worker.worker_id,
            worker.instance_id,
            availability,
            health,
            worker.available_slots,
            devices,
            backend,
            worker.last_worker_sequence,
            worker.features.join(",")
        );
    }
    Ok(())
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
#[path = "main_tests.rs"]
mod tests;
