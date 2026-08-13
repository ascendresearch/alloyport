use alloyport_core::{
    BundlePath, Gate, GenerationStrategy, MigrationSpec, Sha256Digest, TaskState,
    inspect_migration_source,
};
use alloyport_events::{
    Authority, Event, EventEnvelope, EventSequencer, FileChange, FileChangeKind, MessageRole,
    OutputStream, Producer, ProducerEvent, RunReducer, Visibility, producer_event_from_json_line,
    render_plain,
};
use alloyport_proto::interaction_v1::SubscribeRunRequest;
use alloyport_proto::interaction_v1::interaction_service_client::InteractionServiceClient;
use alloyport_proto::management_v1::management_service_client::ManagementServiceClient;
use alloyport_proto::management_v1::{
    CancelMigrationRequest, GetMigrationRequest, GetServerStatusRequest, ListMigrationsRequest,
    ListWorkersRequest, MigrationProjectBundle, MigrationTask, MigrationTaskState, ProjectFile,
    SubmitMigrationRequest,
};
use alloyport_proto::v1::{Backend, WorkerHealth};
use alloyport_proto::{
    MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES, MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES,
};
use prost::Message;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

const MAX_SPEC_BYTES: u64 = 1024 * 1024;
const MAX_SOURCE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_PROJECT_BYTES: u64 = 60 * 1024 * 1024;
const MAX_PROJECT_FILES: usize = 4_096;

const DEFAULT_SERVER_ENDPOINT: &str = "http://127.0.0.1:50051";
const SIBLING_CONFIG_NAME: &str = "alloyport-cli.json";
const SYSTEM_CONFIG_PATH: &str = "/etc/alloyport-cli/client.json";
static CONNECTION: OnceLock<CliConnectionConfig> = OnceLock::new();

#[derive(Clone, Debug)]
struct CliConnectionConfig {
    endpoint: String,
    tls: Option<CliTlsConfig>,
}

#[derive(Clone, Debug)]
struct CliTlsConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    server_ca: PathBuf,
    server_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliFileConfig {
    schema_version: u16,
    server: CliServerFileConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliServerFileConfig {
    endpoint: String,
    tls: Option<CliTlsFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliTlsFileConfig {
    certificate: PathBuf,
    private_key: PathBuf,
    server_ca: PathBuf,
    server_name: String,
}

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
            "usage: alloyport-cli [--config PATH] <migrate PROJECT [--retry]|runs|status TASK_ID|attach TASK_ID|\
             cancel TASK_ID|\
             server status|workers|about|lifecycle|\
             inspect-migration SPEC_PATH BUNDLE_ROOT|render-events [--jsonl]|event-demo [--jsonl]>"
                .to_owned(),
        ),
    }
}

impl CliConnectionConfig {
    fn load(explicit: Option<PathBuf>) -> Result<Self, String> {
        let path = explicit
            .or_else(|| env::var_os("ALLOYPORT_CLI_CONFIG").map(PathBuf::from))
            .or_else(|| {
                env::current_exe()
                    .ok()
                    .and_then(|executable| executable.parent().map(Path::to_path_buf))
                    .map(|directory| directory.join(SIBLING_CONFIG_NAME))
                    .filter(|path| path.is_file())
            })
            .or_else(|| {
                let path = PathBuf::from(SYSTEM_CONFIG_PATH);
                path.is_file().then_some(path)
            });
        let Some(path) = path else {
            return Ok(Self {
                endpoint: DEFAULT_SERVER_ENDPOINT.to_owned(),
                tls: None,
            });
        };
        let path = fs::canonicalize(&path)
            .map_err(|error| format!("cannot open CLI config {}: {error}", path.display()))?;
        let base = path
            .parent()
            .ok_or_else(|| "CLI config has no parent directory".to_owned())?;
        let file: CliFileConfig = serde_json::from_slice(
            &fs::read(&path)
                .map_err(|error| format!("cannot read CLI config {}: {error}", path.display()))?,
        )
        .map_err(|error| format!("invalid CLI config {}: {error}", path.display()))?;
        if file.schema_version != 1 {
            return Err(format!(
                "unsupported CLI config schema {}; expected 1",
                file.schema_version
            ));
        }
        if file.server.endpoint.trim().is_empty() {
            return Err("CLI server endpoint is required".to_owned());
        }
        let tls = file.server.tls.map(|tls| CliTlsConfig {
            certificate: resolve_config_path(base, tls.certificate),
            private_key: resolve_config_path(base, tls.private_key),
            server_ca: resolve_config_path(base, tls.server_ca),
            server_name: tls.server_name,
        });
        if tls
            .as_ref()
            .is_some_and(|tls| tls.server_name.trim().is_empty())
        {
            return Err("CLI TLS server_name is required".to_owned());
        }
        Ok(Self {
            endpoint: file.server.endpoint,
            tls,
        })
    }
}

fn resolve_config_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_relative() {
        base.join(path)
    } else {
        path
    }
}

fn no_extra_arguments(arguments: &mut impl Iterator<Item = String>) -> Result<(), String> {
    if arguments.next().is_some() {
        Err("server status accepts no arguments".to_owned())
    } else {
        Ok(())
    }
}

fn server_endpoint() -> String {
    env::var("ALLOYPORT_SERVER_ENDPOINT").unwrap_or_else(|_| {
        CONNECTION.get().map_or_else(
            || DEFAULT_SERVER_ENDPOINT.to_owned(),
            |config| config.endpoint.clone(),
        )
    })
}

async fn server_channel() -> Result<Channel, String> {
    let endpoint_uri = server_endpoint();
    let mut endpoint = Endpoint::from_shared(endpoint_uri.clone())
        .map_err(|error| format!("invalid AlloyPort server endpoint {endpoint_uri}: {error}"))?;
    if let Some(tls) = CONNECTION.get().and_then(|config| config.tls.as_ref()) {
        let identity = Identity::from_pem(
            fs::read(&tls.certificate).map_err(|error| {
                format!(
                    "cannot read client certificate {}: {error}",
                    tls.certificate.display()
                )
            })?,
            fs::read(&tls.private_key).map_err(|error| {
                format!(
                    "cannot read client private key {}: {error}",
                    tls.private_key.display()
                )
            })?,
        );
        let ca = Certificate::from_pem(fs::read(&tls.server_ca).map_err(|error| {
            format!("cannot read server CA {}: {error}", tls.server_ca.display())
        })?);
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(ca)
                    .domain_name(tls.server_name.clone()),
            )
            .map_err(|error| format!("invalid CLI TLS configuration: {error}"))?;
    }
    endpoint
        .connect()
        .await
        .map_err(|error| format!("cannot connect to AlloyPort server at {endpoint_uri}: {error}"))
}

async fn management_client() -> Result<ManagementServiceClient<Channel>, String> {
    server_channel()
        .await
        .map(ManagementServiceClient::new)
        .map(|client| {
            client
                .max_encoding_message_size(MAX_MANAGEMENT_REQUEST_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MANAGEMENT_RESPONSE_MESSAGE_BYTES)
        })
}

async fn interaction_client() -> Result<InteractionServiceClient<Channel>, String> {
    server_channel()
        .await
        .map(InteractionServiceClient::new)
        .map(|client| {
            client
                .max_encoding_message_size(alloyport_proto::MAX_INTERACTION_REQUEST_MESSAGE_BYTES)
                .max_decoding_message_size(alloyport_proto::MAX_INTERACTION_EVENT_MESSAGE_BYTES)
        })
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
    while let Some(event) = stream
        .message()
        .await
        .map_err(|error| format!("run event stream failed: {error}"))?
    {
        let envelope: EventEnvelope = serde_json::from_slice(&event.envelope_json)
            .map_err(|error| format!("server returned an invalid canonical event: {error}"))?;
        reducer
            .apply(&envelope)
            .map_err(|error| format!("run event sequence is invalid: {error}"))?;
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

    #[test]
    fn first_product_fixture_is_packaged_in_stable_path_order() -> Result<(), String> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/migrations/cuda-reduction-v1");
        let project = load_project(&root)?;

        assert_eq!(project.name, "cuda-reduction-v1");
        assert!(project.files.iter().any(|file| {
            Path::new(&file.path)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("cu"))
        }));
        assert!(
            project
                .files
                .windows(2)
                .all(|pair| pair[0].path < pair[1].path)
        );
        Ok(())
    }

    #[test]
    fn explicit_cli_config_resolves_tls_files_relative_to_it() -> Result<(), String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = directory.path().join("client.json");
        fs::write(
            &path,
            r#"{
              "schema_version": 1,
              "server": {
                "endpoint": "https://controller.example:50051",
                "tls": {
                  "certificate": "pki/client.pem",
                  "private_key": "pki/client-key.pem",
                  "server_ca": "pki/ca.pem",
                  "server_name": "alloyport-server"
                }
              }
            }"#,
        )
        .map_err(|error| error.to_string())?;

        let config = CliConnectionConfig::load(Some(path))?;
        let tls = config.tls.expect("TLS config");
        assert_eq!(config.endpoint, "https://controller.example:50051");
        assert_eq!(tls.certificate, directory.path().join("pki/client.pem"));
        assert_eq!(tls.server_name, "alloyport-server");
        Ok(())
    }
}
