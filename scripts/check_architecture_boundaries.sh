#!/usr/bin/env bash
set -euo pipefail

max_production_module_lines=800
violations=()

while IFS= read -r file; do
    case "$file" in
        */tests/*|*_tests.rs)
            continue
            ;;
    esac
    lines=$(wc -l < "$file")
    if ((lines > max_production_module_lines)); then
        violations+=("$file has $lines lines (limit $max_production_module_lines)")
    fi
done < <(rg --files crates -g '*.rs')

server_application_files=(
    crates/alloyport-server/src/artifact.rs
    crates/alloyport-server/src/assignment_coordinator.rs
    crates/alloyport-server/src/attempt_observer.rs
    crates/alloyport-server/src/control_transport.rs
    crates/alloyport-server/src/interaction_service.rs
    crates/alloyport-server/src/lib.rs
)
worker_application_files=(
    crates/alloyport-worker/src/artifact_download.rs
    crates/alloyport-worker/src/artifact_upload.rs
    crates/alloyport-worker/src/cuda.rs
    crates/alloyport-worker/src/cuda_runtime.rs
    crates/alloyport-worker/src/cuda_supervisor.rs
    crates/alloyport-worker/src/executor.rs
)
if concrete=$(rg -n 'SqliteUploadStore|FilesystemArtifactStore' \
    "${server_application_files[@]}" "${worker_application_files[@]}"); then
    violations+=("concrete Artifact adapter escaped into application code: ${concrete}")
fi
if concrete=$(rg -n 'FilesystemArtifactStore' \
    crates/alloyport-artifacts/src/adapters/sqlite/upload_access_gc.rs); then
    violations+=("Artifact garbage collection regained a concrete filesystem dependency: ${concrete}")
fi
if filesystem_impl=$(rg -n \
    'pub struct FilesystemArtifactStore|use std::fs|use std::path|OpenOptions|STAGING_DIRECTORY' \
    crates/alloyport-artifacts/src/lib.rs); then
    violations+=("filesystem CAS implementation escaped its adapter: ${filesystem_impl}")
fi
if event_responsibility=$(rg -n \
    'pub struct RunReducer|pub enum ReduceError|pub fn render_plain|enum OperationKind' \
    crates/alloyport-events/src/lib.rs); then
    violations+=("event reducer or rendering responsibility returned to the schema module: ${event_responsibility}")
fi
if interaction_runtime=$(rg -n \
    'pub struct InteractionHub|tokio::sync::broadcast|sanitize_display_text|strip_terminal_sequences' \
    crates/alloyport-server/src/interaction.rs); then
    violations+=("interaction live-delivery or sanitization logic returned to the domain port: ${interaction_runtime}")
fi
if mixed_worker_store=$(rg -n 'impl AttemptStore for SqliteAttemptStore' \
    crates/alloyport-worker/src/adapters/sqlite/attempt_store.rs); then
    violations+=("worker lifecycle and outbox capabilities regained one concrete trait impl: ${mixed_worker_store}")
fi
if worker_store_capability=$(rg -n \
    'impl AttemptLifecycleStore|impl WorkerOutboxStore|enqueue_outbox_transaction' \
    crates/alloyport-worker/src/adapters/sqlite/attempt_store.rs); then
    violations+=("worker persistence capability SQL returned to the SQLite composition shell: ${worker_store_capability}")
fi
if executor_artifact_boundary=$(rg -n \
    'pub trait ArtifactPublisher|pub enum ArtifactPublicationError|pub struct ArtifactReferenceIntent|pub\(crate\) async fn store_artifact|pub\(crate\) fn producer_event' \
    crates/alloyport-worker/src/executor.rs); then
    violations+=("Artifact publication or event projection returned to the fake runtime coordinator: ${executor_artifact_boundary}")
fi
if attempt_execution_boundary=$(rg -n \
    '^    async fn ensure_execution|^    async fn handle_execution_update|^    pub\(super\) async fn handle_execution_receive|^async fn run_registered_execution' \
    crates/alloyport-worker/src/attempt_coordinator.rs); then
    violations+=("execution task/update coordination returned to attempt admission: ${attempt_execution_boundary}")
fi
if worker_delivery_boundary=$(rg -n \
    '^    pub\(super\) async fn send_ephemeral|^    pub\(super\) async fn publish_pending_terminal_artifacts|^    pub\(super\) async fn send_pending_outbox|^    pub\(super\) async fn available_slots' \
    crates/alloyport-worker/src/attempt_coordinator.rs); then
    violations+=("worker frame delivery or terminal publication returned to attempt admission: ${worker_delivery_boundary}")
fi
if docker_process_boundary=$(rg -n \
    'struct SystemDockerCommandRunner|trait DockerCommandRunner|^fn follow_command|^fn read_bounded|struct DockerCommandOutput' \
    crates/alloyport-worker/src/cuda_docker.rs); then
    violations+=("Docker process or bounded-I/O implementation returned to the engine adapter: ${docker_process_boundary}")
fi
if cuda_engine_boundary=$(rg -n \
    '^pub type EngineFuture|^pub enum ContainerEngineError|^pub struct ContainerIdentity|^pub trait CudaContainerEngine' \
    crates/alloyport-worker/src/cuda_supervisor.rs); then
    violations+=("CUDA engine port or transport values returned to the supervisor state machine: ${cuda_engine_boundary}")
fi
if cuda_outcome_policy=$(rg -n \
    '^enum Termination|^fn enforce_output_limit|^fn classify' \
    crates/alloyport-worker/src/cuda_supervisor.rs); then
    violations+=("CUDA terminal outcome policy returned to the supervisor state machine: ${cuda_outcome_policy}")
fi
if server_session_boundary=$(rg -n \
    '^    async fn register|^    async fn disconnect|^    async fn consume_stream' \
    crates/alloyport-server/src/lib.rs); then
    violations+=("worker connection session lifecycle returned to the server facade: ${server_session_boundary}")
fi
if observation_transport=$(rg -n \
    '^    pub\(super\) async fn ingest|^    pub\(super\) async fn prepare_transport_ack|validate_worker_acknowledgement|expected_worker_message_id' \
    crates/alloyport-server/src/attempt_observer.rs); then
    violations+=("worker frame sequencing or transport acknowledgement returned to attempt observation: ${observation_transport}")
fi
if observation_projection=$(rg -n \
    '^    pub\(super\) fn record_run_started|^    pub\(super\) fn record_command_started|^    pub\(super\) fn record_command_finished|^    pub\(super\) fn observe_output' \
    crates/alloyport-server/src/attempt_observer.rs); then
    violations+=("canonical interaction projection returned to attempt observation: ${observation_projection}")
fi
if storage_responsibility=$(rg -n \
    '^pub trait Clock|^pub struct WorkerRegistration|^pub enum AttemptState|^pub enum RepositoryError|^pub trait WorkerConnectionRepository|^pub trait ControlRepository' \
    crates/alloyport-server/src/storage.rs); then
    violations+=("control model, clock policy, or repository port returned to the storage facade: ${storage_responsibility}")
fi
if ! rg -q '^pub trait ControlRepository' crates/alloyport-server/src/storage/repository.rs \
    || ! rg -q '^pub struct WorkerRegistration' crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub trait Clock' crates/alloyport-server/src/storage/clock.rs; then
    violations+=("control storage model/clock/repository layering is incomplete")
fi
if raw_execution_enum=$(rg -n 'pub (executor_kind|network|outcome): i32|reason: i32' \
    crates/alloyport-server/src/storage crates/alloyport-worker/src/journal.rs); then
    violations+=("durable assignment contract regained a raw execution enum integer: ${raw_execution_enum}")
fi
if ! rg -q '^pub enum ExecutionKind' crates/alloyport-core/src/execution.rs \
    || ! rg -q '^pub enum NetworkPolicy' crates/alloyport-core/src/execution.rs \
    || ! rg -q '^pub enum AttemptOutcome' crates/alloyport-core/src/execution.rs \
    || ! rg -q '^pub enum RejectionReason' crates/alloyport-core/src/execution.rs \
    || ! rg -q 'pub executor_kind: ExecutionKind' crates/alloyport-server/src/storage/model.rs \
    || ! rg -q 'pub network: NetworkPolicy' crates/alloyport-server/src/storage/model.rs \
    || ! rg -q 'pub outcome: AttemptOutcome' crates/alloyport-server/src/storage/model.rs \
    || ! rg -q 'reason: RejectionReason' crates/alloyport-server/src/storage/model.rs \
    || ! rg -q 'pub executor_kind: ExecutionKind' crates/alloyport-worker/src/journal.rs \
    || ! rg -q 'pub network: NetworkPolicy' crates/alloyport-worker/src/journal.rs \
    || ! rg -q 'pub outcome: AttemptOutcome' crates/alloyport-worker/src/journal.rs; then
    violations+=("shared execution kind/network policy/outcome/rejection vocabulary is not used by both durable contracts")
fi
if ! rg -q 'reason: RejectionReason' crates/alloyport-worker/src/journal.rs; then
    violations+=("worker durable outbox does not use the shared rejection-reason vocabulary")
fi
if core_outer_dependency=$(rg -n \
    'alloyport-proto|rusqlite|tonic|tokio' crates/alloyport-core/Cargo.toml); then
    violations+=("domain core gained an outer-layer dependency: ${core_outer_dependency}")
fi
if interaction_store_capability=$(rg -n \
    'impl InteractionEventWriter|impl InteractionEventReader|impl InteractionRunAccessStore' \
    crates/alloyport-server/src/adapters/sqlite/interaction_store.rs); then
    violations+=("interaction persistence capability SQL returned to the SQLite composition shell: ${interaction_store_capability}")
fi
if assignment_responsibility=$(rg -n \
    'reconcile_preparing_assignments|pub\(super\) async fn prepare_assignment|pub\(super\) async fn prepare_cancel' \
    crates/alloyport-server/src/assignment_coordinator.rs); then
    violations+=("assignment reconciliation or delivery mechanics returned to the use-case coordinator: ${assignment_responsibility}")
fi
if interaction_access_policy=$(rg -n \
    'pub struct RunAuthorization|pub trait InteractionAccessPolicy|pub struct EnrolledInteractionAccessPolicy' \
    crates/alloyport-server/src/interaction_service.rs); then
    violations+=("interaction identity/access policy returned to the gRPC delivery service: ${interaction_access_policy}")
fi
if ! rg -q 'pub trait InteractionEventWriter' crates/alloyport-server/src/interaction.rs \
    || ! rg -q 'pub trait InteractionEventReader' crates/alloyport-server/src/interaction.rs \
    || ! rg -q 'pub trait InteractionRunAccessStore' crates/alloyport-server/src/interaction.rs; then
    violations+=("interaction persistence capability ports are missing")
fi
if ! rg -q 'pub trait AttemptLifecycleStore' crates/alloyport-worker/src/journal.rs \
    || ! rg -q 'pub trait WorkerOutboxStore' crates/alloyport-worker/src/journal.rs; then
    violations+=("worker journal capability ports are missing")
fi
if string_error=$(rg -n 'Future<Output = Result<\(\), String>>' \
    crates/alloyport-worker/src/executor.rs); then
    violations+=("Artifact publisher port regained an untyped String error: ${string_error}")
fi
if string_error=$(rg -n 'Future<Output = Result<T, String>>' \
    crates/alloyport-worker/src/cuda_supervisor/engine.rs); then
    violations+=("CUDA container engine port regained an untyped String error: ${string_error}")
fi
if ! rg -q '^pub trait CudaContainerEngine' \
    crates/alloyport-worker/src/cuda_supervisor/engine.rs; then
    violations+=("CUDA container engine plugin port is missing")
fi

if ((${#violations[@]} != 0)); then
    printf 'Architecture boundary check failed:\n' >&2
    printf '  %s\n' "${violations[@]}" >&2
    exit 1
fi

printf 'Architecture boundary check passed; production modules <= %d lines and plugin ports are abstract and typed\n' \
    "$max_production_module_lines"
