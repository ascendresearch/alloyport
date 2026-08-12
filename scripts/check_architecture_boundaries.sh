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

worker_main=crates/alloyport-worker/src/main.rs
worker_main_lines=$(wc -l < "$worker_main")
if ((worker_main_lines > 20)); then
    violations+=("worker binary entry point has $worker_main_lines lines (limit 20); process wiring belongs in application/")
fi
if worker_main_responsibility=$(rg -n \
    'WorkerFileConfig|BackendPolicy|FilesystemArtifactStore|DockerCliEngine|NvidiaSmi|NpuSmi|run_session|serde' \
    "$worker_main"); then
    violations+=("worker configuration, adapter assembly, or runtime lifecycle escaped into main.rs: ${worker_main_responsibility}")
fi
if ! rg -q '^pub async fn run_from_args\(' \
    crates/alloyport-worker/src/application/mod.rs; then
    violations+=("worker application composition entry point is missing")
fi
if config_runtime_coupling=$(rg -n \
    'OutboundWorker|FilesystemArtifactStore|DockerCliEngine|NvidiaSmi|NpuSmi|run_session' \
    crates/alloyport-worker/src/application/config.rs \
    crates/alloyport-worker/src/application/backend_config.rs); then
    violations+=("worker configuration modules gained runtime or concrete adapter wiring: ${config_runtime_coupling}")
fi
if lifecycle_backend_coupling=$(rg -n \
    'BackendPolicy|FilesystemArtifactStore|DockerCliEngine|NvidiaSmi|NpuSmi|Cuda|Ascend' \
    crates/alloyport-worker/src/application/runtime.rs); then
    violations+=("worker process lifecycle gained backend-specific composition: ${lifecycle_backend_coupling}")
fi

server_main=crates/alloyport-server/src/main.rs
server_main_lines=$(wc -l < "$server_main")
if ((server_main_lines > 20)); then
    violations+=("server binary entry point has $server_main_lines lines (limit 20); process wiring belongs in application/")
fi
if server_main_responsibility=$(rg -n \
    'Sqlite|FilesystemArtifactStore|Server::builder|run_lease_reaper|run_preparation_reconciler|std::env|abort\(' \
    "$server_main"); then
    violations+=("server configuration, adapter assembly, or task lifecycle escaped into main.rs: ${server_main_responsibility}")
fi
if ! rg -q '^pub async fn run_from_args\(' \
    crates/alloyport-server/src/application/mod.rs; then
    violations+=("server application composition entry point is missing")
fi
if ! rg -q 'schema_version: u16' crates/alloyport-server/src/application/config.rs \
    || ! rg -q 'serde\(deny_unknown_fields\)' crates/alloyport-server/src/application/config.rs \
    || ! rg -q 'ALLOYPORT_SERVER_CONFIG' crates/alloyport-server/src/application/config.rs \
    || ! rg -q 'ServerCommand::from_process_args' crates/alloyport-server/src/application/mod.rs; then
    violations+=("strict versioned server configuration or shared command boundary is missing")
fi
if server_config_coupling=$(rg -n \
    'Sqlite|FilesystemArtifactStore|WorkerControlService|Server::builder|run_lease_reaper' \
    crates/alloyport-server/src/application/config.rs); then
    violations+=("server process configuration gained concrete adapter or service assembly: ${server_config_coupling}")
fi
if server_runtime_coupling=$(rg -n \
    'Sqlite|FilesystemArtifactStore|std::env|IdentityRegistry|certificate_fingerprint' \
    crates/alloyport-server/src/application/runtime.rs); then
    violations+=("server task lifecycle gained configuration, identity administration, or storage assembly: ${server_runtime_coupling}")
fi
if server_assembly_environment=$(rg -n 'std::env|env::args|env::var' \
    crates/alloyport-server/src/application/assembly.rs); then
    violations+=("server concrete assembly started reading process environment directly: ${server_assembly_environment}")
fi
if server_identity_environment=$(rg -n 'std::env|env::args|env::var' \
    crates/alloyport-server/src/application/identity_admin.rs); then
    violations+=("server identity administration started bypassing shared process configuration: ${server_identity_environment}")
fi
if ! rg -q 'serve_with_shutdown' crates/alloyport-server/src/application/runtime.rs \
    || ! rg -q 'tokio::time::timeout' crates/alloyport-server/src/application/runtime.rs \
    || ! rg -q 'run_lease_reaper_until' crates/alloyport-server/src/application/runtime.rs \
    || ! rg -q 'run_preparation_reconciler_until' crates/alloyport-server/src/application/runtime.rs; then
    violations+=("server runtime does not retain graceful listener shutdown and bounded cooperative task drain")
fi

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
if ! rg -q '^pub struct InMemoryArtifactStore' \
    crates/alloyport-artifacts/src/adapters/memory.rs \
    || ! rg -q '^impl ArtifactStore for InMemoryArtifactStore' \
        crates/alloyport-artifacts/src/adapters/memory.rs \
    || ! rg -q '^impl ArtifactRetentionStore for InMemoryArtifactStore' \
        crates/alloyport-artifacts/src/adapters/memory.rs \
    || ! rg -q '^fn filesystem_store_satisfies_immutable_artifact_contract' \
        crates/alloyport-artifacts/src/artifact_store_contract_tests.rs \
    || ! rg -q '^fn memory_store_satisfies_immutable_artifact_contract' \
        crates/alloyport-artifacts/src/artifact_store_contract_tests.rs; then
    violations+=("immutable Artifact adapters no longer share one Port contract suite")
fi
if ephemeral_artifact_composition=$(rg -n 'InMemoryArtifactStore' \
    crates/alloyport-server/src/application/assembly.rs \
    crates/alloyport-worker/src/application/assembly.rs); then
    violations+=("non-durable Artifact adapter entered a production composition root: ${ephemeral_artifact_composition}")
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
if broad_control_repository=$(rg -n '^    repository: Arc<dyn ControlRepository>|self\.repository\b|service\.repository\b' \
    crates/alloyport-server/src --glob '*.rs'); then
    violations+=("application use case regained the broad ControlRepository dependency: ${broad_control_repository}")
fi
if broad_assignment_repository=$(rg -n 'repositories\.assignments\b' \
    crates/alloyport-server/src --glob '*.rs'); then
    violations+=("application use case regained the combined assignment repository: ${broad_assignment_repository}")
fi
if ! rg -q 'connections: Arc<dyn WorkerConnectionRepository>' crates/alloyport-server/src/lib.rs \
    || ! rg -q 'assignment_reads: Arc<dyn AssignmentReadRepository>' crates/alloyport-server/src/lib.rs \
    || ! rg -q 'assignment_writes: Arc<dyn AssignmentWriteRepository>' crates/alloyport-server/src/lib.rs \
    || ! rg -q 'attempts: Arc<dyn AttemptLifecycleRepository>' crates/alloyport-server/src/lib.rs \
    || ! rg -q 'outbox: Arc<dyn ServerOutboxRepository>' crates/alloyport-server/src/lib.rs \
    || ! rg -q '^    pub fn with_repository_capabilities\(' crates/alloyport-server/src/lib.rs; then
    violations+=("control service does not expose independently composable narrow repository ports")
fi
if ! rg -q '^pub trait AssignmentReadRepository' crates/alloyport-server/src/storage/repository.rs \
    || ! rg -q '^pub trait AssignmentWriteRepository' crates/alloyport-server/src/storage/repository.rs \
    || ! rg -q '^pub trait AssignmentRepository: AssignmentReadRepository .*AssignmentWriteRepository' \
        crates/alloyport-server/src/storage/repository.rs; then
    violations+=("assignment read/write ports are not capability-segregated")
fi
if raw_execution_enum=$(rg -n 'pub (executor_kind|network|outcome): i32|reason: i32' \
    crates/alloyport-server/src/storage crates/alloyport-worker/src/journal.rs); then
    violations+=("durable assignment contract regained a raw execution enum integer: ${raw_execution_enum}")
fi
if ! rg -q '^pub enum ExecutionKind' crates/alloyport-core/src/execution.rs \
    || ! rg -q '^pub enum NetworkPolicy' crates/alloyport-core/src/execution.rs \
    || ! rg -q '^pub enum AttemptOutcome' crates/alloyport-core/src/execution.rs \
    || ! rg -q '^pub enum RejectionReason' crates/alloyport-core/src/execution.rs \
    || ! rg -q 'pub executor_kind: ExecutionKind' crates/alloyport-core/src/assignment.rs \
    || ! rg -q 'pub network: NetworkPolicy' crates/alloyport-core/src/assignment.rs \
    || ! rg -q '^pub type ExecutionContract = alloyport_core::ExecutionContract' \
        crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub type StoredExecution = alloyport_core::ExecutionContract' \
        crates/alloyport-worker/src/journal.rs \
    || ! rg -q 'pub outcome: AttemptOutcome' crates/alloyport-server/src/storage/model.rs \
    || ! rg -q 'reason: RejectionReason' crates/alloyport-server/src/storage/model.rs \
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
if ! rg -q 'AttemptId,' crates/alloyport-core/src/identity.rs \
    || ! rg -q 'AssignmentId,' crates/alloyport-core/src/identity.rs \
    || ! rg -q 'TaskId,' crates/alloyport-core/src/identity.rs \
    || ! rg -q 'CandidateId,' crates/alloyport-core/src/identity.rs \
    || ! rg -q -U 'pub struct AssignmentContract \{[^}]*pub assignment_id: AssignmentId[^}]*pub attempt_id: AttemptId[^}]*pub task_id: TaskId[^}]*pub candidate_id: CandidateId[^}]*pub execution: ExecutionContract' \
        crates/alloyport-core/src/assignment.rs \
    || ! rg -q -U 'pub struct ExecutionContract \{[^}]*pub environment: Vec<EnvironmentEntry>[^}]*pub bundle: ArtifactDescriptor[^}]*pub image: ArtifactDescriptor[^}]*pub limits: Option<ResourceContract>' \
        crates/alloyport-core/src/assignment.rs \
    || ! rg -q '^pub type AssignmentContract = alloyport_core::AssignmentContract' \
        crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub type StoredAssignment = alloyport_core::AssignmentContract' \
        crates/alloyport-worker/src/journal.rs \
    || ! rg -q '^pub type ExecutionContract = alloyport_core::ExecutionContract' \
        crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub type StoredExecution = alloyport_core::ExecutionContract' \
        crates/alloyport-worker/src/journal.rs \
    || ! rg -q '^pub type EnvironmentEntry = alloyport_core::EnvironmentEntry' \
        crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub type StoredEnvironment = alloyport_core::EnvironmentEntry' \
        crates/alloyport-worker/src/journal.rs \
    || ! rg -q '^pub type ResourceContract = alloyport_core::ResourceContract' \
        crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub type StoredLimits = alloyport_core::ResourceContract' \
        crates/alloyport-worker/src/journal.rs; then
    violations+=("server and worker do not retain the shared core immutable assignment vocabulary")
fi
if duplicate_assignment_contract=$(rg -n '^pub struct (AssignmentContract|ExecutionContract|EnvironmentEntry|ResourceContract|StoredAssignment|StoredExecution|StoredEnvironment|StoredLimits)' \
    crates/alloyport-server/src/storage/model.rs crates/alloyport-worker/src/journal.rs); then
    violations+=("duplicate immutable assignment contract returned outside core: ${duplicate_assignment_contract}")
fi
if ! rg -q 'require_text\("assignment.candidate_id"' crates/alloyport-proto/src/lib.rs \
    || ! rg -q -U 'pub struct Task \{[^}]*pub id: TaskId' crates/alloyport-core/src/lib.rs \
    || ! rg -q -U 'pub struct Candidate \{[^}]*pub id: CandidateId[^}]*pub task_id: TaskId[^}]*pub parent_id: Option<CandidateId>' \
        crates/alloyport-core/src/lib.rs \
    || ! rg -q -U 'pub struct Verdict \{[^}]*pub candidate_id: CandidateId' \
        crates/alloyport-core/src/lib.rs \
    || ! rg -q -U 'pub struct ReleaseManifest \{[^}]*pub candidate_id: CandidateId' \
        crates/alloyport-core/src/lib.rs; then
    violations+=("core task/candidate release models or wire validation regained raw candidate identity semantics")
fi
if ! rg -q '^pub struct Sha256Digest' crates/alloyport-core/src/artifact.rs \
    || ! rg -q -U 'pub struct ArtifactDescriptor \{[^}]*pub digest: Sha256Digest' \
        crates/alloyport-core/src/artifact.rs \
    || ! rg -q 'pub use alloyport_core::\{DigestParseError, Sha256Digest\}' \
        crates/alloyport-artifacts/src/lib.rs \
    || ! rg -q '^pub type ArtifactIdentity = ArtifactDescriptor' \
        crates/alloyport-server/src/storage/model.rs \
    || ! rg -q '^pub type StoredArtifact = ArtifactDescriptor' \
        crates/alloyport-worker/src/journal.rs \
    || ! rg -q -U 'pub struct Candidate \{[^}]*pub source_digest: Sha256Digest[^}]*pub artifact_digest: Option<Sha256Digest>' \
        crates/alloyport-core/src/lib.rs \
    || ! rg -q -U 'pub struct Verdict \{[^}]*pub receipt_digests: Vec<Sha256Digest>' \
        crates/alloyport-core/src/lib.rs \
    || ! rg -q -U 'pub struct ReleaseManifest \{[^}]*pub evidence_digests: BTreeSet<Sha256Digest>' \
        crates/alloyport-core/src/lib.rs; then
    violations+=("shared Artifact descriptor or validated digest vocabulary is not retained across domain contracts")
fi
if duplicate_digest_type=$(rg -n '^pub struct Sha256Digest' crates/alloyport-artifacts/src/lib.rs); then
    violations+=("duplicate digest domain representation returned outside core: ${duplicate_digest_type}")
fi
if duplicate_descriptor=$(rg -n '^pub struct (ArtifactIdentity|StoredArtifact)' \
    crates/alloyport-server/src/storage/model.rs crates/alloyport-worker/src/journal.rs); then
    violations+=("duplicate Artifact descriptor returned outside core: ${duplicate_descriptor}")
fi
for trusted_outbox_variant in AssignmentAccepted ExecutionStarted ExecutionFinished CancellationAcknowledged; do
    if ! rg -q -U "${trusted_outbox_variant} \\{[^}]*(assignment_id: AssignmentId[^}]*attempt_id: AttemptId|attempt_id: AttemptId[^}]*assignment_id: AssignmentId)" \
        crates/alloyport-worker/src/journal.rs; then
        violations+=("trusted worker outbox variant ${trusted_outbox_variant} does not retain typed assignment/attempt identities")
    fi
done
if ! rg -q -U 'AssignmentRejected \{[^}]*assignment_id: String[^}]*attempt_id: String' \
    crates/alloyport-worker/src/journal.rs; then
    violations+=("worker rejection outbox no longer preserves invalid wire identities as boundary text")
fi
if ! rg -q -U 'pub struct ObservedAttempt \{[^}]*pub assignment_id: AssignmentId[^}]*pub attempt_id: AttemptId' \
    crates/alloyport-server/src/storage/model.rs; then
    violations+=("server durable observation contract does not retain typed assignment/attempt identities")
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
if backend_string_error=$(rg -n \
    'BackendExecutionFuture.*ExecutionRuntimeError|Future<Output = Result<ExecutionRun, (String|ExecutionRuntimeError)>|result: Result<\(\), String>' \
    crates/alloyport-worker/src/execution_backend.rs crates/alloyport-worker/src/lib.rs); then
    violations+=("execution backend boundary regained an internal or String error: ${backend_string_error}")
fi
if ! rg -q '^pub enum BackendFailureClass' crates/alloyport-worker/src/backend_error.rs \
    || ! rg -q '^pub enum BackendError' crates/alloyport-worker/src/backend_error.rs \
    || ! rg -q 'Future<Output = Result<ExecutionRun, BackendError>>' \
        crates/alloyport-worker/src/execution_backend.rs \
    || ! rg -q 'result: Result<\(\), BackendError>' crates/alloyport-worker/src/lib.rs; then
    violations+=("execution backend failures do not retain typed categories through coordination")
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
