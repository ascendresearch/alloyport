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
if docker_process_boundary=$(rg -n \
    'struct SystemDockerCommandRunner|trait DockerCommandRunner|^fn follow_command|^fn read_bounded|struct DockerCommandOutput' \
    crates/alloyport-worker/src/cuda_docker.rs); then
    violations+=("Docker process or bounded-I/O implementation returned to the engine adapter: ${docker_process_boundary}")
fi
if server_session_boundary=$(rg -n \
    '^    async fn register|^    async fn disconnect|^    async fn consume_stream' \
    crates/alloyport-server/src/lib.rs); then
    violations+=("worker connection session lifecycle returned to the server facade: ${server_session_boundary}")
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
    crates/alloyport-worker/src/cuda_supervisor.rs); then
    violations+=("CUDA container engine port regained an untyped String error: ${string_error}")
fi

if ((${#violations[@]} != 0)); then
    printf 'Architecture boundary check failed:\n' >&2
    printf '  %s\n' "${violations[@]}" >&2
    exit 1
fi

printf 'Architecture boundary check passed; production modules <= %d lines and plugin ports are abstract and typed\n' \
    "$max_production_module_lines"
