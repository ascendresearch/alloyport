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
