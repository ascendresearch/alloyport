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
if concrete=$(rg -n 'SqliteUploadStore|FilesystemArtifactStore' "${server_application_files[@]}"); then
    violations+=("concrete Artifact adapter escaped into server application code: ${concrete}")
fi

if ((${#violations[@]} != 0)); then
    printf 'Architecture boundary check failed:\n' >&2
    printf '  %s\n' "${violations[@]}" >&2
    exit 1
fi

printf 'Architecture boundary check passed; production modules <= %d lines and server Artifact ports are abstract\n' \
    "$max_production_module_lines"
