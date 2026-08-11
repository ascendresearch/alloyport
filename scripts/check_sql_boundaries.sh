#!/usr/bin/env bash
set -euo pipefail

sql_pattern='SELECT |INSERT |UPDATE |DELETE |CREATE TABLE|ALTER TABLE|PRAGMA |BEGIN IMMEDIATE|COMMIT;'
driver_pattern='(^|[^[:alnum:]_])rusqlite(::|\{)|use[[:space:]]+rusqlite'

# These two mixed modules are acknowledged migration debt. Remove each entry when its SQL moves
# beneath an adapters/sqlite directory. No new legacy location may be added.
is_legacy_database_module() {
    case "$1" in
        crates/alloyport-artifacts/src/upload.rs | \
        crates/alloyport-server/src/interaction.rs)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

is_sqlite_adapter() {
    case "$1" in
        */src/adapters/sqlite/*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

violations=()
while IFS= read -r file; do
    if ! is_sqlite_adapter "$file" && ! is_legacy_database_module "$file"; then
        violations+=("$file")
    fi
done < <(rg -l --glob '*.rs' "$sql_pattern" crates || true)

while IFS= read -r file; do
    if ! is_sqlite_adapter "$file" && ! is_legacy_database_module "$file"; then
        violations+=("$file")
    fi
done < <(rg -l --glob '*.rs' "$driver_pattern" crates || true)

if ((${#violations[@]} != 0)); then
    printf 'SQL or rusqlite escaped the SQLite implementation boundary:\n' >&2
    printf '  %s\n' "${violations[@]}" | sort -u >&2
    exit 1
fi

printf 'SQL boundary check passed; legacy database modules remaining: 2\n'
