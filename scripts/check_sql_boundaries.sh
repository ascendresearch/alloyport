#!/usr/bin/env bash
set -euo pipefail

# A gate that cannot run must not report success. Every search below ends in `|| true` so that an
# empty result is not an error; that also swallowed a missing `rg` and printed a clean pass.
if ! command -v rg >/dev/null 2>&1; then
    printf 'SQL boundary check cannot run: ripgrep (rg) is not installed.\n' >&2
    exit 2
fi

sql_pattern='SELECT |INSERT |UPDATE |DELETE |CREATE TABLE|ALTER TABLE|PRAGMA |BEGIN IMMEDIATE|COMMIT;'
driver_pattern='(^|[^[:alnum:]_])rusqlite(::|\{)|use[[:space:]]+rusqlite'

is_legacy_database_module() {
    return 1
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

printf 'SQL boundary check passed; legacy database modules remaining: 0\n'
