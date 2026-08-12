#!/usr/bin/env bash
set -euo pipefail

target=x86_64-unknown-linux-musl
portable_cargo=${ALLOYPORT_PORTABLE_CARGO:-cargo}

for tool in "$portable_cargo" rustup x86_64-linux-musl-gcc file sha256sum; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "required portable-build tool is missing: $tool" >&2
        exit 1
    fi
done

if ! rustup target list --installed | grep -Fxq "$target"; then
    echo "Rust target $target is not installed" >&2
    exit 1
fi

"$portable_cargo" build \
    --locked \
    --release \
    --target "$target" \
    -p alloyport-server \
    -p alloyport-worker

output_directory="${CARGO_TARGET_DIR:-target}/$target/release"
binaries=(alloyport-server alloyport-worker)
for binary in "${binaries[@]}"; do
    binary_path="$output_directory/$binary"
    description=$(file "$binary_path")
    if [[ $description != *"static-pie linked"* && $description != *"statically linked"* ]]; then
        echo "$binary is not statically linked: $description" >&2
        exit 1
    fi
done

(
    cd "$output_directory"
    sha256sum "${binaries[@]}" > SHA256SUMS
)

echo "portable Linux binaries: $output_directory"
cat "$output_directory/SHA256SUMS"
