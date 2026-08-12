#!/usr/bin/env python3
"""Create the canonical JSON Artifact consumed by AscendFixtureBundle."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path(__file__).with_name("add_custom.cpp"))
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    source = arguments.source.read_text(encoding="utf-8")
    payload = {
        "schema_version": 1,
        "fixture_id": "ascend-add-v1",
        "source_sha256": "sha256:" + hashlib.sha256(source.encode()).hexdigest(),
        "source": source,
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    arguments.output.write_bytes(encoded)
    print("sha256:" + hashlib.sha256(encoded).hexdigest())
    print(len(encoded))


if __name__ == "__main__":
    main()
