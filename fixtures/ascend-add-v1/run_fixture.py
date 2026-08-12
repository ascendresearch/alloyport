#!/usr/bin/env python3
"""Dispatch the verified source to the fixture harness baked into the pinned CANN image."""

import os
import sys

HARNESS = "/opt/alloyport/fixtures/ascend-add-v1/run_fixture.py"
SOURCE = "/alloyport/bundle/add_custom.cpp"


def main() -> None:
    if not os.path.isfile(HARNESS):
        raise SystemExit("pinned image is missing the ascend-add-v1 harness")
    if not os.path.isfile(SOURCE):
        raise SystemExit("verified Ascend C source is missing")
    os.execv(sys.executable, [sys.executable, HARNESS, SOURCE])


if __name__ == "__main__":
    main()
