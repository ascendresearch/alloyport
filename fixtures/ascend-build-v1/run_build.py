#!/usr/bin/env python3
"""Trusted, shell-free build entry point for one materialized candidate bundle."""

import os
import subprocess

SOURCE = "/alloyport/bundle/generated"
BUILD = "/alloyport/work/build"
# The worker sets TMPDIR here, and /alloyport/work is an empty tmpfs at start. Nobody created it,
# so every make invocation printed "TMPDIR value /alloyport/work/tmp: No such file or directory"
# and fell back to /tmp -- four lines of noise in the one output the model is asked to read.
TEMPORARY = os.environ.get("TMPDIR", "/alloyport/work/tmp")


def main() -> None:
    os.makedirs(TEMPORARY, exist_ok=True)
    os.makedirs(BUILD, exist_ok=True)
    subprocess.run(["cmake", "-S", SOURCE, "-B", BUILD], check=True)
    subprocess.run(["cmake", "--build", BUILD, "--parallel", "1"], check=True)
    print("PASS fixture=ascend-build-v1 build=complete")


if __name__ == "__main__":
    main()
