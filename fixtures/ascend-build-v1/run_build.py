#!/usr/bin/env python3
"""Trusted, shell-free build entry point for one materialized candidate bundle."""

import os
import subprocess

SOURCE = "/alloyport/bundle/generated"
BUILD = "/alloyport/work/build"


def main() -> None:
    os.makedirs(BUILD, exist_ok=True)
    subprocess.run(["cmake", "-S", SOURCE, "-B", BUILD], check=True)
    subprocess.run(["cmake", "--build", BUILD, "--parallel", "1"], check=True)
    print("PASS fixture=ascend-build-v1 build=complete")


if __name__ == "__main__":
    main()
