#!/usr/bin/env python3
"""Trusted fixed-fixture runner; invoked directly, never through a shell."""

import os
import subprocess

SOURCE = "/alloyport/bundle/vector_add.cu"
BINARY = "/alloyport/work/vector_add"

compile_result = subprocess.run(
    ["/usr/local/cuda/bin/nvcc", "-std=c++17", "-O2", SOURCE, "-o", BINARY],
    check=False,
)
if compile_result.returncode != 0:
    raise SystemExit(compile_result.returncode)
os.execv(BINARY, [BINARY])
