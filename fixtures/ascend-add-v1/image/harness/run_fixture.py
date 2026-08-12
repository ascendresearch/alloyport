#!/usr/bin/env python3
"""Compile and verify the fixed Ascend-C add fixture without invoking a shell."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

FIXTURE = "ascend-add-v1"
IMAGE_PROJECT = Path(__file__).with_name("project")
WORK_ROOT = Path("/alloyport/work")
SOURCE_LIMIT = 1024 * 1024


def fail(detail: str) -> "None":
    raise SystemExit(f"{FIXTURE}: {detail}")


def checked_source(argument: str) -> Path:
    source = Path(argument)
    try:
        stat = source.stat()
    except OSError as error:
        fail(f"cannot stat verified source: {error}")
    if not source.is_file() or source.is_symlink():
        fail("verified source must be a regular non-symlink file")
    if stat.st_size == 0 or stat.st_size > SOURCE_LIMIT:
        fail("verified source size is outside the fixed harness limit")
    return source


def run(argv: list[str], cwd: Path) -> None:
    try:
        subprocess.run(argv, cwd=cwd, stdin=subprocess.DEVNULL, stdout=sys.stderr,
                       stderr=sys.stderr, check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"command failed: {error}")


def main() -> None:
    if len(sys.argv) != 2:
        fail("expected one verified source path")
    source = checked_source(sys.argv[1])
    if WORK_ROOT.exists() and any(WORK_ROOT.iterdir()):
        fail("work tmpfs is not empty")
    temporary = WORK_ROOT / "tmp"
    temporary.mkdir(parents=True)
    os.environ["TMPDIR"] = str(temporary)
    home = WORK_ROOT / "home"
    (home / "ascend" / "log").mkdir(parents=True)
    log = WORK_ROOT / "log"
    log.mkdir()
    os.environ["HOME"] = str(home)
    os.environ["ASCEND_PROCESS_LOG_PATH"] = str(log)
    project = WORK_ROOT / "project"
    shutil.copytree(IMAGE_PROJECT, project, symlinks=False)
    shutil.copyfile(source, project / "add_custom_kernel.asc")
    build = WORK_ROOT / "build"
    run(["cmake", "-S", str(project), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release"], WORK_ROOT)
    run(["cmake", "--build", str(build), "--target", "ascend_add_fixture", "--parallel", "1"], WORK_ROOT)
    os.execv(str(build / "ascend_add_fixture"), [str(build / "ascend_add_fixture")])


if __name__ == "__main__":
    main()
