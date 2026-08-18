Migrate the immutable `cuda-reduction-v1` fixture described by the configured MigrationSpec to
Ascend C for `Ascend950PR`, CANN `9.1.0-beta.1`, compiler `ccec:dav-3510`, and ACL runtime.
Preserve the public C symbol `alloyport_reduce_sum_f32`, its status codes, zero-element behavior,
the supported bound `0 <= elements <= 1048576`, and the declared unsupported fallback. Produce a
complete bundle under `generated/`: Ascend C device code, host wrapper, build integration, and a
component-mapping document. Do not use a framework reduction operator as a hidden fallback.

The controller appends the exact validated MigrationSpec and every declared immutable CUDA source
file to this task text. It derives the context-projection and input-root digests from those bytes;
do not copy source files or supply those identities manually.

## Build environment

The Build Gate compiles your bundle inside one pinned image with CMake and no network. Replace the
facts below with ones **observed by compiling a known-good kernel in the exact image your deployment
pins** — not merely probed. Listing paths that exist is not the same as having built with them, and
that difference cost one whole migration.

- CANN root: `$ASCEND_HOME_PATH` (exported in the image)
- CANN ships a CMake package for the Ascend C language. `find_package(ASC REQUIRED)` with
  `project(... LANGUAGES ASC CXX)` registers `.asc` as a compiled language and configures the
  device compiler, its linker, and its complete include set.
- Device architecture flag, applied to ASC sources only, e.g.
  `$<$<COMPILE_LANGUAGE:ASC>:--npu-arch=<arch>>`
- ACL runtime headers for the host wrapper: `$ASCEND_HOME_PATH/<arch>-linux/include`
- Host compiler: the image's system C++ compiler; CMake uses it unless your build files say otherwise

Device sources are Ascend C and are not compiled by the host compiler.

**A recorded dead end, so you do not repeat it.** Invoking the device compiler by hand — a raw
`ccec`/`bisheng` command line with `-I .../tikcpp/tikcfw` — does not work. `kernel_operator.h`
includes `kernel_tpipe.h`, which lives in a different top-level tree, and no set of include flags you
can guess from outside the image will resolve it. A previous migration spent three of its remaining
turns on this and ran out. The language package exists precisely to own that include set.
