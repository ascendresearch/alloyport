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
facts below with ones probed from the exact image your deployment pins; they are environment facts,
not a prescribed method, and a wrong one costs a whole build round trip. The first migration to
reach this compiler spent two builds discovering that `acl/acl.h` and `kernel_operator.h` were not
on the default include path.

- CANN root: `$ASCEND_HOME_PATH` (exported in the image)
- ACL runtime headers: `$ASCEND_HOME_PATH/<arch>-linux/include`
- Ascend C kernel headers: `$ASCEND_HOME_PATH/<arch>-linux/tikcpp/tikcfw`
- Ascend C compiler: `$ASCEND_HOME_PATH/<arch>-linux/ccec_compiler/bin/ccec`
- Host compiler: the image's system C++ compiler; CMake uses it unless your build files say otherwise

Device sources are Ascend C and are not compiled by the host compiler. Your build integration is
responsible for pointing at these locations; nothing is added to the include path for you.
