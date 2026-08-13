Migrate the immutable `cuda-reduction-v1` fixture described by the configured MigrationSpec to
Ascend C for `Ascend950PR`, CANN `9.1.0-beta.1`, compiler `ccec:dav-3510`, and ACL runtime.
Preserve the public C symbol `alloyport_reduce_sum_f32`, its status codes, zero-element behavior,
the supported bound `0 <= elements <= 1048576`, and the declared unsupported fallback. Produce a
complete bundle under `generated/`: Ascend C device code, host wrapper, build integration, and a
component-mapping document. Do not use a framework reduction operator as a hidden fallback.

Before a real run, replace this example with the approved context projection containing the exact
immutable CUDA source files and migration contract. Record that projection's digest in the
Candidate Episode configuration.
