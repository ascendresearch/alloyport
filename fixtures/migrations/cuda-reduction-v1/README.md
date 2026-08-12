# cuda-reduction-v1

This is AlloyPort's first product acceptance intake. It is intentionally an original CUDA extension,
not a prepared Ascend fixture.

The source bundle contains:

- a block-level CUDA reduction with explicit `__syncthreads()`;
- host allocation, copies, launch configuration, synchronization, and error propagation;
- one stable C API, `alloyport_reduce_sum_f32`;
- a CMake build;
- deterministic boundary and randomized reference cases.

The directory deliberately contains no Ascend C source, target host launcher, or target build
skeleton. Those are migration outputs. Adding them to the intake would turn the acceptance test back
into a prepared execution fixture.

## Expected release inventory

A successful migration must add, in a separate generated-source artifact:

- Ascend C device implementation;
- Ascend host launch/runtime implementation preserving `alloyport_reduce_sum_f32`;
- target CMake integration and clean build command;
- CUDA-to-Ascend component mapping;
- supported-domain guard and the declared unsupported-domain status;
- correctness, performance, and integration evidence references.

The checked-in [`migration-spec-v1.json`](migration-spec-v1.json) freezes the initial contract. Its
`source_revision` is a fixture identity until intake gains a canonical source-bundle digest; a task
and every candidate still bind to the content digest of the full serialized spec.

