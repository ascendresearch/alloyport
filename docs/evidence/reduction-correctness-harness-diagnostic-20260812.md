# Reduction correctness harness diagnostic — 2026-08-12

This record validates the trusted runner images and the CUDA authority half of the frozen reduction
corpus. It is not a paired Correctness verdict: the experiment and upstream Gate identities are
explicitly diagnostic, and no model-authored Ascend candidate existed for this run.

## Immutable inputs

- CUDA base manifest: `docker.io/nvidia/cuda@sha256:7d2f6a8c2071d911524f95061a0db363e24d27aa51ec831fcccf9e76eb72bc92`
- CUDA Dockerfile digest: `sha256:1e88da47115440c4d0b8b5f1a7d23adbf51c50b0f597b2777d2bc89a069c9445`
- built CUDA image ID: `sha256:4c4b17de7027a387634d4bd1947262ffeb859c46c6b8910b9155e381a37ee01d`
- Ascend base manifest: `swr.cn-south-1.myhuaweicloud.com/ascendhub/cann@sha256:a7770abf2195bd61c87cb094d778b912382d02548b6c10afdc60dd565faa9f7d`
- Ascend Dockerfile digest: `sha256:aca90e47658159999b18d23882ab518ec6b658db2a8a4a6ad1f070f2eb01bad9`
- built Ascend image ID: `sha256:521fea113593a98346b534498f5714a5704a24293150d08dccf229136a0efdde`
- execution bundle digest: `sha256:065a9bf610bd0c26583c103e278aa690bffb0cc9e539df65412515ef50eb870d`
- CUDA implementation digest: `sha256:b495ea483e83b074eb71a559a85a6d5c1644c271b144625c5ca430d6a24579ed`
- trusted runner file digest: `sha256:68c8cc6c50bcf913386bdf25daa42123a36c3f78b0314f9de420e6660fb58c3e`

The CUDA device was an idle NVIDIA GB10, device 0, driver `580.159.03`, compute capability `12.1`.
The image exposed Python 3.12.3, CMake 3.28.3, and CUDA compiler 13.0.88. The Ascend image exposed
Python 3.12.13, CMake 3.27.9, GCC 12.3.1, and the pinned CANN 9.1 environment. Its toolchain image was
built, but no DUT execution was attempted without a genuine generated candidate.

## Execution boundary

The CUDA runner used device 0 with network disabled, read-only root and bundle mount, all
capabilities dropped, `no-new-privileges`, bounded executable tmpfs work/temporary filesystems, and
the fixed Python entrypoint. The first image attempt failed before execution because
`python3-minimal` lacked the JSON standard library. The second failed before CMake because the base
image lacked CMake. Both failures produced no run receipt. The final image explicitly installs
Python, CMake, G++, and Make and completed without relaxing the container boundary.

## Result

- [CUDA run receipt](cuda-reduction-correctness-diagnostic-20260812.json): schema-valid
  `cuda_reference`, 24/24 frozen observations, implementation invoked, synchronized, reference-run
  digest `sha256:9abcab2e2a8555b2635f339453e11bab44e83deb39fa26f7003a71f6dd6a05e2`.
- [Calibration receipt](cuda-reduction-calibration-diagnostic-20260812.json): identity comparison
  passed and all ten required mutants were detected; calibration digest
  `sha256:de2e166a05ac8146c910ab89579fa02244657def98ba8eab14297c446c9c59df`.

This establishes that the checked-in CUDA input, frozen corpus, trusted runner, pinned image, real
device, structured receipt schema, and mutation calibration interoperate. It does not establish an
Ascend result, cross-backend agreement, a real Build-Gate-authorized experiment, or Correctness
PASS.
