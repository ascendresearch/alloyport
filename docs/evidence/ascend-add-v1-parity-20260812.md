# Ascend `ascend-add-v1` transport parity — 2026-08-12

This comparison closes the fixed-fixture legacy-transport parity task. It does not turn the legacy
run into an authoritative AlloyPort receipt and does not assert CUDA-to-Ascend correctness.

The independent legacy observation is
[`ascend-add-v1-legacy-parity-20260812.json`](ascend-add-v1-legacy-parity-20260812.json), with file
digest `sha256:01fcf3617f72d1975264fb1bff820abd15580cd48533eb6b384ead9602c9d095`.
It used the separate Python harness's SSH/SCP transport only for this parity attempt. The AlloyPort
repository runtime and configuration did not gain that transport.

## Comparison

| Field | Accepted outbound attempt | Legacy parity attempt | Result |
|---|---|---|---|
| Fixture | `ascend-add-v1` | `ascend-add-v1` | match |
| Bundle digest | canonical bundle from the unchanged fixed source | `sha256:980e769265d108dddfc89ce845abf68227ec4fd4c969175cdc942c5ba771ee29` | match by canonical reconstruction |
| Source digest | fixed source committed before the outbound gate | `sha256:cce03fcc47da6760f1cbcd478d18288894aa9d15c4b92283b76f8e1778fc5b2e` | match by unchanged source bytes |
| Image ID | `sha256:fc755f6d67a5484ecf6f1e4416c2d97da330122b4fd6842c95c6642ed1f9472c` | same | match |
| Architecture | `Ascend950PR` | `Ascend950PR` | match |
| CANN | `9.1.0-beta.1` | `9.1.0-beta.1` | match |
| Driver | `25.7.rc1.6` | `25.7.rc1.6` | match |
| Firmware | `9.0.0.105.229` | `9.0.0.105.229` | match |
| Device | NPU 3, ready and process-free | NPU 3, serial `10265D495203`, `OK` and zero processes before and after | match at recorded identity/state boundary |
| Stdout | `PASS fixture=ascend-add-v1 elements=16384 checksum=3d2cf971e11e0383` | exact same 68 bytes; digest `sha256:5216cb897528030f49fdf9d3b271077b0f0632206e70852f2460757bf376e7e7` | exact match |
| Stderr | terminal Artifact published by the outbound gate | 817-byte deterministic CMake/ASC build log; digest `sha256:24ef96b77f369c8c3b68dc39e193e9e86fd0cbacdba8f196136ac95727658f75` | both classified as non-terminal build diagnostics; old Artifact bytes were not copied into the handoff |
| Exit/outcome | exit 0 / succeeded | exit 0 / succeeded | match |

The legacy attempt ran for 27,509 ms with networking disabled, a read-only root/source/driver,
bounded tmpfs, the enumerated host device nodes, `cap-drop=ALL`, `DAC_OVERRIDE`, and
`no-new-privileges`. Its stdout is the fixture's own deterministic element-by-element verification
marker. The stderr archival limitation above is explicit: the surviving outbound handoff records
Artifact publication but not those Artifact bytes, so this report does not invent a byte-for-byte
stderr comparison.

## Cutover decision

The fixed transport-parity fields required by Designs 0018 and 0021 now agree: code closure,
environment identity, selected device state, deterministic result, and exit classification. The
legacy path remains a non-authoritative reference only. AlloyPort's outbound worker is the sole
product execution transport, and architecture CI rejects SSH, SCP, rsync, host/key fields, remote
roots, and remote shells if they reappear in runtime crates or process configuration examples.
