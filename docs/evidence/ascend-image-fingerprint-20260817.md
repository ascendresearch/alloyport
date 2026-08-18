# Re-fingerprinting the Ascend image, and what that costs

- Date: 2026-08-17
- Previous config digest: `sha256:521fea113593a98346b534498f5714a5704a24293150d08dccf229136a0efdde`
- New config digest: `sha256:17b6708374ddbde5e36931927aefb2cbcd5596409f3be34244cf43e6de14fb60`
- Base image, unchanged:
  `swr.cn-south-1.myhuaweicloud.com/ascendhub/cann:9.1.0-beta.1-950-openeuler24.03-py3.12-devel`
- Definition: [`fixtures/reduction-correctness-v1/ascend-image/`](../../fixtures/reduction-correctness-v1/ascend-image/)

## Why a rebuild at all, when the image was not the defect

[`ascend-build-path-20260817.md`](ascend-build-path-20260817.md) establishes that the image compiles
Ascend C and that the defect was a prompt prescribing an impossible compiler invocation. That file
also says the image's one real flaw — a `PATH` entry we add that does not exist in this CANN layout —
was recorded rather than fixed, because *"fixing it would spend a fingerprint for no behavioural
gain."*

That reasoning still holds for the `PATH` entry alone. What earns the fingerprint is the other half:
**the image now states the toolchain contract whose absence cost a migration.** A reader — an
operator, a prompt author, a future agent — can now ask the image what it compiles instead of probing
it by hand and drawing the wrong conclusion, which is exactly what happened.

```
$ docker inspect --format '{{json .Config.Labels}}' <image>
org.alloyport.toolchain.cann_version         9.1.0-beta.1
org.alloyport.toolchain.asc_cmake_package    find_package(ASC REQUIRED) before project(<name> LANGUAGES ASC CXX)
org.alloyport.toolchain.asc_source_extension .asc
org.alloyport.toolchain.asc_arch_flag        $<$<COMPILE_LANGUAGE:ASC>:--npu-arch=dav-3510>
org.alloyport.toolchain.device_compiler      bisheng, selected by the ASC language package; a
                                             hand-written command line with a single -I cannot
                                             resolve kernel_operator.h's own includes
org.alloyport.toolchain.host_acl_headers     ${ASCEND_HOME_PATH}/x86_64-linux/include
org.alloyport.toolchain.verified_by          fixtures/ascend-add-v1 compiled and linked in this
                                             image with no accelerator attached, 2026-08-17
```

Labels, not a file, and that distinction was made by a gate rather than by me.

### The first attempt shipped the contract as a file, and a test refused it

The contract was originally a `COPY`-ed `toolchain-contract.json`.
`trusted_images_declare_the_complete_runner_toolchain` asserts `!ascend.contains("COPY ")`, and it
failed. The rule is right: a trusted runner image must not carry content a run could reach outside
the artifact chain. I had talked myself out of baking a kernel in for exactly that reason and then
added a file anyway; the gate did not care about my reasoning.

Labels are strictly better here. `docker inspect` answers without running the image, and nothing a
candidate builds can read them.

That test also used to require the literal `ccec_compiler`, satisfied by the very `PATH` entry that
does not exist. It asserted a guarantee nobody had checked and would have passed just as happily with
the compiler absent — the same shape as the tolerance nobody measured. It now requires the toolchain
labels, and whether the toolchain compiles is established by building `fixtures/ascend-add-v1` in the
image rather than by a substring. Both directions were checked by mutation: deleting a label fails
it, and re-adding a `COPY` fails it.

## It is still a base image

One image serves both Ascend roles — build and reduction correctness are the same id under two tags.
Nothing added here is specific to a migration, a specimen, or a kernel: a version, a build-system
name, a source extension, and where the host headers are. No kernel and no fixture was baked in, so
nothing in the image can be `#include`d by a candidate to make its own build pass.

Two things were deliberately **not** changed:

- **The driver paths in `LD_LIBRARY_PATH` stay.** They are empty in a build container and populated
  when the correctness role attaches a device. They looked like dead entries until the roles were
  checked; one image, two roles.
- **Two dead `PATH` entries remain**, `nnal/atb/.../bin` and
  `ascend-toolkit/latest/compiler/ccec_compiler/bin`. Both come from the vendor's base image, not
  from us. Ours was a *duplicate* of the second and is gone. Removing the vendor's would mean
  rewriting `PATH` wholesale, which is a larger claim about their image than this change wants to
  make.

## The control, run again against the new image

```
[ 50%] Building ASC object CMakeFiles/ascend_add_fixture.dir/host.asc.o
[100%] Linking ASC executable ascend_add_fixture
-rwxr-xr-x 1 root root 365816 build/ascend_add_fixture
```

Byte-identical in size to the same control against the old image, with no accelerator attached.

## What the boundary costs, stated so nobody compares across it

**Every Ascend build and correctness receipt recorded before 2026-08-17 names
`sha256:521fea11…`. Every one after names `sha256:17b67083…`. They are different environments and
their receipts are not directly comparable**, even though the control shows the toolchain behaves
the same. That is the price of this change and the reason it needed a reason.

The previous image is kept reachable as `alloyport-ascend-build-v1:pre-521fea11` and
`alloyport-ascend-correctness-v1:pre-521fea11`, so an old receipt can still be reproduced against the
environment it names.

## The pin lives in three places, and they move together

A rebuild is not a one-file change. Getting it half-done makes every build fail with
`build image identity is not locally allowed`, which is the worker refusing an image the controller
chose:

| where | field | host |
|---|---|---|
| `deployment/candidate.json` | `build.image.digest` + `size_bytes` | controller |
| `deployment/candidate.json` | `correctness.ascend.image.digest` + `size_bytes` | controller |
| `<install>/worker.json` | `image_id` | Ascend worker |

All three moved together, the worker was restarted, and it reconnected READY on the new identity.
The CUDA correctness image is untouched and still pinned at `sha256:4c4b17de…`.

## What this does not establish

- **No build has run in the new image through the Build Gate.** Every Ready NPU on the shared host
  carries another user's process, so the build worker reports zero capacity. The control was run by
  hand, outside the gate.
- **Nothing about whether a device is needed to compile.** The control compiles and links with no
  accelerator attached, while the build contract requires `device_count == 1` and the worker leases
  and mounts a card. That is worth its own decision and is not made here.
