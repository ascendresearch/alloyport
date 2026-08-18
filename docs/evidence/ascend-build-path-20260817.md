# The build image was never the defect. We told the model to compile the wrong way.

- Date: 2026-08-17
- Corrects: [`rebuild-loop-closed-20260817.md`](rebuild-loop-closed-20260817.md), which called the
  Ascend build image the blocking defect. It is not. The image compiles Ascend C.
- Image at the time: `alloyport-ascend-build-v1:local`, config digest `sha256:521fea11…`. It was
  re-fingerprinted later the same day for a different reason:
  [`ascend-image-fingerprint-20260817.md`](ascend-image-fingerprint-20260817.md)

## The control, which should have been run before calling anything impossible

`CLAUDE.md` asks: *if I am calling something impossible, did I run the control, and did I read the
vendor's supported path?* I had done neither. Here is the control — the repository's own
person-written kernel, `fixtures/ascend-add-v1`, compiled inside the exact pinned build image:

```
$ cmake -S project -B build
-- CMAKE_ASC_COMPILER: /usr/local/Ascend/ascend-toolkit/latest/bin/bisheng
-- ASCEND_CANN_PACKAGE_LINUX_PATH: /usr/local/Ascend/cann-9.1.0-beta.1/x86_64-linux
-- CMAKE_ASC_LLD_LINKER: .../ccec_compiler/bin/ld.lld
-- Configuring done / Generating done

$ cmake --build build
[ 50%] Building ASC object CMakeFiles/ascend_add_fixture.dir/host.asc.o
[100%] Linking ASC executable ascend_add_fixture
[100%] Built target ascend_add_fixture
-rwxr-xr-x 1 root root 365816 build/ascend_add_fixture
```

The image is fine. CANN ships a CMake package that registers `ASC` as a compiled language, and it
owns the compiler, the linker, and the include set:

```cmake
find_package(ASC REQUIRED)
project(alloyport_ascend_add_fixture LANGUAGES ASC CXX)
add_executable(ascend_add_fixture host.asc)
target_compile_options(ascend_add_fixture PRIVATE $<$<COMPILE_LANGUAGE:ASC>:--npu-arch=dav-3510>)
```

## What was actually wrong: the prompt prescribed a method that cannot work

The deployed task prompt said:

> - Ascend C kernel headers: `$ASCEND_HOME_PATH/x86_64-linux/tikcpp/tikcfw` (contains `kernel_operator.h`)
> - Ascend C compiler: `$ASCEND_HOME_PATH/x86_64-linux/ccec_compiler/bin/ccec`
> …
> Your build integration is responsible for pointing at these locations; nothing is added to the
> include path for you.

Every one of those facts is true, and the section says they "were probed from that exact image".
They were. **Nobody had ever compiled with them.** `kernel_operator.h` includes `kernel_tpipe.h`,
which lives under `asc/include/`, so a raw compiler invocation with `-I .../tikcpp/tikcfw` cannot
succeed and no correction to that line can make it succeed.

So the harness handed the model a **method**, the method was impossible, and the model obeyed it for
three of its last turns before the run ended. This is `CLAUDE.md`'s rule arriving in a new place:

> *A gate made of the answer's tokens is a blindfold with a verdict attached. Give the model the
> question and the stakes, never the method.*

It is also the same defect as the frozen tolerance: a set of values that were probed individually,
combined into a claim nobody walked. A probe that lists ingredients is not a recipe that was cooked.

## The corpus had the answer, in eight files, all unreachable

```
$ grep -rl "find_package(ASC\|LANGUAGES ASC" vendor/cannbot-skills/
ops/ascendc-direct-invoke-template/references/add_custom/CMakeLists.txt          unreachable
ops/ascendc-direct-invoke-template/references/matmul_fusion_kernel/CMakeLists.txt unreachable
ops/ascendc-registry-invoke-template/references/add_example/CMakeLists.txt        unreachable
ops/ascendc-registry-invoke-template/references/matmul_blaze_example/CMakeLists.txt unreachable
ops/ascendc-blaze-best-practice/references/matmul_custom/CMakeLists.txt           unreachable
ops/ascendc-direct-invoke-to-registry-invoke/references/examples.md               unreachable
plugins-official/ops-direct-invoke-flash/.../SKILL.md                             unreachable
plugins-official/ops-direct-invoke-flash/.../implementation-patterns.md           unreachable
```

Eight files document the supported build. `read_reference` serves 127 of the corpus's 1099 files and
**not one of these eight is among them.** This is open gap #1, and it is no longer theoretical: on
2026-08-16 the model asked for `ops/ascendc-direct-invoke-template/references/add_custom/` by name
and was refused, saying it "can't directly read from this index". That is the exact directory whose
`CMakeLists.txt` contains the pattern it spent the next two runs failing to invent.

The cost of that ledger decision is now measured: **one full paid migration**, plus the three runs
before it that never reached a compiler.

## A second trap, found by walking the gate before the model did

Telling the model to use the supported composition would have walked it straight into the Source
Gate. `MissingBuildReference` required every generated device and host source to appear in the build
text. The supported composition lists one translation unit and `#include`s the kernel into it — so
**`fixtures/ascend-add-v1`, the person-written kernel that compiles, would have been refused by this
repository's own Source Gate.**

Nobody had walked that gate against a supported build. It has been repaired to ask what it means:

> A source counts as reached when the build files name it, or when a source they do name includes it.

Still text, still blocking, and still refusing a source nothing reaches — the compiler would never
see it and the harness would link a candidate that does not contain it. A mention in a comment is
not a build edge; only an `#include` line is. Three tests, each verified to fail against a mutated
implementation: reverting to the old name-in-build-text rule, accepting any mention as an edge, and
reaching everything unconditionally.

## The image fingerprint, deliberately not touched

The evidence chain pins the build image by digest: the assignment contract carries it, and
`AscendBuildReceipt` carries the assignment. A rebuilt image is a different environment, and receipts
either side of it are not comparable without saying so.

**No rebuild was needed**, which is the best available outcome for that chain: `sha256:521fea11…`
still names the environment every build in this repository's history ran in, so the four builds of
`task-002ee08d6d5540c05e5f7361` and whatever runs next can be compared directly.

Had a rebuild been required, it would have meant re-pinning `build.image.digest` in the deployment's
candidate configuration, and recording the digest boundary here so no reader compares a receipt
across it. It would also have had to stay a **base** image: the fix could not be anything that only
`ascend-add-v1` needs, because one image serves every migration task.

One real but cosmetic defect was left alone at the time: the image's `PATH` includes
`${ASCEND_TOOLKIT_HOME}/compiler/ccec_compiler/bin`, which does not exist in this CANN layout. It is
harmless because `${ASCEND_TOOLKIT_HOME}/bin` already carries `ccec`, `bisheng`, and
`bishengir-compile`. Fixing it alone would have changed the digest for no behavioural gain.

**That is what happened next**, for a reason that does earn a fingerprint: the image now states the
toolchain contract this file is about, so nobody has to probe it and draw the wrong conclusion again.
The digest boundary, what it costs, and the three places the pin lives are in
[`ascend-image-fingerprint-20260817.md`](ascend-image-fingerprint-20260817.md).

## What changed

- The build-environment section of the task prompt, deployed and
  [tracked example](../candidate-user-prompt.example.md), now states what the toolchain **provides**
  — a CMake language package that owns the include set — instead of prescribing a compiler command
  line. It says the facts were observed by compiling a known-good kernel, not probed for existence,
  and it carries the dead end as a dead end, because `CLAUDE.md` law 5 asks for the failure too.
- `MissingBuildReference` asks about reachability rather than about literal appearance.
- Nothing in the image, and nothing in its digest.

## What this does not establish

- **Nothing about whether the model can now produce a compiling kernel.** The next run answers that;
  the only claim here is that the environment admits one and that the two obstacles it could not see
  past are gone.
- **Nothing about the arch flag.** `--npu-arch=dav-3510` is what the fixture uses and what the prompt
  already stated; whether it is right for every future target SoC is not established by one fixture.

## The verification runs, and what they did and did not settle

**`task-d59407d835a580f4c5cf5aee` never reached a candidate.** It failed at turn 5, before any
provider call, with `model continuation input digest does not bind its results`. Unrelated to the
build path: a turn that calls no tools leaves the durable provider context with no pending results,
and `review_stop` re-asked the model without rebinding `next_input_digest`, so the loop presented a
digest binding the previous turn's results while the store recomputed it from an empty set. Fixed,
with a test that fails against the reverted implementation.

That defect is very likely what ended `task-ccd149dfc0f421d97ed7feb4` on 2026-08-16 with 21
consecutive `confirmed_not_sent` attempts. **That run's diagnostic was hashed and thrown away as it
was recorded**, so it cost a paid run and a day of not knowing. This one, with bytes behind the
digest, cost twenty minutes and one query. That is the entire value of the fix that publishes
diagnostics before recording them, measured.

**`task-498e257f6379bf01c4a47406` settles the source question.** Its Source-Gate-passing candidate
builds this way:

```cmake
project(alloyport_reduction_candidate LANGUAGES ASC CXX)
find_package(ASC REQUIRED)
add_library(alloyport_reduction_candidate STATIC reduce_sum_kernel.asc reduce_sum_launch.asc)
target_compile_options(alloyport_reduction_candidate PRIVATE
    $<$<COMPILE_LANGUAGE:ASC>:--npu-arch=dav-3510>)
```

`.asc` sources, the ASC language, the arch flag on ASC sources only. No hand-rolled compiler command
line and no guessed include directory. **Its first candidate also passed the Source Gate on the first
attempt**, which no previous run has done.

The candidate still has visible defects — `$ASCEND_HOME_PATH` where CMake needs
`$ENV{ASCEND_HOME_PATH}`, `lib/` where the toolkit has `lib64/`, and `find_package(ASC)` after
`project(... LANGUAGES ASC)` rather than before it, which is the order the working fixture uses.
Those are exactly the errors a compiler names and a correction turn can fix. That is the difference
this change was for: the model is now wrong in ways the loop can resolve, instead of obedient to an
instruction that could not be satisfied.

**The build itself is unverified**, and not for a reason in this repository. Every Ready NPU on the
shared Ascend host carried another user's process, and the one process-free card was in `Alarm`
health, so the device guard correctly refused to run:

```
device probe unavailable: no allowed accelerator is Ready, process-free, and unleased
```

The build request stayed pending. Whether this candidate compiles is the next thing a run answers,
and it needs a free card rather than a change.
