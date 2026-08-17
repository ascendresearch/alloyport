# The rebuild loop closed, and the blocker moved into the build image

- Date: 2026-08-17
- Task: `task-002ee08d6d5540c05e5f7361`, specimen `cuda-reduction-v1`
- Deployment: server, x86-64 Ascend worker, and GB10-native aarch64 worker all rebuilt at `9e4a9c4`
- Terminal state: **budget exhausted** at 32 of 32 model turns, 37 of 80 tool operations
- Record: `alloyport-server candidate-record task-002ee08d6d5540c05e5f7361 --into <dir>`

`NEXT_SESSION.md` said the thing to watch for was a rebuild of a corrected candidate, because that
had never completed. **It completed three times.**

```
* c007 submitted; no gate ran
* c006 source_gate passed | ascend_build exit 1: kernel_operator.h:22:10: fatal error: 'kernel_tpipe.h' file not found
* c005 source_gate passed | ascend_build exit 1: kernel_operator.h:22:10: fatal error: 'kernel_tpipe.h' file not found
* c004 source_gate passed | ascend_build exit 1: kernel_operator.h:22:10: fatal error: 'kernel_tpipe.h' file not found
* c003 source_gate failed: missing_build_reference, incomplete_component_mapping
* c002 source_gate passed | ascend_build exit 1: CalledProcessError: ['cmake', '-S', ..., '-B', ...] returned 1
* c001 source_gate failed: missing_build_reference
```

Seven candidates, six Source Gate runs, **four builds**, four reads of the compiler's output. Every
build after the first was a candidate the model had corrected *after reading the previous build's
diagnostics*. Read → correct → rebuild is closed.

## The failure moved twice, and the second move is out of the model's reach

**Build 1 (`c002`) failed in CMake configuration**, not at a missing header. Every previous run died
before that: `acl/acl.h` on 2026-08-16 and `kernel_operator.h` on 2026-08-17 were both include-path
failures in the *first* translation unit. Pointing the build at the CANN include paths — which the
deployment prompt now states — worked.

**Builds 2, 3 and 4 all failed identically**, inside the vendor's own header:

```
/usr/local/Ascend/cann-9.1.0-beta.1/x86_64-linux/tikcpp/tikcfw/kernel_operator.h:22:10:
fatal error: 'kernel_tpipe.h' file not found
```

Checked inside the image rather than inferred:

```
$ find /usr/local/Ascend -name kernel_tpipe.h
/usr/local/Ascend/cann-9.1.0-beta.1/x86_64-linux/asc/include/interface/kernel_tpipe.h
/usr/local/Ascend/cann-9.1.0-beta.1/x86_64-linux/asc/include/basic_api/kernel_tpipe.h

$ sed -n 21,22p .../tikcpp/tikcfw/kernel_operator.h
#include "kernel_tpipe.h"
```

`kernel_operator.h` includes a header that does not live beside it, or anywhere under `tikcpp/`. It
lives in a different top-level tree, `asc/include/`. Compiling Ascend C by invoking `ccec` with
`-I .../tikcpp/tikcfw` therefore cannot work, and no correction the model makes to that include
line can make it work.

## What the model did about it, which is the part worth keeping

Its three attempts against this error are a clean escalation, readable as three commits:

1. **`c004` → `c005`**: added two more guesses.
   ```diff
        -I ${TIKCPP_INCLUDE}
   +    -I ${TIKCPP_INCLUDE}/impl
   +    -I ${TIKCPP_INCLUDE}/intrinsic
   ```
2. **`c005` → `c006`**: stopped guessing and wrote discovery instead.
   ```diff
   +# Recursively locate the Ascend C impl headers (kernel_tpipe.h etc.) and build
   +# the include path from their directories, so we never hard-code a guessed
   +# sub-layout.
   +file(GLOB_RECURSE KERNEL_TPIPE_HEADERS ${TIKCPP_ROOT}/**/kernel_tpipe.h)
   ```
3. It still failed, because the glob searched `${TIKCPP_ROOT}` and the header is outside that tree
   entirely.

The reasoning was right and the move was right — replace a guess with a measurement — and it failed
because **the model was searching the only subtree anything had told it about.** It cannot list the
image's filesystem. `read_reference` serves the vendored corpus, not the toolchain. So the harness
handed it a compiler whose requirements are unstatable from where it stands, and then charged it
three cycles and the rest of its budget for not guessing a path it had no way to see.

That is the same defect class as everything in
[`fatal-harness-defects-20260816.md`](fatal-harness-defects-20260816.md), relocated: *the harness
ended a paid run over a condition the model could not see or satisfy.* It has moved out of the
control plane and into the build image.

## What to do about it, in order of how much it assumes

1. **Invoke the vendor's supported driver.** CANN ships `ascendc`/`bisheng` wrappers that set their
   own include paths. The trusted build runner constructing a raw `ccec` command line with one `-I`
   is the repository's own choice, and it is the choice that fails. This is the fix that assumes
   least, because it stops the harness from having an opinion about CANN's internal layout.
2. **Give the runner the full include set** if a raw invocation is kept, so `kernel_operator.h`
   resolves. That is a fact about the image and belongs in the image's contract, not in a candidate's
   `CMakeLists.txt`.
3. **Let the model see the toolchain layout it is compiling against** — a listing, a documented
   include contract in the prompt, anything. Its `GLOB_RECURSE` move shows it will use such a thing
   correctly if it exists.

Nothing here needs a model change. **No candidate has been shown to be wrong yet**; four builds have
failed and not one has reached a diagnostic about the model's own Ascend C.

## What this run cost, and where the cost went

| | |
|---|---|
| model turns | 32 of 32 (budget exhausted here) |
| tool operations | 37 of 80 |
| input tokens | 1 557 696 across 32 attempts |
| output tokens | 21 900 |
| largest single input | 72 916 against a 128 000 ceiling |
| wall clock | roughly 12 minutes |

Operations: 14 `read_reference`, 9 `submit_candidate_bundle`, 6 `request_source_gate`,
4 `request_ascend_build`, 4 `read_build_diagnostics`. Four submissions were
`rejected_as_invalid` and each was followed by a successful one, which is Design 0040's recovery
running four times in one paid run.

**Corpus reading has halved.** Nine reads before the first candidate, against 18 and 20 in the two
runs of 2026-08-16, and 14 in total against 24. The largest single input is 73 k rather than 99 k.
The context ceiling was not the binding constraint this time; **model turns were**, and they were
spent three-for-one on an error that could not be fixed.

## A second defect, in the client rather than the run

`alloyport-cli attach` printed two lines and stopped:

```
run event sequence is invalid: run.started must be the first event
```

The migration was unaffected — this is the CLI's own reducer refusing the stream it was served — but
it means the operator-facing view of a live run is broken, and every observation in this document
came from reading SQLite and the CAS instead. Unfixed, and now the most annoying gap: watching a run
is how you decide whether to stop paying for it.

## What this run did not establish

- **Nothing about whether the generated Ascend C is correct.** No build has succeeded, so the
  Correctness Gate still has never judged a generated kernel.
- **Nothing about the candidate's kernel at all.** All four builds failed before the compiler formed
  an opinion about the model's source.
- **Nothing was adjusted by hand to make this run get further.** The only changes since the previous
  runs are the ones in `git log 6302541..9e4a9c4`, none of which touch the prompt, the tools, or the
  build image.
