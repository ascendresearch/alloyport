# A build was leasing an accelerator it never opened

- Date: 2026-08-17
- Decision: [0038 amendment](../design/0038-standalone-ascend-build-worker.md)

## What the build actually runs

`fixtures/ascend-build-v1/run_build.py`, in full, is:

```python
subprocess.run(["cmake", "-S", SOURCE, "-B", BUILD], check=True)
subprocess.run(["cmake", "--build", BUILD, "--parallel", "1"], check=True)
```

It contains no reference to a device, an NPU, `davinci`, `ASCEND_RT_VISIBLE_DEVICES`, `acl`, or a
runtime. Read, not inferred.

## What the accelerator was doing there

Two things, and neither is the build:

1. **`validate_device` cross-checks configuration against configuration** — the configured device's
   `product_name` and `firmware_version` against the configured environment facts — at policy
   construction. No card is read. **Kept**, because it is what binds the receipt's environment claim
   to anything at all.
2. **A per-attempt lease and container mount.** The worker leased a Ready, process-free, unleased
   card, mounted every device node `rwm`, and set `ASCEND_RT_VISIBLE_DEVICES`. The container then
   ignored all of it.

`AscendBuildReceipt` has no device field. So the lease attested nothing that survives into evidence;
it established only that a card happened to be free.

## What it cost

On the shared Ascend host every Ready card carried another user's process all of 2026-08-17. The
queued build of `task-498e257f6379bf01c4a47406` never ran, and the worker logged, repeatedly:

```
device probe unavailable: no allowed accelerator is Ready, process-free, and unleased
```

The control settles it from the other side: the same image compiles and links
`fixtures/ascend-add-v1` to a 365 816-byte executable with **no accelerator attached**.

## What changed

- The build contract asks for `device_count == 0`, controller-side and worker-side.
- The build container mounts no device node and sets no visible-device variable.
- The runtime takes no per-attempt device lease for an `AscendBuild` attempt; it constructs the
  supervisor against the same identity `AscendRuntime::new` already uses before any attempt exists.
- Correctness is untouched: it executes, so it still leases a real card.

Every fixture that stated the old contract had to move with it — seven tests failed until they did,
which is the contract being stated in more than one place and all of them agreeing. Both directions
are mutation-checked: restoring the mounts fails the new test, and requiring a device again fails it.

## What this does not fix

**Builds still queue on this host**, because a worker advertises one capacity number computed from
free cards. The combined worker serving both roles cannot express "no card needed for half my work",
and neither can the standalone build worker as implemented, which binds a device and attaches a probe.

The amendment lists the three ways out and picks none. Until one is chosen, this change removes the
*use* of a card without removing the *wait* for one.

## What this does not establish

- **Nothing about whether a build produces a correct artifact.** No build has succeeded yet.
- **Nothing about `--npu-arch` correctness.** The compiler targets an architecture named by a flag,
  and that flag is not validated against the card the result will eventually run on. With the card
  gone from the build, nothing at build time could validate it even in principle.
