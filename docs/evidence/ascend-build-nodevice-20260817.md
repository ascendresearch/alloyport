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

## Capacity was split by role, and that half is done

A worker now advertises two numbers: `available_slots`, clamped by cards that are Ready,
process-free and unleased, and `device_free_slots`, clamped only by concurrency. The readiness
preflight asks for the one the role consumes, and a role's device need comes from its own configured
limits rather than a feature name matched in the scheduler. The preflight also now waits only for
**builder** roles and defers verifiers until the Correctness Gate, because a run that has never
compiled anything should not be stopped by a gate it may never reach.

That last part was not hypothetical. On the evening of 2026-08-17 the CUDA host's driver stopped
answering `nvidia-smi` entirely and every Ready Ascend card carried another user's process — both
verifier problems, both of which prevented any compilation at all.

## What is still not fixed, and the mistake I made reaching for it

**The worker's execution path still leases and health-checks a card for every attempt, including a
build.** `prepare_attempt` calls `DeviceGuard::acquire_and_preflight`, which requires `health ==
Ready && process_count == 0`.

I tried to shortcut it: for an `AscendBuild` attempt, skip selection and build the supervisor against
`inventory[0]`, on the reasoning that the identity is only what the supervisor is constructed from
because nothing is mounted. On this host `inventory[0]` is device 0, which is in `Alarm` health, so
every build then failed the guard with `device 0 is Unhealthy` — strictly worse than what it
replaced, which at least selected a Ready card. **Reverted.**

The reason the shortcut could not work is worth keeping, because it is the actual shape of the
remaining problem: the worker's own `AscendRunReceipt` attests `device`, `lease`, `pre_observation`
and `post_observation` for every attempt. A build that genuinely holds no device needs that receipt
to be able to say *no device*. That is a decision about what a worker receipt claims, not a branch in
the runtime, and I took the branch without following it that far.

So this change removes a build's *use* of a card — contract, container mount, visible-device
variable — and a build's *wait* for capacity. It does not yet remove the guard's lease, and until the
receipt can express a device-free run it should not.

## What this does not establish

- **Nothing about whether a build produces a correct artifact.** No build has succeeded yet.
- **Nothing about `--npu-arch` correctness.** The compiler targets an architecture named by a flag,
  and that flag is not validated against the card the result will eventually run on. With the card
  gone from the build, nothing at build time could validate it even in principle.
