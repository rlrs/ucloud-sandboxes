# Pre-reap capture barrier: native qualification completed

Subsequent isolated UCloud qualification on worker `12399833` supplied the
missing native kernel and reproduced two real runtime constraints: abort needed
to preserve netstack configuration and resume timers, and restore needed to
remove the captured runtime filestore only from its writable clone. Patch
`0004-ucloud-abort-hibernation.patch` fixes the former. The product Warden now
uses revision-fenced prepare/abort/commit operations and a v3 manifest binding
workspace capture and separately quota-owned memory.

Both the native barrier fixture and the full product lifecycle fixture passed;
see `../benchmarks/split-memory-2026-09-23/native-capture-barrier.json` and
`../benchmarks/split-memory-2026-09-23/product-lifecycle.json`. These prove local
capture semantics, not fleet density or remote migration throughput.

The original prerequisite inspection below is retained as context. Read-only
inspection of the available `rasmus-dev` Linux driver on 2026-09-23
found kernel `5.15.0-190-generic`, no `/dev/ublk-control`, no ublk module under
that kernel's drivers/block directory, and no `runsc` in PATH. The retained
native daemon and workload executables do not supply the missing kernel driver.
No modules were loaded, no runtime was started, and production was not used.
This is a blocked native qualification, not a passing storage/runtime test.

## Existing capabilities and missing contract

`StorageNativeNodeService.freeze_and_seal()` already syncs and freezes the
mounted filesystem, calls the native restack snapshot operation, and unfreezes
it before returning. The daemon's snapshot/restack leaves the device usable.
However, the Python journal moves `MOUNTED -> SEALING -> SEALED`; convergence
then releases the device. There is no supported prepare-snapshot/abort-to-running
transition for the runtime owner. It would be unsafe to resume a guest while the
storage journal still advertises `SEALED` as its durable current state.

Warden's current order is checkpoint, publish COMPLETE, reap sentry, runsc delete,
rootfs detach, seal/release storage. Runsc delete removes its self-backed
filestore before the workspace snapshot. Taking the snapshot earlier retains a
different filesystem, and removing the file from the old live mount afterward
cannot remove it from the immutable snapshot. Its restore semantics need actual
qualification; do not infer them from a successful generic block snapshot.

## Extend the existing native qualification

Use `runtime/storage_native/qualify_volume.py`, not a second independent runtime
bootstrap. It already owns a disposable daemon, devices, mounts, namespaces,
workloads, phases, and cleanup. It currently deletes the sentry before sealing
and keeps the gVisor rootfs outside the snapshotted volume. Both facts must change
for this particular qualification profile:

1. Put the rootfs upper/workspace on the tested native volume and retain
   the pinned runsc application-memory capture and process-state fixture.
2. Capture and assert the exact original sentry remains paused and alive. Freeze,
   snapshot/restack, and unfreeze before reaping; record the captured generation.
3. Exercise abort: resume that same sentry, verify memory and filesystem state,
   and prove the prepared immutable snapshot cannot observe subsequent writes.
4. Exercise commit separately: reap only after retained component validation,
   reconstruct a fresh COW from the pre-reap snapshot, and start paused restore.
   Verify filestore behavior, writable filesystem state, managed process state,
   memory identity, and absence of execution before handoff.
5. Inject errors during freeze, snapshot, thaw, manifest commit and reap. The
   qualification must leave one known execution owner or an explicit fenced
   failure; successful cleanup is part of the result.

Then implement and test the storage journal's explicit capture preparation and
abort semantics before using it in Warden. A filesystem-only snapshot test, an
ordinary-directory gVisor test, or command-runner unit doubles cannot authorize
manifest v3 writers or the memory/workspace split. No new production capture
contract, physical layout, or manifest v3 writer is enabled by this change.
