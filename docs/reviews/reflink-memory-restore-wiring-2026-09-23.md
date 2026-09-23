# Reflink memory restore wiring

The feature remains disabled by default and has not been activated by this
wiring change. Native runtime, owner-mode recovery, failure injection, and
realistic pressure performance must pass their separate qualification before
activation.

Deployment setting: `sandbox.direct_reflink_memory_restore: true`. It requires
`direct_split_memory_backing`; it may be combined with
`direct_ram_memory_backing`. Both UCloud and Hetzner use the existing canonical
VM bootstrap options, which pass `--reflink-memory-restore` to the direct node
agent. Builder bootstrap never receives the worker-only setting. The existing
registry requirement for split checkpoints remains unchanged.

The runtime factory validates the complete pinned gVisor distribution before
constructing the storage/runtime owner. Reflink restore requires the qualified
capture-barrier and RAM sparse-capture patches plus
`20260817/0007-ucloud-reflink-application-memory.patch`, whose current exact
SHA256 is `17933cd7990ac28c0b9bdb8add8330f242fdd3dcf61621c2a7d3905c984f6619`.
Missing, different, or tampered native artifacts fail closed. Existing bundle
packaging/bootstrap already retains and authenticates `build-manifest.json` and
all executable companions; no separate runtime-distribution path is introduced.
A changed native patch must update the build pin and this attestation together.

The selected portable backend identity is `reflink-restore-v1`, independent of
whether fresh owners start on RAM or file backing. Per-owner RAM-to-file restore
placement is durable local ownership state, not a mutation of checkpoint
fingerprints. Consequently, RAM-fresh and file-fresh workers with the same
qualified runtime capability can share the same compatibility fingerprint.
Feature-disabled file and RAM configurations retain their prior identities.
The existing runtime-compatibility capability remains the migration filter;
fully configured workers also advertise `sandbox-memory-reflink-restore-v1`.

Warden owns selection of the native invocation: RAM-fresh operation uses the RAM
flag, whereas a file-backed restore uses the reflink flag. The two native flags
must not be passed together. This wiring does not enable reflink restore on old
runtimes or reinterpret old checkpoint fingerprints. Rollback must retain a
reader that understands persisted per-owner backing modes; an old RAM-only
reader is not safe after owners have switched to file backing.

Validation: **55 Linux tests passed in 4.043 seconds**, covering config type and
split dependency, old config defaults, CLI forwarding, both provider bootstrap
contracts, builder exclusion, shell syntax, exact-patch attestation/tampered
binaries, capability advertisement, and actual runtime assembly over the storage
Unix protocol. The assembly gate explicitly verifies equal reflink-capable
fingerprints across RAM/file fresh placement and different legacy fingerprints.
Privileged filesystem checks/native attestation are substituted in assembly;
these are wiring contracts, not claims of native restore correctness or speed.

## Temporary physical capacity

A restore clone can be charged separately from its immutable source by XFS even
when both initially share physical extents. `DirectSandboxRegistry` schema 6
therefore retains a source-overlap claim identified by sandbox incarnation,
hibernation generation, and manifest digest. The exact authenticated source
allocation is admitted atomically against the same physical budget as create
and import. Duplicate reservations are idempotent only for an identical source
and byte count; a failed/ambiguous caller retains its claim across restart.
The Warden receives this existing registry as its capacity dependency.

Heartbeat storage reservations add these bytes once to the base workspace and
memory reservations, rounding the aggregate upward to MiB. Cleanup must retire
the physical source/candidate and reconcile its project quota before releasing
the exact claim. Deletion cannot forget an unreconciled claim; the per-owner
claim list allows recovery to finish cleanup even after artifact metadata has
already been retired. The allocator/Warden cleanup and separate immutable
retention-project implementation have their own qualification gate.

The registry upgrades schemas 3–5 transactionally to 6; older registry readers
reject schema 6. The memory backing journal separately fences old readers with
its owner-mode schema version. Rollback requires draining or retaining a runtime
that understands both schemas, even while reflink activation remains opt-in.

The retention implementation keeps the live application's project limit
unchanged: the immutable source receives a separate, journaled XFS project,
backed by the registry overlap claim. Reader leases also prevent releasing
physical capacity while an unlinked source remains open. Disabling reflink
restore is rejected while either retained project rows or registry-only overlap
claims still need recovery, including the crash window after allocation deletion.

Recovery qualification adds nine tests using the real allocator, registry, and
Warden with injected kernel/runtime boundaries. They cover capacity denial
before quota assignment; failures before and after assignment; candidate retry
with the same source inode; cleanup after RUNNING and artifact removal; ordered
deletion; open-reader retention; replaced-inode rejection; and both flag-off
crash windows. The latest focused Linux gate passed **31 tests in 5.283 seconds**;
the preceding combined gate passed **79 tests in 9.491 seconds**. These tests
establish journal/ownership behavior, not physical FICLONE speed or fleet SLOs.
