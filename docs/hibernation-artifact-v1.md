# Local hibernation artifact v1

Status: code-level contract, hard quota accounting, and restart matrix
implemented; production capability remains disabled after the stock
Docker/containerd lifecycle gate failed.

This document freezes the first local, single-owner hibernation format used by
`ucloud_sandboxes.hibernation`. It is intentionally not a fork, replay, or
cross-node snapshot format.

## Identity

Every manifest binds:

- sandbox ID and sandbox generation;
- monotonic hibernation generation and idempotent operation ID;
- sandbox-spec SHA-256 and exact Docker container ID;
- runsc binary SHA-256 and source commit;
- gVisor platform, host architecture, and page size;
- CPU-feature, boot-configuration, and immutable-rootfs digests;
- exact regular-file device, inode, logical size, role, and observed allocated
  size.

The manifest must contain exactly one main-memory file, kernel-state file, and
allocator-metadata file. Private page files are optional and repeatable.
Unknown fields, duplicate names, unsafe basenames, invalid digests, changed
inodes, changed sizes, symlinks, and runtime mismatches fail closed before
restore.

`metadata_sha256` authenticates the canonical JSON metadata. The main memory
file is deliberately not content-hashed: scanning it would negate
metadata-only hibernation. It is protected by the root-owned local artifact
tree, dirfd-relative no-follow opens, exact inode/type/size validation, the
serialized allocator graph, and generation fencing.

## Durable layout

```text
<root>/
  <sandbox-id>.sandbox-<sandbox-generation>/
    hibernate-<hibernation-generation>/
      application_memory.img
      checkpoint.img
      pages_meta.img
      [private page files...]
      manifest.json
      COMPLETE
```

A generation is pending until every named artifact is fsynced, `manifest.json`
is atomically replaced and fsynced, and `COMPLETE` is atomically replaced and
its directory fsynced. A directory without a valid `COMPLETE` marker is never
restorable. Publishing is idempotent only for the exact same manifest digest.

The canonical main-memory inode moves from the active directory into the
pending generation after the sentry is reaped. The move is basename-only
between pre-opened real directories and must remain on one filesystem.

## Journal state and authority

The durable state is not sufficient by itself; authority identifies which
object may still mutate sandbox state:

| State | Legal authority | Meaning |
| --- | --- | --- |
| `running` | `live` | One sentry is authoritative. |
| `hibernating` | `live` | The sentry is paused; hibernation may still abort. |
| `hibernating` | `pending` | The sentry is reaped; the pending generation is authoritative. |
| `parked` | `parked` | One valid `COMPLETE` generation is authoritative. |
| `restoring` | `parked` | Restore has intent but no candidate owns execution. |
| `restoring` | `candidate` | One candidate sentry must be verified, adopted, or reaped. |
| `recovery-required` | `none`, `pending`, `parked`, or `candidate` | Automated recovery cannot prove a safe transition. `none` means a previously live PID was proved dead, including PID-reuse detection. |

Journal updates are compare-and-swap operations over a monotonically
increasing revision and are durably replaced with file and parent-directory
fsync. Process authority is an exact `(pid, /proc start-time ticks)` identity,
not a PID alone. A hibernation operation can return to `running/live` only
before `mark_sentry_reaped`. A restore can return to `parked` from candidate
authority only after the caller proves the candidate was reaped.

## Recovery classification

- `running/live` with the exact sentry alive: adopt it; otherwise quarantine.
- `hibernating/live` with the sentry alive: resume or retry the same operation.
- any hibernating state after the sentry is dead: finish the pending
  generation; never assume the old runtime remains authoritative.
- `parked` requires a valid complete manifest.
- `restoring/parked` first asks the runtime owner to resolve any candidate
  started before identity publication. An exact live candidate is durably
  adopted; restore is retried only after the runtime proves no candidate
  exists.
- `restoring/candidate` verifies and adopts the exact candidate, or reaps it
  and rolls back to the complete generation.
- ambiguous or malformed state becomes `recovery-required`; it is never
  garbage-collected automatically.

## Physical disk reservation

Parkable sandboxes change scheduler disk demand from the user-visible writable
limit to a hard physical reservation:

```text
writable_disk_mb
+ (floor(memory_mb / 1024) + 1) * 1024
+ memory_mb private-page allowance
+ 64 MiB metadata overhead
```

The extra allocator chunk reflects measured pgalloc growth: 256 MiB, 1 GiB,
and 4 GiB dense workloads produced 1 GiB, 2 GiB, and 5 GiB logical backing
files. Sparse holes and current allocated blocks never reduce admission.

The additional full memory allowance is conservative: the spike's private
page image was empty, but that is not yet a universal bound. P3 may replace it
with a smaller proven limit only after every private `MemoryFile` is
externalized or bounded by conformance.

For example, a 4 GiB-memory sandbox requesting 8 GiB writable disk currently
reserves 17,472 MiB of node disk. Shared immutable image layers remain a
separate node-level reservation.

The platform does not permit disk overcommit above 1x: configuration and node
heartbeat construction reject it, and scheduler capacity clamps untrusted
heartbeat values. This preserves the physical interpretation of the
reservation rather than multiplying it through the generic overcommit path.

## Capability gate

`hibernate-local-v1` is derived only when:

1. the hibernation end-to-end conformance probe passed;
2. storage and tmpfs quota probes passed; and
3. the report's hibernation runtime fingerprint exactly matches the
   configured expected fingerprint.

The node server also strips this capability unless its runtime was explicitly
constructed with hibernation enabled. CLI enablement additionally requires
execute mode, explicit disk capacity and nonzero headroom, the durable ledger,
the privileged XFS helper, and the exact report fingerprint. No production
report can currently pass the end-to-end gate: direct runsc works, but stock
containerd tears down the shim/gofer/bundle after the sentry exits.
