# Disk density: claims that follow demonstrated usage

Status: implemented in 0.5.114rc44–rc46 and qualified on a UCloud worker; see
[benchmarks/disk-density-2026-09-26](benchmarks/disk-density-2026-09-26/README.md).

## Problem

A parkable sandbox with 1 GiB of memory and a 4 GiB disk used to hold a fixed
7,232 MiB hard claim for its whole life:

| Component | MiB | Held |
|---|---:|---|
| workspace volume (`disk_mb`) | 4,096 | while the volume is local |
| memory backing, chunk-rounded memory | 2,048 | always |
| private checkpoint pages (`memory_mb`) | 1,024 | always |
| fixed overhead | 64 | always |

A Hetzner CCX63 has about 780 GiB of hard storage capacity after the image
cache, remote-block cache and safety headroom. That fits about 110 of these
sandboxes. The density target is about 500 per node.

Almost none of that claim is physically used. A fresh workspace writes a few
hundred MiB. In RAM-backed memory mode a running sandbox writes nothing to its
memory allocation, and a parked sandbox's checkpoint is about as large as its
resident memory, not twice its limit.

## Principles

- **Resource specs are maximums.** `disk_mb` and `memory_mb` bound a sandbox.
  They are not reservations.
- **Hard claims cover what can physically be written.** The node never promises
  more physical bytes than it has. There is no overcommit ratio. When a claim
  cannot grow, a create, park, wake or workspace growth is refused and can be
  retried elsewhere. The shared loop filesystem never fills up.
- **Transient space is reserved only for the transition that needs it.**
  Checkpoint capture space is reserved at park time, sized from the sandbox's
  measured memory, and shrinks to the bytes actually written once the capture
  commits.

## Workspace grants

A workspace volume keeps its ublk device at the full `disk_mb` (the *ceiling*;
`virtual_size` keeps that meaning everywhere). The XFS filesystem on it is
created smaller, at the *grant*, and is grown online with `xfs_growfs` as the
guest fills it.

- **Grant size.** A new split workspace is formatted at
  `min(disk_mb, direct_workspace_initial_grant_mb)`, 512 MiB by default. The
  minimum is 512 MiB, because current xfsprogs refuses filesystems under
  300 MB. A ceiling at or below the grant is formatted at full size, as before.
- **Why the grant bounds physical bytes.** The overlaybd upper only stores
  blocks that the filesystem writes. XFS never writes past its data section,
  so the live upper is bounded by the grant just as it used to be bounded by
  `virtual_size`. Deleted blocks are not trimmed (runtime trim is still
  disabled), so the grant, not the guest's current usage, is the bound.
- **Sealed layers.** A park seals the upper into an immutable local layer.
  After the next wake a new upper can again grow to the grant on top of it. The
  charge is therefore `grant + local sealed layer bytes`. Layer bytes are
  measured as allocated blocks (deduplicated by inode) whenever the layer set
  changes: at seal, when a mount adopts a local compaction, at publish and at
  import. Before this change the charge was `virtual_size`, which
  under-counted sealed layers.
- **Trim before seal.** A sealed layer would otherwise keep every block the
  guest ever wrote. Hibernate punches the gVisor filestore right before the
  capture seals, so a heavy writer's layer used to hold its data twice.
  Before sealing, the daemon runs FITRIM with 1 MiB minimum extents, the
  policy `qualify_xfs_trim.py` qualified, if the live upper holds at least
  `--trim-before-seal-bytes` (256 MiB) more than the filesystem uses. The
  guest is already paused. Runtime trim stays off.
- **Published workspaces** are charged nothing, as before. A remount charges
  the grant again, with retryable refusal.
- **Growth.**
  - A node agent thread polls `statvfs` on mounted workspaces every 0.5 s.
  - It grows a filesystem when free space drops below
    `max(256 MiB, size / 4)`.
  - The step is `max(512 MiB, size / 2)`, capped at the ceiling.
  - The registry first reserves the larger claim. If the node lacks physical
    headroom, it refuses and the filesystem keeps its current size.
  - The storage daemon then journals the new grant and runs
    `xfs_growfs -D`.
  - Growth skips a sandbox whose lifecycle lock is busy, for example one that
    is parking.
- **Recovery.** The daemon journals the grant before growing, so the charge
  never trails the filesystem. After every mount it reads the grant back from
  the XFS superblock (`sb_dblocks × sb_blocksize`), so imports and interrupted
  growth self-correct. Old volume records have no grant field and decode with
  `grant = virtual_size`, which was their real filesystem size.

**Residual risk (accepted).** A guest that writes faster than roughly
`free space / (poll interval + growth latency)` gets `ENOSPC` before it
reaches `disk_mb`. With the defaults that means about 500 MB/s sustained from
the moment free space crosses the threshold. A guest also gets `ENOSPC` early
when the node has no headroom left for growth. Growth refusals are counted in
node metrics, and the cold-offload relief below reacts to the same pressure.
Only that one guest sees the error. Other sandboxes and the node are
unaffected.

## Memory checkpoint claims (RAM-backed mode)

This applies when `direct_ram_memory_backing` is on and
`direct_reflink_memory_restore` is off, which is the Hetzner configuration. In
RAM mode a running sandbox's memory lives on the tmpfs RAM root. Its disk
allocation directory holds only an ownership marker until it parks.

| Phase | Memory claim | XFS project `bhard` |
|---|---|---|
| created, running | 64 MiB (idle) | 64 MiB |
| park admission | `R` = an upper bound of the capture (below) | `R` |
| capture committed (parked) | allocated bytes actually written, plus 1 MiB | same |
| capture failed or rolled back | back to idle. The next park of that incarnation uses the formula. | idle |
| woken, checkpoint deleted | back to idle, after the coalesced FITRIM barrier | idle |
| deleted | released | released |

`formula` is the old per-sandbox memory component (3,136 MiB for 1 GiB).

`R` must be an upper bound, not an estimate. A hibernate capture frees
runtime state as it writes, so running out of space part-way can lose the
sandbox: the rollback can no longer resume it. The qualification worker
showed this on its first attempt. A capture contains:

- the application memory image. For RAM-backed owners this is the tmpfs
  memory file's allocated bytes, which are known exactly;
- other private sentry pages, bounded by the cgroup's resident memory
  (itself at most `memory_mb`);
- **the gVisor filestore**. The guest's rootfs writes, including
  `/workspace`, live in `.gvisor.filestore.<cid>` in the workspace upper.
  A hibernate capture serializes it into `pages.img` and punches the
  filestore's blocks. Its allocated size is measured exactly at park.

`R = memory file + min(resident, memory_mb) + filestore + 64 MiB`, with no
formula cap. If resident memory is unknown, the formula stands in for the
first two terms.

- If the registry cannot reserve `R`, the park is refused with a retryable
  capacity error and the sandbox keeps running.
- The old fixed formula (2 x memory + 64 MiB) never counted the filestore.
  A sandbox that had written more than about 2 GB to its rootfs could fail
  its park. This applies to fixed-claim registrations in production today.
  Dynamic file-backed registrations now reserve formula + filestore at park
  and settle back to at least the formula.
- Lowering a claim after a wake waits for FITRIM, as allocation deletion
  already does. Freed extents in the sparse loop image must reach the parent
  disk before the claim is released.

Two cases keep the formula for the lifetime of the sandbox:
- Reflink restore. It converts an owner to file-backed memory at its first
  park, and that owner's live memory file then grows on disk.
- File-backed workers (no RAM root).

Reflink restore is a real trade-off, not an oversight. It restores a 2 GiB
heap in 78–127 ms instead of 731–1,601 ms, writes about a quarter as much
per park/wake turn, and allows live memory reclaim without parking (see
[memory-tiers](benchmarks/memory-tiers-2026-09-23/README.md)). The cost is
the formula memory claim for the sandbox's lifetime. The Hetzner deployment
turns it off for density. Workspace grants apply in both modes.

Imported and upgraded split registrations start with a fixed claim. They
adopt the equal dynamic claim (workspace ceiling + formula memory) on their
first workspace sync or park, so they also get the filestore-safe capture
bound.

## One ledger, three reporters

The direct registry stays the single admission authority. Each registration
row in `registration_disk` now carries:

- `reserved_mb`: fixed claims, for legacy and imported registrations until
  their first claim update.
- `workspace_mb`: grant plus local layers, synced from the storage daemon
  record.
- `memory_mb`: the checkpoint claim described above.

Increases that create physical capacity (plan, grant growth, park admission,
workspace remount) check capacity atomically. Updates that record bytes
already on disk (sealed layers, captured checkpoint size) are always
accepted: the bytes exist whether or not they fit. The node then refuses new
work until relief frees space.

The storage daemon's ledger (`grant + local layers` per active volume) and the
memory store's (`bhard` per allocation) still check their own subtotals and
still feed the heartbeat's `storage_hard_reserved_mb`. The per-registration
charges in heartbeat inventory accounting come from the registry claims
instead of `quota_total_mb`.

Gateway placement still requires the full `requested_resources()` disk to fit
the node's free hard capacity. That keeps a ceiling-sized safety margin for
growth and burst admission. In-flight create routes, which the heartbeat has
not yet matched, are charged the initial claim the node advertises
(`storage_workspace_grant_mb`, `storage_memory_idle_claim_mb`) instead of
7,232 MiB, so a create burst does not exhaust the node on paper.

## Relief ladder

When claims approach capacity, these kick in, in this order:
1. Published-local cache eviction (existing, runs every heartbeat).
2. Background publication of parked sandboxes (existing), which drops their
   workspace claim to zero.
3. Cold offload of published parked sandboxes (existing autoscaler) at 90%
   `storage_hard_reserved`.
4. Refusal: creates are placed elsewhere or queued, parks and wakes are
   refused with retryable errors, and growth is refused (the per-guest risk
   above).

## Capacity at 1 GiB memory, 4 GiB disk

Old versus new claim:

| State | Old claim | New claim |
|---|---:|---:|
| running (fresh) | 7,232 MiB | 576 MiB (512 MiB grant + 64); 1,088 MiB with a 1 GiB grant |
| parked, unpublished | 7,232 MiB | grant + layers + checkpoint (about 1.5 GiB) |
| parked, published | 3,136 MiB | checkpoint only (about 0.35 GiB) |

With about 780 GiB of capacity:
- 500 running sandboxes need about 530 GiB.
- 500 parked, unpublished sandboxes need about 750 GiB.
- Publication brings parked sandboxes well under that.

RAM (192 GB on a CCX63), not disk, now limits how many run at once.

## Configuration

- `sandbox_pool.direct_workspace_initial_grant_mb`: default 512. `0` formats
  full-size workspaces and disables growth, which is the old behaviour.
- Demonstrated memory claims are on whenever RAM backing is on and reflink
  restore is off.

## Qualification

`runtime/storage_native/qualify_workspace_growth.py` exercises grants on
real ublk/XFS:
- formatting at a grant, ENOSPC containment, and upper allocation under
  churn;
- online growth under writes, remount readback, and monitor-driven growth.

The 2026-09-26 results, an end-to-end park/wake run through the gateway, and
540 sandboxes on one worker are recorded in
[benchmarks/disk-density-2026-09-26](benchmarks/disk-density-2026-09-26/README.md).

Known follow-up: reflink-restored (file-backed) owners could also charge
demonstrated memory, growing their project limit as the live memory file
grows. That would combine reflink's fast wakes with memory-side density.
