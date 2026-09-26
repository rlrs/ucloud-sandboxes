# Disk density qualification (0.5.114rc44–rc46)

These runs qualify the design in [docs/disk-density.md](../../disk-density.md).

- **Where:** UCloud gateway `live-ucloud-20260824a`, with one
  `cpu-amd-zen5-32-vcpu` worker (32 vCPU, 88 GiB RAM, 1.45 TB hard storage
  capacity).
- **Worker configuration:** RAM-backed memory, reflink restore off (the
  Hetzner configuration), and a 1 GiB initial workspace grant.

## Storage service on real ublk/XFS

`workspace-growth-ublk.json` comes from `runtime/storage_native/qualify_workspace_growth.py`.
It ran against a private AgentEnv backend using the hybrid log-structured
upper.

| Check | Result |
|---|---|
| 4 GiB device formatted at a 1 GiB grant | XFS geometry 1 GiB, 953 MB free |
| Fill to the grant | ENOSPC after 948 MB; a neighbour volume kept writing |
| Four rounds of 700 MB delete/rewrite | upper allocation stayed at 1,020,960,768 bytes (within the grant) |
| Online growth under concurrent writes | 11–16 ms per `GrowVolume`; data verified at every step |
| Seal, release, remount | grown 4 GiB read back from XFS geometry; data verified |
| Monitor-driven growth (0.5 s poll) against a streaming writer | 7 GiB written at 317 MB/s from a 1 GiB grant, no early ENOSPC |

## End to end through the gateway

`e2e-park-wake-rc46.json` and `e2e-claims-timeline-rc46.txt` record one
parkable sandbox (1 GiB memory, 4 GiB disk) that:
- wrote 2.5 GiB to `/workspace`;
- was parked, then woken by an exec;
- matched its sha256 checksums after the wake.

Its claims (workspace MiB, memory MiB) moved as follows:

1. Create: `1024 / 64`.
2. Growth under the write: `2048 → 3072 → 4096` (the ceiling).
3. Park admission: memory `3953`, which covers the RAM memory file, resident
   pages, the 2.6 GB filestore and 64 MiB.
4. Capture committed: memory settled to `2866`.
5. Wake: memory returned to `64` after the trim barrier.

### Findings that changed the code

- **rc44: new workspaces were not grown.** The growth monitor was only
  seeded at startup and on mount/release. A new sandbox hit ENOSPC at its
  grant. Fixed in rc45.
- **rc45: memory estimates are not capture bounds.** In gVisor, the guest's
  rootfs writes (including `/workspace`) live in `.gvisor.filestore.<cid>`.
  A hibernate capture serializes that file into `pages.img`.
  - A capture sized from memory ran out of project quota part-way. Hibernate
    frees state while it writes, so the test sandbox was lost.
  - rc46 reserves an upper bound that includes the filestore's allocated
    bytes, with no formula cap.
  - The same exposure exists for fixed (formula) claims: 2 × memory +
    64 MiB never counted the filestore.

## Density: 540 sandboxes on one worker

`density540-1node-rc46.json.gz` and `driver-events-rc46.log.gz` come from
the same workload as the rc43 run in
[density-rc38-2026-09-26](../density-rc38-2026-09-26/README.md), with all
540 sandboxes on a single worker instead of three.

| | rc43, 3 workers (180/node) | rc46, 1 worker (540/node) |
|---|---:|---:|
| correct | yes | yes |
| usable exec p50 / p95 / p99 | 1.00 / 2.45 / 3.12 s | 1.06 / 1.74 / 2.14 s |
| guest continuation p95 | 1.69 s | 1.16 s |
| response commit p95 | 0.20 s | 0.14 s |
| per-sandbox disk claim | 7,232 MiB | 1,088 MiB |
| node disk claims at 540 | would be 3.9 TB (does not fit) | 587,520 MiB of 1.45 TB |
| physical growth at 540 | — | about 73 GB (about 135 MB per sandbox) |

`node-claim-totals-rc46.txt` has one line per sample. The fields are, in
order:
1. timestamp;
2. registrations;
3. total claim in MiB;
4. memory claims in MiB;
5. local workspace claims in MiB;
6. published workspaces;
7. physical MiB used on the shared filesystem.

With warm retention and memory to spare, no sandbox in this workload parked,
so every memory claim stayed idle. The park path is covered by the
end-to-end run above.

The claim is still about 8 × the physical use, because each workspace holds
a 1 GiB grant. A 512 MiB grant roughly halves it. On a CCX63 (about 780 GiB
of capacity) the 1 GiB default fits about 700 running sandboxes on disk.
RAM is the binding limit.
