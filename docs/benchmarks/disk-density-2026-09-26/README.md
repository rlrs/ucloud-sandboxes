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

## Hetzner: 540 sandboxes on one CCX63 (rc48)

**Setup:**
- Gateway: CPX32 on Primary IP 77.42.92.27, 3 gateway processes.
- Worker: one CCX63 (48 dedicated vCPU, 184 GiB, 915 GiB disk), booted from
  golden snapshot `436465822`: Ubuntu 26.04, pinned kernel 7.0.0-30, the
  qualified rc48 node bundle.
- Worker configuration: RAM-backed memory, reflink restore off, 512 MiB
  grants, `storage_native_max_ublk_devices=0`.
- Driver: a CCX23 in the private network.
- Workload: identical to the UCloud run above.

| Run | Placed | Correct | Usable exec p50 / p95 / p99 |
|---|---:|---|---|
| `-ublk128` (cap inherited from rc37) | 128 | no: creates beyond 128 timed out | 0.40 / 0.50 / 0.53 s |
| `-cold` (worker booted seconds before) | 540 | yes, 0 errors | 0.62 / 4.61 / 8.49 s |
| `-warm` (same worker, second run) | 540 | yes, 0 errors | 0.66 / 3.96 / 8.04 s |
| `-gw3` (3 gateway processes) | 540 | yes, 0 errors | 0.62 / 3.83 / 8.80 s |

**Claims:** 540 × 576 MiB = 311,040 MiB of about 795 GiB. Physical use on
the shared filesystem grew by about 60 GB (`node-claim-totals-hetzner-rc48.txt`).

**Where the tail comes from.** All of it falls in the creation burst: 540
creates land on one node within about 80 s. Guest continuation p95 was:
- **first 20 s:** 12.7 s;
- **t+20 to 40 s:** 2.5 s;
- **after that:** 0.16–0.57 s, lower than UCloud's 0.28–1.34 s in the same
  windows.

Three things were ruled out as causes: the driver's event loop (lag p99
0.09 s), the gateway (3 processes changed nothing; response commit p95 is
0.03 s), and cold images. Burst create admission on a single node is the
open item.

**Canary on the CPX32 snapshot source.** A 2.5 GiB write, park, wake and
checksum passed on rc48 (`e2e-park-wake-hetzner-cpx32-rc48.json`). On rc47,
the same capture exceeded the fixed 60 s runsc timeout and the sandbox was
lost; rc48 sizes that deadline by the bytes moved.

## Hetzner: normal load test (rc48)

Both tests were driven from a laptop over the public HTTPS endpoint.

**`hetzner-load150-rc48.json`** (`scripts/live_load_benchmark.py --sandboxes 150`):
- **Builds:** three fresh images (python/pip, node/npm, ubuntu/apt) built on a
  CCX33 booted from the pinned-kernel snapshot, in 104–123 s each including
  builder boot.
- **Ramp:** 150 sandboxes in 0 → 10 → 50 → 150 steps with 0 failures; 100
  creates took 8.6 s.
- **Exec rounds:** 150/150 each.
  - **Exec start** was slow under 100 concurrent callers: p50 about 6 s.
  - **Light exec wait** p50 was 2.6 s; cpu_io wait p50 was 5.2 s.
- **Cleanup:** every sandbox, capacity reservation and the builder cleaned up.

**`hetzner-features-rc48.json`:**
- **Direct sandboxes (10):**
  - a 4 MB file round-trip (upload p50 2.9 s, download p50 1.4 s), with
    hashes verified;
  - a 20 MB exec stdout stream took about 23 s, under 1 MB/s per session;
  - direct egress through the gateway NAT worked.
- **Relay-only sandboxes (2):** direct egress was blocked and the relay at
  10.42.0.2:8092 was reachable.

**Open items:**
- exec start latency under concurrency;
- exec output streaming throughput through the gateway.
