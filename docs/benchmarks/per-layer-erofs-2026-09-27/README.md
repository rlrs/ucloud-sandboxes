# Per-layer EROFS qualification — 2026-09-27

Qualified on a disposable Hetzner CPX42 cloned from the saved sandbox snapshot,
with no production workers or gateway running. Linux 7.0.0-30-generic,
erofs-utils 1.9, Docker 29.8.1 (overlay2), Python 3.14.4. Exact runtime and source
hashes are in [qualification.json](qualification.json) and
[host-and-source.txt](host-and-source.txt).

Run:

```sh
PYTHONPATH=. python runtime/storage_native/qualify_environment.py \
  --runsc /root/runtime/direct/runsc --layers --output qualification.json
```

The fixture builds a scratch base containing static busybox and 64 MiB of
incompressible data, then two derived images. The derived layers exercise file
deletion, deletion of a file created within the squashed group, directory
replacement, hardlinks and symlinks. Each composed EROFS view matches the real
Docker overlay2 reference tree for file contents, types, ownership, permissions,
selected xattrs and hardlink relationships. Timestamps are deliberately excluded:
mkfs normalizes them. The underlying v1 scenario additionally checks preserved
xattrs, opaque directories, writable copy-up, frontend replacement, retained
mount fencing and explicit backend-loss handling.

All three v2 images are mounted simultaneously. Their deterministic null-UUID
EROFS filesystems mount successfully through the relative-lower OverlayFS path.
A live gVisor guest on a layered image verifies the merged contents and writable
copy-up. Removing the base image view leaves both derived images readable and
cannot disconnect their shared component. Republishing a derived image reuses
all components and uploads no new blobs. Cleanup reports no errors.

| Observation | Result |
| --- | ---: |
| Images | 3 |
| Component references / distinct components | 5 / 3 |
| Distinct component image bytes | 68,784,128 |
| Same components counted separately per image | 206,336,000 |
| Bytes avoided by sharing in this fixture | 66.7% |
| Layer scenario elapsed time | 6.08 s |
| Original v1 cold materialization | 0.406 s |
| Original v1 frontend replacement | 0.252 s, zero extra blob bytes |

These numbers describe a deliberately shared-base fixture, not a production image
inventory, fleet density result or park/wake SLO. The HTTP registry is local to
the VM; no claim about Hetzner Volume/WAN throughput follows. The fixture supplies
local diff sizes for grouping instead of compressed OCI tar sizes. Normal
publication uses the registry's layer descriptors.

The qualification harness was also tested with an injected failure after the v1
checks completed. It reports `passed: false`, exits 1 and cleans up successfully.
Previously it could retain `passed: true` after a late exception; the success flag
now commits only after every requested scenario and backend-loss check passes.

Partial device exhaustion has a regression test: components mounted before the
failure are released under exclusive leases so failed placement does not consume
the remaining device pool. Components used by other composed mounts remain
protected by the backend's kernel-dependency check.

This work does not qualify or fix the separate online registry blob-sweep race,
nor memory checkpoint I/O under pressure. Those must not be inferred from the
successful image-format qualification.

Full Linux suite: **2,444 tests ran successfully, 348 skipped** (161.9 s).
The focused environment/retention suite ran 81 tests successfully, one skipped.
See [tests.json](tests.json) for commands and fixture setup. The macOS run had
two platform-specific failures (TCP_NODELAY value and sparse-file accounting);
both pass on Linux. No PostgreSQL service or live fleet was used by this run;
backend-gated tests retain their normal skips.
