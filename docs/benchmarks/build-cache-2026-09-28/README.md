# Build cache optimization — 28 September 2026

**Deployed to production at 12:30 UTC.** The restored forwarded SSH agent allowed
gateway installation and repacking of both node roles. The existing qualified
OS packages, kernel modules, gVisor, and storage-native binaries were preserved.
Gateway health passed after restart. Exact checksums are recorded in
[deployment-receipt.json](deployment-receipt.json).

## Changes

- For whole-image publication, read and hash-check the OCI config before Docker
  materialization. Plan the same groups from the config's diff IDs and manifest
  sizes. Reuse only authenticated components matching their layer lists, parent
  chain, and exact format. Refresh their retention tags and check referenced blob
  availability before committing the signed environment.
- If every group is available, skip Docker pull, extraction, and the temporary
  overlay mount. A partial miss uses the existing cold path, reusing available
  groups there. Unsupported layouts and old mkfs version-query behavior retain
  the existing whole-image fallback. A mismatched OCI config digest fails closed.
- Hold a file lock per layer group across conversion and publication. Recheck
  the cache inside the lock, so another build on the same node can fill it while
  a caller waits. Process exit releases locks; files remain to avoid split-inode
  races. Different groups can still build concurrently. This does not coordinate
  conversions across separate builder VMs.
- Add per-build preflight, Docker pull, lock wait, lookup, squash, mkfs, signing,
  and publication timings, with group-hit/build and EROFS-byte counters. Gateway
  `image_build_completed` metric events retain observed terminal results without
  copying build logs or command contents. Clients need to observe a terminal
  response for this capture; it is not a fleet-wide guaranteed completion stream.

The signed artifact format, component keys, and source-chain binding are
unchanged. Native runtime binaries are unchanged.

## Local Docker comparison

[benchmark_local.py](benchmark_local.py) uses a disposable localhost registry,
a scratch Docker image containing 64 MiB of incompressible data, real overlay2
mounts, and native mkfs.erofs 1.4. The known host version is pinned explicitly
because this older mkfs does not support `-V`. Each comparison removes the
fixture's local image tag; shared Docker/BuildKit caches are not globally pruned.
The registry already contains the signed EROFS component for both comparisons.

The baseline disables only the new preflight lookup, retaining the former
pull-first path. Results are in [local-docker.json](local-docker.json).

| Cached-component publication | Elapsed | Docker commands |
| --- | ---: | ---: |
| Previous pull-first path | 337 ms | 7 |
| New cache-first path | 64 ms | 0 |

The optimized fixture was 5.3 times faster and emitted the **same annotated image
manifest digest**. This is a single local cache-path comparison, not production
latency, fleet throughput, or a promise about entirely new image builds. The
production builder uses erofs-utils 1.9; no new native format was introduced.

An additional [conversion fixture](local-conversion.json), using an in-memory
registry and simulated Docker adapter with real mkfs, verifies three cold group
conversions and zero conversions on a complete hit. Its cold/warm times are not
an old/new implementation comparison.

## Validation and release

The relevant environment, image, gateway polling/control-plane, and registry
integration suite passed 194 tests (two platform/privilege skips). The native
Linux whiteout test passed separately under root. Additional targeted tests
cover warm-path configuration preservation, partial misses, corrupted OCI
configs, concurrent conversion, failed-owner recovery, persistent metrics, and
the old-mkfs fallback. Lint and whitespace checks pass.

The deployed wheel is staged locally under `build/build-optimization/`, retaining
the current 0.7.0 package version; it is not a published release. The initial deployment used
`/work/ucloud-sandboxes/build-optimization-20260928` as its node package root.
The later [gateway qualification](../gateway-capacity-2026-09-28/README.md)
preserved these fixes and installed the current
`/work/ucloud-sandboxes/gateway-inventory-optimization-20260928` bundle root.
Its `rollback/` directory retains the previous gateway virtual environment and
deployment configuration; the old `/work/ucloud-sandboxes/release` bundles remain
available. Rollback requires restoring that configuration and runtime while
gateway, placement, and autoscaler services are stopped, then restarting them.
Already running nodes retain their installed package until replaced.

## Production canary

A fresh CCX33 builder built a BusyBox image containing a unique 512 KiB payload,
then built identical content under another image ID. Both completed successfully.
The measurements in [production-canary.json](production-canary.json) are:

| Phase | Cold component | Cached component |
| --- | ---: | ---: |
| Docker build and push | 2,888 ms | 765 ms |
| Immutable environment publication | 944 ms | 343 ms |
| Total build | 3,845 ms | 1,121 ms |
| Groups converted / reused | 1 / 0 | 0 / 1 |
| Docker pull | 70 ms | skipped |

The gateway retained both terminal results in
[production-canary-metrics.json](production-canary-metrics.json). This compares
cold and warm builds on the new code, not old and new production versions. The
small fixture verifies behavior, not throughput for the earlier workload or
capacity for 500 concurrent containers.

The published image also started on a fresh CCX63 sandbox worker. All five APT
update units were masked and inactive on the gateway, fresh builder, and fresh
worker; the three periodic APT settings were zero on both fresh nodes.
An exec checked the payload length and exited zero; its own output is saved in
[production-canary-exec.json](production-canary-exec.json). The sandbox was
deleted and the builder preparation reservation released after verification;
the empty nodes remain subject to the existing five-minute idle scale-down.
