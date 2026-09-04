# Correctness and performance review — 2026-09-04

Seven additional issues were reproduced or validated across component boundaries and fixed in the working tree. The earlier build-context expiry and model-relay fixes remain in place. This report describes a broad source review with targeted local fault injection, not a production qualification or a claim that every possible defect has been eliminated.

## Findings and fixes

| Priority | Issue | Evidence before the fix | Implemented correction |
| --- | --- | --- | --- |
| P1 | A delayed delete can remove a replacement sandbox generation | Paused a generation-7 delete after its initial check, deleted/recreated the sandbox as generation 8, then resumed the old call. Generation 8 disappeared. | Carry the expected generation into the provisioner and check it in the registry's deletion transaction, including when revisions happen to match. |
| P1 | Storage reconciliation races with live allocation and recovery | Paused reconciliation after an empty journal snapshot, created a mounted volume, then resumed. Reconciliation deleted device 1 as an orphan while its durable record remained mounted. | Shared operation admission for mutations and exclusive admission for reconciliation; nested convergence calls retain concurrency without deadlocking. Recovery waits for active workers instead of treating their transitions as interrupted. |
| P1 | Wake can bypass drain, live resource pressure, and restore limits | Explicit wake returned running with admission closed, CPU at 99%, and only 10 MiB available RAM. | Explicit wake reserves active capacity and acquires the same restore semaphore as implicit wake. Already-running requests retain their successful no-op behavior. |
| P1 | S3 GC can forget lower layers still used after wake | Successful wake clears the route's restore descriptor, while the mounted volume's source config still contains remote lower layers. The old collector read only route restore descriptors. | Separate durable storage dependencies from restore authority. Retain dependencies across wake, include unfinished migrations, and report dependencies in running-worker heartbeats. GC refuses collection until every route has a retained publication or dependency report. |
| P1 | Deleted-volume history eventually breaks storage inventory and heartbeats | 2,001 deleted records and no live devices produced a 1,391,400-byte response; the client rejected it at its 1 MiB limit. | Add bounded, cursor-based live-volume inventory, with an index excluding deleted records. Keep tombstones accessible through exact lookup for ownership and replay fencing. |
| P2 | Exec timeout does not cover inherited pipes | A parent spawned a child holding stdout/stderr and exited. A 0.2-second exec timeout returned success after about 5.05 seconds. | Use nonblocking, bounded I/O under one monotonic deadline. Kill the command's process group on timeout/overflow and close descriptors without blocking on reader-thread locks. |
| P2 | Temporary build-status write failures permanently consume build slots | Failed the timing and terminal writes. The worker exited, the durable build stayed running, active count stayed 1, and the next build was rejected even after writes recovered. Process-based recovery found no interrupted work. | Retain terminal results in memory until durable persistence succeeds. Retry on subsequent reads, admission and heartbeat counts, including failures before worker startup. |

Relevant implementation locations:

- Deletion: [service](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/direct_service.py:566), [provisioner](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/direct_provisioner.py:440), [registry transaction](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/direct_registry.py:556).
- Storage mutation/recovery: [operation gate](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/storage_native_daemon.py:1163).
- Wake admission: [explicit wake](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/direct_service.py:757).
- S3 dependency ownership: [routing journal](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/routing.py:736), [collector](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/storage_native_s3_gc.py:286), [rollout notes](/Users/Rasmus/Git/ucloud-sandboxes/docs/object-storage-snapshots.md:124).
- Inventory: [indexed query](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/storage_native_daemon.py:958), [paginated client](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/storage_native_daemon.py:2575).
- Exec: [process runner](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/direct_service.py:114).
- Build persistence: [terminal retry](/Users/Rasmus/Git/ucloud-sandboxes/ucloud_sandboxes/images.py:1137).

The S3 review also traced the case where a worker publishes and wakes between gateway observations. Retaining only snapshots already seen by the gateway was insufficient. The final implementation therefore carries remote dependency metadata on running-sandbox heartbeats, without granting permission to restore from that older checkpoint. A delayed empty report cannot erase an already retained remote dependency.

## Coverage

The review concentrated on transitions and boundaries where independently correct components can disagree:

| Area | Source paths and behavior inspected |
| --- | --- |
| Sandbox lifecycle | `direct_service`, `direct_provisioner`, `direct_registry`, `direct_warden`, `node_runtime`: create/delete fences, rollback, park/wake, expiry, drain, admission, activity counters and recovery. |
| Storage | `storage_native_daemon`, `storage_native_service`, publication/registry/S3 modules, S3 GC and storage migrations: reservation, import, mount, seal, publication, deletion, replay, reconciliation, protocol limits and retained references. |
| Execution | `sandbox_exec`, `managed_process`, `DirectProcessRunner`, and Go managed-process runtime: streams, output limits, process exit, timeouts, signals, logs, launch state and persistence. |
| Gateway and routing | Lifecycle response fences, wake relocation and commit, heartbeat reconciliation, proxy/build-context paths, SQLite route and migration state. |
| Images/builds | Build admission, idempotency, materialization, logs, completion/recovery, image-rootfs cache ownership and collection. |
| Scheduling and providers | Policy evaluation, packing, provisioning/stop decisions, resource admission, UCloud inventory refresh, Hetzner pagination/error handling, autoscaler operation outcomes. |
| Shared infrastructure | Relay completion/pinning state, build-context retention, metrics persistence and pruning, resource sampling, cache invalidation, network lease persistence and reconciliation. |
| Packaging | Python lint and test discovery, shell syntax, Go tests, wheel creation and an installed-wheel smoke test outside the source directory. |

The dashboard, broad CLI/deployment surface and bootstrap/install scripts received less source-level attention than lifecycle/storage code. Shell syntax checks do not qualify their behavior on a real host. The nested SDK was not independently audited. Existing automated tests may cover these areas, but that is different from a complete manual review.

## Verification and performance

- Python suite: **705 tests, passed; 4 skipped**.
- Added **15 regression tests** in [test_correctness_regressions.py](/Users/Rasmus/Git/ucloud-sandboxes/tests/test_correctness_regressions.py). These exercise races in both directions, generation fencing, CPU/RAM/drain admission, restore-slot contention, inherited pipes, blocked stdin, binary full-duplex I/O, ignored SIGTERM, real Unix-socket pagination, S3 dependency retention/recovery and transient build persistence failures.
- Existing heartbeat serialization tests retain the legacy payload shape when the new optional dependency field is absent.
- `ruff check ucloud_sandboxes tests scripts`: passed.
- `git diff --check`: passed.
- Shell syntax checks for `scripts/*.sh` and `runtime/*/*.sh`: passed. ShellCheck was not installed.
- Go managed-process tests: passed with the repository's macOS external-linker setting.
- Wheel build and installed-wheel smoke test in an isolated environment: passed.

On this machine, querying 2,001 deleted records through the old full-inventory path took a median **45.182 ms** over 20 iterations. The new empty live-page query took **0.195 ms**. SQLite's query plan confirmed use of the partial live-inventory index. This measures journal-query work, not end-to-end production heartbeat latency.

The earlier relay benchmark measured completion planning with 8,192 retained completions at approximately **2.37 ms before** and **0.0096 ms after** its incremental indexing change. These local measurements illustrate the eliminated work; they are not production throughput guarantees.

## Operational limits

No production cloud jobs, real ublk devices, mounted guest filesystems, gVisor workloads, S3 bucket contents or live deployments were changed. Storage/device/S3 failures were exercised with local test backends, with actual journal and socket code where applicable.

Deploy the gateway's new inventory-field support before updated worker reports, and update the node agent together with its storage daemon's paginated protocol support. GC waits until dependencies are known for every route; this deliberately postpones collection during incomplete inventory or upgrades. Existing parked publications are backfilled, and upgraded worker heartbeats recover dependencies erased by older gateways. An unreachable worker cannot supply that proof.

The build-result retry preserves state while the process is alive. If the process exits during a persistent storage failure, the existing dead-owner reconciliation remains responsible after restart. These changes have not been qualified with live kernel/storage fault injection or provider load tests.
