# Resident waits: measured memory and reclaim

The native 1.5 GiB experiment rejected an extra quiesced lifecycle tier. Ordinary cache reclaim can run against the existing live generation; pausing did not show a consistent benefit and added roughly 46–49 ms to thaw. The proposed quiescing/quiesced/thawing product states, journal transitions, gateway readers and associated tests were removed. There is still one live resident wait and one durable park path.

Evidence is in `docs/benchmarks/split-memory-2026-09-23/resident-wait-disk.json` and `resident-wait-ram.json`. These are isolated native-worker measurements, not a fleet-level latency or density claim. Both used an actual 1.5 GiB guest allocation and checked every page, TCP/socket state, timers, threads, signals and persistent filesystem content after each operation. The canonical OCI cgroup path was verified against the actual sentry membership.

| Backing | Result |
| --- | --- |
| Ordinary file, live reclaim | 257–448 MiB freed; 42–109 ms reclaim; first full-heap check 253–509 ms versus 30–36 ms without reclaim. |
| Ordinary file, paused reclaim | Similar reclaim benefit; 46–49 ms additional thaw cost and no consistent latency advantage. |
| RAM-active tmpfs | 1,536.7 MiB charged as shmem; no eligible reclaimable cache, zero reclaim syscalls; checks 31–37 ms versus 31–38 ms baseline. |

A RAM-active heap without swap cannot be reclaimed as ordinary file cache. Real memory deficits still require parking enough waits. The pressure governor must account for actual memory, rather than promising that configured limits or paused guests have released it.

The post-rc13 policy also observes the configured RAM filesystem's available
blocks through the canonical resource sampler. Effective headroom is the smaller
of host memory and backing space, with the existing byte-deficit hysteresis and
queued demand. An unreadable configured backing is unknown, never equivalent to
an unconfigured or empty filesystem. A backing-space deficit skips clean-cache
reclaim because that cannot free unswappable tmpfs blocks; the existing fenced
durable checkpoint path must make progress. Storage pressure still bounds the
reclaim wave. The policy grants no new lifecycle authority. New telemetry reasons
require gateway readers before worker writers.

The focused Linux policy/runtime gate passed 46 tests in 0.545 seconds, including
freshness, configured-unknown behavior, host-versus-backing constraints and
backing-pressure cache avoidance. This gate overlays the policy and capacity
types on rc13; it does not qualify the separate in-progress growth-admission
changes or establish the cause of the rc12 guest SIGBUS failures.

The implementation samples incarnation-fenced `memory.current` and `memory.stat` in maintenance before a guest reaches its first model wait. It never sums process RSS. The sampler checks the original PID/start time, exact OCI cgroup path and cgroup device/inode; unknown or stale samples supply no fictional full-limit reclaim credit. A restore invalidates the previous sample. Shared memory, dirty bytes and writeback are excluded from the clean-cache estimate.

A selected wait can attempt cache reclaim only for a real byte deficit and below the existing I/O-pressure threshold. The probe requests at most 256 MiB in 16 MiB kernel writes, once per relay request. It does not periodically trim caches merely because they exist. At least 16 MiB of measured eligible cache is required to justify the probe. In-flight projection uses the cache target instead of the full live footprint. Actual progress is measured afterward; completed projection is discarded and real `MemAvailable` controls subsequent admission/parking. Unsupported or ineffective reclaim falls through to the existing fenced checkpoint path. Reclaiming half a working set only to refault it triggers a 30-second cache-reclaim backoff for that incarnation; it does not block durable parking or foreground work.

The kernel may evict substantially more than a requested window. The request budget is not a promise about exact eviction volume. In the experiment one small request caused hundreds of MiB of cache eviction. This reinforces measuring achieved progress and refaults, and retaining memory whenever there is headroom.

No lifecycle lock is held over `memory.reclaim`. Every window rechecks the source journal/incarnation, cgroup identity, managed wait, attached activity and durable wake fence. A foreground response cancels subsequent windows; a write already in the kernel may finish after that response. Reclaim changes no execution or checkpoint authority and cannot make a sandbox portable.

The telemetry adds cache attempts, in-flight requests, measured reclaimed bytes and refault backoffs beside existing resident-wait/checkpoint counters. New readers accept old snapshots with zero defaults. Tests cover wake races, shared activity, short source locks, replaced cgroups, unsupported kernels, tmpfs, refault feedback, and continuing to durable parking when reclaim makes no progress.

The native comparison can be repeated with `qualify_split_checkpoint.py --memory-mb 2048 --resident-wait-only`, a conformance binary built with `-DMAPPING_BYTES=1610612736ULL`, and `--ram-active` for the RAM layout. `--compare-pause` retains the experimental ABBA comparison only in the qualifier; product code has no paused wait tier.
