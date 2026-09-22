# Native production relay qualification, 22 September 2026

These are real managed sandboxes on Linux workers, using the public gateway and
an isolated PostgreSQL relay namespace mounted alongside the existing SQLite
relay. They are not the earlier HTTP tests with a timed wake callback. The
production gateway/scheduler still uses SQLite; this does not qualify a full
shared control-plane migration.

The separate Linux driver runs the SDK and a model-worker stub. Each sandbox
holds 128 MiB of incompressible resident memory, modifies 16 MiB per round, writes
64 files of 64 KiB, and does 100 ms of CPU work before a model request. Model
readiness occurs after 10–15 seconds independently of parking. After delivery,
the guest checks all resident memory, checks a file, and starts a Python tool.
The driver then verifies the result through sandbox exec. The first round is
warmup; raw reports retain it. No artificial parking delay is excluded from the
response-ready latency measurement.

The requested target is p95 wake below 0.8 seconds. The harness also measures the
stricter response-ready-to-verified-tool latency, including the guest integrity
check, tool runtime, SDK/network delays, and the driver's exec probe. Neither
metric has passed the target in the completed runs below.

| Report | Agents × rounds | Measured rounds | p95 commit/wake | p95 response-ready/verified tool | Correct |
| --- | ---: | ---: | ---: | ---: | --- |
| pg4-01.json | 4 × 3 | 8 | 0.984 s | 1.609 s | yes |
| pg4-02.json | 4 × 4 | 12 | 0.947 s | 1.530 s | yes |
| pg64-01.json | 64 × 4 | 192 | 1.996 s | 2.858 s | yes |
| pg256-01.json | 256 × 5 | 1024 | 29.437 s | 35.731 s | yes |
| pg256-02.json | 256 × 5, aborted | 332 | 3.420 s | 4.694 s | no: ingress 503 |
| pg256-03.json | 256 × 5 | 1024 | 8.860 s | 11.321 s | yes |
| pg256-04.json | 256 × 5 | 1024 | 4.340 s | 6.753 s | yes |
| pg256-05.json | 256 × 5 | 1024 | 3.854 s | 4.897 s | yes |
| pg256-06.json | 256 × 5 | 1024 | 4.117 s | 6.291 s | yes |
| pg256-07.json | 256 × 5 | 1024 | 5.230 s | 7.962 s | yes |
| pg256-08.json | 256 × 5 | 1024 | 2.791 s | 8.095 s | yes |
| pg256-09.json | 256 × 5 | 1024 | 2.242 s | 3.224 s | yes |
| pg256-10.json | 256 × 5 | 1024 | 2.514 s | 3.616 s | yes |
| pg256-11.json | 256 × 5 | 1024 | 1.782 s | 3.067 s | yes |
| pg256-12.json | 256 × 5 | 1024 | 1.930 s | 2.778 s | yes |
| pg256-13.json | 256 × 5 | 1024 | 9.020 s | 10.481 s | yes |
| pg256-14.json | 256 × 5 | 1024 | 6.005 s | 7.415 s | yes |
| pg256-15.json | 256 × 5 | 1024 | 4.400 s | 5.893 s | yes |
| pg256-16.json | 256 × 5 | 1024 | 2.683 s | 3.759 s | yes |
| pg256-17.json | 256 × 5 | 1024 | 3.216 s | 7.166 s | yes |
| pg256-18.json | 256 × 5 | 1024 | 2.256 s | 3.369 s | yes |
| pg256-19.json | 256 × 5 | 1024 | 2.486 s | 5.706 s | yes |
| pg256-20.json | 256 × 5 | 1024 | 6.496 s | 8.339 s | yes |
| pg256-21.json | 256 × 5 | 1024 | 3.269 s | 3.923 s | yes |
| pg256-22.json | 256 × 5 | 1024 | 2.238 s | 2.947 s | yes |
| pg256-23.json | 256 × 5 | 1024 | 1.403 s | 1.991 s | yes |
| pg256-24.json | 256 × 5 | 1024 | 1.410 s | 1.991 s | yes |
| pg256-25.json | 256 × 5 | 1024 | 1.445 s | 2.011 s | yes |

The aborted run is not a passing result and its partial percentile is not a fair
before/after performance comparison. It stopped after a poll received the
UCloud public ingress's `Job is unavailable` HTML response. Ten cleanup requests
initially conflicted with still-running exec probes; a subsequent scoped cleanup
check confirmed the fleet empty.

## Findings and changes

- PostgreSQL terminal-caller reconciliation originally opened a transaction for
  every historical terminal sandbox each second. The production terminal map
  exposed a transaction storm that fresh-schema tests missed. It now selects
  outstanding callers in the relay deployment once, intersects that set with
  terminal history, and rechecks matching rows under lock. A real PostgreSQL
  regression test covers 10,000 unrelated historical losses and retained results.
- At 64 agents the worker wrote about 570 MiB/s while its CPUs were 75% idle.
  One wake trace spent 330 ms committing a small lifecycle journal record.
  At 256 agents placement was 200/56 across two workers while additional workers
  became ready. The busiest worker read 238 MiB/s and wrote 490 MiB/s, with
  substantially more pressure than its peers. Raw OS snapshots are included.
- Image-cache affinity filtered out other workers before pressure ranking.
  Placement now ranks pressure and pending creates first, using cache locality
  to break comparable choices. The subsequent run distributed 256 sandboxes
  as 49/48/44/43/36/36 across six workers. Boot timing also affects distribution;
  the placement change cannot make a worker usable before it is ready.
- `ip -n namespace` replaces repeated `ip netns exec namespace ip` commands.
  A native-worker experiment measured median command time dropping from 8.85 ms
  to 4.64 ms. Network preparation now overlaps storage preparation, with both
  joined before restore and before releasing lifecycle ownership on failure.
- Timed Python subprocess waits use polling sleeps. The candidate uses Linux
  pidfd exit notification, and anonymous seekable memory for command diagnostics
  instead of temporary filesystem inodes. Native Python 3.14 tests cover child
  exit, output, timeout reaping, inherited descriptors, and the fallback path.
- Driver poll failures now retry within the original polling deadline, recording
  retries. A potentially lost poll lease expires within that recovery window.
  Committed response retries retain the request identity and remain included in
  wake latency. Cleanup retries only transient transport errors and the specific
  active-exec conflict; it never replays exec or ignores generation conflicts.

The third 256-agent run observed parking in only 308 of 1024 measured rounds;
the fourth observed it in all 1024. Check each report's parking coverage when
comparing latency, since an already-running sandbox is cheaper to wake.

The fifth run's traces expose fleet placement lock waits up to 7.5 seconds,
followed by worker restores around 0.35 seconds. Gateway profiling also shows
roughly 45 ms average SQLite writer waits, multiplied across state transitions.
The candidate now coalesces queued local wakes into one placement accounting
pass and durable reservation commit. It retains pressure/device checks and the
existing migration path. Concurrency tests cover shared durable results,
duplicate requests, device capacity, stale owners, and transaction rollback.
Run 06 includes batching. Sampled admission waits fell to 0.06–0.43 seconds,
but restore-slot queues on workers reached 2–4 seconds. The full-run wake tail
did not improve, although the median fell from 2.60 to 1.86 seconds. Removing
a gateway bottleneck exposed worker contention; this is not a passing result.

Run 07 includes batched `ip` configuration and coalescing fresh host-firewall
checks among already-waiting requests. Native namespace validation preserved
MTU, addresses, routes, and link state; configuration median was 2.5 ms versus
9.6 ms for separate commands. The full run was still slow and placement remained
uneven (87/85/70/14). Its worker samples exposed repeated registry schema scans.

Run 08 adds worker registry connection reuse and schema-cookie validation, and
counts completed creates' assigned shape in placement ranking. All 1024 measured
cycles parked, with placement 72/72/56/56. A native 8-thread registry microbenchmark
performed 1600 reads in 49 ms versus 457 ms previously. Pool tests preserve FULL
synchronous durability, exclusive connection use, schema/metadata validation,
rollback, and rejection of a replaced database. Idle retention limits do not
limit concurrent requests. Wake improved, but the strict external tool check
remained slow; neither result meets the requested target.

Gateway profiling still showed roughly 125 ms average writer waits during this
run. Run 09 adds routing connection reuse, after 157 routing/control tests and
four focused pool tests passed on Linux. Writer waits dropped to about 12 ms
on average; full-run wake p95 was 2.242 seconds and verified-tool p95 3.224 seconds.
The median wake was 0.804 seconds, still not a passing tail result.

Direct inspection before the next experiment found **12 restore slots** on live
workers; the source code default of eight does not describe this deployment.
Runs 09 and 10 both used five workers. Run 10 temporarily used 24 slots;
its p95 worsened to 2.514 seconds despite balanced 54/53/52/49/48 placement.
The experiment was reverted to 12. Run 11 used a 1 ms Python thread switch
interval instead of 5 ms, with the same code and 12 slots. A sixth worker became
available. Run 12 restored 5 ms on the same six-worker fleet: wake p95 was about
8% higher, but verified-tool p95 and both p99 values were better. This does not
establish a reliable benefit from changing the interpreter setting; 5 ms remains
in effect. The immutable-rootfs lease optimization was NOT deployed for runs
10–12. It removes repeated Docker inspection from resume of an exact-digest,
validated mounted image while retaining the shared garbage-collection lease
and full recovery when its mount is absent. Fourteen image tests, including
identity rejection and concurrent GC fencing, and 113 related tests passed on
Linux before its qualification rollout.

Runs 13–15 expose why fleet size and parking coverage must accompany the tail
latency. Run 13 used fresh workers and the mounted-image lease, but placed
145/111 sandboxes on only two workers; just 363 measured cycles observed parking
before response readiness. Run 14 used the same two workers (143/113) at a 1 ms
thread interval: 380 cycles observed parking. A third worker became ready but
received no sandboxes in that run. Both workers were restored to 5 ms afterward.
These are correctness passes, not successful performance qualification.

Run 15 adds a fresh Linux statx mount-root query instead of starting mountpoint
processes for restore and park checks. The helper remains the fallback on missing
or failed statx support, and custom runners remain supported. A private mount
namespace test verified ordinary directories and same-filesystem bind mounts;
median query time was 0.0025 ms versus 1.18 ms for the helper on the idle gateway.
120 Linux mount/rootfs/warden/provisioner tests passed. Run 15 placed 95/88/73
sandboxes on three workers and observed parking in 847 measured cycles. Profiles
showed no mountpoint subprocesses and lower rootfs reconstruction times, but the
fleet change prevents attributing its entire tail improvement to that change.

The admission queue also broadcast every release/acquisition to every waiting
thread. The candidate now reserves capacity for eligible FIFO heads and signals
only their individual events. Timeout cancellation, interruption after grant,
weighted reservations and no-barging semantics remain covered. The repeatable
Linux queue benchmark (128 threads, eight slots, 2,560 admissions) reduced CPU
from 2.49–2.51 s to 0.127 s and elapsed time from 2.32–2.34 s to 0.692–0.696 s.
This isolated benchmark does not establish a wake latency improvement. Eighty-eight
Linux admission/wake/provisioning tests passed. Run 16 includes the targeted queue:
85/61/56/54 placements, all 1024 measured cycles observed parked. Run 17 repeats
that candidate, now on five workers (56/56/54/52/38), again with all measured
cycles observed parked. Its slower result is retained. Per-cycle wall-clock
markers were added for matching the measured tail to traces, without changing
the workload or excluding any latency.

Storage profiling still found repeated connection creation, with occasional
journal-transition delays exceeding a second. StorageNativeJournal now retains
up to 16 idle SQLite connections, without limiting active leases. FULL synchronous
commits, foreign keys, the existing writer fence and transition CAS remain;
read snapshots roll back before reuse, errors discard connections, and a fork
or replaced database fails closed. Fifty Linux storage/recovery/migration and
pool tests passed. Run 18 includes this pool on six workers (47/46/45/41/40/37),
with all 1024 measured cycles observed parked, no control retries and no cleanup
errors. Its median wake is 0.698 s, but p95 remains 2.256 s. Connection creation
almost disappears from the sampled storage hot path, and sampled journal
completion p95 values are 1.6–11 ms. Fleet changes and sampling windows prevent
attributing the full end-to-end difference to this one change.

Correlating run 17's measured cycles with native traces finds both slow worker
restores and gateway delays. One 3.17 s measured wake had a 0.22 s worker restore,
0.83 s gateway admission wait, and additional gateway work before forwarding
and after the worker response. A PostgreSQL relay alone has not solved these
worker and gateway paths.

Run 19 tested a 1 ms interpreter thread interval on both the gateway and six
workers. All 1024 measured cycles observed parked, but wake and verified-tool
p95 worsened. The gateway and every worker were restored to 5 ms; restore slots
remain 12. This is not being shipped as a tuning change. Its eight-second OS
sample showed the two-vCPU gateway about 90% busy while workers had substantial
CPU headroom. Gateway timing samples showed metrics append and exec projection
work contributing material waits.

A native Linux SQLite reproduction then found an incomplete metrics vacuum:
`execute(PRAGMA incremental_vacuum(N))` reclaimed only one of 2,250 free pages;
draining its result reclaimed all of them. The candidate drains progress rows
without retaining them. A regression test builds thousands of free pages and
checks that reclamation preserves the remaining ten valid events and meets the
physical size budget. All 21 metrics tests passed.

Exec-route reads already use durable rows, so the process cache was unused for
routing. Its write still took the fleet projection lock, delaying tool
acknowledgements behind inventory work. The candidate removes that cache and
retains the writer transaction and conflicting-session checks. All 146 routing,
control-plane and wake-batching tests passed, including a concurrent exec commit
while the fleet projection lock is held and a deletion through a second store.

Run 20 includes the metrics-vacuum and exec-cache changes. By then autoscaling
had reduced the fleet to two workers: placement was {'12398341': 128, '12398342': 128},
and 438 measured cycles observed parking before response readiness. The current
policy reported that the pool matched demand despite this wake tail. It must not
be compared directly with the earlier six-worker result. The sampled gateway
metrics append average fell to 5.9 ms and exec-result projection average to 27.6 ms;
these measurements confirm the targeted costs fell, not that the SLO passed.
SQLite writer admission still averaged about 89–96 ms for lifecycle/exec writes,
and worker proxy calls remained slow. More than one component constrains the tail.

The 0.8-second p95 wake target remains **unmet**. The controlled comparisons below isolate fleet size; a
cold-start/autoscaling qualification remains necessary. The latter shows
that current capacity accounting can satisfy declared resources without
satisfying wake latency. Restoring all interpreter intervals to 5 ms and keeping
12 restore slots avoids treating the unsuccessful temporary tuning as a fix.

## Controlled six-worker comparison

Runs 21 and 22 explicitly hold `policy.min_nodes=6` on the same six workers
(12398351–12398356). This temporary setting is a comparison control, not a change
to the normal capacity policy or a claim that six workers are necessary. A timed
rollback and explicit post-test restoration return the original minimum of zero.
The temporary configuration initially lost its service-user ownership during
replacement, causing autoscaler startup failures. Ownership was corrected and
six ready workers verified before either run began; no user sandbox was active.

Run 21 is the previous candidate with fixed fleet size: p95 wake 3.269 s.
Run 22 adds reuse of heartbeat database connections and inventory-free heartbeat
reads specifically for freshness and lifecycle epoch checks. These still read
the current durable row, validate its entire canonical payload, and expose
quarantine/reboot changes immediately. Omitted inventory is marked incomplete
so it cannot prove a sandbox absent. Full placement/inventory reads are unchanged.
Connections retain FULL durability, isolated transactions, fresh snapshots and
file/fork identity checks; the idle cache does not limit concurrent readers.

A native Linux benchmark (16 readers, 1600 reads, 128 inventory entries) reduced
wall time from 2.020 s to 0.300 s and CPU from 2.675 s to 0.485 s. Full-inventory
reads with just connection reuse took 1.560 s. The corresponding real-load run
improved wake p95 to 2.238 s, with all 1024 measured cycles observing parked and
no correctness/cleanup errors. The 128 relevant Linux tests passed (one new test
fixture initially used an unsupported constructor argument; its three-test group
passed after correction).

Run 22 sampled 6989 routing commits in 50 seconds. Commit time averaged 1.54 ms;
writer admission for lifecycle changes averaged roughly 94–97 ms, much larger
than the commit itself. Synchronous telemetry append averaged 31.9 ms and reached
652 ms. CPU spent in heartbeat reads fell from 7.95 s/11217 calls to
3.05 s/12269 calls in the respective 50-second windows. This identifies useful
reductions without attributing all end-to-end variation to any one function.

Run 23 buffers gateway telemetry outside request threads, coalescing
already queued events into writes. It bounds queued bytes and event count, reports
losses, preserves original timestamps and detached payloads, and drains on normal
server close. Abrupt process loss may lose queued telemetry. Ownership, lifecycle
state, relay results and admission are not buffered. A blocked or failed metrics
journal cannot stall wakes. All 119 buffered-metrics, metrics, gateway and wake
regression tests passed on Linux before rollout.

Runs 23 and 24 completed correctly with p95 wake 1.403 s and 1.410 s,
respectively, on the same six workers. Each measured all 1024 cycles parked.
Run 23 gateway telemetry enqueue averaged 0.081 ms (maximum 6.9 ms); sampled
routing writer admission averaged 17–18 ms, versus 94–97 ms previously. These
are materially better, but the requested 0.8-second p95 remains unmet.

Worker traces now expose native restore work in the tail. Run 23's 125 sampled
worker wake spans had median 0.447 s and p95 1.199 s (the trace sample includes
warmup, so these are not the full-run SLO percentiles). Run 24 phase profiles
show runsc subprocess p95 0.226–0.341 s across workers, storage mount p95
0.041–0.500 s, and process provenance checks with differing node tails.
Durability, pidfd/provenance validation and readiness checks remain intact.

Run 25 tested four simultaneous restores per worker instead of twelve, retaining
the same six-worker fleet and 5 ms interpreter interval. It completed correctly
with all 1024 measured cycles parked, but p95 wake was 1.445 s; it did not improve
the target. All six workers were restored to twelve slots afterward. A separate
cgroup-throttling sampling attempt found no matching live groups, so it supplies
no evidence for or against CPU-quota throttling.

## Deployment scope

Gateway job: `12379311`; deployment: `live-ucloud-20260824a`.
The qualification relay path is `/qualification-pg20260922`, backed by schema
`ucloud_shared_live_pg20260922`. Only `relay-load-*` registrations are accepted.
The original relay URLs and SQLite journal remain authoritative for normal users.
PostgreSQL 17 is local to the two-vCPU gateway for this experiment, with fsync and
synchronous commit enabled. This is not a production HA PostgreSQL deployment.

Candidate package version remains 0.5.74; use the bundle hashes/qualification
artifacts rather than the version string to identify these experimental builds.
Worker registry v4 requires the compatible candidate code after rollout; an old
worker binary must not be restored over that registry without a migration.

Latest staged bundle hashes (buffered gateway telemetry candidate):

- Sandbox: `1c5396483a948288c0099a7fca4dee6e5035d778e6bdc6b1c3abdc65db7e029b`
- Builder: `4b49206cbd5f640f28f8c804f5564934c3969386e2f33fc339708a71ef970c97`

Gateway and autoscaler use the latest qualification wheel. Existing workers
have the worker restore, mount-query, admission and storage-journal changes;
the last rollout changed gateway telemetry buffering. Existing workers did not
need a restart for the last two gateway-only changes. No benchmark is
intended to remain running after qualification, and the temporary capacity hold
is removed between the preparation handoff and measured runs.

Final readback confirmed gateway, relay and autoscaler active; zero live test
sandboxes; no pending deliveries in either relay; 2152 original relay
registrations preserved; no metrics-drop events in the preceding 15 minutes.
The temporary fixed-fleet setting and expiry timer were removed, `min_nodes=0`
was restored, and all six workers returned to twelve restore slots and 5 ms
thread intervals. No benchmark driver remains running. These cleanup and health
checks do not change the failed latency qualification.
