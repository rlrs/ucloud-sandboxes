# Whole-gateway capacity investigation, 2026-09-28

**Final production state:** CCX23, **four dedicated CPUs and 16 GiB RAM**, down
from eight and 32 GiB. The actual resized machine passed 512 real sandboxes /
6,144 cycles with simulated inference, p95 ready-to-usable latency **0.740 s**,
and simultaneous **124.66 MiB/s NAT, 132.88 MiB/s unique registry writes and
24.88 MiB/s reads**. These measurements include TLS, PostgreSQL, API/relay,
registry, kernel networking and the shared host. Provisioning now defaults to
the qualified shape. This passes the measured 512-agent workload; it does not
qualify 1,000 agents or provide comfortable burst headroom for that target.
All temporary test infrastructure has been removed.

The subsequent [relay/inventory optimization](../gateway-dispatch-2026-09-28/README.md)
was deployed at 18:47:27 UTC. Whole-host capacity measurements below predate
that patch; its functional canaries do not constitute a new capacity test.

The gateway hosts the public TLS ingress, sandbox API, placement executor,
autoscaler, model relay, PostgreSQL, image/checkpoint registry, and private-fleet
NAT. CPU sizing must include these services, kernel networking, storage and
memory pressure. A Python-process quota alone would not test this machine.

## Changes deployed

The production gateway and future worker/builder bundles now contain the
connection-local heartbeat-header cache and autoscaler snapshot projection that
omits unused exec history. nginx has minimum per-worker limits of 4,096
connections and 65,536 descriptors, plus a bounded relay upstream keepalive
pool. Its gateway upstream behavior, TLS material and routing are preserved.

The relay's production durable storage budget was raised from 8 GiB to the
existing 64 GiB application default, including the provisioning template.
The old budget admitted only approximately 253 simultaneous model requests:
each pending request reserves its possible 32 MiB response plus overhead.
The failed 512-agent run repeatedly reached that ceiling. Retries performed
database work that was subsequently rolled back, wasting capacity as well as
delaying admission. The per-request completion guarantee was preserved.

`deployment-receipt.json` identifies the installed wheel, node bundle directory,
nginx hashes, health checks and rollback paths. The earlier EROFS build reuse,
registry-pruning and unattended-upgrade fixes remain installed; their evidence
is in `../build-cache-2026-09-28/`.

## Workload and evidence boundaries

The harness runs 512 real managed sandbox processes with a three-CCX63 worker
cap. The final accepted run placed 256 agents on each of two workers; the third
worker remained idle. Only model inference is simulated. Each agent keeps 512 MiB of incompressible
resident memory, dirties 128 MiB per cycle, uses files and subprocess tools,
waits 20–25 seconds for a model response, and verifies identity, memory, files,
tool upload and exec results. There are 24 concurrent full-fleet inventory
pollers. SDK/model-worker traffic uses public HTTPS; guest relay requests use
the production private relay path. Natural model waits are used. Workers had
enough memory to retain these waits; this does not qualify forced checkpoint
restoration, arbitrary container memory sizes or all possible agent workloads.

The earlier runs used sixteen cycles; the final accepted run used twelve.
Both provide a sustained interval with all 512 processes active.
Measurements distinguish creation, the full active plateau, network-load
windows and completion. A separate private-only diagnostic node generates NAT
and registry traffic so its CPU is not charged to the gateway or sandbox hosts.
The first network fixture tests bounded transfers and warm registry reads;
its achieved rates, timeout and storage limitations are recorded explicitly in
`network-ingress.md`. CPU hotplug tests restrict the entire gateway to four
online CPUs, with independent timed restoration and HTTPS/NAT health checks.
This does not itself change the VM's billable type or its 32 GiB RAM allocation.

The sampler records process/cgroup CPU, host CPU and softirq, storage counters,
pressure, interface errors/drops, TCP retransmits/listen failures and conntrack.
Supplemental sar and qdisc samples include per-CPU saturation, memory and queue
pressure. Service CPU views can overlap and must not be summed indiscriminately;
the host total is authoritative. Profiles have exact windows and active-agent
guards. The earlier accidentally idle profile is marked invalid for loaded
hotpath attribution.

## Completed eight-core baseline

Run `relay-load-e2388974345d` completed **8,192 cycles and all 512 scenarios**
with zero errors, zero cleanup errors and zero guest relay retries. All 512
agents were active for 342 seconds. The relay sustained 449–498 sampled pending
requests, exceeding the old ceiling, with peak reservations of 18.99 GB against
the 64 GiB budget. Database pool acquisition errors were zero.

During the complete active plateau, whole-host CPU averaged 1.806 cores,
with p95 2.410 and maximum 2.900. Average service CPU was:

| Service | CPU cores |
| --- | ---: |
| Gateway API | 0.672 |
| PostgreSQL | 0.440 |
| Model relay | 0.396 |
| nginx, including TLS | 0.101 |
| Registry | 0.078 |
| Autoscaler | 0.026 |
| Placement | 0.011 |

The high network phase achieved 125 MiB/s NAT ingress, 75 MiB/s registry upload
and 75 MiB/s registry readback for 60 seconds while all 512 agents remained
active. Supplemental sampling measured mean 1.99, p95 2.21 and maximum 2.47
whole-host CPU cores; the hottest individual CPU reached 45.9%. No new public
or private qdisc drops occurred. Minimum available RAM was 27.7 GiB, with zero
swap, compaction or allocation stalls. This phase reproduces public NAT and
aggregate registry traffic, not the historical build upload/read ratio or a
cold registry cache.

The original harness's strict latency gate **did not pass**: useful-action p95
was 2.065 seconds. Its growing 23.4 MB report was serialized synchronously on
the load driver's event loop every second. A measured 18.7 MB checkpoint took
555 ms to encode, contaminating response scheduling and observed latency.
This baseline remains useful for correctness and sampled resource attribution;
it is not a passing latency acceptance result. Later acceptance must record the
serializer and its measured checkpoint cost without relaxing the latency gates.

## Rewrite assessment

The guarded loaded relay profile used 0.427 CPU cores on eight online CPUs and
0.433 on four. Most Python samples were in PostgreSQL protocol/transaction and
async I/O code; JSON and base64 were small fractions. A Rust rewrite is not
supported by this measured load. Single-statement PostgreSQL operations are a
possible later refinement, with an optimistic savings ceiling of about 0.07
core; changing multi-step transactional fencing would be unjustified.

See `relay-admission-budget.md`, `relay-loaded-profile.md`,
`eight-cpu-plateau.json`, and the supplemental/network artifacts for detailed
evidence. The four-core results below distinguish correctness, latency and physical resource pressure. These initial trials preceded the actual resize described below.


## Four-core whole-host trials

Both completed four-online-CPU runs exercised nginx/TLS, the API, relay,
PostgreSQL, registry and kernel networking together on the production machine.
All latency limits remained unchanged: p95 response-ready to usable uploaded
exec below one second, and observed guest continuation below 800 ms, including
creation-overlap phases.

| Run | Correct cycles / agents | CPU mean / p95 / max, cores | Ready-to-usable p95 | Strict gate |
| --- | ---: | ---: | ---: | --- |
| `optimized4-512` | 8,192 / 512 | 1.972 / 2.770 / 3.379 | 2.291 s | Failed |
| `acceptance4-512` | 5,120 / 512 | 2.280 / 3.100 / 3.530 | 1.034 s | Failed |

Each completed with zero scenario errors, cleanup errors and guest relay
retries, and passing fleet-health checks. CPU values cover only the interval
when all 512 agent scenarios were simultaneously active. Eight CPUs were
restored after each trial. The initial trial retained the slow report writer;
the second used orjson with p95 checkpoint time 39.4 ms. Neither is a passing
latency qualification.

The stronger second trial generated 425 distinct retained 32 MiB registry
blobs, rather than repeatedly overwriting one blob. During its 110.65-second
load phase, achieved traffic was 124.28 MiB/s NAT, 122.62 MiB/s registry writes,
and 24.83 MiB/s warm registry reads. Physical volume writes totaled 13.56 GiB
including setup, cleanup and a ten-second drain. The requested 150 MiB/s upload
rate was not achieved. Host CPU averaged 2.616 cores, p95 3.183, max 3.387;
registry-volume request latency averaged 17.78 ms and reached 36.52 ms.

Pressure counters matter alongside utilization: that mixed phase averaged
31.48% CPU some-stall, 22.01% I/O some-stall, 9.63% I/O full-stall, and 0.809%
memory some-stall. There were 28 compaction stalls and eight movable allocation
stalls. No new NIC, qdisc or softnet drops occurred. Six outbound fixture
requests timed out and recovered; the available counters do not localize those
forwarded TCP timeouts. These observations do not support claiming an absence
of contention solely from average CPU headroom.

## Driver isolation and final inventory optimization

The corrected report writer exposed another driver bottleneck: 24 synthetic
inventory clients decoded full fleet responses on the event loop scheduling
model completions and latency observations. A diagnostic whole-run GIL profile
attributed 80.12% of 10,030 samples to inventory polling, including 67.01% at
JSON decoding. This profiled run was not a capacity qualification: worker cold
starts meant all 512 scenarios were never simultaneously active.

The harness now keeps the placement observer in its main process and runs the
other 23 SDK inventory clients in a supervised child. All retain the original
GET-then-one-second cadence. Every poll contributes qualification evidence;
child failure or a failed poll still fails qualification. The child has a
deadline, parent-lifetime check, graceful shutdown and reaping. The 38 harness,
report-persistence and isolated-poller tests pass. Gates and sandbox workloads
are unchanged.

The gateway fleet renderer also no longer detaches and recursively copies full
heartbeat inventories for read-only rendering. It uses the existing validated
shared snapshot; complete inventory, reconciliation and external-write
invalidation remain intact. A synthetic 500-route, three-node benchmark
reduced CPU per read from 15.758 ms to 5.381 ms (65.85%), with byte-identical
responses. This is a path benchmark, not a predicted whole-host saving.
The 27 focused cache/renderer tests and 83 HTTP tests passed. This narrow change
was deployed at 14:11:10 UTC after an empty-fleet check, with source backup and
public gateway/relay HTTPS checks. See `fleet-heartbeat-sharing.md`.

Run `isolated4-512` (`relay-load-55d5cf062e75`) passed correctness, fleet health
and the unchanged latency gates: 6,144 correct cycles, all 512 scenarios,
zero scenario/cleanup/guest-relay/poll errors. Measured ready-to-usable p95 was
0.709783 seconds; the creation-overlap phase p95 was 0.769550 seconds. The full
512-active interval lasted 221.18 seconds (14:14:29.692–14:18:10.874 UTC).
Whole-host busy CPU averaged 2.161 cores, p95 3.076, max 3.315. Minimum available
RAM was 27.68 GiB; maximum MemTotal minus MemAvailable was 2.913 GiB. There were
no new qdisc drops, swap, compaction or allocation stalls. CPU some-stall
averaged 16.83%, I/O some-stall 7.72%, and memory some-stall 0.271%.

The 23 child clients completed 9,014 polls, alongside 384 foreground polls,
with all client IDs represented and zero errors. Achieved child rate was
20.382 polls/s, matching 20.390 predicted from measured request duration plus
the unchanged one-second sleep. There were 5,973 full-512-record responses;
the child exited zero and its PID was reaped. Driver loop lag p95 fell to
2.03 ms. This comparison includes the concurrent gateway optimization, so it
is not an isolated end-to-end A/B attribution.

The new combined network phase achieved 148.43 MiB/s distinct registry uploads,
24.85 MiB/s warm reads, and 51.49 MiB/s NAT. It stopped after 57.23 seconds when
the public download endpoint returned HTTP 429. Report this actual duration
and rate, not the requested 110 seconds and 125 MiB/s. All 512 agents stayed
active throughout the measured phase. Host CPU averaged 2.684 cores, p95 3.183,
max 3.315. CPU some-stall averaged 29.26%, I/O some-stall 26.09%; latency gates
still passed. Earlier 125 MiB/s NAT phases remain separate evidence.

Eight CPUs were restored at 14:19:57 UTC. This passing CPU hotplug run still
retained 32 GiB-class RAM and a warm boot; see `resize-readiness.md` for the
remaining actual-shape validation.


## Actual CCX23 validation

The same gateway server (167683409) was gracefully resized from CCX33
(8 dedicated CPUs / 32 GiB) to CCX23 (4 dedicated CPUs / 16 GiB), retaining
its 160 GB system disk, public/private addresses and 1,000 GB registry Volume.
Production was idle: zero sandboxes, in-flight relay requests or image builds.
A compressed PostgreSQL backup and deployment configuration were retained under
`/work/ucloud-sandboxes/gateway-resize-trial-20260928` before shutdown.
The controller preserved provider identity invariants and provided automatic
CCX33 rollback if boot checks failed. Boot validation passed at 14:28:34 UTC:
PostgreSQL, gateway, relay, placement, autoscaler, registry, nginx, private
connectivity, worker NAT, registry UUID and masked automatic-update units.
The reboot activated the previously installed kernel 7.0.0-34 (from -30) and
nginx now has four workers. This qualifies the actual resulting configuration;
it is not an isolated comparison of CPU count alone.

Run `actual4-512` (`relay-load-bac0a6754690`) then passed correctness, fleet
health and all unchanged latency gates, finishing at 14:36:47.657 UTC:

- 512 real managed sandbox processes, 6,144 correct cycles, and zero scenario
  or cleanup errors. Only model inference was simulated.
- Workers 167832403 and 167832409 hosted 256 agents each; worker 167832404
  remained idle. Each loaded worker peaked around 150.8 GiB used, with minimum
  available memory around 33.5 GiB. No checkpoint or snapshot-upload activity
  occurred during the full-agent plateau.
- Measured response-ready to usable uploaded exec p95 **0.740020 seconds**,
  p99 **0.852201 seconds**. The creation-overlap phase also passed at p95
  **0.757997 seconds**; it was not discarded as warmup.
- All 512 scenarios were simultaneously active from 14:31:33.282 until
  14:35:15.983 UTC, approximately 222.70 seconds.
- A private generator fetched synthetic HTTPS bytes through gateway NAT from
  a temporary owned source restricted to the gateway's public IP, with a pinned
  certificate. No private payload or signed access URL was sent to that source.
- The combined 110.298-second load achieved **124.662 MiB/s NAT**, **132.876
  MiB/s distinct registry uploads**, and **24.878 MiB/s warm registry reads**,
  with zero load or fixture-cleanup errors. Upload target 150 MiB/s was not fully
  achieved; the actual measured rate is the supported result.
- All 512 agents remained active for 110.192 seconds of this phase and every
  agent made progress (2,361 completed cycles). Only the final 0.106 seconds of
  traffic followed the first completed scenario.
- 459 distinct 32 MiB blobs (14.344 GiB) were retained through the load. The
  bracketed registry device wrote 14.642 GiB and read 166.94 MiB. Registry reads
  primarily accessed recently written data; this is not arbitrary cold-image
  or forced-checkpoint qualification.

Fresh workers 167832403, 167832404 and 167832409 were individually verified:
all five unattended/APT units masked and inactive, and all three periodic APT
settings zero. The provisioning policy and consistently rebuilt future-node
bundles preserve this behavior.


The final full-512 plateau's canonical one-second host counters measured mean
**2.347**, p95 **3.169**, maximum **3.860 CPU cores**. Service mean CPU was
API 0.722, PostgreSQL 0.604, relay 0.528, registry container 0.225, nginx/TLS
0.119, autoscaler 0.031 and placement 0.013; cgroup views can overlap and are
not a replacement for the host total. Conntrack peaked at 27,775 of 262,144
entries (10.60%). No NIC, qdisc, softnet or listen drops were observed.

During the full-512 mixed-traffic intersection, supplementary two-second sar
samples measured mean 2.810, p95 3.293 CPU cores. The hottest individual CPU
briefly reached 94.98%; there is burst contention, not unlimited spare capacity.
CPU some-stall averaged 32.56%, I/O some-stall 25.73% and memory some-stall
0.223%, while latency gates passed. Minimum available memory remained 12.11 GiB,
with no swap, compaction or allocation stalls. Registry device request latency
averaged 19.38 ms, p95 24.44 ms. Request and lifecycle relay PostgreSQL pools
reported no errors; aggregate wait per request was 0.00511 and 0.01051 ms.

The late upload-rate drop is not evidence of a raw upload ceiling. Fixture
manifest PUTs were serialized and referenced an increasing retained-blob list:
mean manifest request time rose from 112.7 to 214.6 ms, while mean blob-upload
PUT time fell from 377.4 to 335.9 ms. This identifies growing manifest work as
a contributor, without separating client serialization from registry metadata
and volume latency. See `network-ingress.md` for the measured phase breakdown.

The four-core shape is retained for this qualified workload. This result does
not establish 1,000-agent capacity, 150 MiB/s sustained uploads, arbitrary
cold-image read workloads, or forced checkpoint/restore performance. The
measured workload passed without a Rust rewrite; that does not resolve which
implementation is appropriate for greater capacity.

Both final samplers were stopped and archived. All temporary private generators,
the owned public HTTPS source, its firewall/public IP, and synthetic S3 fixtures
were deleted with provider/object-store readback. Owned registry links and
upload sessions were removed; unreferenced blob storage follows normal GC.
The PostgreSQL/config backup and software rollback artifacts remain on the
gateway. `actual-resize-events.jsonl` records the controlled resize and boot
checks; `inventory-package-deployment-receipt.json` records the installed wheel
and matching future-node bundle root.

## Next optimization priorities for 1,000 agents

API, PostgreSQL and relay together averaged approximately 1.85 CPU cores at
512 agents. A simple linear projection gives approximately 3.62 cores at
1,000, before registry, TLS and kernel networking. This is a warning about
headroom, not a measured 1,000-agent result.

1. **Reduce redundant lifecycle database scans.** The relay dispatch loop
   scans both wake and park queues on each global lifecycle hint, repeats after
   claims, and receives further hints on task completion. Locally signalled
   notifications can also return through the process's PostgreSQL listener.
   In the matched 220-second window, the pools recorded 775.13 acquisitions/s
   for 21.22 model completions/s, including the additional observer relay
   request and background operations. Acquisitions are not SQL statement
   counts; empty-claim counts were not recorded. Instrument those operations,
   use action-specific hints, suppress self echoes, and rescan on capacity
   release only when work was blocked by that capacity. Preserve durable
   reconciliation, lease fencing, wake priority and transaction guarantees.
2. **Reduce repeated full-fleet reads and rendering.** A full 512-record
   response was approximately 850 KB. Around 20 completed polls/s means
   approximately 17 MB/s of full JSON; unchanged polling at 1,000 agents would
   approach 33 MB/s if record size remains constant. Add compact projections
   and bounded ID filtering for clients that need only status, then a shared
   observer or durable change cursor for fleet watchers. Existing coalescing
   only joins overlapping reads within each HTTP process. Preserve deletion,
   generation, heartbeat-expiry and reconnect semantics.
3. **Separate bulk registry traffic through a direct private endpoint.** This
   can remove registry CPU (approximately 0.47 cores during mixed traffic),
   device contention and bulk transfers from the gateway. Keeping the gateway
   as a proxy retains its bandwidth burden. This isolates work rather than
   reducing total fleet CPU. TLS averaged only 0.119 cores; total host softirq
   averaged approximately 0.16 cores and includes more than NAT, so neither is
   the first CPU target in these measurements.

The earlier API GIL profile had only 89 samples during declining concurrency
and omitted fleet-reader children. Before selecting a native transport rewrite,
capture loaded profiles of all HTTP processes and their readers, per-route
work, and database operations. Relay readiness and delivery already batch;
exec event waits already support asynchronous long polling. Reimplementing
those existing features is not an optimization plan.

Qualification should progress through sustained 512, 750 and 1,000 agents with
the same correctness/latency checks and concurrent bulk traffic, adding lost
notifications, listener reconnect and claim-fencing failure tests for relay
changes. Two workers cannot retain 1,000 copies of the tested 512 MiB heap:
500 GiB of guest heap alone exceeds their configured 360 GiB usable capacity.
That density requires a smaller working set or explicitly qualified memory
reclaim/checkpoint/restore. EROFS image-layer sharing does not share these
private dirty heaps. No new optimization or production change was applied in
this follow-up assessment.
