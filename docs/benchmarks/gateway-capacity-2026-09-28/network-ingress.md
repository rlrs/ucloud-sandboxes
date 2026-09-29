# Gateway networking and nginx ingress

Read-only production inspection found that this machine is also the private
fleet's NAT router and registry host. The retained 06:20:16–06:30:01 sample had
public RX of 132,403 KiB/s, private RX/TX of 151,733/151,231 KiB/s, and loopback
traffic of 89,253 KiB/s in each direction. `network-history.json` contains the
original sar output. These are one roughly ten-minute average, not peak rates.

Across eight CPUs, user + system + softirq time averaged **4.129 CPU cores**.
I/O wait averaged another 9.41% of host time; it is waiting, not consumed CPU.
Softirq alone accounted for 0.554 cores, concentrated on CPU 2 (19.92%) and
CPU 5 (24.51%). A four-core capacity claim must account for this network and
storage work as well as application processes.

Both public `eth0` and private `enp7s0` report one combined channel as both their
current setting and hardware maximum. RPS is disabled; GRO/GSO/TSO and checksum
offload are enabled. Enabling more hardware queues is therefore unavailable on
these devices. IRQ affinity permits all eight CPUs, although actual receive
processing concentrates on two. `irqbalance` is inactive. Changing RPS could
spread processing but may add CPU overhead; no saturation evidence justifies
changing it without a controlled comparison.

At inspection, conntrack held 2,295 of 262,144 available entries. No NIC receive
or transmit errors/drops, TCP listen overflows, backlog drops or receive-queue
drops were recorded. Private `fq_codel` did record 1,246 lifetime drops over
approximately 100 million transmitted packets and two million requeues. These
counters lack attribution to the production run; capture their deltas under
load before drawing conclusions. The NAT rule masquerades private
`10.42.0.0/24` traffic leaving `eth0`.

## Local ingress candidate

Production nginx 1.28.3 has eight workers selected by `worker_processes auto`,
768 connections per worker, and a soft descriptor limit of 1,024 per worker.
The hard descriptor limit is 524,288, sufficient for the proposed 65,536 soft
limit. A proxied active HTTP request ordinarily needs both a client and an
upstream socket. Reducing the number of workers would also reduce aggregate
connection admission if these limits remained unchanged.

The provisioning script now sets minimum values of 4,096 worker connections
and 65,536 worker descriptors. Its scoped edit preserves larger existing values
and unrelated configuration, creates rollback copies, validates the candidate
main configuration with the new site, and restores the previous site if
validation fails. It does not reload an invalid candidate.

The model relay uses aiohttp with a five-second idle connection timeout. nginx
previously opened a new backend TCP connection for every request. The candidate
caches at most eight idle relay connections per nginx worker, for one second
and at most 100 requests each. Only `/relay/` clears the upstream `Connection`
header. Raw URI mapping, authorization forwarding, proxy-authorization removal,
TLS settings, buffering and active-request timeouts are preserved.

The gateway itself deliberately closes connections because its bounded thread
pool holds a thread for the connection's lifetime. Its policy is unchanged.
Adding a gateway nginx cache alone would not make these sockets reusable.

## Qualification

Five unit tests cover scoped configuration edits, quoting/comments, preserving
higher limits, idempotence, backups and failure rollback. Bash syntax, Ruff and
diff checks pass. `scripts/qualify_nginx_relay_keepalive.py` additionally runs
the actual generated configuration through nginx 1.28 in disposable local
Docker containers and an aiohttp fixture. It verifies encoded rollout IDs and
query strings, distinct authorization/body values across reused connections,
error framing, idle expiration, server-requested connection closure, and an
aborted POST that is received exactly once and followed by a successful request.

The retained local qualification used one nginx worker and the new capacity
limits for both connection-reuse variants:

| Workload | Backend connections before | With relay cache |
| --- | ---: | ---: |
| 20 sequential POSTs | 20 | 1 |
| 64 clients, 256 requests | 256 | 167 |
| 512 clients, 2,048 requests | 2,048 | 1,792 |

For the 512-client batch, whole-local-host `ActiveOpens` fell from 2,496 to
2,240 and `PassiveOpens` from 2,499 to 2,243. Those counters also include frontend
connections and unrelated local traffic. Raw results are in
`nginx-relay-qualification.json`. Timing varied between runs; these results
establish connection reuse and correctness, not a production CPU or latency gain.

## Whole-host qualification before reducing CPUs

Compare the same 512 real-agent workload before and after the candidate, including
public HTTPS model workers, private relay requests, exec/event polling and
simultaneous registry/build traffic. Record per-process CPU separately from
per-CPU user/system/softirq/iowait, interface byte/packet rates, new TCP opens,
conntrack occupancy, softnet budget exhaustion, qdisc drops and health latency.
Use the existing correctness and latency gates, with an idle-fleet preflight.

A separate controlled private-NAT download phase should reproduce the observed
roughly 129 MiB/s public ingress with bounded concurrency and total bytes, using
an owned endpoint or an existing qualified build workload. Compare agent latency
while this traffic is active, not only after it finishes. Avoid changing NIC/RPS
settings during the initial comparison.

A real four-core gateway candidate is stronger evidence than limiting Python
processes to four CPUs: application-only limits leave networking interrupts,
PostgreSQL, nginx and storage work outside the constraint. A language rewrite
cannot eliminate NAT softirq or registry-volume I/O costs. Profile the remaining
application CPU after these narrower changes before choosing a rewrite boundary.

Ingress deployment status belongs in the parent task's deployment receipt.
The following network probe used a separately authorized temporary compute node
and isolated disposable fixtures; it changed no production application state.


## Concurrent production network probes on eight CPUs

A temporary private-only CCX23 node (server 167823398, 10.42.0.3) generated
HTTPS downloads through gateway NAT from an owned64 MiB S3 object. It also
uploaded/read one isolated32 MiB registry blob, repeatedly. Writes traverse the
registry upload/hash path but deduplicate retained content; reads are warm-cache.
The node is outside the autoscaler-managed fleet and has unattended APT upgrades
masked. Its identity and fixture keys are recorded for cleanup.

Both windows overlapped all 512 active real agents. Every agent progressed in
each window; first scenario completion was 13:32:41.150UTC.

| Window (UTC) | Duration | NAT MiB/s | Registry upload / read MiB/s | Completed agent cycles | Probe result |
| --- | ---: | ---: | ---: | ---: | --- |
|13:28:42.772–13:29:56.477|73.70s|101.90|68.60 /68.60|1,545|One HTTPS TimeoutError stopped the first profile early|
|13:31:27.499–13:32:27.537|60.04s|124.99|74.96 /74.96|1,206|No probe errors|

The first profile used four NAT streams and requested110 s; it stopped at67.42 s
after one HTTPS timeout, then drained outstanding operations. Its reported rates
include that drain. The second used eight NAT streams, requested 60 s, transferred
16.117 GiB below an 18 GiB cap, and used 33.3 generator CPU-seconds. Both removed
their own registry blob links successfully (HTTP 202). Logical unreferenced blob
data is left for the registry's normal garbage collection.

These rates reproduce the historical public ingress within about 3%. They do
not reproduce the exact historical write/read split: the high phase implies
roughly 75 MiB/s private ingress and 200 MiB/s private egress, compared with about
148 MiB/s each historically. Historical traffic may have had heavier registry
writes. Cold registry reads, image conversion, sustained volume flushing, and
a downsized host's smaller RAM require separate consideration. A four-CPU result
should be compared at the same achieved traffic and with the same agent gates.

Raw transfer results are in `mixed-network-eight-cpu.json`. Gateway CPU, I/O,
latency, and softirq conclusions use the independent parent-task host sampler.


The matched four-online-CPU trial repeated the two rate profiles. All 512 agents
remained active and every agent progressed in each window:

| Window (UTC) | Duration | NAT MiB/s | Registry upload / read MiB/s | Agent cycles | Probe result |
| --- | ---: | ---: | ---: | ---: | --- |
| 13:36:57.968–13:38:48.009 | 110.04 s | 124.56 | 74.97 / 74.97 | 2,311 | No probe errors |
| 13:38:56.635–13:39:57.280 | 60.64 s | 113.98 | 74.20 / 74.20 | 1,266 | Five HTTPS timeouts clustered around 13:39:37.44, retried within deadline |

The full 110-second four-CPU phase achieved higher sustained NAT throughput than
the first eight-CPU phase. The subsequent timeout burst's location is not known
from the client alone; correlate host network counters rather than attribute it
to the gateway or object store without evidence. Both registry cleanup calls
returned HTTP 202. Raw results are in `mixed-network-four-cpu.json`.


## Four-CPU acceptance with retained unique registry writes

An earlier acceptance phase ran 13:48:17.160424–13:50:07.810828 UTC with only
CPUs 0–3 online. It requested 125 MiB/s NAT, 150 MiB/s registry writes, and
25 MiB/s reads. Actual rates were **124.28 / 122.62 / 24.83 MiB/s** respectively
over 110.65 seconds. The upload target was not fully attained. The temporary
generator used 86.77 CPU-seconds (about 0.78 core); one sample showed 82% idle
across its four cores, without swap or I/O wait.

Each 32 MiB incompressible upload had a unique first 64 bytes and content digest.
One evolving OCI manifest retained every completed layer through the load,
including 425 distinct blobs totaling 13.281 GiB. Superseded manifest revisions
were removed after the new manifest was stored. Final cleanup removed all owned
manifest and blob links without errors; global orphan data is left for normal
registry garbage collection. Successful and failed-setup cleanup paths were
checked against a disposable local HTTP registry fixture before running.

The gateway's `/dev/sdb` counters recorded **13.562 GiB physically written** and
32 KiB read over the bracket covering setup, transfer, cleanup, and a final
10-second writeback observation. These are whole-volume counters and include
concurrent production activity. Dirty memory peaked at 50.5 MiB; final Writeback
was zero. This confirms sustained physical writes rather than only deferred
buffering. Registry reads still came from recently written, cached blobs.

All 512 agents were active throughout, every agent progressed, and 2,293 cycles
completed during the transfer. First agent completion was 13:50:46.629 UTC, after
the phase. Six HTTPS NAT timeouts clustered 15.372–15.377 seconds into the phase
and recovered within the deadline; no registry errors occurred. The client alone
does not identify the source of those timeouts. Agent correctness and latency
gates are reported independently in the parent qualification report.

Raw transfer data and timestamped block/memory observations are retained in
`strong-network-four-cpu.json`. The test reduced online CPU count on the original
host; it did not reduce installed RAM or change the registry volume.


The first temporary generator (167823398) and its isolated S3 fixture were
verified deleted at 13:56:08 UTC. Replacement 167826942 is prepared for continued
qualification. Its staged fixture endpoint is the credential-free public
`https://fsn1-speed.hetzner.com/100MB.bin`, which Hetzner documents for
[bandwidth testing](https://docs.hetzner.com/storage/object-storage/troubleshooting/bandwidth-latency-issues/).
A small urllib GET succeeded from the private node; curl HEAD checks returned an
SSL EOF, so readiness was checked with the same GET client as the probe. NAT-source changes must
be considered when comparing timeout counts across subsequent runs.


## Actual CCX23 gateway with an owned HTTPS source

The gateway was subsequently resized to CCX23 with four dedicated CPUs and
16 GiB RAM. A credential-free external speed-test endpoint had stopped a prior
57-second phase with HTTP 429. The final fixture instead used a temporary CCX13
HTTPS source in the same project, serving only random synthetic bytes. Its cloud
firewall admitted TCP 22/443 only from the gateway public IP. The TLS private key
stayed on the source; the private generator pinned its public certificate. This
exercised the gateway's public-to-private NAT path without third-party rate limits.

The combined phase ran **14:33:25.791280–14:35:16.089512 UTC**. Requested NAT,
unique registry upload, and registry read rates were 125/150/25 MiB/s. Achieved
rates were **124.662/132.876/24.878 MiB/s** over 110.298 seconds, with zero transfer
or cleanup errors. The upload target was not fully attained. Four concurrent
writers retained 459 distinct 32 MiB blobs, totaling 14.344 GiB, until the phase
ended. No global cache flush or filesystem cache dropping was used; reads
selected recently written fixture blobs.

All 512 agents remained active for 110.192 seconds of the phase, completing 2,361
cycles with every agent progressing. First scenario completion was
14:35:15.983237 UTC; only the final 0.106 seconds of transfer occurred afterward.
Whole-volume `/dev/sdb` counters over setup, transfer, cleanup, and a 10-second
writeback observation recorded **14.642 GiB written and 166.94 MiB read**. These
counters include any concurrent production volume activity. Dirty memory peaked
at 115.53 MiB, final Writeback was 4 KiB, and available memory never fell below
12.124 GiB in the two-second wrapper samples. CPUs 0–3 were online throughout.

The upload rate reached approximately 145–150 MiB/s during the first 70 seconds,
then fell to 102–120 MiB/s in the final four ten-second intervals. Pacing used
cumulative bytes and one time origin per writer, so transaction overhead accrued
catch-up credit rather than adding a fixed sleep after each body. The fixture
also serialized each growing retention-manifest PUT and old-revision DELETE.
Registry logs show manifest PUT mean latency rising from 112.7 ms in the first
70 seconds to 214.6 ms in the last 40 seconds (p95: 289.7 to 453.5 ms). Those
last 140 serialized PUTs consumed about 30 seconds. Meanwhile blob-upload PUT
mean latency fell from 377.4 to 335.9 ms. Growing manifest transaction work
therefore contributed to the achieved upload rate; underlying metadata or volume
latency may explain its cost. This fixture does not establish a raw upload
throughput ceiling. No additional experiment was run to separate those causes.

The completed test repository was verified to have zero remaining blob/manifest
links and zero upload sessions. Global unreferenced blob data remains for normal
registry GC. Source server 167832212, firewall 11696212, and its auto-deleting IPv4
152128468 were verified absent at 14:39:11 UTC. Generator 167826942 and its unused
synthetic S3 fixture were verified absent at 14:39:21 UTC. The earlier generator
167823398 and its object had already been removed at 13:56:08 UTC. No temporary
qualification compute or object-store fixture remains.

Exact transfer, overlap, timestamped disk/memory observations, pacing bins,
registry timing summaries, and resource cleanup records are retained in
`strong-network-actual-ccx23.json`. Independent whole-host samples and the agent
correctness/latency acceptance gate determine whether the downsizing is accepted.
