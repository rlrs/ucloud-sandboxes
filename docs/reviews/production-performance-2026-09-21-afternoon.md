# Production performance investigation — September 21, 2026, afternoon

Read-only inspection of DFM Pretraining (`4827bd3a-4e74-4393-9b82-49f71636c141`),
starting at 13:45 UTC (15:45 Copenhagen). Gateway and all nine fresh worker/builder
heartbeats reported release 0.5.70. No services were restarted, resource policies
changed, sandboxes created, or provider jobs stopped for this investigation.
The existing local CLI session was refreshed to access diagnostics.

## Findings

The workload is progressing, but park/wake coordination and storage work are
expensive. Adding worker CPU alone would not address the observed bottlenecks.
There is also significant CPU contention on the two-vCPU gateway VM.

The 13:46 UTC observability report covers 15 minutes of operation counts and
five minutes of recent histogram rates. Approximate latency quantiles:

| Operation | Median | p95 |
| --- | ---: | ---: |
| Relay park notification | 20.1 s | 53.9 s |
| Relay wake notification | 35.5 s | 58.2 s |
| Worker park | 3.3 s | 9.9 s |
| Worker wake | 4.2 s | 10.6 s |
| Model-worker wait | 14.1 s | 43.4 s |
| Snapshot publication | 15.4 s | 76.5 s |
| Snapshot compaction and upload | 40.0 s | 102.0 s |

These are fleet histogram estimates, not paired per-request measurements. Relay
spans include lifecycle coordination, lock waits and retries; they do not measure
only checkpoint/restore execution. Model and lifecycle work can overlap, so these
columns must not be added into an estimated total request latency.

### Publication loses races with local wake, leaving long local chains

Worker `12397962` had 51 journal operations with the error
`publication superseded by local lifecycle operation`. Its volume inventory
included chains of 25, 27 and 29 unpublished sealed layers. Only one volume in
that inventory had any published layers. Deleted volumes are also retained in
the journal and were included in the raw state counts.

Trace `7ab43126f4df3c89158d5f42caf30710` shows a 63.3-second publication uploading
3,100,180,480 bytes, followed by a storage publication conflict. The associated
sandbox successfully woke on its existing worker. Another trace,
`1eaf2e3d32e99c31190658e31369eed7`, compacted 16 local layers with approximately
4.58 GB of estimated input and uploaded 934 MB, also ending in a publication
conflict. Both began with zero existing published layers.

The code explains how this happens: blocked local wake can request background
publication to enable migration. Local wake is allowed to supersede publication
when capacity becomes available. The publication gate checks ownership before
export starts, but the export/upload itself runs to completion; the final journal
commit still requires the original revision. This preserves lifecycle safety but
can spend substantial I/O on a result that is not attached to the current volume.

The 0.5.70 optimization retains an *already published* dominant base. It cannot
help a chain with no published base. Publication-triggered compaction also does
not bound the local chain during repeated local park/wake cycles that never
successfully publish. This is the clearest next storage optimization target.

### Worker storage and reclaim dominate despite available CPU and memory

An 8.3-second sample of worker `12397962` measured physical `vda` throughput of
approximately 235 MiB/s reads and 692 MiB/s writes. CPU was approximately 75% idle.
The native storage backend accounted for about 229 MiB/s reads and 418 MiB/s
writes in process accounting; Docker was also writing about 73 MiB/s. Process
I/O accounting is not additive with disk counters or between parents and exited
children, so physical throughput is calculated separately from `/proc/diskstats`.

The same sample reclaimed roughly 13.4 GiB of file pages, recorded 136 direct
compaction stalls (127 unsuccessful), and showed XFS log-space waits and page
waits. Memory PSI full was 8.4%, despite about 79 GiB reported available memory.
A later zone snapshot showed fragmented free memory with no order-10 blocks in
the Normal zone. This establishes reclaim/fragmentation activity, not its exact
allocation caller or a provider failure. The sampled 24 sandbox cgroups had no
memory-limit or OOM events. Host OOM counters did not increase during the sample.

A later sample on worker `12397961` also showed storage-heavy work: approximately
435 MiB/s physical writes, file-page reclaim, and XFS/page-fault waits. Its native
backend wrote about 343 MiB/s. The workers' filesystems had substantial free disk
space; disk capacity exhaustion is not the explanation for these stalls.

### Gateway CPU contention adds latency

The gateway VM has two vCPUs and about 5.4 GiB RAM. CPU idle was roughly 7% in the
first eight-second sample and 20% in a second sample. CPU PSI some was 57–71%.
The gateway process consumed 0.81 CPU cores in the first sample and 1.34 in the
second. Registry uploads and telemetry competed for CPU in the first sample;
the gateway remained the largest consumer in the second sample when registry
uploads had subsided. The first sample overlapped the diagnostic trace query,
so its telemetry CPU is not an undisturbed baseline.

Local and public health probes initially took 0.99 and 1.07 seconds. Ten later
local probes ranged from 15 to 600 ms. The gateway processed about 75 successful
HTTP requests/s in the five-minute metrics window and wrote approximately
5.3–5.5 MiB/s in both host samples. Function-level CPU profiling was not completed;
the available Python environment did not provide the sampling-profiler module.
These measurements do not establish that Python or asyncio itself is the cause.

### Spare workers are not immediately useful to local-only parked state

The first snapshot had six sandbox workers and three builders. The two oldest
sandbox workers held 80 of the 106 inventory entries. Newer workers were much
less occupied. Most sampled wake events still targeted the two old workers, and
shadow placement recorded `route_not_portable` for some sandboxes.

The autoscaler had reached its configured six-worker maximum, but there was
already substantial spare capacity elsewhere. Increasing that maximum alone
would not make local-only checkpoints movable. Publication and placement must
be improved together. The single retained capacity reservation was for 97
sandboxes; this is not evidence of a 256- or 512-sandbox qualification run.

## Final observation

At 13:50 UTC the gateway, relay and autoscaler were still active, all six sandbox
workers had fresh heartbeats, and the pending-create table was empty. Health
latency had improved to 43 ms locally and 67 ms publicly. Worker `12397962` still
had severe I/O pressure: PSI some reached 74.6%, with 46 running and three
restoring sandboxes. The problem fluctuates with workload and remains visible
even when the public health endpoint is fast.

## Priorities

1. Avoid wasting large exports when local wake supersedes publication. Explore
   cancelable exports and safe reuse of an immutable published prefix without
   weakening generation/revision fencing or falsely marking current state durable.
2. Bound and compact local sealed-layer chains independently of successful remote
   publication. Measure checkpoint bytes, local export cost and foreground wake
   latency under the actual agent workload.
3. Make parking decisions sensitive to expected wait time and lifecycle cost.
   Immediate parking during short model waits can create more storage work than
   the reclaimed capacity is worth. Keep response-ready work ahead of maintenance.
4. Give the control plane CPU headroom and profile gateway request/state-store
   work. Separate registry traffic where appropriate; increasing worker count does
   not remove this shared gateway bottleneck.

Reported operation errors include retries and background conflicts, not just
terminal client failures. For example, the inspected 41.6-second relay wake trace
contained a 503 attempt followed by a successful wake. The aggregate error counts
must not be interpreted as failed-rollout counts. One generation-fenced program
projection conflict was recorded; this inspection did not establish it as a
throughput bottleneck.

[Structured evidence](../benchmarks/production-performance-2026-09-21-afternoon.json)
contains host counters, initial heartbeat snapshots, latency estimates and the
selected trace summaries. It contains no prompts, API tokens or command output
from user sandboxes. Measurements are observations of a changing live workload,
not a controlled before/after benchmark.


## Implemented follow-up: publication cancellation and registry hot paths

The first optimization pass is implemented on `codex/checkpoint-publication-efficiency`.
It has not been deployed or measured against the live workload.

Publication ownership now propagates through the backend router, dense export,
compacted export, upload and metadata commit. Stream reads check ownership at
bounded intervals, including when an exporter has connected but stops producing
output. Upload boundaries also check ownership. A superseded publication closes
its Unix stream and aborts the incomplete Registry upload or S3 multipart upload.
It does not remove source layers, publish a new volume revision, or interfere with
an independently successful blob. In-flight network calls and backend shutdown
still retain their existing timeouts; cancellation does not forcibly kill threads.
The final journal revision check remains authoritative.

Gateway registry bookkeeping now touches only the requested image/lease. Updates
no longer decode the whole registry under a write transaction. Registry health
checks use a read-only schema/generation availability check, without running
expiry cleanup or requesting the maintenance writer lock. Full snapshots and
pruning still validate all rows and remove expired leases. Expired target leases
cannot be renewed, and digest immutability and generation fencing remain intact.

In an isolated local SQLite fixture with 5,000 permanent leases and 500 images,
three alternating before/after rounds of 20 operations produced:

| Component | Before, median per operation | After | Ratio |
| --- | ---: | ---: | ---: |
| Registry health check | 33.36 ms | 0.185 ms | 180× |
| Image touch | 33.13 ms | 0.717 ms | 46× |
| Lease update | 33.26 ms | 0.695 ms | 48× |

This measures local component overhead, not full production latency or a claim
about production registry cardinality. The baseline is merge commit `4175036`.
[Raw timings and fixture parameters](../benchmarks/registry-usage-hotpaths-2026-09-21.json).

The full Python suite passed 1,008 tests with six environment-dependent skips.
Ruff and `git diff --check` passed. New regressions stream real Unix-socket data
through both publishers, cancel dense and compacted exports, verify partial-upload
cleanup and preserved source files, and check cancellation of an idle stream.
Registry tests cover writer-lock independence of health reads, missing/corrupt
schemas, scoped mutations, expiry, and immutable digests. The optional Docker
Distribution contract additionally verifies that an aborted upload cannot be
finalized; Docker is unavailable locally, so that integration check awaits CI.

Independent local-chain compaction and cost-aware parking remain subsequent
work. This pass prevents stale publication work from consuming the full export
and removes a confirmed linear-cost registry path; it does not yet bound every
unpublished local layer chain or change parking policy.

## Additional hot-path fixes

The gateway image-protection helper still requested a complete registry snapshot
after the first store-level changes. It now reads only the requested lease using
the primary key, without requesting a writer transaction. Existing permanent
references and sufficiently long transient leases remain write-free; renewal,
expiry, digest immutability and maintenance generation fences are preserved.

Single-sandbox and pending-demand reads no longer acquire the routing writer
mutex. SQLite WAL supplies committed rows while heartbeat reconciliation writes;
mutations continue to validate their identities in transactions. A concurrency
regression holds both the mutex and an uncommitted database write, confirms that
readers return the previous committed rows, and then verifies visibility after
commit. Deleting an absent route uses one pending-demand lookup instead of loading
all routes, sessions and pending entries.

Snapshot streaming now receives directly into one reusable buffer, hashes filled
chunks, and copies once into immutable bytes for the upload consumer. This avoids
the receive/append/slice copies and preserves independently retained S3 chunks.
Local upload time is excluded from the subsequent socket idle deadline.

The follow-up local benchmark compares the complete gateway helper and Unix
stream consumer against `4175036`, using three alternating rounds:

| Operation | Before | After |
| --- | ---: | ---: |
| Existing image reference, no touch | 33.48 ms/call | 0.204 ms/call |
| Existing image reference with usage touch | 69.11 ms/call | 0.964 ms/call |
| Stream 128 MiB, process CPU time | 201 ms | 180 ms |
| Stream 128 MiB, elapsed time | 195 ms | 176 ms |
| Stream Python allocation peak, measured separately | 40.12 MiB | 32.01 MiB |

The registry fixture has 5,000 permanent leases and 500 images. Streaming uses
8 MiB chunks and retains two upload chunks; it excludes native compaction,
storage and network latency. Allocation peaks are Python tracemalloc figures,
not total RSS. These results do not establish production speedups.
[Raw measurements](../benchmarks/gateway-storage-hotpaths-2026-09-21.json) and
[reproducible benchmark](../../scripts/benchmark_gateway_storage_hotpaths.py).

Validation after this pass: 1,012 Python tests, six environment-dependent skips;
Ruff and diff whitespace checks passed. Additional regressions cover gateway
lease renewal/expiry, immutable digests, read-only identity lookup with a writer,
and retained stream chunks across buffer reuse and a partial final chunk.
These changes remain local and have not been deployed.

## Avoiding unnecessary lifecycle work

Further inspection found a relay queue race: the accepted-request handler checks
whether a model result is ready before invoking its park notifier, but the
notifier can then wait for a park dispatch slot. A response committed during that
wait previously still allowed a checkpoint, and its wake waited behind the park
lifecycle lock. A durable-response event now releases queued parks immediately
and interrupts park retry backoff. The event is signaled only after response
commit succeeds; failed commits do not skip parking. Already-dispatched parks
still finish before wake so a late checkpoint cannot park an already-woken
caller. Trace events report dispatch queue seconds and reasons for skipped parks.

Wake placement previously started publication whenever local capacity was
blocked and a checkpoint was not portable, without checking whether the fleet
had a usable destination. It now checks destination eligibility first. If none
exists, the sandbox remains parked and its pending demand remains visible to
the autoscaler. Subsequent attempts can wake locally or start publication after
destination capacity appears. This is an eligibility probe, not a reservation;
the destination is checked again when migration actually begins. Capacity can
still change during a long upload, and already-running idle-park publication is
not canceled by this new probe.

The probe exposed another existing issue: migration source capability lookup
used the destination-ready worker list. Closing admission or draining a source
therefore incorrectly disabled offloading from it. Source capabilities can now
come from its fresh owner heartbeat, even when that source cannot accept new
work. Closed/draining destinations remain ineligible, and local wake now also
honors the explicit admission gate. No provider lifecycle or node-loss policy
was changed.

The wake scheduling critical section now queries the active migration by sandbox
using the existing partial unique index, and reuses that result instead of
decoding the fleet's active migrations twice for one sandbox.

Validation: 1,019 Python tests, six environment-dependent skips; Ruff and diff
whitespace checks passed. Regressions cover queued and retrying parks, simultaneous
response/admission completion, already-dispatched park fencing, durable commit
failure, full-fleet demand preservation, newly available destinations, and closed
source versus destination admission. These changes have not been deployed and
their production latency impact is not yet measured. They reduce avoidable
checkpoint and publication work. The next pass implements independent local
compaction to reduce long unpublished layer chains; see the
[implementation and native qualification report](local-checkpoint-compaction-2026-09-21.md).
