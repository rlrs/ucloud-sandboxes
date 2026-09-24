# Afternoon gateway contention, 24 September 2026

Read-only production investigation at approximately 15:39–15:46 UTC, runtime
0.5.114rc27. No restart, configuration change, profiler attachment, or synthetic
traffic was applied.

## Evidence

Gateway load was 9–12 on four vCPUs, with CPU PSI some near 45%. A two-second
process snapshot showed the main gateway process at 123% CPU and relay at 44%;
the snapshot is directional, not a function-level CPU profile. Worker snapshots
showed 28–34 sandboxes each, 10–60% CPU, 67–71 GiB available RAM, and relatively
low memory and I/O pressure. Available RAM alone does not represent admission
headroom: workers also reported substantial admitted future memory growth.

Recent five-minute metrics estimated about 198 gateway HTTP requests/second and
20 create attempts/second, about 17 marked unsuccessful. These are attempts,
not distinct client failures. A bounded, sampled trace search returned 60 create
error traces: 59 placement_busy, one image_warmup_pending. This is not an unbiased
frequency estimate, but it identifies an active retry source. Create placement
waits up to 250 ms for the shared scheduling lock, then returns a retryable 503.
Creation currently resolves the image before reaching that lock.

Image-resolution p95 was approximately 7.68 seconds; create p95 9.18 seconds.
Histogram estimates include repeating attempts. The image resolver still reads
fleet inventory even when the gateway already owns the preferred published image
record. New images invalidate that shared inventory; misses query nodes serially.

A sampled worker park trace spent 60.06 seconds in runsc checkpoint and ended
with TimeoutExpired. Another sampled job-log request waited about 15 seconds at
the worker before returning 503. These do not prove the cause of every long wake.
Recorded wake coordination p95 remained 0.687 seconds over five minutes, but six
completions exceeded ten seconds and the maximum was 121.5 seconds. Guest-visible
latency and the mechanism behind those outliers remain unqualified.

## Implemented locally

* ImageStore now provides an indexed single-image read; ImageManager.get_image
  no longer loads and decodes the entire image table.
* Name resolution first checks the gateway's published image record, which
  already had precedence over worker copies in the existing selection policy.
  Known published names therefore avoid fleet discovery. Registry manifest and
  digest-protection checks still execute. Missing, unpublished or rejected local
  records use the existing discovery and incomplete-inventory behavior.
* Placement traces now separate route reading/indexing, heartbeat reading,
  candidate evaluation, scheduling-lock wait, and durable reservation time. This
  should distinguish expensive selection from reservation/writer contention
  without installing a live profiler or changing scheduling limits.

127 local image, gateway, cache and placement tests pass; Ruff and whitespace
checks pass. New tests cover direct published-name resolution without full-table
or fleet scans, replacement visibility, deletion/unpublished fallback, and digest
protection failures. Existing synthetic inventory tests now explicitly supply an
empty local image manager.

No production changes were made. These changes remove confirmed unnecessary
discovery work; they do not establish that scheduling contention or checkpoint
timeouts are solved. After qualification/deployment, compare placement phase
timings and retry rates before changing admission policy or adding resources.

## Follow-up: gateway CPU attribution and fleet rendering

Two short nonblocking py-spy samples were taken at 20 Hz for ten seconds each,
without restarting or modifying application services. The temporary sampler and
remote output files were removed afterward. Only GIL-owning stacks were selected;
these samples are directional and do not attribute all native/kernel CPU.

The gateway sample contained 102 successful samples (four sampling errors), with
24 stacks in request image resolution. The previously proposed indexed image
lookup therefore targets observed work, though no production reduction is claimed.
The other sampled child, PID 651434, was initially suspected to be the routing
writer but was actually the fleet reader. All 41 successful samples (12 errors)
were in its fleet-list render path. A prior two-second process snapshot measured
that child at 31% of one CPU core. Rendering repeatedly decoded all routing rows,
constructed full records and JSON-encoded the whole fleet.

Implemented incremental fleet rendering in the isolated reader. Every call still
reads current routing rows and heartbeats; only validation/rendering/encoding of
identical rows is reused. The comparison includes all raw route fields, the full
heartbeat and its clock-driven freshness. Changed/corrupt rows are revalidated;
deletions, process restarts and replaced database files retain their existing
behavior. Retained encodings have an 8 MiB budget, which does not limit responses
or fleet size. Removed rows are dropped immediately. There is no time-based
response cache and no scheduling-limit change.

A synthetic local benchmark with 320 routes, ten workers, 30 alternating
old/new iterations and approximately 1 KiB specs measured:

| Workload | Previous median CPU/read | Incremental median CPU/read | Reduction |
| --- | ---: | ---: | ---: |
| Unchanged rows | 11.87 ms | 4.40 ms | 62.9% |
| 10% of routes changed between reads | 14.33 ms | 6.12 ms | 57.3% |

Both versions include actual SQLite reads and heartbeat processing. The comparison
is against the previous full-render implementation, and output bytes match for
the initial snapshot. It is not a production throughput measurement. The replay
script and sampled stacks are in `docs/benchmarks/gateway-cpu-2026-09-24/`; run the
benchmark from the repo root with `PYTHONPATH=. .venv/bin/python
docs/benchmarks/gateway-cpu-2026-09-24/benchmark.py`.

228 image, gateway, fleet, routing and placement tests pass locally. New cache
tests cover exact reuse, replacement/spec/generation/state changes, heartbeat
changes/expiry, deletion, corruption and cache-budget fallback; existing child
restart/database replacement tests also pass. Ruff and whitespace checks pass.
All fixes remain local: live gateway load, placement retry rates and the separate
checkpoint timeout still require post-deployment verification.

### Additional local optimization: buffered metrics encoding

Retained gateway CPU samples also include the metrics append path. Buffered
metrics previously serialized the whole event, decoded it to detach caller-owned
containers, and serialized its data and whole event again in the writer. The
queue now retains immutable serialized data plus precomputed byte accounting;
the writer submits each existing batch with `executemany`. This removes the JSON
round trip and repeated data encoding while retaining the same compact queue
limits, truncation threshold, spaced storage byte accounting, event ordering,
and recovery/drop reporting. This does not change flush cadence or durability.

The reproducible `metrics-benchmark.py` under the gateway CPU benchmark directory
compares encoding work only, excluding SQLite and queue locks. Across synthetic
8/32/128-field nested events, median CPU per event fell 35.8%, 39.0%, and 41.6%.
These are component microbenchmark results, not overall gateway CPU savings;
production impact remains unmeasured. The change is local and not deployed.

### Native decoding for the fleet row envelope

The incremental renderer still parses the full SQLite JSON row envelope on every
fresh read. This path now uses `orjson` (locked to 3.11.9), with the standard JSON
decoder as a fallback for values the native decoder rejects. Its scope is only
the database-generated outer array of SQL strings and integers. Specs, resource
JSON and snapshot JSON remain strings inside that envelope and keep their
existing decoders and validation; public API JSON behavior is unchanged.

`json-decoder-benchmark.py` measures fresh database reads through response
rendering for 320 routes and ten workers, comparing identical incremental
renderers with standard versus native envelope decoding. Decoder selection is
outside the timed section. Median CPU per unchanged refresh fell from 4.13 ms to
2.96 ms (28.3%); with 10% of routes changed, it fell from 9.35 ms to 7.08 ms
(24.3%). These are local synthetic measurements, not gateway-wide savings or a
production latency qualification.

187 routing, fleet-reader, rendering and control-plane tests passed. Additional
tests cover exact 64-bit generations, Unicode, nested oversized integers and
non-finite values retained inside opaque JSON strings, empty fleets and decoder
fallback. Ruff and diff checks pass. The lock includes Linux x86-64 wheels for
Python 3.10 and 3.14; Linux runtime qualification remains pending. No production
changes were made.

### Failed run: placement contention and request starvation

At approximately 16:27 UTC the gateway was still running rc27; health responded
in 8 ms, 14 nodes were registered, active sandboxes were zero, relay pending
work was zero, and its database pool had no waiters. The workload had stopped;
this is not evidence that the gateway process itself died. A bounded Tempo
query of failed create spans over the preceding 30 minutes returned 100 traces,
all with outcome `placement_busy`. These are sampled failed HTTP attempts,
not 100 independently failed client provisions.

Creates previously abandoned the shared placement mutex after 250 ms, while
wakes/migrations used blocking acquisition of the same non-FIFO RLock. This
allows a create to repeatedly lose the race under sustained wake traffic,
repeating admission/image resolution on every client retry. The local fix uses
a reentrant FIFO mutex built on the existing FairCapacity queue and gives
already-admitted creates the existing admission wait budget (30 s by default).
Capacity accounting, cross-process file locking, and durable reservation fences
remain intact. Expired waits retain a retryable 503 and now include the explicit
`gateway_placement_busy` error code. This reduces normal contention retries; it
does not guarantee every request succeeds under sustained excess demand.

The current SDK already retries this structured 503 for a stable create ID until
the caller's create deadline (10 minutes by default). The actual runner's SDK
version and deadline are unavailable, so whether the observed terminal errors
were deadline exhaustion or different client behavior remains unconfirmed.
The empty provisioning error cannot be attributed from its text alone.

105 local admission/control-plane/wake tests passed, including 32 FIFO waiters,
no lock barging, nested acquisition, timeout cleanup, foreign release rejection,
and a create waiting through contention longer than the former 250 ms cutoff.
Production has not been modified or restarted, and this change has not yet been
qualified against a realistic Linux load.

### Verify the reported SDK retry behavior

A real local HTTP server now returns the exact reported placement-busy body and
production Retry-After/X-UCloud-Sandbox-Retryable headers. Both sync urllib and
async aiohttp clients made three identical create requests (two 503 responses,
then success). The same four original tests passed against the archived v0.4.27
SDK source, which the local Verifiers checkout pins. This does not establish the
version or effective deadline in the failed production runner.

The reproduction also demonstrates why the terminal message is ambiguous: if
there is insufficient deadline remaining for Retry-After, the SDK raises the
original server error, with no indication whether earlier retries occurred. The
local SDK change preserves status/body/headers and adds retry-budget exhaustion
and HTTP-attempt count to that message. Six exact-response/diagnostic tests,
eight image-poll tests, and 28 existing retry/timeout tests passed. No retry
policy was broadened and no SDK release or production deployment was performed.

### Reduce scoring work inside placement reservations

`_node_placement_state` previously normalized image references in two separate
route passes and scanned a worker's cached-image tuple for every creating route.
It now computes load/count/image projections in one pass, normalizes each
 distinct reference once, and checks each distinct in-flight image against a
single cached-image set. The helper with no remaining callers was removed.
All caches are local to that invocation: no stale placement or heartbeat state
is retained. Generation/inventory/device accounting remains unchanged.

The reproducible `placement-scoring-benchmark.py` compares the prior scorer with
the new scorer and asserts identical results. At 32 routes per worker and 256
cached images, CPU fell from 164 to 55 microseconds with one shared image, 169
to 71 microseconds with eight images, and 168 to 119 microseconds with 32 images
(29–66%). This excludes database reads/commits and lock wait; the saving is about
0.5–1.1 ms over ten workers, not a claimed solution to multi-minute starvation.
117 placement, control-plane and wake tests passed, with explicit coverage of
unknown caches, digest/tag aliases, migration reservations, state counts, and
per-distinct-image work. Production is unchanged.
