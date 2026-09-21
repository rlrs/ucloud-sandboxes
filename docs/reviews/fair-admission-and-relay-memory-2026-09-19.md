# Fair admission and relay memory, September 19

The user's 256-agent production run exposed starvation at gateway admission
and a separate native-memory accumulation in relay lifecycle HTTP requests.
[Production samples and heap evidence](../benchmarks/prod-256-admission-oom-2026-09-19.json)
cover the incident on server 0.5.48.

## Measured causes

The gateway's configured 16-request create semaphore also covered wakes,
file requests, and other sandbox commands. Admission was nonblocking, so a
waiting wake competed against new arrivals after every client retry.
Trace `acc8536335d13b830ed38b2e38584355` spent 55.151 seconds delivering a wake,
with repeated `gateway_startup_busy` responses; the successful node wake took
1.441 seconds. Other retained traces show the same pattern. Workers had free
memory and no publication queue; the earlier storage publication problem was
not the bottleneck in this run.

The relay was globally OOM-killed and restarted at 11:05:28 UTC. The host has
5,503 MiB of RAM and no swap; the killed relay's anonymous RSS was 4,149,060 KiB.
After the run, the replacement process still held approximately 3.5 GiB, while
its accounted in-flight plus completed payloads totalled approximately 5 MB.
A live CPython heap inspection (0.35 seconds, no payload contents recorded)
found 5,011 SSLContext objects and 5,009 urllib openers/handler sets.

Every `_post_bounded_json`/`_delete_bounded_json` call constructed a fresh
urllib opener, including an HTTPS handler and certificate store. Handler/opener
cycles retained native TLS allocations until cyclic collection, whose Python
object accounting did not reflect that native cost. Repeated lifecycle retries
therefore amplified both delay and memory consumption.

## Changes in 0.5.49–0.5.52

- Wakes, streamed downloads and small control requests bypass the gateway's
  cold-create queue. Large buffered requests reserve bytes in a separate
  256-MiB memory budget before reading their bodies.
- Cold creates queue in FIFO order instead of immediately rejecting and
  requiring a new race on every retry. Node cold starts/uploads, restores,
  and buffered reads have independent FIFO admission. A node restore no longer
  needs a cold-start permit as well as a restore permit.
- Waiting has a 30-second deadline and remains bounded by the HTTP server's
  existing request-thread capacity. Deadline expiry returns the existing
  explicitly retryable response before operation side effects. Capacity is
  released on completion, failure, and expired queue tickets.
- Control HTTP openers are reused per thread, with one shared verified TLS
  context per process. Redirect refusal, hostname/certificate verification,
  response-size bounds and request-specific authentication remain intact.

The initial production qualification also exposed a 100-sandbox validation cap on
capacity reservations, before any test sandbox was created. Version 0.5.50
removes this arbitrary cap: a reservation stores the requested demand, while
the autoscaler retains fleet and per-cycle provisioning budgets. The count is
bounded only by SQLite's signed 64-bit representation. The placement planner
batches identical requests and stops simulating nodes beyond the fleet ceiling,
so large demand does not cause work proportional to the requested count. Tests
exercise 256, 512 and one billion requested sandboxes against a six-node policy.

The physical concurrency and memory budgets remain bounded. This change
removes unrelated global contention and retry starvation; it does not claim
unlimited capacity or remove every resource safeguard.

## Validation

Regression tests exercise weighted FIFO ordering, no barging, timeout cleanup,
a 256-request queued burst including an injected failure, restore/read progress
while creation is blocked, gateway wake/health progress while creation queues,
and unread-body rejection only after an upload's admission deadline. A transport
test performs 8,000 lookups on four threads and verifies four reused openers
sharing one certificate-verified TLS context.

The 0.5.52 canonical check passed 920 server tests (six platform skips), 118 SDK
tests, Ruff, shell lint, Go tests and installed-wheel checks. The native build
passed 31 protocol tests and three ownership concurrency regressions. Two
compaction regressions could not run on the Linux 5.15 build host (`channel
closed` during io_uring setup); the exact test executable passed both on the
production kernel before deployment.

## Mixed production qualification

[Machine-readable attempts and heap evidence](../benchmarks/mixed-admission-256-2026-09-19.json)
record a 256-agent mixed create/park/wake workload, with four cycles per agent,
64 MiB of random resident memory, a 16-MiB writable file, 32-KiB model payloads,
and single-attempt response commits through the async SDK transport.

The 0.5.50 attempt exposed another backend bug rather than passing: 11 agents
failed, with nine relay workers reporting committed-response/wake failures.
All 492 gateway and 492 relay health probes passed. Relay RSS peaked around
150 MiB, and the post-run heap had three TLS contexts and 64 HTTP openers,
compared with 5,011 contexts and 5,009 openers before the transport fix.

Worker 12396460 reported two owner identities for `/dev/ublkb18`, referring to
different sandbox generations. Inventory correctly rejected this inconsistent
ownership, making storage metrics unavailable. Wakes then attempted publication
and relocation; trace `383fb07734fce7df0805fa8994fd0ff1` includes a 56.978-second
publication queue and 16.058-second export/upload of 517,210,112 bytes. This was
not evidence of saturated worker CPU or network throughput.

The AgentEnv owner-identity adapter updated its forward/reverse maps after an
awaited pool release. Another acquisition could reuse that device number before
the old completion removed ownership, deleting the new binding and retaining
the old one. Version 0.5.51 adds an atomic ownership index, conditional cleanup
against the captured owner, and per-owner acquisition/release fencing. It never
holds the registry lock across device I/O or serializes unrelated acquisitions.
The native binary SHA-256 is
`c8035bd0d1c1bc0c3a76bbf261f854d82490efb40336c30038e0b1e1b86cb501`.

Version 0.5.52 also shares the pinned native patch list between bundle packaging
and generated boot validation. The earlier 0.5.51 boot validator still expected
three patches and rejected the fourth ownership patch; no sandbox workload ran
on those rejected nodes. Both real sandbox/builder bundles passed the generated
validator on Ubuntu 26.04 and the production gVisor revision before replacement
workers started. All four replacement workers have the native SHA above.

The first 0.5.52 qualification was invalidated by the diagnostic setup. Its
root-run SQLite readers could race service-owned SQLite WAL/SHM creation;
uncaught `PermissionError` in gateway `_chmod_sqlite_state_files` terminated
HTTP handling. An upload failure at 12:24:36 UTC coincides with exactly this
exception; public ingress surfaced it as “Job is unavailable | UCloud”. Three
relay commits reported the gateway closing its connection without a response.
This is evidence of an origin-side failure, not evidence that UCloud ingress
was independently malfunctioning. The mixed driver and database-reading probes
were corrected to run as `ucloud`, matching the services. A proposed SDK retry
workaround was withdrawn; SDK 0.4.23 remains unchanged.

The failed diagnostic run cleaned up all its own sandboxes and reservation.
Its result remains in the evidence with `success: false`; it is not counted as
successful validation. The next harness revision ran as the service user but introduced a test
interference: polling result files implicitly woke callers while the mock
upstream waited to observe their parked state. The final driver waits for the
single response commit to finish before reading result files. This maintains
create/wake overlap without injecting unrelated wakes into each model wait.
Further inspection found the staged SDK package directories had mode 0700.
After switching to `ucloud`, Python silently fell back to the globally installed
SDK **0.4.18**, with its shared default 100-connection aiohttp pool. Pending model
requests accumulated while idle long polls held connections needed for forward
and response traffic. The harness's hard-coded 0.4.23 report field was therefore
incorrect in the two service-user attempts before this discovery. Staged package
permissions were corrected to directories 0755/files 0644; actual import version,
path and client/relay module hashes were verified against SDK commit 335e368.
The driver now asserts the imported version and path before creating resources.
These failed/interrupted diagnostic attempts are not capacity qualification.

After the fleet was empty, the configured fleet-wide create gate was disabled
(`gateway_max_concurrent_sandbox_creates: 0`). The existing worker admission
queues and resource checks, separate buffered-body budget and gateway HTTP
request budget remain enforced. This lets added workers contribute independent
startup capacity instead of sharing 16 in-flight creates across the whole fleet.
The gateway was restarted and health-checked; sandbox/relay state was preserved.
## Verified 256-agent result

The 12:40:07 UTC run imported SDK 0.4.23 from the verified package path and
completed all 256 scenarios with zero errors. Every agent performed four
model/park/wake/tool cycles: 1,024 upstream calls, 1,024 parks, 1,024 wakes, and
exactly one response-commit attempt per request. No model response remained
pending delivery. The workload completed in 102.895 seconds; cleanup and the
post-run observation brought total time to 118.877 seconds. No own sandboxes,
reservations, or relay registrations remained; unrelated user state was preserved.

Create latency was p50 29.098 s / p95 80.847 s. Full model/park/wake/tool cycle
latency was p50 4.427 s / p95 9.290 s / max 11.596 s. All 113 gateway and 113
relay health probes passed; gateway health p95 was 122 ms. Relay lifetime peak
RSS at completion was 293,884 KiB, with zero restarts since deployment.

Five-second worker samples included 841.6 MiB/s of disk writes on one worker,
with about 23% aggregate CPU busy and 4.3% I/O wait. These are utilization
observations, not a hardware-ceiling benchmark. The workload deliberately waits
for mock model replies and does little CPU work; it does not prove CPU or
network saturation. Removing the fleet-wide create gate allows worker capacity
to scale independently; physical maximum throughput remains unqualified.


## 512-agent result and count-ceiling removal

The first verified 512-agent run on 0.5.52 passed all 2,048 cycles, with one
commit attempt per request, zero worker/cleanup errors, and all 445 gateway
and 445 relay health checks passing. Total elapsed time including cleanup was
497.270 seconds. Create p95 was 350.982 s; cycle p95 18.197 s and max 83.159 s.

The three image-warm workers reached the configured ceiling of 128 active
storage owners each, despite roughly 80 GiB of available RAM per worker and
1,449,984 MiB of hard storage capacity. Subsequent wakes caused snapshot
publication/relocation to another worker. One publication queue reached four
active plus twelve waiting jobs. Thus passing the count was not sufficient:
this arbitrary ceiling was still adding avoidable delay and network work.

Version 0.5.53 (commit `89e70c3368d80e0eba5d3f10c50c09c07cdc4e9a`) makes
both fleet-wide create and per-worker storage-device count overrides optional,
with zero as their default. The native service already supported zero; provider
configuration now accepts it and validates pool watermarks only against positive
overrides. Disk reservations, physical memory/resource admission, lifecycle
fencing, and independently bounded startup/restore/publication queues remain.
Explicit positive operator overrides remain enforced.

All 921 server tests (six platform skips), 118 SDK tests, canonical lint/build
checks and CI run 35444021096 passed. Both 0.5.53 worker bundles passed real
production boot validation. Gateway and all five then-live workers were upgraded;
heartbeats confirmed version 0.5.53 and `storage_ublk_max_devices=0` on each.
The native backend binary is unchanged from the ownership fix. The relay was
not restarted for this configuration/code update. The SDK remains 0.4.23.


An eight-second diagnostic profile during the 0.5.53 run measured 761 heartbeat
decodes (1.062 thread-CPU seconds), 33,815 route decodes (0.887 thread-CPU seconds),
and 14 placement selections (1.657 wall seconds / 0.466 thread-CPU seconds).
These measurements overlap and must not be summed as independent phases.
Temporary timing wrappers were removed automatically at the end of the sample.
Repeated metadata decoding and scheduling work remains an optimization target;
this profile does not establish a fundamental Python/asyncio throughput limit.
## Verified 512-agent result on 0.5.53

The 12:56:26 UTC repeat (`mixed-admission-512-85e71c5a`) completed all 512
scenarios and 2,048 model/park/wake/tool cycles. Each response was committed
exactly once. There were no scenario, worker, cleanup, or health-check failures,
and no pending response deliveries. All 269 gateway and 269 relay health probes
passed; gateway health p95 was 247 ms. Cleanup auditing found zero test routes,
pending creates, reservations, or rollout registrations across all eight test
prefixes, while preserving the 77 unrelated pre-existing pending/leased requests.

Full cycle latency was p50 4.072 s / p95 5.482 s / max 8.380 s, compared with
p95 18.197 s / max 83.159 s before removing the storage count ceiling.
Create latency remained substantial: p50 124.910 s / p95 236.869 s / max
248.914 s. Total time including cleanup was 294.340 seconds. The repeat used
four warm workers; the preceding 512 run started with three warm workers and
added another, so the wall-time improvement is not an isolated causal benchmark.

One worker reached 141 active storage devices, exceeding the old 128 ceiling.
Sampled storage errors, publication activity, publication queue depth, snapshot
publication counts, and uploaded snapshot bytes were all zero throughout this
run. Relay lifetime peak RSS was 413,616 KiB, without a service restart.

The SDK recovered from 24,943 HTTP 503 responses during the mixed workload.
The instrumentation grouped these by status and did not retain endpoint/error
bodies, so this is **not** evidence of zero overload responses or a completed
backpressure redesign. The successful lifecycle result and lower wake latency
are qualified; startup retry amplification and the remaining multi-minute cold
burst latency still warrant optimization. This CPU-light mock-model workload
also does not establish maximum sustainable CPU, disk, or network throughput.

Production uses server 0.5.53, runtime commit `89e70c3`. The tested SDK is
unchanged 0.4.23; no new Verifiers change is required for these server fixes.
