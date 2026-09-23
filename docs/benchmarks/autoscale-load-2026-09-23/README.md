# Production autoscaling and density load tests — 23 September 2026

Release `0.5.114rc22`, SDK `0.4.26`, four-vCPU gateway with PostgreSQL,
production policy unchanged at zero to eight workers. These runs used the real
SDK, managed guest agents and model relay, with simulated model responses.
They are workload reproductions, not a replay of the user's application.

## Workload

`scripts/live_relay_load_benchmark.py` ran from the Linux `rasmus-dev` host.
Each sandbox had a 2 GiB memory limit, eight model/tool cycles, model waits of
20 seconds with five seconds of jitter, 64 files of 64 KiB, a 64 KiB tool upload,
and 100 ms of CPU work. The harness checks memory and file integrity, agent PID,
response delivery and useful execution. Parking was natural, not forced.
Creates used concurrency 16 and rolling startup. Request and continuation
timeouts remained 180 seconds. The first cycle per sandbox is excluded from
the headline latency quantiles.

The two 256 runs allocated 1,536 MiB of real resident memory per guest and
dirtied 384 MiB per cycle. The 512 run used 1,024 MiB resident and 256 MiB dirty
per guest. These are different memory footprints, not a same-workload doubling.
All used the same pinned Python image recorded in the JSON summaries.

## Findings

| Run | Completed cycles | Correct | p95 continuation | p95 useful execution |
| --- | ---: | --- | ---: | ---: |
| Cold/autoscaling 256 | 910 / 2,048 | No | 20.829 s | 29.681 s |
| Warm eight-worker 256 | 2,048 / 2,048 | Yes | 0.381 s | 0.802 s |
| Warm eight-worker 512, smaller heaps | 4,096 / 4,096 | Yes | 16.405 s | 21.757 s |

The cold run started with one ready worker. That worker received 76 guests
while six later workers received 30 each; the eighth worker arrived too late
to receive any. The first worker reached 86.71% I/O PSI and its continuation
p95 was 52.61 seconds; peers were 0.25–0.31 seconds. One agent start timed out
on that worker, causing the harness to cancel and clean up. Gateway health
p95 was 28 ms, with no failed gateway or fleet health checks. The overloaded
worker's kernel log showed no SIGBUS/OOM; this does not prove the precise
cause of the agent-start timeout.

Temporary scale-up latency is acceptable if placement balances and recovers.
The observed interval with all eight workers registered lasted 73 seconds
before the harness aborted on the agent-start timeout. At its end, the first
worker still reported 70 active sandboxes, six reported 30, and the eighth
reported zero. This did not demonstrate recovery within the request deadline;
it does not establish that eventual recovery would never happen. The harness
stops on its first correctness failure, so a future recovery test should retain
the failure in its result while separately measuring subsequent convergence.

Repeating the same workload with eight ready workers gave exactly 32 guests
per worker and passed both original latency gates (continuation p95 below
0.8 seconds and useful execution p95 below one second). There were no test or
cleanup errors. Sampled checkpoint counters did not increase on any worker:
this establishes fast resident waits, not subsecond disk-backed park/restore.

The 512 run assigned exactly 64 guests per worker and completed all cycles
with no execution, integrity or cleanup errors. It did not pass the latency
gates. Sampled checkpoint counters increased by 231 across the fleet, and peak
per-worker I/O PSI reached 69.63%. Whole-worker counters recorded about 278 GiB
read and 761 GiB written. The startup parking burst coincided with long delays
despite balanced placement. Gateway health checks all passed (p95 158 ms),
but the fleet monitor recorded ten stale-heartbeat observations and eight
resource-probe failures between 18:35:13 and 18:35:29 UTC; these were temporary
observations, not eighteen node losses.

Continuation p95 by cycle fell from 30.46 seconds in cycle two to 2.27 seconds
in cycle four and 0.62 seconds in the final cycle. Final-cycle useful execution
p95 was still 1.22 seconds. Completion is staggered, so the later cycles also
benefit from agents finishing: these results do not prove a sustained
subsecond plateau at 512 continuously active agents. The pressure/parking
transient is a separate optimization target from cold-fleet placement.

The cold result exposes a remaining placement problem: adding capacity did not
repair excessive assignments to the first owner within the observed window. A future
fix should coordinate pending assignments with booting capacity and queue
resource demand before saturating an owner. It should not introduce a fixed
sandbox-count ceiling or permanently reserve every parked sandbox's RAM limit.

## Evidence

The JSON summaries retain workload configuration, release/SDK/harness identity,
latency and correctness results, resource aggregates and source-report hashes.
The `*-analysis.json` files add placement and per-worker evidence. Raw reports
remain on the Linux driver under
`/home/alex-admin/ucloud-architecture-rc5-driver/candidate22-*.json` and locally
under `/private/tmp/architecture-candidate22-*.json`.
Whole-worker I/O counters are not per-sandbox attribution. These short runs do
not establish long-duration production reliability or fast wake-up for arbitrary
heaps at the physical RAM limit.

All test-owned sandboxes were removed. The final idle check reported zero
routes, zero inflight relay requests and zero pending deliveries. Gateway,
relay, autoscaler and registry services were active. No product code or
production policy was changed during these tests.
