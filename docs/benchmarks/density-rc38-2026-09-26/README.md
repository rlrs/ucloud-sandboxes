# Density ramp on UCloud, rc38 (2026-09-26)

One rolling run of `scripts/live_relay_load_benchmark.py` against the production
gateway (0.5.114rc38) with the fleet temporarily capped at three sandbox workers
(`cpu-amd-zen5-32-vcpu`, 96 GiB, 2 TB disk). The driver ran off-gateway over the
public gateway and relay URLs. Policy was restored afterwards (max 10 nodes, 300 s
idle scale-down); all three workers were stopped by the autoscaler within four
minutes of the run ending.

Workload per sandbox (rolling-512 shape): 1 vCPU, 1024 MiB, 4096 MiB disk
(parkable, so 7232 MiB reserved), 128 MiB random resident memory, 16 MiB dirtied
per round, 64 × 64 KiB files, 100 ms CPU, 10–15 s synthetic model wait, natural
parking. 540 sandboxes, create concurrency 32, one warm-up and two measured cycles.

## Result

All 540 scenarios and 1,620 cycles completed with integrity checks; 0 errors,
0 cleanup errors, 3 control retries. Placement ended exactly 180/180/180.

End-to-end latency (model response ready → verified SDK exec), binned by fleet
residents when the cycle completed:

| Residents per node | Phase | n | p50 | p95 | p99 |
|---|---|---|---|---|---|
| 30–59 | ramp | 11 | 0.57 s | 0.62 s | 0.62 s |
| 60–89 | ramp | 128 | 1.32 s | 3.18 s | 3.39 s |
| 90–119 | ramp | 172 | 2.05 s | 3.44 s | 3.88 s |
| 120–149 | ramp | 347 | 6.09 s | 9.53 s | 11.35 s |
| 150–179 | ramp | 781 | 1.59 s | 27.17 s | 40.13 s |
| 180 (all created) | steady | 181 | 0.73 s | 0.97 s | 1.71 s |

Whole-run: p50 2.01 s, p95 22.1 s, p99 38.5 s, so the harness's 1 s p95 SLO
failed. Response acceptance stayed fast throughout (p95 0.18 s).

Worker heartbeats (`node-heartbeats.jsonl`) show the nodes were never the
bottleneck: CPU ≤ 24 %, 59–82 GB memory free, memory PSI 0, IO PSI ≤ 2.4 (one
10 s sample of 9.9 after cleanup), storage operation queue empty. Almost nothing
parked during the run; this measures resident density. The only resource near its
limit was the hard disk reservation: 89.8 % of 1,449,984 MB at 180 per node,
consistent with the ~200-per-node ceiling for unpublished parkable sandboxes.

The slow ramp coincides with the create burst and with gateway event-loop lag:
the gateway's `node-http-io` loop (worker proxying, event long-polls and the
driver's observation traffic) reached p99 lag of 0.23–0.83 s between 22:19:00 and
22:20:00 UTC, while the placement and relay loops stayed below 0.1 s. Once creates
stopped, 180 sandboxes per node met the SLO. Gateway process CPU was not recorded,
so this attribution is circumstantial.

## Files

- `density540-rc38.summary.json` — benchmark report without per-event lists.
- `density540-rc38.json.gz` — full report.
- `driver-events.log.gz` — driver event stream (creation, placement job, cycles).
- `node-heartbeats.jsonl` — per-worker heartbeat samples from the gateway's
  control state (read-only), from 22:19:40 UTC; the early ramp was not sampled.

## Follow-up: rc39 (growth forecast fix)

Traces from the rc38 run showed every slow cycle waiting in relay response
delivery, inside the worker's `sandbox.wake.growth_admission`: continuations were
refused with "node memory headroom is reserved by in-flight transitions" while
nodes had most memory free. rc39 (`49c1149`) credits growth observations for up
to 30 s and forecasts a continuation's physical growth from its cgroup
`memory.peak`, keeping the full bound for launches and for the RAM-backing
(tmpfs) projection.

The identical rerun (`*-rc39*` files) did not remove the tail: whole-run p95 21.5 s
(rc38 22.1 s), p50 5.96 s (rc38 2.01 s). Traces show the same growth-admission
waits, now refused with "node RAM backing headroom is reserved by in-flight
transitions". The unswappable tmpfs projection, which still charges each active
sandbox its full 1 GiB bound, became the binding limit at roughly the same number
of concurrently active sandboxes. Nodes again stayed below 67 % CPU, with 57–78 GB
memory free and no memory PSI. The p50 difference is not explained; the gateway
exec segment was also slower in this run (post-continuation exec p50 2.8 s vs
0.9 s) with no gateway code change.

## Follow-up: rc40 (peak forecast for RAM backing too)

rc40 (`4b81cbf`) forecasts a continuation's RAM-backing growth from `memory.peak`
as well (accepted risk: SIGBUS if a guest outgrows its previous peak while RAM
backing is nearly full; launches keep the full bound). Identical rerun
(`*-rc40*` files):

| | rc38 | rc39 | rc40 |
|---|---|---|---|
| response ready → usable exec p50 | 2.01 s | 5.96 s | 6.17 s |
| p95 | 22.1 s | 21.5 s | 9.25 s |
| p99 | 38.5 s | 34.7 s | 10.3 s |
| max | 53.3 s | 47.2 s | 12.0 s |
| relay delivery (commit → guest) p95 | 16.8 s | 15.3 s | 4.7 s |

Growth admission no longer refuses anything; in sampled slow wakes the worker
answers in ~50 ms. The remaining latency is gateway-bound: slow wakes spend ~2.1 s
in the gateway, 1.3 s of it waiting for that 50 ms worker reply (proxy-loop
queueing) and 0.3 s in the routing transaction; the follow-up exec adds ~0.5 s of
gateway time plus ~1 s of worker exec start. With worker admission no longer
throttling continuations, more concurrent work reaches the single-process gateway,
which is the likely reason the median rose while the tail collapsed.

## Gateway host profile and rc41

A py-spy GIL profile (rc40) and per-service cgroup CPU sampling showed the 4-vCPU
gateway host saturated during the create burst (3.2–3.8 cores per 2 s interval):
PostgreSQL 0.7–0.95, gateway 0.55–0.93, placement 0.67–0.88, relay 0.45–0.61, and
docker-proxy relaying every PostgreSQL byte ~0.3. The telemetry stack was ~0.03.
The largest gateway costs were PostgreSQL round trips (~36 %), a `stat` before every
routed query (~7 %) and repeated heartbeat reads; the placement worker spent ~18 %
re-decoding every route of its candidate node on each create.

rc41 (`c97d4be`) rate-limits file identity checks to once a second, runs
single-statement reads as one autocommit statement, opens transactions with one
`BEGIN ISOLATION LEVEL` statement, reuses decoded routes for unchanged rows on the
placement node read, and reuses the warm-check route on warm wakes. The relay
PostgreSQL container was recreated on the host network (same image, data and DSN),
removing docker-proxy. Identical rerun (`*-rc41*`, `host-cpu-rc41.txt`):

| | rc40 | rc41 |
|---|---|---|
| response ready → usable exec p50 | 6.17 s | 5.48 s |
| p95 | 9.25 s | 7.12 s |
| p99 | 10.3 s | 8.2 s |
| max | 12.0 s | 9.9 s |

Peak host CPU fell to ~3.0 cores (max interval 3.35): docker-proxy 0.29 → 0.02,
placement 0.6–0.9 → 0.23. The public gateway process now uses ~1.14 cores, the
practical ceiling of one CPython process, so it is the next bottleneck.
