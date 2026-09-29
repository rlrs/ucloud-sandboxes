# Relay admission capacity, 2026-09-28

The failed 512-agent run reached the production relay's durable reservation
limit. This was a configured admission ceiling; the available evidence does
not establish gateway CPU exhaustion.

## Evidence

- Run: `relay-load-60a053fb26f2`; rolling startup, 512 agents, 16 intended
  cycles, 20–25 second synthetic model waits, 32 KiB model payloads.
- All agents had started at 13:04:22.506 UTC. The run failed at 13:06:04.452
  and completed cleanup at 13:06:17.336. No successful 512-agent qualification
  or four-CPU qualification resulted from this run.
- The production configuration's `relay_postgres.storage_budget_bytes` was
  **8,589,934,592 bytes (8 GiB)**, with 16 PostgreSQL pool connections.
- `shared_control/relay.py` reserves the input payload and metadata plus
  `MAX_WORKER_RESPONSE_BYTES + 65536` before accepting a request.
  `MAX_WORKER_RESPONSE_BYTES` is 32 MiB. Exhaustion rolls back admission and
  returns HTTP 429 with `Retry-After: 1`.
- A sweep of retained request `created_at` / `completed_at` events reached
  **253 simultaneous pending requests** repeatedly: 13:04:05.660225,
  13:04:11.707691, 13:04:12.747257 and 13:04:24.517787 UTC. Both the exact
  synthetic prefix and the aggregate retained timeline had the same peak.
- The response reservations alone for 253 requests are **8,505,851,904 bytes**,
  leaving only 84,082,688 bytes of the 8 GiB budget for inputs, retained
  completed responses and compaction delay. Completed requests have their
  `payload_bytes` set to zero, so this reconstruction deliberately excludes
  the original input bytes and is a lower bound.
- The exact failed rollout `relay-load-60a053fb26f2-0288` registered at
  13:04:05.520147 UTC, but had no retained request or lifecycle rows. Its
  synthetic managed process exited after approximately 120 HTTP retries on
  its first model call. The report truncated the traceback before the HTTP
  status, so **the exact final status is not retained**. Persistent 429
  admission failures are consistent with these observations, but that exact
  status is not independently proven for this individual process.
- There were 1,579 model requests and 1,141 observation requests retained for
  this run. The shared observation rollout competes for the same byte budget;
  there is no separate per-rollout request-count quota.
- Sampled gateway network counters from 13:04:00.334 through 13:06:19.346 UTC
  had zero listen overflows/drops, zero softnet drops and zero NIC errors or
  drops. TCP retransmissions increased by 3,120 against 2,627,401 outbound
  segments (about 0.119%). Conntrack reached 70,423 of 262,144 entries at the
  last sample. The relay listener backlog is 128, but these counters provide
  no evidence that this listener limit caused the failure.

## Budget sizing

Even 512 model requests with these small inputs require approximately
16.05 GiB of logical reservations. A 16 GiB budget cannot admit all of them.
Conservatively allowing 512 active requests plus 512 completed requests whose
reservations have not yet been compacted requires approximately 32.1 GiB,
before retained responses. This also covers model/observation turnover in the
benchmark. The existing application default of **64 GiB** gives room for this
workload and retention without changing the 32 MiB maximum response guarantee.
It is a workload-specific sizing recommendation, not a promise to accept any
number of maximum-size inputs and retained results.

At inspection, PostgreSQL and `/work` shared a 160,921,083,904-byte root
filesystem with **148,866,015,232 bytes available** (about 138.6 GiB). The
entire PostgreSQL database occupied 49,895,103 bytes. The current logical relay
reservation was 263,116,203 bytes after cleanup/compaction. Raising the logical
budget does not itself allocate the reserved bytes on disk.

No production configuration or services were changed while collecting this
evidence. Only numeric configuration, aggregate database metadata, exact
synthetic rollout metadata and retained kernel counters were read.
