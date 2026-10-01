# Read-only gateway incident findings — 2026-09-30

Local, untracked incident evidence. Do not publish these artifacts. Inspection covered 2026-09-29 23:11 UTC through approximately 2026-09-30 06:15 UTC; the workload was concentrated around 04:50–05:12 UTC.

The retained evidence supports CPU, storage and database contention during the run, with client-visible admission/create failures. It does not show a gateway crash, OOM, reboot or automatic upgrade. All four control services remained active with zero restarts since the planned 22:59:57–58 deployment. Docker, nginx and PostgreSQL remained on their existing processes. The kernel journal has no entries since 23:11. All five automatic-update units are masked/inactive. At 06:05, root storage was 15% used and the registry volume 28% used; available memory was about 12.4 GiB.

## Resource pressure

These are retained roughly ten-minute averages, not instantaneous peaks. CPU includes all host activity; process-level attribution was not retained. Disk percentage utilization is deliberately excluded because earlier qualification found those counters unreliable.

| UTC interval | Busy CPU / 4 cores | CPU / I/O PSI some | Registry read / write MiB/s | Registry await / queue | Root-disk await / queue |
| --- | ---: | ---: | ---: | ---: | ---: |
| 04:50:15–05:00:10 | 3.136 | 65.7% / 25.7% | 68.71 / 123.62 | 48.2 ms / 12.88 | 0.17 ms / 0.14 |
| 05:00:10–05:10:26 | 1.092 | 13.7% / 33.7% | 49.84 / 125.85 | 67.6 ms / 15.80 | 885.22 ms / 225.14 |
| 05:10:26–05:20:00 | 0.082 | 0.1% / 3.2% | 0 / 17.56 | 205.0 ms / 3.32 | 0.29 ms / 0 |

The first interval averaged 0.361 core in softirq and 125.8/201.8 MiB/s private receive/transmit traffic. All four cores averaged approximately 78–79% busy, so this was not solely one saturated core. The next interval averaged 20.76% iowait. Available memory remained at least about 12.1 GiB in these samples, with zero memory PSI. Retained interface error/drop rates were zero; that does not rule out every network failure. The exceptionally high root-disk await is retained as an observation, with no process-level cause assigned.

## HTTP and database evidence

- **6,176 POST `/v1/images/build` 503 responses**, 04:50:50–05:01:08. Build and import share this endpoint; the access log alone cannot distinguish them or establish unique operations.
- **32 POST `/v1/sandboxes` 504 responses**, 05:00:50–05:01:06, plus four 503 responses on that route. The default combined log has no request-duration, body, idempotency-key or create-ID field, so these are response counts, not unique failed jobs.
- At **05:01:08**, 129 POST sandbox requests and numerous event streams ended with nginx 499 client-disconnect status. Five gateway traces at 05:01:01–09 are `BrokenPipeError` while sending responses. The exact cause of the client disconnect boundary requires caller-side evidence.
- PostgreSQL logged **4,087 serialization aborts and one deadlock**, 04:50:50–05:04:16. Of these, 3,455 failing statements targeted `worker_capacity_revisions`; the deadlock at 05:00:44.514 involved deleting sandbox storage dependencies and updating sandbox activity. `shared_control/routing_repository.py` explicitly retries these SQLSTATEs with a bounded deadline, so the abort count is contention evidence, not proof of 4,088 failed jobs.
- Placement logged seven approximately 31-second-spaced import-submission timeouts at 05:08:23–05:11:28 for the same image, followed by seven broken-pipe response traces at 05:11:37–44. Parent investigation correlates these with the durable import/create history.
- Retained registry request-completion logs contain no 5xx responses in this interval. The slowest completed request was **57.45 seconds**. The 404 entries are manifest/blob-not-found responses, which can be normal cache/mount probes; they are not independently proof of missing required data.

## Maintenance and limits

Hourly registry pruning ran **05:03:22.801–05:04:27.662**, consuming 9.691 CPU seconds over 64.859 elapsed seconds and exiting successfully. Its shutdown emitted the existing `ConnectionPool.__del__`/`PythonFinalizationError` warning. This was after the observed create-504/disconnect cluster and cannot explain its onset; it may have added later load, which these records do not quantify. No registry GC ran during 04:45–05:15. The later 06:04 GC completed successfully. Journald suppression notices were recorded at 00:04 and 06:04, outside the failure window.

PostgreSQL's checkpoint ending 05:10:22 reported 703.117 seconds of checkpoint writing and 0.006 seconds of sync. Checkpoint writing includes deliberate pacing; that duration is not request-latency proof or, by itself, a failure or demonstrated cause.

Primary evidence: `host-findings.json`, `sysstat-sa30.json`, `cpu-network-retained.txt`, `database-routes-detail.json`, `http-db-evidence.json`, `system-evidence.json`, `maintenance-workload.json`, and the bounded local gateway/placement journal. No production mutations, service restarts, workloads or credential exports were performed.
