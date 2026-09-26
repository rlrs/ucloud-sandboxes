# Exec latency under concurrency (0.5.114rc49, SDK 0.4.31)

The rc48 load test on Hetzner ([disk-density-2026-09-26](../disk-density-2026-09-26/README.md))
had an exec start p50 of about 6 s under 100–150 concurrent callers. This
note records the root cause, the fixes and the rerun.

**Setup:** Hetzner gateway CPX32 (3 gateway processes) and one CCX63 worker
booted from the pinned-kernel snapshot. Probes ran either on the gateway
over loopback HTTP or from a laptop over the public HTTPS endpoint.

## Where the time went (rc48)

`exec_timings.py` posts raw exec starts through the gateway and aggregates
the node's per-phase timings, which are returned in the start response. With
150 concurrent starts across 50 sandboxes (3 per sandbox):

| | rc48 | rc49 |
|---|---:|---|
| rejected with 503 "sandbox lifecycle is busy" | 48 of 150 | 0 |
| node start wall p50 | 1,075 ms | 838 ms |
| node start CPU p50 | 19 ms | 9.9 ms |
| lifecycle phase wall p50 | 708 ms | 287 ms |

Start time was waiting, not CPU. There were three causes:

1. **Per-sandbox try-lock.** `DirectLifecycle.acquire_shared` took the
   sandbox's lifecycle lock without blocking. Two commands for the same
   sandbox arriving together meant one was rejected as retryable, and the
   SDK (and the load benchmark's client) retried it after `Retry-After: 1`.
   Under load that retry chain was most of the 6 s.
2. **Registry connection churn.** A py-spy `--idle` profile of the node agent
   put about 70% of exec-start thread time in SQLite registry reads.
   - The pool kept 16 idle connections for about 150 concurrent borrowers, so
     most reads opened a new connection.
   - Each new connection ran full schema validation: about 40% of the time.
   - Each exec reads its registration four times.
3. **Session table scan.** Once 1,024 sessions were retained, every start
   sorted all of them to find an evictable one: 16% of GIL samples.

The node agent's soft descriptor limit was also 1024 under systemd, with
about 320 in use at idle. Each concurrent exec holds three pipes.

## Fixes

**rc49** (commit `904ba9a`):
- concurrent commands queue for the lifecycle lock within the same bounded
  wait as the transition fence;
- the registry keeps 64 idle connections, and new ones start from the last
  validated schema stamp. Every statement still compares the live stamp and
  validates on mismatch;
- terminal sessions are kept in update order and evicted from the front;
- the node agent raises its soft descriptor limit toward 65536, and the unit
  sets `LimitNOFILE=65536`.

**SDK 0.4.31:** the synchronous client reuses HTTP/1.1 connections and builds
its TLS context once. It used to open a new TCP and TLS connection per
request.

## Results

**`exec_probe.py`**, a trivial command (`printf ready`), in ms:

| | rc48 | rc49 |
|---|---:|---:|
| loopback, sequential p50 | 35 | 36 |
| loopback, 50 concurrent p50 | 387 | 356 |
| loopback, 150 concurrent p50 | 1,350 | 849 |
| laptop, sequential p50 (SDK 0.4.30 → 0.4.31) | 196 | 80 |
| laptop, 100 concurrent p50 | — | 963 |

With SDK 0.4.31, client CPU for 50 concurrent execs from the laptop fell
from 17.3 s to 0.56 s. The sequential p95 (about 195 ms) is the first
request, which still pays the TLS handshake.

**`hetzner-load150-rc49.json`** (`scripts/live_load_benchmark.py
--sandboxes 150`, same run as the rc48 load test):

| | rc48 | rc49 |
|---|---:|---:|
| cpu_io round, 150 execs | 15.8 s | 1.24 s |
| cpu_io exec start p50 / p95 | 6,067 / 6,158 ms | 550 / 840 ms |
| cpu_io end to end p50 | 11,389 ms | 664 ms |
| light end to end p50 | 9,135 ms | 766 ms |
| ramp step 50 → 150 | 8.6 s | 4.6 s |
| builds, end to end | 104–123 s | 90–97 s |
| builds, docker build and push | 37–55 s | 21–28 s |

These changes do not touch the build path. The build difference is probably
run-to-run variance in package mirrors and Docker Hub.

**Remaining exec ceiling.** A 150-command burst on one node is now bound by
about 10 ms of node-agent CPU per start: the GIL admits about 115 starts per
second.

## Builds

On the rc48 run, three concurrent builds each took about 105–125 s end to
end:
- **Builder cold boot:** about 65 s (Hetzner create, boot, init).
- **Build and push:** 21–55 s.

Network paths measured on a CCX33 builder (`regprobe.sh`):

| Path | Result |
|---|---|
| Public download through the gateway NAT | 276 MB/s (hel1 speed test); not a bottleneck |
| `docker pull node:22` (1.13 GB unpacked) from Docker Hub | 36.6 s, including extraction |
| `docker push` of the same image to the gateway registry | 46.3 s; docker recompresses each layer |
| raw 300 MB blob upload to the registry (S3-backed distribution) | 9.7 s (about 32 MB/s over a single stream), plus a 3.4 s commit |
| raw blob download from the registry | 80 MB/s |

The main build costs are builder boot, Docker Hub pulls and layer
recompression. Our NAT and registry are not.
