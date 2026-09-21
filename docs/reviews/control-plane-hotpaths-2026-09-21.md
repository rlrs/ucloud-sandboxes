# Gateway and relay performance changes — September 21, 2026

These are working-tree changes following the production health investigation,
not a deployed release or a new 512-agent qualification. They require no SDK
or Verifiers protocol changes and apply to the shared UCloud/Hetzner services.

The production sample showed approximately 50-second median relay wake latency
and 116-second p95, versus 4.6-second median worker restore. The two-vCPU gateway
had substantial CPU scheduling pressure. The busiest worker also showed heavy
storage I/O and memory reclaim; several individual 1-GiB sandbox cgroups hit
memory limits while host MemAvailable remained substantial. This patch targets
control-plane overhead and retry queueing. It does not resolve or quantify the
worker-side I/O/reclaim component, increase sandbox memory limits, or establish
that all production delay comes from the mechanisms below.

## Changes

The relay previously held one of its 48 wake dispatch slots, and its executor
thread, throughout retry backoff. Enough requests blocked on one worker could
therefore prevent ready workers from receiving wakes. Dispatch now reserves a
slot for one HTTP attempt, releases it before asynchronous backoff, and rejoins
the queue afterward. Parks retain their separate lane. The original wake
deadline includes dispatch queue time; retries retain the same operation ID,
generation and request body. Cancellation cannot abandon an accepted operation
or release a slot while its HTTP call still runs. Shutdown also waits for
operations in backoff. Permanent errors and unclassified failures retain their
existing handling. The physical HTTP concurrency budgets remain unchanged.

Heartbeat reads still query SQLite every time, but reuse decoding and canonical
schema validation when the exact persisted payload is unchanged. The cache
keeps at most 64 rows and 16 MiB of serialized payloads; those are eviction
budgets, not worker or admission limits, and decoded Python objects consume
additional memory. Changed rows, external writers, quarantine, reboot fences,
and corruption are observed immediately. Returned labels and nested inventory
JSON are copied, so callers cannot mutate cached authority. JSON container
copying avoids generic deepcopy's overhead; immutable validated fields are
shared.

Metrics recording previously ran a truncating SQLite checkpoint with a
one-second busy timeout when the physical retention budget was exceeded. A
reader holding a WAL snapshot could cause that wait on every append. Checkpoint
and reclamation now run with zero busy timeout and restore the writer's timeout
afterward. Logical retention still applies; physical reclamation resumes when
the reader releases its snapshot. Interrupted maintenance rolls back only its
unfinished cleanup transaction, preserving the already committed event.

## Local measurements and regression coverage

[Raw results](../benchmarks/control-plane-hotpaths-2026-09-21.json) compare the
baseline at `c1826b9` with this candidate, using CPython 3.10.13 on macOS.
The heartbeat fixture has six workers and 86 inventory entries per worker,
including four nested dependency descriptors per entry. Three alternating
baseline/candidate repetitions measured:

| Work | Baseline median CPU | Candidate median CPU | Reduction |
| --- | ---: | ---: | ---: |
| 1,000 owner heartbeat reads | 2.057 s | 1.288 s | 37% |
| 200 fleet heartbeat reads | 1.866 s | 0.980 s | 47% |

With a retained SQLite reader, eight metrics writes took 5.275 seconds on the
baseline and 0.00255 seconds on the candidate, retaining ten events after the
reader was released in both cases. This deliberately reproduces the blocking
mechanism; it is not a measurement of its frequency in production.

Run the synthetic fixture from the desired checkout with:

```sh
PYTHONPATH=. .venv/bin/python scripts/benchmark_control_plane_hotpaths.py
```

Regression coverage includes 64 capacity-blocked wakes allowing a ready wake to
proceed, cancellation during HTTP and backoff, expiry while queued, shutdown,
and real loopback HTTP retries preserving authentication and exact operation
identity. Cache tests cover external updates/deletion/corruption, quarantine,
reboot retirement, mutation isolation and eviction. Metrics tests retain a real
SQLite reader, check logical/physical retention, and inject cleanup failure.

The canonical `scripts/check.sh` completed successfully: 992 server tests (six
platform skips), 118 SDK tests, Ruff, shell syntax checks, Go tests, wheel builds
and isolated install verification. Shellcheck is unavailable on this host and
was explicitly skipped using the check script's supported override. The
standalone benchmark also completed a smoke run. These changes were subsequently
deployed in release 0.5.69; see the [production qualification](release-0.5.69-production-2026-09-21.md)
for live smoke and production-host component measurements. A comparable loaded
production run remains outstanding.
