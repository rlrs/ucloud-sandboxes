# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| prep-repeat | 48 / 48 | 87.754 / 136.559 | 41.709 / 98.797 | 0.004 / 0.015 | 34.215 / 50.801 | 3.021 / 9.789 | 36.162 / 60.631 | 16 / 16 | 4 |
| prep-repeat / python-agent | 16 / 16 | 41.274 / 76.292 | 7.884 / 67.921 | 0.004 / 0.010 | 29.352 / 36.773 | 3.048 / 4.495 | 31.652 / 40.015 | 13 / 13 | 4 |
| prep-repeat / typescript-multistage | 16 / 16 | 97.755 / 136.662 | 65.255 / 99.373 | 0.005 / 0.015 | 35.482 / 49.191 | 0.705 / 0.881 | 36.230 / 50.180 | 7 / 7 | 3 |
| prep-repeat / typescript-tools | 16 / 16 | 99.659 / 124.510 | 44.937 / 98.091 | 0.004 / 0.009 | 35.974 / 50.900 | 4.813 / 9.975 | 40.791 / 60.816 | 10 / 10 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| prep-repeat / builder-167957685 | 99.3% | 2.753 / 6.795 / 6.980 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| prep-repeat / builder-167957686 | 99.3% | 3.773 / 6.785 / 6.920 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| prep-repeat / builder-167957690 | 99.3% | 3.782 / 6.375 / 6.705 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| prep-repeat / builder-167957691 | 97.8% | 3.879 / 6.265 / 6.660 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| prep-repeat / gateway | 97.8% | 0.360 / 1.620 / 1.961 | 0.069 / 0.145 / 0.220 | 0.104 / 0.305 / 0.525 | 0.115 / 1.550 / 1.720 | 23 / 23 | 11.080 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| prep-repeat / builder-167957685 / loop0 | 0.000 / 0.484 | 198.434 / 561.746 | 6.747 | 54.099 |
| prep-repeat / builder-167957685 / sda | 0.143 / 1.121 | 219.910 / 424.160 | 3.983 | 10.717 |
| prep-repeat / builder-167957685 / sda1 | 0.143 / 1.121 | 219.910 / 424.160 | 3.983 | 12.900 |
| prep-repeat / builder-167957686 / loop0 | 0.008 / 1.984 | 163.413 / 490.125 | 12.091 | 76.250 |
| prep-repeat / builder-167957686 / sda | 0.051 / 1.121 | 164.048 / 512.848 | 3.342 | 39.500 |
| prep-repeat / builder-167957686 / sda1 | 0.051 / 1.121 | 164.048 / 512.848 | 3.342 | 40.100 |
| prep-repeat / builder-167957690 / loop0 | 0.121 / 1.359 | 152.519 / 609.722 | 4.692 | 72.600 |
| prep-repeat / builder-167957690 / sda | 0.051 / 1.120 | 171.927 / 572.094 | 4.426 | 40.051 |
| prep-repeat / builder-167957690 / sda1 | 0.051 / 1.120 | 171.927 / 572.094 | 4.425 | 41.950 |
| prep-repeat / builder-167957691 / loop0 | 0.619 / 0.623 | 187.555 / 640.403 | 3.727 | 67.601 |
| prep-repeat / builder-167957691 / sda | 0.129 / 1.121 | 171.582 / 549.802 | 4.067 | 38.752 |
| prep-repeat / builder-167957691 / sda1 | 0.129 / 1.121 | 171.582 / 549.802 | 4.067 | 39.852 |
| prep-repeat / gateway / sda | 0.000 / 0.023 | 22.170 / 38.990 | 1.034 | 0.950 |
| prep-repeat / gateway / sda1 | 0.000 / 0.023 | 22.170 / 38.990 | 1.036 | 1.300 |
| prep-repeat / gateway / sdb | 0.006 / 0.066 | 19.971 / 35.537 | 8.653 | flagged |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **prep-repeat:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 775 HTTP 503 responses and 775 repeated submit attempts; use categories to distinguish admission from polling failures. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for prep-repeat: 160 groups reused, 48 built, 383520768 newly built bytes. Cached multistage compile-vertex evidence: 3 records; this does not exclude concurrent executed vertices.

## Interpretation limits

- Client latency includes SDK compression/upload and gateway admission; worker queue time begins after context materialization.
- Execution overlap spans execution_started_at to finished_at: it includes build/push and EROFS publication, not only RUN instructions, and excludes subsequent cleanup.
- Overlap is computed from complete, unique build intervals. Missing intervals make the observed peak a lower bound; timestamps across hosts depend on synchronized clocks.
- CACHED evidence proves only the described vertex. It may follow waiting for a concurrently computed stage and does not prove the request avoided compilation. Missing/truncated evidence is unknown, not a miss; a warm local hit does not prove cross-builder sharing.
- Repeated submit HTTP attempts and 503s are transport observations, not duplicate execution. Context GET 404s are normal cache lookup misses.
- Fixture hashes exclude fixture.json and differ from SDK archive hashes. Repeated fixtures can share/coalesce BuildKit graphs even with unique image/build IDs.
- EROFS bytes built exclude reused components and are neither total image size nor registry physical growth.
- Gateway-local drivers consume gateway CPU. Driver process attribution is separate but incomplete for short-lived processes; subtracting process p95 from host p95 is invalid.
- Disk devices and network interfaces are separate accounting views; their rates must not be summed. Health probes originate on the sampled host.
- Disk busy counters exceeding 105% over an interval are flagged as unreliable utilization (possible delayed/batched counter accounting). Raw derived statistics are retained in JSON, without clamping.
- Record UTC timestamps have one-second precision. Telemetry includes only complete intervals within that recorded client window; latency distributions use all records with that measurement, including failures.
