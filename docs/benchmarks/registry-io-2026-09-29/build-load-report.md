# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| io-repeat | 48 / 48 | 76.800 / 132.432 | 40.367 / 90.446 | 0.004 / 0.006 | 31.155 / 57.641 | 4.196 / 12.288 | 33.949 / 62.017 | 16 / 16 | 4 |
| io-repeat / python-agent | 16 / 16 | 40.473 / 61.238 | 7.752 / 50.367 | 0.003 / 0.005 | 26.922 / 29.774 | 4.219 / 4.369 | 31.146 / 34.090 | 13 / 13 | 4 |
| io-repeat / typescript-multistage | 16 / 16 | 89.942 / 118.721 | 42.390 / 101.594 | 0.004 / 0.006 | 33.390 / 63.551 | 0.475 / 0.974 | 33.956 / 64.635 | 10 / 10 | 4 |
| io-repeat / typescript-tools | 16 / 16 | 108.025 / 132.853 | 60.855 / 87.239 | 0.004 / 0.007 | 33.477 / 52.468 | 5.732 / 13.502 | 38.990 / 60.258 | 10 / 10 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| io-repeat / builder-167948343 | 98.5% | 2.924 / 6.315 / 6.645 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| io-repeat / builder-167948344 | 98.5% | 4.445 / 6.525 / 6.670 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| io-repeat / builder-167948358 | 98.5% | 3.839 / 6.230 / 6.605 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| io-repeat / builder-167948359 | 98.5% | 3.819 / 6.474 / 6.790 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| io-repeat / gateway | 98.5% | 0.380 / 1.695 / 1.915 | 0.070 / 0.165 / 0.240 | 0.102 / 0.360 / 0.690 | 0.142 / 1.580 / 1.715 | 0 / 22 | 9.642 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| io-repeat / builder-167948343 / loop0 | 0.002 / 1.242 | 219.257 / 491.843 | 11.285 | 64.449 |
| io-repeat / builder-167948343 / sda | 0.068 / 1.122 | 218.670 / 428.560 | 3.460 | 13.250 |
| io-repeat / builder-167948343 / sda1 | 0.068 / 1.122 | 218.670 / 428.560 | 3.460 | 16.650 |
| io-repeat / builder-167948344 / loop0 | 0.363 / 0.861 | 280.175 / 622.984 | 5.353 | 61.151 |
| io-repeat / builder-167948344 / sda | 0.066 / 1.121 | 282.202 / 581.113 | 4.504 | 44.798 |
| io-repeat / builder-167948344 / sda1 | 0.066 / 1.121 | 282.202 / 581.113 | 4.504 | 45.501 |
| io-repeat / builder-167948358 / loop0 | 0.123 / 1.367 | 197.160 / 554.444 | 4.058 | 69.000 |
| io-repeat / builder-167948358 / sda | 0.016 / 1.123 | 202.288 / 491.166 | 3.561 | 42.450 |
| io-repeat / builder-167948358 / sda1 | 0.016 / 1.123 | 202.288 / 491.166 | 3.562 | 43.650 |
| io-repeat / builder-167948359 / loop0 | 0.619 / 1.365 | 163.378 / 641.688 | 5.094 | 62.501 |
| io-repeat / builder-167948359 / sda | 0.010 / 1.121 | 145.371 / 516.934 | 5.504 | 39.950 |
| io-repeat / builder-167948359 / sda1 | 0.010 / 1.121 | 145.371 / 516.934 | 5.504 | 40.600 |
| io-repeat / gateway / sda | 0.123 / 0.334 | 18.287 / 29.148 | 1.073 | 0.900 |
| io-repeat / gateway / sda1 | 0.123 / 0.334 | 18.287 / 29.148 | 1.070 | 1.050 |
| io-repeat / gateway / sdb | 0.004 / 0.014 | 16.215 / 18.853 | 5.172 | 69.250 |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **io-repeat:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 711 HTTP 503 responses and 711 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for io-repeat: 160 groups reused, 48 built, 383524864 newly built bytes. Cached multistage compile-vertex evidence: 2 records; this does not exclude concurrent executed vertices.

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
