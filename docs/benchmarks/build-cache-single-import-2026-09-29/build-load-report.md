# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| single-import-repeat | 48 / 48 | 17.356 / 21.683 | 14.769 / 19.522 | 0.002 / 0.006 | 1.318 / 2.355 | 0.578 / 0.940 | 1.958 / 3.216 | 16 / 16 | 4 |
| single-import-repeat / python-agent | 16 / 16 | 14.220 / 15.394 | 11.020 / 13.313 | 0.002 / 0.003 | 1.930 / 2.384 | 0.827 / 0.944 | 2.803 / 3.265 | 13 / 13 | 4 |
| single-import-repeat / typescript-multistage | 16 / 16 | 18.382 / 20.244 | 15.997 / 18.170 | 0.002 / 0.008 | 1.268 / 2.046 | 0.392 / 0.498 | 1.704 / 2.566 | 7 / 7 | 4 |
| single-import-repeat / typescript-tools | 16 / 16 | 19.118 / 29.986 | 17.037 / 19.645 | 0.002 / 0.005 | 1.179 / 10.128 | 0.578 / 1.327 | 1.900 / 11.315 | 8 / 7 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| single-import-repeat / builder-167971499 | 98.2% | 0.446 / 1.985 / 2.580 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| single-import-repeat / builder-167971500 | 94.5% | 0.269 / 1.410 / 1.540 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| single-import-repeat / builder-167971515 | 98.2% | 0.376 / 2.050 / 2.110 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| single-import-repeat / builder-167971516 | 98.2% | 1.389 / 2.575 / 2.720 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| single-import-repeat / gateway | 98.2% | 0.875 / 3.075 / 3.135 | 0.040 / 0.245 / 0.260 | 0.322 / 1.145 / 1.235 | 0.325 / 1.505 / 1.590 | 0 / 5 | 14.274 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| single-import-repeat / builder-167971499 / loop0 | 0.000 / 0.000 | 32.429 / 35.028 | 6.218 | 32.301 |
| single-import-repeat / builder-167971499 / sda | 0.000 / 1.121 | 33.668 / 35.886 | 1.000 | 22.050 |
| single-import-repeat / builder-167971499 / sda1 | 0.000 / 1.121 | 33.668 / 35.886 | 0.543 | 22.450 |
| single-import-repeat / builder-167971500 / loop0 | 0.000 / 0.000 | 13.186 / 29.697 | 3.014 | 28.100 |
| single-import-repeat / builder-167971500 / sda | 0.002 / 1.121 | 20.912 / 29.964 | 0.688 | 20.900 |
| single-import-repeat / builder-167971500 / sda1 | 0.002 / 1.121 | 20.912 / 29.964 | 0.688 | 21.050 |
| single-import-repeat / builder-167971515 / loop0 | 0.000 / 0.000 | 15.312 / 42.149 | 2.344 | 35.100 |
| single-import-repeat / builder-167971515 / sda | 0.000 / 1.121 | 24.475 / 42.949 | 0.961 | 19.350 |
| single-import-repeat / builder-167971515 / sda1 | 0.000 / 1.121 | 24.475 / 42.949 | 1.000 | 19.600 |
| single-import-repeat / builder-167971516 / loop0 | 0.002 / 0.121 | 213.104 / 587.400 | 9.092 | 55.650 |
| single-import-repeat / builder-167971516 / sda | 0.014 / 1.121 | 217.288 / 589.260 | 2.389 | 19.700 |
| single-import-repeat / builder-167971516 / sda1 | 0.014 / 1.121 | 217.288 / 589.260 | 2.389 | 20.200 |
| single-import-repeat / gateway / sda | 0.307 / 0.340 | 37.984 / 77.410 | 4.237 | 2.350 |
| single-import-repeat / gateway / sda1 | 0.307 / 0.340 | 37.984 / 77.410 | 4.211 | 2.500 |
| single-import-repeat / gateway / sdb | 0.014 / 0.016 | 16.518 / 16.711 | 10.476 | 95.250 |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **single-import-repeat:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 55 HTTP 503 responses and 55 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for single-import-repeat: 207 groups reused, 1 built, 14925824 newly built bytes. Cached multistage compile-vertex evidence: 16 records; this does not exclude concurrent executed vertices.

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
