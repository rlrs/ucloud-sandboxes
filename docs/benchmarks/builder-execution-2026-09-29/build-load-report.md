# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| exec-repeat | 48 / 48 | 69.718 / 112.104 | 32.275 / 79.793 | 0.002 / 0.005 | 27.074 / 44.012 | 1.805 / 3.738 | 27.866 / 46.050 | 16 / 16 | 4 |
| exec-repeat / python-agent | 16 / 16 | 32.037 / 35.400 | 7.705 / 15.476 | 0.002 / 0.003 | 22.069 / 23.841 | 1.845 / 2.098 | 24.054 / 25.639 | 15 / 15 | 4 |
| exec-repeat / typescript-multistage | 16 / 16 | 79.512 / 108.886 | 48.069 / 80.791 | 0.003 / 0.005 | 33.182 / 44.658 | 0.783 / 0.963 | 33.962 / 45.670 | 10 / 10 | 4 |
| exec-repeat / typescript-tools | 16 / 16 | 79.767 / 115.342 | 35.761 / 79.469 | 0.002 / 0.004 | 32.229 / 43.443 | 3.494 / 3.955 | 35.893 / 47.054 | 8 / 8 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| exec-repeat / builder-167960902 | 98.3% | 4.031 / 6.815 / 7.045 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| exec-repeat / builder-167960903 | 98.3% | 3.084 / 6.705 / 7.045 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| exec-repeat / builder-167960906 | 98.3% | 4.225 / 6.665 / 6.870 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| exec-repeat / builder-167960907 | 98.3% | 3.599 / 6.605 / 6.760 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| exec-repeat / gateway | 98.3% | 0.429 / 1.730 / 2.280 | 0.066 / 0.160 / 0.255 | 0.122 / 0.555 / 0.755 | 0.169 / 1.650 / 1.850 | 0 / 19 | 12.950 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| exec-repeat / builder-167960902 / loop0 | 0.242 / 1.246 | 338.462 / 580.939 | 5.706 | 72.699 |
| exec-repeat / builder-167960902 / sda | 0.154 / 1.121 | 342.205 / 581.890 | 1.768 | 12.752 |
| exec-repeat / builder-167960902 / sda1 | 0.154 / 1.121 | 342.205 / 581.890 | 1.768 | 14.402 |
| exec-repeat / builder-167960903 / loop0 | 0.242 / 1.242 | 338.572 / 663.602 | 6.109 | 67.061 |
| exec-repeat / builder-167960903 / sda | 0.127 / 1.121 | 342.294 / 645.191 | 2.456 | 13.649 |
| exec-repeat / builder-167960903 / sda1 | 0.127 / 1.121 | 342.294 / 645.191 | 2.456 | 16.700 |
| exec-repeat / builder-167960906 / loop0 | 0.619 / 1.859 | 283.665 / 491.698 | 5.630 | 45.149 |
| exec-repeat / builder-167960906 / sda | 0.068 / 1.121 | 285.184 / 441.174 | 2.655 | 13.950 |
| exec-repeat / builder-167960906 / sda1 | 0.068 / 1.121 | 285.184 / 441.174 | 2.655 | 15.200 |
| exec-repeat / builder-167960907 / loop0 | 0.621 / 0.741 | 400.260 / 480.392 | 5.607 | 67.241 |
| exec-repeat / builder-167960907 / sda | 0.205 / 1.121 | 361.629 / 557.147 | 1.815 | 13.203 |
| exec-repeat / builder-167960907 / sda1 | 0.205 / 1.121 | 361.629 / 557.147 | 1.815 | 15.248 |
| exec-repeat / gateway / sda | 0.213 / 0.465 | 7.250 / 10.204 | 0.689 | 1.050 |
| exec-repeat / gateway / sda1 | 0.213 / 0.465 | 7.250 / 10.204 | 0.687 | 1.100 |
| exec-repeat / gateway / sdb | 0.002 / 0.004 | 19.197 / 41.203 | 6.868 | flagged |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **exec-repeat:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 560 HTTP 503 responses and 560 repeated submit attempts; use categories to distinguish admission from polling failures. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for exec-repeat: 162 groups reused, 46 built, 363257856 newly built bytes. Cached multistage compile-vertex evidence: 1 records; this does not exclude concurrent executed vertices.

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
