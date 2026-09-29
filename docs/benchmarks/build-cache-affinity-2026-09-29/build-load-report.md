# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| affinity-seed | 48 / 48 | 77.732 / 116.639 | 42.352 / 88.266 | 0.004 / 0.007 | 32.266 / 47.592 | 1.715 / 3.992 | 33.781 / 49.533 | 16 / 16 | 4 |
| affinity-seed / python-agent | 16 / 16 | 43.666 / 61.731 | 8.265 / 52.108 | 0.003 / 0.009 | 27.525 / 36.041 | 1.740 / 1.899 | 29.451 / 38.105 | 14 / 14 | 4 |
| affinity-seed / typescript-multistage | 16 / 16 | 93.871 / 115.675 | 50.349 / 85.937 | 0.004 / 0.006 | 32.743 / 47.706 | 1.040 / 1.331 | 33.781 / 48.722 | 10 / 10 | 4 |
| affinity-seed / typescript-tools | 16 / 16 | 94.089 / 118.892 | 47.083 / 89.801 | 0.004 / 0.007 | 31.833 / 47.677 | 3.574 / 4.248 | 35.562 / 51.162 | 9 / 9 | 4 |
| affinity-repeat | 48 / 48 | 57.648 / 97.275 | 13.856 / 65.685 | 0.002 / 0.006 | 21.647 / 50.656 | 1.019 / 4.249 | 23.668 / 52.161 | 16 / 16 | 4 |
| affinity-repeat / python-agent | 16 / 16 | 12.870 / 32.977 | 7.689 / 10.498 | 0.002 / 0.004 | 3.034 / 22.478 | 1.175 / 1.967 | 4.209 / 24.508 | 14 / 14 | 4 |
| affinity-repeat / typescript-multistage | 16 / 16 | 65.779 / 90.639 | 31.360 / 66.761 | 0.002 / 0.003 | 29.100 / 51.385 | 0.518 / 0.780 | 29.419 / 51.998 | 9 / 9 | 4 |
| affinity-repeat / typescript-tools | 16 / 16 | 69.333 / 98.081 | 24.479 / 65.539 | 0.003 / 0.007 | 32.427 / 49.120 | 3.710 / 4.825 | 36.261 / 53.556 | 10 / 10 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU attribution is unavailable for these SSH-launched phases (N/A).

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| affinity-seed / builder-167966880 | 99.2% | 4.145 / 6.100 / 6.165 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-seed / builder-167966883 | 97.6% | 2.604 / 6.260 / 6.970 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-seed / builder-167966886 | 97.6% | 4.147 / 6.414 / 6.610 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-seed / builder-167966887 | 99.2% | 3.764 / 6.276 / 6.510 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-seed / gateway | 99.2% | 0.405 / 1.575 / 1.905 | 0.070 / 0.145 / 0.215 | 0.110 / 0.345 / 0.735 | N/A | 0 / 17 | 11.711 |
| affinity-repeat / builder-167968139 | 99.0% | 3.459 / 5.982 / 6.290 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-repeat / builder-167968141 | 99.0% | 4.512 / 6.600 / 6.840 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-repeat / builder-167968156 | 99.0% | 3.011 / 5.760 / 5.935 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-repeat / builder-167968157 | 99.0% | 3.237 / 6.815 / 7.060 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| affinity-repeat / gateway | 97.0% | 0.496 / 1.770 / 2.690 | 0.068 / 0.145 / 0.235 | 0.139 / 0.475 / 0.925 | N/A | 0 / 14 | 45.614 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| affinity-seed / builder-167966880 / loop0 | 0.498 / 0.744 | 227.427 / 633.507 | 4.770 | 55.350 |
| affinity-seed / builder-167966880 / sda | 0.053 / 1.121 | 233.737 / 474.868 | 2.926 | 29.850 |
| affinity-seed / builder-167966880 / sda1 | 0.053 / 1.121 | 233.737 / 474.868 | 2.926 | 30.350 |
| affinity-seed / builder-167966883 / loop0 | 0.000 / 1.244 | 227.187 / 524.117 | 13.708 | 68.700 |
| affinity-seed / builder-167966883 / sda | 0.080 / 1.121 | 226.058 / 523.268 | 3.824 | 36.900 |
| affinity-seed / builder-167966883 / sda1 | 0.080 / 1.121 | 226.058 / 523.268 | 3.824 | 37.400 |
| affinity-seed / builder-167966886 / loop0 | 0.121 / 1.857 | 168.681 / 515.850 | 4.637 | 68.699 |
| affinity-seed / builder-167966886 / sda | 0.018 / 1.121 | 228.095 / 483.000 | 4.871 | 47.830 |
| affinity-seed / builder-167966886 / sda1 | 0.018 / 1.121 | 228.095 / 483.000 | 4.871 | 48.380 |
| affinity-seed / builder-167966887 / loop0 | 0.619 / 0.625 | 235.772 / 576.888 | 5.136 | 88.882 |
| affinity-seed / builder-167966887 / sda | 0.051 / 1.121 | 223.629 / 590.326 | 6.866 | 56.150 |
| affinity-seed / builder-167966887 / sda1 | 0.051 / 1.121 | 223.629 / 590.326 | 6.867 | 55.950 |
| affinity-seed / gateway / sda | 0.014 / 0.045 | 2.527 / 17.009 | 0.865 | 0.950 |
| affinity-seed / gateway / sda1 | 0.014 / 0.045 | 2.527 / 17.009 | 0.892 | 0.950 |
| affinity-seed / gateway / sdb | 0.002 / 0.004 | 18.834 / 37.484 | 7.779 | flagged |
| affinity-repeat / builder-167968139 / loop0 | 0.121 / 0.121 | 343.795 / 613.367 | 4.847 | 44.949 |
| affinity-repeat / builder-167968139 / sda | 0.010 / 1.121 | 350.300 / 577.776 | 2.298 | 10.700 |
| affinity-repeat / builder-167968139 / sda1 | 0.010 / 1.121 | 350.300 / 577.776 | 2.298 | 15.500 |
| affinity-repeat / builder-167968141 / loop0 | 0.002 / 0.242 | 311.710 / 591.832 | 5.886 | 58.149 |
| affinity-repeat / builder-167968141 / sda | 0.010 / 1.129 | 310.518 / 584.964 | 3.473 | 15.950 |
| affinity-repeat / builder-167968141 / sda1 | 0.010 / 1.129 | 310.518 / 584.964 | 3.473 | 17.650 |
| affinity-repeat / builder-167968156 / loop0 | 0.002 / 0.242 | 343.223 / 586.521 | 6.792 | 53.849 |
| affinity-repeat / builder-167968156 / sda | 0.014 / 1.121 | 350.134 / 594.048 | 2.304 | 17.950 |
| affinity-repeat / builder-167968156 / sda1 | 0.014 / 1.121 | 350.134 / 594.048 | 2.304 | 18.100 |
| affinity-repeat / builder-167968157 / loop0 | 0.002 / 0.121 | 116.373 / 573.405 | 4.988 | 37.850 |
| affinity-repeat / builder-167968157 / sda | 0.004 / 1.121 | 123.776 / 575.200 | 2.018 | 14.550 |
| affinity-repeat / builder-167968157 / sda1 | 0.004 / 1.121 | 123.776 / 575.216 | 2.018 | 15.450 |
| affinity-repeat / gateway / sda | 0.092 / 0.143 | 16.168 / 20.078 | 0.917 | 1.050 |
| affinity-repeat / gateway / sda1 | 0.092 / 0.143 | 16.168 / 20.078 | 0.917 | 1.200 |
| affinity-repeat / gateway / sdb | 0.006 / 0.066 | 19.649 / 21.319 | 8.156 | 84.600 |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

Builders with retained sample lifespans entirely outside a phase and no builds owned in it are omitted from that phase's display and coverage warnings. Their evidence remains in JSON as `outside_phase_window`. Expected owners retain missing-coverage warnings.

## Observed evidence

- **affinity-seed:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 668 HTTP 503 responses and 668 repeated submit attempts; use categories to distinguish admission from polling failures. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for affinity-seed: 162 groups reused, 46 built, 363261952 newly built bytes. Cached multistage compile-vertex evidence: 1 records; this does not exclude concurrent executed vertices.
- **affinity-repeat:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 309 HTTP 503 responses and 309 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for affinity-repeat: 191 groups reused, 17 built, 205791232 newly built bytes. Cached multistage compile-vertex evidence: 7 records; this does not exclude concurrent executed vertices.

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
