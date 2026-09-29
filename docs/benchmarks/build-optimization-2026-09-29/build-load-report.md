# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| opt-repeat | 48 / 48 | 84.219 / 118.652 | 44.886 / 90.898 | 0.002 / 0.007 | 30.748 / 39.395 | 4.016 / 8.490 | 36.329 / 42.437 | 16 / 16 | 4 |
| opt-repeat / python-agent | 16 / 16 | 45.108 / 60.269 | 7.637 / 53.164 | 0.002 / 0.003 | 28.671 / 35.836 | 7.709 / 8.449 | 36.843 / 40.318 | 14 / 14 | 4 |
| opt-repeat / typescript-multistage | 16 / 16 | 90.861 / 115.163 | 49.536 / 90.568 | 0.003 / 0.008 | 32.312 / 41.012 | 0.634 / 1.139 | 33.211 / 41.707 | 9 / 9 | 4 |
| opt-repeat / typescript-tools | 16 / 16 | 91.722 / 121.003 | 50.850 / 91.350 | 0.002 / 0.007 | 31.465 / 36.893 | 4.554 / 9.505 | 36.608 / 44.504 | 8 / 8 | 4 |
| opt-fresh | 48 / 48 | 58.929 / 102.754 | 18.055 / 63.155 | 0.004 / 0.010 | 30.972 / 38.997 | 3.877 / 7.169 | 32.900 / 44.768 | 16 / 16 | 4 |
| opt-fresh / python-agent | 16 / 16 | 18.013 / 21.441 | 7.589 / 10.732 | 0.003 / 0.009 | 6.050 / 6.533 | 3.931 / 4.233 | 9.602 / 10.486 | 15 / 15 | 4 |
| opt-fresh / typescript-multistage | 16 / 16 | 65.107 / 97.303 | 49.414 / 63.054 | 0.005 / 0.029 | 32.700 / 38.548 | 0.695 / 1.401 | 33.543 / 40.086 | 8 / 8 | 4 |
| opt-fresh / typescript-tools | 16 / 16 | 68.019 / 105.954 | 23.659 / 66.900 | 0.004 / 0.012 | 36.300 / 39.375 | 6.034 / 7.908 | 41.406 / 46.605 | 10 / 10 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| opt-repeat / builder-167941784 | 98.4% | 3.189 / 5.875 / 6.145 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-repeat / builder-167941786 | 98.4% | 4.078 / 6.485 / 7.010 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-repeat / builder-167941801 | 98.4% | 2.494 / 5.800 / 6.218 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-repeat / builder-167941802 | 98.4% | 3.614 / 5.995 / 6.570 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-repeat / gateway | 98.4% | 0.626 / 1.600 / 2.155 | 0.080 / 0.150 / 0.215 | 0.199 / 0.800 / 0.910 | 0.158 / 1.615 / 1.935 | 0 / 20 | 11.449 |
| opt-fresh / builder-167941784 | 98.1% | 4.210 / 6.755 / 6.915 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-fresh / builder-167941786 | 98.1% | 4.741 / 6.825 / 7.061 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-fresh / builder-167941801 | 98.1% | 3.470 / 6.460 / 6.545 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-fresh / builder-167941802 | 98.1% | 4.772 / 6.875 / 7.075 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| opt-fresh / gateway | 98.1% | 0.402 / 1.565 / 1.955 | 0.064 / 0.155 / 0.260 | 0.117 / 0.430 / 0.640 | 0.149 / 1.560 / 1.710 | 0 / 18 | 11.475 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| opt-repeat / builder-167941784 / loop0 | 0.121 / 1.363 | 174.648 / 597.679 | 7.137 | 49.250 |
| opt-repeat / builder-167941784 / sda | 0.014 / 1.121 | 178.011 / 532.188 | 2.050 | 13.400 |
| opt-repeat / builder-167941784 / sda1 | 0.014 / 1.121 | 178.011 / 532.188 | 2.050 | 14.750 |
| opt-repeat / builder-167941786 / loop0 | 0.619 / 0.744 | 190.559 / 620.848 | 5.283 | 46.800 |
| opt-repeat / builder-167941786 / sda | 0.012 / 1.129 | 185.259 / 619.983 | 2.045 | 15.300 |
| opt-repeat / builder-167941786 / sda1 | 0.012 / 1.129 | 185.259 / 619.983 | 2.043 | 16.500 |
| opt-repeat / builder-167941801 / loop0 | 0.121 / 1.861 | 229.782 / 644.025 | 6.590 | 50.701 |
| opt-repeat / builder-167941801 / sda | 0.010 / 1.121 | 225.261 / 640.794 | 2.295 | 15.600 |
| opt-repeat / builder-167941801 / sda1 | 0.010 / 1.121 | 225.261 / 640.794 | 2.295 | 18.250 |
| opt-repeat / builder-167941802 / loop0 | 0.121 / 1.865 | 150.094 / 692.736 | 4.710 | 46.700 |
| opt-repeat / builder-167941802 / sda | 0.014 / 1.121 | 126.950 / 623.255 | 2.192 | 15.900 |
| opt-repeat / builder-167941802 / sda1 | 0.014 / 1.121 | 126.950 / 623.255 | 2.192 | 16.850 |
| opt-repeat / gateway / sda | 0.000 / 0.002 | 31.101 / 58.588 | 2.048 | 8.500 |
| opt-repeat / gateway / sda1 | 0.000 / 0.002 | 31.101 / 58.588 | 2.048 | 9.250 |
| opt-repeat / gateway / sdb | 0.002 / 0.008 | 270.955 / 299.924 | 146.718 | 99.150 |
| opt-fresh / builder-167941784 / loop0 | 0.619 / 0.740 | 73.607 / 93.705 | 2.982 | 42.600 |
| opt-fresh / builder-167941784 / sda | 0.002 / 0.020 | 76.762 / 98.770 | 2.057 | 11.950 |
| opt-fresh / builder-167941784 / sda1 | 0.002 / 0.020 | 76.762 / 98.770 | 2.057 | 12.250 |
| opt-fresh / builder-167941786 / loop0 | 0.619 / 1.480 | 73.436 / 164.201 | 2.836 | 67.250 |
| opt-fresh / builder-167941786 / sda | 0.076 / 0.178 | 98.512 / 167.527 | 2.029 | 65.200 |
| opt-fresh / builder-167941786 / sda1 | 0.076 / 0.178 | 98.512 / 167.527 | 2.029 | 94.398 |
| opt-fresh / builder-167941801 / loop0 | 0.121 / 1.238 | 51.415 / 84.736 | 3.647 | 51.000 |
| opt-fresh / builder-167941801 / sda | 0.020 / 0.178 | 57.328 / 96.631 | 1.439 | 16.450 |
| opt-fresh / builder-167941801 / sda1 | 0.020 / 0.178 | 57.328 / 96.631 | 1.439 | 17.850 |
| opt-fresh / builder-167941802 / loop0 | 0.121 / 1.482 | 56.158 / 121.547 | 3.063 | 59.500 |
| opt-fresh / builder-167941802 / sda | 0.051 / 0.127 | 58.044 / 126.929 | 1.523 | 19.950 |
| opt-fresh / builder-167941802 / sda1 | 0.051 / 0.127 | 58.044 / 126.929 | 1.523 | 20.950 |
| opt-fresh / gateway / sda | 0.164 / 0.963 | 15.025 / 23.461 | 0.835 | 0.900 |
| opt-fresh / gateway / sda1 | 0.164 / 0.963 | 15.025 / 23.461 | 0.831 | 1.150 |
| opt-fresh / gateway / sdb | 0.004 / 0.006 | 25.961 / 34.680 | 7.240 | 77.350 |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **opt-repeat:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 761 HTTP 503 responses and 761 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for opt-repeat: 160 groups reused, 48 built, 383524864 newly built bytes. Cached multistage compile-vertex evidence: 5 records; this does not exclude concurrent executed vertices.
- **opt-fresh:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 396 HTTP 503 responses and 396 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for opt-fresh: 160 groups reused, 48 built, 383516672 newly built bytes. Cached multistage compile-vertex evidence: 3 records; this does not exclude concurrent executed vertices.

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
