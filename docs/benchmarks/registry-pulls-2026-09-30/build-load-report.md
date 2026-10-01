# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| slotq-pulls-a-cold | 48 / 48 | 176.751 / 234.683 | 61.405 / 167.792 | 0.003 / 0.010 | 56.620 / 105.054 | 11.684 / 56.499 | 69.046 / 165.589 | 24 / 24 | 4 |
| slotq-pulls-a-cold / python-agent | 16 / 16 | 200.889 / 269.134 | 56.667 / 157.907 | 0.003 / 0.013 | 98.159 / 108.023 | 45.538 / 60.045 | 151.316 / 167.544 | 14 / 14 | 4 |
| slotq-pulls-a-cold / typescript-multistage | 16 / 16 | 165.966 / 223.956 | 108.775 / 169.706 | 0.004 / 0.008 | 47.106 / 59.166 | 0.981 / 4.347 | 54.033 / 69.209 | 7 / 7 | 4 |
| slotq-pulls-a-cold / typescript-tools | 16 / 16 | 143.589 / 232.591 | 76.499 / 163.159 | 0.003 / 0.022 | 51.300 / 60.120 | 11.684 / 20.394 | 67.205 / 87.668 | 8 / 8 | 4 |
| slotq-pulls-b-cold | 48 / 48 | 128.196 / 219.445 | 49.838 / 139.890 | 0.004 / 0.012 | 46.633 / 102.991 | 12.616 / 19.538 | 60.739 / 124.017 | 23 / 23 | 4 |
| slotq-pulls-b-cold / python-agent | 16 / 16 | 199.328 / 241.019 | 88.077 / 143.848 | 0.005 / 0.007 | 94.803 / 104.721 | 16.297 / 17.603 | 111.817 / 126.204 | 14 / 14 | 4 |
| slotq-pulls-b-cold / typescript-multistage | 16 / 16 | 94.565 / 177.806 | 49.838 / 136.858 | 0.004 / 0.014 | 41.786 / 44.874 | 0.831 / 2.957 | 42.527 / 51.942 | 5 / 5 | 4 |
| slotq-pulls-b-cold / typescript-tools | 16 / 16 | 112.333 / 191.989 | 46.113 / 137.155 | 0.004 / 0.011 | 46.376 / 49.381 | 12.716 / 20.613 | 60.227 / 66.276 | 9 / 9 | 4 |
| slotq-pulls-a2-cold | 48 / 48 | 155.339 / 259.908 | 51.822 / 156.507 | 0.007 / 0.019 | 50.466 / 103.130 | 11.498 / 41.870 | 60.809 / 142.385 | 24 / 24 | 4 |
| slotq-pulls-a2-cold / python-agent | 16 / 16 | 185.110 / 267.455 | 44.045 / 155.897 | 0.005 / 0.017 | 96.874 / 104.671 | 35.855 / 43.134 | 137.987 / 147.134 | 12 / 12 | 4 |
| slotq-pulls-a2-cold / typescript-multistage | 16 / 16 | 144.706 / 193.669 | 91.438 / 145.093 | 0.007 / 0.011 | 45.413 / 54.631 | 0.639 / 4.607 | 48.123 / 55.505 | 8 / 8 | 4 |
| slotq-pulls-a2-cold / typescript-tools | 16 / 16 | 142.062 / 212.301 | 76.615 / 157.402 | 0.007 / 0.023 | 48.395 / 55.266 | 11.498 / 15.075 | 60.809 / 97.164 | 8 / 8 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| slotq-pulls-a-cold / builder-168016286 | 98.9% | 4.114 / 6.605 / 6.980 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a-cold / builder-168016287 | 98.9% | 4.235 / 6.485 / 7.165 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a-cold / builder-168016314 | 98.9% | 4.683 / 7.215 / 7.570 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a-cold / builder-168016315 | 98.9% | 4.433 / 6.800 / 7.220 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a-cold / gateway | 99.6% | 0.411 / 1.270 / 2.235 | 0.079 / 0.145 / 0.180 | 0.170 / 0.630 / 1.265 | 0.049 / 0.035 / 1.165 | 0 / 24 | 14.245 |
| slotq-pulls-b-cold / builder-168016286 | 99.8% | 4.210 / 6.665 / 7.159 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-b-cold / builder-168016287 | 99.8% | 4.170 / 6.760 / 7.361 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-b-cold / builder-168016314 | 99.8% | 3.986 / 6.353 / 7.055 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-b-cold / builder-168016315 | 99.8% | 4.618 / 6.552 / 6.895 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-b-cold / gateway | 99.0% | 0.416 / 1.105 / 2.405 | 0.075 / 0.170 / 0.235 | 0.179 / 0.625 / 1.730 | 0.053 / 0.030 / 1.390 | 0 / 21 | 11.862 |
| slotq-pulls-a2-cold / builder-168016286 | 99.5% | 4.448 / 6.785 / 7.350 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a2-cold / builder-168016287 | 99.5% | 3.927 / 6.785 / 7.065 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a2-cold / builder-168016314 | 99.5% | 4.445 / 6.670 / 7.030 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a2-cold / builder-168016315 | 99.5% | 4.428 / 6.850 / 7.195 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-pulls-a2-cold / gateway | 98.8% | 0.374 / 1.240 / 2.060 | 0.073 / 0.145 / 0.230 | 0.159 / 0.730 / 1.335 | 0.044 / 0.025 / 1.375 | 0 / 25 | 11.754 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| slotq-pulls-a-cold / builder-168016286 / loop0 | 0.002 / 1.240 | 292.145 / 832.359 | 4.337 | 68.950 |
| slotq-pulls-a-cold / builder-168016286 / sda | 0.014 / 0.465 | 345.519 / 837.335 | 3.554 | 16.400 |
| slotq-pulls-a-cold / builder-168016286 / sda1 | 0.014 / 0.465 | 345.519 / 837.335 | 3.554 | 21.050 |
| slotq-pulls-a-cold / builder-168016287 / loop0 | 1.209 / 20.801 | 369.162 / 541.224 | 5.172 | 65.751 |
| slotq-pulls-a-cold / builder-168016287 / sda | 0.123 / 1.121 | 370.226 / 553.998 | 4.429 | 12.950 |
| slotq-pulls-a-cold / builder-168016287 / sda1 | 0.123 / 1.121 | 370.226 / 553.998 | 4.429 | 18.000 |
| slotq-pulls-a-cold / builder-168016314 / loop0 | 1.242 / 30.584 | 271.579 / 504.678 | 7.038 | 74.076 |
| slotq-pulls-a-cold / builder-168016314 / sda | 0.350 / 10.067 | 330.196 / 508.405 | 4.000 | 15.750 |
| slotq-pulls-a-cold / builder-168016314 / sda1 | 0.350 / 10.067 | 330.196 / 508.405 | 4.000 | 20.700 |
| slotq-pulls-a-cold / builder-168016315 / loop0 | 24.704 / 40.177 | 407.506 / 721.778 | 5.437 | 72.583 |
| slotq-pulls-a-cold / builder-168016315 / sda | 15.898 / 26.647 | 409.998 / 726.233 | 4.583 | 14.050 |
| slotq-pulls-a-cold / builder-168016315 / sda1 | 15.898 / 26.647 | 409.998 / 726.233 | 4.583 | 17.650 |
| slotq-pulls-a-cold / gateway / sda | 3.928 / 39.604 | 3.471 / 7.352 | 0.687 | 2.700 |
| slotq-pulls-a-cold / gateway / sda1 | 3.928 / 39.604 | 3.471 / 7.352 | 0.687 | 2.750 |
| slotq-pulls-a-cold / gateway / sdb | 0.004 / 181.484 | 278.665 / 306.013 | 172.217 | flagged |
| slotq-pulls-b-cold / builder-168016286 / loop0 | 35.330 / 44.478 | 167.214 / 508.861 | 3.678 | 55.250 |
| slotq-pulls-b-cold / builder-168016286 / sda | 26.018 / 42.285 | 226.098 / 515.682 | 2.851 | 15.450 |
| slotq-pulls-b-cold / builder-168016286 / sda1 | 26.018 / 42.285 | 226.098 / 515.682 | 2.850 | 20.650 |
| slotq-pulls-b-cold / builder-168016287 / loop0 | 24.260 / 57.631 | 131.935 / 526.828 | 3.062 | 48.446 |
| slotq-pulls-b-cold / builder-168016287 / sda | 10.072 / 37.290 | 205.063 / 538.024 | 3.018 | 19.450 |
| slotq-pulls-b-cold / builder-168016287 / sda1 | 10.072 / 37.290 | 205.063 / 538.024 | 3.018 | 25.300 |
| slotq-pulls-b-cold / builder-168016287 / sda15 | 0.000 / 0.807 | 0.000 / 0.000 | 0.407 | 0.000 |
| slotq-pulls-b-cold / builder-168016314 / loop0 | 36.727 / 58.589 | 188.516 / 414.608 | 3.918 | 50.551 |
| slotq-pulls-b-cold / builder-168016314 / sda | 25.183 / 43.144 | 274.655 / 734.156 | 3.127 | 15.100 |
| slotq-pulls-b-cold / builder-168016314 / sda1 | 25.183 / 43.144 | 274.655 / 734.156 | 3.127 | 19.941 |
| slotq-pulls-b-cold / builder-168016315 / loop0 | 60.973 / 95.381 | 196.812 / 523.433 | 3.122 | 61.549 |
| slotq-pulls-b-cold / builder-168016315 / sda | 54.310 / 70.354 | 263.087 / 608.890 | 2.886 | 18.850 |
| slotq-pulls-b-cold / builder-168016315 / sda1 | 54.310 / 70.354 | 263.087 / 608.890 | 2.886 | 24.050 |
| slotq-pulls-b-cold / gateway / sda | 0.096 / 4.850 | 5.338 / 10.951 | 0.735 | 1.050 |
| slotq-pulls-b-cold / gateway / sda1 | 0.096 / 4.850 | 5.338 / 10.951 | 0.755 | 1.050 |
| slotq-pulls-b-cold / gateway / sdb | 0.004 / 0.023 | 290.404 / 301.914 | 168.450 | flagged |
| slotq-pulls-a2-cold / builder-168016286 / loop0 | 53.576 / 80.918 | 369.492 / 643.702 | 3.497 | 74.249 |
| slotq-pulls-a2-cold / builder-168016286 / sda | 32.842 / 54.885 | 398.611 / 665.168 | 3.019 | 16.000 |
| slotq-pulls-a2-cold / builder-168016286 / sda1 | 32.842 / 54.885 | 398.611 / 665.168 | 3.019 | 21.200 |
| slotq-pulls-a2-cold / builder-168016287 / loop0 | 37.912 / 55.622 | 187.545 / 427.102 | 3.448 | 62.850 |
| slotq-pulls-a2-cold / builder-168016287 / sda | 33.055 / 59.217 | 230.651 / 409.709 | 3.636 | 15.700 |
| slotq-pulls-a2-cold / builder-168016287 / sda1 | 33.055 / 59.217 | 230.651 / 409.709 | 3.636 | 18.600 |
| slotq-pulls-a2-cold / builder-168016314 / loop0 | 53.370 / 89.129 | 284.651 / 515.373 | 4.029 | 76.350 |
| slotq-pulls-a2-cold / builder-168016314 / sda | 40.870 / 65.149 | 295.970 / 699.188 | 2.756 | 22.300 |
| slotq-pulls-a2-cold / builder-168016314 / sda1 | 40.870 / 65.149 | 295.970 / 699.188 | 2.756 | 26.776 |
| slotq-pulls-a2-cold / builder-168016315 / loop0 | 32.078 / 54.748 | 312.013 / 454.700 | 3.724 | 73.100 |
| slotq-pulls-a2-cold / builder-168016315 / sda | 23.531 / 29.648 | 320.320 / 449.285 | 2.942 | 16.500 |
| slotq-pulls-a2-cold / builder-168016315 / sda1 | 23.531 / 29.648 | 320.320 / 449.285 | 2.942 | 20.952 |
| slotq-pulls-a2-cold / gateway / sda | 0.137 / 5.256 | 9.486 / 14.219 | 1.000 | 1.250 |
| slotq-pulls-a2-cold / gateway / sda1 | 0.137 / 5.256 | 9.486 / 14.219 | 0.960 | 1.250 |
| slotq-pulls-a2-cold / gateway / sdb | 0.008 / 0.016 | 250.620 / 299.420 | 154.764 | 91.701 |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **slotq-pulls-a-cold:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 24/24. Observed 1473 HTTP 503 responses and 1473 repeated submit attempts; use categories to distinguish admission from polling failures. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for slotq-pulls-a-cold: 113 groups reused, 95 built, 12919083008 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.
- **slotq-pulls-b-cold:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 23/23. Observed 1206 HTTP 503 responses and 1206 repeated submit attempts; use categories to distinguish admission from polling failures. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for slotq-pulls-b-cold: 120 groups reused, 88 built, 12893163520 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.
- **slotq-pulls-a2-cold:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 24/24. Observed 1311 HTTP 503 responses and 1311 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for slotq-pulls-a2-cold: 122 groups reused, 86 built, 12885766144 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.

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
