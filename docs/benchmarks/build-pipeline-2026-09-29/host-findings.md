# Build-load host findings

Observed 240/240 successful builds across 5 phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.

Gateway API CPU averaged 0.068–0.129 occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.

| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |
|---|---:|---:|---:|---:|---:|---:|
| slotq-a-condition | 48/48 | 16 | 0.006 | 791 | 20.478 | 0.225 |
| slotq-a-warm | 48/48 | 16 | 0.017 | 37 | 103.131 | 2.254 |
| slotq-a-cold | 48/48 | 16 | 0.014 | 1497 | 28.967 | 0.100 |
| slotq-b-warm | 48/48 | 17 | 0.053 | 16 | 162.573 | 2.603 |
| slotq-b-cold | 48/48 | 24 | 0.027 | 1236 | 44.249 | 0.140 |

CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. The SDK driver runs on the gateway and has its own attribution.

| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| slotq-a-condition | 97.4% | 0.367 / 1.660 / 2.350 | 0.079 / 0.145 / 0.200 | 0.115 / 0.420 / 0.730 | 0.078 / 1.230 / 1.310 | 0/0; missing |
| slotq-a-warm | 91.0% | 1.384 / 2.485 / 2.485 | 0.123 / 0.215 / 0.215 | 0.454 / 0.745 / 0.745 | 0.617 / 1.355 / 1.355 | 0/0; missing |
| slotq-a-cold | 99.6% | 0.353 / 1.090 / 2.090 | 0.068 / 0.145 / 0.220 | 0.143 / 0.665 / 1.415 | 0.046 / 0.025 / 1.390 | 0/0; missing |
| slotq-b-warm | 81.4% | 1.594 / 2.390 / 2.390 | 0.129 / 0.180 / 0.180 | 0.545 / 0.775 / 0.775 | 0.688 / 1.095 / 1.095 | 0/0; missing |
| slotq-b-cold | 99.6% | 0.376 / 1.130 / 2.555 | 0.069 / 0.155 / 0.230 | 0.150 / 0.600 / 1.955 | 0.048 / 0.025 / 1.090 | 0/0; missing |

Observed 0 failed health probes among 0 samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.

The gateway disk below is configured as `sdb` (the registry volume in this run). Disk busy percentage remains excluded because earlier qualification exposed unreliable counter jumps; it is not used as capacity evidence. This run's anomaly records are retained in JSON.

| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |
|---|---:|---:|---:|---:|---:|---:|
| slotq-a-condition | 5.482 / 25.861 / 31.035 | 0.621 | 3.80 | 0.704 / 4.831 / 10.721 | 9.67 / 39.76 | 0.316 / 1.300 / 1.350 |
| slotq-a-warm | 13.059 / 23.049 / 23.049 | 0.204 | 2.25 | 1.407 / 2.010 / 2.010 | 34.60 / 50.40 | 0.942 / 1.465 / 1.465 |
| slotq-a-cold | 47.826 / 251.013 / 299.059 | 14.198 | 47.68 | 6.053 / 36.432 / 49.166 | 14.44 / 61.07 | 0.433 / 1.530 / 3.455 |
| slotq-b-warm | 15.046 / 22.316 / 22.316 | 0.206 | 3.02 | 2.504 / 4.620 / 4.620 | 44.72 / 50.30 | 1.191 / 1.400 / 1.400 |
| slotq-b-cold | 50.171 / 275.754 / 300.900 | 14.209 | 45.41 | 6.339 / 38.392 / 47.084 | 15.56 / 67.76 | 0.475 / 2.175 / 3.420 |

Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.

| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |
|---|---:|---:|---:|---:|---:|---:|
| slotq-a-condition / 168008406 | 26.9% | 2.291 / 5.710 / 5.710 | 2.218 / 8.854 / 8.854 | 26.46 | 1.604 / 8.104 / 8.104 | 0 |
| slotq-a-condition / 168008407 | 26.9% | 2.761 / 4.360 / 4.360 | 2.152 / 9.002 / 9.002 | 27.07 | 3.020 / 17.002 / 17.002 | 0 |
| slotq-a-condition / 168008410 | 26.9% | 0.864 / 4.960 / 4.960 | 0.646 / 3.934 / 3.934 | 27.22 | 1.039 / 6.331 / 6.331 | 0 |
| slotq-a-condition / 168008411 | 26.9% | 1.350 / 4.980 / 4.980 | 1.593 / 11.949 / 11.949 | 26.85 | 1.712 / 10.340 / 10.340 | 0 |
| slotq-a-warm / 168008406 | 91.0% | 1.816 / 2.835 / 2.835 | 6.664 / 13.678 / 13.678 | 27.94 | 12.223 / 21.493 / 21.493 | 0 |
| slotq-a-warm / 168008407 | 91.0% | 2.222 / 2.925 / 2.925 | 7.911 / 11.446 / 11.446 | 27.88 | 12.561 / 17.981 / 17.981 | 0 |
| slotq-a-warm / 168008410 | 91.0% | 1.426 / 2.490 / 2.490 | 5.752 / 15.531 / 15.531 | 28.01 | 9.721 / 22.145 / 22.145 | 0 |
| slotq-a-warm / 168008411 | 91.0% | 2.042 / 2.835 / 2.835 | 7.592 / 12.541 / 12.541 | 27.92 | 13.525 / 20.923 / 20.923 | 0 |
| slotq-a-cold / 168008406 | 99.6% | 3.911 / 6.052 / 6.840 | 4.903 / 14.135 / 36.813 | 25.49 | 2.311 / 8.918 / 19.174 | 0 |
| slotq-a-cold / 168008407 | 99.6% | 3.985 / 5.985 / 6.660 | 5.361 / 19.145 / 23.009 | 24.95 | 2.537 / 11.137 / 14.951 | 0 |
| slotq-a-cold / 168008410 | 99.6% | 4.009 / 6.225 / 6.680 | 7.329 / 19.594 / 27.664 | 25.57 | 2.341 / 10.530 / 14.464 | 0 |
| slotq-a-cold / 168008411 | 99.6% | 3.553 / 6.050 / 6.660 | 5.647 / 20.814 / 27.625 | 25.20 | 2.178 / 8.551 / 15.116 | 0 |
| slotq-b-warm / 168008406 | 93.1% | 2.442 / 3.230 / 3.230 | 8.510 / 13.497 / 13.497 | 27.00 | 12.126 / 20.706 / 20.706 | 0 |
| slotq-b-warm / 168008407 | 81.4% | 1.984 / 3.145 / 3.145 | 6.899 / 13.767 / 13.767 | 26.83 | 10.636 / 19.928 / 19.928 | 0 |
| slotq-b-warm / 168008410 | 81.4% | 2.308 / 3.145 / 3.145 | 9.071 / 14.825 / 14.825 | 26.98 | 15.282 / 19.430 / 19.430 | 0 |
| slotq-b-warm / 168008411 | 81.4% | 2.247 / 3.430 / 3.430 | 8.005 / 16.160 / 16.160 | 26.95 | 15.215 / 23.353 / 23.353 | 0 |
| slotq-b-cold / 168008406 | 98.9% | 3.570 / 6.859 / 7.290 | 7.758 / 33.961 / 42.419 | 24.83 | 3.021 / 10.736 / 19.156 | 0 |
| slotq-b-cold / 168008407 | 99.6% | 4.300 / 6.535 / 6.860 | 7.930 / 24.775 / 35.569 | 23.41 | 4.221 / 12.775 / 28.482 | 0 |
| slotq-b-cold / 168008410 | 98.9% | 4.384 / 6.780 / 7.120 | 9.623 / 30.680 / 45.137 | 25.08 | 4.003 / 12.664 / 15.873 | 0 |
| slotq-b-cold / 168008411 | 99.6% | 3.940 / 6.620 / 6.880 | 7.577 / 32.411 / 38.849 | 24.99 | 3.613 / 13.347 / 18.417 | 0 |

Minimum sampled available memory: gateway 12.97 GiB; builders 23.41 GiB.
No OOM kills or swap activity were observed in covered phase intervals.
Largest phase/host mean memory PSI was 0.2981%; transient maxima are retained in JSON.

Work and CPU pressure vary by builder and recipe. Low batch averages do not justify raising all concurrency limits: inspect the busiest execution intervals and queue/admission behavior.
Low measured API CPU alongside registry I/O pressure supports reducing avoidable registry transfer/publication work before changing the gateway implementation for this build workload.

Coverage warnings:

- slotq-a-condition / builder-168008406: only 26.9% of the client window has complete telemetry intervals.
- slotq-a-condition / builder-168008407: only 26.9% of the client window has complete telemetry intervals.
- slotq-a-condition / builder-168008410: only 26.9% of the client window has complete telemetry intervals.
- slotq-a-condition / builder-168008411: only 26.9% of the client window has complete telemetry intervals.
- slotq-b-warm / builder-168008407: only 81.4% of the client window has complete telemetry intervals.
- slotq-b-warm / builder-168008410: only 81.4% of the client window has complete telemetry intervals.
- slotq-b-warm / builder-168008411: only 81.4% of the client window has complete telemetry intervals.
- slotq-b-warm / gateway: only 81.4% of the client window has complete telemetry intervals.

Limits:

- These results describe the sampled image-build pipeline. They do not qualify 500 or 1,000 running agent sandboxes, model-relay traffic, or their mixed load.
- Batch averages include submission, queueing, execution and completion tails. Repeated fixture graphs can share BuildKit work. Execution overlap includes build, push and EROFS publication, not only CPU-heavy RUN instructions.
- The gateway-local SDK driver consumes host CPU. Process groups are exclusive, but short-lived processes and asynchronous counter reads limit exact attribution. Never subtract independently calculated percentiles.
- Disk busy_percent is deliberately excluded from all conclusions and tables because earlier qualification exposed unreliable busy_ms counter jumps. This run's raw counters and any flagged anomalies are retained in JSON. No disk-utilization ceiling is inferred.
- Disk bytes, request latency, queue depth, PSI and iowait are separate signals. Traffic includes buffering, writeback, metadata, cache exports and maintenance. Physical reads near zero can mean page-cache hits. Do not sum overlapping devices or network interfaces.
- Low gateway CPU does not establish why admission returned 503 or why a build queued. Correlate per-build phases before assigning a specific cause or raising concurrency.
- Telemetry means are time-weighted and p95 is nearest-rank over sampled intervals. API/client p95 uses linear interpolation. Only complete counter intervals inside each client window are included; short phases lose a larger fraction at their boundaries.
- Health probes originate on the gateway; SDK timing includes a gateway-local client. Neither is an external end-to-end capacity test. No OOM delta means none observed in covered intervals, not a complete kernel-log audit.

Reproduce after copying complete raw telemetry and phase summaries:

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/build-pipeline-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-pipeline-2026-09-29
```
