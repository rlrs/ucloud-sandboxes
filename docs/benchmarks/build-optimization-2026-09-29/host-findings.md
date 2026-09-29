# Build-load host findings

Observed 96/96 successful builds across 2 phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.

Gateway API CPU averaged 0.064–0.080 occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.

| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |
|---|---:|---:|---:|---:|---:|---:|
| opt-repeat | 48/48 | 16 | 0.007 | 761 | 15.340 | 0.201 |
| opt-fresh | 48/48 | 16 | 0.010 | 396 | 24.917 | 0.395 |

CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. The SDK driver runs on the gateway and has its own attribution.

| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| opt-repeat | 98.4% | 0.626 / 1.600 / 2.155 | 0.080 / 0.150 / 0.215 | 0.199 / 0.800 / 0.910 | 0.158 / 1.615 / 1.935 | 0/20; 11.45 |
| opt-fresh | 98.1% | 0.402 / 1.565 / 1.955 | 0.064 / 0.155 / 0.260 | 0.117 / 0.430 / 0.640 | 0.149 / 1.560 / 1.710 | 0/18; 11.47 |

Observed 0 failed health probes among 38 samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.

The gateway disk below is configured as `sdb` (the registry volume in this run). Disk busy percentage remains excluded because earlier qualification exposed unreliable counter jumps; it is not used as capacity evidence. This run's anomaly records are retained in JSON.

| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |
|---|---:|---:|---:|---:|---:|---:|
| opt-repeat | 47.083 / 270.955 / 299.924 | 5.518 | 31.05 | 5.461 / 35.437 / 45.039 | 17.37 / 78.89 | 0.530 / 2.285 / 2.795 |
| opt-fresh | 7.446 / 25.961 / 34.680 | 0.756 | 3.01 | 0.540 / 1.668 / 3.056 | 10.36 / 38.37 | 0.329 / 1.190 / 1.450 |

Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.

| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |
|---|---:|---:|---:|---:|---:|---:|
| opt-repeat / 167941784 | 98.4% | 3.189 / 5.875 / 6.145 | 5.551 / 14.813 / 25.694 | 26.00 | 1.088 / 5.339 / 6.745 | 0 |
| opt-repeat / 167941786 | 98.4% | 4.078 / 6.485 / 7.010 | 8.012 / 24.535 / 34.435 | 25.14 | 1.006 / 4.115 / 5.671 | 0 |
| opt-repeat / 167941801 | 98.4% | 2.494 / 5.800 / 6.218 | 4.016 / 14.881 / 25.767 | 26.06 | 1.068 / 5.341 / 7.149 | 0 |
| opt-repeat / 167941802 | 98.4% | 3.614 / 5.995 / 6.570 | 5.596 / 16.044 / 23.455 | 25.36 | 0.979 / 4.315 / 5.119 | 0 |
| opt-fresh / 167941784 | 98.1% | 4.210 / 6.755 / 6.915 | 11.121 / 29.325 / 29.898 | 25.26 | 0.964 / 3.685 / 6.685 | 0 |
| opt-fresh / 167941786 | 98.1% | 4.741 / 6.825 / 7.061 | 12.885 / 30.545 / 33.804 | 24.69 | 2.765 / 29.611 / 36.182 | 0 |
| opt-fresh / 167941801 | 98.1% | 3.470 / 6.460 / 6.545 | 7.307 / 21.917 / 25.105 | 25.72 | 1.022 / 5.583 / 5.918 | 0 |
| opt-fresh / 167941802 | 98.1% | 4.772 / 6.875 / 7.075 | 11.840 / 30.870 / 37.091 | 24.84 | 1.206 / 7.007 / 12.015 | 0 |

Minimum sampled available memory: gateway 13.04 GiB; builders 24.69 GiB.
No OOM kills or swap activity were observed in covered phase intervals.
Largest phase/host mean memory PSI was 0.0276%; transient maxima are retained in JSON.

Work and CPU pressure vary by builder and recipe. Low batch averages do not justify raising all concurrency limits: inspect the busiest execution intervals and queue/admission behavior.
Low measured API CPU alongside registry I/O pressure supports reducing avoidable registry transfer/publication work before changing the gateway implementation for this build workload.

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
python3 scripts/build_load_report.py --root docs/benchmarks/build-optimization-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-optimization-2026-09-29
```
