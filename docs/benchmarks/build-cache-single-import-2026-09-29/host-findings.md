# Build-load host findings

Observed 48/48 successful builds across 1 phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.

Gateway API CPU averaged 0.040–0.040 occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.

| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |
|---|---:|---:|---:|---:|---:|---:|
| single-import-repeat | 48/48 | 16 | 0.006 | 55 | 78.994 | 0.620 |

CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. The SDK driver runs on the gateway and has its own attribution.

| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| single-import-repeat | 98.2% | 0.875 / 3.075 / 3.135 | 0.040 / 0.245 / 0.260 | 0.322 / 1.145 / 1.235 | 0.325 / 1.505 / 1.590 | 0/5; 14.27 |

Observed 0 failed health probes among 5 samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.

The gateway disk below is configured as `sdb` (the registry volume in this run). Disk busy percentage remains excluded because earlier qualification exposed unreliable counter jumps; it is not used as capacity evidence. This run's anomaly records are retained in JSON.

| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |
|---|---:|---:|---:|---:|---:|---:|
| single-import-repeat | 3.752 / 16.518 / 16.711 | 0.198 | 3.57 | 0.727 / 3.455 / 7.234 | 9.87 / 46.61 | 0.250 / 1.300 / 1.445 |

Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.

| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |
|---|---:|---:|---:|---:|---:|---:|
| single-import-repeat / 167971499 | 98.2% | 0.446 / 1.985 / 2.580 | 1.933 / 10.143 / 13.474 | 28.61 | 1.142 / 6.096 / 7.329 | 0 |
| single-import-repeat / 167971500 | 94.5% | 0.269 / 1.410 / 1.540 | 1.041 / 8.294 / 8.977 | 28.68 | 0.674 / 5.876 / 6.117 | 0 |
| single-import-repeat / 167971515 | 98.2% | 0.376 / 2.050 / 2.110 | 1.556 / 9.503 / 10.163 | 28.57 | 0.865 / 6.641 / 7.018 | 0 |
| single-import-repeat / 167971516 | 98.2% | 1.389 / 2.575 / 2.720 | 2.192 / 8.888 / 9.113 | 27.47 | 1.097 / 5.036 / 6.603 | 0 |

Minimum sampled available memory: gateway 13.04 GiB; builders 27.47 GiB.
No OOM kills or swap activity were observed in covered phase intervals.
Largest phase/host mean memory PSI was 0.0000%; transient maxima are retained in JSON.

Work and CPU pressure vary by builder and recipe. Low batch averages do not justify raising all concurrency limits: inspect the busiest execution intervals and queue/admission behavior.

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
python3 scripts/build_load_report.py --root docs/benchmarks/build-cache-single-import-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-cache-single-import-2026-09-29
```
