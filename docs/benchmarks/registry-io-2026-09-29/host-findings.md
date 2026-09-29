# Build-load host findings

Observed 48/48 successful builds across 1 phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.

Gateway API CPU averaged 0.070–0.070 occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.

| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |
|---|---:|---:|---:|---:|---:|---:|
| io-repeat | 48/48 | 16 | 0.006 | 711 | 19.518 | 0.266 |

CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. The SDK driver runs on the gateway and has its own attribution.

| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| io-repeat | 98.5% | 0.380 / 1.695 / 1.915 | 0.070 / 0.165 / 0.240 | 0.102 / 0.360 / 0.690 | 0.142 / 1.580 / 1.715 | 0/22; 9.64 |

Observed 0 failed health probes among 22 samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.

The gateway disk below is configured as `sdb` (the registry volume in this run). Disk busy percentage remains excluded because earlier qualification exposed unreliable counter jumps; it is not used as capacity evidence. This run's anomaly records are retained in JSON.

| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |
|---|---:|---:|---:|---:|---:|---:|
| io-repeat | 4.971 / 16.215 / 18.853 | 0.651 | 2.84 | 0.462 / 1.629 / 4.229 | 8.08 / 29.08 | 0.257 / 0.960 / 1.165 |

Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.

| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |
|---|---:|---:|---:|---:|---:|---:|
| io-repeat / 167948343 | 98.5% | 2.924 / 6.315 / 6.645 | 4.918 / 18.803 / 36.320 | 25.96 | 1.322 / 6.853 / 11.343 | 0 |
| io-repeat / 167948344 | 98.5% | 4.445 / 6.525 / 6.670 | 9.152 / 23.580 / 31.888 | 25.04 | 2.584 / 9.884 / 13.016 | 0 |
| io-repeat / 167948358 | 98.5% | 3.839 / 6.230 / 6.605 | 6.863 / 19.465 / 24.706 | 25.86 | 2.498 / 11.331 / 12.353 | 0 |
| io-repeat / 167948359 | 98.5% | 3.819 / 6.474 / 6.790 | 8.817 / 21.363 / 35.211 | 25.89 | 2.676 / 8.638 / 11.097 | 0 |

Minimum sampled available memory: gateway 13.01 GiB; builders 25.04 GiB.
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
python3 scripts/build_load_report.py --root docs/benchmarks/registry-io-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/registry-io-2026-09-29
```
