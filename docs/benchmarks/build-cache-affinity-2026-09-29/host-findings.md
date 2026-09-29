# Build-load host findings

Observed 96/96 successful builds across 2 phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.

Gateway API CPU averaged 0.068–0.070 occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.

| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |
|---|---:|---:|---:|---:|---:|---:|
| affinity-seed | 48/48 | 16 | 0.007 | 668 | 17.819 | 0.273 |
| affinity-repeat | 48/48 | 16 | 0.006 | 309 | 23.607 | 0.346 |

CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. For affinity-seed and affinity-repeat, the SDK driver was launched through SSH, not a ucloud-build-load-client*.service cgroup recognized by the sampler. Gateway benchmark_driver attribution is unavailable, not zero CPU. Whole-host CPU includes this work; API and registry attribution remain valid.

| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| affinity-seed | 99.2% | 0.405 / 1.575 / 1.905 | 0.070 / 0.145 / 0.215 | 0.110 / 0.345 / 0.735 | N/A | 0/17; 11.71 |
| affinity-repeat | 97.0% | 0.496 / 1.770 / 2.690 | 0.068 / 0.145 / 0.235 | 0.139 / 0.475 / 0.925 | N/A | 0/14; 45.61 |

Observed 0 failed health probes among 31 samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.

The gateway disk below is configured as `sdb` (the registry volume in this run). Disk busy percentage remains excluded because earlier qualification exposed unreliable counter jumps; it is not used as capacity evidence. This run's anomaly records are retained in JSON.

| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |
|---|---:|---:|---:|---:|---:|---:|
| affinity-seed | 5.124 / 18.834 / 37.484 | 0.620 | 3.72 | 0.686 / 1.706 / 6.503 | 11.32 / 35.85 | 0.367 / 1.220 / 1.280 |
| affinity-repeat | 4.700 / 19.649 / 21.319 | 0.441 | 3.76 | 0.635 / 2.335 / 4.167 | 11.77 / 38.36 | 0.348 / 1.229 / 1.475 |

Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.

| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |
|---|---:|---:|---:|---:|---:|---:|
| affinity-seed / 167966880 | 99.2% | 4.145 / 6.100 / 6.165 | 6.867 / 17.814 / 23.288 | 26.09 | 2.695 / 8.077 / 12.448 | 0 |
| affinity-seed / 167966883 | 97.6% | 2.604 / 6.260 / 6.970 | 6.522 / 25.686 / 33.393 | 25.45 | 2.239 / 12.455 / 15.501 | 0 |
| affinity-seed / 167966886 | 97.6% | 4.147 / 6.414 / 6.610 | 8.052 / 21.275 / 22.337 | 25.89 | 3.147 / 11.134 / 22.993 | 0 |
| affinity-seed / 167966887 | 99.2% | 3.764 / 6.276 / 6.510 | 6.935 / 19.333 / 22.305 | 26.05 | 4.278 / 21.720 / 36.057 | 0 |
| affinity-repeat / 167968139 | 99.0% | 3.459 / 5.982 / 6.290 | 5.725 / 16.592 / 19.533 | 26.55 | 1.158 / 4.932 / 8.458 | 0 |
| affinity-repeat / 167968141 | 99.0% | 4.512 / 6.600 / 6.840 | 9.748 / 25.697 / 30.155 | 25.51 | 1.085 / 4.803 / 6.302 | 0 |
| affinity-repeat / 167968156 | 99.0% | 3.011 / 5.760 / 5.935 | 4.409 / 14.065 / 14.391 | 26.39 | 1.219 / 4.963 / 9.401 | 0 |
| affinity-repeat / 167968157 | 99.0% | 3.237 / 6.815 / 7.060 | 7.600 / 31.786 / 35.687 | 25.62 | 0.863 / 4.738 / 7.034 | 0 |

Minimum sampled available memory: gateway 12.90 GiB; builders 25.45 GiB.
No OOM kills or swap activity were observed in covered phase intervals.
Largest phase/host mean memory PSI was 0.0149%; transient maxima are retained in JSON.

Work and CPU pressure vary by builder and recipe. Low batch averages do not justify raising all concurrency limits: inspect the busiest execution intervals and queue/admission behavior.

Builders whose retained samples lie entirely outside a phase and which owned none of its builds are omitted from that phase's tables. JSON marks them `outside_phase_window`; no missing-data conclusion is drawn for a replaced pool.

Limits:

- These results describe the sampled image-build pipeline. They do not qualify 500 or 1,000 running agent sandboxes, model-relay traffic, or their mixed load.
- Batch averages include submission, queueing, execution and completion tails. Repeated fixture graphs can share BuildKit work. Execution overlap includes build, push and EROFS publication, not only CPU-heavy RUN instructions.
- The gateway-local SDK driver consumes host CPU. Process groups are exclusive, but short-lived processes and asynchronous counter reads limit exact attribution. Never subtract independently calculated percentiles.
- Disk busy_percent is deliberately excluded from all conclusions and tables because earlier qualification exposed unreliable busy_ms counter jumps. This run's raw counters and any flagged anomalies are retained in JSON. No disk-utilization ceiling is inferred.
- Disk bytes, request latency, queue depth, PSI and iowait are separate signals. Traffic includes buffering, writeback, metadata, cache exports and maintenance. Physical reads near zero can mean page-cache hits. Do not sum overlapping devices or network interfaces.
- Low gateway CPU does not establish why admission returned 503 or why a build queued. Correlate per-build phases before assigning a specific cause or raising concurrency.
- Telemetry means are time-weighted and p95 is nearest-rank over sampled intervals. API/client p95 uses linear interpolation. Only complete counter intervals inside each client window are included; short phases lose a larger fraction at their boundaries.
- Health probes originate on the gateway; SDK timing includes a gateway-local client. Neither is an external end-to-end capacity test. No OOM delta means none observed in covered intervals, not a complete kernel-log audit.
- For affinity-seed and affinity-repeat, the SDK driver was launched through SSH, not a ucloud-build-load-client*.service cgroup recognized by the sampler. Gateway benchmark_driver attribution is unavailable, not zero CPU. Whole-host CPU includes this work; API and registry attribution remain valid.

Reproduce after copying complete raw telemetry and phase summaries:

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/build-cache-affinity-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-cache-affinity-2026-09-29
python3 docs/benchmarks/build-cache-affinity-2026-09-29/host-attribution.py
```
