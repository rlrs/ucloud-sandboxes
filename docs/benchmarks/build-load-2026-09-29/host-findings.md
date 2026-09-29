# Build-load host findings

Observed 138/138 successful builds across 6 phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.

Gateway API CPU averaged 0.012–0.053 occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.

| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |
|---|---:|---:|---:|---:|---:|---:|
| cold | 12/12 | 12 | 0.014 | 0 | 8.770 | 2.021 |
| warm | 24/24 | 14 | 2.941 | 0 | 61.318 | 2.539 |
| app | 24/24 | 16 | 47.080 | 3 | 21.384 | 3.170 |
| dependency | 6/6 | 6 | 0.012 | 0 | 12.402 | 3.839 |
| overload | 48/48 | 16 | 108.377 | 133 | 18.576 | 1.525 |
| replacement | 24/24 | 16 | 99.967 | 0 | 9.661 | 2.746 |

CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. The SDK driver runs on the gateway and has its own attribution.

| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| cold | 98.2% | 0.228 / 0.845 / 1.335 | 0.020 / 0.035 / 0.110 | 0.106 / 0.505 / 1.100 | 0.019 / 0.010 / 1.220 | 0/28; 11.53 |
| warm | 98.4% | 0.457 / 1.760 / 2.090 | 0.027 / 0.080 / 0.165 | 0.177 / 0.685 / 1.320 | 0.139 / 1.640 / 1.790 | 0/10; 11.81 |
| app | 98.0% | 0.262 / 1.170 / 1.655 | 0.028 / 0.075 / 0.125 | 0.083 / 0.265 / 0.635 | 0.089 / 0.690 / 1.775 | 0/16; 9.98 |
| dependency | 97.7% | 0.114 / 0.440 / 1.175 | 0.012 / 0.025 / 0.045 | 0.045 / 0.155 / 1.070 | 0.002 / 0.005 / 0.010 | 0/21; 8.90 |
| overload | 98.8% | 0.341 / 1.605 / 2.110 | 0.053 / 0.145 / 0.275 | 0.107 / 0.405 / 1.390 | 0.111 / 1.510 / 1.965 | 0/29; 11.04 |
| replacement | 98.0% | 0.254 / 1.130 / 1.585 | 0.032 / 0.060 / 0.185 | 0.114 / 0.560 / 1.350 | 0.049 / 0.015 / 1.655 | 0/25; 9.61 |

Observed 0 failed health probes among 129 samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.

The gateway disk below is configured as `sdb` (the registry volume in this run). Disk busy percentage is excluded because its raw counter produced implausible jumps; it is not used as capacity evidence.

| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |
|---|---:|---:|---:|---:|---:|---:|
| cold | 44.577 / 250.651 / 301.019 | 7.226 | 67.55 | 5.440 / 31.888 / 48.698 | 11.25 / 56.74 | 0.333 / 1.810 / 3.660 |
| warm | 8.470 / 67.405 / 83.136 | 0.513 | 6.90 | 0.892 / 4.114 / 9.011 | 14.22 / 40.90 | 0.415 / 1.220 / 1.465 |
| app | 10.267 / 31.813 / 240.572 | 0.983 | 11.27 | 1.198 / 2.821 / 38.385 | 6.98 / 24.50 | 0.223 / 0.820 / 1.435 |
| dependency | 16.854 / 99.069 / 289.177 | 2.074 | 67.17 | 2.341 / 11.257 / 48.478 | 6.51 / 39.26 | 0.195 / 1.230 / 3.335 |
| overload | 19.544 / 157.581 / 300.720 | 3.245 | 20.42 | 2.468 / 18.844 / 54.475 | 9.75 / 47.00 | 0.328 / 1.325 / 3.350 |
| replacement | 28.629 / 216.315 / 302.350 | 4.082 | 35.65 | 3.306 / 25.674 / 46.343 | 11.54 / 64.12 | 0.368 / 2.010 / 3.345 |

Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.

| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |
|---|---:|---:|---:|---:|---:|---:|
| cold / 167927353 | 99.4% | 2.050 / 3.510 / 4.855 | 2.244 / 6.682 / 20.529 | 27.54 | 1.292 / 6.493 / 8.262 | 0 |
| cold / 167927354 | 99.4% | 1.691 / 3.455 / 3.955 | 1.579 / 5.014 / 7.612 | 27.38 | 1.129 / 6.709 / 8.702 | 0 |
| cold / 167927367 | 99.4% | 1.715 / 5.350 / 6.305 | 2.583 / 9.718 / 15.974 | 25.92 | 1.010 / 5.276 / 9.643 | 0 |
| cold / 167927368 | 98.2% | 0.859 / 2.375 / 3.435 | 0.928 / 5.138 / 7.201 | 27.69 | 0.373 / 2.178 / 3.938 | 0 |
| warm / 167927353 | 95.2% | 1.611 / 3.015 / 3.140 | 1.946 / 10.607 / 12.748 | 27.19 | 2.044 / 15.979 / 16.465 | 0 |
| warm / 167927354 | 95.2% | 1.850 / 3.635 / 3.635 | 2.137 / 12.822 / 13.577 | 27.15 | 1.787 / 11.430 / 13.928 | 0 |
| warm / 167927367 | 95.2% | 2.512 / 4.575 / 4.650 | 3.834 / 10.293 / 12.205 | 26.96 | 2.734 / 12.226 / 17.909 | 0 |
| warm / 167927368 | 98.4% | 1.246 / 2.890 / 3.310 | 1.268 / 7.849 / 10.424 | 27.27 | 0.509 / 3.606 / 4.960 | 0 |
| app / 167927353 | 98.0% | 3.908 / 5.900 / 6.011 | 6.339 / 13.800 / 22.046 | 25.33 | 2.487 / 9.350 / 11.074 | 0 |
| app / 167927354 | 98.0% | 1.703 / 4.625 / 4.880 | 2.231 / 12.028 / 26.213 | 26.40 | 1.616 / 11.604 / 19.571 | 0 |
| app / 167927367 | 98.0% | 2.110 / 5.740 / 6.010 | 3.259 / 14.040 / 14.732 | 25.71 | 1.525 / 8.410 / 12.416 | 0 |
| app / 167927368 | 98.0% | 2.634 / 6.660 / 7.065 | 6.786 / 32.198 / 34.532 | 24.70 | 0.608 / 4.452 / 6.714 | 0 |
| dependency / 167927353 | 99.2% | 1.218 / 1.700 / 2.405 | 1.074 / 4.160 / 5.977 | 27.06 | 0.772 / 4.002 / 8.472 | 0 |
| dependency / 167927354 | 99.2% | 0.947 / 2.450 / 3.070 | 0.989 / 3.031 / 13.091 | 26.87 | 0.879 / 6.230 / 13.261 | 0 |
| dependency / 167927367 | 99.2% | 0.128 / 0.345 / 0.395 | 0.052 / 0.157 / 0.162 | 27.45 | 0.003 / 0.015 / 0.040 | 0 |
| dependency / 167927368 | 97.7% | 0.090 / 0.230 / 0.355 | 0.053 / 0.116 / 0.154 | 27.56 | 0.003 / 0.023 / 0.053 | 0 |
| overload / 167927353 | 98.8% | 3.396 / 6.485 / 6.910 | 7.929 / 24.596 / 28.784 | 24.54 | 2.092 / 9.192 / 15.375 | 0 |
| overload / 167927354 | 98.8% | 3.253 / 6.473 / 6.750 | 6.545 / 22.678 / 32.278 | 24.96 | 1.955 / 9.680 / 14.735 | 0 |
| overload / 167927367 | 98.8% | 3.097 / 6.625 / 6.955 | 8.128 / 26.901 / 32.805 | 24.24 | 1.971 / 10.510 / 16.201 | 0 |
| overload / 167927368 | 98.8% | 2.247 / 6.370 / 7.035 | 4.600 / 20.490 / 37.789 | 24.65 | 0.591 / 2.945 / 7.847 | 0 |
| replacement / 167933203 | 98.0% | 2.726 / 5.180 / 5.555 | 4.467 / 12.173 / 21.567 | 26.34 | 2.060 / 9.630 / 14.097 | 0 |
| replacement / 167933204 | 98.0% | 2.909 / 5.314 / 5.715 | 3.805 / 13.696 / 23.508 | 26.30 | 1.768 / 8.010 / 14.290 | 0 |
| replacement / 167933217 | 98.0% | 2.595 / 5.220 / 5.710 | 4.027 / 14.000 / 21.941 | 26.43 | 2.081 / 9.536 / 18.470 | 0 |
| replacement / 167933218 | 98.0% | 1.945 / 5.510 / 6.185 | 3.103 / 9.151 / 16.728 | 25.86 | 1.281 / 8.161 / 11.317 | 0 |

Minimum sampled available memory: gateway 12.62 GiB; builders 24.24 GiB.
No OOM kills or swap activity were observed in covered phase intervals.
Largest phase/host mean memory PSI was 0.0290%; transient maxima are retained in JSON.

Work and CPU pressure vary by builder and recipe. Low batch averages do not justify raising all concurrency limits: inspect the busiest execution intervals and queue/admission behavior.
Low measured API CPU alongside registry I/O pressure supports reducing avoidable registry transfer/publication work before changing the gateway implementation for this build workload.

Run-specific confounder: cold builds caused a sandbox worker to be provisioned while sandbox demand remained zero. Warm phases inherited that node; the autoscaler later stopped it. See `build-triggered-worker.json`; whole-host cold/warm differences are not entirely cache effects.

Builders whose retained samples lie entirely outside a phase and which owned none of its builds are omitted from that phase's tables. JSON marks them `outside_phase_window`; no missing-data conclusion is drawn for a replaced pool.

Limits:

- These results describe the sampled image-build pipeline. They do not qualify 500 or 1,000 running agent sandboxes, model-relay traffic, or their mixed load.
- Batch averages include submission, queueing, execution and completion tails. Repeated fixture graphs can share BuildKit work. Execution overlap includes build, push and EROFS publication, not only CPU-heavy RUN instructions.
- The gateway-local SDK driver consumes host CPU. Process groups are exclusive, but short-lived processes and asynchronous counter reads limit exact attribution. Never subtract independently calculated percentiles.
- Disk busy_percent is deliberately excluded from all conclusions and tables. Raw busy_ms jumps produced implausible values in this run; raw counter evidence is retained in JSON. No disk-utilization ceiling is inferred.
- Disk bytes, request latency, queue depth, PSI and iowait are separate signals. Traffic includes buffering, writeback, metadata, cache exports and maintenance. Physical reads near zero can mean page-cache hits. Do not sum overlapping devices or network interfaces.
- Low gateway CPU does not establish why admission returned 503 or why a build queued. Correlate per-build phases before assigning a specific cause or raising concurrency.
- Telemetry means are time-weighted and p95 is nearest-rank over sampled intervals. API/client p95 uses linear interpolation. Only complete counter intervals inside each client window are included; short phases lose a larger fraction at their boundaries.
- Health probes originate on the gateway; SDK timing includes a gateway-local client. Neither is an external end-to-end capacity test. No OOM delta means none observed in covered intervals, not a complete kernel-log audit.

Reproduce after copying complete raw telemetry and phase summaries:

```sh
python3 scripts/build_load_report.py --root docs/benchmarks/build-load-2026-09-29
python3 scripts/build_load_host_analysis.py --root docs/benchmarks/build-load-2026-09-29
```
