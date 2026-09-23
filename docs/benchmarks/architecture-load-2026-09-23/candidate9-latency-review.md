# Candidate 9: 256-agent latency review

Read-only review of `architecture-candidate9-natural256.summary.json`, its raw
2,048-cycle report, and retained Tempo spans. No profiling, production mutation or
benchmark timing-definition change was made.

The run completed correctly, but launch-overlap useful-action p95 was 1.055 s;
steady-state p95 was 0.931 s. Continuation p95 was 0.588 s during launch and
0.486 s afterward. These are warm natural waits, not forced-restore qualification.

Across the ten slowest launch cycles, median continuation was 0.611 s versus
0.287 s for all launch cycles. Median post-continuation exec wait was 0.201 s
versus 0.077 s. Median response commit was only 0.065 s versus 0.053 s; durable
commit delay does not explain most of that tail.

For sandbox suffix `0047`, cycle 0, useful action took 1.220 s. Retained trace
`123da658a23c56a30a2a7eb61a9fc4a4` measured gateway exec HTTP at 0.101 s, worker
HTTP at 0.063 s, exec admission at 0.012 s, and guest exec session at 0.437 s.
Its in-guest tool took 0.124 s and integrity scan 0.917 s. Fast launch sandbox
`0060` took 0.399 s overall, with tool 0.025 s and integrity scan 0.327 s.
They ran on different workers. The sampled data supports guest/worker contention
for this example; it does not establish gateway CPU saturation.

The benchmark guest begins scanning its entire 512 MiB resident allocation just
after sending the continuation receipt. That scan overlaps the driver's uploaded
tool in the same one-vCPU sandbox. Across all cycles, integrity duration and
post-continuation exec-wait duration have Pearson correlation 0.811. This is a
credible contention mechanism, not proof that the scan is the sole cause. The
existing CPU metric samples are too short/infrequent to rule out bursts or
per-sandbox throttling; cumulative CPU accounting starts with candidate 10.

Do not silently move that verification work outside the measured interval and
claim a product speedup. A bounded diagnostic comparison should measure sandbox
CPU throttling and compare the same workload with a different CPU allocation;
keep the acceptance profile fixed. The rare approximately 2.2-second continuation
tails had normal guest tool times, but no retained matching traces, so their
cause remains unassigned. Likewise, the slowest launch request was not sampled.

Selected, credential-free trace fields are in `candidate9-selected-traces.jsonl`.
