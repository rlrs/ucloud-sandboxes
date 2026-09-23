# Positioned backing reads and I/O pressure scaling

The 0.5.103 reused-worker test failed latency acceptance: 256 guests completed
2,048 correct cycles, but measured response-ready-to-wake p95 was 25.969 s and
response-ready-to-usable-exec p95 was 28.576 s. This is not release acceptance.
The preceding forced-park 64-guest run on one worker completed all cycles but
missed the same tool target at 1.298 s p95. The earlier 0.5.100 forced-park run
spread 64 guests over six workers; it is not a comparable density result.

A 20-second, 1-in-64 sampled direct-compaction trace on worker 12399540 found
95 backing ext4 buffered-read stacks, no XFS-fault stacks and no ext4-write
stacks. Native ublk queues were blocked in asynchronous page-cache readahead
allocation. The filesystem read-ahead fix removed one path, not this second
layer of speculation. The instrumented 256 run is diagnostic, not clean
performance qualification.

The added AgentENV patch advises buffered LocalFile descriptors with
POSIX_FADV_RANDOM on Linux. Positioned reads still use the page cache and
writes remain buffered. Direct-I/O paths, non-Linux behavior, hybrid writable
format v2, gVisor, and OS dependencies are unchanged. The advice prevents
speculative sequential kernel reads outside the backend's explicit ranges.
Errors are checked using the return code from posix_fadvise.

The autoscaler also treats sustained full I/O PSI as pressure even when CPU
and software queue usage are low. It uses the existing headroom policy:
no extra worker when idle capacity exists, one addition at a time, bounded
by the configured fleet maximum. This is a capacity signal, not a new client
admission error. Existing serialized configurations receive the new default.

## Validation before load qualification

- Linux Rust tests: 257 passed across local-file, image, LSMT, cache and daemon
  modules; three upstream ignored tests. OverlayBD clippy passed.
- Real io_uring discard/rewrite/reopen test on worker 12399540 passed.
- Isolated old-to-new, new-to-old and full-cache gVisor restore cases passed.
- 111 Linux Python tests passed, covering packaging, VM init, scaling,
  configuration and workload timing helpers.
- Same-worker native microbenchmark, old → new: random mixed IOPS 95,182 →
  109,150; sequential writes 1.323 → 1.309 GB/s; metadata .417 → .425 s.
  Uncached buffered sequential reads regressed substantially (old samples
  3.835/1.384 GB/s, new .402/.423 GB/s). The native-loop comparisons vary too.
  Both old and new fail the benchmark's random-I/O-within-15%-of-loop gate;
  this is not an all-gates pass. Full workload qualification must establish
  whether the latency benefit outweighs lost speculative read throughput.

Raw compatibility, I/O, previous load reports and compaction trace are in
[the benchmark directory](../benchmarks/positioned-read-advice-2026-09-23/).
The native artifact SHA is
`1467ab24852d33d5ad20c0c78855cbf8eb3260c5d4aeda5d761a001e29f94feb`.

## Production result

Deployed commit 1640dad at 02:57:58 UTC. Fresh worker 12399546 verified the
expected native SHA and 4 KiB device read-ahead. Forced 64 × 4 completed all
256 cycles correctly: measured wake p95 .889 s, confirmed first tool 1.155 s.
Immediately reusing that worker, rolling 256 × 8 completed 2,048 correct cycles:
wake p95 .470 s, first tool p95 1.074 s. After provisioning, tool p95 was .893 s;
including launch overlap, that phase was 3.357 s. The 20-second diagnostic trace
found no application-triggered compaction stacks, only background kcompactd.
This run was profiled after missing the launch-overlap target, so it is not
a clean latency acceptance run. Placement was 116/106/32/2 over four workers.

A subsequent rolling 512 × 8 run failed after 2,189 cycles with a timeout.
Cleanup succeeded, but correctness and SLO both failed. Measured wake p95
2.962 s, confirmed tool p95 8.751 s; guest tool p95 .127 s. Gateway CPU was
about 92% occupied overall, with gateway and relay processes each near a core.
A 45-second gateway GIL-switch-interval trial (.005 → .001 seconds) began
after the scenario failure had stopped most traffic. Its results are invalid
for comparison; .005 was restored automatically and no tuning was shipped.

Both sizes retain every raw report; neither qualifies the requested target.
