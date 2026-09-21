# Release 0.5.72 production verification

Runtime commit `28e83405028921d761bd65541980adb1eae9962b` was pushed to
`codex/checkpoint-publication-efficiency` and deployed at **16:02:54 UTC on
September 21, 2026** in DFM Pretraining, project
`4827bd3a-4e74-4393-9b82-49f71636c141`.

This release fixes hardlink-induced invalidation of completed checkpoint uploads
and unnecessary serialization of uncapped native device acquisition. See the
[analysis and regression evidence](storage-upload-and-acquisition-hotspots-2026-09-21.md).

## Rollout

Preflight found no sandbox routes, pending creates, reservations or fresh worker
heartbeats. The gateway package's 95 files matched the staged wheel; both sandbox
and builder bundles passed Linux boot validation. The configured bundle root is
`/work/ucloud-sandboxes/release/0.5.72`, so future nodes receive the same release.
Shared gateway reconciliation restarted the control-plane services successfully.
There was no active older worker fleet requiring an in-place upgrade.

Canary worker **12398108** booted on 0.5.72. Both changed storage Python files
matched the release wheel byte-for-byte. The native backend hash remained
`75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`.
Production's explicit device ceiling is disabled (0), so the concurrent device
acquisition path is in use. No native binary, SDK or Verifiers update is required.

## Validation

- Canonical local checks passed: 1,045 server tests (six platform skips), 118 SDK
  tests, Go tests, Ruff, shellcheck and server/SDK wheel-install checks.
- 169 targeted tests passed on the Linux production host against the staged wheel.
- Both Python 3.10 and 3.13 [CI jobs](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35622898128)
  passed for the runtime commit.
- Canary passed 16 park/publish/detach/restore cycles plus 16 local park/resume
  cycles. SDK exec succeeded, process identity persisted, counters advanced after
  every wake, and lifecycle retries were zero.
- Four additional sandboxes ran concurrently on the canary worker, each completing
  two park/resume cycles with SDK exec. All eight cycles passed with zero lifecycle
  retries; the concurrent test took 4.24 seconds, excluding capacity preparation.

Sequential canary timings:

| Operation | Median | Maximum |
| --- | ---: | ---: |
| Park before published restore | 178 ms | 230 ms |
| Publish and detach | 443 ms | 1,827 ms |
| Wake from published checkpoint | 735 ms | 1,030 ms |
| Local park | 181 ms | 234 ms |
| Local wake | 438 ms | 484 ms |

Local compaction completed and adopted two replacements with zero failures or
deferred jobs. The published-local cache recorded 130 hits and 16 misses during
the sequential tests. Completed-upload reuse remained zero in this canary; the
hardlink race and retry reuse were verified by regression tests, not observed
naturally during this light workload.

Final inventory had no sandbox routes, pending creates or reservations. Worker
12398108 had zero active sandboxes and no quarantine. Gateway, relay and
autoscaler were active. All 20 public health requests returned 200 and 0.5.72,
with **15.6 ms median and 29.5 ms maximum** latency.

[Raw deployment, canary, concurrent-test and final-health results](../benchmarks/release-0.5.72-live-smoke-2026-09-21.json).
These checks establish light-load correctness; they do not measure a production
throughput gain or qualify loaded 256/512 concurrency.
