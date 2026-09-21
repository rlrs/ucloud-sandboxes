# Release 0.5.73 production verification

Runtime commit `c612136ea87ab0649531a7021630a7fe31331058` was pushed to
`codex/checkpoint-publication-efficiency` and deployed at **16:19:05 UTC on
September 21, 2026** in DFM Pretraining, project
`4827bd3a-4e74-4393-9b82-49f71636c141`.

This release removes historical-record scans from storage capacity admission,
heartbeat inventory decoding and volume-scoped retirement checks. The
[analysis and local benchmark](storage-journal-hotpaths-2026-09-21.md) describes
the changes and their safety checks.

## Rollout

Preflight found zero sandbox routes, pending creates and reservations, with no
fresh worker heartbeats. All 95 installed gateway package files matched the
release wheel. Both sandbox and builder bundles passed Linux boot validation,
and the configured bundle root is `/work/ucloud-sandboxes/release/0.5.73`.
The shared gateway reconciliation completed with services active. There was no
active older worker fleet requiring an in-place upgrade.

Canary worker **12398113** booted on 0.5.73. The storage Python files matched the
wheel byte-for-byte. Its actual journal contained `volumes_live_capacity` and
`retired_devices_volume`; `EXPLAIN QUERY PLAN` confirmed capacity admission uses
`SEARCH volumes USING COVERING INDEX volumes_live_capacity (state=?)`.
The native backend remained
`75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`.
No SDK, Verifiers or native binary update is required.

## Verification

- Canonical local checks passed: 1,049 server tests (six platform skips), 118 SDK
  tests, Go tests, Ruff, shellcheck and server/SDK wheel-install checks.
- 173 targeted tests passed against the staged wheel on the Linux production host,
  including the new bounded-query and existing-journal index tests.
- Python 3.10 and 3.13 [CI jobs](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35624715930)
  passed for the runtime commit.
- Canary passed 16 published-checkpoint park/detach/restore cycles and 16 local
  park/resume cycles. SDK exec worked, process identity persisted, counters
  advanced after every wake, and lifecycle retries were zero.
- Four additional concurrent sandboxes each passed two park/resume cycles with
  SDK exec and zero lifecycle retries. That concurrent test took 3.88 seconds,
  excluding capacity preparation.

Sequential canary timings:

| Operation | Median | Maximum |
| --- | ---: | ---: |
| Park before published restore | 172 ms | 329 ms |
| Publish and detach | 411 ms | 1,801 ms |
| Wake from published checkpoint | 662 ms | 760 ms |
| Local park | 166 ms | 212 ms |
| Local wake | 401 ms | 477 ms |

Local compaction completed and adopted two replacements with zero failures or
deferred jobs. The published-local cache recorded 130 hits and 16 misses during
the sequential tests. Publication compaction retained the original base.

Final inventory had no sandbox routes, pending creates or reservations. Worker
12398113 reported zero active sandboxes and no quarantine. Gateway, relay and
autoscaler were active. All 20 public health requests returned 200 and 0.5.73,
with **16.0 ms median and 27.7 ms maximum** latency.

[Raw deployment, canary, concurrent-test and final-health results](../benchmarks/release-0.5.73-live-smoke-2026-09-21.json).
These are light-load correctness checks and confirmation of the deployed query
plan; they do not establish a production throughput gain or loaded 256/512 capacity.
