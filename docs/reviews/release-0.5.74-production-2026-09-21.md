# Release 0.5.74 production verification

Runtime commit `1b43a4dc7778e78a12f55696cdda0bcc2dfb3bbc` was pushed to
`codex/checkpoint-publication-efficiency` and deployed at **16:54:42 UTC on
21 September 2026** in DFM Pretraining, project
`4827bd3a-4e74-4393-9b82-49f71636c141`.

The release scopes lifecycle migration-history reads to one sandbox, avoids
redundant SQLite permission writes, and records relay lifecycle-lock and gateway
placement-lock wait times. See the [production investigation](production-lifecycle-followup-2026-09-21.md).

## Rollout

Preflight and the immediate pre-install check found no sandbox workload or
capacity reservations. There were no fresh workers to upgrade in place. Both
sandbox and builder bundles passed Linux boot validation; future nodes use
`/work/ucloud-sandboxes/release/0.5.74`.

All 95 gateway package files matched the release wheel. Shared gateway
reconciliation completed and gateway, relay, registry, and autoscaler were
active. Canary worker **12398126** booted on 0.5.74. Its storage files matched the
wheel, and its live journal used the expected capacity covering index. The native
backend binary is unchanged. No SDK or Verifiers update is required.

## Verification

- Canonical local checks passed: 1,051 server tests (six skips), 118 SDK tests,
  Ruff, shell checks, Go tests, and server/SDK wheel-install verification.
- 302 targeted tests ran successfully against the staged wheel on the Linux
  production host (one skip).
- Python 3.10 and 3.13 [CI jobs](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35628491184)
  passed for the runtime commit.
- Canary passed 16 published-checkpoint park/detach/restore cycles and 16 local
  park/resume cycles, with SDK execution and process/counter continuity.
- Four concurrent sandboxes each passed two park/resume cycles with SDK
  execution, in 3.88 seconds excluding capacity preparation.
- Across all **40 cycles**, park, detach, and wake retries were **zero**.
- Publication compaction retained the original base. Local compaction adopted
  two replacements with zero failures or deferred jobs; the local published
  cache recorded 130 hits and 16 misses during sequential testing.

| Sequential operation | Median | Maximum |
| --- | ---: | ---: |
| Park before published restore | 161 ms | 242 ms |
| Publish and detach | 423 ms | 1,717 ms |
| Wake from published checkpoint | 679 ms | 953 ms |
| Local park | 161 ms | 213 ms |
| Local wake | 412 ms | 456 ms |

The new `gateway.placement.lock_wait_seconds` attribute appeared in production
traces, with 43–75 microseconds in the four inspected observations. This confirms
telemetry export at light load, not an estimate for the earlier overloaded run.

At **16:57:05 UTC**, public gateway health passed 20/20 checks with **16.5 ms
median and 22.4 ms maximum** latency. Relay health reported 0.5.74. All four
services were active. No sandbox routes, pending creates, or reservations
remained; the canary reported zero active sandboxes and no quarantine.

[Raw rollout and live-test evidence](../benchmarks/release-0.5.74-live-smoke-2026-09-21.json).
These are light-load correctness checks. They do not qualify 256/512 concurrent
agents or establish the loaded performance gain; the next real run can use the
new wait timing to locate remaining coordination delays.
