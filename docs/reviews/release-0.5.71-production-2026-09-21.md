# Release 0.5.71 production verification

Runtime commit: `59f95b68a153930d4fc2327f052c88d9301f4458`, pushed to
`codex/checkpoint-publication-efficiency`. Deployed September 21, 2026 at
**15:24:41 UTC** in DFM Pretraining, project
`4827bd3a-4e74-4393-9b82-49f71636c141`.

## Rollout

Preflight found zero sandbox routes, pending creates and capacity reservations,
with no fresh worker heartbeats. All 95 installed gateway package files matched
the release wheel. Sandbox and builder bundles passed Linux boot validation;
the autoscaler now uses `/work/ucloud-sandboxes/release/0.5.71`. There was no
active pre-existing worker fleet to upgrade. No provider stop was issued by the
release scripts; ordinary autoscaling still manages idle canary capacity.

The shared `gateway-reconcile` routine converged the services. The old relay
exhausted its 90-second stop grace period and systemd killed it; preflight had
confirmed no active callers or pending deliveries. New services became active
and health returned the exact version. Resetting the absent S3 snapshot-GC unit
emitted a harmless message on this filesystem-registry deployment; that optional
action is non-fatal. Native storage and gVisor artifacts were unchanged.

## Verification

- Local checks: 1,042 server tests, six platform skips; 118 SDK tests; Go tests;
  server/SDK wheel-install checks; Ruff and shell syntax. Shellcheck was initially
  absent from PATH, then ran separately from its existing temporary installation
  and passed all scripts.
- Linux production-host qualification: 166 targeted tests passed against the
  staged wheel before installation, covering the changed runtime paths.
- Both Python 3.10 and 3.13 [CI jobs](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35618408485)
  passed for the runtime commit.
- Newly reserved worker **12398091** booted on **0.5.71**, with backend hash
  `75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`, memory
  watermark scale factor 100 and heartbeat working directory `/`.

The final canary passed 16 park/publish/detach/restore cycles and 16 local
park/resume cycles. Both used sandbox-scoped SDK exec for waking, preserved
process identity, advanced the application counter after every wake and needed
**zero lifecycle retries**.

| Operation | Median | Maximum |
| --- | ---: | ---: |
| Park before published restore | 174 ms | 220 ms |
| Publish and detach | 432 ms | 1,814 ms |
| Wake from published checkpoint | 706 ms | 979 ms |
| Local park | 171 ms | 254 ms |
| Local wake | 445 ms | 473 ms |

Published chain depths were
`1, 3, 4, 5, 6, 7, 8, 2, 3, 4, 5, 6, 7, 8, 2, 3`.
Compaction retained the original 241,238,016-byte base every cycle; the last
merged delta was 9,830,400 bytes.

Local compaction completed and adopted two replacements, with zero failures or
deferred jobs, 98,258,944 estimated input bytes and 17,915,904 output bytes.
Local published-cache counters showed 203 hits, 18 misses, 23 retained entries
and 604,073,984 retained bytes. Cache counters are cumulative on the canary and
include the initial attempt below, not an isolated hit-rate measurement.

An initial eight-cycle benchmark passed, but its older reporting wrapper then
called the `registry_url` property as a function and failed. Correcting that
wrapper produced the final passing 16+16-cycle result without changing the
deployed runtime. The initial attempt is not counted as a successful complete
release check.

Final inventory contained zero routes, pending creates and reservations. Worker
12398091 reported 0.5.71, zero active sandboxes, open admission and no quarantine.
Gateway, relay and autoscaler were active. Twenty public health requests all
returned 200 with version 0.5.71: **16.4 ms median, 23.6 ms maximum**.

[Raw deployment and canary results](../benchmarks/release-0.5.71-live-smoke-2026-09-21.json).
This is light-load correctness qualification, not evidence of loaded 256/512
throughput or measured production I/O reduction. No SDK or Verifiers update is
required. Filesystem trim and fixed parking delays remain disabled.
