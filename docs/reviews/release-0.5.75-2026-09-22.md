# Release 0.5.75: production rollout and realistic load results

Commit `746ad507d03e0f197fb4d47640f4e94112a94382` was pushed to
`codex/checkpoint-publication-efficiency`. It includes the PostgreSQL relay
implementation already qualified/activated in production, subsequent gateway
and worker coordination improvements, durable journal batching, warm retention,
size-tiered compaction, and cancellation of deleted volumes' maintenance work.
Automatic trim is still disabled. No SDK or Verifiers update is required.

The exact server wheel SHA-256 is
`69bd3d2d73f11602c0f72ecd97ee932b7cec56b26bdc02a07f0204099d4566b8`.
Linux validation passed **1,195 tests, 10 skipped**, with the real PostgreSQL
qualification database enabled, in 101.818 seconds. Ruff and diff checks passed.
The built wheel installed and passed the installed-package verifier in a clean
Linux virtual environment. [Evidence](../benchmarks/release-0.5.75-2026-09-22/status.json).

## Recovery and rollout

UCloud suspended gateway **12379311** at 09:17 UTC on September 22 for shared
workspace storage quota. Cleanup and delayed accounting restored **13,376 GB**
of headroom (159,624 / 173,000 GB). See
[the storage investigation](storage-quota-2026-09-22.md).

The provider also removed the gateway's data-drive resource. Reattached the
existing `/998037` drive and cleanly stopped PostgreSQL/services before another
suspend/resume to apply the mount. `/work/data` returned; gateway-local PostgreSQL
data survived. Preserved PostgreSQL authority, the SQLite cutover fence, and the
mount guards. Triggered and verified the PostgreSQL backup before installation.

Installed **0.5.75 at 11:24:12 UTC** in the actual qualification venv and the
legacy venv still used by the registry. Updating both fixed the registry's old
configuration parser, which rejected `relay_postgres`. Exact wheel validation
matched 110 package files. All five running workers were upgraded, and future
worker/builder bundles were published. Both bundles passed validation on the
actual worker kernel, `7.0.0-30-generic`; the resumed gateway uses kernel 31 and
cannot substitute for that worker qualification. Autoscaling resumed afterward.

Worker 12398541 held an unrouted sandbox from the earlier `super-r5-...` run.
Parked it to preserve its state before upgrading; it was not deleted. An older
migration on worker 12397503 broke expiry cleanup and heartbeat responses. The
subsequent **0.5.76** patch defers opportunistic deletion to the migration's
fenced cleanup path. See [the follow-up rollout](release-0.5.76-2026-09-22.md).

Also restored telemetry: its Docker port binding still used the gateway's old
private IP. Updated the binding to the recovered gateway IP and verified collector,
VictoriaMetrics, and Tempo health. The installer still uses a fixed private bind
address; another provider IP change requires updating that binding.

## Realistic public-path qualification

Linux driver: `rasmus-dev`, separate from the production gateway. Each managed
agent retains 128 MiB incompressible memory, dirties 16 MiB, writes 64 × 64 KiB
files, runs 100 ms CPU work, waits 10–15 seconds for a synthetic model response,
then verifies process identity, memory, files, and a subprocess result. No model
API is called. Measurements exclude the first warmup cycle.

| Run | Completed cycles | Wake p95 | Submit-to-verified-exec p95 | Natural response-ready-to-exec p95 |
| --- | ---: | ---: | ---: | ---: |
| 4 × 3 natural | 12/12 | 0.697 s | 1.263 s | 1.264 s |
| 64 × 8 natural | 512/512 | 0.248 s | 0.709 s | **0.710 s** |
| 256 × 8 natural | 2,048/2,048 | **1.504 s** | 2.177 s | **2.177 s** |
| 64 × 3 wait-for-park | 192/192 | 0.812 s | 1.391 s | 43.132 s including imposed wait for parking |

All four runs passed integrity, health, and cleanup checks, with no harness
control retries or guest transport retries. Full raw results are in
[`live/`](../benchmarks/release-0.5.75-2026-09-22/live/).
The 0.8-second full-path target passed at 64 natural agents and **failed at 256**.

Natural measured cycles with parking observed were 2/8, 8/448, and 9/1,792.
Inventory sampling can miss transient parking; these are observed counts, not
proof of every lifecycle transition. Warm retention dominates the natural runs.
The separate wait-for-park run observed parking in all 128 measured cycles. That
mode deliberately withholds a ready response until parking is observed, so its
43-second response-ready latency includes the imposed wait. It is not a normal
product latency result; its 0.812-second submit-to-wake p95 measures the parked path.

The 256 run placed 104/84/68 sandboxes on workers 12398539/12398500/12398499.
The preserved-orphan worker remained draining, and worker 12397503 was unavailable
until the follow-up heartbeat fix. Worker 12398499 sampled about 2.2% full I/O
pressure and no memory pressure during the 64 run, with 94–95% CPU idle in that
short sample. Do not generalize that sample to peak 256 load.

## Remaining bottleneck evidence

Only 9 measured natural 256 cycles observed parking. p95 post-wake execution was
0.726 s, guest memory verification 0.146 s, and guest subprocess execution 0.148 s.
The coordination path remains a major contributor; more storage optimization
alone cannot establish the latency target.

Across the smoke/natural/parked sequence, the relay's 16-connection PostgreSQL
pool recorded 5,619 queued requests, 410,460 ms aggregate wait, and 64,971 total
requests at the sampled point. This is evidence of contention, not proof that
raising the pool maximum improves throughput. Profile transaction/lock hold time
and lifecycle notification dispatch before selecting the next change. Gateway
wake admission/reservation also showed long-tail waits in coarse histogram
telemetry. Production correctness has been validated at 256; **sub-0.8-second
wake at that load is not achieved**.
