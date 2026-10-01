# Bounded build pipeline qualification

This local evidence archive contains production telemetry and operational
identities. Publication of the raw archive was rejected by automatic approval
review; the destination repository is public. The separate
[public aggregate report](../build-pipeline-2026-09-29.md) excludes those raw
records. This archive remains local.

The bounded pipeline is **enabled in production**. The matched comparison completed
all **192 measured builds** within each wave's common 600-second arrival deadline. On the same four 8-CPU, 32-GiB builders, the
dependency-invalidated burst took **291.26 seconds versus 305.18 seconds** with
the original policy; warm throughput was essentially unchanged. This is one
sequential A/B comparison, not a universal 4.56% speedup claim. Builder pressure
increased, and Docker-pull/registry-I/O tails remain substantial.

The change keeps four preparation/build/push permits and adds two finishing
permits, allowing another Docker build to start while a completed build finishes
immutable filesystem publication.

The [deployment receipt](deployment-receipt.json) records the final runtime
deployment at **20:54:30 UTC on 2026-09-29**, with
`builder.max_finishing_builds=0` retained for qualification. Gateway HTTPS, relay
HTTPS and metrics checks passed; the relay storage budget remained 64 GiB.
The [activation receipt](activation-receipt.json) records the two-finisher
future-builder policy enabled at **21:17:02 UTC**, after both candidate gates
passed. It binds their exact summary hashes and the same wheel; gateway HTTPS,
relay HTTPS and metrics checks passed. All three [guest execution checks](image-smokes/summary.json)
passed and their owned sandboxes were deleted. The [fresh worker audit](smoke-worker-identity.json)
verified its packaged Python files against the released wheel and confirmed that
automatic upgrades remain masked and inactive.

## Admission contract

| Stage | Per-builder bound | Ownership ends |
|---|---:|---|
| Context preparation and Docker build/push | 4 | After Docker succeeds and a finishing permit is obtained |
| Immutable filesystem publication and cleanup | 2 when enabled | After finalization completes |
| All owned builds | 6 when enabled | After their admission ownership is released |

[ImageManager](../../../ucloud_sandboxes/images.py) persists each phase in the
existing build-record JSON. The existing SQLite writer transaction enforces
both the total limit and the preparation/build limit across managers and
processes. Transitioning to finishing also takes that transaction. A completed
Docker build retains its first-stage permit while finishing capacity is full;
additional demand stays at the gateway. The phase wait consumes the server
execution deadline and appears in `timings.phases.finishing_wait_ms`.

Terminal status does not release ownership prematurely. Cleanup still fences
node drain, and history compaction retains the record until release. Release
intent is queued before reading SQLite, allowing a later heartbeat or poll to
retry a transient release-read/write failure. Startup reconciliation releases
ownership belonging to dead processes, including terminal cleanup records.

The gateway's live admission hint is an **absolute current ceiling**, calculated
as `active + max(0, min(4 - preparing_solving, 6 - active))`. An idle candidate therefore
advertises **4**, not 6. Concurrent dispatch accounting and the builder's atomic
reservation remain authoritative. Missing hints retain the legacy four-slot
behavior; malformed hints reject new admission. Replays and conflicting requests
reach their current owner even when full or terminal-but-still-cleaning, before
new-work capacity is checked.

The provisioned BuildKit setting remains `worker.oci.max-parallelism=4`
([configuration source](../../../ucloud_sandboxes/vm_init.py)). This is a
concurrency control, not a CPU-core quota: a RUN can use multiple cores, and
filesystem publication also consumes CPU, disk and registry capacity. It is
not a fleet-wide limit on registry HTTP operations. There is no CPU-pressure
prediction or unlimited local build queue.

## Qualification protocol and results

The [qualification harness](../../../scripts/qualify_builder_slots.py) uses the
pinned SDK **0.4.34** and 48 frozen synthetic contexts: three recipes, with
`app-change-5` through `app-change-20` for each. All tasks share one arrival time
before local context preparation. Every request's preparation, upload, admission
and completion must fit the declared deadline; a later server success does not
erase a client deadline miss.

Warm and invalidated-dependency arms compare the original four-owned-build
policy with the four-plus-two pipeline. Both dependency-invalidated arms add distinct per-case ARG
markers before dependency installation and requires observed dependency RUN
output. Base images and package sources can remain cached; this is not a proof
of empty disks or a network-cold package ecosystem. The immutable base pins and
locks remain unchanged, and no shared cache is deleted.

The harness verifies four exact builder identities, live idle state, free disk,
candidate upgrade receipts and node epochs. It drains owned work after client
failures and retains those failures. Heartbeat `active_image_builds` can also
include in-flight HTTP operation fences; it must not be presented as an exact
count of durable admission records. Driver CPU runs on the gateway and must be
separated from service CPU when interpreting host telemetry.

The baseline ran first; the same four builders then received the frozen runtime
and two-finisher policy. The earlier 48-build conditioning wave is retained
separately and excluded from the 192-build comparison below. Base/package caches
and previously published immutable layers persisted between arms. Both cold
arms recorded 48 context-cache GET 404s and 48 successful context uploads;
both warm arms recorded 48 context-cache GET 200s. The cold comparison therefore
does not have an observed context-upload-cache advantage for the candidate.

| Arm | Success / cases | Batch wall s | Arrival-to-completion p95 / max s | Submission p95 s | Deadline misses |
|---|---:|---:|---:|---:|---:|
| [Warm baseline](slotq-a-warm/summary.json) | 48/48 | 17.578 | 17.196 / 17.578 | 14.100 | 0 |
| [Warm pipeline](slotq-b-warm/summary.json) | 48/48 | 17.195 | 15.795 / 17.194 | 12.633 | 0 |
| [Dependency-invalidated baseline](slotq-a-cold/summary.json) | 48/48 | 305.181 | 290.089 / 305.179 | 175.463 | 0 |
| [Dependency-invalidated pipeline](slotq-b-cold/summary.json) | 48/48 | 291.264 | 276.900 / 291.263 | 153.285 | 0 |

All four arms had zero client errors and drained their admitted builds. Cold
submission p95 improved 12.64%, while context-preparation p95 increased from
7.184 to 9.713 seconds. Candidate cold finishing-permit wait was median 4 ms,
p95 5.778 seconds and maximum 18.296 seconds: backpressure remains visible in
the end-to-end deadline. The comparison changes overlap, not CPU allocation or
the amount of dependency work required by the fixtures.

| Release gate | Status |
|---|---|
| Future-builder policy activation, health and exact wheel/bundle identity | Passed, [receipt](activation-receipt.json) |
| Owned build drain, capacity reservation release and sampler cleanup | Passed, [release receipt](owned-resource-release.json) |
| Installed runtime/bundles, empty fleet, healthy services and masked upgrades | Passed, [post-cleanup audit](final-post-cleanup-audit.json) |
| Exact owned test-image cleanup | Passed, [cleanup status](image-cleanup-status.json) |
| All nine owned VMs absent at the provider | Passed, [provider receipt](provider-absence.json) |

These short image-build arms do not qualify a multi-hour training run or builds
mixed with 500–1,000 running agent sandboxes.

All 240 owned test-image manifests and their 480 aliases were removed with
explicit user approval. The first pass deleted 237 and stopped on active pull
leases for the three successful, already-deleted smoke sandboxes. A separately
reviewed helper released only those three exact leases after validating retained
scheduling events, immutable lease fields and the retired worker's provider
absence; the original cleanup then verified all 240 images absent. Shared caches,
filesystem components, all blobs and unrelated images were preserved. The final
audit at 21:39:36 UTC verified healthy gateway and relay HTTPS, active services,
empty workload/fleet state and no waiting database-pool requests.

## Resource costs and remaining bottleneck

[Resource attribution](resource-attribution.md) includes independently selected
busiest 30-second windows and both the last and following 60 seconds of each
burst. [Host findings](host-findings.md) retain all phase/host measurements.
Cold-arm telemetry coverage was 98.9–99.6%; warm windows were much shorter.
Disk busy counters are excluded. These samplers had no health probe configured;
zero probes is missing evidence, separate from successful SDK operations and
deployment-controller health checks.

| Cold-arm resource | Baseline | Pipeline |
|---|---:|---:|
| Gateway API mean occupied cores | 0.068 | 0.069 |
| Registry physical writes GiB | 14.198 | 14.209 |
| Registry I/O-weighted await ms | 47.68 | 45.41 |
| Registry queue depth p95 | 36.43 | 38.39 |
| Gateway I/O PSI mean / p95 % | 14.44 / 61.07 | 15.55 / 67.76 |
| Per-builder busiest-30s CPU mean range, of 8 cores | 5.81–5.93 | 6.13–6.87 |
| CPU PSI means in those builder windows % | 11.02–14.52 | 18.36–32.23 |
| Minimum available builder memory GiB | 24.95 | 23.41 |

No sampled OOM or swap activity occurred. More work overlapped, with less CPU
headroom during busy windows; these results do not justify raising the bounds
again. Registry write volume stayed nearly identical. Gateway I/O PSI in its
busiest 30-second window increased from 38.32% to 46.32%; both arms settled to
about 0.002 GiB of registry writes and less than 0.2% I/O PSI in the following
minute.

All 16 Python cold builds in each arm fell back from selective extraction.
Their mean publication time was 43.897 → 39.608 seconds, of which Docker pull
accounted for 38.362 → 33.817 seconds. The retained fallback boolean does not
identify whether compressed/unpacked bounds or unsupported filesystem semantics
caused that fallback. The 128-MiB compressed and 1-GiB unpacked limits are not
proof of the specific cause. TypeScript-tools publication was 11.228 → 11.626
seconds; multistage publication was 0.760 → 0.703 seconds. The final four builds
in both arms were Python, so improving that pull path is a stronger next target
than another gateway CPU increase.

## Maintenance overlap

The sanitized [baseline](maintenance-baseline.json) and
[candidate](maintenance-candidate.json) journal receipts show no pruning or
registry GC during either measured cold burst. Hourly pruning ran at
21:04:00.883–21:04:22.681 UTC, after the baseline's completion tail and 123 seconds
before candidate warm submission. It inventoried 208 cache tags and deleted no
cache/image manifests. Pressure checks throughout both arms reported no cleanup
and zero GC runs; registry usage stayed below 18%.

Pruning emitted a `psycopg_pool` interpreter-finalization warning on exit.
Systemd recorded success, exit status 0 and no remaining main/control process.
This was a shutdown warning outside the measured arms, not a failed build or
registry restart; orderly application-level pool closure was not proven.

## Tests and local overhead

The [affected-suite log](service-tests.log) records **278 passing tests in
14.440 seconds**, with no skips. After the owner-retry correction, the selection,
phase-admission and polling subset passed **51 tests in 1.941 seconds**. These
suites overlap; they are not 329 distinct tests. Coverage includes four-plus-two
overlap, full-capacity replay/conflict handling, preparation and thread-start
failures, deadline-bound phase waits, cleanup ownership, cross-process
reservations, dead-owner reconciliation and transient release-read/write errors.

```sh
(
umask 077
.venv/bin/python -m unittest \
  tests.test_image_build_admission tests.test_build_admission tests.test_images \
  tests.test_image_build_deadline tests.test_build_publication_deadline \
  tests.test_image_rootfs tests.test_node_agent tests.test_config tests.test_vm_init \
  tests.test_cli tests.test_build_history tests.test_builder_selection \
  tests.test_builder_packing tests.test_image_polling tests.test_registry \
  tests.test_agent tests.test_policy tests.test_reconcile -q
)

.venv/bin/python -m unittest tests.test_builder_selection \
  tests.test_image_build_admission tests.test_image_polling -q
```

The qualification and upgrade helpers also passed 17 focused tests; the indexed
admission sampler passed two. The cleanup and exact-lease-release helpers passed
six tests covering scoped aliases, identity checks and read-only lease inspection.
Changed files passed Ruff. The production template was executed
with its output intercepted and [validated](provisioning-template-check.json),
preserving the existing local build configuration.

A partial index limits admission snapshots and phase-wait queries to owned
records instead of decoding retained log tails. The [local query measurement](admission-query-benchmark.json)
used 256 terminal records with 64-KiB log tails and 200 calls: snapshot median/p95
**0.182/0.203 ms**, blocked phase-transfer median/p95 **0.232/0.255 ms**. This is
local query overhead, not production build-throughput evidence.

## Release identity and rollback

The runtime changes are commits `c66317aabccd6438e1f079191716f0fd91b21b1a` and
`3cdae92b546d634c804035e3c6a9efd00ed2441e`. The release root is
`/work/ucloud-sandboxes/build-pipeline-20260929-r2`. The
[activation helper](enable-pipeline.py) pins wheel SHA256
`405516727b99eed510a0bcfcee39203cb69f67a4b56ad7c68bc5d3eaa672e628`.
The reviewed `images.py` SHA256 is
`d04d15b635ab68058725c7d76157a81a053e113995353965312d671d48a35c85`.
The [staging receipt](staging-receipt.json), whose SHA256 matches the deployment
receipt, binds this wheel and the exact builder/sandbox bundle hashes. It records
unchanged dependency and native-file inventories for both bundles; the deployment
receipt also confirms those components remained unchanged during installation.

The [deployment controller](deployment-controller.py) preserves dependencies,
native artifacts, the relay storage budget and an exact config/runtime rollback.
The [owned-builder upgrade helper](../../../scripts/upgrade_owned_builder.py)
clones the existing dependency tree, verifies the candidate wheel, and changes
only the explicitly identified idle builder under its own drain token. It
refuses to stop a candidate when its idle fence cannot be verified or admission
may already have reopened. The activation helper changes only the future-builder
policy from zero to two finishing permits and restarts the autoscaler after the
qualification gates pass.

Omitted configuration and legacy records retain zero-finisher behavior. Empty
`admission_phase` fields are omitted from serialized legacy-mode records.
**Setting the policy back to zero does not make pipeline records readable by an
older strict runtime parser.** Retire the owned candidate builders before an old
runtime downgrade, or separately review an idle-state migration. Never downgrade
an active builder or discard its admission records to force capacity open.

## Remaining reliability boundaries

- The 1800-second execution budget starts with the worker. Server-side context
  materialization happens after reservation but before that budget starts
  (`images.py`, `start_build` and `_run_tracked_build`). Large-context preparation
  therefore needs its own deadline qualification; this change only bounds the
  number of simultaneous preparations.
- Native filesystem operations and arbitrary Python callbacks are cooperative
  deadline boundaries. Finishing waits are bounded, but a blocked underlying
  operation is not a hard preemption guarantee. See [build deadlines](../../build-deadlines.md).
- Two publishing builds can increase registry or filesystem pressure. The new
  stage bounds alone cannot guarantee lower end-to-end tail latency, prevent
  every deadline miss, or guarantee FIFO order between finishing waiters.
- Process reconciliation releases lost admission ownership; it does not resume
  a build after builder-VM loss. Status retries and transient-failure handling
  are covered by the preceding [reliability release](../build-reliability-2026-09-29/README.md),
  while mixed-load soak and failure-recovery qualification remain separate work.
