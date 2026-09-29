# Hetzner production builds — 28 September 2026

Investigated the 06:18–06:31 UTC workload on 0.7.0 using live gateway state,
retained metrics, registry metadata, sysstat, and the deployed source. No new
load test was run; a subsequent small deployment canary is linked below.
Values below describe this run or the explicitly identified
retained registry inventory, not a 500-container capacity qualification.

## OS updates and pruning

The 06:43–06:45 PostgreSQL restarts followed unattended package upgrades.
`/var/log/unattended-upgrades/unattended-upgrades-dpkg.log` explicitly records
`systemctl restart ... postgresql@18-main.service`; the system journal attributes
the package activity to `apt-daily-upgrade.service`. These restarts happened
after the workload's placement failures.

Automatic APT updates are now disabled on the gateway with
`/etc/apt/apt.conf.d/99zz-ucloud-no-unattended-upgrades`:
`APT::Periodic::Enable`, `Update-Package-Lists`, and `Unattended-Upgrade` are zero.
Both APT daily timers, both APT daily services, and `unattended-upgrades.service`
are masked. At that point the gateway was the only remaining server: all workers/builders
had scaled down during the investigation. Their SSH rollout could not complete
because the instances had been deleted.

The updated `vm_init.py` is installed in the gateway runtime and the autoscaler
was restarted, so future sandbox and builder initialization applies this policy
before checking the cached runtime receipt. Gateway preparation, installation,
and snapshot preparation also persist it in source. Manual package maintenance
remains possible. Subsequent deployment canaries verified the policy on both a
fresh builder and a fresh sandbox worker.

The hourly registry-prune unit now opens its root-owned maintenance lock before
running the prune command as `ucloud`, matching PostgreSQL peer authentication.
The 07:02:42–07:02:45 run exited successfully and removed nine unreferenced
manifests. The timer remains enabled.

## Observed bottleneck

- Four CCX33 builders, each with eight active builds: 32 active builds at peak;
  98 pending builds at peak. Median sampled CPU usage was 4.99–5.96 vCPU per
  8-vCPU builder, with all four reaching approximately 8 vCPU. Sampled I/O full
  pressure peaked between 4.53% and 11.77% depending on builder.
- During the 06:20:16–06:30:01 sysstat interval, the gateway registry volume
  averaged 147,597 KiB/s writes, 14.93 queued I/Os, and 92.05 ms request latency.
  This establishes substantial storage work; it does not isolate EROFS upload
  from OCI upload, filesystem bookkeeping, and concurrent worker reads.
- 4,222 image-build HTTP 503 responses include client retries, not that many
  distinct failed builds. The no-ready-builder path is explicitly retryable.
- All 157 recorded create-command 504s carry `placement_deadline_expired`:
  approximately 601 seconds elapsed before allocation was authorized. Admission
  waited behind image availability and other placement work; the retained data
  does not attribute every timeout individually to an image-build phase.
- The three CCX63 workers had peak sampled working sets of 41,489, 50,799, and
  49,289 MiB, with zero sampled memory full pressure. The autoscaler's large
  deficit was a forecast for cold/prepared demand, not observed RAM exhaustion.
  There is no evidence here that three workers are intrinsically too small for
  500 running containers.

## What 0.7.0 sharing achieved

The retained registry inventory had 111 distinct environment roots, referencing
545 components in total and 408 distinct components. Of those, 405 used the v2
layer format and three used v1 whole-image format. Distinct EROFS content occupied
90,262,589,440 bytes (84.1 GiB), versus 119,561,973,760 bytes (111.4 GiB) when
component sizes are counted for every referencing root: a 24.5% saving.
54 distinct components were referenced by more than one root. Layer groups
with the same source layers, parent chain, and format had identical EROFS image
digests in this inventory.

The v2 sharing mechanism is functioning. This corpus shares less than the
deliberately common-base qualification fixture's 66.7%; most components here
are referenced by only one distinct root. These numbers are storage/content
deduplication, not measured CPU-time or network savings.

A later count of surviving root tags timestamped in the 06:00 hour found 281
publications for 109 distinct root digests, with one root published 15 times.
This is evidence of repeated publication of identical results, not proof that
every repetition rebuilt the EROFS data: the layer cache may have served it.
Retention can change these live inventory counts.

## Work that sharing currently does not avoid

`images.py` runs Buildx with direct push on builder nodes. Then
`FreshEnvironmentBuilder.publish_image` unconditionally runs `docker pull` of
the pushed result before `build_layers` checks the shared component tags. Thus
the pipeline is BuildKit build/push → Docker pull/extract → component lookup →
missing-group squash/mkfs/hash/upload → signed environment attachment.
Even a complete component-cache hit goes through the Docker pull/local-image
path first. New groups still incur conversion and registry writes.

`_publish_layer_group` does a lookup followed by conversion on a miss without
per-group coordination. Concurrent builds, including on different builders,
can duplicate conversion before either publishes the shared tag. Blob existence
checks avoid an upload after another writer has already committed, but do not
prevent the preceding duplicated CPU and local I/O. The retained metrics do
not quantify how frequently this race happened in this run.

Production `builder.buildx_cache_ref` is empty. Builders terminate after 300
idle seconds, losing local BuildKit and Docker caches. Registry EROFS components
survive, but they are downstream of the cold build/push/pull path.

## Changes identified by the investigation

Items 1–3 below were implemented and deployed at 12:30 UTC, with one-node
conversion coordination and persistence of terminal results observed by clients.
Cold/warm builds and fresh-worker execution passed. The exact scope, remaining
limits, checksums, and measured canary timings are in the
[deployment report](../benchmarks/build-cache-2026-09-28/README.md).
Items 4–5 remain follow-up opportunities requiring representative measurements.

1. Persist build phase timings and terminal outcomes on the gateway before
   builders are removed. Add separate timing/counter fields for pull, cache
   lookup/hits, squash, mkfs, hashing, and upload. Existing
   `docker_build_and_push_ms` and `immutable_environment_ms` live on builders;
   no build records survived in the gateway database and OTLP export was
   disabled. The registry container was recreated at 06:44:15, so its retained
   logs also do not cover the build run. Precise phase percentages cannot be
   reconstructed honestly from the remaining evidence.
2. Move authenticated component lookup ahead of Docker materialization. Read
   the OCI config's diff IDs and manifest sizes to plan groups; when every
   component exists, validate its source-chain binding and publish the new
   root without pulling/extracting the image. Preserve signature validation,
   registry writer fencing, and retention protection. Follow with selective
   materialization of missing groups if measurements justify the complexity.
3. Prevent simultaneous conversion of the same group, with a second cache
   check after acquiring ownership. Start with coordination across builds on
   one node; fleet-wide ownership needs lease expiry and failed-owner recovery.
4. Configure persistent BuildKit cache with explicit retention and concurrent
   writer semantics. The client already supports registry cache-from/cache-to,
   but a single shared mutable cache tag should not be assumed to merge all
   builders' exports. Compare this against keeping one warm builder, including
   its cost, using representative repeated and cold workloads.
5. Prepare reusable base/task images before starting the container placement
   deadline, and bound build submission to measured throughput. Increasing
   sandbox-worker count does not fix the cold image pipeline. Increasing build
   concurrency alone could amplify CPU and registry I/O contention.

Validate changes with cold and warm runs using the same representative image
set. Record queue wait, build phases, bytes transferred, component hits/misses,
duplicate group work, and create-deadline failures separately. Do not infer a
500-container result from the current lower-concurrency observations.

## Validation of the update-policy change

The VM-init, snapshot-preparation, and registry maintenance tests passed:
24 tests. Shell syntax checks and `git diff --check` passed. The installed
gateway module was checked against the original source checksum before
replacement and compiled successfully. The gateway autoscaler is active;
effective APT configuration reports zero automatic updates and masked units.
