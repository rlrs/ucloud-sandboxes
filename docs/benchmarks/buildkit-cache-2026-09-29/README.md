# Shared BuildKit cache: controlled canary, 2026-09-29

An empty replacement BuildKit store completed the canary in **1.758 seconds
with a registry cache import**, compared with **4.522 seconds without an
import**. Both builds succeeded and published an image with its immutable
environment annotation. These are controlled builder measurements; production
deployment and fresh-VM qualification are recorded below.

## Measurements

The receipts record a population run followed by two independent replacement
BuildKit stores on the same otherwise idle builder VM. Each replacement store
reported `0B` before its measured build. All three used the same 32 MiB
deterministic payload, Dockerfile, pinned BuildKit 0.33.0 image and staged wheel.
The Dockerfile copies the payload into BusyBox 1.37.0 and hashes it 32 times.
The exported registry cache uses `mode=min` and flat OCI manifests.

| Case | Measured wall time | Build and push | EROFS publication | Cached Dockerfile steps |
| --- | ---: | ---: | ---: | ---: |
| Populate shared cache | 4.727 s | 3.519 s | 1.155 s | 0 |
| Empty replacement store, no import | 4.522 s | 3.462 s | 1.009 s | 0 |
| Empty replacement store, shared import | 1.758 s | 1.521 s | 0.184 s | 2 |

Sources: [population receipt](populate.json),
[replacement without cache](replacement-no-cache.json),
[replacement with cache](replacement-cache.json), and
[canary implementation](qualify_builder.py). Receipts include the wheel digest,
payload digest, pinned BuildKit image, exact build command and image manifest
digest.

The observed wall-time reduction is 2.764 seconds, approximately 61%, or 2.57×
faster. The build/push phase fell by approximately 56%. The wall-time improvement
also includes EROFS reuse: the no-import case built and published a component,
while the import case reused a component and skipped the Docker pull. The full
61% improvement therefore cannot be attributed to BuildKit execution alone.

This demonstrates cache reuse after losing local BuildKit state. It does not
measure a newly booted VM: the replacement cases created fresh BuildKit
containers on an already-used VM. Host page caches, Docker state and registry
state were not reset. Container creation/bootstrap, context generation and
payload generation happened before the wall-time interval; the interval covers
the local image-manager build through completion, including image push and
environment publication, rather than the public API upload or fleet placement.

There is one sample per case, in a fixed order, with no concurrent workload.
There are no percentile estimates, large multi-stage dependency builds,
registry bandwidth measurements or sustained storage-growth measurements here.
The receipts confirm successful image publication and the environment
annotation; public SDK execution and durable gateway history verification are
recorded separately below.

## Storage interpretation

The intended local BuildKit GC target is **20 GiB per builder**, with 10 GiB
free-space and 1 GiB reserved-cache targets on the documented 160 GiB Docker
filesystem; smaller filesystems scale these values down. This local state is
private to each VM. Active build references can keep usage above a GC target.

The shared registry retention target is **32 GiB of unique blob descriptors**,
with 64-owned-tag and seven-day publication-age limits.
Builders import from the same dedicated cache repository and publish independent
immutable tags. The 32 GiB target applies across that shared repository, rather
than separately to every builder. Local builder storage and shared registry
storage are separate; neither GC policy is a hard disk quota.

The population receipt reports one cache tag and 35,777,593 logical retained
bytes. The imported replacement reports two tags and the same 35,777,593 bytes,
showing that shared blob descriptors are counted once. Distribution can also
share identical blob digests between final images and caches. These receipts
do not measure the cache's incremental physical disk footprint: cache metadata,
otherwise-unreferenced layers, uploads and storage awaiting GC still consume
space.

The later [live descriptor accounting](state-after-builder-retirement.json)
finds three cache tags still referencing 35,777,593 unique blob bytes.
35,776,579 bytes also occur in the three controlled final-image repositories;
only 1,014 blob bytes are cache-only in that comparison. This verifies shared
layer digests for this fixture. It excludes manifest/tag metadata, filesystem
overhead, uploads and delayed GC, and is not a general storage-growth estimate.
The live registry volume was 14.91% used, with about 794 GiB available.

Hourly reference pruning and subsequent quiescent registry GC determine when
physical space is recovered. Protected aliases can keep even the logical
retained set above its target. Uploads, the pruning interval and GC grace period
can additionally leave physical usage above that target. GC
preserves a shared layer while a retained image or cache still references it.
See [the policy documentation](../../build-cache.md) for configuration and
failure behavior.

## Configuration and rollback

The enabling configuration is `builder.buildx_cache_ref`, pointing at this
deployment's private registry and a reserved `ucloud-build-cache` repository.
The supporting keys are `buildx_cache_max_bytes`, `buildx_cache_max_entries` and
`buildx_cache_max_age_seconds`. Builder provisioning also installs the dedicated
`ucloud-shared-cache` Buildx driver under the builder agent's user and Docker
configuration. Changing gateway configuration alone does not retrofit the
driver into an existing VM; new builder provisioning must be verified.

The [deployment controller](deployment-controller.py) coordinates the gateway
package, deployment configuration and node-package selection. Rollback must
restore the compatible previous configuration together with the previous
gateway environment: older configuration parsers reject the new builder keys.
Restoring only the Python package is insufficient. Builders already provisioned
from a candidate bundle need separate attention; changing the gateway's
node-package selection does not replace an existing builder. Restoring the old
release also does not immediately remove registry cache data.

New builder image records have additive timing fields. An older strict image
record decoder cannot read those new records: replace the ephemeral candidate
builders or restore their pre-change image database as part of a rollback.

The [test log](tests.log) records 212 passing tests. The
[wheel comparison](wheel-comparison.json) lists changed wheel members; it is
not a deployment receipt. The gateway [qualification script](qualify_gateway.py)
defines public SDK build, history and sandbox-content checks; the script alone
is not evidence that those checks completed.

## Production deployment qualification

The final package was deployed at **2026-09-29 07:30:36 UTC**. See the
[staging receipt](staging-receipt.json) and
[deployment receipt](deployment-receipt.json). Its SHA-256 is
`1e771767b7ed21c0a447f141c26fde0b298aae2fd83f40de2f328efc945f877f`.
Native bundle contents and dependency files were unchanged. Gateway and relay
HTTPS, metrics and PostgreSQL pool checks passed. The configured package root
is `/work/ucloud-sandboxes/buildkit-cache-optimization-20260929-r3`.

The earlier R2 attempt automatically restored its old package/configuration
after its verifier retained an imported old configuration parser and rejected
the newly added settings. The services had started successfully. The final
controller verifies installed-package health in a fresh interpreter, including
after rollback; [rollback health verification](r2-automatic-rollback-receipt.json)
passed. The measured controlled canary used an earlier wheel; the
final wheel additionally fixes Buildx instance inspection for idempotent setup.
The [restart check](restart.log) confirms the stopped daemon restarted and
retained its 107.4 MB local cache.

Fresh builder VM **167925517** provisioned the final bundle and pinned BuildKit
without manual setup. [Initial inspection](fresh-builder-before.log) shows a
running `docker-container` driver, the intended concurrency/GC settings and
**0 B** local cache. A build through released SDK **0.4.33** then reused the
earlier VM's registry cache:

| Fresh VM public build | Result |
| --- | ---: |
| SDK wall time, including context upload | 2.997 s |
| Builder execution | 1.687 s |
| Build and push | 1.471 s |
| EROFS publication | 0.201 s |
| Context preparation / queue wait | 36 ms / 2 ms |
| Reused EROFS groups / Docker pulls | 1 / 0 |

The [build receipt](public-build-receipt.json) records successful build
`e88ee9b7-8a3d-48df-93cd-5458a85e3d77` and its gateway SQLite summary.
[Step details](production-checks.json) identify both `COPY payload /payload`
and the `RUN` command as cached. [Subsequent local usage](fresh-builder-after.log)
was 33.56 MB. This is still one small-fixture sample, not a general production
speedup or concurrent-build throughput qualification.

A fresh sandbox worker **167925809** ran that image through the public SDK.
The [sandbox receipt](public-sandbox-receipt.json) verifies the payload SHA-256,
all 32 build-time hash outputs, exit code zero and deletion of the test sandbox.
The gateway, fresh builder and fresh worker retained all five unattended APT
upgrade unit/timer masks (also see the [worker check](fresh-worker.log)).
The temporary builder reservation was [released](public-release-receipt.json).
At 07:38:37 UTC, [state after builder retirement](state-after-builder-retirement.json)
confirms the terminal summary remains readable while builder 167925517 is
absent from control state. There were no sandboxes, prepared reservations or
active builds. The empty sandbox worker was awaiting normal idle scale-down.

At **07:40:04 UTC**, [final state](final-state.json) confirms both temporary
VMs have left control state, with no sandboxes, active builds or prepared
reservations. The [autoscaler log](cleanup.log) records their stop requests.
Gateway/relay health remained good and the durable build summary was still
readable. Small published canary image/cache artifacts remain subject to normal
registry retention; no temporary compute remains reserved for qualification.
