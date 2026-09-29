# Bounded build pipeline qualification

The bounded pipeline is enabled in production. Each builder retains four permits
for context preparation and Docker build/push, plus two permits for immutable
filesystem publication and cleanup. At most six builds own admission on a node.
BuildKit parallelism remains four. A build waiting for a publication permit keeps
its preparation/build permit, so slow publication applies backpressure.

Admission phases are durable and bounded through SQLite transactions across
processes. Terminal results continue owning their admission and drain fences
until cleanup finishes. Polling and heartbeat retries recover transient ownership
release failures; process reconciliation handles dead owners. Gateway retries
continue reaching the existing owner while that build is cleaning up.

## Synthetic workload results

One sequential A/B comparison used the same four 8-CPU, 32-GiB builders, with
48 concurrent synthetic image builds per wave. Recipes covered a Python agent
with 1,500 modules, TypeScript tools with 2,000 modules, and a TypeScript multistage
image with 2,000 modules. Each wave used a common 600-second deadline beginning
before client context preparation.

| Wave | Original policy, batch seconds | Pipeline, batch seconds | Successful builds |
|---|---:|---:|---:|
| Warm | 17.578 | 17.195 | 96/96 |
| Dependency-invalidated | 305.181 | 291.264 | 96/96 |

The dependency-invalidated batch completed 4.56% faster. Submission p95 fell
from 175.463 to 153.285 seconds, a 12.64% reduction. Arrival-to-completion p95
fell from 290.089 to 276.900 seconds. Warm throughput was essentially unchanged.
All 192 measured builds passed without client errors or deadline misses.
Three resulting images also passed execution checks in a fresh sandbox worker.

Both dependency-invalidated arms used distinct per-case markers before dependency
installation and verified dependency execution. Both uploaded all 48 contexts;
neither cold arm benefited from a context-upload-cache hit. Base images, package
sources and immutable filesystem layers could remain cached. This is one
comparison, not a universal speedup or an empty-cache benchmark.

## Interpretation and limits

More work overlapped, with increased builder CPU and I/O pressure. These results
do not justify raising concurrency again. Python publication remained dominated
by the Docker-pull fallback: mean publication time decreased from 43.897 to
39.608 seconds, including 38.362 to 33.817 seconds in Docker pull. The retained
fallback flag does not establish why selective extraction was bypassed.
Reducing that pull cost is the next performance target.

The qualification did not mix builds with 500–1,000 running agent sandboxes or
simulate a multi-hour training run. It therefore does not establish gateway
sizing or guarantee training-job latency under those conditions. Server context
materialization still precedes the worker execution budget, native operations
have cooperative deadline boundaries, and lost builder VMs do not resume builds.
See [build deadlines](../build-deadlines.md).

The affected service suite passed 278 tests. After the final owner-retry change,
an overlapping 51-test subset passed. Qualification and deployment helpers passed
17 tests, the admission sampler passed two, and cleanup helpers passed six.
Ruff and whitespace checks passed.

The implementation and provisioning are deployed. This is a server-side change;
SDK 0.4.34 needs no new opt-in. Test images, sandboxes, reservations and temporary
VMs were cleaned up, preserving shared caches and blobs. Final service and HTTPS
health checks passed. Raw production telemetry and operational receipts are
retained locally and are not included in this public report.

Setting the finishing limit to zero restores the original concurrency policy.
It does not make nonempty admission-phase records readable by an older strict
runtime parser: retire pipeline builders before downgrading their runtime.
