# Shared BuildKit cache

Builder VMs are disposable. An optional registry-backed BuildKit cache retains
Dockerfile build results across their replacement. Every builder can import
the same cache; local BuildKit state remains private to each VM. This is
separate from the signed EROFS component cache used by running sandboxes.

The deployment's `builder.buildx_cache_ref` enables the managed cache. It must
name this deployment's private registry and a repository whose final component
is `ucloud-build-cache` or begins `ucloud-build-cache-`, for example
`10.42.0.2:5000/ucloud-build-cache:shared`. The configured tag is a namespace
placeholder, not a mutable shared export destination. Each build exports a
unique tag. A verified context archive, Dockerfile and build arguments select
the newest matching `bc2-...` cache alone. Without an exact match, imports prefer
the same Dockerfile and diverse recent recipes, up to eight. Legacy `bc1-...`
tags remain readable. BuildKit validates the actual build inputs; these hints
never authorize reusing a completed image.

## Storage and retention

The initial policy exports `mode=min`, preserving cache for final-image layers
without retaining every intermediate stage. Cache/image blobs with identical
digests share physical storage within Distribution. New cache config blobs,
manifests and retention of otherwise-unreferenced layers still consume space.

Defaults, configurable in the `builder` section:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `buildx_cache_max_bytes` | 34,359,738,368 | 32 GiB of unique blob descriptors retained across the shared cache |
| `buildx_cache_max_entries` | 64 | Retained owned cache tags |
| `buildx_cache_max_age_seconds` | 604,800 | Seven days since cache publication |

Hourly `registry-prune` applies these rules separately from ordinary image and
EROFS/snapshot retention. The newest export for each distinct verified context
is considered before duplicate exports of a hot context; spare capacity retains
duplicates by recency. Legacy entries have no verified context equivalence and
retain their recency policy. Shared cache blobs count once. The Hetzner production
profile retains up to 512 tags within the same 32 GiB byte budget; this is not a
guarantee that every context fits. Unknown aliases protect their manifest from deletion;
ambiguous or changing inventories defer cleanup. A failed cache cleanup does
not stop image/reference maintenance. These are logical retention targets,
not hard storage quotas: active uploads, the pruning interval, protected aliases
and blobs awaiting physical GC can exceed them. Existing quiescent registry GC
runs every six hours with its writer fence and blob grace period; it preserves
blobs still referenced by final images. Existing disk-pressure controls remain
in force.

The dedicated BuildKit worker targets 20 GiB local cache, 10 GiB free space and
1 GiB reserved cache on the current 160 GiB Docker filesystem. Smaller disks
scale those settings down. GC cannot reclaim active references, so these too
are cleanup targets; the existing Docker filesystem capacity is the outer
storage limit. Four concurrent solver tasks and four registry connections
bound BuildKit work on each builder.

## Provisioning and failure behavior

Shared caching uses the `ucloud-shared-cache` docker-container Buildx driver.
Docker retains overlay2 for the EROFS materialization path. The BuildKit image
is pinned by version and digest in `vm_init.py`; the qualified initial version
is 0.33.0, matching the deployed Docker builder's embedded version.

Initialization provisions the driver under the same user, HOME and Docker
configuration as the builder agent. It reuses an existing matching instance,
including after a stopped daemon or failed bootstrap. Policy drift requires a
replacement VM instead of deleting a live cache beneath builds. HTTP is enabled
only for the explicitly configured private registry endpoint. Managed image
exports remain single manifests for signed EROFS attachment.

Cache lookup errors fall back to an ordinary build, and cache-export failures
do not fail the final image export. Image build/push and EROFS/signature errors
retain their normal failure behavior.

## Avoiding repeated registry uploads

Before pushing a managed image, the builder can link blobs from one recent
cache with the same Dockerfile into that image's new registry repository.
Fresh BuildKit stores may otherwise upload large existing private-registry
blobs again because they know only the original public source repository.
Registry mounts transfer no blob payload; BuildKit still checks its inputs,
produces the image, and uploads any missing or changed layers normally.

This optional preparation accepts only the configured registry and managed
image namespace. It verifies the cache manifest's raw digest and all
descriptors before mounting, considers at most 64 layers from a 256 KiB
manifest, and attempts larger layers first. Its three-second deadline is
cooperative: a final socket operation and the short cleanup allowance for a
declined mount can extend elapsed time. Missing cache entries, pruning races,
unsupported responses, and transport failures preserve ordinary build/push
behavior. Declined mounts' new upload sessions are cancelled individually.

The emitted mount counters report descriptor bytes linked, not measured
network traffic or physical disk savings. Registry request aggregates and
host disk counters establish those separately. EROFS component reuse also
avoids fetching the same manifest twice, while retaining signature checks,
retention refreshes and final root validation.

## Partial EROFS misses and build admission

A complete signed-component hit avoids Docker materialization. For a partial
hit, the builder can fetch just the missing groups if their OCI diffs are
self-contained. It verifies compressed blob size/digest and uncompressed diff
IDs before signing, and preserves the existing EROFS layout. The fast path is
bounded to 128 MiB of compressed missing layers and 1 GiB unpacked per image.

Whiteouts, missing parent-directory metadata, cross-layer hardlinks, unsupported
extended metadata and oversized groups use the existing Docker path. Cold
images with no reusable groups also use Docker. This deliberately preserves
general OCI filesystem semantics instead of guessing about unseen lower layers.
Metrics distinguish successful `selective_materializations`, fallbacks,
downloaded OCI bytes/layers and the existing Docker-pull phase.

Production selective preparation runs in a fresh Python subprocess after a
partial signed-component miss. This avoids concurrent extraction and squashing
competing inside the node agent's interpreter. One child belongs to each admitted
build and has a ten-minute timeout; timeout and cancellation kill and reap it
before its private scratch directory is removed. Complete cache hits start no
child. The parent retains component locks, signing, and registry publication.
This is process isolation for performance, not a separate security boundary.

The numeric `selective_subprocess_ms` includes startup, extraction, squashing
and IPC. `selective_materialization_ms` and `squash_ms` are nested child phases;
do not add them to subprocess or environment totals. Integrity, child startup,
protocol and timeout failures fail the build; the existing unsupported-extraction
fallback remains available.

Each production builder admits four nonterminal builds, including context
preparation, for its four execution slots. New work waits through the existing
SDK admission retries when all slots are occupied; it can then choose whichever
builder becomes available. Duplicate submissions for an existing build still
join that build, and conflicting contexts remain conflicts. This reduces work
stranded in local queues; it may increase retryable submission responses under
overload. End-to-end client time includes that admission wait.

## Build history

Gateway-observed terminal results also enter `build-history.sqlite` beside
`metrics.sqlite`, independently of noisy autoscaler event retention. It retains
at most 10,000 summaries, 32 MiB of summary JSON, and 30 days; SQLite page/index
overhead is additional. It contains IDs, status, timestamps and numeric timings,
never build logs, commands, contexts or error text.

`started_at` keeps its existing admission meaning. New `queued_at` and
`execution_started_at` timestamps distinguish context preparation from actual
execution. Timing summaries include `preparation_ms`, `queue_wait_ms` and
`end_to_end_ms`; existing `total_ms` remains execution time. Capture still
requires the gateway to observe a terminal builder response and is not a
guaranteed completion-delivery stream.

See [the production qualification](benchmarks/buildkit-cache-2026-09-29/README.md)
for deployment receipts, measured cache reuse and scope limits.

Cache preparation and mount durations are retained as numeric `cache_prepare_ms`
and `cache_mount_ms` in build `timings.phases`, including terminal history. These
are subphases of `docker_build_and_push_ms`; do not add them to it. They survive
truncated build log tails and do not depend on parsing Docker output.

## Prepared image resolution

The gateway automatically resolves bounded, uploaded build contexts against
`prepared-images.sqlite3`, beside the configured image store. Preparation tools
register successful source imports and validated foundations there. Existing
receipts can be imported as the gateway service account with:

```sh
python scripts/register_prepared_images.py --config /etc/ucloud-sandboxes/deployment.json \
  --catalog /path/to/source-pool/catalog.json --catalog /path/to/foundations/catalog.json
```

Clients submit their usual Dockerfile and context; no SDK update or rewritten
client image database is required. Matching uses a literal source reference,
exact dependency-prefix identity (including installer bytes), or the exact
SWE-smith enrichment recipe. A different task image from the same project is
never treated as equivalent. Prepared dependency snapshots retain the package
versions captured during preparation, like a build cache; this does not promise
fresh package-manager or Git results on every build.

The gateway leases the chosen private digest and creates a deterministic derived
context. The ordinary builder still handles admission, remaining instructions,
output names and labels, publication, and status. Accepted submissions include
additive `prepared` metadata (`kind`, `reference`, and foundation `key` when
applicable). BuildKit remains the cache for remaining OCI operations; EROFS
components remain the shared sandbox filesystem representation.

Automatic matching currently covers source imports, TMax explicit/inline
installers, OpenSWE Python foundations, and Terminal dependency prefixes. It
accepts literal external-image `COPY --from=image:tag` instructions in a single
unnamed stage, including Terminal-Lego's UV verifier wrapper. The copy and all
remaining verifier/task instructions retain their original bytes. Terminal
matching selects the longest already-prepared prefix at complete instruction
boundaries, so appended verifier setup does not hide a shorter cached prefix.
Prefix matching is bounded to 256 instructions.
Matching conservatively skips build arguments, alternate Dockerfiles, nonempty ignore
files, custom frontends, multiple stages, broad context copies, bind mounts,
and contexts over the matching bounds (8 MiB file data, 1,024 entries). Unsupported
recipes use ordinary building. The raw build API accepts `prepared_cache: "off"`
to bypass this optimization.

The original request fingerprint durably freezes both hits and misses, including
across gateway restarts and catalog additions. A new image identity can select
newly prepared work; retrying an existing request cannot silently change its
context. The catalog contains metadata, not duplicate filesystem blobs. Back up
its decision table with gateway state to preserve retry identities. Source and
foundation entries can be reconstructed from preparation receipts. Keep those
receipts, immutable source pins, and preparation inputs with the campaign.
