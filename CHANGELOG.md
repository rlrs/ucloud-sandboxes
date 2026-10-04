# Changelog

## Unreleased

- **`chunk-migrate release-oci --wave N [--execute]`** (M2 plan §5.4) deletes the OCI manifest of each `released` image that is not a build input. It first remembers the image's tags in `image-roots.sqlite3`. Deletes are fenced in the usage store's writer transaction, as retention's are. A digest stays when it is leased by an owner other than catalog owners, routes and create pulls, or when a route without a pinned root, a prepared sandbox or a warmup names it. Without `--execute`, it only counts images and layer bytes.
- **A released image resolves without its manifest.** When the registry has no manifest, the gateway answers a digest from the image's `released` row, and a tag from its remembered digest. This covers creates by tag, digest and name, image listings, the dependency resolver, and the prune's stale build-record check. Pressure eviction skips dispatched images. A commit on a released parent is refused with `commit_parent_released`.
- **The gateway's per-node pull sends the dispatched root** (`environment_root`), and immutable-environment workers attach that root instead of reading the manifest's annotation.
  - Why: a worker's first pull of a released image attached its old root, which retention deletes after `release`.
  - Deploy workers with the gateway before running `release-oci --execute`.

## 0.9.19 - 2026-10-04 (gateway only)

- **`chunk-migrate release --wave N [--execute]`** (M2 step 4, EROFS only). For each switched image, it drops its durable owners' `:environment` leases on digests in the old closure that the new closure does not contain, and marks the row `released`. Retention already ignores a dispatched image's annotation, so its hourly prune then deletes the old roots and components nothing else keeps, and the registry sweep frees their bytes.
  - An owner's lease stays when another of its images still needs the digest, by that image's dispatched root or its annotation. If such an image cannot be resolved, all of that owner's leases stay.
  - The OCI manifest stays: readers for a deleted manifest are not built, and builds read `FROM` layers from it.
  - Without `--execute`, it only counts.

## 0.9.18 - 2026-10-04 (gateway and workers)

- **A nydusd attach loads only what nydusd reads.** The worker fetches the bootstrap and chunk map from the store node by the digests the signed component pins, with no index call. It verifies both digests, checks the chunk map's regions against the bootstrap's device table, and writes the bootstrap for nydusd. nydusd checks every chunk against the bootstrap itself.
  - Why: building the Python reader's per-chunk state took seconds per large image, and all of a node's attaches share env-io's one GIL. In a warm 512-rollout burst, env-io sat at 120-160% CPU and an attach took 8 s instead of 0.5 s. Traces showed 64% of create time was image preparation.

## 0.9.17 - 2026-10-04 (gateway only)

- **Awaited environment attaches are no longer capped at 32 per gateway process.** The cap (`MAX_BACKGROUND_CREATE_IMAGE_PULLS`) bounds pulls that outlive their 2 s callers. An awaited attach always has a waiting create, so creates in flight bound it. With 96 creates in flight (0.9.16), the cap turned away 579 creates in 10 s as `image_warmup_pending`.

## 0.9.16 - 2026-10-04 (gateway only)

- **The placement worker allows every node's startup slots in flight:** `create_target_concurrency_per_node × max_nodes` creates (96 in production), not a fixed 32. With 0.9.15's awaited attaches a create takes ~5-6 s, so 32 in flight capped a 3-node burst near 6 creates/s while nodes had 32 slots each. A stopgap on today's create path; C4.3 replaces the queue-side bound.

## 0.9.15 - 2026-10-04 (gateway only)

- **On immutable-environment workers, a create waits for its image's attach** (up to 30 s) instead of getting a 503 `image_warmup_pending` after 2 s.
  - There, the gateway's per-node "pull" is the attach (resolve and mount the components): seconds, not an OCI pull's minutes. Creates for one image on one node still share one attach.
  - Why: 0.9.14's deferral log showed it was nearly every retry in a 512-rollout burst. About 100-160 creates per 10 s went back to the durable queue and polled with backoff while their attach ran.
  - OCI workers keep the 2 s answer.

## 0.9.14 - 2026-10-04 (gateway only)

- **The placement worker says why it deferred commands.** Every 10 s with deferrals, it logs a warning counting them by kind, status, error code (or transport `code`) and the gist of the message, digits elided. A warm 512-rollout burst retried 182 creates up to 34 times, and nothing recorded the cause. `node_active_admission_deferred` covers both startup slots and memory, so the message is part of the key.

## 0.9.13 - 2026-10-04 (gateway and store node)

- **The store node's reads move to Go: `ucloud-chunk-serve`** (`runtime/chunk_serve`), enabled by pinning its sha256 in `store_node.native_server_sha256`.
  - Why: the Python node is bound by one GIL, near one core. In a warm 512-rollout burst it served ~1,400 small reads a second at ~15% of the CX43's CPU, and first commands waited 7-12 ms p50 (240 ms p99) to be sent.
  - It answers what it can from verified, resident extents: virtual nydusd blobs and plain objects, with the same statuses, headers and bytes. It hashes each extent on its first open, as the Python node does, never serves a torn one, and uses sendfile on all cores.
  - Everything else goes to the Python node on 127.0.0.1 at the same port: misses (it fills from S3), bad tokens, malformed or unsatisfiable ranges, warm jobs, residency, health. `/v1/metrics` returns the Python node's metrics with a `native` section beside them.
  - Store init extracts it from the bundle (`runtime/chunk_serve`, added by `repack_node_bundle.add_chunk_serve` from `build_pinned.sh`), refuses a binary that does not match the pin, and runs it as `ucloud-chunk-serve.service`.

## 0.9.12 - 2026-10-04 (store node only)

- **The store node times the reads it serves** (`serve` in `/v1/metrics`): the wait for a read thread, the read (fills, layout lookups, page faults) and the send, each as p50/p90/p99/max over recent requests. It used to time only its S3 fills, which hid where a warm burst's requests waited.

## 0.9.11 - 2026-10-04 (store node only)

- **A slow disk read no longer stalls the store node.**
  - The node sent response bytes with `sendfile` on its one event loop. When those bytes were not in the page cache, the loop waited on the disk, and every other request waited with it.
  - Local NVMe hid this. On the Volume (0.9.10), a cold read takes about 7 ms. Behind 32 cold readers, a hot 64 KiB read went from 1.3 to 88 ms p50, and a 512-rollout warm burst's first commands from 1.5 to 17 s p50 for SWE-smith.
  - Now a read thread faults each response's ranges into the page cache first, so the loop's `sendfile` only copies from memory. The on-loop fast path for cached objects is gone: every read takes the read pool.

## 0.9.10 - 2026-10-04 (gateway and store node)

- **The store node can keep its replica and index on a Volume** (`store_node.data_device`, a `/dev/disk/by-id/` path).
  - Store init mounts the device at `/mnt/store-replica`, bind-mounts `cache_dir` and the index directory from it, and makes both services require those mounts. It never formats: the device must already be ext4.
  - Why: wave 3 (mostly ScaleSWE) dedups less than waves 1 and 2, and the replica would outgrow the CCX43's 360 GB disk. A Volume grows on its own and outlives the server, so the node can be a small, cheap type and be replaced without refilling from S3.
  - Off by default, and not rendered when unset, so older releases still read the block.

## 0.9.9 - 2026-10-04 (gateway and store node)

- **The chunk index keeps stored locators in its own database.**
  - A worker's first attach of an image reads that image's locator. The index used to fetch it from S3 on a cold request, and S3 stalled it for 56 s once in M2 wave 2.
  - Now the index reads its database first, falls back to S3 once (for a fresh index), and writes both when it builds a locator.
- **`chunk-migrate switch` verifies the whole closure before switching an image.**
  - An image with a missing component goes to `not_warm` instead of being half-switched.
  - Why: one wave 2 image had lost a component to the retention incident.

## 0.9.8 - 2026-10-04 (gateway and store node)

- **The store node is a full replica of its S3 prefix** (`store_node.replica`).
  - S3 is the permanent store, and the deduplicated corpus is small: 76 GB after waves 1 and 2, about 150–220 GB projected.
  - The node never evicts. A fill past `cache_bytes` is refused and counted (`full_refusals`), so the operator grows the disk.
  - A mirror lists the prefix every `mirror_seconds` and fills whatever is not resident. A fresh node refills itself the same way.
  - `POST /v1/resident` returns the keys that are not wholly resident.
- **`chunk-migrate switch` works in batches.** Per batch of 100 images it makes one residency check, one fill of whatever is missing, and one recheck. Before, it warmed about 3 s per image.
- **Removed:** 0.9.4 and 0.9.5's first-to-evict placement of builder reads and `keep=false` warms. They existed only to protect a warm set in an undersized cache.

## 0.9.7 - 2026-10-04 (gateway only)

- **Retention gives a converter's roots 72 hours to be recorded.**
  - Chunk-store tags (`rafs-root-*`, `rafs-*`) younger than the window are no longer deleted as unreferenced.
  - Why: wave 2's converted roots waited hours before `chunk-migrate record`, and the one-hour grace deleted 1,115 of them. The old roots and S3 chunks were untouched.
- **The registry prune no longer crashes.**
  - Since 0.8.6, `manifest_document` passed `timeout_seconds` to the build cache's `_json_request` override, which did not accept it.
  - As a result, every hourly prune failed in its build-cache pass.

## 0.9.6 - 2026-10-04

- **env-io runs with `LimitNOFILE=65536`.** A 512-rollout burst on 0.9.4 lost creates to EMFILE, because env-io sat at systemd's default of 1024. At about 140 attached images per node it held roughly 600 NBD sockets and 355 device fds. The node agent already ran with 65536.

## 0.9.5 - 2026-10-04 (store node and converters)

- **A converter's verification no longer warms hot.**
  - Warm requests take `keep: false`: extents fill first-to-evict and nothing is promoted. The converter's mount verifier uses it.
  - Why: the verifier warms each image's objects before mounting it, so M2 wave 2 would have evicted wave 1's warm set.
- **`chunk-migrate convert --shard I/N`:** converters split a wave, each image once.

## 0.9.4 - 2026-10-04

- **A converter's store-node reads never evict the warm set.**
  - Write-token demand reads put their extents at the eviction end and never promote existing ones. Restarts keep that order through mtime.
  - Warm jobs promote what they find.
  - Why: M2 waves are larger than the store node's cache, and converters verify by reading through it.
- **Only a blob's first nydusd starts alone.**
  - Each blob has a lock, held only until the first daemon on it is ready. Daemons on prepared blobs need no lock.
  - Before, every daemon start on a node took one lock, and 20 RAFS creates on a node queued about 14 s.
- **`chunk-migrate switch` skips an image whose warm call fails,** instead of stopping.

## 0.9.3 - 2026-10-04

What M2 wave 1's post-switch canaries found (wave 1 was reverted until this ships):
- **A stalled read is latency, not EIO.**
  - nydusd sets a fixed 60 s NBD timeout, and each expiry marks one of the device's connections dead for good.
  - Hetzner S3 stalls about 2% of GETs for 6–60 s, often again on retry of the same key.
  - Now the device timeout is 600 s, set after nydusd connects, and nydusd retries the store node for up to 270 s. Each retry joins the node's in-flight fill.
- **Warming covers everything workers read.** `warm-chunk-store` and M2 switches include every nydusd blob's tail and layout. Workers had fetched these from S3 at each first attach.
- **`chunk-migrate switch` dispatches only fully warm images.** It warms each image's objects first and leaves any image with a failed object unswitched (`not_warm`).
- **RAFS attaches never queue behind the EROFS attach limit.** They get a fixed 8, while production keeps EROFS serial. Serial nydusd attaches had cost a 20-sandbox burst about 30 s.

## 0.9.2 - 2026-10-04 (store node only)

- **Worker fills never wait behind a builder's.**
  - On the store node, requests with the write token (converters verifying through the node, and warm jobs) fill from their own pool of half the S3 concurrency, unhedged.
  - Worker fills (the read token) always have the other half.
  - Why: M2 wave 1's post-switch canary lost one sandbox to EIO. While the converter verified 20 large images through the node, a worker's fill missed the 60 s deadline and its NBD read timed out.
- **M1 gate:** the baseline worker drops production's chunk store and root dispatch.

## 0.9.1 - 2026-10-03 (store node and converters only)

- **Chunk-index registration survives S3's tail.**
  - The index now range-reads only each blob tail's chunk table, not the whole tail object with nydus's metadata.
  - Its presigned S3 reads retry transport errors and 5xx at 0.5, 1 and 2 s.
  - The index client waits up to 600 s for a registration.
  - Why: M1 gate run 4 lost 2 of 157 conversions and production wave 1 lost 4 of 53, to one 30 s S3 read timeout each on the register or commit path.
- **Deployment.** Workers and the gateway stay on 0.9.0: neither runs the index.

## 0.9.0 - 2026-10-03

The production chunk store's release (docs/rollout-0.8.0.md, "0.9.0"). It changes no image path. Every sandbox still mounts its EROFS root until a wave is switched, and `dispatch_roots` stays off.

- **nydusd in the sandbox bundle.** It is built by `runtime/nydusd/build_pinned.sh`: v2.4.5 `e3190057`, `block-nbd`, Rust 1.94.0, Apache-2.0. VM init verifies and installs it at `/usr/local/libexec/ucloud-sandboxes/nydusd`. `chunk_store.nydusd` naming that path is checked against the bundle's pin.
- **Stored locators and blob layouts.** Registration writes each component's locator and each nydusd blob's layout to the store. Workers and the store node then read them, not the index. M1 gate run 3 stalled for 4.5 minutes while one index process computed locators per attach.
- **nydusd's shared cache stays within `immutable_environments.cache_bytes`.** Idle images are detached, least recently used first.
- **M2 waves (C2.13):** `chunk-migrate convert` converts a wave's images with full-tree verification, one process per image. Rerunning it resumes. `record`, `switch`, `revert` and `status` run on the gateway, and a switch re-points the image's durable owners to the new closure. Retention keeps a reverted root. Release is not built yet ([chunk-store-m2-plan.md §5.1](docs/chunk-store-m2-plan.md)).

## 0.8.8 - 2026-10-03 (gateway only)

- **The relay reads a local wait's acknowledgment from a duplicated socket.** A guest that was not paused reads its answer and closes at once. The relay then found its own copy of the socket closed and counted the answer unacknowledged: it dispatched a needless wake through the gateway and marked the request reattachable. The 0.8.7 fallback canary showed it after every restore, when the next call is not paused. A reset still counts as unacknowledged.
- **M1 gate driver:** canaries create the staging directory before nydusd is copied there.

## 0.8.7 - 2026-10-03 (gateway only)

- **Node-local model waits cover the relay's HTTP tunnel too.** 0.8.6 enqueued only OpenAI-route calls (`/rollouts/<id>/v1/chat/completions`) as local waits. Agents using `http_tunnel_url`, such as the relay benchmark's, still got the relay-driven park and wake: correct, but no saving. The canary found it: 456 of 456 calls had `local_wait` false. Gateway only: the relay runs there, and workers keep the 0.8.6 bundle.

## 0.8.6 - 2026-10-03

Deployed 2026-10-03 (gateway 20:44Z, snapshot `439244700`, switch on). Gateway first: the relay needs its additive `relay_requests.local_wait` column, applied by `shared_control migrate` before the relay restarts. Then workers on a new snapshot, then the switch. Everything below except the switch is inert by default.

- **Node-local model waits, off by default (`sandbox.direct_local_model_waits`, needs the pause tier).** For agents that call the relay over plaintext on the private network (`http://10.42.0.2:8092`), the node pauses a sandbox while its call is outstanding and thaws it on the answer's first packet. It sees only TCP headers, through nftables NFLOG. The relay sends no park and delivers at once; it wakes through the gateway only an answer the guest never acknowledged, which means a sandbox hibernated mid-call. Per wait: no `/park` or `/wake`, no gateway statements, and about 10 ms from answer to agent. New relay column `relay_requests.local_wait` (additive). Design and gates: `docs/node-local-model-waits.md`. Spike: `docs/benchmarks/node-local-wake-2026-10-03/`. Verified on an unregistered worker: 400 of 400 calls paused, answer to agent 17 ms; a stall when a status read re-paused an answered call is fixed.
- **Chunk-store converters reserve chunks before packing** (`POST /v1/chunks/reserve`, which replaces `/v1/chunks/lookup`). Builders converting at once pack each shared chunk once: per-pack commits alone left 2.3 GB of duplicate chunks in the M1 gate's 20 GB. A dead builder's hold lapses after 10 minutes. Builders and the index must be upgraded together (no chunk store is deployed).
- **nydusd is configuration, not a spike switch** (`immutable_environments.chunk_store.nydusd`, off when absent). It names a nydusd v2.4.5 built with `block-nbd`, pinned by sha256, and needs `store_node`. Workers serve RAFS images with it from the store node's virtual blobs, through one shared filecache. The store node serves those blobs only when the block is set. This replaces `UCLOUD_ENVIRONMENT_NYDUSD`, `UCLOUD_ENVIRONMENT_NYDUSD_SHARED_CACHE` and `UCLOUD_CHUNK_STORE_VIRTUAL_BLOBS`, and drops the per-image cache. The M1 gate driver installs the binary on canaries (`--nydusd`) when the block names it.
- **Blob-tail chunks live with their root.** Registration reads the root's bootstrap and counts every chunk in its blobs' tail tables (nydusd conversions) in the missing check and in `root_packs`. A merged bootstrap leaves out chunks that only whiteout-hidden files use, but nydusd reads across them, so GC must keep them (`docs/chunk-store-design.md` §6). Registration now needs the bootstrap in the store, which the converter already writes first.
- **Removed the attach-timing diagnostic** (`UCLOUD_ENVIRONMENT_TIMING_LOG`). It served the attach spike, which is done (`docs/benchmarks/attach-spike-2026-10-03/`).
- **M1 gate driver:** each parallel converter is its own owner. A shared owner let 12 converters take one another's layer claims and chunk reservations: the gate's "convert race" (no effect on production converters).

## 0.8.5 - 2026-10-03

Deployed with the pause tier on for new workers: snapshot `439222185`, `swap_gb` 64, zswap off (`docs/rollout-0.8.0.md`, "0.8.5"). Everything below except the admission change and the pause tier is inert by default.

- **Admission puts running rollouts first (on by default).** A new sandbox may only spend the headroom that every queued continuation and restore leaves; before, only the head one's was protected, so launches slipped in while later wakes waited. A relay wake whose pages a pause reclaimed owes its thaw prefetch (1 GiB) instead of its whole swapped footprint; without swap nothing changes. In a 140-rollout memory-pressure run on one CCX63 (today's path), 137 waits were hibernated instead of 160 and the slowest wake fell from 57 s to 32 s (`docs/benchmarks/admission-priority-2026-10-03/`).
- **Pause tier (C1.1) second pass, on in production (`sandbox.direct_pause_tier`; default off).** Reclaim now actually frees a paused sandbox's memory. Validated under the same pressure (`docs/benchmarks/pause-reclaim-2026-10-03/`): every model wait came back as a resume (wake p95 0.13 s against 14.2 s on today's path) and nothing was hibernated.
  - **zswap bounded per sandbox:** with zswap on, a paused cgroup may hold at most 25% of its memory bound there (`memory.zswap.max`, set at pause); the rest goes to swap. Incompressible guest memory filled a zswap pool charged to the same cgroup, so reclaims freed 13 MB each.
  - **128 MiB reclaim windows** (16 MiB halved the rate).
  - **No permanent stall:** a reclaim freeing under 16 MiB backs off (doubling from 10 s); only two in a row escalate to hibernate.
  - **Eviction order:** hinted waits by expected idle × resident bytes, then the most recently paused, not the longest paused.
  - **Heartbeat counters** for why reclaims stop: `pause_reclaim_target_reached`, `_not_shrinking`, `_partial`, `_errors` (new `ResidentWaitMetrics` fields: gateway first).
- **M2 readers and gateway (C2.13), inert.** `SandboxSpec.environment_root` (gateway-only), worker capabilities `environment-root-dispatch-v1` and `environment-rafs-v1`, the gateway's `image_roots` table and retention view, and `chunk-migrate inventory`. Nothing dispatches roots while `immutable_environments.chunk_store.dispatch_roots` is false (the default) and no `image_roots` state exists.
- **Shared startup traces (C2.7 groundwork), off** (`immutable_environments.shared_traces`).
- **Opt-in diagnostics and spikes, off unless an environment variable is set:** the attach timing log (`UCLOUD_ENVIRONMENT_TIMING_LOG`) and the nydusd device (`UCLOUD_ENVIRONMENT_NYDUSD`, `UCLOUD_CHUNK_STORE_VIRTUAL_BLOBS`, `--nydusd-blobs`).
- **Chunk-store converter fixes** (off with the chunk store): per-pack commits, path-ordered layers, exact rollback symlinks.

## 0.8.4 - 2026-10-02

- **Component attach is serial again by default.** 0.8.3 attached image components in parallel on every worker. In a 48-sandbox burst on one CCX63, creates finished sooner (time to ready p50 6.1 s against 9.1 s on 0.8.2), but the first command inside each sandbox took a median of 17.1 s against 2.5 s, and the whole burst took 61 s against 49 s. New `immutable_environments.attach_concurrency` (integer 1–256, default 1) bounds concurrent attaches per worker backend. At 1 a worker attaches one component at a time, as in 0.8.2, and the rendered node init is unchanged. Single flight per component is kept at every setting. Workers need the new node bundle.
## 0.8.3 - 2026-10-02

Workers need the new node bundle. The chunk store (`immutable_environments.chunk_store`, plus its `store_node`) is off unless configured. Concurrent attach is always on, and also changes today's EROFS attach path.

- **Chunk store node (C2.6, Phase B), off by default.** S12 found S3's tail in seconds and private-only workers reaching S3 only through the gateway's NAT, so workers now read chunk-store images from a store node on the private network, never from S3. Nothing changes unless `immutable_environments.chunk_store.store_node` is set; rendered configs, node init and gateway units are otherwise the same. Design: `docs/chunk-store-design.md` (§2, "C2.6 store node (as built)"); bring-up: `docs/hetzner.md#chunk-store-node-c26`; benchmarks: `docs/benchmarks/chunk-store-node-2026-10-02/`.
  - **`ucloud-chunk-store`** (`serve-chunk-store`): ranged GETs of packs, bootstraps and chunk maps (those keys only) with the index's read token. Misses fill aligned extents (`extent_bytes`, 4 MiB in production) from S3 with the node's own key through M1's SigV4 presigner. Concurrent misses on one extent share one fill. An S3 GET that stops making progress (no first byte, or no body bytes) for 3× the median time to first byte (150 ms–2 s) is hedged, at most twice; transfers that are slow but flowing are not duplicated. Errors retry with backoff for 60 s, then the read answers 503.
  - **Cache:** LRU under a byte budget on local NVMe. Fills are hashed while written and renamed into place with their sha256 in the name; after a restart each extent is hashed on first use, so a torn one is refetched, never served. Whole packs and chunk maps that do not match their names are not kept.
  - **Serving:** one asyncio loop with sendfile(2) for cached extents; fills wait in a thread pool. Locally: 3.7–3.9 GiB/s of 1 MiB ranges at concurrency 64–256, p99 25–101 ms, on one core. A thread-per-connection server served the same bytes but needed up to 2.1 s to accept a 256-connection burst.
  - **Prefetch:** `POST /v1/warm` (write token) fills objects or ranges with bounded concurrency, behind demand fills, with progress at `GET /v1/warm/<job>`; `warm-chunk-store --component …` warms a component's packs and metadata.
  - **Metrics and health:** `/v1/metrics` (hits, misses, coalesced, bytes, S3 requests, retries, hedges, TTFB and fill percentiles, cache occupancy, evictions, failed checks); `/healthz`.
  - **Index relocation:** with `store_node.serve_index`, `ucloud-chunk-index` runs on the store node (`serve-chunk-index --chunk-store-config`), and the gateway's unit only creates the tokens and exits 78. Worker locators name store-node URLs; builder lookups stay presigned.
  - **Workers fail closed:** with `--chunk-store-url` (rendered by node init) the environment backend reads only from the store node, with the read token, and refuses a locator naming anything else. An outage is EIO after the fetch deadline, never zeros and never S3. Workers initialized before the switch must be replaced.
  - **Node init:** a new `store` role (`init-vm --role store`) installs only the bundle's agent runtime, the block, both tokens, the S3 key and the `ucloud-chunk-store` and `ucloud-chunk-index` units; no Docker, node agent or heartbeats, so placement never sees it. `make_config.py` renders the block behind `CHUNK_STORE`.
  - **Fill unit:** in a local cold-burst benchmark with S12's latency shape, 4 MiB extents beat 1 MiB and whole 64 MiB packs (burst 20.4 s against 30.0 s and 24.5 s; workers reading S3 directly, 40.0 s), and hedging took the cold-start p99 from 29.0 s to 17.4 s.
  - **Line budgets:** the package budget rises from 109,453 to 110,834 lines (+1,381) and the suite budget from 94,939 to 95,338 (+399).
- **Chunk-store converter keeps file owners.** `nydus-image` v2.4.5 `--repeatable` writes every file owner as 0:0, which spike S13 found on all 7 images it checked. The converter no longer passes it; the layer claim, not byte-identical output, is what makes a rerun converge. The converter identity changes from `…;repeatable;…` to `…;owners;…`, so no layer converted with the old flags is reused. The sample images now include a non-root-owned directory, and the real-binary test fails if owners are lost.
- **Chunk store core (C2.13, milestone M1), off by default.** Nothing changes unless `immutable_environments.chunk_store` is configured; rendered configs and node init are the same, and the new `ucloud-sandbox-chunk-index.service` is installed but disabled. Design: `docs/chunk-store-design.md`; operator summary: `docs/immutable-environments.md#chunk-store-images-c213-m1`.
  - **Formats:** packs of at most 64 MiB named by sha256, the signed chunk map (`ucloud-chunk-map-v1`) and the unsigned locator, all strictly bounded. A new component kind, `ucloud-environment-rafs-v1`, signs the bootstrap and chunk-map digests; the root schema is unchanged. zstd uses the node's `libzstd` through ctypes, so there is no new package dependency.
  - **`ucloud-chunk-index`** on the gateway (SQLite, `serve-chunk-index`): batch lookups, pack commits, layer claims, root registration and locators with SigV4-presigned GET URLs (our own presigner, checked against AWS's published vector and botocore). Only this service and builders hold the S3 key; workers get a read token.
  - **`convert-environment`:** converts a registry image with nydus-image v2.4.5 (RAFS v6, per layer, no chunk dictionary), packs only chunks the index does not know, and publishes the root last, so a crash leaves nothing visible and a rerun gives the same root digest. `--layout image|layer` (or `mount_granularity`) chooses one merged bootstrap per image or one stacked bootstrap per layer, for S12 to decide. `--verify-device` adds the full-tree check through the worker's own device. `--attach-tag` tags an annotated copy for workers.
  - **Workers:** the environment backend serves RAFS components over one NBD device each, fetching 1 MiB windows from S3 by presigned range GETs and verifying every chunk after decompression. The node cache is keyed by chunk id and shared across images; traces record chunk ids. Workers need the new node bundle before any converted image reaches them.
  - **Concurrent attach:** the backend's guard no longer spans the registry load, NBD bind and mount; each component attaches in its own single flight, for EROFS components too. A per-layer composition ensures its components in parallel.
  - **`unpack-environment`:** regenerates a one-layer OCI image from a converted root for rollback (merged layout only).
  - **Line budgets:** the package budget rises from 107,016 to 109,453 lines (+2,437) and the suite budget from 93,681 to 94,939 (+1,258). The migration that follows retires about 3.6k package lines (design §8).
- **Images that share a filesystem no longer collide on a worker.** The worker keys an immutable-environment composition (the mounted lower filesystem) by its component manifest. It then compared the whole signed root, including the image config, against the one already mounted. Two task images with identical components but different configs (for example Terminal-Lego images that differ only in `ENV`, `WORKDIR` or `CMD`) therefore failed with `environment config changed for an existing composition` for whichever attached second. That happened in every parallel run of the fscache spike. The worker now requires only the component manifests to match: such images share one mount, and each sandbox still gets its own image config. A different component list for the same composition fails as `environment components changed for an existing composition`.
## 0.8.2 - 2026-10-02

Workers need the new node bundle (the environment backend fix). The upstream mirror is off unless `upstream_mirror` is configured. The benchmark changes are in `scripts/` and are not part of the wheel.

- **Environment backend no longer refuses bursts with EAGAIN.** The node's artifact I/O backend served its Unix socket with socketserver's default backlog of 5. Its client sets a timeout, which makes the socket non-blocking, and a non-blocking AF_UNIX `connect` to a full accept queue fails at once with `BlockingIOError: [Errno 11] Resource temporarily unavailable` instead of waiting. So a create burst on one worker (one `ensure` per composition, plus `drop` and heartbeat `metrics` calls) failed creates whenever more than about 6 connections were pending. In a test, 59 of 64 concurrent calls failed. The server now listens with a backlog of 1,024 (capped by `net.core.somaxconn`), and the client retries EAGAIN on connect with backoff from 5 ms to 100 ms within the call's own timeout. Nodes pick this up with the next node bundle.
- **`bench_rl_scale.py rollout --think-mode {sleep,relay,park}` (C9.2).** Measures density and park behaviour under a training-shaped load without LLM inference. Usage and the split between direct and inferred measurements: `docs/rl-scale-architecture-plan.md`, C9.2 think modes.
  - **`sleep`** (the default) keeps today's behaviour.
  - **`relay`** registers a relay rollout for each sandbox with `register_agent_rollout` (parkable, `managed_process`, container profile). It starts an uploaded agent with `start_agent`. The agent runs each turn and ends it with a blocking chat-completions call through the sandbox's tunnel URL. A fake worker in the benchmark answers each call with a fixed completion after the turn's think time. The gateway's own wait policy decides whether to park. Each call records issue and return in the sandbox, receipt and answer on the driver, `accepted_notified_at` from a lease renewal, and whether the status inventory saw the sandbox parked.
  - **`park`** parks explicitly after each turn and wakes with the current generation. It records park and wake latency, retries and error codes.
  - **Density timeline.** With `--operator-token-file`, every mode records per-node running and parked sandboxes, heartbeat memory, paused gauges and `resident_wait` counter deltas.
  - **New report fields.** `metrics.rollout` gains `density`, `think` and `failures_by_phase`.
  - **Validation and cleanup.** Relay mode is refused without a relay worker token, and park mode without `--parkable` and an operator token. Relay registrations are released on exit and on interrupt.
  - **Line budget.** The test suite budget rises from 93,450 to 93,606 lines (+156). The new tests drive the real in-sandbox agent against a localhost relay tunnel, and cover park mode, the density timeline, validation and interrupt cleanup. Merging existing bench tests saved 31 lines.
- **Upstream pull-through mirror (C2.15).** This is an opt-in deployment section, `upstream_mirror`. Without it, nothing changes, and the rendered `deployment.json`, node init and gateway units are the same. Full description and rollout order: `docs/managed-registry.md#upstream-pull-through-mirror`.
  - **Gateway:** runs one `registry:3.1.1` proxy-mode instance per upstream registry, for example `docker.io`, `ghcr.io`, `quay.io` and `mcr.microsoft.com`. Each runs as `ucloud-sandbox-upstream-mirror@<registry>.service` on its own port on the private address. Its cache lives on the registry Volume, outside the private registry's data root.
  - **Docker Hub account:** optional, from a root-only `credentials_file`. Its values reach Docker only through the environment.
  - **Cache bounds:** Distribution's `proxy.ttl` (`ttl_hours`, default 168). In addition, an hourly `ucloud-sandbox-upstream-mirror-trim.timer` empties the largest caches while their total is above `max_bytes` (default 256 GiB).
  - **Gateway services:** `gateway-reconcile` starts and health-checks configured instances and stops removed ones. The Hetzner installer and `deploy-all-in-one` install the units with the registry mount gate. The Hetzner installer also installs staged `/tmp/ucloud-sandboxes-upstream-mirror-<registry>.env` credentials.
  - **Builders:** new builders get `[registry."<upstream>"] mirrors` with plain-HTTP transport in the shared `buildkitd.toml`, and the Docker Hub mirror in `daemon.json` `registry-mirrors` plus `insecure-registries`. Sandbox nodes get the `daemon.json` mirror too.
  - **Fallback:** BuildKit and Docker fall back to the upstream itself when a mirror fails. Pulls are digest-verified.
  - **Imports:** request-time imports are builder builds, so they take that route, and their ids and build contexts are unchanged.
  - **Campaign:** `prepare_image_pool.py` resolves through the configured mirror without an upstream token, and so does `--stage-upstream` in `stage_source_image.py`. The campaign's cooldowns stay for now (see the doc).
  - **Validation:** upstreams other than `docker.io` require `builder.buildx_cache_ref`, because Docker's own builder mirrors only Docker Hub.
  - **Line budgets:** the package budget rises from 106,730 to 107,000 lines and the suite budget from 93,200 to 93,390 for this feature.
- Add the `rollout` scenario to `scripts/bench_rl_scale.py` (plan C9.2). It samples N tasks (default 500) from the training selection archive or a plain images file, weighted by `upstream_rows` like training or uniformly, with `--seed` and `--family`. All N creates are issued at once (one thread per sandbox; optional `--ramp-seconds`). Each sandbox runs its family's startup command (SWE: `git status` plus a Python startup; TMax/Terminal-Lego: workdir listing plus a Python startup; overridable), then `--turns` agent turns with think time in the rc57 mix (grep, test file, edit plus `git diff`), and is deleted. Attach-only sources resolve through their import alias and SWE-smith through its prepared digest. OpenSWE, TMax and Terminal-Lego recipe builds cannot be driven without the integration's recipe index, so they fall back to `prepared_reference`; the report records this per sample. `--fleet-state {zero,warm-empty,warm-seeded}` is declared and recorded, never changed. An optional `--operator-token-file` verifies `zero` from node heartbeats and records node count and `environment_io` counter deltas. The new `metrics.rollout` section reports time to ready and time to first command per family and overall, the 20 slowest sandboxes, C0.2 create phases, failures by error code and turn latency by kind. The ten survey metrics stay in every report, and earlier reports without `rollout` still validate. Usage: `docs/rl-scale-architecture-plan.md`, C9.2.

## 0.8.1 - 2026-10-02

Node-failure semantics; see `docs/node-failure-semantics.md`. Upgrade the gateway first: it understands the worker's new 409 `sandbox_registration_conflict`, and the autoscaler's `pending_delete_attempts` table is additive.

- **Reboot = process loss (D1).** A proven reboot (a fresh authenticated heartbeat with a new node_epoch) now loses only the old guest's processes. Running, paused and half-captured sandboxes answer 410 with error_code node_lost and a new `reason: rebooted` field. A complete local park that the new boot reports with its exact incarnation keeps its route and stays wakeable. The gateway sends generation-fenced worker deletes for the remaining old-boot registrations and delivers recorded client deletes. This frees their reservations, so the same ids can be created again and the worker rejoins placement. Every 410 for a lost sandbox now says error_code node_lost and carries `reason`. A worker answers a create or import refused because another incarnation owns the id with 409 `sandbox_registration_conflict` (retryable false), not an unclassified 503. Parked-sandbox migration never picks the route's former owner, or any worker still registering that id, as the destination. Watch the `sandbox_reboot_reap` metrics events.
- **Silence is never loss (D2).** Before the gateway answers exec, file, park, wake, DELETE, create-replay or exec-session traffic with a retryable 503 sandbox_worker_unreachable, it pulls the worker's GET /v1/heartbeat once (2 s timeout). The pull is shared by every concurrent request to that worker boot. The next pull waits at least 2 s, doubling up to 32 s while the worker does not answer. A pulled sample counts only when its node, job, deployment, agent version and URL match the stored heartbeat. It goes through the same ingest as a push: inventory reconciliation, and on a new boot epoch, retirement of the old boot's routes as node_lost (410). The gateway emits a node_heartbeat_pull event for each pull, with outcome refreshed, epoch_changed, unreachable, identity_mismatch or rejected, plus the age of the last receipt. The wake-only BlockedOwnerRefresh and WakeCapacityRefreshPending are removed: a concurrent wake now waits for the shared pull instead of getting 503 node_active_exec_deferred. After a pull finds a new boot, the request that pulled answers one retryable 503 when the old boot's route is still pending a recorded delete; the retry or the reboot reaper delivers it.
- **Worker loss semantics (autoscaler and providers).**
  - **UCloud quarantine:**
    - A RUNNING job's latest timed post-start suspension (`interrupted_at`) is watermarked by recovery in the controller label `ucloud-sandboxes/controller-continuity-verified-through`. That label is durable, only moves forward, and workers cannot forge it. So a historical suspension quarantines once instead of every cycle. Untimed history still stays unavailable.
    - A same-boot verified probe with complete inventory now retires absent routes by the normal reconcile rules, which breaks the quarantine/reconcile deadlock.
    - A quarantine taken before a reboot reported is re-anchored on the new boot once ingest has retired the old one, so a rebooted worker rejoins the pool.
    - A quarantined worker wakes its own parks while its boot is unchanged.
  - **Repeated reboots:** the gateway records each proven boot change atomically in `ucloud-sandboxes/controller-epoch-retirements`. Two within 24 h make the worker a failing host: it stops counting as capacity, holds the soft-drain slot whatever the demand, and stops through the ordinary drain handshake once idle.
  - **Hetzner:** `off`/`stopping` servers are unavailable, not lost. They count against `max_nodes` as unreachable, are replaced one for one within it, and are never stopped automatically.
  - **Unreachable-empty stops, every provider:** these now need a direct probe in the same cycle that failed in transport. A reachable worker whose heartbeat push broke is refreshed instead. Prepared unreachable stops written by older controllers lack `directProbeFailed` and are never replayed; they stay inert in `prepared`.
  - **Pending-delete replay:** least recently attempted first, with attempt times persisted in the additive `pending_delete_attempts` table. Up to 8 run concurrently with a 30 s timeout each, so a silent or hung worker cannot starve younger intents.
  - **Loss codes:** provider-confirmed termination, final-job pruning and stale-route deletion record `node_lost`. Clients get 410 and exec sessions are recorded as worker-lost, instead of a 404.
  - **Metrics:** `vm_observed` carries `phase`, `interrupted_at` and the last known `node_epoch`. A new `node_epoch_retired` event records each boot change with its downtime.
  - **Provider plugins:** external providers must declare boolean `requires_continuity_history` and `requires_guest_continuity`. `unreachable_lease_expiry_loss` is gone.
- Record startup traces of immutable environment components when the sandbox is deleted inside the trace window (plan C2.3). Deleting a sandbox collects its image, which drops each component from the artifact backend, and the drop discarded the open 30 s recording window. A sandbox created, used and deleted within 30 s of attach therefore never saved a trace; production counted `trace_recordings_started` but no `traces_recorded`. A detach now ends the window early and saves the chunks read so far. A failed mount still discards it, and a window with no reads saves nothing. The qualification script missed this because it keeps its components attached until the window closes.

## 0.8.0 - 2026-10-02

The first tagged release since 0.5.114rc24. It also covers the untagged 0.6 and 0.7 production builds. Rollout: `docs/rollout-0.8.0.md`. The node-failure fixes are not in it; they follow in 0.8.1.

- Continue splitting `ControlPlaneHandler` into `ucloud_sandboxes/gateway/` (plan C6.1, PR4–PR6):
  - `heartbeats`: `HeartbeatIngest` validates, persists and reconciles worker heartbeats. It runs the deployment and identity checks, the retired-epoch cleanup and the snapshot inventory reconcile, and it releases routes the worker no longer reports. The handler schedules image warmups only when a heartbeat is accepted.
  - `placement`: `Placement` ranks workers and reserves one for creates, wakes and migrations. `_GATEWAY_SCHEDULING_LOCK`, the per-host placement file lock, `InflightCreatePlacements` and the scoring and fit rules move with it. The lock order is unchanged: the process lock, then the file lock.
  - `image_resolution`: `ImageResolution` resolves image ids and tags to digest-pinned worker references. It owns the image inventory cache, the Registry status cache, the managed-manifest cache and the eviction epoch that flushes that cache.

  `build_server` wires them once through `build_services`. The inventory, Registry status and manifest caches and the eviction epoch were handler class attributes and are now per-server instance state, so in-process test servers no longer share them. A gateway process runs one server, so there is no wire, schema, HTTP response, telemetry-name or behavior change. Code that imported the moved names from `control_plane` must import them from `ucloud_sandboxes.gateway.placement` or `ucloud_sandboxes.gateway.image_resolution`, for example `_GATEWAY_SCHEDULING_LOCK`, `InflightCreatePlacements`, `RegistryLayerMetadataCache`, `RegistryManifestResolutionCache`, `IMAGE_REFERENCE_KIND_HEADER` and `MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE`. `scripts/qualify_build_optimization.py` also records source hashes for `gateway/registry_refs.py` and `gateway/image_resolution.py`. The package shrinks by 64 lines.
- **Metadata zone in layout 2 (C2.12).** Layout-2 components (`immutable_environments.preserve_mtimes`, still off by default) are now built with `mkfs.erofs -T 0 --mkfs-time --MZ`. Inodes and directory blocks go into one metadata zone, so attach-time metadata hints cover few chunks and fit the default 32 MiB budget. On the 13.5k-inode scientific rootfs that is 37 instead of 272 chunks, and `find` after attach makes zero remote reads instead of 144. No feature bit changes, so workers and gateways need nothing new. Builders need erofs-utils 1.9+: the builder bootstrap reads the whole `mkfs.erofs --help` once and refuses a mkfs without `--mkfs-time` or `--MZ` ("layout-2 publication requires erofs-utils 1.9+ (mkfs.erofs <option>)"), which supersedes the earlier 1.8+ check. mkfs stages the zone in an unlinked temporary file; the builder points `TMPDIR` at the build's own scratch directory, so it never lands in /tmp. The walker completeness proofs now include the builder's exact layout-2 image when mkfs is 1.9+. A new Python-path test shows layout 1 overflowing the attach budget (155 hint chunks, 128 prefetched, later reads go remote) while layout 2 fits (6 chunks, zero remote reads).
- C1.1 pause-tier follow-ups (all behind `sandbox.direct_pause_tier`; replaces the "Not yet implemented" line and corrects the reclaim pacing and zswap bullets of the C1.1 entry):
  - **Thaw prefetch.** Before `runsc resume`, the Warden reads a RAM-backed sandbox's application-memory file back in parallel when the sandbox cgroup holds at least 32 MiB of swap. It reads SEEK_DATA extents in 4 MiB pieces with 1 MiB `preadv` calls.
    - Limits: 8 readers per thaw and 64 per node (each reader holds a node slot for its whole life), at most 1 GiB and 2 s per thaw.
    - No prefetch for file-backed memory, read-only status and log exchanges, hibernate captures, or failed-pause rollbacks.
    - A delete cancels a running prefetch. Errors only log, and the thaw still resumes.
    - Once a thaw starts reading back, any in-flight reclaim of that sandbox stops at its next window.
  - **Node reclaim budget.** At most 2 paused reclaims run at once, sharing 512 MiB/s. Before, up to cpu_count reclaims could each run at 256 MiB/s. Only the `memory.reclaim` write runs at nice 19, and only where the agent can restore its priority.
  - **Escalation to hibernate.** Reclaim stops short of the last 10% of swap (`SwapTotal`/`SwapFree`).
    - What swap cannot hold, plus any wait whose reclaim failed or freed less than one 16 MiB window, hibernates the best-ranked paused waits through the durable park path: at most 2 at a time, with no background upload.
    - The pause marker is rechecked under the exclusive lifecycle lease, so activity wins. A thaw that beats a planned reclaim is not a stall.
    - Waits holding less than 16 MiB, or more swapped than resident, are not escalated.
  - **zswap is opt-in.** New `sandbox.direct_pause_tier_zswap` (strict boolean, default false, requires the pause tier). Without it, a pause-tier node writes zswap `enabled=N`; with it, zswap uses zstd. Measure the profile first: a full reclaim through zswap was 2–5× slower, and random heaps do not compress.
  - **Metrics.** `ResidentWaitMetrics` gains `pause_reclaim_stalls`, `pause_escalations`, `thaw_prefetches`, `thaw_prefetched_bytes` and `thaw_prefetch_ms_total`. Readers default them to 0; upgrade gateways before nodes.
  - **Correction.** `runsc pause` does not freeze the cgroup, and the Warden does not write `cgroup.freeze`.

  Rollout: these follow-ups have no flags of their own and are active wherever the pause tier is on. A pause-tier node that relied on zswap must set `direct_pause_tier_zswap=true` after measuring its profile. Watch `thaw_prefetch_ms_total / thaw_prefetches` (it should stay under 2 s), `pause_reclaim_stalls` and `pause_escalations`.
- C3.1 commit, worker and builder halves (no gateway route yet). Nodes with a checkpoint registry serve the node-control POST /v1/sandboxes/{id}/commit-export and advertise it as sandbox-commit-export-v1. The export is fenced by the route generation and the lifecycle and Warden locks. It freezes the sandbox (a stale pause marker is not trusted), runs `runsc tar rootfs-upper`, restores the recorded pause state, and stages the raw upper as one content-addressed blob in commits/<sha256(image_id)[:32]>. It is idempotent per operation_id, refuses too-large exports (413) and missing scratch space (503, retryable) before any pause, and a restart sweep clears leftover staging. Builders gain FreshEnvironmentBuilder.publish_commit. It filters the upper in the keyless preparation child under the new commit_policy rules: mount escapes, malformed members, the size and member bounds, and live relay tokens fail; host-written, identity, volatile, build-residue and caller paths are dropped and counted. Whiteout and opaque encodings are normalized to OCI. It then publishes a signed ucloud-environment-erofs-commit-v1 component under its own signing domain, a deterministic gzip OCI layer, a root that extends the parent root, and last the annotated manifest. Workers and gateways now parse commit components.
- Node agents send their own heartbeats (C4.4, node side). One long-lived thread in serve-direct-node-agent and serve-builder-agent replaces the 20 s oneshot `ucloud-sandbox-heartbeat.timer`. It POSTs the same heartbeat GET /v1/heartbeat serves, with provider labels merged in, to the same gateway URL with the same token, so the gateway needs no change. The first heartbeat goes out as soon as the agent serves. After that it sends every interval ±20% (the deployment's heartbeat_interval_seconds). Transport errors, sampling errors, 408, 429 and 5xx retry after 1 s, doubling up to one interval; any other rejection waits a full interval. Every attempt takes a fresh sample. Sending stops when serving does, and a sample taken while serving ends is never sent. New strict flags: --heartbeat-url, --heartbeat-bearer-token-file, --heartbeat-interval-seconds and repeatable --heartbeat-label. VM init puts them in the node unit and, before the agent restarts, disables and removes any older release's timer and service; init fails if systemd cannot stop the timer. The agent-heartbeat command is removed. Heartbeat POSTs no longer follow redirects. The heartbeat token is read once at agent start. In the local fleet harness, heartbeats go through each agent's real sender, and node.start() returns once the gateway has accepted the new agent's heartbeat.
- **Tests: local fleet harness, second slice (C8.4).** Six scenario modules exercise the public contract end to end. They cover S2 (crash replay), S3 (worker loss), S4 (signed exec routing), S8 (create burst), S10 (delete races) and S13 (drain and admission):
  - `test_local_fleet_crash.py`: SIGKILL of the node agent during create (planned, quota_ready, after `runsc create`, before `runsc start`), delete, park and wake. Each operation replays exactly once, reuses its storage device and reaps orphan sentries.
  - `test_local_fleet_worker_loss.py`: a silent worker answers 503, a reboot 410 node_lost, and a quarantined worker serves existing work but admits none.
  - `test_local_fleet_exec_routing.py`: signed sessions are routed without reading the routing store, and give 503, 404 or 410 after silence, deletion, replacement or reboot. Unsigned workers keep the durable route.
  - `test_local_fleet_burst.py`: 20 concurrent creates never overbook disk, reselect around a closed worker, and queue their refused demand.
  - `test_local_fleet_delete.py`: a delete wins over a slow create, a failed delete stays durable intent, and generation fencing holds.
  - `test_local_fleet_drain.py`: drain fences new work, survives a restart and is owned by its token; live admission follows the host sample.

  The harness changes:
  - `LocalFleet(node_processes=True)` forks each node agent from a preloaded zygote so a test can SIGKILL it. Agents die with the zygote, which exits with the test process.
  - One-shot `fail`, `hang` and `hang-after` faults on the fake runsc and mount commands, plus storage-daemon holds.
  - `node.reboot()`, `fleet.expire_heartbeat()`, `fleet.routing_calls()`, the `node.requests` log and a settable host sample.
  - Fleet roots move to /dev/shm when it allows exec.

  No production code changed.
- Speed up the node create pipeline (plan C5.2, first part). A create now makes three durable registry commits instead of four: `plan`, `commit_rootfs` and `commit_owned`. The storage quota (project ID, MiB, path) is recorded on `commit_rootfs`. Storage prepare is keyed by owner, so a crash before that commit replays it from `planned` and gets the same volume and project. Registrations left in `quota_ready` by earlier releases still advance, and earlier releases advance `planned` records as before. New network leases take a pre-created netns+veth pair from a durable pool of 32 per node. The slot moves from `pool` to `leases` in the lease's single durable write, and the namespace is then bind-mounted under the lease's name, so a pool hit runs no `ip` process. A background thread refills the pool, deferring to in-flight creates. Interrupted hand-offs are finished on the lease's next ensure or dropped on release, and unverified pool pairs are rebuilt at start. Earlier releases ignore `pool`; a slot one of them leases is dropped from the pool on the next read. Restore probes the host veth with `if_nametoindex` instead of two `ip link show` processes. The per-create host firewall check is kept and reported as `network_host_rules_ms`. With fakes on tmpfs, 32 concurrent creates fall from 309 / 313 ms to 191 / 198 ms (p50 / p99). Registry write transactions now dominate. See `docs/benchmarks/create-pipeline-2026-10-01/`.
- RL-scale qualification on a disposable Hetzner VM (kernel 7.0.0-30, erofs-utils 1.9, pinned runsc), recorded in `docs/benchmarks/rl-scale-qualification-2026-10-02/`.
  - **Walker (C2.2).** The EROFS metadata walker is qualified on erofs-utils 1.9. On a 13.5k-inode scientific rootfs (production flags, with and without `--mkfs-time`, and `--MZ`), a kernel mount, fsck and dump read no non-data block outside its ranges, and an overwrite proof leaves the tree unchanged.
  - **Walker tests.** They now accept the empty `trusted.overlay.origin` that erofs-utils 1.9 adds to directories holding whiteouts, and they cover `--MZ` images when mkfs supports them.
  - **`qualify_environment.py --prefetch`.** It publishes signed metadata hints, exports backend metrics, and replays startup traces on a fresh cache with a second live guest; the replay had zero demand misses.
  - **`qualify_environment_layers.py`.** It runs again under the project's Python 3.10.
  - **Finding, `--MZ`.** It packs a real image's metadata into 37 instead of 272 chunks, so a `find` after attach makes zero remote reads; without it, the 32 MiB hint budget is exceeded.
  - **Finding, thaw prefetch.** The pause tier needs it. Guest refault from swap took 3.6–5.0 s for 640 MiB; an 8-thread host prefetch took 0.8–0.95 s.
  - **Finding, reclaim.** A full-target `memory.reclaim` with zswap on compresses and then writes everything back, 2–3× slower than without zswap.
  - **Finding, `runsc pause`.** It does not freeze the cgroup.
  - **Finding, host-bound listener.** A guest listener created with `--host-uds=create` makes checkpoint fail and destroys the sandbox, so the in-guest agent should dial out with `--host-uds=open`.
- Add an off switch and heartbeat export for immutable-environment prefetch (plan C2.2, C2.3). `immutable_environments.prefetch_enabled` (strict boolean, default `true`) reaches workers through the bootstrap. When it is `false` the backend starts as `serve-environment-io --disable-prefetch` and only demand-loads, with no metadata-hint or startup-trace replay or recording. Bootstrap never restarts a live backend, so a change applies to newly provisioned workers. The artifact backend RPC now accepts `{"method": "metrics"}`, with a 1 s client timeout and counters behind their own lock rather than the attach guard. Each EROFS worker heartbeat carries the backend's counters as `runtime_metrics.environment_io`, which appears in each node's gateway `actual_usage`. The map has a strict, exact schema (`models.ENVIRONMENT_IO_METRICS`): integer counters, float `*_seconds` totals and the running `prefetch_enabled` mode. The field is `null` on Docker workers, while the backend is unreachable, and while it predates the RPC. Upgrade gateways before workers. Remove the duplicate `python -m ucloud_sandboxes.environment_backend` entry point, since `serve-environment-io` is the only one, and the unused `EnvironmentBackendClient.mounted()`. See `docs/immutable-environments.md`.
- Add a power-of-k create placement library (plan C4.3, `placement_choice.py`). The gateway does not use it yet. Each API process samples k = 3 eligible workers uniformly from its fresh fleet view and scores them by node pressure, by in-flight creates per the per-node create target, and by image residency. In-flight creates are the heartbeat's count plus `api_processes` × this process's own unconfirmed creates. A per-process overlay charges each unconfirmed create, keyed by its exact sandbox incarnation. The charge ends when a heartbeat reports the create, when the worker boot it targeted is retired, when a definite node reject releases it, or after a 60 s TTL. Fit uses the same rules as reservation time: disk, storage devices and memory, with CPU used only for ranking. Dynamic-claim workers are charged the initial claim. `pack` fills the best sampled worker up to a per-node group budget, for C3.2 group creates. In a herding simulation (10 workers, 8 or 32 processes, 300 creates/s), the peak excess of one worker's concurrent creates over the fleet mean falls from 32–160 to 12–15. Node rejects on a near-full fleet fall from 1,156 to 46. Both are measured against the lexicographic ranking given the same stale view. The package line budget rises to 103,330; wiring the library deletes the whole-fleet route scan per create, `InflightCreatePlacements`, worker capacity revisions and the advisory placement turns.
- **EROFS layer components can keep file mtimes (layout 2, C2.11).** Python's timestamp `.pyc` caches stay valid on immutable images. On the scientific stack the first import fell from 3.74 s, with 943 recompiled files written to the sandbox layer, to 1.42 s from a cold cache with no rewrites.
  - **Readers:** workers and gateways now accept layer format layout 1 or 2 and still reject any other value.
  - **Writers:** `immutable_environments.preserve_mtimes` (strict bool, default false; not rendered while false, so the previous release can still read the config) turns layout-2 publication on. Builders then get `--environment-preserve-mtimes` and run `mkfs.erofs -T 0 --mkfs-time`. The builder bootstrap reads the whole `mkfs.erofs --help` output and refuses an erofs-utils without `--mkfs-time` (1.8+). `publish-environment` and `serve-builder-agent` accept the same flag.
  - **Timestamps:** builder-owned views (squashed groups, selective extractions, prepared views, allowlisted views) set directory and whiteout times to 0. A borrowed single Docker diff and the whole-image merged rootfs keep Docker's times.
  - **Keys:** the layout is part of each layer-group key, so layout-2 groups never reuse layout-1 components. Existing layout-1 components, roots and certificates stay valid.
  - **Qualification:** shared-image qualification requires all of an image's components to share one known layout. Under layout 2 it compares whole-second file and symlink mtimes with the source tar headers, not directory times.
  - **Rollout:** ship readers everywhere with the flag off. Then enable it and replace the builders. Republish base and foundation images first, then task images. Workers must not roll back below this release while layout-2 images are in use.
- Add the C1.1 pause tier behind `sandbox.direct_pause_tier` (default off; needs `sandbox.swap_gb` > 0). On a pause-tier node, idle-timer parks and relay model waits `runsc pause` the sandbox in place. Explicit API parks (durable park, drain, offload moves) still hibernate. A relay wait also hibernates when the Aries rule says it should: predicted remaining wait x resident bytes > 300 s (the five-minute rule) x footprint (resident + swapped bytes). If that capture is refused for disk space, the wait pauses instead.
  - A pause changes no ownership. The Warden journal stays RUNNING/LIVE and the route stays `running`. A marker under `runtime_root/warden-paused/` is written durably before `runsc pause` and removed only after `runsc resume`, so a frozen runtime always has a marker.
  - The Warden exec lease thaws under its lifecycle fence before every runsc exec (exec, files, managed control). Explicit and relay wakes thaw too, and a hibernate capture thaws first. Read-only managed status and log exchanges thaw only for the exchange and re-pause under the same fence, so SDK `JobHandle.wait()` polling does not end a model-wait pause. Paused sandboxes survive a node-agent restart and stay paused.
  - Under measured memory pressure only (headroom or PSI; drain's closed admission does not count), the relay-parking loop swaps paused sandboxes out with `memory.reclaim "<bytes> swappiness=200"`: 16 MiB windows paced to 256 MiB/s per sandbox, run on the existing resident-reclaim executor. Sandboxes are ranked by expected idle time x resident bytes, and every pause marker is adopted, including after restarts. A window is cancelled on thaw, stop or a journal change, or when the cgroup stops shrinking.
  - With the flag, vm_init passes `--pause-tier`, mounts the RAM application-memory tmpfs swappable and sized RAM x 0.95 + SwapTotal, and enables zstd zswap in front of the swapfile. `MemoryBackingStore` refuses a mount whose swap policy differs from the flag.
  - Heartbeat memory observations now report `memory.current` + `memory.swap.current`.
  - `ResidentWaitMetrics` gains `paused_sandboxes`, `pauses`, `thaws`, `thaw_ms_total`, `thaw_ms_max`, `pause_reclaims`, `pause_reclaimed_bytes`, `pause_reclaim_ms_total` and `pause_reclaim_cancellations`. Readers default them to 0. Every node on this release sends them (as 0 when the flag is off), so upgrade gateways before nodes.
  - Direct exec start timings no longer include a separate `thaw` entry; thaw time is part of `exec_lease`.
  - The hibernation record, runtime-fingerprint, artifact-file and manifest codecs, and `ResidentWaitMetrics.from_dict`, now derive their keys and defaults from dataclass fields. The wire and journal formats are unchanged.

  Rollout: deploy everywhere with the flag off, gateways first. Then enable it only on fresh workers with `swap_gb` sized from the W0 interference matrix. Rollback: turn the flag off and restart the agent; activity still thaws any remaining paused sandbox. Before downgrading below this release, thaw all paused sandboxes.

  Not yet implemented: thaw prefetch, a node-wide reclaim budget, and escalating from pause to hibernate under memory pressure once swap is full.
- Make the test suite green, hermetic and fast (C8.1/C8.3). `scripts/run_tests.py` runs each test module in its own process across a pool. It gives every test a faulthandler watchdog (default 120 s) that dumps stacks and names the unfinished test, cleans up each child's process group (leaked processes, a dead runner, SIGINT/SIGTERM), and installs the same warning filter as `python -m unittest`. It also supports `--tier unit|contract|linux|live` (each module's `TEST_TIER`; unmarked modules are `unit`), `--json` per-test timings, and `--no-fsync` via eatmydata. `scripts/check.sh` and CI use it, and the PostgreSQL CI job runs the whole `contract` tier. Fixtures create directories with explicit modes (`tests/support.make_dirs`), so the suite passes under umask 0002, 0022 and 0077. Tests that need the SDK are order-independent and skip with a reason, or by minimum SDK version (`requires_sdk`). Five modules no longer re-run TestCase classes they imported. Build-deadline and metrics tests use injected clocks.
- Fix shutdown hangs on Python 3.10, where a psycopg timed wait can consume a cancellation that races readiness. The model relay's maintenance loop, placement LISTEN/NOTIFY hints and the placement completion poller now repeat cancellation until they stop. `cancel_until_done` in `shared_control/model.py` is shared with `PostgresRelayState.aclose`.
- Placement waits and placement worker RPCs that hit their total timeout now take the retryable path on Python 3.10. Before, they raised `asyncio.TimeoutError`, which is not the builtin `TimeoutError`: client waits now return the retryable 504 `placement_wait_timeout`, and the worker defers or completes the command instead of leaving it until its lease expires.
- `StorageNativeNodeServer.serve_forever` accepts `poll_interval`; the default of 0.5 s is unchanged.
- Start splitting `ControlPlaneHandler` into `ucloud_sandboxes/gateway/` (plan C6.1, PR1–PR3):
  - `request_parsing`, `auth` and `node_rpc` (the worker HTTP pools, bounded proxy RPCs and their error contract) move out unchanged.
  - `RegistryReferences` owns the route, snapshot and pull Registry owners.
  - `FleetView` owns the freshness-filtered heartbeat reads and the heartbeat image-cache hits.

  `build_server` builds them once into `GatewayServices` (`BoundHandler.services`). Per-request transport state stays on the handler, behind the `Exchange` protocol: the reusable worker origin and whether a streamed upload consumed the body. There is no wire, schema or behavior change. Code that imported the moved names from `control_plane` must import them from `ucloud_sandboxes.gateway.registry_refs` or `ucloud_sandboxes.gateway.node_rpc` instead: `release_registry_route_references`, `_persist_registry_image_protection`, `_managed_registry_build_tag`, `_open_node_request` and the `_NODE_*_POOL` pools. The CLI and `scripts/` are updated. The dead `_sandbox_request_wakes` is deleted.
- Remove the unshipped shared-control scheduling qualification store
  (`qualification.py`, `dispatcher.py`, `fixtures.py`, `schema.sql`), its
  `qualification-migrate`/`qualification-status` commands, benchmark script and
  doc. The PostgreSQL contract and database crash checks now cover only the live
  relay and shared pool. Delete the unapplied July gVisor patches and correct
  stale rootfs, NBD, Docker store and exec-route documentation. Add
  `tests/test_package_budget.py`, which fails when `ucloud_sandboxes/` exceeds
  its checked-in line budget.
- Remove the shadow-only program scheduler (`program_scheduler.py`) and its
  outputs:
  - the per-wake shadow plan (`program_wake_shadow_plan` events), which read
    every fleet route on each response-ready wake for portable parks;
  - the per-cycle wake plan, program demand signals and calibration
    (`program_wake_plan`, `program_signals`);
  - `programs.shadow_wake_queue` and its dashboard panels.

  Program request phases still fence relay delivery, keep active model waits
  and deliveries local during cold offload, and are reported in metrics.
  `node_pressure_score` moves to `resource_admission.py` and
  `observed_memory_mb` to `consolidation.py`.

  The policy schema no longer accepts `program_aware_autoscaling_enabled`,
  `model_wait_capacity_weight` or `model_wait_max_headroom_nodes`. Re-render
  `deployment.json` with `scripts/hetzner_prod/make_config.py` before
  upgrading; rolling back to an earlier release requires restoring those keys.
  Lower the package line budget to 102,000.
- Warm immutable environment components on attach (plan C2.2, C2.3). At publication, builders walk each EROFS image with a strict reader of the on-disk format. They attach a separately signed metadata hint as an annotation on the component manifest; older workers ignore it. The hint lists the 256 KiB chunks that hold metadata and how many metadata bytes each holds. Images with unqualified layouts publish the unchanged hint-free manifest. On attach, the artifact backend fetches the densest hinted chunks (32 MiB, capped at a quarter of the cache) as verified ranges of up to 4 MiB, concurrently with the mount. `ensure` waits up to 5 s after the mount for them. The first attach of a component on a node records a startup chunk trace, which later attaches replay in the background. Prefetch uses at most a quarter of the miss slots and yields to waiting demand misses. It never fails an attach, and a reader that joins a bulk read keeps a demand read's single 30 s budget. Trace replays share at most half the cache, and metadata is never starved by them. The backend RPC now serves each request on its own thread, so one attach's wait no longer delays other components' liveness checks or drops. Node-local traces are bounded to 4,096. See `docs/immutable-environments.md`.
- **Tests: local fleet E2E harness (C8.4, first slice).** `tests/harness/` runs the real gateway (`control_plane.build_server`, real tokens) and real direct node agents over loopback HTTP, with heartbeats relayed node → gateway. Each node runs the real DirectSandboxService, Warden, hibernation journal, OverlayRootfsManager and storage-native service over its unix socket. Routing is SQLite, or PostgreSQL through `ucloud-postgres-routing-v1`. Only root or kernel boundaries are faked:
  - a stdlib `fake_runsc` that refuses any invocation it does not model;
  - a fake /proc cgroup line;
  - a copy-based overlay mount;
  - directory-backed block devices with hard-link seals;
  - local-directory images;
  - a libc pidfd shim, used only when the interpreter lacks `os.pidfd_open`.

  The fake runsc's `delete` signals only the recorded `sandbox.pid`, so the Warden's PID fence has to reap the sentry itself. Its `exec` forwards signals and reports a signalled command as 128 + signal, like `runsc exec`.

  `tests/test_local_fleet.py` adds five scenarios in about 9 s, one of them also run against PostgreSQL:
  - create → exec → files → delete, with no runtime, storage, mount or journal residue;
  - exec session events, replay, stdin and signal;
  - park → wake keeping disk and tmpfs state;
  - quarantine and delete of a crashed sentry;
  - node-agent restart.

  The scenarios pin three current behaviours as product findings:
  - worker loss shows up only on the next request; heartbeats and the status view keep reporting "running";
  - a missing file returns 503 instead of 404;
  - uv's CPython 3.10 cannot fence sentries.
- Route exec sessions by a signed prefix instead of a durable row. The gateway
  passes `X-UCloud-Exec-Session-Prefix` (HMAC over sandbox, generation and worker
  job) on exec start; current workers name sessions under it, so polls, stdin,
  signals and closes skip the routing database and the per-exec SERIALIZABLE
  route upsert. Lost, replaced and silent owners keep their 410/404/503 answers;
  older workers keep the durable route. No SDK change. The gateway's routed
  exec-session metric now counts only unsigned sessions.
- Report per-phase node create timings (admission waits, image resolve, storage,
  network, OCI build, rootfs, guest files, init install, `runsc create`/`start`,
  journal and registry commits) in the create response `timings.manager.phases`
  and the gateway's `node.timings` trace event.

- Add the RL-scale W0 measurement tools. `scripts/bench_rl_scale.py` is an
  SDK-driven benchmark with one `ucloud-rl-scale-bench/v1` report schema. It
  covers cold and warm time to first command, burst completion, creation rate,
  density at p99 tool latency, and park/wake, and its `merge` command builds a
  baseline from single-scenario reports. Node-side metrics and fork stay
  explicit nulls. `runtime/gvisor/spike_rl_scale.py` is a root-only spike probe
  for S1, S2 (page sharing), S4, S6, S7 and S8. See `runtime/gvisor/README.md`.

- Pipeline image building and filesystem publication with separate bounded
  admission phases. Allow up to two additional finishing builds while retaining
  four preparation/build slots; publish live capacity to the gateway and retain
  cleanup ownership until it finishes. See `docs/build-deadlines.md`.

- Preserve retryable builder-status failures instead of returning false build-not-found
  responses. Bound build execution, publication and owned subprocess cancellation
  with one configurable server budget; retain independent client wait semantics.
- Retain caches for distinct verified contexts before duplicate exports of a hot
  context. Raise the Hetzner production tag budget to 512 within the existing
  32 GiB shared-blob budget. See `docs/build-deadlines.md` for timeout and capacity behavior.

- Isolate selective OCI extraction and private filesystem squashing in a bounded
  fresh Python process per admitted build. Preserve parent-side signing and
  publication, complete-cache-hit behavior, integrity checks, and Docker fallback
  for unsupported layers; retain process and subphase timings in build history.

- Prepare selective EROFS layers with bounded kernel payload copies and reuse
  validated paths. Move files from private disposable diffs during squashing
  instead of copying their payloads and metadata again; borrowed Docker layers
  retain the existing copy path. Persist cache preparation and mount timings.
- Link known shared-cache blobs into managed-image repositories before pushing
  to avoid repeated large OCI uploads from fresh builders. Bound optional mount
  preparation and preserve normal upload fallback; remove redundant EROFS
  manifest reads while retaining authentication and retention checks.
- Materialize bounded partial EROFS cache misses directly from authenticated OCI
  layers, preserving signed output and falling back to Docker for unsupported
  filesystem semantics. Admit four builds per builder so excess work can choose
  the next available builder instead of waiting in a busy owner's local queue.
- Add bounded shared BuildKit caching for ephemeral builders, with immutable
  concurrent exports, final-layer cache mode, dedicated driver provisioning and
  separate registry retention. Preserve Docker overlay2 for EROFS publication.
- Persist observed terminal build summaries separately from autoscaler metrics
  and expose preparation, queue wait and execution timing independently.
- Reduce relay lifecycle queue scans with action-specific local hints, suppression
  of self-published notification echoes, and capacity-aware dispatch. Preserve
  periodic durable recovery and expose operation counts in relay statistics.
- Add `GET /v1/sandboxes?view=status` with optional exact `id` filters for compact
  fleet monitoring, omitting full specifications and attached snapshot metadata
  from database reads while preserving current state and worker freshness.
- Reuse authenticated EROFS layer components before pulling a completed build
  into Docker. Complete cache hits preserve the signed environment identity
  without image extraction or temporary mounts.
- Coordinate conversion of identical layer groups across threads and processes
  on one builder, rechecking the registry after acquiring the group lock.
- Record publication phase timings and component reuse counters in build results;
  retain observed terminal build timings in gateway metrics after builders exit.

## 0.5.114rc24

Resident continuations no longer occupy restore I/O permits. Cached backing placement reads no longer wait for allocator I/O. Exec output applies bounded cursor-acknowledged backpressure instead of evicting unread events; stalled consumers fail explicitly and completion waits for buffered output. Pair large-stdin exec workloads with SDK 0.4.27.

## 0.5.86 - 2026-09-22

- Let scheduling scans bypass the public fleet-poll queue, avoiding placement
  lock inversion when creates overlap wakes and heavy inventory polling.
- Make unchanged program lifecycle retries read-only using a joined route
  generation snapshot. Changed projections still recheck their fences inside
  the durable transaction.

## 0.5.85 - 2026-09-22

- Retain relay model waits according to live memory headroom, reclaim and queued
  demand instead of forcing a checkpoint after 15 seconds. Deferred parks
  recheck pressure with finite retries; drains and explicit parks still reclaim.
- Add rolling startup to the realistic load harness and reject performance
  qualification when provisioning overlap is absent or slow warmup is hidden.

## 0.5.84 - 2026-09-22

- Queue full fleet routing scans separately so concurrent polling cannot
  starve lifecycle writes with competing SQLite row iteration. Reads remain
  fresh; single-sandbox reads and lifecycle writes bypass the scan queue.
- Reconcile worker inventories and allocate creates under their atomic SQL
  transaction without holding the additional fleet projection lock.
- Exercise concurrent fleet polling in the realistic relay load harness,
  including poll latency and failures in its qualification report.

## 0.5.83 - 2026-09-22

- Interpret memory PSI as a percentage in warm retention, avoiding immediate
  park/checkpoint churn from minor reclaim stalls when memory is available.
- Make unchanged managed-process status polls read-only and let changed records
  use the transactional generation fence without the fleet projection lock.
- Read active image warmups without taking a write transaction or fleet lock,
  so heartbeat responses do not queue behind unnecessary durable commits.

## 0.5.82 - 2026-09-22

- Upgrade the pinned AgentEnv storage backend to v0.2.2, preserving streamed
  exports, ownership fences, cache identity and warm-device reuse. Include
  cache exhaustion/eviction safety, bounded premerged-index maintenance,
  hybrid discard/rewrite allocation reuse and an explicitly enabled jemalloc.
- Require fresh hybrid writable uppers and validate the complete native patch
  manifest at packaging and boot. Existing workers require sealed snapshot
  migration; do not replace the native daemon in place. Add cross-version
  migration, rollback, full-cache and lifecycle memory qualification.

## 0.5.81 - 2026-09-22

- Queue SQLite writers in arrival order and wake only the next writer. Avoid
  repeatedly waking every blocked request at each commit, while preserving
  grouped durable commits, per-operation rollback and ownership fences.

## 0.5.80 - 2026-09-22

- Commit relay worker poll heartbeats with inference claims, eliminating a
  separate durable transaction per poll. Claim and hydrate batches in one SQL
  statement while preserving registration fencing, distinct leases, ordering,
  and rollback if payload loading fails.

## 0.5.79 - 2026-09-22

- Coalesce gateway routing writes for 5 ms so concurrent lifecycle transitions
  share durable commits. This reduces writer contention without rejecting work
  or acknowledging uncommitted state; worker journal timing is unchanged.

## 0.5.78 - 2026-09-22

- Stop waiting SQLite writers from repeatedly waking one another while a closed
  batch awaits commit. Signal the flusher separately and release writers after
  durability; retain per-operation rollback and generation fences.
- Skip placement simulation for already-running or already-waking sandboxes,
  while still sending the fenced wake to the owner. Keep expected warm-retention
  deferrals out of durable program error writes.

## 0.5.77 - 2026-09-22

- Return a warm-retention retry deadline without holding worker or gateway HTTP
  execution. Preserve the original grace period and durable wake fences across
  retries, and dispatch both parks and wakes using asynchronous HTTP.
- Give durable wakes dispatch capacity independent of park backlog. Release
  deferred lifecycle claims until their next attempt instead of sleeping while
  holding dispatch admission. Keep accepted work queued durably during overload.
- Renew load-test inference leases while deliberately waiting for parking; fail
  promptly if renewal loses ownership. Use subsecond PostgreSQL latency buckets
  to distinguish connection waits, transactions, and durable commit time.

## 0.5.76 - 2026-09-22

- Keep node inventory and heartbeats available when an expired sandbox is still
  owned by a migration. Opportunistic expiry cleanup defers to the fenced
  migration deletion path without discarding the sandbox or its ownership.

## 0.5.71 - 2026-09-21

- Keep bounded local equivalents of successfully published layers using
  hardlinks. Same-worker wakes can read those files without downloading the
  published blobs; eviction preserves active and retired-device pins. Remote
  descriptors remain authoritative, and cache misses use the existing remote
  path. Count cache allocation once per inode instead of sparse logical sizes.
- Reuse completed Registry/S3 layer uploads after a later layer or snapshot
  metadata commit fails. Check immutable input identity and remote blob presence
  before reuse; retry missing blobs normally. Repeated compacted exports can
  also reuse their completed upload without rereading the source stack.
- Compact unpublished local checkpoint layers in the background, independently
  of remote publication. Reuse a dominant local base, retain completed work
  across wakes and appended deltas, and adopt it during a journaled mount or
  publication. Preserve retired-device inputs until release and clean up
  abandoned export hardlinks during reconciliation.
- Stop superseded snapshot exports during streaming for both Registry and S3,
  clean up incomplete uploads, and forward publication ownership checks through
  the backend router. Preserve immutable source layers and final revision fences.
- Remove whole-registry scans from image touches and lease updates. Check registry
  availability without a maintenance write transaction; retain full validation
  and expired-lease cleanup in maintenance snapshots.
- Use indexed lease lookups in gateway image protection, and let sandbox route
  reads proceed during heartbeat writes using committed SQLite snapshots.
  Deleting an absent sandbox no longer loads the entire routing database.
- Receive snapshot uploads into a reusable buffer to reduce copying and temporary
  memory, preserving immutable upload chunks, digest checks, and cancellation.
- Skip queued or retrying relay parks as soon as the model response is durably
  committed; keep already-dispatched parks fenced through completion. Trace
  dispatch queue time separately from lifecycle execution.
- Defer wake-triggered snapshot exports when no destination can admit the
  sandbox, retaining autoscaler demand. Allow busy/draining source workers to
  offload while keeping destination and local-wake admission checks intact.
  Use indexed per-sandbox migration lookup during wake placement.


## 0.5.70 - 2026-09-21

- Retain a dominant published base during depth-only snapshot compaction and
  merge the newer deltas instead. Registry and S3 preserve the existing depth
  and accumulated-delta bounds, with full merges for byte pressure and origin
  changes. This reduces full-base reads and uploads on repeated small updates.

## 0.5.69 - 2026-09-21

- Release relay lifecycle slots during retry backoff, cache validated heartbeat
  decoding, and keep best-effort metrics cleanup from waiting on SQLite readers.
- Avoid repeatedly compacting large snapshot bases; count accumulated delta data
  and allocated sparse-layer bytes while retaining the chain-depth bound.
- Include worker I/O pressure in placement and avoid optional consolidation onto
  more heavily stalled workers. Upgrade the gateway before workers.

## 0.5.54

- Retain generation-fenced node-loss records for seven days. Requests for a lost
  sandbox return HTTP 410 with `error_code: node_lost` and `retryable: false`,
  instead of a generic missing-route response. Existing retained program failures
  are backfilled; a newer sandbox incarnation is never labeled with an old loss.
- Preserve the existing relay behavior: acknowledge retained model responses for
  unavailable callers without replaying model work or claiming a successful wake.

## 0.5.53 - 2026-09-19

- Remove default fleet-wide create and per-worker active-device count ceilings.
  Worker admission queues, disk quota, memory checks, and gateway HTTP/body
  budgets continue to provide backpressure. Explicit operator count overrides
  remain supported; zero disables those optional count ceilings.

## 0.5.40 - 2026-09-19

- Isolate gateway-to-node exec event polling in its own bounded connection pool,
  preserving connections for tools, uploads, and lifecycle operations.
- Report connection-pool admission exhaustion as a safe pre-dispatch 503 instead
  of an ambiguous node transport 502.

## 0.5.39 - 2026-09-19

- Provide HTTP thread headroom for 256 live agent streams and concurrent tools:
  768 gateway request threads and 512 per node, with startup admission unchanged.
- Allow 1024 bounded exec sessions per node, so resident agent processes leave
  room for tool commands and retained results.
- Retain completed exec results for at least 30 seconds under capacity pressure;
  reject new commands before dispatch with safe retry information instead of
  evicting results before their callers can read them.

## 0.5.38 - 2026-09-19

- Preserve early handler rejection responses when clients are still sending
  upload bodies, using the same bounded socket drain as thread-cap rejections.
- Keep device reservations for newly running sandboxes until a heartbeat
  observes their restored devices, preventing concurrent wakes from overbooking.
- Coalesce live capacity refreshes before migrating work away from an apparently
  full owner, avoiding unnecessary publication after a full worker parks.

## 0.5.37 - 2026-09-19

- Preserve structured HTTP overload rejections while clients finish sending a
  request body. Rejected connections now half-close the response and drain
  incoming data with bounded time, bytes, and sockets, without blocking the
  accept loop or consuming request threads. This fixes broken-pipe failures
  seen during a 256-way park burst.

## 0.5.36 - 2026-09-18

- Isolate relay journal work from lifecycle HTTP calls so saturated park/wake
  threads cannot hold up durable responses, leases, and unrelated rollouts.
- Reserve device slots for in-flight wakes and migration destinations before
  heartbeats reflect them; include those reservations in local wake admission.
- Publish local-only parked checkpoints on demand when an owner cannot wake
  them, enabling migration to spare workers without publishing every relay park.
- Bound background checkpoint publication work and consume completed publication
  metadata immediately, with generation and placement fences intact.

## 0.5.35 - 2026-09-18

- Share startup admission across creates, restores and file transfers; reject
  before buffering uploads and keep gateway status/control traffic independent.
- Request bounded early worker headroom for sustained capacity queues, excluding
  reservation age and non-capacity failures, with provisioning credit intact.
- Use targeted sandbox inventory snapshots and cached publication metadata to
  avoid full-node storage RPC amplification during startup polling.
- Return explicit retryable restore/startup rejections before tool execution,
  and pass the scheduler's startup concurrency limit through worker bootstrap.
- Include the deployed relay descriptor-limit and terminal caller-loss fixes.

## 0.5.34 - 2026-09-18

- Balance image builds across live builder load while preserving active-build
  ownership and retry deduplication; report configured builder capacity caps.
- Bound cold-image preparation waits and node connection/pool acquisition so
  provisioning retries release gateway request and create-admission capacity.
- Refresh activity after wake and recheck idle parking under the sandbox lock
  to prevent immediate re-parking before a resumed command starts.
- Add optional consolidation of published parked sandboxes onto occupied
  workers at wake, with pressure/headroom checks, durable migration fencing,
  stable placement order, and a cooldown. Legacy configurations stay disabled.
- Report mixed operation errors and successes without implying recovery, and
  document the production health, capacity, and live restoration checks.

## 0.5.27 - 2026-09-04

- Fenced deletion by sandbox generation and made explicit wake honor node
  admission, live resource pressure, and concurrent restore limits.
- Serialized storage reconciliation against active mutations and introduced
  indexed, paginated live-volume inventory without removing replay tombstones.
- Retained remote storage dependencies across wake and reported them in running
  worker heartbeats; snapshot GC waits for complete dependency metadata.
- Bounded exec input/output by the overall timeout and retained failed build
  completion writes for later persistence instead of leaking build capacity.
- Fixed relay completion pin cleanup and replaced full completion scans with
  incremental indexes; refreshed retention when reusing uploaded build contexts.
- Added cross-component regression coverage and documented review findings and
  gateway-first upgrade requirements.

## 0.5.25 - 2026-09-03

- Made relay-driven parking retry transient lifecycle conflicts uniformly, so
  concurrent SDK status or log polling cannot disable later agent-aware park
  cycles while persistent activity remains bounded by the existing timeout.

## 0.5.24 - 2026-09-03

- Reduced steady-state UCloud autoscaler inventory work to state-filtered active
  jobs, with a periodic deployment-scoped full census and exact transition
  checks, instead of repeatedly paging through the project's job history.
- Avoided reinstalling already configured host dependency packages during the
  verified offline VM bootstrap, removing unnecessary initramfs regeneration
  from the Ubuntu 26.04 cold-node path.
- Unified Registry and S3 snapshot publication concurrency and queue telemetry,
  raised the configurable defaults to four publications and 128 ublk devices,
  and exported publication queue, duration, count, compaction, and byte totals
  through worker heartbeats.
- Split gateway-to-worker proxy latency into response-header and response-body
  spans, added a worker exec-start span with manager phases, and made matching
  image warmups an explicit retryable admission state until usable warm capacity
  exists.

## 0.5.23 - 2026-09-03

- Kept the public gateway on HTTP/1.1 while closing each reverse-proxy-side
  connection after its response, preventing UCloud ingress's idle upstream
  keep-alives from occupying every bounded gateway request thread. Private
  gateway-to-worker polling retains pooled connections.

## 0.5.22 - 2026-09-03

- Identified HTTP admission exhaustion as a guaranteed pre-dispatch fence, so
  coordinated clients can safely retry saturated exec, polling, and cleanup
  requests without treating an already-started mutation as replayable.

## 0.5.21 - 2026-09-03

- Preserved the typed storage-capacity result across the worker's
  storage-daemon Unix protocol, so the worker rollback and gateway requeue path
  introduced in 0.5.20 also applies in the real multi-process deployment.

## 0.5.20 - 2026-09-03

- Classified storage-native hard-capacity and ublk-device exhaustion as
  retryable node admission, rolling back partial worker ownership before the
  gateway requeues or selects another node instead of returning a raw 503.

## 0.5.19 - 2026-09-03

- Closed body-bearing HTTP connections immediately after header parsing and
  consumed valid sandbox-create bodies before admission control, preventing
  pre-body authorization or overload responses from leaving bytes that UCloud
  ingress could replay as the next request.

## 0.5.18 - 2026-09-03

- Prevented body-bearing requests from reusing HTTP/1.1 connections at the
  public gateway and worker server boundaries, so ingress cannot leave bytes
  that corrupt a later create, exec, metrics, or delete request; bodyless
  hot-path polling retains pooled keep-alive connections.

## 0.5.17 - 2026-09-03

- Prevented body-bearing gateway-to-worker requests from reusing HTTP/1.1
  connections, so an early worker response cannot leave unread bytes that
  corrupt a later create, exec, or delete request; bodyless hot-path polling
  retains pooled keep-alive connections.
- Updated the production load harness to construct the current public SDK
  `SandboxSpec` directly instead of relying on the removed keyword shortcut.

## 0.5.16 - 2026-09-03

- Scoped image-use leases to the deployment's managed Registry, so public
  host-qualified images such as GHCR tags no longer fail sandbox creation when
  their external manifests have no managed digest reference.

## 0.5.15 - 2026-09-02

- Made VM bootstrap wait for base-image cloud initialization and reconcile
  pending package configuration before installing the verified offline runtime,
  preventing Ubuntu 26.04 workers from racing `cloud-final` package activity.

## 0.5.14 - 2026-09-02

- Made Ubuntu 26.04 offline worker bootstrap bundles include the complete
  version-locked `util-linux` family instead of mixing base and update-pocket
  packages during installation.

## 0.5.13 - 2026-09-02

- Updated UCloud gateway, builder, and sandbox VM submissions to the live
  `vm-ubuntu:26.04` catalog entry after UCloud retired `vm-ubuntu:24.04`.
- Fenced node restarts with the host boot identity, immutable exec-session
  ownership, and one provider-declared destructive-loss proof contract; UCloud
  guest loss is terminal while recoverable Hetzner power states retain routes.
- Unified route loss, deletion, portable detach, Registry reference cleanup,
  lifecycle observation, and wake eligibility behind single authoritative
  classifiers, including crash-safe migration and publication compensation.
- Versioned fenced park/wake acknowledgements as `hibernate-local-v2`, so new
  parkable placements avoid legacy workers while existing legacy routes fail
  closed until their workers are drained and replaced.
- Reduced gateway and worker hot-path work with exact heartbeat lookup, compact
  JSON, shared short-lived runtime sampling, single-flight image inventory, and
  buffered incremental exec output; the SDK now uses adaptive exec long polls.
- Reported healthy as well as pressured runtime samples in autoscaling metrics,
  separated exec drain leases from create concurrency, and aligned direct-node
  disk admission with the sandbox's actual hard resource claim.

## 0.5.12 - 2026-08-29

- Added an authenticated gateway signal endpoint for attached exec sessions,
  allowing SDK process handles to terminate or kill remote processes without
  conflating signals with stdin or output streaming.

## 0.5.11 - 2026-08-28

- Made park and wake follow AgentEnv's single-flight lifecycle model: concurrent
  transitions join, then re-evaluate the stable runtime state, and repeated
  wake calls against an already-running sandbox succeed idempotently even when
  attached activity is present.
- Centralized the snapshot-publication wake fence in the node lifecycle owner
  and required sandbox-bound relay registrations to declare the managed-agent
  contract, preventing ordinary attached execs from entering agent parking.
- Updated all first-party parking qualification paths to use `start_agent()`
  and `register_agent_rollout()` rather than lower-level job or rollout calls.

## 0.5.10 - 2026-08-28

- Fixed gateway file uploads dropping the request body before proxying to the
  worker, which had produced empty files while returning HTTP 200.
- Kept only file downloads on the streaming-response path and routed uploads
  through the shared body-preserving mutation path.
- Added a production smoke test covering sandbox creation, PEP 723 upload,
  exact byte-for-byte read-back, execution, deletion, and cleanup.

## 0.5.9 - 2026-08-27

- Unified create, wake, exec, and autoscaler admission around measured resource
  pressure, with shared CPU and memory headroom rules and additive disk demand.
- Moved idle parking into the node lifecycle and reserved managed-agent parking
  for the coordinated SDK/relay contract, avoiding competing park decisions.
- Consolidated sandbox HTTP routing, scale-down eligibility, deployment
  convergence, artifact version discovery, and provider runtime profiles so
  every execution path follows the same policy.
- Aligned sandbox networking defaults with the SDK and removed unsupported
  snapshot-publication API surface that could not work end to end.

## 0.5.8 - 2026-08-27

- Made completed background snapshot publication visible through a validated,
  cached worker inventory descriptor without rebuilding it on every heartbeat.
- Acquired permanent snapshot references before granting portable route
  authority, made Registry reference reconciliation exact-key and idempotent,
  and deleted exact routes before releasing their references.
- Restored bounded create-pressure headroom while keeping durable actionable
  demand able to scale toward the configured fleet maximum; publication-only
  waits no longer create ineffective VM demand.
- Required portable authority for remote program-aware wakes, retained local
  wakes for local-only parks, and exposed publication saturation as diagnostics
  without merging it into actionable storage pressure.
- Kept snapshot publication concurrency below the storage operation ceiling so
  wake, mount, release, and delete work cannot be starved by uploads.

## 0.5.7 - 2026-08-27

- Fixed deletion of migrations whose source prepare response was lost: the
  source worker can now recover the unknown snapshot digest from its durable
  moving-out fence while still requiring the exact migration id.

## 0.5.6 - 2026-08-27

- Fixed deletion of successfully migrated sandboxes: activated imports retain
  their migration identity as a storage fence but no longer fail every ordinary
  delete with HTTP 503.
- Split durable-delete retries from the storage-detach budget, allowing cleanup
  backlogs to drain promptly without starving node detachment.
- Added bounded controller reconciliation and metrics for legacy active
  migration journals whose canonical sandbox route is already absent.

## 0.5.5 - 2026-08-27

- Replayed durable sandbox delete intents from the autoscaler so a client does
  not need to remain connected until cleanup succeeds.
- Made gateway deletion cancel an uncommitted storage-native migration before
  retrying the generation-fenced worker delete.
- Terminalized orphaned migration journals when their sandbox route is deleted,
  preventing stale migration reservations from surviving cleanup.
- Added bounded per-cycle pending-delete results to autoscaler observability and
  verified the complete delete, drain, and provider-stop sequence.

## 0.5.4 - 2026-08-27

- Added traffic-independent model-relay maintenance so expired requests and
  worker leases advance even when no client is polling relay state.
- Made park failures explain when attached exec or file activity cannot survive
  a gVisor restore and direct long-lived agents to the checkpoint-owned managed
  process API.
- Stopped lifecycle notification retries immediately for this permanent park
  conflict while retaining bounded retries for transient lifecycle races.
- Documented the coordinated backend/SDK contract for parking-aware agents:
  managed process state and logs survive park/wake, while attached exec
  transports deliberately fence parking.
- Accepted Go RFC3339 nanosecond timestamps on Python 3.10 by retaining their
  representable microsecond precision, keeping managed-agent state portable
  across the supported Python matrix.

## 0.5.0 - 2026-08-13

- Replaced SQLite pseudo-traces with bounded, nonblocking OTLP/HTTP traces and
  metrics, W3C propagation, exporter-health reporting, and a strict schema-5
  telemetry contract.
- Added correlated gateway, worker, relay, exec, image-build, park/wake,
  storage Unix-socket, S3/Registry publication, provider, and VM-bootstrap
  spans, including async context capture and queue-wait phases.
- Enabled AgentEnv's loopback Prometheus endpoint, removed the old dashboard
  trace store, and added overload and microbenchmark coverage proving exporter
  backpressure cannot delay product work.
- Added sampled per-span thread CPU duration, from which the trace backend can
  derive CPU/wall ratios and distinguish wait-heavy phases from optimization
  candidates.

## 0.4.1 - 2026-08-13

- Moved detached sandbox snapshot authority from the gateway Registry to
  Hetzner Object Storage while retaining worker-local NVMe for active COW,
  application memory, and attached parks.
- Added direct bounded-parallel multipart publication, content-addressed
  commits, lost-completion recovery, verification, backend-switch compaction,
  and AgentEnv native S3 range-read configuration.
- Added route-referenced mark-and-sweep snapshot GC, a daily systemd timer,
  incomplete-multipart lifecycle guidance, and strict environment-only S3
  credential propagation.
- Qualified real CPX62 park, detach, compact, cold wake, lazy faulting, and
  full-working-set correctness against the `hel1` Object Storage service,
  including 107.93 MiB/s for a verified 1 GiB publication.
- Added explicit pure-Python dependency injection when repacking a qualified
  node bundle so the S3 runtime remains compatible with golden snapshots.
- Recovered provider-accepted deletes after controller restart so Hetzner may
  safely reuse a deleted worker's private IP without colliding with its stale
  heartbeat binding.
- Defaulted SDK sandbox networking to the production isolated bridge path.

## 0.4.0 - 2026-08-12

- Added a production-shaped Hetzner provider and qualified CPX12 gateways,
  CPX62 workers, Ubuntu 26.04 golden images, private networking, Volume-backed
  registry storage, and end-to-end agentic park/wake behavior.
- Made durably published parked sandboxes portable and detachable so local
  worker disk limits the active working set rather than the total parked
  population, with crash-fenced publication, eviction, and cold wake.
- Upgraded the storage-native backend to AgentEnv v0.1.2 and added streamed
  snapshot-chain compaction, shared bounded remote-layer caching, and corrected
  owner and pooled-device lifecycle handling.
- Split low-latency gateway databases from the Registry blob root, added mount
  fencing for the Volume-backed blob store, and recorded live detached-wake and
  compaction qualification evidence.
- Corrected Hetzner decimal-GB disk normalization, removed worker swap, and
  bounded the CPX62 active storage profile without over-advertising local disk.
- Made the direct gVisor Warden and storage-native backend the only sandbox
  runtime and durable park/migration path.
- Made deployment identity and the gateway, heartbeat, and node-control
  credentials mandatory and distinct.
- Made role-specific, digest-verified runtime bundles mandatory for node boot.
- Reduced the gateway, node agent, SDK, relay, dashboard, persisted state, and
  configuration to one strict greenfield contract.
- Removed historical runtimes, implicit state conversion, protocol aliases,
  duplicate service assets, planning documents, and alternate migration paths.
