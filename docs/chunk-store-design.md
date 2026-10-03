# Content-addressed chunk store (C2.13)

Design for the chunk store that C2.13 decided on 2026-10-02
([plan](rl-scale-architecture-plan.md) C2.13/C2.14, spikes
[S10](benchmarks/nydus-spike-2026-10-02/README.md) and
[S11](benchmarks/fscache-spike-2026-10-02/README.md)). Images are converted per
layer to Nydus RAFS v6 with nydus-image v2.4.5, with no chunk dictionary. Workers
mount the bootstrap with kernel EROFS over our NBD backend. We store each 256 KiB
chunk once, keyed by the sha256 of its uncompressed bytes. This is a design only;
no code has changed.

## Summary

- **S3 holds the truth from day one** (Hetzner Object Storage, the existing
  `hel1` bucket): packs, bootstraps and chunk maps. The registry Volume never
  receives chunk bytes, so it only shrinks. C2.6 store nodes come later as a
  read-through NVMe cache, with no data migration.
- **Packs** are immutable, at most 64 MiB, named by sha256 and written one per
  conversion, in image address order. Their sorted footers can always rebuild
  the index.
- **The global index is SQLite** (`ucloud-chunk-index`, about 40 GB at 500M
  chunks), used only by builders and GC. Each image carries a signed **chunk
  map** (address → chunk id) and an unsigned, regenerable **locator** (chunk →
  pack range).
- **Trust is unchanged in kind.** Signed roots cover the bootstrap and chunk-map
  digests, and every chunk is verified after decompression. S3, the store tier,
  the index and locators stay untrusted.
- **GC** is mark-and-sweep by pack from live roots, condemning before it
  deletes. Compaction is deferred.
- **Migration** runs in three waves, foundations first. The Volume falls from
  about 3.8 TB of image data to about 0.5 TB (the OCI build inputs), and S3
  gains about 0.3 TB.

## 1. Data model

### 1.1 Chunks

| Field | Definition |
| --- | --- |
| Chunk id | `sha256(uncompressed bytes)`, the `block_id` of the bootstrap's 80-byte `RafsV5ChunkInfo`. Requires `--digester sha256`; blake3 is the default. |
| Size | At most 256 KiB (`--chunk-size 0x40000`, our cache unit, `environment_artifact.py:26`). Chunks never span files, so most are small: mean 19 KB compressed (S10). |
| Encoding | A flags byte, `zstd` or `raw`. We keep the bytes nydus-image emitted (zstd, chunk flag bit 0) and never recompress. The packer stores a chunk `raw` when zstd saves under 3% or the chunk is under 4 KiB. All-zero chunks dedupe to one entry. |
| Identity | The id covers the uncompressed bytes, so two builders' encodings of a chunk are interchangeable. The first committed copy wins; readers verify after decompression. |

### 1.2 Pack files

Key `production/chunks/packs/<hh>/<sha256>.pack` in bucket
`ucloud-sandboxes-prod-20260926` (`scripts/hetzner_prod/make_config.py:20`).

| Part | Layout |
| --- | --- |
| Header | 16 bytes: magic `UCPK`, version 1, flags, reserved |
| Data | Chunk payloads back to back, with no record headers, in the order the conversion's bootstrap addresses them (file order) |
| Footer | N entries sorted by id, 49 B each: `id[32] offset:u32 clen:u32 ulen:u32 flags:u8 reserved[4]` |
| Trailer | 48 bytes: footer length, entry count, `sha256(footer)`, magic |

At most 64 MiB keeps each pack to one PUT, with cheap retries and no multipart
state; a large conversion writes several packs. Small packs are fine (a ScaleSWE
image adds a median 0.7 MB); expect about 0.5M packs at full corpus. Naming by
sha256 makes uploads idempotent and nothing is overwritten. A suffix range read
(`bytes=-N`) fetches a footer, which is how the index is rebuilt without the
database.

### 1.3 Global index (builders and GC only)

SQLite in WAL mode, owned by `ucloud-chunk-index` (§5):

```sql
CREATE TABLE chunks (id BLOB PRIMARY KEY, pack INTEGER, off INTEGER, clen INTEGER,
                     ulen INTEGER, flags INTEGER, condemned INTEGER DEFAULT 0) WITHOUT ROWID;
CREATE TABLE packs  (pack INTEGER PRIMARY KEY, digest BLOB UNIQUE, bytes INTEGER, chunks INTEGER,
                     origin_root BLOB, created INTEGER, state TEXT);          -- live | condemned
CREATE TABLE layers (diff_id BLOB, converter TEXT, bootstrap BLOB, created INTEGER, claimed_until INTEGER,
                     PRIMARY KEY (diff_id, converter)) WITHOUT ROWID;          -- per-layer conversion cache
CREATE TABLE roots  (component BLOB PRIMARY KEY, chunk_map BLOB, registered INTEGER, epoch INTEGER) WITHOUT ROWID;
CREATE TABLE root_packs (component BLOB, pack INTEGER, bytes INTEGER, PRIMARY KEY (component, pack)) WITHOUT ROWID;
```

`chunks` is a derived cache of pack footers, and `roots`/`root_packs` of chunk
maps. All of it can be rebuilt from S3.

### 1.4 Per-image references

| Object | Content | Signed | Median size | Where |
| --- | --- | --- | --- | --- |
| Bootstrap | RAFS v6 metadata from `nydus-image merge --original-blob-ids`: inodes, directories, device table, chunk table | digest and size in the component | 8 MB (2.8 MB zstd) | `meta/<sha256>.boot.zst` |
| Chunk map | `ucloud-chunk-map-v1`. Header: bootstrap size, device size, blob regions (id, `mapped_blkaddr`, size). Then entries `(device_offset:u64, ulen:u32, id[32])` sorted by offset in the image's unified device space. | digest and size in the component | 31.6k × 44 B ≈ 1.4 MB | `meta/<sha256>.map` |
| Locator | Entry *i* ↔ chunk-map entry *i*: `(pack:u32, offset:u32, clen:u32)`, plus a pack table with presigned URLs and an `epoch` | **no**; a hint only, since wrong data causes a verified miss | ≈ 150 KB zstd | served by the index service; cached on the node per (component, epoch) |

**Why a chunk map when the bootstrap's chunk table already has the ids?** It pins
our trust surface to our own format (upstream has a "TODO: get rid of the chunk
info array", S10 risks). It is sorted in device coordinates, so a read is one
binary search. And the builder proves it against the mounted tree before
signing: S10's corrupt dictionary images had chunk tables and inode addresses
that disagreed while every chunk verified, which checking ids alone cannot
catch.

### 1.5 What we sign and what workers verify

- **Root:** today's `ImmutableEnvironment` schema, unchanged
  (`environment_artifact.py:531-577`). Its `base` is a new component kind,
  `ucloud-environment-rafs-v1`, signed under the domain
  `ucloud.immutable-environment-rafs.v1\0`. Toolkits can still overlay it
  (`environment_rootfs.py:134-169`).
- **The component signs** `source_image` (the OCI config digest),
  `source_layers` (the diff IDs), `bootstrap {digest, size}`,
  `chunk_map {digest, size}`, `device_size`,
  `format {rafs: 6, converter: "nydus-image v2.4.5", chunk_bytes: 262144, digester: sha256, compressor: zstd}`
  and `producer_key`.
- **Publication:** an OCI manifest in `environments` with `layers: []`, as roots
  are today (`environment_artifact.py:668-673`). Protection tags, owner leases
  and root retention (`registry_retention.py:289-327`) keep working.
- **Determinism:** Ed25519 is deterministic and conversion is `--repeatable`, so
  the same OCI input and converter always give the same root digest. That is
  C2.14's "same task, same root" gate.

**Workers verify** (steps 1–4 before any privileged ioctl, as today at
`environment_nbd.py:136-137`):

1. The root and component signatures, against the producer trust file.
2. That `root.source_image` equals the config digest the gateway dispatched.
3. The bootstrap's sha256 and size, after zstd.
4. The chunk map's sha256 and size, and its structure: sorted, non-overlapping,
   every `ulen` at most 256 KiB and inside `device_size`, and regions equal to
   the bootstrap's device table.
5. Every chunk, as today (`environment_cache.py:288-337`): decompressed with
   output capped at `ulen`, then `len == ulen` and `sha256 == id`, before it is
   installed or served.
6. The locator, for bounds only. A digest failure triggers one locator refetch,
   then EIO, never zeros.

**Never trusted:** S3, store nodes, the index service, locators, compressed bytes.

**Builders verify before signing:** every new chunk decompresses to its id; the
chunk-map regions match the bootstrap's device table; and the full tree
compares equal (§3, step 7).

## 2. Where it lives

| | Registry on the gateway Volume (today) | S3, Hetzner Object Storage | NVMe store nodes (C2.6) |
| --- | --- | --- | --- |
| Capacity | One Volume, 3.5–4 TB now, at most 10 TB. The full corpus (7–10 TB) does not fit. | Unbounded | ≤ 960 GB local disk per Cloud VM: a hot-set cache |
| Cost (list price, verify) | ≈ €44–57/TB-month | ≈ €5–7/TB-month, egress ≈ €1/TB | Two CCX-class VMs, compute-dominated |
| Demand-read latency | 3 ms p50 through the registry (`image-import.md:68`) | 319 / 1,785 ms p50 / max **via Distribution's S3 driver** (`image-import.md:54`). Direct ranged GETs are assumed at 30–100 ms; S12 measures them. | ≈ 1–3 ms (assumed) |
| Bulk throughput | 50–69 MiB/s under build load at 48–205 ms await (plan C2.6); about 315 MB/s sequential at best (`hetzner.md:535`) | ≈ 240 MiB/s from one worker (256 MiB in 1.06 s, `object-storage-snapshots.md:247`). Assumed per worker, up to a per-bucket request limit. | ≥ 1 GB/s across two nodes (plan C2.6) |
| Writes | 144 × 256 KiB/s at 32-way (`image-import.md:67`) | 80–108 MiB/s for parallel multi-MB PUTs (`object-storage-snapshots.md:242-243`). Packs keep writes large. | Fills from S3 |
| GC | The physical sweep stops the registry (`managed-registry.md:386`) | DELETE, online | Cache eviction |
| Gateway in the byte path | Yes, on one NIC | No | No |

**500-sandbox burst.** C2.6's budget case is 64 tasks × 8 rollouts, each
touching about 360 MB: 23 GB with groups packed, 69 GB spread over 3 nodes. C9.1
is unmeasured, so these are assumptions:

- Compressed transfer brings that to about 0.53× (S11), 12–37 GB. It is lower
  still because the node cache, keyed by chunk id, shares foundation chunks
  across images.
- From S3 directly, 3 workers at about 250 MiB/s each move 37 GB in about 50 s,
  against 9–12 minutes from the Volume today.
- 1 GB/s in 1 MiB requests is about 1,000 GET/s, which may hit a per-bucket
  limit. Prefetch and hydration therefore use 4–8 MiB coalesced ranges, and only
  demand misses use 1 MiB windows.
- Latency matters only for un-prefetched demand misses. A cold `import sys`
  reads 8–12 MB, about 10–20 windows, and trace replay gave zero demand misses
  in qualification (plan §9).

**Recommendation.**

- **Phase A (M1–M3):**
  - S3 holds packs, bootstraps and chunk maps.
  - `ucloud-chunk-index` is its own systemd unit on the gateway's local disk
    (about 1.2 GB for the migrated corpus), backed up nightly to `index/backup/`.
  - Workers range-read S3 through per-pack presigned GET URLs in the locator
    (24 h, refreshed on 403). They hold no S3 key, since Hetzner keys are
    project-wide (`hetzner.md:795`).
  - Builders and the index service use `HETZNER_S3_*` through
    `Boto3S3ObjectClient` (`storage_native_s3.py:78`), lifted out of code that
    C1.3 deletes.
  - Config: a `chunk_store` block shaped like `snapshot_store` (`config.py:44-125`).
- **Phase B (C2.6), built from the start because S12 failed the gate:** a
  store node on the private network, described next. It replaced the earlier
  sketch (nginx with `slice 1m` and `proxy_cache`, S3 as the locator's
  fallback): workers have no S3 fallback, and the index moved with it.

### C2.6 store node (as built)

`ucloud-chunk-store` (`chunk_store_node.py`, `serve-chunk-store`) is a
read-through cache over S3 on one store node's NVMe; operator steps are in
[hetzner.md](hetzner.md#chunk-store-node-c26). Benchmarks:
[chunk-store-node-2026-10-02](benchmarks/chunk-store-node-2026-10-02/README.md).

- **Reads.** `GET /v1/objects/<key>` with a single `Range` (or none), for the
  chunk store's content-addressed keys only (`packs/hh/<sha>.pack`,
  `meta/<sha>.boot.zst`, `meta/<sha>.map`), authenticated by the index's read
  token. Workers hold that token and no S3 credential or URL.
- **Fills.** A miss fetches one aligned extent of `extent_bytes` (4 MiB in
  production) from S3 with the node's own key, by a SigV4-presigned GET (M1's
  `S3Presigner`). Concurrent misses on one extent join one fill. A GET that
  has made no progress (no first byte, or no body bytes) for 3 × the median
  TTFB, within 150 ms–2 s, is hedged by another, at most twice; the first
  complete attempt wins and the losers are cut off. A slow but flowing GET is
  never duplicated: in a bandwidth-bound burst that only splits the
  bandwidth (hedging on elapsed time, as S12 tried, made 3.4× the hedges).
  5xx, 429, timeouts and short bodies retry with backoff for 60 s; then the
  read answers 503, which workers retry and finally turn into EIO.
- **Cache.** LRU under a byte budget, with in-flight fills reserved. A fill is
  written in `tmp/` while hashed and renamed into place with its sha256 in the
  file name; nothing is fsynced. After a restart every extent is hashed on
  first use, so one torn by power loss is dropped and refetched, never
  served. Whole packs and chunk maps must also match their names, or they are
  not kept.
- **Serving.** One asyncio loop: a request whose extents are present answers
  in the loop with sendfile(2); a fill (or a first hash) waits in a thread
  pool. On this machine the loop served 1 MiB ranges from page cache at
  3.7–3.9 GiB/s with p99 25/56/101 ms at concurrency 64/128/256, saturating
  one core. Thread-per-connection served the same bytes but took up to 2.1 s
  to accept a 256-connection burst under a busy GIL (asyncio: 121 ms).
- **Fill unit: 4 MiB extents, not whole packs.** In a cold 500-sandbox burst
  against an S3 stand-in with S12's latency and a 250 MiB/s cap, 4 MiB
  extents with hedging ended the burst in 20.4 s with a cold-start p99 of
  17.4 s (median of 3 runs). Whole 64 MiB packs: 24.5 s and 23.3 s, moving
  3.3× the bytes readers need against 2.4×; 1 MiB extents: 30.0 s, since a
  1 MiB window usually spans two. Workers reading S3 directly (Phase A):
  40.0 s and 28.2 s. Hedging took the 4 MiB cold-start p99 from 29.0 s to
  17.4 s. The cold burst is S3-bandwidth-bound, so warming matters more: with
  the run's foundation packs filled first (10.5 s), the burst took 12.4 s
  with a cold-start p99 of 6.2 s. Warm jobs take whole objects, filled
  extent by extent.
- **Prefetch.** `POST /v1/warm` (write token) takes objects or ranges and fills
  them with bounded concurrency, behind demand fills; `GET /v1/warm/<job>`
  reports extents done, cached, failed and bytes. `warm-chunk-store
  --component …` warms a component's packs and metadata from its locator
  (C9.3).
- **Metrics.** `/v1/metrics`: requests, hits, misses, coalesced, bytes served,
  fill waits, S3 requests, bytes, errors, retries, hedges and hedge wins, TTFB
  and fill percentiles, cache bytes, extents, evictions and failed checks.
  `/healthz` is open.
- **Index.** With `store_node.serve_index`, `ucloud-chunk-index` runs on the
  store node (its RAM holds the 31 GB index resident, S12) and the gateway's
  unit only creates the tokens. Worker locators name store-node URLs, which do
  not expire; builder lookups stay presigned, since builders hold the key.
- **Workers fail closed.** With `--chunk-store-url` the backend reads only
  URLs under the store node, with the token, and refuses any locator that
  names something else; a store outage is EIO after the fetch deadline, never
  zeros, and never S3.
- **Node init.** The `store` role of VM init: the bundle's agent runtime, the
  block, both tokens, the S3 key and the two units; no Docker, node agent or
  heartbeat, so placement never sees the node.

## 3. Write path

One path serves both migration (§7) and C2.14 builds, on every builder (CCX33),
with 16–32 conversions in parallel.

1. **Input.** Read OCI layers from our registry by digest; there are no upstream
   pulls. Verification needs the Docker merged rootfs, from our registry as today
   (`environment_builder.py:945`). A C2.14 build is already local.
2. **Per layer.** Claim `(diff_id, converter)` in `layers` for 30 minutes; a
   second builder waits, which removes the common duplicate of parallel builds on
   one foundation. If the row is complete, download its bootstrap and skip the
   layer. Otherwise run `nydus-image create -t targz-rafs --fs-version 6
   --digester sha256 --compressor zstd --chunk-size 0x40000`, at about 27 CPU-s
   per GB of gzip (S10). Never `dir-rafs`, which drops mtimes, and never
   `--repeatable`, which zeroes owners. A layer tar out of depth-first path order
   is first rewritten in that order (uncompressed, `-t tar-rafs`): `nydus-image`
   drops whiteouts of a layer that returns to a directory it left (OpenSWE's slim
   layers, M1 gate). The converter identity records it (`;order=path`).
3. **Dedupe.** Parse the chunk table (S10's `scripts/rafs.py` is about 40 lines).
   Reserve the distinct ids in batches of 256 (`POST /v1/chunks/reserve?owner=`),
   just before packing them, and again from the current id after each pack is
   committed. Per id the reply is known (live, not condemned), reserved for this
   builder, or busy (another builder's hold, 10 minutes, renewed each batch).
   - Busy ids are asked for again only after this builder's own pack is
     committed, so builders never wait on each other.
   - A hold of a builder that died lapses, and the next asker packs the chunk.
   - Every chunk is committed before its layer is.
4. **Pack.** For each unknown id: read its compressed bytes from the local Nydus
   blob, decompress and verify it, and append it in blob order. When a pack is
   full, write the footer, hash the pack, PUT it to S3 (skipped if a HEAD shows
   it exists) and commit it (step 5) before starting the next. The Nydus blobs
   are discarded.
5. **Commit.** Per pack, `POST /v1/chunks/commit {pack digest, size}`. The
   service HEADs the object and reads its footer, and one transaction does
   `INSERT OR IGNORE` of the chunks and the pack row. After the last pack and the
   layer bootstrap are durable, a final commit completes the layer row.
   Invariant: a pack is durable in S3 before any row names it.

   Converters running at once share chunks across different layers. With one
   lookup per layer and one commit after all its packs, the M1 gate's 12-way
   convert pass stored 5.1 GB of duplicate chunks in 22.9 GB. Per-pack commits
   left 2.3 GB (run 2). Reservations (step 3) pack each chunk once. A commit
   clears its chunks' holds; a condemned chunk is reserved and repacked like an
   unknown one.
6. **Image.** Run `nydus-image merge --original-blob-ids` over the layer
   bootstraps, with at most 254 blobs (the largest image seen has 25 layers).
   Then derive the chunk map, upload the bootstrap and map, and sign the
   component and root.
7. **Verify.** Mount the image through **the worker's own RAFS device**, reading
   this conversion's packs plus the store, so exactly the bytes workers will
   serve are checked. Compare the full tree with the Docker merged rootfs: names,
   types, modes, owners, sizes, the sha256 of every file, symlinks, xattrs
   (excluding overlay internals), hardlink groups, whiteouts, opaque directories,
   and file mtimes against the tar headers. Directory times are compared loosely
   (`immutable-environments.md:83-86`).
8. **Register.** `POST /v1/roots/register {component, chunk_map}`: the service
   fetches the map, checks that every id is present and not condemned, writes
   `root_packs`, and sets locator epoch 1. Then publish the root manifest
   (`publish_environment`, `environment_artifact.py:639-674`), and the gateway
   records image → root (§7).

| Crash point | State left behind | Recovery |
| --- | --- | --- |
| Before the pack PUT | Local temporaries | Rerun |
| After the PUT, before commit | Orphan pack | GC deletes row-less packs once they are over 24 h old |
| After a pack's commit, before the layer row | Committed chunks, an incomplete layer | A rerun claims the layer again and skips the committed chunks |
| After commit, before registration | Committed, unreferenced chunks | A 7-day grace protects them; a rerun reuses them |
| After the root manifest, before the gateway record | Unreferenced root | Registry retention drops it after 1 h; a rerun yields the same digest |
| Registration finds condemned ids | Nothing registered | Reconvert; the chunks are written again |

**Write concurrency.** The index takes one SQLite writer: about one commit per
pack, a few per second at 32 builders, plus one lookup per 256 new ids and one
after each pack. Lookups are concurrent WAL reads.

## 4. Read path on workers

1. **Resolve.** Today the worker reads the root from the OCI annotation
   (`environment_rootfs.py:171-213` → `environment_artifact.py:717-735`). New:
   the gateway dispatches `(config digest, root digest)` and the worker calls
   `load_environment` directly, so a migrated image's OCI manifest can be
   deleted. The annotation remains the fallback for unmigrated images.
2. **Attach** (one NBD device per image, 0.18–0.24 s in S10). Fetch and verify
   the bootstrap, chunk map and locator in parallel (§1.5). The device exposes
   the unified space: the bootstrap at offset 0 and each blob at
   `mapped_blkaddr × 4096`, mounted with `mount -t erofs -o ro`. Same-image
   sandboxes share the mount, and 500 distinct images fit `nbds_max=1024`. The
   backend dispatches on component kind, so EROFS components keep today's path
   until migration ends.
3. **Read(offset, length)** (at most 32 MiB, `environment_nbd.py:21`). The
   bootstrap region is served locally. Elsewhere, binary-search the chunk map;
   holes read as zeros, since the full-tree check proved no file data maps
   there. Each id is looked up in the node cache, which is already keyed by
   digest hex (`environment_cache.py:147-169`) and so shared across images
   unchanged. Misses join in-flight fetches through the existing single-flight
   map (`environment_cache.py:171-240`).
4. **Fetch planning.** Group misses by locator pack and sort by offset. Merge
   across gaps under 64 KiB while the window stays within 1 MiB compressed.
   Extend each window with other uncached chunks of *this image's* map in the
   same pack, up to 1 MiB; packs are in address order, so this read-around is
   free. Issue one GET per window and verify and install every chunk in it.
   Raise `concurrent_misses` from 8 to 32 for S3 latency.
5. **Prefetch.** The bootstrap *is* the metadata and is local after attach, so
   `find /` makes no remote reads and C2.2's signed hints are unnecessary for
   RAFS images. Traces record chunk ids rather than component indices (the
   cache's `_observe`), per root in `LocalTraceStore`
   (`environment_trace.py:51-117`), and replay as pack-coalesced 4–8 MiB ranges
   under today's budgets (256 MiB, a quarter of the miss slots). Ids are global,
   so one image's trace warms its siblings. C2.7 hydration and C9.3 seeding ship
   trace sets with the bootstrap, map and locator.
6. **Attach concurrency.** Today `_attach` holds the backend-wide `_guard`
   across the registry load, the NBD bind and the mount
   (`environment_backend.py:203-265`), which put the median attach at 1.6 s under
   a 20-way burst (S11). New: a single-flight future per component. The global
   guard covers only device selection and the `_active`/`_components` maps; the
   fetches and the mount run outside it. Target: S11's 0.25 s.
7. **C2.1's Rust device** implements this same contract (chunk map, locator,
   pack ranges, the cache), so the Python version is the reference
   implementation.

**Trade-off.** Today, images on one foundation share its component mount and so
its page cache. Per-image RAFS mounts share chunks on disk but not in the page
cache, unless `EROFS_FS_PAGE_CACHE_SHARE` arrives. Rollout groups use one image,
the case S2 measured, so the loss should be small. M1 measures it.

## 5. Index scale

| Quantity | Migrated corpus (≈ 0.3 TB, ≈ 15M chunks) | Full corpus (500M chunks) |
| --- | --- | --- |
| `chunks` rows: 49 B of payload, about 80 B in a B-tree at about 70% fill | ≈ 1.2 GB | ≈ 40 GB |
| `packs` | ≈ 10k | ≈ 0.5M (40 MB) |
| `root_packs` (about 200 per root) | ≈ 1.3M | ≈ 13M (0.5 GB) |
| Chunk maps / bootstraps (zstd) in S3 | ≈ 9 GB / 18 GB | ≈ 95 GB / 0.17 TB |
| Index RAM (page cache) | 0.5 GB | 2–4 GB. Interior pages stay resident, so a lookup costs at most one NVMe read. |

**Engine: SQLite.** It is how this codebase already keeps state (`images.sqlite`,
`registry-usage.sqlite`): one file, transactional, and one writer is all the
write rate needs. PostgreSQL is rejected because the gateway's instance shares a
160 GB disk and the control plane's I/O, and would need about 60–80 GB.
Immutable sorted runs (LSM style) would add merge and compaction code; the
per-pack footers already exist, so we can switch later without a format change.

**Write-path cost.** The layer cache skips every converted layer, so lookups
cover only new layers: at most about 30k ids per image in one request, about
0.1–1 s against a 70 s build. New entries average 500M / 66.8k ≈ 7.5k per
image, or about 3.4k inserts/s at 32 builds per 70 s, in per-pack transactions.
S12 must confirm a 30k-id batch in ≤ 1 s and ≥ 10k inserts/s on a synthetic
500M-row database.

**Recovery.** Restore the nightly backup, then replay newer packs from their
footers. A full rebuild is about 0.5M suffix GETs (about 25 GB) and takes hours.

**Workers** never query the index. They fetch only the image's chunk map (signed,
immutable) and its locator (per epoch).

## 6. Garbage collection and retention

**Mark-and-sweep, not reference counts.** Counting would need exactly-once
increments and decrements across S3, the index and registry retention, for 500M
entries: a lost decrement leaks, and a lost increment deletes live data. Marking
from roots recomputes the truth every run, and an interrupted run is harmless.

**Live roots** come from today's retention, which already handles owner leases,
routes, builds and grace (`registry_retention.py:274-327`). They are the roots
with a durable or leased owner in `RegistryUsageStore`
(`managed_registry.py:780,1025`), the roots the gateway's `image_roots` maps from
a live image, and the annotated roots of images not yet migrated.

**Training corpus (C2.14).** Each corpus root gets a durable reference under
owner `training-corpus:<id>`. Durable references never expire, and pressure
eviction already skips referenced images. The index reports unique chunk bytes
per owner, which is C2.14's growth budget.

**Weekly run:**

1. **Mark.** Live packs are the union of `root_packs` over live roots, plus
   every pack younger than 7 days.
2. **Condemn,** in one transaction. Set `packs.state` and `chunks.condemned` for
   every unmarked pack. Delete `layers` rows that no live root lists in
   `source_layers` and that are older than 7 days, so a later build reconverts
   rather than skips. Lookups then report those ids as unknown, and registration
   rejects them (§3, step 8). Both run in one database, so they serialize: a
   builder that looked an id up before condemnation fails registration and
   rewrites the chunk. No root can come to reference a deleted chunk.
3. **Delete,** after a 48 h hold (more than the longest build plus
   registration): delete each condemned pack's rows and its S3 object, and
   delete row-less S3 packs older than 24 h. Dead components' bootstraps and
   maps follow the same cycle.

**Compaction** waits until dead bytes exceed 20% of the store, which the frozen
corpus makes slow to arrive. Candidates are packs whose origin root is dead but
which are still referenced; their live set comes from the referencing roots'
chunk maps. A pack under 50% live has its live chunks copied into a new pack.
The `chunks` rows are updated, the affected roots' locator epoch is bumped
(locators are unsigned, so nothing is re-signed), and the old pack is condemned.
The 48 h hold outlives a locator's 24 h URLs.

**What it replaces in registry retention for converted images.** The registry
holds only their root and component manifests, a few KB each. `layer-*` tags,
component liveness and environment-driven Volume pressure eviction no longer
apply. Capacity becomes a budget question (C2.14) instead of a 70%/90% Volume
emergency, and `image_evicted` / `rebuild_required` cannot hit training images.

## 7. Migration of the existing corpus

> The executable plan, with the code changes the survey of 2026-10-03 found and measured
> production numbers (2.8 TB used on the Volume, not 3.81 TB), is
> [chunk-store-m2-plan.md](chunk-store-m2-plan.md).

**Inventory** (from the registry and the prepared catalogs; sizes from S10's
extrapolation):

| Wave | Family | Images | Today: OCI / EROFS, TB | Build input | Released, TB | New chunks, TB |
| --- | --- | ---: | --- | --- | ---: | ---: |
| 1 | All foundations and prepared sources (TMax, Terminal-Lego, OpenSWE) | ≈ 3,600 | 0.52 / 1.01 | yes: keep OCI | 1.01 | 0.10 |
| 2 | ScaleSWE | 2,469 | 0.59 / 0.90 | no | 1.49 | 0.05 |
| 3 | SWE-smith, R2E-Gym, SWE-Lego, rebench v2, MultiSWE | 391 | 0.33 / 0.46 | no | 0.79 | 0.14 |
| 4 | Other managed images (user builds) | small | — | no | all | small |

Foundations go first, so later waves dedupe against them. Build inputs (images
named by a prepared catalog or a pinned `FROM`) keep their OCI manifests
byte-identical, so digest pins keep working.

**Steps:**

0. **Readers first.** Ship a release whose workers and gateway read RAFS
   components and accept a dispatched root, with the flag off. This is the
   layout-2 pattern (`immutable-environments.md:110-117`).
1. **Convert and verify** with §3, rate-limited to keep production Volume reads
   under about 100 MB/s. About 1.4 TB of unique OCI layers is read once, at
   about 11 CPU-hours of conversion. Every image gets the full-tree check,
   because its originals will be deleted.
2. **Switch.** Add a gateway table `image_roots(manifest_digest → root, config
   digest)`, written per image, with a migration journal holding the old digest.
   Dispatch then carries the root. Retention (`registry_retention.py:274-286`)
   consults `image_roots` before the annotation, so the old root stops being
   live. Canary 20 sandboxes per family, running the family's first command
   (C9.2).
3. **Hold for 7 days.** Sandboxes still on old roots keep their route
   references, so those EROFS components survive until they end.
4. **Release.** For non-build-input images, delete the OCI manifest and its
   `ucloud-digest-*` protection tags; the digest survives only as an
   `image_roots` alias. Old roots and components lapse through online reference
   retention. Then run one blob sweep in a maintenance window (it stops the
   registry) and record Volume usage.
5. **The next wave starts only after the previous sweep,** and each wave adds at
   most 150 GB of new chunks.

**Storage timeline** (image data only, TB; snapshots, build cache and mirror
excluded):

| Point | Volume | S3 chunks | Total |
| --- | ---: | ---: | ---: |
| Today | 3.81 | 0 | 3.81 |
| Wave 1 converted → released and swept | 3.81 → 2.80 | 0.10 | 3.91 → 2.90 |
| Wave 2 converted → released | 2.80 → 1.31 | 0.15 | 2.95 → 1.46 |
| Wave 3 converted → released | 1.31 → **0.52** | 0.29 | 1.60 → **0.81** |

- The Volume never grows; it receives no chunk bytes.
- Total storage peaks at today's figure plus 0.10 TB (+2.6%), then falls 4.7×.
- C2.14 growth lands in S3, within its budget.

**Rollback.** Before release, revert the `image_roots` rows from the journal;
the old roots are intact. After release, regenerate an OCI image from the chunk
store (`nydus-image unpack` → tar → push) and rebuild today's EROFS root with
today's builder, a path tested on 10 images in M1. What is lost for good is a
Docker-worker rollback for non-build-input images; production workers already
run EROFS (`make_config.py:122-124`).

## 8. What it replaces and deletes

| Code at HEAD | Lines | When |
| --- | ---: | --- |
| `environment_builder.py`: allowlisted views (71–207), layer planning and squash (208–422), whole-image build and mkfs (441–538), layer groups and reuse (540–905) | ≈ 800 of 1,128 | after migration; a converter of about 250 lines replaces them |
| `environment_prepare.py`: subprocess layer-group preparation (the commit filter stays) | ≈ 200 of 347 | after migration |
| `environment_artifact.py`: layer components, group keys, `bind_source_layers`, the per-chunk layout | ≈ 180 of 735 | once the last layer-group root lapses |
| `erofs_metadata.py`, `environment_metadata.py` (C2.2 hints; the bootstrap replaces them) | 481 | after migration |
| `oci_flat_delta.py`, `flat_image_qualification.py` | 397 | with C2.10 |
| Scripts: `plan_flat_image_delta`, `score_shared_task_anchors`, `prepare_layered_task_pool`, `prepare_shared_task_image`/`_pool`, `plan_shared_task_pool` | ≈ 1,350 | after wave 3 |
| Retention: component liveness, `layer-*` tags, environment pressure eviction (`registry_retention.py`, `registry_disk.py`) | ≈ 150 (estimate) | after migration |
| `environment_cache.py`: component-index reads and the `whole_image` branches | ≈ 100, rewritten | M1 |

**Net:** about 3.6k lines deleted against about 1.8k added: the converter (250),
pack and chunk-map formats (250), the index service (450), the RAFS device and
fetch planner (350), GC (250) and the migration tool (250). **Kept:** foundations
as BuildKit bases, the backend process and its trust configuration, toolkit
overlays, and C3.1 commit, where a commit tar converts with `-t tar-rafs` as one
more layer.

## 9. Build plan

| Milestone | Contents | Gate | Effort |
| --- | --- | --- | --- |
| **S12** (first) | See below | Decides Phase A or B, and the window sizes | 3–4 days |
| **M1: store core** (behind `immutable_environments.chunk_store`) | pack and chunk-map formats; `ucloud-chunk-index`; the converter; the RAFS component and root; the worker RAFS device with presigned S3 reads; concurrent attach; chunk-id traces; the `unpack` rollback tool | On S10's 181-image sample: <br>• 181/181 full-tree equal; <br>• stored bytes within 5% of 17.5 GB; <br>• cold first commands within 1.3× of S10, read from S3 with traces; <br>• 20-way cold burst ≤ 5.5 s (S11); <br>• crash injection at every §3 step leaves no visible partial image | 2.5–3 weeks |
| **M2: migration** | waves 1–3, the `image_roots` switch, release and sweeps | Per wave: <br>• 100% verified; <br>• canaries pass; <br>• Volume drops by the predicted release ±10%; <br>• zero `environment_io` corruptions | 1.5 weeks of work, about 4 calendar weeks with holds |
| **M3: GC and corpus** | mark, condemn, delete; the `training-corpus` owner; the per-owner budget report; then the C2.14 campaign | GC dry run marks 100% of corpus roots live; deleting a test image frees its unique packs; a build racing GC either registers or retries, and never dangles | 1 week |
| **M4: C2.6 and compaction** | the store node, built ahead of M1's gates after S12 (§2, C2.6 store node); compaction once dead bytes exceed 20%; the §8 deletions | C2.6 gates; the deletion ledger | 1–2 weeks |

**Total:** about 6–8 engineer-weeks.

**M1 gate run** (`scripts/chunk_store_gate.py`, host side in
`scripts/chunk_store_gate_remote.py`). After S12, cold commands read through the
store node, not from S3 directly; the run also checks the 10-image `unpack`
rollback from §7. Every production action is one plain `scripts/hetzner_prod/gw '…'`,
`gscp` or `hz.py` command (`--dry-run` prints them all). The phases are
`provision`, `configure`, `convert`, `crash`, `workers`, `rollback`, `report` and
`teardown`. Each phase records its finished steps in
`build/m1-gate/<run>/state.json`, and every server, staging directory,
`known_hosts` entry and the S3 prefix is recorded before it exists, so
`--phase teardown` cleans up after a crash at any point.

- **Isolation.** The sample is copied once, read-only, from production into a
  gate registry on the store node. Conversion, attach tags, `unpack` and
  today's builder write only to that registry. The run signs with its own
  producer key, writes S3 only under `spike/m1/<run>`, and gives the
  `chunk_store` block only to config copies; the live gateway config, registry
  and services are never written.
- **Canary workers** run VM init from the gateway as `ucloud`, with this
  release's CLI, the bundle given by `--bundle` and a canary config copy. That
  copy has the source-config disk overrides, the merged producer trust and the
  registry alias pointed at the gate registry. They heartbeat to the
  production gateway, so a production create placed on one would fail, and the
  phase runs only with `--accept-canary-placement`. The bench drives each
  canary's node agent directly; workers are drained before anything deletes
  them.
- **Crash injection** kills the converter with SIGKILL from its step hook, on
  fresh images (unique top layer) held back from `convert`. It checks that no
  attach tag and no unloadable root became visible, then converts twice and
  requires the same root with no new layers or packs.
- **Store node adapter.** The store service's interface is `--store-url`,
  `--store-read-token-file`, `--store-write-token-file`,
  `--store-start-command` and the `chunk_store` block (`--chunk-store-block`).
  The default starts today's `serve-chunk-index`.
- **Cost.** About 4–6 hours of wall time and about 13 VM-hours (CCX43 store
  node, CCX63 converter, two CCX43 workers for about an hour), roughly EUR 3–4
  at list price.

**Riskiest unknowns:**

1. **S3 demand latency and per-bucket request limits** under a 500-sandbox
   burst, which decide whether C2.6 must precede W9.
2. **Pack locality:** ranges per cold start when shared chunks sit in many
   foundation packs, and the bytes 1 MiB windows waste.
3. **Mapping correctness:** S10's dictionary images verified every chunk while
   files were wrong. The full-tree gate guards it, at an unmeasured cost across
   6.5k images.
4. **Python CPU for zstd plus sha256 under burst** (S11: 9.8 s of backend CPU per
   20-sandbox burst), which couples to C2.1.
5. **Page-cache sharing lost** across images on one foundation (§4), and the
   pinned Nydus internals (v2.4.5, the chunk table, `--original-blob-ids`).

**S12, the first spike.** One disposable CCX63, read-only against production,
writing only to a `spike/` bucket prefix:

- **(a) Corpus:** reconvert S10's 181 images in its shuffled order through a
  prototype packer, 16 in parallel. Report packs, bytes, the duplicate ratio and
  the per-image pack fan-out.
- **(b) S3:** direct ranged GET TTFB and throughput for 64 KiB, 1 MiB and 4 MiB,
  at concurrency 1–256, from 1 worker and then 3, up to the request-rate ceiling.
- **(c) Workloads:** cold `import sys`, `git status` and `pip --version` on
  S10's 7 images, with its NBD harness reading from S3, demand-only, with 1 MiB
  read-around, and with trace replay. Record requests, bytes and amplification.
- **(d) Index:** a synthetic 500M-row SQLite on NVMe: size, a 30k-id batch
  lookup, and the insert rate.

**Pass:** trace-replayed cold commands within 1.3× of S10's loopback numbers;
1 MiB GET p99 ≤ 150 ms; index ≤ 45 GB with batch lookup ≤ 1 s. Otherwise M1
starts with Phase B's store node.

## Decisions (2026-10-02)

> **S13 result** (`docs/benchmarks/fanotify-spike-2026-10-02/`).
> - **Mount granularity for M1: per-image merged mounts.** Page cache per extra
>   distinct image is 21.0 MB for per-image mounts, 12.3 MB for per-layer and
>   3.1 MB with `inode_share` (with `-o directio`; about double without it).
> - **`inode_share` comes later.** It needs an out-of-tree `erofs.ko`, and per-file
>   fingerprints that only `mkfs.erofs` 1.9 can write (`--xattr-inode-digest`),
>   not nydus-image.
> - **NBD stays in M1.** fanotify pre-content hooks with file-backed EROFS work and
>   are correct, but they are not adopted until C2.1's native daemon:
>   - per-blob backing files matched or beat NBD at 20-way (5.6–6.2 s against
>     7.1–7.2 s), but the Python prototype lost at 100-way (37 s against 27.6 s);
>   - **the kernel fails open:** when the listener dies, pending and new reads
>     return zeros, where NBD returns EIO in 45 ms.
>
>   C2.1 must:
>   - use per-blob files holding each layer's complete chunk table;
>   - deny (EIO) any range it cannot place;
>   - keep the group and write fds in systemd's fd store, journal events and
>     take over on restart, and retire the node's mounts if the group is lost;
>   - unmark a blob once it is complete;
>   - meet or beat NBD on the 100-way burst.
> - **Disk sharing:** reflink from a shared chunk cache cuts disk per extra image
>   to a median of 29 MB (chunks padded to 4 KiB).
> - **Fixed in M1:** nydus-image `--repeatable` zeroed every file owner; the
>   converter no longer passes it.

> **S12 result** (`docs/benchmarks/s3-chunk-spike-2026-10-02/`): Phase B from the start.
> - **S3 tail latency fails the gate.** 1 MiB GETs at concurrency 32 had a p99 of
>   5.5 s (the gate is 150 ms), with multi-second stalls even one request at a
>   time. 4 of 9 trace-replayed cold commands were 1.4–29× slower than local.
> - **NAT puts the gateway back in the byte path.** Workers are private-only and
>   reach S3 through the gateway's NAT, and the bucket has no private endpoint.
> - **So:** S3 stays the durable store. Workers read from a store node on the
>   private network: NVMe, a read-through cache over S3, filled ahead by trace
>   and foundation prefetch.
> - **Packs:** 17.53 GB for the 181 images (within 0.2% of S10). An image touches a
>   median of 39 packs, with 90% of its bytes in 9.
> - **Index:** 31.3 GB at 500M rows. Lookups meet the gate only while resident
>   (0.51 s against 10–13 s cold), so it cannot stay on the 16 GB gateway at full
>   scale. Move it to the store node.
> - **Page-cache sharing (`inode_share`) is feasible:** `erofs.ko` builds with it,
>   and `mkfs.erofs` 1.9 can write the per-file fingerprints
>   (`--xattr-inode-digest`), but nydus-image v2.4.5 cannot. Not yet measured.

1. **Storage:** S3 (Hetzner Object Storage) from day one, provided S12 passes its
   performance gate. If it fails, M1 starts with Phase B's store node in front of
   S3.
2. **Worker access to S3:** presigned URLs from the index service. Workers hold no S3 key.
3. **OCI copies of non-build-input images** are deleted when their wave is released.
4. **Storage limit:** S3 is acceptable for the corpus, OpenSWE included. The
   registry Volume must still shrink.
5. **Index service** runs on the gateway until C2.6.
6. **No calendar holds in the migration.** A wave is released as soon as all of
   these hold:
   - every image in it is verified (full-tree equal);
   - its canaries pass;
   - no live route or registration references an old root or component (the
     existing retention references).

   GC keeps its condemn → delete delay, which must outlive the longest build and
   the presigned-URL lifetime. With 24 h URLs that is the 48 h in §6; shorter URLs
   allow a shorter delay.
7. **S12 also measures page-cache sharing.** It runs per-image RAFS mounts against
   per-layer mounts stacked as today, on one node with many distinct images on few
   foundations, so M1 can choose the mount granularity.

## Open decisions (superseded by the decisions above)

1. **S3 from day one** (recommended), or the registry Volume first and a second
   migration later.
2. **Worker access:** presigned URLs from the index service (recommended), or an
   S3 key on workers.
3. **Delete the OCI copies of non-build-input images** after the 7-day hold
   (recommended; `unpack` can regenerate them), or keep them for Docker-worker
   rollback at about 0.9 TB.
4. **Whether "not many TB more" also binds S3.** OpenSWE adds 7–9 TB (C2.14),
   which is affordable in S3 but not on the Volume.
5. **Index host:** the gateway until C2.6 (recommended), or a dedicated VM now.
