# Nydus RAFS v6 spike (S10, plan item C2.13), 2026-10-02

The question: should we adopt Nydus RAFS v6 (v2.4.5) as our image format and keep
our NBD block serving, signed roots and chunk cache? The spike measured conversion
and dedupe on 181 training images and 18 locally built OpenSWE task images. It
also measured kernel mounts and first commands under runsc against today's EROFS
components. Plan context: [`rl-scale-architecture-plan.md`](../../rl-scale-architecture-plan.md)
C2.13–C2.15 and [the prepared-image-cache review](../../reviews/prepared-image-cache-2026-10-02.md).

## Decision

**Adopt RAFS v6 with the kernel EROFS path over our NBD backend. Do not adopt
Nydus's chunk dictionary, and do not adopt `nydusd`.**

- **Format and mount work as planned.** Kernel 7.0 EROFS mounts a RAFS v6
  bootstrap directly, with no fscache and no `nydusd`. Three ways work:
  - one NBD device that exposes the image's unified address space (the
    bootstrap, then each blob at its `mapped_blkaddr`);
  - one device per blob with `-o device=`;
  - `nydus-image export --block` over a loop device or as a file.

  Attach took 0.18–0.24 s with one device, against 0.23–0.79 s for today's 2–8
  component devices. A cold `python3 -c 'import sys'` took 0.36–0.56 s against
  0.47–0.64 s, and read 8–12 MB against 31–56 MB. Content matched today's
  EROFS. Unlike today's layout-1 components, file mtimes survive, so `.pyc`
  caches stay valid.
- **Take dedupe from our chunk store, not from the dictionary.**
  - **The dictionary does not work at our scale.** A RAFS v6 bootstrap,
    dictionary included, holds at most 254 blobs (an 8-bit blob index). Our
    incremental dictionary panicked at image 158 of 181.
  - **It corrupted images.** Dictionaries grown with `nydus-image merge`
    produced images that Nydus's own exporter cannot read: 7 of 9 that we
    checked. One image mounted with 2,076 of 42,138 files holding the wrong
    bytes.
  - **Our own chunk store does better.** Deduplicating by chunk digest (sha256
    of the uncompressed chunk, from the bootstrap's chunk table) stored the
    sample in **17.4 GB**. The comparisons: 27.4 GB with the (corrupt) stock
    dictionary, 55.2 GB with Nydus and no dictionary, and **149.8 GB today**
    (OCI plus EROFS).
- **Use 256 KiB chunks**, our cache unit. Stored bytes rose 0.5% and bootstraps
  rose 3% against 1 MiB chunks.
- **C2.13 needs one amendment.** Replace "the chunk dictionary covers every image
  converted so far" with: convert each layer with no dictionary, and store each
  chunk once in our own content-addressed store. The `nydusd` fallback is not
  needed.

## Setup

- **VM:** one `sandboxes-spike-nydus` server, cpx62 (16 vCPU, 30 GiB, 601 GB
  disk), from the 0.8.1 worker snapshot `438710747`. It ran kernel
  7.0.0-30-generic, Python 3.14.4, `/usr/local/libexec/ucloud-gvisor/runsc`,
  Docker 29.8.1 and erofs-utils 1.9. VM init never ran, so the VM never
  registered as a worker.
- **Runtime:** created 11:38:30Z and deleted 13:47:27Z, so **2 h 09 min**. The
  Hetzner API confirmed that no server of that name remained.
- **Tools:** Nydus v2.4.5 static release (`nydus-static-v2.4.5-linux-amd64.tgz`,
  sha256 `1ad7b793…19072`, which matches the release checksum). A VM-local
  registry (distribution v3.1.2 on 127.0.0.1:5001) served as the `nydusify`
  target and held the OpenSWE builds. All of these came from GitHub releases;
  there were no Docker Hub pulls.
- **Production access was read-only.** The VM read manifests, layers,
  environment roots and component blobs from 10.42.0.2:5000 (the `environments`
  repository holds the components). It wrote nothing to the production registry,
  gateway, `images.sqlite` or any service. The gateway's `known_hosts` entry and
  staging directory were removed afterwards.

### Sample (from `all-cached-training-tasks-with-terminal-lego-2026-10-01.zip`)

[`raw/sample.json`](raw/sample.json) lists the 181 images (seed 20261002).

| Group | Images | Notes |
|---|---:|---|
| ScaleSWE, repositories with ≥3 images | 30 | 3 images each from 10 repositories |
| ScaleSWE, random | 30 | from the other repositories |
| SWE-smith, R2E-Gym, SWE-Lego, SWE-rebench v2 | 30 | 10, 8, 7 and 5. These have no remaining build work (the selection has no literal `complete` kind) |
| TMax | 40 | distinct `prepared_reference` values (foundations and sources) |
| Terminal-Lego | 40 | distinct `prepared_reference` values |
| OpenSWE foundations | 11 | every foundation that OpenSWE `foundation` rows use. The "12th" `foundation_key` is null, on the 110 `source` rows |

- **Size:** 799 unique blobs. Manifest layers total 86.4 GB summed and 58.6 GB
  unique. Fetching them took 189 s.
- **OpenSWE task images:**
  - **Source of recipes:** the gateway's `/work/ucloud-sandboxes/openswe*`
    directories hold only foundation Dockerfiles and plans. The rewritten task
    recipes came instead from the local copy of the coverage index,
    `build/openswe-foundations-expanded-20260930/coverage.sqlite`. Its `FROM`
    lines are pinned by digest to the registry foundations.
  - **Builds:** the Docker legacy builder ran 4 and then 3 builds in parallel,
    fetching from GitHub, PyPI, conda and Debian mirrors.
  - **Outcome:** 33 attempts produced 18 images, 9 pandas-dev/pandas and 9 from
    other projects ([`raw/openswe-builds.jsonl`](raw/openswe-builds.jsonl)).
  - **Failures:** 9 of 18 pandas recipes failed against today's upstream,
    either in dependency resolution or in their own import checks. That
    supports the C2.14 lockfiles. 5 GitHub clones failed under parallel load and
    succeeded on retry. One other recipe failed outright.

## Method

**Conversion** ([`scripts/convert.py`](scripts/convert.py)):

- **Per layer:** each OCI layer is converted on its own, as `nydusify` does:
  `nydus-image create -t targz-rafs --fs-version 6 --digester sha256
  --compressor zstd --chunk-size 0x100000|0x40000 --repeatable`.
- **Per image:** `nydus-image merge` combines the image's layers. It needs
  `--original-blob-ids`; without it, merge names each blob after its bootstrap
  file name.
- **No dictionary:** unique layers convert in parallel, 10 at a time.
- **Stock dictionary:** images convert one at a time in a fixed shuffled order
  across families (seed 13). Each layer is created with
  `--chunk-dict bootstrap=D`, and each image is merged with the same `D`. Then
  `D ← merge --chunk-dict D [D, the image's layers]`, so the dictionary covers
  every image converted so far.
  - This is the only incremental mechanism the v2.4.5 tools offer.
    `--chunk-dict` takes a single bootstrap. `merge` refuses multi-blob sources
    unless their blobs belong to the dictionary.
  - The `chunkdict generate` tooling, in `nydus-image` and `nydusify`, keeps
    only chunks that are frequent across versions, selected by exponential
    smoothing and DBSCAN. It references the original blobs, so it neither
    covers every chunk nor repacks blobs.
- **Chunk-store accounting** ([`scripts/analyze.py`](scripts/analyze.py)): each
  image's final chunk table is read from its bootstrap with
  [`scripts/rafs.py`](scripts/rafs.py). The superblock extension at byte 1152
  gives the chunk table, made of 80-byte `RafsV5ChunkInfo` entries. A chunk
  costs its compressed size the first time its sha256 is seen. Chunks of
  shadowed files are never stored.
- **Today's bytes:** unique OCI layer blobs plus unique EROFS component blobs,
  found from each image's `org.ucloud.immutable-environment.v1` root.
  Today's components are lz4-compressed EROFS (`lz4_0padding`).

**Mount and first command** ([`scripts/mounttest.py`](scripts/mounttest.py) and
[`scripts/spike_nbd.py`](scripts/spike_nbd.py)):

- **Backend:** a Python NBD backend with the same ioctl plumbing as
  `environment_nbd.py`.
  - **Nydus mode:** it serves the unified address space. A read maps to chunks
    through the chunk table. Each chunk's compressed bytes are read from the
    local zstd blob, decompressed and checked against the chunk's sha256, then
    cached.
  - **Today's mode:** it serves each EROFS component through 256 KiB windows
    checked against a precomputed sha256 index. That stands in for the signed
    index, and it is built outside the timed attach.
- **Composition:** the lower is either the single Nydus mount or today's
  overlay of components. A read-write OverlayFS upper sits on top. A minimal
  `runsc --platform=systrap --network=none run` bundle uses the image config's
  `Env` and `WorkingDir`.
- **Each cycle:** `drop_caches`, then fresh backends, then attach. The first
  command runs cold, then again warm. After `import sys`, two more runs import
  `json, email.mime.text, http.client, asyncio, decimal`. A separate cycle runs
  `git status` in `/testbed` cold.
- **Repeats:** 2 per path. The tables give medians.
- **Content checks:**
  - `size`, `mode` and sha256 of 300 sampled files, comparing Nydus with
    today's tree (up to 20,000 files per image);
  - mtimes of 4,000 random final-tree paths against the OCI tar headers;
  - every timestamp `.pyc` header against its source's `stat()`.

## Results

### Stored bytes, sample of 181 images

All figures are in GB and count each unique blob or chunk once
([`raw/storage-1m.json`](raw/storage-1m.json), [`raw/storage-256k.json`](raw/storage-256k.json)).

| Family | Images | OCI unique | EROFS unique | **Today: OCI + EROFS** | Nydus, no dict | Nydus, stock dict ¹ | **Nydus + our chunk store** | of which bootstraps |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ScaleSWE | 60 | 16.81 | 25.88 | **42.68** | 16.48 | 5.09 | **2.26** | 0.49 |
| SWE-smith, R2E, SWE-Lego, rebench | 30 | 24.05 | 33.55 | **57.60** | 20.82 | 14.32 | **10.84** | 0.43 |
| TMax | 40 | 9.81 | 17.67 | **27.49** | 10.03 | 3.32 | **1.17** | 0.14 |
| Terminal-Lego | 40 | 3.46 | 7.35 | **10.81** | 3.42 | 1.91 | **1.39** | 0.08 |
| OpenSWE foundations | 11 | 4.49 | 6.78 | **11.27** | 4.43 | 2.74 | **1.75** | 0.08 |
| **Total, 1 MiB chunks** | 181 | 58.61 | 91.23 | **149.84** | 55.18 | 27.37 | **17.41** | 1.21 |
| Total, 256 KiB chunks | 181 | | | 149.84 | 55.80 | 27.22 | **17.49** | 1.25 |

¹ The stock-dictionary images do not read back correctly (see
[Chunk dictionary](#chunk-dictionary-what-nydus-v245-offers-and-why-we-should-not-use-it)),
and the dictionary froze at image 158. Treat this column as indicative only.

- The registry stores each image today in OCI form and again as EROFS
  components (91.2 GB), about 2.6× the OCI bytes.
- **Nydus without a dictionary** is about the OCI size, because zstd compresses
  much as gzip does. The real saving is the second copy.
- **Our chunk store** holds 8.6× less than today. Most of the saving is whole
  files repeated across images: chunks never span files, and 256 KiB chunks
  saved no more than 1 MiB chunks.
- **ScaleSWE dedupes very well.** The repositories with several images added a
  median 0.7 MB of new chunks per image, and the random images a median 12.9 MB.

**Per image** (medians; marginals are means over the later half of the
shuffled order, so the corpus already holds about half the sample):

| Family | OCI MB | EROFS MB | Bootstrap MB | Chunks (1 MiB / 256 KiB) | Marginal today MB | Marginal no-dict MB | **Marginal chunk store MB** (+ bootstrap) | Convert wall s / CPU s (no dict) | Convert wall s (dict) | Blobs per image (dict) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ScaleSWE | 496 | 777 | 8.0 | 33.0k | 605 | 225 | **11.5** (+8.0) | 13.4 / 13.2 | 10.6 | 30 |
| SWE-smith, R2E, Lego, rebench | 1,103 | 1,639 | 11.2 | 37.8k | 2,013 | 695 | **331** (+17.3) | 19.5 / 19.0 | 29.3 | 61 |
| TMax | 231 | 375 | 2.9 | 12.0k | 674 | 242 | **11.7** (+3.2) | 5.8 / 5.7 | 12.3 | 29 |
| Terminal-Lego | 82 | 136 | 1.4 | 5.8k | 294 | 89 | **32.2** (+2.2) | 1.2 / 1.1 | 9.5 | 17 |
| OpenSWE foundations | 766 | 1,185 | 7.4 | 26.9k | 875 | 341 | **66.4** (+7.3) | 8.9 / 8.7 | 15.3 | 22 |

Across all 181 images, the median image has 26.7k chunks at 1 MiB and 31.6k at
256 KiB.

**OpenSWE task images built on the VM**
([`raw/openswe-tasks.json`](raw/openswe-tasks.json)), as marginal bytes on top of
the 181-image corpus:

| Group | n | OCI MB (new) | New layers, uncompressed MB | Nydus no-dict new MB | **Chunk store new MB** mean (median) | Bootstrap MB | Convert s | Build s (median) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| pandas, first image | 1 | 652 | 1,132 | 618 | **613** | 9.3 | 16 | 517 |
| pandas, later commits | 8 | 799 | 1,645 | 791 | **276** (244) | 10.2 | 16 | 238 |
| other projects, first of each | 9 | 275 | 588 | 274 | **188** (100) | 9.9 | 7.8 | 45 |

- Commits of one project share far less than ScaleSWE images do. Each pandas
  build compiles C extensions into an editable tree, and its unpinned
  dependencies resolve to different versions.
- Mounted, every OpenSWE task image runs `python3` at the same speed as the
  rest. A cold `git status` on the pandas and OasisLMF trees took 2–11 s.

**Conversion cost.**

- **No dictionary:** 1,601 CPU-s for 58.6 GB of unique gzip layers, or
  27 CPU-s per GB. That is 151 s of wall time with 10 parallel conversions. The
  256 KiB run took 1,823 CPU-s.
- **Stock dictionary:** 2,736 CPU-s, of which 1,361 s were dictionary merges.
  One merge grew to 17 s per image at 158 images and a 140 MB dictionary.
- **`nydusify convert` end to end** (pull from 10.42.0.2, convert, push to the
  VM registry, see [`raw/nydusify.jsonl`](raw/nydusify.jsonl)):
  - Terminal-Lego, 50 MB and 7 layers: 2.9 s wall, 3.0 CPU-s;
  - ScaleSWE, 500 MB: 24.9 s wall, 32.6 CPU-s.

  The output is one Nydus blob per layer plus a gzip bootstrap layer (1.3 MB
  compressed to 0.54 MB). `nydusify` copies the source manifest's annotations,
  including our `org.ucloud.immutable-environment.v1`. A converter must strip
  or replace them.

### Chunk dictionary: what Nydus v2.4.5 offers, and why we should not use it

1. **There is a cap of 254 blobs per bootstrap**, from the 8-bit
   `RafsV6InodeChunkAddr` blob index (`builder/src/core/v6.rs:653` asserts
   `blob_table_entries < u8::MAX`). Every converted layer adds a blob. The
   incremental dictionary reached 246 blobs, and `merge` then panicked at
   image 158 of 181. The last 23 images deduplicated only against a frozen
   dictionary. One merged dictionary for 66,786 images, or even for 255
   layers, cannot exist. Repacking chunks into fewer blobs would avoid this, and
   the v2.4.5 tools do not repack.
2. **Merging is an overlay.** When a later image writes the same path, chunks of
   the shadowed file leave the dictionary's chunk table. Blobs whose chunks have
   all been shadowed disappear. 205 cached layer bootstraps referenced such
   blobs and had to be rebuilt against the current dictionary.
3. **The merged dictionary records truncated blob sizes**
   ([`raw/dict-integrity.json`](raw/dict-integrity.json)).
   - In the final `dict.boot`, blob `a32d8fd3…` has `blocks=2230` (9.1 MB),
     while its chunk table reaches the uncompressed offset 52.8 MB. The real
     blob has 12,946 blocks.
   - Images built against such a dictionary have device regions that overlap
     in the unified address space.
   - `nydus-image export --block` failed with `EINVAL` (`cachedfile.rs:934`)
     on 7 of 9 dictionary images checked (positions 4–176). Positions 1 and 80
     exported and matched the no-dictionary conversion.
   - Through our flat device, image 47 had 2,076 of 42,138 files whose bytes
     differed from both the no-dictionary conversion and today's EROFS
     ([`raw/hashdiff-047.json`](raw/hashdiff-047.json)). Every chunk still
     passed its sha256 check. The chunk table and the inode chunk addresses
     simply no longer agree.
   - We did not trace the bug inside Nydus. Using `merge` to grow a dictionary
     may simply be unsupported.
4. **The cost grows with the corpus.** Merging the dictionary is linear in its
   size: 17 s per image after 158 images. Dictionary images also reference many
   blobs, 17–61 per family (median) and up to 87.

None of this applies to a chunk store of our own. The bootstrap's chunk table
already gives each chunk's sha256, compressed and uncompressed size, and
offsets. Conversion needs no state across images and parallelizes completely.

### Mount and first command (cold page cache, fresh backend, runsc)

Medians of 2 repeats ([`raw/mount-*.json`](raw/)). The "MB read" column counts
compressed chunk bytes for Nydus and 256 KiB EROFS windows for today, during the
cold `import sys` cycle.

| Image | Variant | Path | Devices | Attach s | `import sys` cold / warm s | stdlib imports s (then again) | `git status` cold s | MB read |
|---|---|---|---:|---:|---:|---:|---:|---:|
| ScaleSWE evergreen-ci_evergreen.py_pr102 | 1 MiB | **nydus** | 1 | 0.22 | 0.44 / 0.16 | 0.50 (0.33) | 0.24 | 9.3 |
| | | today | 2 | 0.26 | 0.57 / 0.23 | 0.84 (0.64) | 0.26 | 35.5 |
| SWE-Lego reata_1776_sqllineage-107 | 1 MiB | **nydus** | 1 | 0.23 | 0.43 / 0.16 | 0.59 (0.31) | 0.31 | 9.5 |
| | | today | 7 | 0.72 | 0.59 / 0.22 | 0.95 (0.60) | 0.36 | 52.5 |
| SWE-smith oauthlib_1776_oauthlib | 1 MiB | **nydus** | 1 | 0.24 | 0.41 / 0.16 | 0.55 (0.33) | 0.52 | 11.6 |
| | | today | 8 | 0.79 | 0.53 / 0.19 | 0.82 (0.63) | 0.53 | 55.8 |
| R2E-Gym aiohttp_final:2834… | 1 MiB | **nydus** | 1 | 0.22 | 0.50 / 0.17 | 0.45 (0.38) | 0.52 | 10.8 |
| | | today | 7 | 0.72 | 0.56 / 0.20 | 0.74 (0.59) | 0.49 | 43.3 |
| TMax task_003753_0602e9c3 | 1 MiB | **nydus** | 1 | 0.20 | 0.36 / 0.13 | 0.47 (0.29) | 0.26 | 8.7 |
| | | today | 2 | 0.23 | 0.52 / 0.20 | 0.85 (0.63) | 0.27 | 35.8 |
| Terminal-Lego 004302 | 1 MiB | **nydus** | 1 | 0.18 | 0.40 / 0.16 | 0.61 (0.43) | 0.26 | 8.3 |
| | | today | 2 | 0.26 | 0.47 / 0.15 | 0.93 (0.72) | 0.26 | 31.4 |
| OpenSWE foundation (webpty-12) | 1 MiB | **nydus** | 1 | 0.20 | 0.40 / 0.17 | 0.58 (0.32) | 0.26 | 11.3 |
| | | today | 5 | 0.50 | 0.47 / 0.20 | 0.86 (0.62) | 0.28 | 44.1 |
| ScaleSWE evergreen-ci_evergreen.py_pr102 | 256 KiB | **nydus** | 1 | 0.22 | 0.46 / 0.18 | 0.61 (0.34) | 0.25 | 8.8 |
| SWE-smith oauthlib_1776_oauthlib | 256 KiB | **nydus** | 1 | 0.22 | 0.41 / 0.15 | 0.55 (0.32) | 0.50 | 10.2 |
| ScaleSWE canonical_operator_pr1017 (58 blobs) | stock dict ¹ | nydus | 1 | 0.24 | 0.52 / 0.21 | 0.63 (0.36) | 0.26 | 9.3 |
| R2E-Gym aiohttp_final:2c7f… (81 blobs) | stock dict ¹ | nydus | 1 | 0.21 | 0.54 / 0.18 | 0.53 (0.39) | 0.45 | 9.4 |
| OpenSWE task pandas-20422 | 1 MiB | nydus | 1 | 0.19 | 0.43 / 0.17 | 0.64 (0.32) | 2.12 | 9.1 |
| OpenSWE task pandas-54945 | 1 MiB | nydus | 1 | 0.24 | 0.56 / 0.20 | 0.50 (0.30) | 3.41 | 9.5 |
| OpenSWE task OasisLMF-1406 | 1 MiB | nydus | 1 | 0.24 | 0.50 / 0.18 | 0.53 (0.31) | 11.20 | 10.6 |

¹ The dictionary images mount and run, but some of their files hold wrong bytes
(see item 3 above).

- **Attach.** Of the time, 0.12–0.2 s is the Python spike backend: process
  start, and parsing the bootstrap or the index. The EROFS mount itself took
  about 10 ms. Nydus stays at one device whatever the image's layer and blob
  count. Today's path adds a device and an NBD export per component.
- **First commands.** These are 10–25% faster on Nydus, and read about 4× fewer
  bytes. Nydus fetches compressed chunks, while today reads 256 KiB windows of
  lz4 EROFS. `git status` is the same on both paths.
- **Repeated Python imports.** The second stdlib import run takes 0.29–0.43 s
  on Nydus and 0.59–0.72 s today. runsc's default root overlay kept writes
  inside the sandbox: no `.pyc` reached the host upper. So each new run starts
  with the image's own `.pyc` files. Today's
  `.pyc` files are all stale, so CPython recompiles on every run. See
  [Feasibility](#feasibility).
- **Other kernel paths.** For a 5-blob Terminal-Lego image
  ([`scripts/devmode.sh`](scripts/devmode.sh)):
  - `-o device=/dev/nbdX`, one per blob, plus the bootstrap device, mounted in
    11 ms, and 200 files read back correctly;
  - `nydus-image export --block` took 0.77 s and wrote a 150 MB raw image
    (from 50 MB of OCI). It mounted through a loop device and also file-backed
    (`CONFIG_EROFS_FS_BACKED_BY_FILE=y`).
- **The kernel needs uncompressed data on its devices.** Chunk-based EROFS
  inodes address raw blocks, and Nydus blobs are zstd per chunk. The backend
  must decompress, as the spike's did, or blobs must be built with
  `--compressor none`. The kernel has `CONFIG_EROFS_FS_ONDEMAND` off, so
  fscache is not an option.
- **`nydusd` was not needed**, so it was not measured.

**Content.** For the 7 no-dictionary images compared with today's EROFS, there
were 0 missing files and 0 size, mode or sampled-sha256 differences, over up to
20,000 files per image.

## Feasibility

**Signing surface.** The signed root would hold:

- the image config digest;
- the **bootstrap's sha256 and size**. The bootstrap authenticates everything
  the kernel and backend use:
  - the inode tree and each inode's chunk addresses (device and block);
  - the device table, with blob ids, sizes and `mapped_blkaddr`;
  - the chunk table, with each chunk's sha256 over its uncompressed bytes and
    its compressed and uncompressed offsets and sizes;
- the digests of the registry objects that hold the chunks (Nydus blobs, or our
  pack files), to bind them to registry GC;
- the chunk size and digester. Use `--digester sha256`, because the default is
  blake3.

Workers verify the bootstrap before exposing a device, then verify every chunk
after decompression, as the spike backend did. Compressed bytes never need to
be trusted.

**Chunk cache.**

- **The cache can serve Nydus ranges.** At `--chunk-size 0x40000` a Nydus chunk
  is at most 256 KiB, the same as our cache unit. The cache key becomes the
  chunk's content digest instead of a window of one component, so chunks hit
  across images.
- **Locating a chunk** takes no extra metadata. A device offset gives the blob
  and its uncompressed offset; the chunk table gives the digest; a pack index
  gives the compressed bytes.
- **Exposure:** the 80-byte chunk table in the bootstrap, which
  [`scripts/rafs.py`](scripts/rafs.py) parses in about 40 lines, and the
  blob's `blob.meta` ToC.
- **Chunks are small.** Chunks never span files, so most are small files. The
  mean compressed chunk is 19 KB, and the median image has 31.6k chunks. Fetch
  contiguous runs of chunks as one range request.
- **Prefetch hints and traces** carry over at chunk granularity, as offsets or
  digests.

**Layout 2, mtimes and `.pyc`.**

- **Conversion from a tar keeps mtimes.** It builds extended inodes
  (`Node::new` sets `v6_compact_inode: false`). File, directory and symlink
  mtimes matched the OCI tar headers for 4,000 of 4,000 sampled paths, on all
  10 images checked.
- **`.pyc` caches stay fresh.** 0 of 651–4,000 timestamp `.pyc` files per image
  were stale. On today's layout-1 components, every mtime is 0 and every `.pyc`
  is stale.
- **Never convert from a directory.** `dir-rafs` input builds compact inodes,
  which drop mtimes for everything except `.pyc` (`v6_set_inode_compact`).
- **So RAFS v6 conversion delivers layout 2's mtime goal.** The layout-2
  republish of the corpus and the C2.14 precompute become a single pass.

**What blocks C2.13 as written.** Only the dictionary (items 1–4 above).
Nothing blocks the format, the kernel mount path, signing or the chunk cache.

## Extrapolation to the full selection (66,786 images)

The table assumes that every image is precomputed (C2.14) and that per-image
marginals stay at the sample's later-half values. All figures are TB.

| Family | Images | Basis | Today: OCI + EROFS | Nydus, no dict | **Nydus + our chunk store** |
|---|---:|---|---:|---:|---:|
| ScaleSWE | 2,469 | marginals measured on real images | 1.49 | 0.57 | **0.05** |
| SWE-smith, R2E, Lego, rebench, MultiSWE | 391 | measured (MultiSWE assumed the same) | 0.79 | 0.28 | **0.14** |
| OpenSWE, scenario A | 35,549 | 10,326 projects × "other project" mean + 25,223 later commits × pandas later-commit mean | 53.4 | 23.1 | **9.3** |
| OpenSWE, scenario B | 35,549 | all at the "other project" mean | 23.2 | 10.1 | **7.0** |
| TMax foundations and sources | 1,241 refs | measured at foundation level | 0.84 | 0.30 | **0.02** |
| Terminal-Lego foundations and sources | 2,354 refs | measured at foundation level | 0.69 | 0.21 | **0.08** |
| **Total, A / B** (TMax and Terminal-Lego task deltas excluded) | 66,786 | | **57.2 / 27.0** | **24.5 / 11.4** | **9.6 / 7.3** |
| plus TMax and Terminal-Lego task deltas | 28,377 tasks | per 100 MB of unique compressed delta per task | ≈ +9 (3.3× chunk store) | | **+2.8** |

Assumptions:

- **Today** means precomputing in today's format: OCI layers plus lz4 EROFS at
  the sample's ratio of 0.64 × uncompressed.
- **OpenSWE is uncertain.** The "other project" mean (188 MB) rests on 9
  projects with a median of 100 MB, and pandas is an extreme project. Until the
  corpus is large, cross-project package dedupe barely shows in the sample, so
  later marginals should fall. Read both scenarios as upper-leaning.
- **TMax and Terminal-Lego task deltas** (context transfer, verifier and task
  setup) were not built or measured.
- **Bootstraps** are included in the per-image marginals, about 0.48 TB raw for
  all images. They compress about 2.9× with zstd -3, to about 0.17 TB.
- **Conversion CPU:** at 27 CPU-s per GB, roughly 220 CPU-hours for the whole
  selection. That is negligible next to the 1,200–1,300 build-slot hours of
  C2.14.
- **The existing prepared corpus** would shrink about 8.6× when migrated, going
  by the sample.

**Reading the numbers.**

- **OpenSWE dominates.** Even with chunk-level dedupe, precomputing all of it
  needs about 7–9 TB of new chunks. That is more than the "registry must not
  grow by many TB" constraint allows.
- **C2.14 needs a growth budget per family.** Possible cuts:
  - prune build residue such as pip caches, build trees and `.o` files;
  - precompute OpenSWE in waves;
  - accept that the registry grows.
- **Every other family fits easily**, at under 0.3 TB in total plus the
  TMax/Terminal-Lego task deltas.

## Risks

- **We must build the chunk store ourselves.** The sample produced 0.86M unique
  256 KiB chunks averaging 19 KB compressed. The full corpus implies roughly
  400–500M chunks. That needs:
  - pack files, which can be one per conversion, with the image's new chunks
    appended;
  - a digest index and a per-image chunk locator;
  - garbage collection by reference.

  The spike only modelled the store's accounting.
- **The chunk table is the trust anchor.** Upstream has a "TODO: get rid of the
  chunk info array" (`builder/src/core/v6.rs`). Pin v2.4.5, or be ready to
  derive the map from the inode chunk indexes plus `blob.meta`.
- **Merge details are easy to get wrong.** Blob ids come from bootstrap file
  names unless `--original-blob-ids` is passed. An image is limited to 254
  layers or blobs; the largest image here had 25 layers (an OpenSWE pandas
  task).
- **The production backend must replace the Python spike backend.** It needs
  chunk mapping, decompression and batched range fetches. The attach figures
  above include the spike's Python start-up.
- **Bootstraps are a real share of storage:** 7% overall, and 41% for ScaleSWE
  at 8 MB of bootstrap against 11.5 MB of new chunks. Compress them, and keep
  them in the metadata prefetch path.
- **Content equivalence rests on a sample.** It was checked on 7 images and
  sampled files. Whiteout and opaque-directory handling showed no differences,
  but the qualification gate should compare full trees.
- **Builds are not reproducible.** 9 of 18 pandas recipes failed against live
  upstream. That is independent of Nydus, but it decides how much of OpenSWE
  can be precomputed at all; see C2.14 lockfiles and the C2.15 mirror.

## Files

- [`raw/storage-1m.json`](raw/storage-1m.json),
  [`raw/storage-256k.json`](raw/storage-256k.json): per-image and per-family
  accounting (OCI, EROFS, bootstrap, new blob bytes with and without the
  dictionary, chunk-store bytes, conversion wall and CPU).
- [`raw/runs/*.images.jsonl`](raw/runs/): raw converter records for the four
  sample runs and the OpenSWE run, including dictionary size, blob counts and
  merge errors.
- [`raw/mount-*.json`](raw/): every attach and command run, backend fetch
  counters, tree comparison, tar mtimes and `.pyc` checks.
- [`raw/openswe-tasks.json`](raw/openswe-tasks.json),
  [`raw/openswe-builds.jsonl`](raw/openswe-builds.jsonl),
  [`raw/openswe-task-selection.json`](raw/openswe-task-selection.json): the
  OpenSWE task builds and their marginals.
- [`raw/dict-integrity.json`](raw/dict-integrity.json),
  [`raw/hashdiff-047.json`](raw/hashdiff-047.json): the dictionary corruption
  evidence. [`raw/nydusify.jsonl`](raw/nydusify.jsonl): `nydusify` end-to-end
  runs. [`raw/sample.json`](raw/sample.json): the sample.
- [`scripts/`](scripts/): what ran on the VM (fetch, convert, analyze, the NBD
  backend, the mount harness, the build and integrity checks). These are spike
  code, not product code.
