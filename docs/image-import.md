# Importing external images for immutable workers

Immutable-environment workers (docs/immutable-environments.md) read image
content on demand from signed EROFS components in the managed registry. They
can only run images the trusted builder has published with an environment
attachment, so a sandbox that names an external image (for example
`aweaiteam/scaleswe:…` on Docker Hub) cannot run there directly.

When the deployment enables `immutable_environments.worker_enabled`, the gateway
imports such images transparently (`ucloud_sandboxes/image_import.py`):

1. **Deterministic ID.** An external reference maps to the managed image ID
   `import-<sha256(reference)[:40]>`. Managed references pass through
   unchanged.
2. **Import as a normal build.** The gateway stores a one-line build context
   (`FROM <reference>`) and submits it through its own `/v1/images/build`, as
   an ordinary managed build. The builder pulls the image, pushes it to the
   managed registry and publishes its EROFS components before it records the
   image. So once the import ID resolves, the image is guaranteed to carry its
   attachment.
3. **Creates wait; preparation does not.**
   - A create for an image still importing gets a retryable 503
     `image_import_pending` with `Retry-After: 5`. The SDK retries creates until
     their deadline (10 minutes by default).
   - `prepare_capacity` with an external image starts the import and continues
     with the original reference, so announcing capacity early warms the import.
4. **Resolved creates** use the pinned managed reference.
5. **Failure.** If the latest import build failed, creates get a
   non-retryable 400 `image_import_failed` with the build error. The import is
   resubmitted at most every 30 seconds per gateway process, so a transient
   failure recovers on a later create.

Each external image is imported once per deployment, and the managed copy
persists. The first create of a large image waits for the builder's pull, push
and EROFS publication (minutes for a 12 GB image). Later creates start
immediately and read only the files they touch.

The builder's allowlist entry `*` publishes every top-level path of an image
except the runtime mounts `dev`, `proc`, `sys` and `run`. Imported and task
images keep content in places a fixed list cannot know.

## Hetzner canary (2026-09-26, rc53): the chunk store is the blocker

The first Hetzner canary built `FROM python:3.12` plus a pip install. EROFS
publication then failed after about 6 minutes. Each 256 KiB chunk upload to the
S3-backed registry took over a second. One commit ran past the client timeout,
and the cleanup DELETE then failed on S3 ("append to zero-size path"), which
masked the original error.

Measured on the gateway, with `registry:3.1.1` and monolithic blob uploads:

| Storage | 256 KiB upload (sequential) | 256 KiB uploads (32 parallel) | 256 KiB read p50 / max |
|---|---:|---:|---:|
| Hetzner Object Storage (S3 driver) | 1.26 s | 3.8/s | 319 / 1,785 ms |
| Gateway local disk (filesystem driver) | 0.01 s | 334/s | 2 / 7 ms |

With S3, a 1 GB image (about 4,000 chunks) would take roughly 17 minutes to
publish, and a cold start that reads a few hundred chunks would wait seconds to
minutes. DSec's on-demand loading relies on 3FS, an RDMA SSD cluster. Chunks
need a comparably low-latency store before immutable workers are enabled on
Hetzner. `IMMUTABLE_WORKERS` in `scripts/hetzner_prod/make_config.py` therefore
stays off, and the producer key is already provisioned.

## Hetzner canary on the registry Volume (2026-09-27, rc54)

The registry moved to a 500 GB Hetzner Volume (ext4, `/mnt/ucloud-registry`):
- **Uploads:** 256 KiB chunks take 50 ms each, or 144/s with 32 in parallel.
- **Reads:** 3 ms p50.

EROFS publication now uploads 16 chunks at a time and retries transient
errors. The canary (`benchmarks/erofs-hetzner-2026-09-27/`) ran on a CCX63
worker and a CCX33 builder, both EROFS-enabled:

| Step | Seconds |
|---|---:|
| Managed build `FROM python:3.12` + pip install, including builder boot | 237 |
| — Docker build and push | 90.6 |
| — EROFS publication (fresh view, `mkfs.erofs`, parallel chunk upload) | 70.7 |
| First sandbox on a worker that had never seen the image | 1.5 |
| First `python3` exec (TLS, SQLite, requests import) | 3.9 |
| Second sandbox, same image | 0.7 |
| Docker Hub `node:22`, first create (includes the import build) | 151 |
| First `node` exec | 2.1 |
| Second `node:22` sandbox | 0.8 |

After both images ran, the worker's verified chunk cache held 129 MB, about
6% of the two images' ~2.2 GB of content. A Docker worker stores both images
in full. Distinct images per worker are bounded by the 128 GiB cache and 512
NBD devices, not by an image store sized for whole images.

## Publication cost (2026-09-27, rc55 → rc56)

rc55 published each EROFS image as one blob with worker Range reads per chunk.
Importing SWE-bench images five at a time on the CCX33 builder still spent
36–100 s per image in publication. The builder agent was one Python process at
~95% of a core while the machine sat ~45% idle. Measured on the builder for a
2.6 GB, 70k-file image:

| Step | Seconds |
|---|---:|
| Python copy of the merged rootfs into a fresh view | 12.8 |
| `mkfs.erofs` (uncompressed) | 2.3 |
| Signing hash (image and per-chunk digests) | ~3.8 |
| Local re-hash before upload | ~1.9 |
| Upload to the volume registry (~180 MB/s) | ~15 |

rc56 changes:
- **No copy for `*`.** For a whole-image allowlist, `mkfs.erofs` reads the
  merged overlay directly, with `--exclude-regex=^(dev|proc|run|sys)$`. The
  resulting tree is identical to the copied view (same size, empty diff of
  paths, modes, owners and sizes). Explicit allowlists still build a fresh view.
- **lz4 compression.** EROFS images are about 38% smaller. The worker kernel
  decompresses, and every chunk a worker fetches carries more content.
- **No re-hash.** The registry verifies the signed digest when it commits the
  upload, and the upload streams in 1 MiB blocks instead of `http.client`'s
  8 KiB.

Compression options on the same 2.5 GB image (8 mkfs threads, cold read of
the whole tree from a local loop mount):

| Option | mkfs | Size | Full read |
|---|---:|---:|---:|
| uncompressed | 6.8 s | 2.54 GB | 7.3 s |
| lz4 | 8.2 s | 1.57 GB | 5.5 s |
| lz4, 64 KiB clusters | 6.8 s | 1.50 GB | 7.2 s |
| zstd | 16.6 s | 1.42 GB | 8.6 s |
| zstd level=1 | 20.2 s | 1.25 GB | 9.5 s |
| zstd, 64 KiB clusters | 18.5 s | 1.35 GB | 6.7 s |

zstd saves another 10–20% of bytes for twice the mkfs time and slower
decompression on every worker read. Workers fetch only what they touch, so lz4
is the default (`FreshEnvironmentBuilder.compression`).
