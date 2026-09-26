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
