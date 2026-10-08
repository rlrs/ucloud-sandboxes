# Sandbox builds: recipe images built on the workers (C2.14)

Status: in production since 0.9.58 (2026-10-08), fixed in 0.9.59; results are below.
Motivated by the [build pilot](benchmarks/build-pilot-2026-10-07/README.md) and
[image recipes](image-recipes.md).

## Why

A task image is a prepared foundation plus a few task steps: copy the task's files, an
`apt-get`/`pip`/`uv` install, a verifier bootstrap. Building it with Docker on a builder VM
paid for much more than those steps:

- regenerating the foundation's OCI copy from the chunk store, because BuildKit needs a
  registry image to start from (once per foundation, 3,535 of them);
- BuildKit pulling that base and pushing the result as gzip layers;
- converting the pushed image back into the chunk store, where the foundation already was;
- a builder VM for all of it.

In the pilot (250 tasks) the task steps took 490 s of TMax's 7,582 s end to end in Docker,
1,997 s of Terminal-Lego's 3,813 s, and 2,064 s of OpenSWE's 4,730 s, once apt went through
the [package cache](#package-cache).

## The design

A recipe the prepared catalog splits into a base and a remainder is built in a sandbox:

1. **Plan** (`sandbox_build.plan_build`, pure). The catalog's rewritten Dockerfile (`FROM
   <base>` and the task's remaining instructions) becomes one POSIX shell script plus the image
   config Docker would record (Entrypoint, Cmd, Env, WorkingDir, User).
2. **Run.** A sandbox starts from the base's chunk-store root on a worker (the workers already
   mount it). The script and the build context arrive as one archive under
   `/var/tmp/.ucloud-build`, and the script runs as one managed job.
3. **Export.** The worker's `commit-export` (C3.1) stages the sandbox's overlay upper in the
   registry, the sandbox is deleted, and the keyless commit filter turns the upper into a
   sorted OCI layer tar.
4. **Stack** (`RafsConverter.extend`). Only that layer is converted. `nydus-image merge
   --parent-bootstrap` stacks it on the base's merged bootstrap, and the result is signed as a
   whole-image RAFS component and root whose source layers are the base's plus this one.

No OCI image is read or written, no foundation copy is regenerated, and no builder VM is
needed. Recipes the plan cannot run keep going to the builders, unchanged.

### What a sandbox build runs

Recipes reach the plan only after the catalog's `safe_rewrite`, so they have one `FROM` and no
`ARG`, `ADD`, heredocs, `RUN --` flags or COPY globs and flags (other than `COPY
--from=<literal image>`). Of the rest:

| Instruction | Sandbox build |
|---|---|
| `RUN` (shell or JSON form) | `env -i` with the image's Env as of that step, plus `HOME` (if unset), `TMPDIR` and `APT_CONFIG`; in its WORKDIR; as its USER through `setpriv` |
| `COPY src... dest` | Docker's rules: a directory's contents; a file into `dest/` when it ends in `/` or several sources share it; parents made; owners 0:0; modes, mtimes and symlinks kept |
| `COPY --from=<public image>` | the gateway host pulls the named paths of that image (linux/amd64, anonymous, cached by digest). Only public registries (`docker.io`, `ghcr.io`, `quay.io`, `gcr.io`, `registry.k8s.io`, `public.ecr.aws`, `mcr.microsoft.com`) |
| `ENV`, `WORKDIR`, `USER`, `SHELL`, `CMD`, `ENTRYPOINT` | recorded as Docker does: substitution with `$VAR`, `${VAR}`, `${VAR:-x}` and `${VAR:+x}`, and an `ENTRYPOINT` resetting an inherited `CMD` |
| `LABEL`, `EXPOSE`, `VOLUME`, `STOPSIGNAL`, `HEALTHCHECK`, `MAINTAINER` | ignored: a root's image config has no place for them, and sandboxes do not use them |
| anything else | the builders |

Every Terminal-Lego task copies uv from `ghcr.io/astral-sh/uv:0.9.5`. TMax uses only `ENV`,
`COPY` and `RUN`.

**Where it differs from Docker, on purpose:**

- `/tmp` and `/run` are not committed. A sandbox mounts tmpfs there, so an image's files there
  were never visible to a sandbox anyway.
- Steps get `TMPDIR=/var/tmp/.ucloud-build-tmp` (removed before the commit). The sandbox's
  `/tmp` is a 64 MB tmpfs, and one OpenSWE build needed several GB of temporaries.
- `HOSTNAME` is not set in steps.
- Build caches are kept, as in a Docker image: pip, uv and npm caches and apt archives (the
  commit policy's `keep_build_residue`; agent commits still drop them).

### Images born in the chunk store

A sandbox-built image never had an OCI manifest. Its identity is the digest of the manifest it
would have had: its config, no layers, and the root as the environment annotation
(`born_manifest_digest`). Ensure adopts a finished build:

- an `image_roots` row, `released` from the start (wave `sandbox-build`), with its tag
  remembered and its OCI marked released (0 bytes);
- the gateway image record (`source: build:sandbox`, that digest).

Resolution, root dispatch, pinning and toolkits then treat it as they treat any OCI-released
image (the M2 corpus, recipe images built by builders).

### Where it runs

- **The decision** is the gateway's: `gateway/sandbox_builds.py`, called by ensure's submit
  before the builder dispatch. It reads the context, asks the catalog, and checks that the base
  dispatches one whole-image chunk-store root and that the remainder plans.
- **The builds** run in `ucloud-sandbox-builds.service` (`serve-sandbox-builds`) on the gateway
  host, `slots` at a time. It holds the chunk store's S3 key (`/etc/ucloud-sandboxes/s3.env`),
  the index write token and the environment signing key; the gateway's API processes never
  hold the S3 key.
- **The spool** (`<state>/sandbox-builds/`) connects them: the gateway writes
  `jobs/<image>.json`; the service writes `results/<image>.json`. Ensure reads a result by its
  build id; a job with neither a job file nor a result is lost and resubmitted after
  `LOST_BUILD_SECONDS`, as builder builds are.
- **The sandboxes** are ordinary managed sandboxes created through the gateway API (root, all
  capabilities, as a Docker build step has; `cpus`/`memory_mb`/`disk_mb` from the config), so
  placement, capacity and scale-up are the fleet's. `commit-export` is admin-only.

A failed step fails that build attempt with the step's number, exit code and the end of its
output. Ensure's retry rules apply: 3 attempts, 10 minutes apart.

### Package cache

Steps' apt requests to the package cache's hosts (Ubuntu's and Debian's archives) go through
the store node's cache (`APT_CONFIG` with per-host proxies; the image's `/etc/apt/apt.conf`
is kept). Every other client goes direct. Canonical's archive stalls from UCloud; the cache
serves it from a Danish mirror.

## Configuration

```json
"immutable_environments": {
  "sandbox_builds": {"enabled": true, "slots": 8, "stack_slots": 3, "cpus": 4.0, "memory_mb": 8192,
                     "disk_mb": 32768, "timeout_seconds": 1800}
}
```

It requires `signing_key_file`, `dispatch_roots` and a `chunk_store` with
`nydus_image_sha256` and a `store_node`. Off, every recipe builds on the builders as before.
The service exits 78 and stays stopped.

## Results (2026-10-08)

**Dry runs** (the runner against production, no gateway state written; 33 pilot tasks, 6 at once):

| | TMax | Terminal-Lego | OpenSWE |
|---|---|---|---|
| Built (Docker in the pilot) | 11/11 (11/11) | 11/11 (11/11) | 8/11 (8/11): the same 3 fail, recipe rot |
| Total per build, warm worker | 6–30 s | 27–79 s | 91–227 s (one 640 s outlier) |
| Steps | 1–9 s | 11–32 s | 23–139 s |
| Stacking on the gateway | 3–26 s | 7–37 s | 32–127 s (442 s for 5.8 GB) |
| New chunk bytes | 0.2 KB–23 MB | 1–8 MB | 29–275 MB (2.7 GB outlier) |

The outlier, openswe-211, is large under Docker too (4.3 GB of new OCI, 390 s end to end).

**Same trees as Docker.** The final trees of 24 dry-run builds were compared with the pilot's
Docker images of the same tasks (3 more had aged out of the registry) by path, kind, mode,
owner, size and content:
- every TMax tree has the same paths; content differs only where the build is
  nondeterministic (`/etc/shadow`'s date, generated SSH keys, git objects);
- Terminal-Lego differs only in uv's randomly named cache entries (two directory modes came from
  the dry run's own context packing, not the executor);
- OpenSWE differs only by upstream drift (boto3 1.43.108 to .109 between the runs).

**Live checks through ensure.**
- 0.9.58: 15 never-built tasks (6 TMax, 6 Terminal-Lego, 3 OpenSWE) all built in sandboxes and
  became ready in 100–240 s, worker boot included. TMax sandboxes created by name ran, but
  Terminal-Lego's and OpenSWE's died with SIGBUS on their first command: nydusd could not read
  the new layers' blobs (below).
- 0.9.59: 14 more never-built tasks all built in sandboxes; sandboxes created by name started in
  0.6 s and ran (`/app/task_file` and `uvx 0.9.5` for Terminal-Lego, the testbed's commit and
  Python for OpenSWE).

**Found: blobs nydusd could not read (fixed in 0.9.59).** Workers read RAFS images with nydusd
from blobs the store node rebuilds from stored chunks, which must be nydus's own encoding. The
index keeps a chunk's first stored copy, and conversions without nydusd blobs stored a chunk raw
when zstd saved under 3% where nydus compressed it. A new layer sharing such a chunk (apt-installed
files, for example) got an unreadable blob. Builds into the chunk store by builders had the same
exposure: 8 of the 20 recipe images built in 0.9.56's live check were unreadable, as were 3 of
0.9.58's sandbox builds. 0.9.59 re-stores such chunks in nydus's encoding (the index's `commit`
moves them, `supersede`), checks every converted layer before signing, and makes the store node
check stored layouts against blob tables. The 11 images were rebuilt.

**Faster stacking (0.9.61-0.9.63).** Measured per phase on production builds:
- The commit filter wrote names in string order and the converter wants path order, so every
  layer was copied twice before conversion; it is written in path order now.
- Uploads to the bucket run about 11 MB/s per stream from UCloud and 66 MB/s with four; packs now
  upload four at a time while the next fills (two in 0.9.61, one before).
- The pack writer summed its entries on every chunk it took, quadratic in a pack's chunks; it
  keeps a running size.

Results: openswe-211's 5.6 GB layer stacked in 54 s (442 s in the 0.9.58 dry run), openswe-208's
in 63 s, Terminal-Lego's in about 9 s (13-37 s). Large OpenSWE builds with fresh content upload at
40-64 MB/s (5-6 before). What remains is `nydus-image create` (single-threaded, 2-30 s) and the
index's whole-image work at registration (2-10 s); the steps themselves now dominate a build.

## Not yet

- **Cached recipe images are never collected.** An `image_roots` row keeps its root (and
  chunks) for good; this was already true of builder-built recipe images.
- Multi-stage Dockerfiles, `ARG` and `ADD` stay on the builders.
- Registration's index work grows with the whole image (2-10 s), not with the new layer.
