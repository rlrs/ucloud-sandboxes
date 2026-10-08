# Sandbox builds: recipe images built on the workers (C2.14)

Status: implemented for 0.9.58 (2026-10-08); dry runs against production are below.
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
  "sandbox_builds": {"enabled": true, "slots": 8, "cpus": 4.0, "memory_mb": 8192,
                     "disk_mb": 32768, "timeout_seconds": 1800}
}
```

It requires `signing_key_file`, `dispatch_roots` and a `chunk_store` with
`nydus_image_sha256` and a `store_node`. Off, every recipe builds on the builders as before.
The service exits 78 and stays stopped.

## Not yet

- **Cached recipe images are never collected.** An `image_roots` row keeps its root (and
  chunks) for good; this was already true of builder-built recipe images.
- Multi-stage Dockerfiles, `ARG` and `ADD` stay on the builders.
- The stacking step reads the whole parent chunk map (index work grows with the foundation).
