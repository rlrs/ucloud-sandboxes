# Image recipes: names built on demand or ahead (C2.7)

Status: in production since 0.9.54 (2026-10-07); the live check is at the end. Motivated by the
[build pilot](benchmarks/build-pilot-2026-10-07/README.md).

## The gap

Training names images (`prime/primeintellect/tmax:task_000001_abc`,
`terminal-lego/000123:latest`) and never sends a recipe. verifiers v1 does not build Dockerfiles.
The gateway resolves a name only when that exact image was prepared as a source. Most
OpenSWE, TMax and Terminal-Lego tasks are only foundation-backed: their task images were never
built, so a create naming one has nothing to resolve to.

## The design

**The recipe index** (`gateway/image_recipes.py`, `image-recipes.sqlite3` beside the image
store) maps each registered name to a recipe: an uploaded build context, its Dockerfile path and
build arguments, plus a retention class.

- **One image per recipe.** Identical recipes share one gateway-managed image id,
  `recipe-<sha40 of the inputs>`, so a recipe is built once whatever names it carries. Its tag
  follows from the id.
- **Contexts are kept.** Uploads age out of the gateway's context store after a day (8,192 at
  most), so registration copies each context into `<image store>-recipe-contexts/`, which never
  expires. It is put back into the upload store before a build.
- **Retention.** `pinned` images get a durable registry reference when they become ready (a
  corpus built ahead). `cached` ones age out like any managed image and are rebuilt on demand.

**`POST /v1/image-recipes`** `{"recipes": [{name, context_archive_digest, context_archive_size,
dockerfile?, build_args?, retention?}]}`, up to 1,000 per call. The contexts must already be
uploaded (`PUT /v1/image-contexts/<digest>`). Re-registering an unchanged recipe is a no-op; a
changed recipe moves its name to a new image.

**`POST /v1/images/ensure`** `{"names": [...]}`, up to 1,000 per call, returns each name's
state:

- `ready`, with `reference`, the pinned worker reference a sandbox gets;
- `building`, with `build_id`;
- `queued`: no builder slot yet, or past this call's submission budget;
- `failed`, with `error` and `attempts`;
- `unknown`: not registered.

Missing images are submitted through the gateway's ordinary build dispatch (the same as
`/v1/images/build`: prepared foundations, regenerated bases, builder selection and scale-up), at
most 32 per call. The call is idempotent and cheap, so callers poll it.

**Build state** is the store's, not the builders'. Builders forget their builds when they scale
down. Each image has a row: `absent`, `submitting`, `building`, `ready` or `failed`.

- A submission is claimed atomically, so the gateway's processes never submit one image twice.
- A succeeded build's image record is adopted by the gateway, so it resolves after its builder is
  gone.
- A build no builder knows is resubmitted after 3 minutes.
- A failed build is retried after 10 minutes, 3 attempts in all: network failures pass, recipe rot
  does not.

**A create naming a registered recipe** gets the built image's reference, or a retryable 503
`image_building` (`Retry-After: 15`) that also starts the build. The SDK already retries those, so
a rollout on an unbuilt task waits for its build (0.5–4 minutes in the pilot). A recipe that
failed for good answers 409 `image_build_failed`.

## Builds into the chunk store (builder_format "rafs")

Today a build ends as EROFS components in the registry, and only an operator's M2-style
waves move images into the chunk store. The pilot showed what that costs: about 80 MB of
registry per TMax image and 220 MB per Terminal-Lego image, against about 5 and 15 MB of
deduplicated chunks. With `immutable_environments.builder_format = "rafs"`, a build ends
in the chunk store instead.

- **On the builder.** The build's last step, where the EROFS publisher ran, converts the
  pushed image with `RafsConverter` (`chunk_convert.rafs_build_publisher`).
  - The tag moves to a copy annotated with the chunk-store root: the same contract as the
    EROFS publisher, so resolution and dispatch are unchanged.
  - Layers another build converted are reused through the index's layer claims, so the
    first build on a foundation converts it once (93 s for TMax's largest) and later ones
    convert only their own layers (about 20 s each, mostly whole-image index work).
- **Credentials.** Rafs builders get the chunk-store block, the index's write token and the S3
  key (`chunk-store.env`, root-only, as on the store node), never the read token.
  - They already hold the environment signing key, the stronger credential: workers trust
    whatever it signs. So this does not widen what a builder can do.
  - `nydus-image` comes from the builder bundle, checked against
    `chunk_store.nydus_image_sha256`.
- **Release.** When ensure first finds a recipe image ready with a chunk-store root, the
  gateway records an `image_roots` row (old and new root are the same; wave `recipe`), moves it
  to `released`, and deletes the OCI manifest through `release_oci`'s fences.
  - A lease or a route still reading the manifest defers this to a later ensure.
  - Release-aware resolution then answers the image from its row, as for the M2 corpus.
  - No regeneration receipt is needed: a recipe can always rebuild its image.
  - An EROFS build keeps its OCI copy (`oci: kept`).
- **Pinned** recipe images are an OCI-free owner (`OCI_FREE_OWNERS`): their `image_roots` row
  keeps the root, so their durable reference no longer pins the manifest.
- **No full-tree check** on builders, unlike the M2 waves, whose originals could not be rebuilt.

## Clients (SDK 0.4.37)

```python
client.register_image_recipes([ImageRecipe(name, context_dir, retention="pinned"), ...])
client.ensure_images(names)        # {name: {"state": ..., ...}}; builds what is missing
client.wait_for_images(names)      # polls until every name is ready, failed or unknown
```

## Where lookahead lives

Only the trainer's sampler knows the next step's tasks. When it samples step N+1, it calls
`ensure_images` with their image names, so the builds overlap step N. Without that call,
training is still correct, just slower on a task's first use.

## Sandbox builds

Since 0.9.58, ensure builds a recipe on a chunk-store base in a sandbox on the workers instead
of on a builder, when its Dockerfile allows: see [sandbox-builds.md](sandbox-builds.md).
Everything above (states, retries, create by name, release) is unchanged.

## Not yet

- **The trainer-side lookahead call.**

## Live check (0.9.54, 2026-10-07)

- **What ran:** 9 pilot tasks (3 per family) were registered under fresh names through the new SDK
  with no builders running.
- **Building:** ensure reported `queued` until builders came up, then `building`, then `ready`
  for all 9, each with a pinned `ucloud-managed/recipe-…@sha256:` reference.
- **Creating by name:** the first sandbox needed a worker boot (83 s). On a warm worker, managed
  sandboxes started in 0.2–0.9 s, ran commands, and had their task files (`/app`, `/testbed`).
- **Found:** recipe builds were submitted without `wait: false`, so one ensure call held its
  request through all nine builds (530 s). Fixed in 0.9.55.

## Live check, builds into the chunk store (0.9.56, 2026-10-07)

- **What ran:** 20 never-built tasks (12 TMax pinned, 8 Terminal-Lego cached), registered by
  `scripts/import_image_recipes.py register --prebuild 16`.
- **Built:** all 20 ready, the 12 TMax in 18 minutes (including builder boot) and the 8
  Terminal-Lego in 4. Conversion on the builders took 16–33 s per image; most added a few KB
  of new chunks, the largest 61 MB.
- **Released:** ensure released all 20 OCI copies. Their `image_roots` rows are `released` (wave
  `recipe`) and their manifests answer 404.
- **Created by name** from chunk-store roots only: 0.4–0.5 s on a warm worker (86 s for the first,
  a worker boot), with commands and task files as expected.

