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

## Not yet

- **Chunk-store conversion of built images.** The pilot's dedup numbers (about 300 GB for TMax
  and Terminal-Lego) need the built images converted into the chunk store and their EROFS and
  OCI copies released.
- **An importer** that walks a pinned dataset revision and registers its recipes, writing the
  whole `environment/` tree as the pilot's `prepare.py` does.
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

