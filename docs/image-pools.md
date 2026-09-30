# Preparing larger image pools

The pool workflow resolves source images to linux/amd64 digests, builds their signed EROFS artifacts ahead of a run, validates a sandbox, and retains the complete artifact closure. It supports public Docker Hub, GHCR, and Microsoft registry sources. A failed image remains explicitly failed; it cannot enter the ready catalog.

Prepare an inventory with `schema: 1` and an `images` array. Each entry has a unique `source`, a `families` array, and positive `task_rows`. Optional `uses` records should identify dataset revisions and distinguish full upstream task images from base-only preparation. Counts describe their declared pools, not an unknown training sampler. Reuse one immutable artifact across training/evaluation selections; do not duplicate an image merely because two splits reference it.

```json
{
  "schema": 1,
  "images": [
    {
      "source": "swebench/example-repository-environment:latest",
      "families": ["SWE-smith"],
      "task_rows": 100,
      "preparation": "swesmith-v1"
    }
  ]
}
```

Use the planner to reserve a small per-family sample, then prioritize remaining entries by task reuse. Optional `repository` metadata makes that sample prefer distinct repositories, ordered by their total declared task count. Add `--balanced` to continue by the fraction of each family visited after the initial sample; otherwise the remaining order prioritizes task reuse. Repository membership is a diversity hint, not proof that those images share dependency layers:

```sh
uv run python scripts/plan_image_pool.py plan \
  --inventory /data/inventory.json --output /data/preparation-batch \
  --limit 100 --per-family 2
```

The `source` preparation (default) preserves the upstream image. `swesmith-v1` additionally fetches all repository branch refs and installs ripgrep, matching the pinned integration's common preparation recipe. These fetched refs and resolved packages are frozen in the published artifact; refreshing them requires an intentionally versioned preparation recipe. This does not stage hidden test files or precompute test results. The smoke validates the signed closure and sandbox mount/exec; SWE-smith also checks its repository refs and ripgrep. Exact task branch membership and task-specific dependency readiness still require the actual selection or stronger family-specific qualification.

Run the preparer on the gateway, as its service account:

```sh
python scripts/prepare_image_pool.py \
  --root /data/preparation-batch \
  --sdk-wheel /data/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl \
  --gateway https://sandbox.example \
  --limit 100 --workers 2 \
  --growth-limit-gib 160 --free-floor-gib 300 --max-image-gib 5
```

`--workers` accepts 1–32 concurrent preparations; backend admission still controls actual build and finishing slots. Each source resolution and accepted build is recorded before waiting. Public source resolution is serialized per registry across coordinators, with a persisted cooldown for rate limits and transient gateway errors. Long cooldowns become explicit deferrals. An image's preparation identity includes its pinned source, platform, and actual Dockerfile. Published artifacts are checked before build history; completed publication receipts and gateway image records survive builder replacement. Every outcome has an atomic per-source journal; the large catalog is checkpointed periodically and recovered from those journals on restart. The signed registry closure is checked again on resume. Fleet inventory is fetched at most once per run, rather than once per image. Per-image claims and accepted-build journals are shared across pool directories on the same gateway. They do not claim coordination across independent gateways.

The batch's initial disk usage persists across restarts. Admission accounts for current registry growth, a configurable free-space floor, and estimated space for other images in that coordinator. Per-image compressed size is capped before submission. The estimate is four times compressed source bytes with a 1 GiB minimum; this is a conservative scheduling estimate for ordinary images, not a hard decompression or filesystem quota. Other coordinators can consume space concurrently, so retain substantial headroom and the gateway's existing disk-pressure controls. Deferred entries remain visible rather than silently dropping tasks.

Successful images acquire persistent `image-pool:<key>` retention owners. Sandboxes used for validation have a finite TTL and are deleted. Validation uses UID/GID `0:0`, matching the pinned training integration; it does not relax fleet or sandbox defaults.

## Using the prepared images

After validation and retention, faithful `source` imports atomically acquire ordinary import aliases in the gateway's image store. Existing requests for those external references then resolve the prepared image through the normal import path, without a new SDK or another build. Existing aliases are never overwritten; `preserved-existing` records a conflict with a previously imported snapshot and must not be interpreted as activation of the new snapshot. Enriched preparations such as `swesmith-v1` change image contents and defaults, so they use explicit managed image IDs or rewritten recipe indexes; they must not replace upstream digest aliases.

For recipes that still build on these sources:

```sh
uv run python scripts/plan_image_pool.py rewrite-index \
  --source /data/images.sqlite --catalog /data/preparation-batch/catalog.json \
  --output /data/images.with-prepared-bases.sqlite
```

This changes only matching FROM operands. Stage aliases, remaining commands, and context inputs are preserved; unsupported Dockerfile layouts are unchanged. An exact plain-source or supported SWE-smith preparation recipe can use `prepared_image` directly and skip the entire build. Enriched images are accepted only for their exact supported preparation recipe; arbitrary derivatives are unchanged. Other changed recipes clear that field and keep their remaining task work. The source database is read-only and an existing output is never replaced. Combine this with the [shared foundation rewriter](image-foundations.md) for OpenSWE and TMax.

Audit the resulting **actual run selection**, including all foundation and pool catalogs:

```sh
uv run python scripts/plan_image_pool.py audit-index \
  --source /data/images.with-prepared-bases.sqlite \
  --catalog /data/preparation-batch/catalog.json \
  --catalog /data/tmax-foundations/catalog.json \
  --max-live-builds 100 --max-cold-builds 0
```

The limits are explicit allowances, not recommended capacity numbers. This example exits unsuccessfully if more than 100 recipe rows still require a build, any recipe has an unprepared or unrecognized base input, or a `prepared_image` ID lacks a validated catalog record. Multi-stage bases and external COPY/mount inputs are checked conservatively. The result is a read-only receipt audit: it cannot establish live registry availability, network independence of remaining RUN commands, or build-duration bounds. Qualify the allowed live recipes and exercise the ready artifacts before using the result as a launch gate. A source-pool inventory is not a substitute for the actual selected recipe index.

For broader pool accounting, use `report --inventory /data/inventory.json --catalog /data/preparation-batch/catalog.json`. It reports base-only coverage separately from upstream task-image coverage and sums the union of EROFS component digests.

## Coverage and storage limits

The production preparation campaign has a 3 TB registry ceiling. Its expanded queues use a 500 GiB free-space admission reserve, account for pending work, and stop admitting new images when their growth budget is exhausted. Raising builder capacity does not raise the storage ceiling. Oversized or inadmissible images remain deferred; this workflow never automatically resizes the volume.

Report three separate states: prepared task image, shared foundation/base ready with task work remaining, and cold/unresolved. A cached operating system or Python image does not imply that a task's dependencies are prepared. A mounted upstream task image does not prove that every later verifier command is offline-ready. Bash/OpenCode/Pi harness setup remains separate from these task artifacts.

Measure storage from the union of component digests, and also account for OCI/cache blobs and conversion scratch space. Flattened upstream images can contain gigabytes of unique data even when repository names match. Importing every such image into a bounded registry is not a substitute for measuring the selected working set or designing file-level sharing.

Persistent preparation owners require explicit retirement when a pool is no longer used. Never delete shared blobs directly. A ready catalog is not a promise that unrelated, unprepared images will avoid a cold import.
