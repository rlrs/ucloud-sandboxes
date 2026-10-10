# The image index: how training image names work

Status: in production since 0.9.66 (2026-10-08).

## In one paragraph

verifiers asks for each task's image by **name**, the same name it would send to Prime
(`prime/primeintellect/tmax:task_000123_abcd`, `aweaiteam/scaleswe:beetbox_beets_pr3661`). It
never sends a Dockerfile. The gateway answers a name only if it is in the **image index**. Every
name in the index is backed by something already in the chunk store: either the whole image, or
the base its recipe builds on. A task whose image has nothing prepared is not in the index, and
the trainer does not sample it: the index exports, per environment, the task ids it may sample,
in the `task_ids_file` format the tasksets read.

## What a name is

Each name in the index has:

- an **environment** (`tmax`, `terminal-lego`, `openswe`, `scaleswe`, `r2e-gym`, `swe-lego`,
  `swe-rebench-v2`, `multiswe`, `swe-smith`);
- its **tasks**: the dataset tasks that use the image, by the id the environment's taskset
  filters on (below). One SWE-smith image serves hundreds of tasks; elsewhere it is one;
- its **source**: dataset, revision, the inventory it came from and the environments revision;
- its **kind**:
  - `prepared`: the whole image is in the chunk store. It is ready from registration; nothing is
    built;
  - `recipe`: a Dockerfile and context that build on a prepared base. The gateway builds it the
    first time it is asked for (in a sandbox, usually under a minute or two), or ahead with
    `ensure`, and serves it by that name from then on.

The gateway refuses to register a prepared name whose image is not in the chunk store, and a
recipe name whose recipe builds on nothing prepared (the build path's own match,
`prepared_images.choose`). So "in the index" means "starts from something prepared".

## States

| State | Meaning | Sampled? |
|---|---|---|
| `ready` | Prepared, or built | yes |
| `not_built` | A recipe not built yet: the first create waits for its build (`503 image_building`, retried by the SDK) | yes |
| `building` | Its build is running | yes |
| `retrying` | Its build failed and will be retried (3 attempts, 10 minutes apart) | yes |
| `failed` | Failed for good (recipe rot, such as yanked packages), or a prepared image the chunk store lost | **no** |

## Looking at it

With the gateway URL and the SDK key (`UCLOUD_SANDBOX_URL`, `UCLOUD_SANDBOX_API_TOKEN`, or
`--gateway-url` and `--api-token-file`):

```bash
ucloud-sandboxes image-index summary                       # names and tasks per environment, by state
ucloud-sandboxes image-index show 'aweaiteam/scaleswe:beetbox_beets_pr3661'
ucloud-sandboxes image-index list --environment openswe --state failed
ucloud-sandboxes image-index task-ids tmax --out tmax.task-ids.json
ucloud-sandboxes image-index export-task-ids allowlists/  # every environment's task_ids_file
```

The same views are gateway endpoints, readable with the SDK key: `GET /v1/image-index`,
`/v1/image-index/names?environment=&state=&after=&limit=`, `/v1/image-index/name?name=` and
`/v1/image-index/task-ids?environment=`. Clients read them without this package: SDK 0.4.38's
`image_index_summary`, `image_index_names`, `image_index_name` and `image_index_task_ids`, and
from verifiers-ucloud 0.3.1 `verifiers-ucloud task-ids DIR` (the same files as
`export-task-ids`) and `verifiers-ucloud summary`.

## The trainer's allowlist

`export-task-ids DIR` writes `DIR/<environment>.task-ids.json`: a JSON array of the task ids whose
names are not `failed`. Pass it as the taskset's `task_ids_file`. The ids are what each taskset
names its tasks by on `feat/lumi-ucloud-envs-20260930`:

| Environment | Task id |
|---|---|
| tmax, terminal-lego | the task's directory name (`task_000123_abcd`, `task_01234`) |
| r2e-gym | `commit_hash` |
| swe-smith | `<language>:<instance_id>` (`py:...`) |
| the others | `instance_id` |

Every one of the nine tasksets takes a `task_ids_file` since 99262da5 on that branch
(2026-10-10), which added it to ScaleSWE, R2E-Gym and SWE-Lego; before it, those three
sampled tasks with no registered name.

Re-export after builds fail for good, so the trainer stops sampling them; a failed name stays
visible in `list --state failed` with its error.

## Filling it

`scripts/build_image_index.py` registers every name of the 2026-10-01 training inventory
(`all-cached-training-tasks-with-terminal-lego-2026-10-01.zip`): the image names the pinned
tasksets ask for, with what was prepared for each.

1. `export`, where the datasets are: names with no remaining build work become `prepared`
   names; the rest become recipes, from the TMax and Terminal-Lego exports
   (`scripts/import_image_recipes.py`) and OpenSWE's recipe database, each checked against the
   inventory's `recipe_sha256`. Task ids come from the datasets at their pinned revisions.
2. `register`, on the gateway: uploads contexts and registers in batches; names the gateway
   refuses (nothing prepared) land in `<environment>/refused.jsonl`.

Re-running either step is safe. Names prepared later are added by running it again with a
newer inventory.

## Filled (2026-10-08/09, 0.9.66)

- **Registered:** 66,774 names, 130,241 tasks. Prepared: MultiSWE 60, R2E-Gym 67, ScaleSWE 2,469, SWE-Lego 72,
  SWE-rebench v2 74, SWE-smith 118 (63,585 tasks). Recipes: TMax 14,600, Terminal-Lego 13,765, OpenSWE 35,549.
- **Refused:** 12 Terminal-Lego names whose base is no longer in the chunk store (`terminal-lego/refused.jsonl`).
- **Checked by name, as `verifiers-ucloud` creates (managed, parkable):** a prepared name and a built recipe start
  in 0.3-0.5 s; never-built Terminal-Lego and OpenSWE names built and started on first use in 127-173 s (a worker
  boot included). One OpenSWE recipe failed on first use (PyYAML no longer builds): `retrying`, then `failed`
  and out of the next export.

