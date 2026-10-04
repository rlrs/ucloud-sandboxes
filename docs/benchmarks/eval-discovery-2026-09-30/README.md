# Eval task-set discovery inputs (2026-09-30, preserved 2026-10-04)

The discovery behind `build/eval-image-pool-20260930` and the cached training
list ran from `/tmp/ucloud-cache-study-20260930/`. These are its inputs, kept
here so they survive `/tmp`. The full study, about 1.2 GB, is in
`build/cache-study-20260930/` on the operator machine.

| File | What it is |
| --- | --- |
| `harbor-registry.json.gz` | The Harbor dataset registry snapshot: tasks, git URL, commit and path per task set |
| `collect-eval-images.py` | Fetches each task's `environment/Dockerfile` (and `task.toml` for the terminal sets) at the pinned commit |
| `eval-image-source-receipt.json.gz` | That fetch's receipt (URL and sha256 per file) |
| `eval-image-inputs.tar.gz` | The fetched Dockerfiles and `task.toml` files |
| `make-eval-pool.py` | Turns them into the eval inventory, upstream task image or base only. It hard-codes 10 images per eval set. |
| `cached-training-list-generation-script.py` | The cached training list's generator, which reads the same inputs |

**SWE-bench Pro** (`swebenchpro 1.0`, 731 tasks, harbor-datasets `c8e8f3fac7`)
was never fetched before. On 2026-10-04 every task's Dockerfile turned out to
be `FROM jefzda/sweap-images:<per-task tag>` plus three steps. The 731
distinct tags average 1.39 GB compressed (p50 1.09 GB, max 4.83 GB), and a
30-image sample shares 24% of its layers. The Dockerfiles are in
`build/eval-discovery-20261004/swebenchpro/`.
