# Shared image foundation qualification

On 2026-09-30, four foundations were built in production, validated in temporary sandboxes, and retained with their signed artifact dependencies. The complete aggregate measurements are in [summary.json](summary.json). The preparation workflow is documented in [image-foundations.md](../../image-foundations.md).

| Foundation | Candidate source recipes | EROFS bytes |
| --- | ---: | ---: |
| TMax common scientific/toolchain installer | 5,134 | 1,985,335,296 |
| OpenSWE Python 3.12 | 10,623 | 1,198,120,960 |
| OpenSWE Python 3.11 | 5,290 | 1,184,616,448 |
| OpenSWE Python 3.10 | 4,849 | 1,154,953,216 |

These four images contain **4,310,667,264 unique EROFS bytes**, after accounting for shared components. This excludes OCI layers and BuildKit cache storage. Candidate counts come from the pinned upstream inventory, not a particular training selection.

The pinned research repository's OpenSWE generation logic was applied locally to all 36,884 projected source recipes. The resulting coverage fixture accepted **20,759 exact prefix replacements** with the three validated Python foundations. Three additional planner candidates had unusual layouts and were preserved. This fixture is not the user's training index; the actual index was unavailable.

## Real task qualification

Two complete, real TMax task contexts were built from the foundation. An original-recipe version of the first task was also built, with the same pinned Ubuntu base, for comparison.

| Case | Build seconds | Build plus smoke seconds | Reused foundation bytes | Additional EROFS bytes |
| --- | ---: | ---: | ---: | ---: |
| Original task `000023_004a85b4` | 15.95 | 25.45 | 1,985,335,296 | 80,338,944 |
| Factored task `000023_004a85b4` | 15.47 | 24.84 | 1,985,335,296 | 80,338,944 |
| Factored task `000099_1cd88af1` | 15.72 | 25.04 | 1,985,335,296 | 80,338,944 |

Every case reused both foundation components and built one additional component. The original and factored first task had identical hashes, ownership, and permissions for the checked regular files under `/app` and `/home/user`, identical `pip freeze` output, and matching selected environment variables. The probe covered regular non-symlink files smaller than 1 MiB; it did not compare every byte of the entire root filesystem or grade either task. Temporary validation sandboxes were deleted.

This was a **warm-cache comparison**, and it does not establish a substantial warm-build speedup or throughput under load. The durable foundation removes dependence on ephemeral BuildKit cache for the common installation and preserves the same EROFS component identities for task derivatives. The initial TMax foundation took 393 seconds of builder execution, including 273 seconds building/pushing and 120 seconds preparing the immutable environment. Autoscaler startup was additional.

The task builds still spent approximately 11 seconds building/pushing and 5 seconds preparing EROFS. Task scripts repeated package-manager checks. Layer whiteouts triggered the existing Docker materialization fallback, including about 2.6 seconds pulling on this warm builder. A fresh builder can have substantially more pull work: the initial foundation's Docker pull took 104 seconds. These costs are not eliminated by prefix factoring.

## Activation and limits

The four foundations and two factored task images are published and retained in production. Per-family `catalog.json` files and resumable receipts are stored under the gateway's dated `image-foundations-20260930` and `openswe-foundations-20260930` preparation directories, with local copies under `build/`.

The implementation includes dependency-only context generation, content identities, bounded resumable builds, artifact retention, smoke validation, and atomic recipe-index rewriting. Nine focused tests and Ruff passed. No gateway daemon change or SDK release is needed for this workflow.

The actual training recipe index has **not** been switched. It must be rewritten with the validated catalogs and selected through the existing `image_recipe_db` setting. Foundations do not prebuild the task-specific remainder. Other environment families, the eight remaining OpenSWE Python foundations, and the rare alternate TMax installer were not built in this batch.
