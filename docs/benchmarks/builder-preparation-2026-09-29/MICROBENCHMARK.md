# Filesystem preparation benchmark

`scripts/benchmark_environment_preparation.py` runs local baseline/candidate comparisons of OCI layer materialization and layer-group squashing. It performs no registry, Docker, or production operations. Each arm imports its selected source directory in a fresh interpreter and writes only its own temporary extraction trees and chosen output directory.

The default fixture has 2,000 application modules and 2,000 `node_modules` packages, four compressed OCI layers, 12,012 entries in the merged tree, same-layer hardlinks, symlinks, and overwrites that must break earlier hardlinks. Every archive parent has explicit metadata. A separate small overlay fixture exercises file/symlink replacement, retained hardlinks, user xattrs, deletion whiteouts, retained lower-layer whiteouts, and trusted opacity when the host permits these operations. Selective OCI extraction intentionally does not accept whiteouts or extended metadata; those semantics are checked through the general squasher.

## Run

Prepare two frozen source directories containing `ucloud_sandboxes/`. Use the same Python environment, interpreter, user and filesystem for both arms. The local baseline is commit `48f30bd1bbc3e74fbf16576c8b7964db60b2817a`, preserved under `/tmp/ucloud-preparation-baseline-48f30bd`; a remote run needs its own staged baseline. The script verifies the imported module locations and records SHA-256 fingerprints of both runtime files, rejecting source changes within an arm.

Single publication, including separate profiling runs:

```sh
.venv/bin/python scripts/benchmark_environment_preparation.py \
  --baseline-root /path/to/baseline \
  --candidate-root /path/to/candidate \
  --candidate-consume-private-diffs \
  --modules 2000 --repeats 2 --concurrency 1 --profile \
  --output /tmp/preparation-single-r1
```

Four publications sharing one process and interpreter:

```sh
.venv/bin/python scripts/benchmark_environment_preparation.py \
  --baseline-root /path/to/baseline \
  --candidate-root /path/to/candidate \
  --candidate-consume-private-diffs \
  --modules 2000 --repeats 2 --concurrency 4 \
  --output /tmp/preparation-four-r1
```

The explicit candidate flag matches production's new private-extraction path: it passes `consume_private_diffs=True` for freshly materialized, privately owned diffs. The baseline always uses the borrowed-source copy contract. Each repetition starts with new trees; no input from a previous consume run is reused. Without this flag, both arms exercise the copy path and do not measure the main squash optimization.

The default two repetitions alternate A/B/B/A. cProfile runs are additional single-publication runs, excluded from latency comparison. Profiles report bounded function/call/time summaries, without argv or credentials. Default subprocess timeout is 180 seconds and maximum fixture size is 5,000 modules. Output paths must be new. Temporary destination trees are cleaned automatically; fixture blobs are removed after successful comparison unless `--keep-fixture` is requested. A failed run retains its fixture for diagnosis.

## Gates and interpretation

`summary.json` must report `exact_semantics_equal: true` and `sources_stable_within_arms: true`. Each run records actual capability probes. On the intended Linux root builder, require an empty `capability_skips` object in **every** arm; UID 0 alone does not prove trusted-xattr or device-node permissions. Local nonroot runs may establish only the explicitly exercised subset.

Comparison covers contents, object type/mode, uid/gid, nanosecond modification times, symlink targets, xattrs, device identity, and hardlink equivalence classes. It also independently checks that overwriting a linked file preserves the untouched alias's old content. Borrowed source trees must remain identical after squash. Atime, ctime, and numeric inode IDs are excluded because independent reads/extraction change them. Newly generated whiteout modification times are normalized: the existing squasher assigns execution time rather than preserving that input timestamp. This is filesystem equivalence, not an EROFS byte/signature or full sandbox execution test.

Four-thread runs synchronize extraction, then squashing, with validation between stages. Validation does not run alongside another thread's timed phase. `batch_phases` reports total wall time and process CPU for each stage; `phases` reports mean per-publication wall time, with per-publication CPU omitted in concurrent mode because process CPU is shared. Stage synchronization deliberately exposes contention within each operation. It does not reproduce production's arbitrary overlap between extraction, squash, registry traffic, BuildKit, or cleanup.

Fixture bytes are shared and warm; destinations are always fresh. The synthetic files compress unusually well, so this isolates filesystem/object overhead rather than network bandwidth. There is no Docker pull, network, registry, `mkfs.erofs`, signing, cache dropping, or global cleanup. Run one benchmark at a time on an idle host and use the representative live build burst to establish production latency.

## Local evidence

[Initial 2,000-module comparison](local-initial-2000.json) predates the consume-path benchmark flag and tests the default copy path. Both arm outputs matched. Mean extraction wall time was 1.322 seconds for the baseline and 1.097 seconds for the in-progress candidate; copy-path squash was 0.894 versus 0.928 seconds. These are local development measurements, not final candidate qualification or a production speedup claim. The file fingerprints identify the exact measured sources.

The harness also passed an eight-module comparison with the consume flag and four threads, and a separate profiling smoke. A direct snapshot check detected deliberately broken hardlink equivalence and changed contents, while capturing a dangling symlink without following it. Ruff and Python compilation passed.

[Four-thread, 2,000-module ABBA](local-four-consume-2000.json) exercises the actual candidate private-diff flag. All 16 publications matched the baseline's contents and metadata under the documented comparison rules; source fingerprints remained stable within each arm. Mean four-publication extraction wall time fell from 9.196 to 6.497 seconds (29.4%), and squash from 10.634 to 3.852 seconds (63.8%). The sum of timed stages fell from 19.830 to 10.349 seconds (47.8%). Local user xattrs and both real whiteout cases were exercised; trusted opacity was unavailable (`EPERM`). These development results still require the root-capable builder semantic gate and the live workload comparison. The initial eight-module profiling smoke briefly overlapped the first run's startup; later full-fixture arms ran alone. Do not treat this small local sample as a precise production speedup estimate.
