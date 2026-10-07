# Toolkit layers (C2.5): design for review

Status: built and in production (0.9.51, 2026-10-07); measurements at the end. Plan entry: [rl-scale-architecture-plan.md](rl-scale-architecture-plan.md), C2.5.

## Why

The harness dry run (2026-10-06) spent **12 s of a rollout's ~25 s in setup**:
verifiers' `prepare_uv_script` runs in every sandbox, installs `uv`, then
`uv sync`s the harness's dependencies (`openai`, `mcp`, `httpx`, `tenacity` and
theirs) from PyPI. Sandbox boot was 2 s. At 1,024 rollouts on two workers that is
a thousand concurrent PyPI installs through two NATs, and a real coding harness
installs more.

A **toolkit** is a versioned, read-only tree that a sandbox gets at a fixed path
without installing anything: here, `uv`, a standalone Python and the harness's
prebuilt environment. The same mechanism later carries the managed init and file
helper that every create copies into its rootfs today (C2.5's original scope).

## The model in one paragraph

A toolkit is a **signed environment** (the same artifact an image's root is: a
list of signed EROFS components plus config) whose files all live under
`/opt/ucloud/toolkits/<name>/`. A sandbox asks for toolkits by name; the
**gateway composes** the image's root and the toolkits' components into one
combined root, signs it with the key it already holds, and dispatches it as the
sandbox's `environment_root`. **Nodes change nothing:** they already lease,
mount and share compositions by dispatched root, verify every root's signature,
and give each sandbox a copy-on-write upper over its composition.

## Decisions

### 1. Compose at the gateway, not on the node

- **Gateway composition:** combined root = the image root's manifest with the
  toolkits' components appended to `EnvironmentManifest.toolkits` (the slot the
  signed-artifact rules already reserve for them), so they sit on top, with the image's `source_image` and
  `image_config` unchanged. Signed with `immutable_environments.signing_key_file`
  (the gateway holds it to ship it to builders, so nothing new is exposed).
  Published to the environment repository content-addressed, so composing twice
  is idempotent; recorded as (image root, toolkit roots) → combined root next to
  `image_roots`.
- **Why not on the node:** node composition would change the Python environment
  store, the daemon's image lease index (keyed by image and signed root), receipts
  and their crash recovery, for no gain. A composition per (image, toolkit set) is
  exactly what nodes already do per image.
- **Where:** the create spec resolution step that pins the dispatched root
  (`control_plane.py`, the `dispatch_environment_roots` branch). Retries keep the
  root their route pinned, as today. The group create (`:batch`) resolves one
  spec per group, so a group composes once.

### 1b. With RAFS images (chunk store, nydusd)

- **Production images** are RAFS in the `image` layout: one merged bootstrap per
  image, so their root has one component. nydusd exports it as an NBD block
  device that the kernel mounts as EROFS, and the composition bind-mounts it.
- **With a toolkit** the root has two components, and the composition becomes a
  read-only overlay of two lowers (image, then toolkit on top). That path exists
  (layer-layout images stack up to 33 components); both lowers are kernel EROFS
  mounts from NBD devices. It is new for single-bootstrap roots, so it gets its
  own test, including a toolkit path the image's merged bootstrap also has a
  parent directory for (`/opt`).
- **Toolkits are plain signed EROFS components**, not RAFS. The signed-artifact
  rules already reserve this: an image's RAFS (or layer) components lead its root
  and rebuild exactly its layers, and "only independently signed toolkits follow"
  (`bind_rafs_layers`, `bind_source_layers`), as whole-image EROFS components in
  `EnvironmentManifest.toolkits`. A toolkit lives in the registry and is served by
  the Python NBD export. That is cheap here: the composition is mounted once per
  node and shared, so its pages cross the export once per node and come from the
  page cache for every later sandbox. RAFS toolkits would need a change to those
  rules; a later option, not needed now.
- **No bootstrap merge:** merging the toolkit into each image's bootstrap would
  build a new bootstrap (up to 128 MB) per (image, toolkit) pair; stacking a
  second lower avoids that.

### 2. A fixed, namespaced path; files only

- Every toolkit file is under `opt/ucloud/toolkits/<name>/`. Prebuilt Python
  environments are not relocatable (absolute interpreter paths, shebangs), so a
  fixed path is required, not a limitation.
- **No environment changes.** A toolkit never edits `PATH` or anything else in
  the sandbox's environment: putting its `bin` first would shadow the task's own
  `python` or `uv` for every command, tests included. The client that asked for
  the toolkit points its own processes at it (decision 5).
- **Validated at publish:** only regular files, directories and symlinks under the
  prefix; no whiteouts or opaque directories (a toolkit must never hide image
  content); no setuid/setgid; a size bound. A toolkit that fails is refused, not
  partially composed.
- **Top of the stack:** toolkit components are the highest lowers, so they win
  only on their own paths, which no image should have.

### 3. Read-only, copy-on-write per sandbox

The composition is a read-only overlay lower; each sandbox writes to its own
upper, as today. `uv` writes lock and cache files even on a cache hit; those land
in the sandbox's upper and never touch the shared layer. Pages of the toolkit are
shared across all sandboxes on a node.

### 4. API

- **SandboxSpec.toolkits:** a list (at most 4) of `name@sha256:<root>` or
  `name:tag`. The gateway resolves tags to digests before placement, and the
  stored spec carries digests only, so a sandbox's composition is reproducible
  across park, wake, migration and retries.
- **Omitted when empty** in the canonical spec, as `environment_root` is, so every
  existing spec fingerprint is unchanged. The daemon's spec codec mirrors it
  byte for byte.
- **Toolkit registry:** `name:tag` → root digest, in the gateway's image roots
  database. `POST /v1/toolkits` publishes one (operator-authenticated), from a
  built environment root; `GET /v1/toolkits` lists them.

### 5. Using it from verifiers

- **verifiers-ucloud** gets `toolkits = [...]` in its runtime config and passes it
  in the sandbox spec. For the harness's processes only, it sets
  `UV_CACHE_DIR=/opt/ucloud/toolkits/<name>/uv-cache`,
  `UV_PYTHON_INSTALL_DIR=/opt/ucloud/toolkits/<name>/python`,
  `UV_OFFLINE=1` and prepends the toolkit's `bin` to `PATH` in those processes'
  environment. Task commands and tests see the image's environment unchanged.
- **verifiers** needs one change: `_ENSURE_UV` always runs
  `pip install -U --user uv` (a network round trip per sandbox, and it can install
  a second `uv` into `~/.local/bin` that shadows ours). It should use a `uv` that
  is already on `PATH` and install only when none is found.
- **Fallback:** if the harness program or its dependencies change, its uv script
  environment hash changes, the prebuilt environment misses, and setup installs
  dynamically as today (slow, correct). `UV_OFFLINE=1` would turn that miss into a
  failure, so verifiers-ucloud sets it only when the toolkit declares the exact
  verifiers version it was built for and that matches the installed one.

## Building the harness toolkit

- **Inputs:** a pinned verifiers commit, its harness program files (the PEP 723
  scripts `prepare_uv_script` runs), a pinned `uv`, and a pinned
  python-build-standalone release.
- **Build:** in a builder container, at the final path
  `/opt/ucloud/toolkits/vf-harness-<version>/`: install `uv` into `bin/`, the
  standalone Python into `python/`, then run exactly verifiers' preparation
  (`uv sync --script <program>` with `UV_CACHE_DIR` set to the toolkit's
  `uv-cache/`) for every harness training uses. Copy the tree into an image
  `FROM scratch` and publish it through the existing environment builder, which
  converts and signs its components.
- **Manifest:** `toolkit.json` at the toolkit root: name, version, verifiers
  commit, uv and Python versions, the script digests it prebuilt, and the libc it
  targets.
- **libc:** python-build-standalone's glibc builds run on glibc images (all of the
  training selection checked so far is Ubuntu or Debian). musl images get a
  separate build or the dynamic fallback; the toolkit's manifest says which.

## Lifecycle and capacity

- **Park, wake, migration:** the spec pins the combined root; a wake or a
  migration leases the same root. Nothing new.
- **Commit and fork:** a commit's parent is the combined root, so a committed
  image keeps the toolkit in its chain. Documented; a later commit can be
  re-composed onto a newer toolkit by the gateway.
- **Capacity:** one composition per (image, toolkit set) per node. With every
  rollout on one toolkit that is the same count as today. Toolkit components
  attach once per node and are shared by every composition. The device budget and
  the idle LRU are unchanged.
- **Placement:** image locality keys stay the image's; a node holding the image's
  components and the toolkit's attaches nothing new.

## What changes, by codebase

| Where | Change | Size |
|---|---|---|
| gateway | compose + sign + publish combined roots; toolkit registry table and endpoints; resolve `toolkits` in the create and group-create spec path | medium |
| Python models / SDK | `SandboxSpec.toolkits` (omitted when empty), validation, docs | small |
| daemon (`registry/spec.rs`) | the field in the spec codec, byte-identical | small |
| builder | the toolkit publish check (prefix, no whiteouts, no setuid, size); toolkits publish as whole-image EROFS components | small |
| toolkit build | a script that builds the verifiers harness toolkit | small |
| verifiers-ucloud | `toolkits` config, harness-process env | small |
| verifiers | use an existing `uv` | tiny |

Nodes' mount, lease and recovery code does not change.

## Gate

- The stub-model dry run at 512 rollouts: setup p50 under 1 s (from 12 s), with
  no PyPI requests from sandboxes, and identical results with and without the
  toolkit.
- Every existing spec fingerprint unchanged (golden tests); Python and Rust spec
  codecs agree with `toolkits` present.
- A toolkit with a whiteout, a path outside its prefix or a setuid file is
  refused at publish.
- Park and wake of a toolkit sandbox; migration to a node that has never seen the
  toolkit.

## Open questions

1. **Does `uv sync --script` on a satisfied cached environment touch the network?**
   **No (measured 2026-10-07).** The bash harness program (sha256 `5ddf3f27…`,
   the file the dry run prepared) prebuilt with uv 0.12.23 and a managed
   Python 3.12 under `/opt/ucloud/toolkits/vf-harness`, then copied into a fresh
   Ubuntu 22.04 image as a layer: with `--network none`, verifiers' preparation
   (`uv sync --script` and `uv python find --script`) succeeds in 110 ms with or
   without `UV_OFFLINE=1`, the imports load and the program starts. The toolkit
   is 216 MB. `UV_PYTHON_PREFERENCE=only-managed` is required: without it uv
   builds the environment on the image's own Python (the dry run's image has a
   Miniconda 3.12), which differs per image.
2. **Toolkit tags across releases:** should a create that names `name:tag` pin the
   digest for the whole training run (the client resolves once at start), or per
   create? Per run is safer for reproducibility.
3. **The managed init and file helper** (C2.5's original items) move into a
   platform toolkit in a second step, deleting the per-create init copy.

## Measured (0.9.51, 2026-10-07)

The stub-model dry run (verifiers `bash` harness, gsm8k, the stub OpenAI server, two
64-vCPU workers), with the toolkit `vf-harness:e914dc9` (the bash and null harnesses
and gsm8k's verifier prebuilt; `uv_toolkit = "vf-harness"`) and without it:

| Rollouts | Setup p50 | p95 | max | Errors |
|---|---|---|---|---|
| 8, no toolkit | about 12 s | | | 0 |
| 8, toolkit (warm node) | 0.6 s | | | 0 |
| 512, no toolkit | **71.8 s** | 105 s | 111 s | 0 |
| 512, toolkit | **4.2 s** | 10.1 s | 16.7 s | 0 |

- **What the toolkit removes:** every sandbox installing uv and its scripts'
  dependencies from PyPI, about a thousand installs at once at 512. In a toolkit
  sandbox, `uv` is the toolkit's and the preparation finds its prebuilt environment
  (0.5 s first time, 0.18 s after, measured in one sandbox).
- **What is left at 512:** each script costs one upload (`runtime.write`, p50 1.3 s
  under the burst, through the Python node agent) and one exec (p50 0.6 s); gsm8k
  prepares two scripts per rollout. Files over the daemon (phase 2d) is the lever.
- **Task scripts count too:** gsm8k's verifier, prepared by the task's own setup,
  took 2 s per rollout until it went into the toolkit. Training's environments will
  have their own; `build.sh` takes them as paths in the verifiers checkout.
- **Not reached:** the setup p50 under 1 s gate holds at 8 rollouts, not at 512.
