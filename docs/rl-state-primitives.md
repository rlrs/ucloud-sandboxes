# RL state primitives: commit, group create, fork, hydration

Status: design only, 2026-10-01, against the uncommitted tree on `90ce959`
(package about 102,850 lines). Nothing here is implemented. It specifies plan
items C3.1 (commit), C3.2 (group create), C3.3 (fork) with C3.4 (template
lifecycle), and C2.7 (hydration) from
[rl-scale-architecture-plan.md](rl-scale-architecture-plan.md).

One deliberate departure from the plan: C3.1 does not run `mkfs.erofs` or
sign on the worker. Workers hold only public trust keys, so a builder converts
and signs (§3.2).

## 1. What this design stands on

| Spike ([evidence](benchmarks/rl-scale-spikes-2026-10-01/README.md)) | Consequence here |
| --- | --- |
| S4: `runsc tar rootfs-upper` exports a running container's upper (7 members) | Commit needs no gVisor patch. |
| S5: one checkpoint restores into several children; `runsc restore` does not consume the image; each child takes its own netns address, keeps in-memory `/tmp`, sees a different `/dev/urandom`; checkpoint 62 ms, restore about 115 ms per child (busybox) | Fork needs no network scheme; the template is reusable. |
| S2: image pages are already shared; +12–15 MiB private per sandbox after a scientific-stack import | Packing and forking save fetched bytes and setup time, not RAM. |
| S1: guest writes live only in the Sentry filestore | The exported upper is exactly the guest's writes. Until C1.2, a checkpoint carries those bytes. |

Code facts the rules below rely on:

| Fact | Where |
| --- | --- |
| `/tmp`, `/run`, `/dev` and `/dev/shm` are OCI tmpfs mounts, never in the upper; only a checkpoint captures them | `DirectOciConfigBuilder._mounts` |
| The node writes `/.ucloud-init`, `/.ucloud-job-init`, the ledger `/.ucloud-managed/state.json`, `etc/resolv.conf` and `etc/hosts` into the rootfs host-side, below the Sentry overlay | `install_init`, `install_managed_init`, `prepare_network_files` |
| The `linux_host` entrypoint writes `~/.ssh/authorized_keys` from the spec and runs `ssh-keygen -A` | `linux_host_entrypoint_script` |
| Relay registration tokens match `^[0-9a-f]{32}$`, and guest agents see them | `model_relay.REGISTRATION_TOKEN_RE` |
| Builders sign EROFS components with Ed25519; workers only verify, against `producers.json`; untrusted OCI diffs are parsed in a keyless subprocess | `environment_artifact.py`, `environment_prepare.py` |
| `resume` consumes a single-owner hibernation generation | `_finalize_restore_artifacts` |
| Patch 0007 clones an immutable same-filesystem memory file without consuming it, and charges the clone its full quota | `runtime/gvisor/README.md` |
| C1.1's in-flight pause keeps the journal RUNNING/LIVE behind a durable marker | `DirectRunscWarden.pause`/`thaw` |
| Sandbox IDs are client-chosen (1–64 characters); routes are fenced by `generation` and `create_operation_id` | `SANDBOX_ID_RE`, `allocate_sandbox_create_with_pending` |

## 2. Shared model

| Record | Owner and store | Authority |
| --- | --- | --- |
| Sandbox route | gateway routing store (`routing.py`, `shared_control/routing_schema.sql`) | Unchanged. Every child, member and seed is an ordinary route with its own generation. |
| `image_commits` (new) | gateway routing store | Commit idempotency (`image_id`) and progress. No execution authority. |
| `sandbox_groups` (new) | gateway routing store | Group and fork request identity, and the hydration plan. Never owns a member. |
| `sandbox_templates` (new) | gateway routing store | Declarative template recipe and `revision`. |
| Export result (new) | worker, `<state_root>/commit-exports/<operation_id>.json` | Replay cache only. |
| Build record | builder `ImageBuildStore` | The conversion job, as for image builds. |
| Template entry (new) | worker `TemplateStore` on the memory filesystem | Node-local cache, never consumed. |
| Hibernation journal | worker Warden | Unchanged. A child starts PARKED from an instantiated generation. |

- **No authority outside routes.** A group, fork or template record never
  authorizes a sandbox, and deleting one never deletes a route.
- **Templates are caches.** Losing one costs latency, never a create (§5.4).
- **Capabilities gate every new request kind.** Workers advertise
  `sandbox-commit-export-v1`, `sandbox-template-v1` and `image-hydrate-v1`;
  builders advertise `image-commit-build-v1`.
- **Readers before writers.** The commit-component reader, the builder's
  `kind: "commit"` reader and the new tables each ship one release before
  any writer.

| Operation | Idempotency key | Replay with a different body |
| --- | --- | --- |
| Commit | `image_id` (client-chosen, like a build `id`) plus `operation_id` | 409 `commit_conflict` |
| Group create, fork | `group_id` | 409 `group_conflict` |
| Hydrate | `hydrate_id` | 409 `hydrate_conflict` |
| Template | `template_id`; each `PUT` increments `revision` | New revision; existing children are unaffected |

Member IDs are `f"{group_id}-{index:04d}"` unless the request gives `ids`, so
`group_id` has at most 59 characters; `count` is at most 1,024. Retries converge
through the existing per-route rule: an existing route with the same spec hash
is recovered, and a different one conflicts.

All the routes below are SDK-public (sandbox API key). Each is a C6.1 use case
on `GatewayServices` that takes an `Exchange` and never imports
`control_plane`. `commit` and `fork` join `http_contract.match_sandbox_http_route`
with `wakes=True`, so the existing implicit wake brings back a hibernated
source first.

| Route | Module |
| --- | --- |
| `POST /v1/sandboxes/{id}/commit`, `GET /v1/images/commits/{image_id}` | `gateway/commit.py` |
| `POST /v1/sandboxes:batch`, `GET`/`DELETE /v1/sandboxes:batch/{group_id}`, `POST /v1/sandboxes/{id}/fork` | `gateway/groups.py` |
| `PUT`/`GET`/`DELETE /v1/templates/{template_id}` | `gateway/templates.py` |
| `POST /v1/images/hydrate`, `GET /v1/images/hydrate/{hydrate_id}` | `gateway/hydrate.py` |

## 3. C3.1 Commit

### 3.1 API

```json
POST /v1/sandboxes/{id}/commit
{"operation_id": "c-7f…", "generation": 3, "image_id": "swe-1234-setup",
 "include_paths": ["/workspace", "/usr/local"], "exclude": ["/workspace/build/tmp"],
 "resume": true}
```

| Field or result | Meaning |
| --- | --- |
| `include_paths` | Optional allowlist of clean absolute guest paths. Only members at or below them survive, with their parents' directory metadata. Whiteouts outside are dropped, so deletions there do not propagate. |
| `exclude` | Prefix rules (no globs) added to the default drop set (§3.3). |
| `resume` | Default true: restore the previous state. A paused sandbox stays paused. |
| Response | `202 {"commit": {"image_id", "state", "sandbox_id", "generation", "build_key"}}` |
| `GET /v1/images/commits/{image_id}` | `exporting`, `staged`, `converting`, `published` or `failed`. At `published` it adds the image record, environment root, byte counts and per-rule drop counts. The `image_id` then works wherever `spec.image` takes a gateway-managed image ID. |
| SDK | `SandboxHandle.commit(image_id, *, include_paths=(), exclude=(), resume=True, wait=True) -> CommitResult`, `Client.get_commit(image_id)`, async twins. One `operation_id` per call, reused on retries. |

### 3.2 Where work runs

```
SDK ─► gateway/commit.py: insert image_commits row            [exporting]
         ├─► worker POST /v1/sandboxes/{id}/commit-export
         │     DirectRunscWarden.export_upper: pause ─► runsc tar rootfs-upper ─► thaw
         │     upload the raw tar as one blob to commits/<sha256(image_id)[:32]>
         │     ◄── {blob_digest, size}                           [staged]
         ├─► _select_builder_node(image_id) ─► builder POST /v1/images/build {"kind": "commit"}
         │     keyless environment_prepare "commit-upper": fetch, verify, commit_policy filter, view
         │     parent: mkfs.erofs (_mkfs flags) ─► sign_commit_component ─► publish  [converting]
         └─► _record_successful_build_image ─► ImageRecord        [published]
```

**Driving.** Every step is asynchronous. The node and builder answer 202 and
are polled. `GET /v1/images/commits/{image_id}` advances the row one step
before answering, and `CommitReconciler` advances rows nobody reads. Each
transition is a compare-and-set on the row's state, so any API process may
drive it.

**Worker.** `DirectRunscWarden.export_upper` holds the Warden lock
throughout, inside the lifecycle lock:

1. After the §8 admission, write the result file as an intent recording
   `was_paused` (a replay keeps the crashed attempt's intent).
2. Unless the runtime is already frozen (a marker alone is not proof), write
   the C1.1 pause marker and run `runsc pause`.
3. Run `runsc tar rootfs-upper` into
   `<state_root>/commit-staging/<operation_id>.tar`.
4. Thaw when `resume` is set and the recorded `was_paused` is false, which
   also undoes a crashed attempt's pause.
5. Upload the tar as one content-addressed blob, delete the tar, then complete
   the result file.

Steps 1–4 run in the request; the upload runs on its own thread once the
locks are released (in the `bg` class once C1.5 lands). The staging
repository hashes `image_id`, which is not always a valid repository name.
The worker parses and signs nothing, and needs only the registry push access
it already has for snapshot publication.

**Builder.** A commit is an ordinary `ImageBuildStore` build. It shares
admission, the deadline (`build_deadline.py`), `GET /v1/images/builds/{key}`,
pending-builder demand and autoscaling with image builds. `ImageBuildSpec`
gains a strict `kind: "build" | "commit"` and a `commit` object:
`{sandbox_id, generation, operation_id, parent_image (digest ref),
parent_root, blob_digest, blob_size, policy, secret_digests}`. Parsing and
filtering happen only in the keyless `environment_prepare` subprocess; the
signing key never leaves the parent process.

**Publication.** `FreshEnvironmentBuilder.publish_commit` publishes, in
order: the EROFS component; the filtered tar as a deterministic gzip OCI
layer (`oci_layer_materialize`, behind Dockerfile `FROM`, reads only tar and
gzip); the environment root (§3.4); and last the OCI manifest under the
managed tag for `image_id`, annotated with the root
(`org.ucloud.immutable-environment.v1`) and provenance
(`org.ucloud.commit.v1`). Every step is idempotent and content-addressed. The
gateway then writes `ImageRecord` and marks the row `published`.

The OCI layer is the durable truth. A `layer_format` change (such as C2.11's
timestamp mode) can regenerate the EROFS component without the sandbox, and
Docker-adapter workers and Dockerfile `FROM` can use the image.

### 3.3 Trust and residue rules

The builder's signature attests a deterministic conversion (these EROFS bytes
come from the filtered tar with diff ID *D*, under policy *P*, on parent root
*R*), not benign content: guest-authored files are as untrusted as a user
Dockerfile's output, and children run under the same isolation. The promise
in `immutable-environments.md` that fresh builds never publish a live
workspace or a path-removed snapshot still holds, because commit is a separate
provenance class with its own schema and signing domain (§3.4). The rules live
in the new pure module `ucloud_sandboxes/commit_policy.py`; the component
records `CommitPolicy.sha256`.

| Rule | Members | Result |
| --- | --- | --- |
| Mount escape | anything at or below `run/ucloud`, `proc`, `sys` or `dev`. These are mounts, so such an entry means the guest bypassed one. | fail `commit_residue_forbidden` |
| Malformed | absolute or `..` paths; non-UTF-8 or NUL; paths over 4,096 bytes; device nodes other than char 0:0 whiteouts; hardlinks to dropped or absent targets; `trusted.*` xattrs other than the opaque marker the filter writes | fail `commit_residue_forbidden` |
| Bounds | more than 1,000,000 members, or more than `commit_max_bytes` (default 8 GiB) after filtering | fail `commit_too_large` |
| Credentials | a 32-hex token, in a member name, link target, kept xattr or regular-file body, whose SHA-256 is in `secret_digests` | fail `commit_secret_residue` |
| Host-written | `.ucloud-init`, `.ucloud-job-init`, `.ucloud-managed/`, `etc/resolv.conf`, `etc/hosts`, `etc/hostname`. The host upper shadows them anyway. | drop, count |
| Identity | `etc/ssh/ssh_host_*`, and the SSH user's `.ssh/authorized_keys` when `spec.ssh.enabled` or `linux_host.enable_sshd` is set. The node sends these paths from the new `DirectOciConfigBuilder.platform_written_paths(spec)`. | drop, count |
| Volatile | other members under `run/`, `tmp/`, `dev/shm/` | drop, count |
| Build residue | `var/cache/apt/archives/*.deb`, `root/.cache/pip/`, `root/.cache/uv/`, `root/.npm/_cacache/` | drop, count |
| Caller | `exclude` prefixes, and anything outside `include_paths` | drop, count |

- **Credentials.** `secret_digests` holds the SHA-256 of every live relay
  registration token bound to the sandbox's rollouts, plus any fixed-format
  class in `commit_policy.TOKEN_CLASSES`. Plaintext never reaches the builder,
  and the scan streams in the keyless subprocess before signing (W7). Secrets
  a user puts in `spec.env` or on disk are user data; the API reference says
  commit captures whatever the guest wrote.
- **Normalization.** Whiteouts become OCI `.wh.` entries and opaque
  directories `.wh..wh..opq`, whichever encoding runsc emits (Q1). Owners,
  modes, `security.capability`, `user.*` xattrs and mtimes are kept (mtimes
  keep `.pyc` valid, C2.11). Members are sorted, so one upper yields one diff
  ID.
- **Out of reach.** Reference answers and encoded credentials cannot be
  detected. Harnesses keep answers out of the sandbox or pass
  `include_paths`. Committed images are private to the deployment, like
  managed builds.

### 3.4 Artifact format

- **Component.** A new `CommitEnvironmentComponent` in
  `environment_artifact.py`: schema `ucloud-environment-erofs-commit-v1`,
  source kind `sandbox-commit-v1`, fields `diff_id`, `parent_root`,
  `policy_sha256`, `format` (`FreshEnvironmentBuilder.layer_format()`, with
  C2.11's timestamp mode), `image_digest`, `image_size`, `chunks`,
  `producer_key`, `signature`. It is signed under its own domain,
  `ucloud.immutable-environment-commit.v1\0`, and `EnvironmentComponent.from_dict`
  dispatches on its schema as it does for v2.
- **Root.** An ordinary `ImmutableEnvironment` whose `source_image` is the new
  OCI config digest, with `environment = EnvironmentManifest(parent.base,
  toolkits=(*parent.toolkits, commit))` (the upper sat above every lower, so
  the commit is topmost) and the parent's `image_config`.
- **Publish check.** `publish_environment(parent_root=R)` requires three
  things: R authenticates; its components are an exact prefix of the new
  list; and the new config's `diff_ids` are R's plus `diff_id`.
- **Worker check.** `bind_source_layers` becomes `bind_components`. It accepts
  commit components only after every other component. For each one, the
  component list of `parent_root` (fetched once, cached by digest) must equal
  the preceding components, so not even a misbehaving builder can splice a
  commit onto another base.
- **Bounds.** `MAX_COMMIT_DEPTH = 8`, within the existing 33-component bound
  (409 `commit_chain_too_deep`). `rootfs_fingerprint` covers the commit, so
  checkpoints and templates of a committed image have their own identity. The
  parent must carry a signed root, as every Hetzner worker image does (else
  409 `commit_requires_environment_root`).

### 3.5 Fencing, crash boundaries, recovery

At insert, the row binds `image_id` to `(sandbox_id, generation,
operation_id, policy_sha256)`, and at `staged` to `blob_digest`; a replay
compares every bound field. The export carries the route generation, which
the worker checks under the Warden lock (409 on a mismatch, or while a park or
delete holds the lifecycle). Commit changes no route state, and a source
deleted after export does not stop the commit: the tar is self-contained.

| Crash or loss | Recovery |
| --- | --- |
| Worker, mid-export | The C1.1 marker keeps the sandbox safely paused. A replay reads `was_paused` from the intent and restores the right state. Any activity also thaws it, because C1.1 thaws before every exec. Startup sweeps `commit-staging/`. |
| Worker, after upload | A replay returns the recorded result without pausing again. |
| Worker lost before `staged` | The row fails with `commit_source_lost`. After `staged`, the worker is no longer needed. |
| Builder | `ImageBuildStore.reconcile_interrupted` fails the job. `CommitReconciler`, run from the existing pending-build loop (no new thread), re-dispatches `staged` or `converting` rows older than the build deadline, up to three times. Outputs are byte-identical. |
| Gateway | The SDK retries with the same `operation_id`; the row's state drives the replay. |

A `RegistryUsageStore` owner lease `commit-staging:<image_id>` protects the
staging blob until the row is `published` or `failed`; as elsewhere, an
uncertain release keeps the data. Published images are protected like managed
builds. A create naming an unpublished `image_id` gets the existing retryable
503 for a pending managed image.

## 4. C3.2 Group create

### 4.1 API

```json
POST /v1/sandboxes:batch
{"group_id": "task-0042", "count": 8, "ids": null, "template": null,
 "spec": {"image": "swe-1234-setup", "cpus": 1, "memory_mb": 1024, "parkable": true},
 "placement": "pack", "wait": true}
```

`spec` is a `SandboxSpec` without `id`; with `template` (§5.3) it may be
omitted and is inherited. `toolkits` is accepted once C2.5 adds
`SandboxSpec.toolkits`. The response is:

```json
{"group_id": "…",
 "members": [{"sandbox_id", "state", "generation", "node_id", "route", "source"}],
 "placement": {"nodes": [{"node_id", "members"}], "overflow", "pending"}}
```

- `state` is `running`, `creating`, `pending` or `failed`; `route` is the C4.1
  token once tokens exist; `source` is `image`, `template` or `image+setup`.
- With `wait` (default true) the call returns once every member is running,
  failed or pending; with `wait: false`, right after placement. The status is
  201 when every member runs, else 202. Repeating the request converges the
  rest.
- `GET /v1/sandboxes:batch/{group_id}` reads member routes by derived ID, and
  `DELETE` issues ordinary per-member deletes.
- SDK: `Client.create_group(spec, count, *, group_id=None, template=None,
  placement="pack", ids=None) -> SandboxGroup`, with
  `SandboxGroup.wait_ready()`.

### 4.2 Placement

One decision per group, made by a pure `plan_group(members, candidates, *,
mode, budget, k, rng)`. It moves into C6.1's `gateway/placement.py` seam.

- **Candidates.** A `Candidate` is a heartbeat plus available resources after
  the in-flight overlay (`_node_placement_state`,
  `InflightCreatePlacements.adjusted`), pressure, a residency bonus (C2.8
  missing bytes for the image) and template presence. Before C4.3 they come
  from the existing full scan in `Placement.select`. After it, they come from
  C4.3's power-of-k sample (k = 3), plus the group's hydration-planned nodes
  and the best node already holding the image or template. Group code is the
  same either way.
- **Budget.** `B = min(fit, group_max_members_per_node,
  2 × create_target_concurrency_per_node)`. `fit` is the number of members of
  this shape that still fit, and `group_max_members_per_node` defaults to 32.
  The concurrency term keeps a pack to about two startup waves per node.
- **`pack`.** Take the candidate that maximizes `(min(fit, B, remaining),
  residency, −pressure)`, assign that many members, drop the node, and repeat
  for the overflow.
- **`spread`.** Give each sampled candidate `ceil(remaining / |S|)`, capped by
  `fit` and `B`. This mode is for burst-sensitive tests.
- **Claims.** Each assignment is claimed in the overlay
  (`InflightCreatePlacements.claim`) before any node call, so concurrent
  groups in one process see each other. Other processes see them through
  heartbeats, which is C4.3's premise.
- **Leftovers.** Members nothing fits become demand
  (`upsert_pending_with_demand`) and are returned as `pending`.

### 4.3 Dispatch, fencing, recovery

- **Routes.** Each member is an ordinary route: today through
  `allocate_sandbox_create_with_pending`; under C4.3 through one
  `INSERT … ON CONFLICT DO NOTHING` after the node accepts. The group row
  holds only the request digest (`spec_sha256`, count, template, placement)
  and the plan.
- **One call per node.** The gateway sends node `POST /v1/sandboxes:batch`
  with `{"members": [{spec, generation, create_operation_id, spec_hash,
  source, setup?}]}`. `DirectSandboxService.create_batch` admits each member
  on the normal path: the startup slot, `_reserve_active_capacity`, and
  C5.3's admission function once it exists. It runs members concurrently
  within those slots and returns a status per member: 201 with the record,
  409, or 503 with a reason.
- **The node is final.** A rejected member's route is removed with
  `delete_sandbox_if_current`, and the member re-enters `plan_group`
  excluding that node, for at most four rounds. An ambiguous member (a
  timeout, or 5xx after dispatch) keeps its route and resolves through
  `_retry_sandbox_create_on_assigned_node`. Rejects are counted, not hidden;
  nothing is overbooked.
- **Images and recovery.** Each (node, image) pair is ensured once through the
  existing `_ensure_image_for_create` single-flight (normally a no-op after
  C2.7). A gateway crash leaves some members routed and some absent; a replay
  recovers the routed ones and places the rest. A crash between route
  allocation and dispatch is the existing `creating` case.

## 5. C3.3 Fork and C3.4 templates

### 5.1 API

```json
POST /v1/sandboxes/{id}/fork
{"operation_id": "f-…", "generation": 3, "group_id": "roll-17", "count": 16,
 "min_count": 16, "source_after": "running", "allow_open_connections": false}
```

Children run on the source's node only: S5 covered one host, and the
template is node-local. The response is the group response with `source:
"template"`. The node creates between `min_count` and `count` children, or
refuses with 503 `fork_capacity_unavailable`. `source_after` is `running`,
`parked` or `deleted`. SDK: `SandboxHandle.fork(count, *, group_id=None,
min_count=None, source_after="running") -> list[SandboxHandle]`.

Declarative templates make setup run once per node, or once overall:

```json
PUT /v1/templates/{template_id}
{"kind": "memory", "spec": {"image": "…", "cpus": 1, "memory_mb": 512},
 "setup": {"command": ["bash", "-lc", "pip install -e . && python -c 'import pkg'"],
           "timeout_seconds": 600}}
```

| `kind` | Setup runs | Children |
| --- | --- | --- |
| `filesystem` | once, then a commit (§3) | Ordinary creates from the committed image. They survive runtime upgrades and run on any node. |
| `memory` | once per node, then a capture | Restored with running processes and warm interpreter state. |

SDK: `Client.put_template(template_id, kind, spec, setup)`.

### 5.2 Node mechanics

**Source checks.** The node refuses with 409 `template_source_busy` if the
source has an exec session or managed job (a ledger other than
`managed-primary-v1:no-job`), an SSH session, a non-terminal relay program
request (checked at the gateway against `program_requests`), or an
established TCP connection in `/proc/net/tcp*` (one file-helper exec) without
`allow_open_connections`. LISTEN sockets are fine.

**Capture** (internal node `POST /v1/sandboxes/{id}/template`):

1. A RUNNING source is hibernated by the unchanged `DirectRunscWarden.park`.
2. `TemplateStore.capture(key, generation_dir)`, in the new
   `node_templates.py`, reflinks the kernel-state and pages image and
   `application_memory.img` into `<memory_root>/templates/<key>.pending/`. It
   writes a `TemplateManifest` (key, runtime fingerprint,
   `template_spec_sha256`, file inventory, allocated bytes, source identity,
   created and last-used times), fsyncs, renames the directory to `<key>/`,
   and writes `COMPLETE`.
3. With `source_after=running`, `resume` runs. Its finalize deletes only the
   source's own generation; the reflinks are separate inodes.

A parked source skips steps 1 and 3. The source's route and generation never
change. A crash between steps 1 and 3 leaves the source PARKED, which implicit
wake and heartbeat reconciliation already handle.

**Instantiate.** A node batch member with `source: {"template_key"}` is a
local parked import followed by an ordinary wake:

1. `DirectProvisioner.create_from_template` follows `create` with the new
   registry phase `template_planned`. It allocates quota and memory, builds
   the bundle and rootfs, and makes a fresh netns with `_network_namespace`
   (not `_migration_network_namespace`: per S5 the child takes a new
   address). It does not run `runsc create`.
2. `TemplateStore.instantiate(key, child)`, under a shared `flock`, requires
   the template's runtime fingerprint to equal `_runtime_fingerprint(child)`
   (any difference is a key miss). It reflinks the files into the child's
   allocation as `hibernate-1/`, and writes a version-3 `HibernationManifest`
   bound to the child's `sandbox_id`, generation, `spec_sha256`,
   `container_id` and memory/workspace references.
3. `HibernationJournal.initialize_parked(manifest)`, then registry phase
   `owned`.
4. The unchanged `DirectRunscWarden.resume`, with reflink restore and the
   paused candidate handoff. For children, one identity exec (§7) writes
   `/run/ucloud/fork.json` and sets the hostname. It replaces the readiness
   exec, so it costs no extra round trip.

Children restore concurrently within `_restore_slot`. An ad hoc fork template
is deleted when the fork completes, because its source has moved on.

**Crash boundaries.** `<key>.pending` is deleted when the store starts;
`COMPLETE` is the only commit point. A child in `template_planned` with no
COMPLETE generation rolls back like a failed create, and its route stays
`creating` until a fork replay or complete-inventory reconciliation. A child
past `initialize_parked` is a parked sandbox for the existing reconciler.

**Before C1.2.** The filestore is serialized in the pages image, so each
child's restore copies the source's written bytes.
`template_max_filestore_mb` (default 256) caps that cost; above it, the fork
fails with 409 `template_filestore_too_large`, and a filesystem template is
the right tool. After C1.2 the filestore is reflinked like memory, and the
cap goes away.

### 5.3 Declarative templates in group create

**Filesystem templates.** If the template row has a published
`filesystem_image_id`, members are created from it. Otherwise members are
created from `spec.image` plus `setup` (`source: image+setup`), and the
gateway starts, once, a routed seed `tpl-<sha256(template_id,
revision)[:24]>` with `ttl_seconds`. The seed runs setup, is committed, and
is deleted. The `image_commits` row for `tpl-<template_id>-r<revision>` makes
this single-flight.

**Memory templates.** The planner prefers nodes whose heartbeat lists
`(template_id, revision)`. Each batch carries `{"template": {template_id,
revision, template_spec_sha256, setup_sha256}, "fallback": {"setup": …}}`.
The node computes the full key: on a hit it instantiates, and on a miss it
creates from the image and runs setup. The gateway then builds that node's
template in the background (a routed seed on the node, setup, capture with
`source_after=deleted`). `template_wait_seconds` (default 0) trades
first-group latency for hits, and hydration (§6) builds templates during the
previous batch.

**Setup on create.** Setup runs on the node inside create, after `runsc
start` and before readiness, through `DirectSandboxService.exec`. A non-zero
exit fails that member with `setup_failed` (not retryable) and deletes it.
Seeds use the same code.

### 5.4 Template lifecycle

The key is computed on the node, so the gateway never needs the runtime
fingerprint:

```
sha256(canonical({"runtime": HibernationRuntimeFingerprint.digest,
                  "spec": template_spec_sha256, "setup": setup_sha256,
                  "revision": n, "memory_mode": mode}))
```

The runtime digest covers runsc and its companions, platform, CPU features,
boot config and `rootfs_sha256` (backend ABI and ordered components).
`template_spec_sha256` is `sandbox_spec_fingerprint` with `id` and `labels`
emptied; children must match on everything else, including `env` (baked into
running processes), shape, network, `managed_process` and mounts. An ad hoc
fork key uses the source's `(sandbox_id, generation, hibernation_generation)`
in place of setup and revision.

| Concern | Rule |
| --- | --- |
| LRU | Each entry records `last_instantiated_ns`. `TemplateStore.evict` removes least recently instantiated, unleased, COMPLETE entries while the cache exceeds `direct_template_cache_mb` (default: the smaller of 32 GiB and 5% of the memory filesystem) or the memory root is below its free-space floor. The shared `flock` held during instantiation pins an entry. |
| Runtime upgrades | At start, the store deletes every entry whose runtime digest differs from the node's. An upgrade drops all memory templates; filesystem templates are images and survive. |
| Rebuild | Memory templates are rebuilt from (image, spec, setup). Filesystem templates are re-committed; a committed image that is gone or fails to resolve sends members to image plus setup and starts a recommit. Ad hoc fork templates are not rebuildable, and no create depends on them. |
| Never fail a create | A missing, evicted, stale or failing template changes only the member's `source` and `ucloud.template.cache.lookups{outcome}`. The only template-related member failure is `setup_failed`, which a plain image-plus-setup create would hit too. |
| Heartbeat | `templates: [{template_id, revision, bytes}]`, at most 256 entries: a placement hint, not authority. |

## 6. C2.7 Hydration

```json
POST /v1/images/hydrate
{"hydrate_id": "batch-0018", "deadline_seconds": 300,
 "groups": [{"group_id": "task-0042", "image": "swe-1234", "toolkits": [],
             "template": null, "expected_sandboxes": 8, "placement": "pack"}]}
```

| Part | Behavior |
| --- | --- |
| Plan | `gateway/hydrate.py` runs the same `plan_group` with `expected_sandboxes`, and upserts each group's `sandbox_groups` row as `planned` with the node IDs and `expires_at = now + deadline_seconds`. A later `create_group` with that `group_id` and image puts those nodes first; a changed image or full nodes mean normal placement. A plan never causes an error. |
| No reservation | Hydration reserves no capacity, writes no route and creates no demand. That separates it from `/v1/capacity/prepare`, which keeps only its capacity half. |
| Response | 202 with the plan. `GET /v1/images/hydrate/{id}` reports per-node progress from node replies and the C2.8 residency summary. |
| Node | `POST /v1/images/hydrate` takes `{"images": [{"image", "components"}], "budget_bytes", "deadline_unix"}`. The node resolves the signed root and calls the new `EnvironmentBackend.hydrate(digests)`: `_start_prefetch` (C2.2 metadata hint plus C2.3 trace) into the chunk cache, with no device or mount attached. It runs in the `bg` class (C1.5), bounded by `PrefetchPolicy` and a quarter of the cache. Docker-adapter workers use the existing pull path under `pull_slot` until C2.9. |
| Templates | A group with `template` also has the gateway build the memory template on each planned node (§5.3), so batch *k+1* restores while batch *k* runs. |
| Store tier | Chunks come from the registry until C2.6; after it, the store tier fills from S3 first, so hydrated batches take no S3 demand faults. Shared traces replace `LocalTraceStore` through the seam in `environment_trace.py`. |
| SDK | `Client.hydrate(groups, *, deadline_seconds=300, hydrate_id=None) -> HydrationHandle`, with `.wait(timeout)`, called for batch *k+1* while batch *k* runs. |

C2.7 **deletes** the 503 `image_warmup_pending` path in
`_create_sandbox_on_node_locked` and the warmup half of
`/v1/capacity/prepare`: in `control_plane.py` (about 310 lines)
`_schedule_image_warmups`, `_active_image_warmup_for_image`,
`_schedule_image_warmup`, `_start_image_warmup_task`,
`_warm_image_on_ready_nodes`, `_run_image_warmup_task`,
`_warmup_node_units`; in `routing.py` (about 230 lines) `PendingImageWarmup`
and the `image_warmups` table and methods, with their metrics and retention
references. The SDK's `prepare_capacity(image=…)` keeps working by issuing a
hydration.

## 7. Identity hazards

| Hazard | Commit | Fork and memory templates | Treatment |
| --- | --- | --- | --- |
| User-space RNG (Python `random`, NumPy generators seeded at import) | not captured | **cloned**; correlated rollouts bias advantages | Documented, with a seed provided. Each child gets `/run/ucloud/fork.json` = `{"schema": "ucloud-fork-v1", "sandbox_id", "group_id", "index", "count", "seed", "restored_at_ns"}`; `seed` is 32 bytes from the node's `os.urandom`. Harnesses must reseed from it. Capture templates when only PID 1 and idle servers are alive. |
| Kernel RNG, guest IP | n/a | fresh per child (S5) | Qualified (Q3) |
| Clocks | n/a | realtime jumps to now; monotonic continues from the checkpoint; pending timers fire late | Documented |
| Hostname | n/a | the source's UTS name is restored | Enforced: the identity exec sets the child's ID |
| SSH host keys, `authorized_keys` | dropped (§3.3) | a running sshd holds the source's keys | Documented; live SSH sessions refused |
| Open TCP connections | n/a | every child would own the same peer connection | Enforced: refused unless `allow_open_connections`; LISTEN allowed |
| Exec sessions, managed jobs | n/a | the host side is gone after restore | Enforced: refused |
| Relay registration | the token scan fails the commit | every child would answer as one rollout | Enforced: non-terminal program requests refused |
| `/etc/machine-id` and similar | captured if written | cloned | Documented; `exclude` it if it matters |

`/run/ucloud/` lives on the `/run` tmpfs, so it is never in an upper, and §3.3
fails any commit where it shows up.

## 8. Quotas and disk accounting

| Where | Accounting |
| --- | --- |
| Worker commit | Before pausing, reserve `_filestore_bytes(sandbox) + 64 MiB` of scratch on the staging filesystem (`statvfs` of the state root less in-flight exports, keeping 1 GiB free; the physical-disk ledger accounts the memory filesystem, where the tar does not live); otherwise 503 `commit_capacity_unavailable` (retryable). A filestore over `commit_max_bytes` gets 413 `commit_too_large` before any pause. The upload runs in the `bg` class once C1.5 lands. |
| Builder commit | One `ImageBuildStore.reserve_build` slot; scratch of about three times the filtered tar (tar, view, EROFS) under the build deadline. Refused under registry disk pressure (`_write_registry_disk_pressure`), like builds. Published commits count in `RegistryUsageStore` like builds; both the gzip OCI layer and the EROFS component are stored. |
| Groups | Every member is a full claim, with no group discount. Pending members are ordinary autoscaler demand. |
| Template entry | Its own exact-quota retention project on the memory filesystem, like a retained checkpoint (`MemoryBackingStore.retain_checkpoint`). Admitted through the physical-capacity ledger before capture; counted once, at allocated bytes, against `direct_template_cache_mb`. |
| Fork child | Full memory allocation and workspace claim. Copy-on-write divergence can reach full size, so shared extents are not free quota (patch 0007's rule). Sharing shows up as physical free space, never as admitted capacity. |
| Hydration | Bounded by `budget_bytes` and a quarter of the chunk cache; disposable cache bytes under the existing ceiling. |

## 9. Metrics

Telemetry invariants hold: labels are only operation and outcome, and there
are no per-sandbox series. Detail goes to spans and `metrics_store` events.

| Kind | Name |
| --- | --- |
| Operation histograms (`ucloud.platform.operation.duration`, `.count`) | `sandbox.commit` (phase spans `commit.pause`, `commit.export`, `commit.upload`, `commit.filter`, `commit.mkfs`, `commit.publish`); `sandbox.group_create` (`group.plan`, `group.dispatch`); `sandbox.fork`; `template.capture`; `template.instantiate`; `images.hydrate`; `node.images.hydrate` |
| Counters | `ucloud.commit.bytes{kind=exported\|filtered\|erofs\|oci}`; `ucloud.commit.dropped_members{rule}`; `ucloud.group.members{outcome=placed\|overflowed\|rejected\|pending}`; `ucloud.template.cache.lookups{outcome=hit\|miss\|fallback}`; `ucloud.template.cache.evictions{reason=lru\|runtime\|floor}`; `ucloud.hydrate.bytes{kind=metadata\|trace}`; `ucloud.hydrate.create_hits{outcome=resident\|missing}` (attach-time cache state) |
| Gauges, via heartbeat | `ucloud.template.cache.bytes`, `ucloud.template.cache.entries` |
| Events | `sandbox_commit` (image, sandbox, generation, bytes, drops, outcome); `group_placement` (group, nodes, overflow, rejects); `sandbox_fork` (group, source, children, capture and restore ms) |
| Benchmark (`scripts/bench_rl_scale.py`) | `fork` stops being an `unsupported` placeholder; new `commit_recreate` (commit, then first command on another node); `burst --group` (fetched bytes per node from registry counts) |

## 10. Test plan

### 10.1 Unit tests

| Unit | Cases |
| --- | --- |
| `commit_policy` | Every row of the §3.3 table; both whiteout encodings; symlinks kept, never followed; `include_paths` with whiteouts outside it; a diff ID stable across member orders; a token split across read chunks |
| `CommitEnvironmentComponent` | Sign and verify; schema dispatch; a v2 signature never verifies a commit component; `bind_components` refuses a splice onto another parent |
| `plan_group` (property tests) | Never more than `fit` or `B` per node; `pack` uses the fewest nodes and `spread` balances; claims visible to later plans; placed + overflowed + pending = `count` |
| `TemplateStore` | Pending-directory recovery; `COMPLETE` as the commit point; a leased entry survives eviction; LRU order; the runtime sweep; quota and floor accounting |

### 10.2 Local fleet tests (`tests/test_local_fleet.py`)

Harness additions: the fake runsc gains `tar rootfs-upper` (the overlay upper
with OCI whiteouts), and its restores read only the child's private copy, so
a template is never consumed. `LocalFleet(builders=1)` adds the existing
in-memory test registry and a builder agent with a fake `mkfs.erofs`.
Committed images resolve through `LocalRootfsStore` from their OCI layers.

| Scenario | Asserts |
| --- | --- |
| `commit_round_trip` | Write kept and dropped paths, commit, then create from the image on the other node. Kept files are present; dropped files, `/tmp` and `/run` are absent; drop counts are reported. |
| `commit_replay_and_crash` | After an agent restart mid-export, a replay returns the same digest and the sandbox is not left paused. After a builder restart, `CommitReconciler` finishes the commit. |
| `commit_residue_refused` | With a live registration token in a file, or a `run/ucloud` escape, the commit fails, no tag exists, and the staging lease is released. |
| `group_pack_overflow` | Two nodes, `B = 3`, `count = 5`: 3 + 2 placed. A replay creates no duplicates; a changed spec gets 409. |
| `group_node_rejection` | When a node rejects one member, the member is re-placed and the reject is counted. |
| `fork_children` | Four children have distinct seeds and hostnames and inherit `/tmp`. The source keeps running, and deleting it leaves the children intact. |
| `fork_crash_recovery` | After an agent restart mid-instantiate, only running children or cleaned routes remain. The template is unconsumed and there are no orphan registrations. |
| `template_fallback_never_fails` | When capture fails, members come from image plus setup, with `source = image+setup`. |
| `template_runtime_change` | After the fake runtime hash changes, the key misses, the template is rebuilt, and the old entry is swept. |
| `hydrate_then_group` | `create_group` lands on the planned nodes with zero pulls at create (fake pull counter). |

### 10.3 VM qualification (disposable CPX62, as for the spikes)

`runtime/gvisor/qualify_state_primitives.py` joins the C8.6 lane and writes
one JSON report.

| ID | Check |
| --- | --- |
| Q1 | `runsc tar rootfs-upper` under `runsc pause`. Whiteout and opaque encoding. Xattrs, hardlinks and copy-up through commit → EROFS, using the `qualify_environment.py` filesystem checks. |
| Q2 | C3.1 gate: a committed SWE-smith setup re-creates in under 1 s on another node. |
| Q3 | C3.3 gate: 16 children reach their first command in ≤ 500 ms (512 MiB / 128 MiB), with distinct seeds, IPs, kernel RNG and hostnames. The template is byte-identical after 16 reflink restores. |
| Q4 | A source with established TCP is refused; a LISTEN-only source forks. |
| Q5 | C3.2 gate: 1,024 sandboxes over 128 images on 3 nodes are all ready in ≤ 30 s, and each image is fetched at most once per node (registry logs). |
| Q6 | C2.7 gate: a hydrated batch's cold time to first command equals the warm numbers. |

## 11. Implementation order

The phases respect the in-flight worktrees: C2.11 (`environment_builder`,
`environment_artifact`), C1.1 (`direct_warden`, `direct_service`,
`node_runtime`, `vm_init`, `config`), C8.1/C8.3 (tests and CI) and C6.1
(`control_plane.py`, `gateway/`). Each phase is one PR with its own tests and
line ledger. The estimates count package lines only, not tests or the SDK.

| Phase | Waits for | Contents | Est. lines |
| --- | --- | --- | ---: |
| P0 | nothing | New pure modules `commit_policy.py` and `node_templates.py`; `plan_group` as a pure function; schema-only readers for `image_commits`, `sandbox_groups`, `sandbox_templates` in `routing.py` and `routing_schema.sql`; fake runsc `tar rootfs-upper` | +900 |
| P1 | C2.11 merged | Readers: `CommitEnvironmentComponent`, `bind_components`, `publish_environment(parent_root=)`. Builders accept `kind: "commit"`. | +250 |
| P2 | C6.1 `image_distribution` seam | C2.7: `gateway/hydrate.py`, node `/v1/images/hydrate`, `EnvironmentBackend.hydrate`, SDK `hydrate`. **Deletes the warmup machinery.** | +350 / −600 |
| P3 | C1.1 merged, C6.1 `lifecycle` seam | C3.1: `export_upper`, node `commit-export`, `publish_commit`, the `commit-upper` preparation request, `gateway/commit.py`, `CommitReconciler`, SDK `commit` | +700 |
| P4 | C6.1 `placement` and `create` seams | C3.2: `gateway/groups.py`, node batch and `create_batch`, setup on create, SDK `create_group`. `plan_group` moves into `gateway/placement.py` and adopts C4.3's sampler when it lands. | +650 |
| P5 | P3, P4 | C3.3/C3.4: capture, `create_from_template`, identity exec, `gateway/templates.py`, the fork route, heartbeat `templates` | +600 |
| P6 | C1.2 | Fork v2: reflink the filestore and drop `template_max_filestore_mb` | +80 / −40 |

The total is about +3,500 / −640 lines, a net of about +2,900. That does not
fit the 103,000-line budget, which has about 150 lines of headroom. Either
P2's deletion lands first and P3–P5 wait for a deletion tranche (C4.1/C4.3,
or C1.1's `warm_park` shrink), or the PR that raises the budget explains why. Commit (C3.1) and
toolkits (C2.5) are the plan's levers for retiring offline factoring (C2.10);
this tranche does not delete it.

## 12. Open questions

- **Q1 outcome.** Does `runsc tar rootfs-upper` work while paused? If not,
  export runs live and promises only a crash-consistent upper.
- **UTS on restore.** Does gVisor restore the UTS name from the checkpoint or
  from the bundle? If the bundle, the hostname step is unnecessary.
- **Partial forks.** Should `min_count < count` be allowed, or should fork be
  all-or-nothing?
- **Cross-deployment sharing.** Should commits be shareable across
  deployments? That needs a residue review beyond "private to the
  deployment".
