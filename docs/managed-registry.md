# Managed Registry

The deployment can host a private Docker registry for builder output. This can
run on the public control-plane VM or on a dedicated VM attached to the same
private network. The live deployment uses the all-in-one control-plane VM:
gateway, relay, registry, registry GC, and autoscaler run together, and registry
storage is backed by the mounted project drive. Current live job and address
details are maintained in [deployment-flow.md](deployment-flow.md):

1. Clients submit a managed build with a stable image id and no registry
   coordinates.
2. The gateway assigns a worker-private tag, builders push it, and the gateway
   records its manifest digest under the image id.
3. Sandbox nodes receive a gateway-resolved, digest-pinned pull reference.

The registry is a standard Docker Distribution container. Back it with an
explicit UCloud project storage path mounted into the gateway VM. UCloud mounts
the project drive under `/work/<drive-title>`, so the registry data path should
be below that mount, not on the VM root disk or an incidental `/work`
directory.

Submit or replace the registry-capable all-in-one VM with the project drive
attached. The validated DFM Pretraining deployment mounts drive `/998037`,
whose title is `data`, so it appears inside the VM as `/work/data`:

```bash
ucloud-sandboxes submit-vm \
  --config /path/to/deployment.json \
  --role gateway \
  --mount /<drive-id> \
  ...
```

## Gateway Service

The normal path is `deploy-all-in-one`; it installs Docker and the exact
`/etc/ucloud-sandboxes/deployment.json`, installs the packaged registry and GC
systemd units, and starts the registry:

```bash
uv run ucloud-sandboxes deploy-all-in-one <job-id> \
  --config /path/to/deployment.json \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
  --direct-runsc /path/to/ucloud-direct-runsc \
  --managed-init /path/to/managed-init \
  --storage-native-manifest /path/to/storage-native-manifest.json \
  --execute
```

Verify on the VM:

```bash
curl -fsS http://127.0.0.1:5000/v2/_catalog
```

For the first UCloud deployment, HTTP on the private network is acceptable if
builder and sandbox nodes are initialized with Docker trust for the private
registry host. For a wider network boundary, put TLS and authentication in front
of the registry instead of using Docker's insecure registry setting.

Do not bind the registry to a UCloud public link in the default deployment. The
registry is an internal control-plane service for builders and sandbox nodes on
the private network.

The deployment manifest fixes the loopback registry URL from `registry_port`.
This enables the dashboard registry
page and the `/v1/registry` status endpoint without exposing the registry itself
publicly. The derived worker registry URL separately names the same service as
builders and sandbox nodes reach it. That URL is gateway deployment state;
clients never need its hostname or port.

The current registry service will survive container and service restarts as
long as the manifest-derived registry data directory is on the mounted project folder. A
control-plane VM replacement must attach the same project drive before starting
the registry, otherwise it will start with an empty registry.

Raw registry blobs and gateway image metadata both live on the project drive in
the updated deployment. Registry blobs remain under
`/work/data/ucloud-sandbox-registry/docker-registry`; gateway state, including
the image-id index and registry-use records, lives under
`/work/data/ucloud-sandboxes/state`.

The deployment creates gateway state directly under
`/work/data/ucloud-sandboxes/state`. It stages a refreshed UCloud session
separately so session copying cannot make an uninitialized target appear ready.

Generated systemd drop-ins use `RequiresMountsFor=/work/data` and a `mountpoint`
preflight. Without those gates, a boot can race the project-drive mount and bind
a hidden directory on the VM root disk. The inspected 2026-07-12 boot mounted
`/work/data` two seconds before starting the registry, so that race did not
cause the observed index loss. Deployed version 0.3.48 still lacks the gates
until this change is released and converged.

## Node Init

Builders and sandbox nodes must trust the private registry if it is served over
HTTP. VM initialization derives the restart-stable registry endpoint and any
private-IP host alias from `deployment.json`; there is no second init or
autoscaler override.

The init script writes Docker's `insecure-registries` daemon setting and
restarts Docker before starting the node agent. UCloud's restart-stable private
DNS name avoids per-node `/etc/hosts` state when the gateway's private IP
changes. Image ids resolve to the immutable registry digest recorded by the
control plane; worker registry coordinates are never supplied by clients.

The same init script configures Docker's bridge MTU from the VM default-route
interface. This matters on UCloud private-network VMs where the host interface
MTU can be lower than Docker's default `1500`; without this, large HTTPS
responses during `docker build` or registry pulls can stall even though host
networking works.

## Build And Run

Use a stable gateway image id; do not provide registry coordinates:

```python
from ucloud_sandboxes_sdk import Image

client.build_image(
    Image.from_dockerfile(
        name="mini-swe-python311",
        context_path="./build-context",
    )
)
```

Then create sandboxes with the same image id:

```python
client.create_sandbox(
    id="sample-1",
    image=Image.from_name("mini-swe-python311"),
    cpus=1,
    memory_mb=2048,
    disk_mb=10240,
)
```

The gateway resolves the image id to its recorded digest-pinned internal
reference. Managed SDK builds are always pushed because builder-local Docker
images are not durable and are not copied to sandbox nodes. Explicit registry
tags remain supported for external and administrative flows.

## Cleanup

The all-in-one deployment installs `ucloud-sandbox-registry-prune.timer`.
By default it runs hourly, deletes image tags whose last recorded sandbox use
is older than 30 days, and keeps no per-repository floor. The zero keep floor is
deliberate: many generated build repositories have only one tag, so a keep
floor would prevent those images from ever becoming eligible for cleanup.

The gateway records successful sandbox creation and idempotent create recovery
in `<data_root>/registry-usage.sqlite`. Scheduled pruning uses that database as the
age source. Tags with no usage entry are kept, because deleting by image
creation time can remove shared base images that are still actively used.

The same database persists both durable image references and finite transient
leases. Every lease requires the exact immutable manifest digest; repository,
tag, and owner identify its lifecycle, while pruning protects only the digest.
Durable references have no expiry; transient pull and warmup leases retain an
expiry. The schema is strict: canonical snake-case fields, `generation`,
and digest-bearing leases are required.

Every resolved managed digest also has a deterministic internal
`ucloud-digest-sha256-<hex>` tag. The gateway creates it by copying the exact
manifest media type and bytes under that name. This keeps a pinned digest
reachable by the blob sweep and Distribution garbage collection after its user-facing tag
moves. Internal tags are hidden from registry summaries, and retention floors
count distinct digests rather than tag aliases.

Sandbox routes acquire a durable reference before image pull/create dispatch.
The owner is deterministic across restart and includes the persisted route
generation and deployment, sandbox, node/job, creation-time, and image
identities. A matching successful node delete releases it. Timeout, non-2xx
delete, ambiguous create, or control-plane outage retains it. This deliberately
prefers a safe leaked reference over an expired live reference; an explicit
reconciliation command may later remove references whose route/runtime absence
has been proven.

Pushed builds acquire a distinct durable reference before local Docker
build/push or remote builder dispatch. Synchronous terminal completion releases
the exact reference. An ambiguous or asynchronous accepted build retains it;
after a gateway crash this can leak until explicit terminal reconciliation, but
it cannot expire underneath an active build. There is no renewal thread or
separate build-lease SQLite database.

Explicit pull and warmup operations remain transient and use finite leases:

```python
reference = usage_store.acquire_reference(repository, tag, owner)
lease = usage_store.acquire_lease(
    repository,
    tag,
    transient_owner,
    ttl_seconds=180,
)
usage_store.release_lease(repository, tag, owner)
```

Prune planning protects the complete digest when any tag alias has either a
persistent reference or an unexpired transient lease. Execution revalidates the
protection immediately before each digest deletion and holds the usage-file
lock through that bounded registry `DELETE`. Callers must persist protection
before dispatch and still verify that the subsequent pull/push succeeds; the
local store cannot fence registry clients that bypass it.

The prune command now takes one `usage_store.snapshot()`, passes
`snapshot.records`, `snapshot.leases`, and `snapshot.generation` into planning,
retains the complete registry tag list, then calls `execute_registry_prune` with
`usage_store`, `all_records`, and `expected_usage_generation`. A generation
change aborts that stale execution and rebuilds the plan, with a bounded retry
limit, rather than continuing to delete from the old snapshot.

The prune service also receives `<data_root>/images.sqlite`. When it deletes a
private-registry manifest, it removes matching pushed build records from that
image metadata cache. It also prunes stale pushed build records whose manifests
are already missing. This matters for SDK clients because `list_images()` is
used as the build cache signal; stale metadata must not make a deleted image
look reusable.

Prune, blob sweep, and pressure cleanup share one maintenance lock
(`/run/lock/ucloud-sandbox-registry-maintenance.lock`). Each waits up to 30
minutes for the others rather than failing.

Tune `registry_retention_days` and `registry_keep_per_repository` in
`deployment.json`, then converge the deployment.

### Reference retention

The age rules above cover image repositories only. Two repositories are
retained by reference instead (`ucloud_sandboxes/registry_retention.py`):

- the snapshot repository (`sandbox.storage_native_repository`, with the
  `registry` snapshot store): a manifest is live while a sandbox route, a
  worker-reported storage dependency, or an active migration names its tag or
  digest. A woken sandbox can still read layers of the snapshot it restored
  from, so snapshot retention is skipped while any sandbox has not reported its
  storage dependencies;
- the immutable environment repository (`immutable_environments.repository`):
  a root is live while a tagged image in another repository carries its
  `org.ucloud.immutable-environment.v1` annotation, and a component while a live
  root lists it as base, workspace, or toolkit.

Anything else is deleted once its newest known registry time (the filesystem
tag link mtime, the image config time, or the last recorded use) is older than
`registry_reference_grace_seconds` (1 hour). A tag with no known time is kept.
Leases fence every deletion as in age pruning, and liveness is recomputed just
before deleting. The prune output reports, per repository, how many manifests
it would delete and why the rest stayed (`live`, `grace`, `leased`,
`age_unknown`), plus layer bytes for environments.

### Blob sweep

Deleting a manifest frees no space until its blobs go. For the filesystem store,
`ucloud-sandbox-registry-gc.service` (every six hours, and from pressure
cleanup) runs an online sweep (`ucloud_sandboxes/registry_sweep.py`) while the
registry keeps serving reads and writes. It marks every blob that any
repository's manifest revisions reach, including index children, and deletes
unreachable blobs whose data and repository links are older than
`registry_blob_grace_seconds` (2 hours, longer than any push). It also removes
repository layer links that none of that repository's manifests use.

A push can make an old blob reachable again only through a manifest PUT, which
Distribution verifies against the repository's layer link and the blob data. The
sweep therefore unlinks a batch, renames its blob directories aside, waits two
seconds, and rescans links and manifests written since the sweep started. A hit
restores the blob and its links; otherwise the renamed directories are deleted.
A PUT that races the unlink fails with `BLOB_UNKNOWN` and is retried by the
pusher; it cannot commit a manifest that points at a deleted blob. Because
Distribution's in-memory blob descriptor cache would hide such deletions, the
filesystem registry runs with `REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR=none`. A
journal next to the data undoes a batch interrupted by a crash.

S3 registry stores keep Distribution's offline garbage collection, which stops
the registry. It remains available for the filesystem store as a manual tool:

```bash
sudo /work/ucloud-sandboxes/gateway-venv/bin/python -m ucloud_sandboxes.systemd \
  registry-gc --offline --config /etc/ucloud-sandboxes/deployment.json
```

### Disk pressure

For the filesystem store, the gateway, autoscaler, and maintenance units
measure the registry volume with `statvfs` (`ucloud_sandboxes/registry_disk.py`):

- at `registry_disk_cleanup_percent` (70 %), `ucloud-sandbox-registry-pressure.timer`
  (every minute) prunes by age and reference and sweeps blobs, at most once per
  `registry_disk_gc_interval_seconds` (30 minutes) unless eviction follows. If
  usage stays above the threshold, it evicts managed images (`ucloud-managed/*`)
  least recently used first until the projected usage reaches
  `registry_disk_target_percent` (60 %), deletes the environments only they
  used, and sweeps again. An image is never evicted while leased, while a route,
  prepared capacity, or warmup names it, or within the reference grace of its
  last push or use. The projection counts a shared layer or environment
  component only when its last owner goes;
- at `registry_disk_refuse_percent` (90 %), the gateway answers image builds and
  external image imports with 503 `registry_disk_pressure`, `retryable: true`
  and `Retry-After: 60` before dispatching to a builder, so no push meets a
  full disk.

Eviction removes the gateway image records of evicted images and remembers
their ids. An external image is then imported again on its next create. A
create naming an evicted managed build fails with 400 `image_evicted` and
`rebuild_required: true` instead of a registry manifest miss; building it again
restores it. Gateways drop their cached manifest resolutions after each
eviction.

Snapshot publication after a park is not gated: workers publish straight to the
registry, and a failed publication leaves the park local and is retried later.

Usage, thresholds, and the last prune and sweep appear under `registry.disk` in
`/v1/metrics` and the dashboard's registry page, and as `registry_disk` in each
autoscaler cycle event. The autoscaler logs a warning while usage is above the
cleanup threshold.

For manual inspection, the registry prune command can plan deletions by
last-used age, repository keep floor, or both:

```bash
ucloud-sandboxes registry-prune \
  --config /etc/ucloud-sandboxes/deployment.json
```

Add `--execute` to delete the selected manifest digests:

```bash
ucloud-sandboxes registry-prune \
  --config /etc/ucloud-sandboxes/deployment.json \
  --execute
```

Add `--evict-lru` to also plan (or, with `--execute`, perform) the
least-recently-used eviction that pressure cleanup runs above the cleanup
threshold.

Sweep blobs on demand, for example after an out-of-band manifest deletion, or
run pressure cleanup at once:

```bash
sudo systemctl start ucloud-sandbox-registry-gc.service
sudo systemctl start ucloud-sandbox-registry-pressure.service
```
