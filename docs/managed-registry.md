# Managed Registry

The deployment can host a private Docker registry for builder output. This can
run on the public control-plane VM or on a dedicated VM attached to the same
private network. The live deployment uses the all-in-one control-plane VM:
gateway, relay, registry, registry GC, and autoscaler run on job `12349450`, and
registry storage is backed by the mounted project drive:

1. Builder nodes build and push
   `ucloud-sandbox-registry:5000/repo/name:tag`.
2. The gateway records pushed image metadata by image id and registry tag.
3. Sandbox nodes pull the registry tag before creating containers.

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
  --role gateway \
  --private-network-id 12345327 \
  --public-link-id 12345368 \
  --public-link-port 8090 \
  --mount /<drive-id> \
  ...
```

## Gateway Service

The normal path is `deploy-all-in-one`; it installs Docker, writes
`/etc/ucloud-sandboxes/registry.env`, installs the packaged registry and GC
systemd units, and starts the registry:

```bash
uv run ucloud-sandboxes deploy-all-in-one <job-id> \
  --project <project-id> \
  --deployment-id <deployment-id> \
  --private-network-id <private-network-id> \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
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

The generated gateway env file sets
`UCLOUD_REGISTRY_URL=http://127.0.0.1:5000`. This enables the dashboard registry
page and the `/v1/registry` status endpoint without exposing the registry itself
publicly.

The current registry service will survive container and service restarts as
long as `UCLOUD_REGISTRY_DATA_DIR` points at the mounted project folder. A
control-plane VM replacement must attach the same project drive before starting
the registry, otherwise it will start with an empty registry.

## Node Init

Builders and sandbox nodes must trust the private registry if it is served over
HTTP. Use a stable alias in image tags, and map that alias to the current
gateway private-network address during VM init:

```bash
ucloud-sandboxes init-vm <job-id> \
  --docker-insecure-registry ucloud-sandbox-registry:5000 \
  --host-alias ucloud-sandbox-registry=<gateway-private-ip> \
  ...
```

For autoscaled nodes, pass the prefixed option to the autoscaler:

```bash
ucloud-sandboxes autoscaler-loop \
  --execute-init \
  --init-docker-insecure-registry ucloud-sandbox-registry:5000 \
  --init-host-alias ucloud-sandbox-registry=<gateway-private-ip> \
  ...
```

The init script writes Docker's `insecure-registries` daemon setting and
restarts Docker before starting the node agent. It also writes the host alias to
`/etc/hosts`, so image tags do not need to change when the gateway's private IP
changes; only the deployment's host-alias value needs updating.

The same init script configures Docker's bridge MTU from the VM default-route
interface. This matters on UCloud private-network VMs where the host interface
MTU can be lower than Docker's default `1500`; without this, large HTTPS
responses during `docker build` or registry pulls can stall even though host
networking works.

## Build And Run

Use a registry tag as the build tag and push it:

```python
client.build_image(
    id="mini-swe-python311",
    tag="ucloud-sandbox-registry:5000/prime-rl/mini-swe-python311:mswe-2.2.8",
    context_path="./build-context",
    push=True,
)
```

Then create sandboxes with either the registry tag or the image id:

```python
client.create_sandbox(
    id="sample-1",
    image="mini-swe-python311",
    cpus=1,
    memory_mb=2048,
    disk_mb=10240,
)
```

When `image` is an image id, the gateway resolves it to the recorded pushed
registry tag. If the image was built without `push=True`, the gateway rejects
the create request with a clear error because builder-local Docker images are
not durable and are not copied to sandbox nodes.

## Cleanup

The all-in-one deployment installs `ucloud-sandbox-registry-prune.timer`.
By default it runs daily, deletes tags whose last recorded sandbox use is older
than 30 days, and keeps no per-repository floor. The zero keep floor is
deliberate: many generated build repositories have only one tag, so a keep
floor would prevent those images from ever becoming eligible for cleanup.

The gateway records successful sandbox creation and idempotent create recovery
in `<state_dir>/registry-usage.json`. Scheduled pruning uses that file as the
age source. Tags with no usage entry are kept, because deleting by image
creation time can remove shared base images that are still actively used.

The prune service also receives `<state_dir>/images.json`. When it deletes a
private-registry manifest, it removes matching pushed build records from that
image metadata cache. It also prunes stale pushed build records whose manifests
are already missing. This matters for SDK clients because `list_images()` is
used as the build cache signal; stale metadata must not make a deleted image
look reusable.

After a successful prune, `ucloud-sandbox-registry-prune.service` starts
`ucloud-sandbox-registry-gc.service`. The GC service stops the registry, runs
Docker Distribution garbage collection with `--delete-untagged`, and starts the
registry again so disk space is reclaimed from unreferenced blobs.

Tune the scheduled policy with deployment flags:

```bash
ucloud-sandboxes deploy-all-in-one ... \
  --registry-retention-days 30 \
  --registry-keep-per-repository 0 \
  --execute
```

For manual inspection, the registry prune command can plan deletions by
last-used age, repository keep floor, or both:

```bash
ucloud-sandboxes registry-prune \
  --registry-url http://127.0.0.1:5000 \
  --max-age-days 30 \
  --keep-per-repository 0 \
  --usage-file /work/ucloud-sandboxes/state/registry-usage.json \
  --image-file /work/ucloud-sandboxes/state/images.json \
  --prune-stale-image-records
```

Add `--execute` to delete the selected manifest digests:

```bash
ucloud-sandboxes registry-prune \
  --registry-url http://127.0.0.1:5000 \
  --max-age-days 30 \
  --keep-per-repository 0 \
  --usage-file /work/ucloud-sandboxes/state/registry-usage.json \
  --image-file /work/ucloud-sandboxes/state/images.json \
  --prune-stale-image-records \
  --execute
```

Run GC manually after an out-of-band manifest deletion:

```bash
sudo systemctl start ucloud-sandbox-registry-gc.service
```
