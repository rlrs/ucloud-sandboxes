# Managed Registry

The deployment can host a private Docker registry for builder output. This can
run on the public control-plane VM or on a dedicated VM attached to the same
private network. The live deployment currently uses a dedicated registry VM so
registry storage can be mounted from UCloud project storage without moving the
public gateway links:

1. Builder nodes build and push
   `ucloud-sandbox-registry:5000/repo/name:tag`.
2. The gateway records pushed image metadata by image id and registry tag.
3. Sandbox nodes pull the registry tag before creating containers.

The registry is a standard Docker Distribution container. Back it with an
explicit UCloud project storage path mounted into the gateway VM. UCloud mounts
the project drive under `/work/<drive-title>`, so the registry data path should
be below that mount, not on the VM root disk or an incidental `/work`
directory.

Submit or replace the registry-capable VM with the project drive attached. The
validated DFM Pretraining deployment mounts drive `/998037`, whose title is
`data`, so it appears inside the VM as `/work/data`:

```bash
ucloud-sandboxes submit-vm \
  --role gateway \
  --private-network-id 12345327 \
  --no-public-link \
  --mount /<drive-id> \
  ...
```

## Gateway Service

Install Docker and the checked-in units on the VM that will run the registry:

```bash
sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io

sudo install -d -m 0755 /etc/ucloud-sandboxes
sudo tee /etc/ucloud-sandboxes/registry.env >/dev/null <<'EOF'
UCLOUD_REGISTRY_BIND=0.0.0.0
UCLOUD_REGISTRY_PORT=5000
UCLOUD_REGISTRY_DATA_DIR=/work/data/ucloud-sandbox-registry/docker-registry
UCLOUD_REGISTRY_IMAGE=registry:2
EOF

sudo install -m 0644 deploy/systemd/ucloud-sandbox-registry.service \
  /etc/systemd/system/ucloud-sandbox-registry.service
sudo install -m 0644 deploy/systemd/ucloud-sandbox-registry-gc.service \
  /etc/systemd/system/ucloud-sandbox-registry-gc.service
sudo install -m 0644 deploy/systemd/ucloud-sandbox-registry-gc.timer \
  /etc/systemd/system/ucloud-sandbox-registry-gc.timer
sudo systemctl daemon-reload
sudo systemctl enable --now ucloud-sandbox-registry.service
sudo systemctl enable --now ucloud-sandbox-registry-gc.timer
curl -fsS http://127.0.0.1:5000/v2/_catalog
```

For the first UCloud deployment, HTTP on the private network is acceptable if
builder and sandbox nodes are initialized with Docker trust for the private
registry host. For a wider network boundary, put TLS and authentication in front
of the registry instead of using Docker's insecure registry setting.

Do not bind the registry to a UCloud public link in the default deployment. The
registry is an internal control-plane service for builders and sandbox nodes on
the private network.

Start the gateway with a registry URL that reaches the registry over the private
network. If the registry runs on the same VM, use
`--registry-url http://127.0.0.1:5000`. In the current live deployment, the
public gateway uses
`--registry-url http://sandbox-gateway-registry-mount-07011413:5000`. This
enables the dashboard registry panel and the `/v1/registry` status endpoint
without exposing the registry itself publicly.

The current registry service will survive container and service restarts as
long as `UCLOUD_REGISTRY_DATA_DIR` points at the mounted project folder. A
registry VM replacement must attach the same project drive before starting the
registry, otherwise it will start with an empty registry.

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

The registry prune command plans deletions by keeping the newest tags per
repository:

```bash
ucloud-sandboxes registry-prune \
  --registry-url http://127.0.0.1:5000 \
  --keep-per-repository 5
```

Add `--execute` to delete the selected manifest digests:

```bash
ucloud-sandboxes registry-prune \
  --registry-url http://127.0.0.1:5000 \
  --keep-per-repository 5 \
  --execute
```

After manifests are deleted, `ucloud-sandbox-registry-gc.timer` stops the
registry daily, runs Docker Distribution garbage collection with
`--delete-untagged`, and starts the registry again. Run the GC service manually
after a large prune:

```bash
sudo systemctl start ucloud-sandbox-registry-gc.service
```
