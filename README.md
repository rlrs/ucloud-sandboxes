# ucloud-sandboxes

Autoscaler for secure CPU sandboxes on top of SDU UCloud.

The autoscaler manages UCloud VM jobs as pool nodes. Each VM node is expected to
run a node agent which starts and stops per-request sandboxes locally with
containers and gVisor. The autoscaler does not treat individual UCloud jobs as
sandboxes.

Current status: live public gateway plus autoscaler loop. The gateway stores
node heartbeats, routes sandbox and exec requests across registered VM nodes,
records pending sandbox demand when no node can fit a request, records prepared
capacity signals for upcoming batches, records pending image-build demand when
no builder is ready, and runs a systemd autoscaler loop that reconciles that
demand against UCloud VM jobs. It can render or submit
UCloud VM jobs for sandbox nodes, builder nodes, or a gateway/control-plane VM,
and it keeps destructive stops gated by deployment labels. The live `vm-ubuntu:24.04`
app rejects `sshEnabled=true`, so submitted VM jobs default to no UCloud SSH;
after boot UCloud still announces an SSH proxy command that `init-vm` can use
for post-boot initialization. Automatic node initialization from the gateway VM
still needs either SSH credentials on the gateway or UCloud startup-script
integration.

## Useful commands

Print a starter config:

```bash
uv run ucloud-sandboxes sample-config
```

Inspect the known small VM job:

```bash
uv run ucloud-sandboxes inspect-job 12345311 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141
```

Plan one reconciliation cycle from live UCloud jobs:

```bash
uv run ucloud-sandboxes plan \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --include-job 12345311 \
  --pending-vcpu 1 \
  --pending-memory-mb 2048 \
  --pending-disk-mb 10240 \
  --oldest-pending-seconds 300
```

Use `--jobs-file` and `--heartbeats` to run the planner without touching
UCloud.

Autoscaling demand is resource-based. Sandbox requests and manual planning use
vCPU, memory, and disk requirements; count-only sandbox demand is not accepted.

The default policy scales to zero. When VM startup is slow, tune in-flight VM
caps, provisioning resource discounts, and idle grace before paying for a warm
pool. See [docs/scaling-policy.md](docs/scaling-policy.md).

Sandbox runtime security is tracked in
[docs/security-stance.md](docs/security-stance.md). On an initialized VM node,
run the local conformance probe before trusting the runtime:

```bash
uv run ucloud-sandboxes runtime-conformance --sudo --execute --output json
```

Render one full reconciliation cycle, including VM create payloads and stop
intents. This is dry-run by default:

```bash
uv run ucloud-sandboxes reconcile \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --pending-vcpu 2 \
  --pending-memory-mb 4096 \
  --pending-disk-mb 10240 \
  --output json
```

Create execution and stop execution are separate flags. Stop execution calls
UCloud job termination, so it should only be enabled when the stop intents look
right:

```bash
uv run ucloud-sandboxes reconcile \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --pending-vcpu 2 \
  --pending-memory-mb 4096 \
  --pending-disk-mb 10240 \
  --execute

uv run ucloud-sandboxes reconcile \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --execute-stops
```

Render the UCloud job-submission fragment for attaching a VM job to a private
network. The resulting `hostname` is the DNS name other jobs in that private
network should use for the node:

```bash
uv run ucloud-sandboxes vm-network-attachment \
  --private-network-id "$UCLOUD_PRIVATE_NETWORK_ID" \
  --hostname-seed 12345317
```

With a config file, `private_network_id` and `node_hostname_prefix` provide the
node defaults. `gateway_public_link_id` and `gateway_public_link_port` describe
the single public link that should be bound to the gateway/control-plane VM, not
to autoscaled sandbox nodes:

```json
{
  "private_network_id": "12345327",
  "gateway_public_link_id": "12345368",
  "gateway_public_link_port": 8090,
  "node_hostname_prefix": "sandbox-node"
}
```

Render the UCloud job-submission fragment for binding a public link to a VM
port:

```bash
uv run ucloud-sandboxes vm-public-link-attachment \
  --public-link-id 12345368 \
  --port 8090
```

Render the VM job submission payload. This is dry-run by default:

```bash
uv run ucloud-sandboxes submit-vm \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --hostname-seed dev-1 \
  --output json
```

Submit the VM job only when ready:

```bash
uv run ucloud-sandboxes submit-vm \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --hostname-seed dev-1 \
  --execute
```

For a gateway/control-plane VM, bind the public link to the gateway service
port while still joining the private network for node traffic:

```bash
uv run ucloud-sandboxes submit-vm \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --deployment-id live-20260629 \
  --role gateway \
  --private-network-id 12345327 \
  --public-link-id 12345368 \
  --public-link-port 8090 \
  --hostname-seed gateway-1 \
  --output json
```

After the VM is running and the gateway service is listening, activate UCloud's
VM web forwarding for the public-link target port:

```bash
uv run ucloud-sandboxes open-vm-web 12346251 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --port 8090
```

For a build/control-plane VM, keep it on the private network but give it a
separate role so the sandbox autoscaler does not treat it as disposable sandbox
pool capacity:

```bash
uv run ucloud-sandboxes submit-vm \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --role builder \
  --private-network-id 12345327 \
  --hostname-seed builder-1 \
  --product-id cpu-amd-zen5-16-vcpu \
  --disk-gb 250 \
  --output json
```

Use a larger CPU product here when UCloud capacity exposes one in the project.
This VM is for Docker builds, registry push/pull work, or running the gateway;
sandbox nodes should remain separate.

`submit-vm` does not request UCloud SSH by default. The current live
`vm-ubuntu:24.04` app rejects `--ssh` with
`This application does not support SSH but it is required`. Running VMs can
still announce an SSH proxy update such as
`ssh ucloud@ssh.cloud.sdu.dk -p <port>`.

The live `DFM Pretraining` private network created for this project is:

```text
id: 12345327
name: ucloud-sandboxes
subdomain: dfm-sandboxes
provider: ucloud
```

The live unbound gateway public link in the same project is:

```text
id: 12345368
domain: app-sandboxes.cloud.sdu.dk
product: ucloud/u1-publiclink/u1-publiclink
state: READY
```

Render the post-boot VM init script locally:

```bash
uv run ucloud-sandboxes render-vm-init-script \
  --job-id 12345317 \
  --node-id sandbox-node-12345317 \
  --heartbeat-url http://control-plane:8080/v1/nodes/heartbeat \
  --total-vcpu 2 \
  --total-memory-mb 6144 \
  --total-disk-mb 51200
```

`init-vm` supports SSH-based post-boot init for VM jobs that are running and
have announced an SSH command. This works even though `vm-ubuntu:24.04` is
submitted with `sshEnabled=false`:

```bash
uv run ucloud-sandboxes init-vm 12345317 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --node-id sandbox-node-12345317 \
  --heartbeat-url http://control-plane:8080/v1/nodes/heartbeat

uv run ucloud-sandboxes init-vm 12345317 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --node-id sandbox-node-12345317 \
  --heartbeat-url http://control-plane:8080/v1/nodes/heartbeat \
  --init-authorized-key-file /work/ucloud-sandboxes/state/ssh/gateway-init.pub \
  --ssh-private-key-file /work/ucloud-sandboxes/state/ssh/gateway-init \
  --execute
```

Initialize sandbox VM nodes without image builds enabled:

```bash
uv run ucloud-sandboxes init-vm 12345318 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --node-id sandbox-node-12345318 \
  --heartbeat-url https://app-sandboxes.cloud.sdu.dk/v1/nodes/heartbeat \
  --heartbeat-bearer-token-file /work/ucloud-sandboxes/state/gateway-token \
  --heartbeat-bearer-token-source-file /work/ucloud-sandboxes/state/gateway-token \
  --package-spec /work/ucloud-sandboxes/release/ucloud_sandboxes-0.1.0-py3-none-any.whl \
  --total-vcpu 2 \
  --total-memory-mb 6144 \
  --total-disk-mb 250000 \
  --cpu-overcommit 2 \
  --memory-overcommit 1.2 \
  --docker-quota-image-gb 200 \
  --init-authorized-key-file /work/ucloud-sandboxes/state/ssh/gateway-init.pub \
  --ssh-private-key-file /work/ucloud-sandboxes/state/ssh/gateway-init \
  --execute
```

The init script installs Docker and gVisor/runsc, creates a sparse XFS
project-quota image for Docker under `/work/ucloud-sandboxes/docker-xfs.img`,
mounts it at `/work/ucloud-sandboxes/docker-xfs`, installs this package into a
VM-local venv owned by `--service-user` (`ucloud` by default), and enables
systemd services for the node agent and heartbeat timer that run as that user
with Docker group access. Use `--docker-quota-image-gb 0` to disable
quota-backed Docker storage, or set a larger value for large sandbox nodes. For
private network use, the generated node agent binds to `0.0.0.0` and advertises
`http://<node-id>:8090` in heartbeats by default.

When the gateway initializes nodes, generate a dedicated gateway keypair on the
gateway and pass only its public key with `--init-authorized-key-file`. The
private key is used by `init-vm --ssh-private-key-file` for the SSH transport
and should not be copied into node init scripts or release artifacts.

Register the gateway public key with UCloud once so new VM jobs accept it during
their first SSH login:

```bash
ucloud-sandboxes ensure-ucloud-ssh-key \
  --session-file /work/ucloud-sandboxes/state/ucloud-session.json \
  --public-key-file /work/ucloud-sandboxes/state/ssh/gateway-init.pub \
  --title "ucloud-sandboxes gateway init"
```

Live note: this path has been tested on UCloud job `12345813` using a wheel
copied to `/work/ucloud-sandboxes/release/`. Docker, `runsc`, quota-backed
`--storage-opt size=...`, the node agent, heartbeat delivery, real sandbox
creation, exec, and cleanup all worked.

## Python SDK

The client SDK is split into a separate `ucloud-sandboxes-sdk` package so
benchmark runners and user code can talk to the gateway without installing the
autoscaler, node agent, VM init code, or UCloud control-plane tooling.
In this checkout, its source lives as a separate nested repository at
`ucloud-sandboxes-sdk/`; the parent repo ignores that directory.

Install the SDK package in client environments:

```bash
uv add "ucloud-sandboxes-sdk[async]"
```

Import from `ucloud_sandboxes_sdk`. Point the client at a node-agent URL directly
during development, or at the public gateway URL in production. Public gateway
routes should be protected with a bearer token loaded by the server from
`--gateway-bearer-token-file`; pass that token as an `Authorization` header from the
SDK:

```python
from ucloud_sandboxes_sdk import SandboxClient

client = SandboxClient(
    "https://app-sandboxes.cloud.sdu.dk",
    headers={"Authorization": "Bearer <token>"},
)
sandbox = client.create_sandbox(
    id="example-1",
    image="busybox:latest",
    command=["sleep", "300"],
    cpus=0.25,
    memory_mb=128,
    disk_mb=64,
    ttl_seconds=600,
)
try:
    result = sandbox.exec(
        ["sh", "-lc", "echo stdout; echo stderr >&2; read line; echo got:$line"],
        input="hello\n",
        timeout_seconds=30,
    )
    assert result.success
    print(result.stdout)
    print(result.stderr)
finally:
    sandbox.delete()
```

If a benchmark runner knows it will soon need a burst of sandboxes, send an
expiring prepared-capacity signal before starting the samples. This is not a
reservation and does not create placeholder sandboxes; it only asks the
autoscaler to make enough VM resources available soon:

```python
prepare = client.prepare_capacity(
    prepare_id="mbpp-run",
    count=16,
    cpus=1,
    memory_mb=2048,
    disk_mb=10240,
    ttl_seconds=900,
)
print(prepare["demand"]["prepared_resources"])
```

The signal expires automatically. Cancel it early when the run is abandoned:

```python
client.delete_prepared_capacity("mbpp-run")
```

Live note: `https://app-sandboxes.cloud.sdu.dk` is bound to gateway VM job
`12346094`, which forwards to node job `12345813` over private network
`12345327`. A public SDK smoke test created a `busybox` sandbox, ran an exec
with stdin/stdout/stderr, and deleted it through this gateway.

The async client mirrors the same methods:

```python
from ucloud_sandboxes_sdk import AsyncSandboxClient

async with AsyncSandboxClient(
    "https://app-sandboxes.cloud.sdu.dk",
    headers={"Authorization": "Bearer <token>"},
) as client:
    sandbox = await client.create_sandbox(
        id="async-1",
        image="busybox:latest",
        cpus=0.25,
        memory_mb=128,
        disk_mb=64,
    )
    try:
        result = await sandbox.exec(["true"], timeout_seconds=30)
    finally:
        await sandbox.delete()
```

Image cache operations are also exposed. Custom builds should use a registry
tag and push from the build-capable gateway/control-plane machine; sandbox nodes
pull that tag before creating the container:

```python
client.pull_image("python:3.12-slim", image_id="python-base")
client.build_image(
    id="custom-base",
    tag="registry.example.org/ucloud/custom-base:latest",
    context_path="/work/ucloud-sandboxes/build-contexts/custom-base",
    push=True,
)
client.snapshot_sandbox("example-1", "local/example-1-snapshot:latest")
```

When `context_path` points to a local directory, the SDK sends a compressed
build context to the gateway. Pass `upload_context=False` to build from a path
that already exists on the gateway/control-plane machine.

The current JSON exec API supports text stdin and streams stdout/stderr events.
The SDK waits for the final stdout/stderr chunks before returning from
`exec()`. File upload/download uses raw byte HTTP endpoints instead of
base64-over-exec. A binary/WebSocket exec path exists in the async node-agent
prototype and should become the production gateway path for high-volume
streaming.

## Inspect AI integration

An optional Inspect AI provider is available as `ucloud`. It follows the same
task/sample lifecycle shape as cloud providers such as Modal: one sandbox is
created per sample, `exec()` is routed through the SDK, and cleanup deletes the
tracked sandbox ids.

Install the optional Inspect dependencies in the environment that runs
benchmarks:

```bash
uv add "ucloud-sandboxes-sdk[inspect]"
```

Set the gateway or node-agent URL before running Inspect:

```bash
export UCLOUD_SANDBOX_URL="https://app-sandboxes.cloud.sdu.dk"
export UCLOUD_SANDBOX_API_TOKEN="<token>"
export UCLOUD_SANDBOX_IMAGE="python:3.12-slim"
export UCLOUD_SANDBOX_CPUS="1"
export UCLOUD_SANDBOX_MEMORY_MB="2048"
export UCLOUD_SANDBOX_DISK_MB="10240"
export UCLOUD_SANDBOX_START_TIMEOUT_SECONDS="1800"
export UCLOUD_SANDBOX_BUILD_TIMEOUT_SECONDS="1800"
export UCLOUD_SANDBOX_RETRY_INTERVAL_SECONDS="10"
```

Then use Inspect's sandbox selector:

```bash
inspect eval task.py --sandbox ucloud
```

For a one-sample MBPP smoke test with `inspect-evals` and an OpenAI-compatible
chat-completions endpoint:

```bash
uv run \
  --with "ucloud-sandboxes-sdk[inspect]" \
  --with inspect-evals \
  --with openai \
  inspect eval inspect_evals/mbpp \
  --model "openai/${OPENAI_MODEL}" \
  --model-base-url "${OPENAI_BASE_URL}" \
  -M responses_api=false \
  --sandbox ucloud \
  --limit 1 \
  --epochs 1 \
  --epochs-reducer mean \
  --max-samples 1 \
  --max-sandboxes 1 \
  --max-connections 1 \
  -T temperature=0.0
```

Use `-M responses_api=false` for OpenAI-compatible endpoints that implement
`/v1/chat/completions` but not `/v1/responses`.

For environments where the sandbox cannot reach the model host directly, run
the outbound-only model relay. It is part of this service package and can run
on the same public gateway/control-plane VM as the autoscaler, or on another
public host reachable from both UCloud sandboxes and LUMI workers:

```bash
uv run ucloud-sandboxes serve-model-relay \
  --host 0.0.0.0 \
  --port 8092 \
  --sandbox-bearer-token-file /work/ucloud-sandboxes/state/relay-sandbox-token \
  --worker-bearer-token-file /work/ucloud-sandboxes/state/relay-worker-token \
  --request-timeout-seconds 7200 \
  --worker-lease-seconds 600
```

Then create sandboxes with outbound networking and an OpenAI-compatible base URL
scoped to the rollout:

```python
client.create_sandbox(
    image="registry.example.org/swebench/task:latest",
    cpus=1,
    memory_mb=2048,
    disk_mb=10240,
    network="bridge",
    env={
        "VF_RELAY_ROLLOUT_ID": "run-001",
        "OPENAI_BASE_URL": "https://relay.example.org/rollouts/run-001/v1",
        "OPENAI_API_KEY": "<sandbox-relay-token>",
    },
    labels={"rollout": "run-001"},
)
```

The worker near the model endpoint registers the rollout, long-polls
`/worker/poll`, calls local inference, and posts the response to
`/worker/respond`. See [docs/model-relay.md](docs/model-relay.md).

The provider accepts `None`, a single-service Compose config, a compose YAML
file, or a Dockerfile. Compose `image`, `command`, and `environment` are mapped
into the sandbox spec. Dockerfile configs call `build_image`; the SDK uploads
the local build context to the gateway/control-plane build runtime.
When the gateway returns a scale-up `503` because no sandbox or builder node is
ready yet, the provider keeps retrying until the corresponding timeout expires.
Debug SSH can be enabled with `UCLOUD_SANDBOX_SSH=1` for images that explicitly
support an SSH server. Normal benchmark control uses gateway exec and file APIs,
and model calls should use the relay path above.

Deployment metadata is first-class: use `--deployment-id` on production-like
commands so VM job labels and node heartbeats identify the owning deployment.
See [docs/deployment-flow.md](docs/deployment-flow.md) for the rollout,
credential, versioning, and cleanup-safety contract.

Run a local heartbeat receiver:

```bash
uv run ucloud-sandboxes serve-control-plane --host 127.0.0.1 --port 8080
```

Run a public gateway that routes API traffic to node agents from heartbeats and
an in-memory route index. The `--route-file` is a write-through recovery and
pending/prepared-demand database used by the gateway and autoscaler, not the
normal per-request routing path:

```bash
uv run ucloud-sandboxes serve-control-plane \
  --host 0.0.0.0 \
  --port 8090 \
  --heartbeat-file /work/ucloud-sandboxes/state/heartbeats.json \
  --route-file /work/ucloud-sandboxes/state/routes.sqlite \
  --gateway-bearer-token-file /work/ucloud-sandboxes/state/gateway-token \
  --enable-image-builds \
  --execute-image-builds
```

Run the outbound model relay as a sibling service on the same gateway/control
VM. It uses separate bearer tokens: the sandbox token is passed to sandboxes as
`OPENAI_API_KEY`, and the worker token is used by the LUMI-side worker that
registers rollouts, polls work, renews leases, and posts responses:

```bash
uv run ucloud-sandboxes serve-model-relay \
  --host 0.0.0.0 \
  --port 8092 \
  --sandbox-bearer-token-file /work/ucloud-sandboxes/state/relay-sandbox-token \
  --worker-bearer-token-file /work/ucloud-sandboxes/state/relay-worker-token \
  --request-timeout-seconds 7200 \
  --worker-lease-seconds 600 \
  --completed-request-retention-seconds 3600
```

The systemd unit template for production-like gateway VMs is
`deploy/systemd/ucloud-sandbox-relay.service`. Expose the relay publicly with a
UCloud public link bound to port `8092`, or put a reverse proxy on the existing
gateway ingress and forward relay paths to the local relay process.

Live note: `https://app-sandboxes-relay.cloud.sdu.dk` is bound to gateway VM job
`12346251` through UCloud public link `12346842` on port `8092`.

The gateway writes dashboard metrics to `<state-dir>/metrics.jsonl` by default
and exposes a snapshot at `GET /v1/metrics`. The snapshot includes fresh node
scheduler load, actual VM CPU/memory pressure sampled from `/proc`, aggregate
vCPU/RAM/disk reservations, active/pending sandbox counts, prepared capacity
signals, pending image builds, recent per-node heartbeat samples, recent
autoscaler decisions, and measured scale-up wait time for requests that first
entered pending demand before a node became available.

The gateway also serves a browser dashboard at `/dashboard`. The static
dashboard shell is public so it can load in a normal browser, but live data still
comes from the bearer-protected `/v1/metrics` endpoint. Paste the existing
gateway bearer token into the dashboard; it is stored only in browser
`sessionStorage`.

Run the autoscaler loop on the gateway/control VM:

```bash
uv run ucloud-sandboxes autoscaler-loop \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --deployment-id live-20260629 \
  --state-dir /work/ucloud-sandboxes/state \
  --session-file /work/ucloud-sandboxes/state/ucloud-session.json \
  --route-file /work/ucloud-sandboxes/state/routes.sqlite \
  --heartbeats /work/ucloud-sandboxes/state/heartbeats.json \
  --private-network-id 12345327 \
  --product-id cpu-amd-zen5-16-vcpu \
  --disk-gb 250 \
  --scale-down-idle-seconds 300 \
  --max-builder-nodes 1 \
  --builder-product-id cpu-amd-zen5-16-vcpu \
  --builder-disk-gb 250 \
  --builder-scale-down-idle-seconds 900 \
  --init-retry-seconds 30 \
  --init-cpu-overcommit 2 \
  --init-memory-overcommit 1.2 \
  --execute \
  --execute-init \
  --init-heartbeat-url https://app-sandboxes.cloud.sdu.dk/v1/nodes/heartbeat \
  --init-heartbeat-bearer-token-file /work/ucloud-sandboxes/state/gateway-token \
  --init-heartbeat-bearer-token-source-file /work/ucloud-sandboxes/state/gateway-token \
  --init-package-spec /work/ucloud-sandboxes/release/ucloud_sandboxes-0.1.0-py3-none-any.whl \
  --init-docker-quota-image-gb 200 \
  --init-authorized-key-file /work/ucloud-sandboxes/state/ssh/gateway-init.pub \
  --init-ssh-private-key-file /work/ucloud-sandboxes/state/ssh/gateway-init
```

With `--execute-init`, the autoscaler initializes labelled RUNNING sandbox and
builder VMs that do not yet have fresh heartbeats. Init attempts are recorded in
`<state_dir>/vm-bootstrap.json` and retried after `--init-retry-seconds`; the
node is only considered schedulable once the gateway receives a fresh heartbeat.
Manual `init-vm` is still useful for debugging a specific VM, but should not be
part of the steady-state autoscaling path.

Emit one node heartbeat from a VM node:

```bash
uv run ucloud-sandboxes agent-heartbeat \
  --from-node-agent-url http://127.0.0.1:8090 \
  --post-url http://control-plane:8090/v1/nodes/heartbeat \
  --bearer-token-file /work/ucloud-sandboxes/state/gateway-token
```

For local development, write a heartbeat file directly:

```bash
uv run ucloud-sandboxes agent-heartbeat \
  --job-id 12345311 \
  --node-id local-dev \
  --total-vcpu 2 \
  --total-memory-mb 6144 \
  --total-disk-mb 51200 \
  --heartbeat-file /tmp/ucloud-sandboxes-heartbeats.json

uv run ucloud-sandboxes heartbeats \
  --heartbeat-file /tmp/ucloud-sandboxes-heartbeats.json
```

Run the VM-side node agent locally in dry-run mode:

```bash
uv run ucloud-sandboxes serve-node-agent \
  --job-id 12345311 \
  --node-id local-dev \
  --total-vcpu 2 \
  --total-memory-mb 6144 \
  --total-disk-mb 51200 \
  --host 127.0.0.1 \
  --port 8090
```

Run the high-performance async exec/SSH data-plane node agent:

```bash
uv run ucloud-sandboxes serve-async-node-agent \
  --host 127.0.0.1 \
  --port 8091
```

The node agent runs sandboxes and can pull/cache registry images. It should not
be used for custom Docker builds in the production path; builds belong on the
gateway/control-plane build machine.

Create a sandbox through the node agent:

```bash
curl -sS http://127.0.0.1:8090/v1/sandboxes \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "demo-1",
    "image": "busybox",
    "command": ["sh", "-lc", "echo ok"],
    "cpus": 0.5,
    "memory_mb": 128,
    "disk_mb": 1024,
    "network": "none"
  }'
```

Create an SSH-enabled sandbox. The node agent allocates a localhost port from
its configured SSH port range when `ssh.host_port` is omitted:

```bash
curl -sS http://127.0.0.1:8090/v1/sandboxes \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "ssh-demo-1",
    "image": "local/sandbox-ssh:latest",
    "cpus": 0.5,
    "memory_mb": 256,
    "disk_mb": 1024,
    "network": "bridge",
    "ssh": {
      "enabled": true,
      "user": "sandbox",
      "authorized_keys": ["ssh-ed25519 AAAA... user@example"]
    }
  }'
```

The response includes an SSH command such as:

```bash
ssh -p 22000 sandbox@127.0.0.1
```

List and delete sandboxes:

```bash
curl -sS http://127.0.0.1:8090/v1/sandboxes
curl -sS -X DELETE http://127.0.0.1:8090/v1/sandboxes/demo-1
```

Upload or download a file as raw bytes:

```bash
curl -sS -X PUT \
  "http://127.0.0.1:8090/v1/sandboxes/demo-1/files?path=/workspace/input.txt" \
  --data-binary @input.txt

curl -sS \
  "http://127.0.0.1:8090/v1/sandboxes/demo-1/files?path=/workspace/output.txt" \
  -o output.txt
```

By default the node agent only returns/stores the planned Docker command. On an
actual VM with Docker and gVisor configured, add `--execute-runtime` to execute
commands such as:

```bash
docker run -d --name ucloud-sandbox-demo-1 --runtime runsc --network none ...
```

Build a custom image on the gateway/control-plane build machine and push it to
a registry:

```bash
curl -sS http://127.0.0.1:8090/v1/images/build \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "python-base",
    "tag": "registry.example.org/ucloud/python-base:latest",
    "context_path": "/srv/sandbox-images/python-base",
    "dockerfile": "Dockerfile",
    "push": true,
    "build_args": {
      "PYTHON_VERSION": "3.12"
    }
  }'
```

The raw HTTP API expects `context_path` to exist on the gateway/control-plane
machine. The Python SDK uploads local build contexts automatically. The gateway
does not transfer node-local images between VMs; sandbox nodes pull registry
tags before container creation.

Pull a shared image from a registry:

```bash
curl -sS http://127.0.0.1:8090/v1/images/pull \
  -H 'Content-Type: application/json' \
  -d '{"image": "registry.example.org/ucloud/python-base:latest"}'
```

Snapshot a sandbox container to a reusable image:

```bash
curl -sS http://127.0.0.1:8090/v1/sandboxes/demo-1/snapshot \
  -H 'Content-Type: application/json' \
  -d '{"image": "local/demo-1-snapshot:latest"}'
```

Signal upcoming capacity through the gateway before launching a burst. This
does not bind future sandbox ids to nodes and does not reserve capacity for a
particular caller; it only adds expiring resource demand to the autoscaler:

```bash
curl -sS http://127.0.0.1:8090/v1/capacity/prepare \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "mbpp-run",
    "count": 16,
    "cpus": 1,
    "memory_mb": 2048,
    "disk_mb": 10240,
    "ttl_seconds": 900
  }'
```

List or cancel active signals:

```bash
curl -sS http://127.0.0.1:8090/v1/capacity/prepare
curl -sS -X DELETE http://127.0.0.1:8090/v1/capacity/prepare/mbpp-run
```

`GET /v1/demand` reports pending sandbox resources, prepared resources, and
their combined desired resources. The autoscaler reads that demand from the
route database and scales normal sandbox VM nodes to satisfy it.

## Heartbeat API

`POST /v1/nodes/heartbeat` accepts:

```json
{
  "node_id": "ubuntu-8263",
  "job_id": "12345311",
  "updated_at": "2026-06-28T09:43:15+00:00",
  "active_sandboxes": 1,
  "draining": false,
  "node_url": "http://sandbox-node-12345317:8090",
  "capabilities": ["sandbox", "image-cache"],
  "total_resources": {
    "vcpu": 16,
    "memory_mb": 32768,
    "disk_mb": 500000
  },
  "used_resources": {
    "vcpu": 2.5,
    "memory_mb": 4096,
    "disk_mb": 20000
  },
  "cpu_overcommit": 2,
  "memory_overcommit": 1.2,
  "disk_overcommit": 1.0,
  "labels": {
    "pool": "default"
  }
}
```

`GET /v1/nodes` returns the stored heartbeat list.

`GET /v1/metrics` returns a dashboard-oriented snapshot derived from
heartbeats, route state, and the rolling metrics event log:

```json
{
  "nodes": {"fresh": 1, "sandbox": 1, "builder": 0, "samples": 24},
  "resources": {
    "sandbox": {
      "effective": {"vcpu": 16, "memory_mb": 32768, "disk_mb": 204800},
      "used": {"vcpu": 1, "memory_mb": 512, "disk_mb": 1024},
      "load": {"vcpu": 0.0625, "memory": 0.015625, "disk": 0.005},
      "actual_usage": {
        "cpu_vcpu": 0.8,
        "cpu_percent_avg": 5.0,
        "memory_used_mb": 3072,
        "memory_percent": 9.375
      }
    }
  },
  "sandboxes": {"active_routes": 1, "pending": 0},
  "capacity": {
    "prepared": 1,
    "prepared_sandboxes": 16,
    "prepared_resources": {"vcpu": 16, "memory_mb": 32768, "disk_mb": 163840}
  },
  "scale_up": {"samples": 1, "last_ms": 391000, "p95_ms": 391000}
}
```

## Node Agent API

The VM-side node agent exposes:

- `GET /healthz`
- `GET /v1/heartbeat`
- `GET /v1/images`
- `POST /v1/images/pull`
- `GET /v1/sandboxes`
- `POST /v1/sandboxes`
- `DELETE /v1/sandboxes/<sandbox-id>`
- `PUT /v1/sandboxes/<sandbox-id>/files?path=<absolute-container-path>`
- `GET /v1/sandboxes/<sandbox-id>/files?path=<absolute-container-path>`
- `GET /v1/sandboxes/<sandbox-id>/ssh`
- `POST /v1/sandboxes/<sandbox-id>/exec`
- `GET /v1/exec/<session-id>`
- `GET /v1/exec/<session-id>/events`
- `POST /v1/exec/<session-id>/stdin`
- `POST /v1/exec/<session-id>/close-stdin`
- `POST /v1/sandboxes/<sandbox-id>/snapshot` (requires `--enable-image-builds`)

The gateway/control plane additionally exposes `POST /v1/images/build` when
started with `serve-control-plane --enable-image-builds`.

`POST /v1/sandboxes` accepts:

```json
{
  "id": "demo-1",
  "image": "busybox",
  "command": ["sh", "-lc", "echo ok"],
  "env": {
    "REQUEST_ID": "req-1"
  },
  "memory_mb": 128,
  "cpus": 1,
  "disk_mb": 1024,
  "filesystem": {
    "enforce_disk_quota": false,
    "workspace_path": "/workspace"
  },
  "network": "none",
  "ttl_seconds": 600,
  "ssh": {
    "enabled": true,
    "user": "sandbox",
    "host_port": 22000,
    "container_port": 22,
    "authorized_keys": ["ssh-ed25519 AAAA... user@example"]
  },
  "labels": {
    "tenant": "example"
  }
}
```

At least one resource field (`cpus`, `memory_mb`, or `disk_mb`) is required.

SSH-enabled sandboxes must use `"network": "bridge"`. The node agent binds SSH
to localhost on the VM by default; external access should go through the
gateway/tunnel layer rather than exposing container SSH ports publicly.

Exec commands are session-based and async-capable. The compatibility node-agent
HTTP API records ordered stdout/stderr/status/exit events and accepts stdin
writes. The high-performance path uses the aiohttp async node agent with
WebSocket binary streaming and bounded per-session queues. See
[docs/routing-gateway.md](docs/routing-gateway.md).

Image builds should not run on sandbox nodes. The intended model is:

- control plane/gateway: scheduler, routing, and optionally Docker builds on a
  sufficiently large machine
- builder nodes: autoscaled, builder-only VMs for Docker builds and registry push
- sandbox nodes: run already-built images and pull/cache registry tags
- registry: durable image cache for common building blocks and custom images

The gateway handles `POST /v1/images/build` locally when started with
`--enable-image-builds`; otherwise it routes builds to ready builder-only nodes
advertising `image-build`. If no builder is ready, it records pending image-build
demand in the route file so the autoscaler can create a builder VM. Built tags
should use `"push": true` and a registry tag; sandbox nodes pull registry tags
before creating containers. Sandbox placement only considers nodes advertising
the `sandbox` capability, and builder nodes scale back to zero when pending
image-build demand is gone and the builder idle grace has elapsed.

Sandbox placement is resource-based. Each sandbox request can ask for its own
`cpus`, `memory_mb`, and `disk_mb`. Nodes report physical
resources plus overcommit multipliers in their heartbeat, so the control plane
can pack small and large sandboxes differently. The live sandbox pool currently
uses `--init-cpu-overcommit 2` and `--init-memory-overcommit 1.2`, with disk
left at `1.0`. CPU overcommit is the main packing lever; memory overcommit
should stay modest until pressure and failure behavior are measured under mixed
workloads.

By default `disk_mb` maps to Docker `--storage-opt size=...` for the container
writable layer. VM init creates a sparse XFS image under `/work`, mounts it with
project quotas, configures Docker to use `overlay2` on that data root, and
disables the containerd snapshotter path that did not honor this quota. A node
only advertises `disk-quota` after `ucloud-sandboxes runtime-conformance
--execute` reports `storage-opt-quota-enforced: ok`; the scheduler credits disk
capacity only for nodes with that capability, and the node runtime rejects
`disk_mb` when quota support has not been validated. If
`filesystem.enforce_disk_quota` is true, the runtime uses a read-only root plus
a bounded tmpfs workspace at `filesystem.workspace_path`; that stricter mode is
only enabled when the `tmpfs-quota-enforced` probe passed. Sandboxes also get
explicit bounded `/tmp` and `/run` tmpfs mounts so common temporary writes do
not bypass writable-layer quota as an unbounded runtime default.

## Architecture

- UCloud VM jobs are pool nodes.
- The control plane and VM nodes should be attached to the same UCloud private
  network. VM jobs get a stable private-network hostname, and node heartbeats
  advertise the node-agent URL based on that hostname.
- A node agent on each VM reports resources, active sandbox count, capabilities,
  and drain state.
- The autoscaler reconciles pending sandbox resource demand against UCloud VM
  job state and node heartbeats.
- A later gateway/router should keep client routing stable and forward traffic
  to sandboxes through registered VM nodes.
- Mutating UCloud operations are gated behind explicit `--execute` flags.

See [docs/vm-init.md](docs/vm-init.md) for the current live-API findings and
recommended post-boot VM init strategy.
