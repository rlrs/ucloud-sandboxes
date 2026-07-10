# CLI And Operations

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
pool. See [scaling-policy.md](scaling-policy.md).

Sandbox runtime security is tracked in
[security-stance.md](security-stance.md). On an initialized VM node,
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

`reconcile` is deliberately read-only. To run one mutating production cycle,
use `autoscaler-loop --once`; it reads pending demand from the routing state and
uses the same local process lock and provider journal as the recurring service.
Create, stop, and VM initialization execution remain separate flags:

```bash
uv run ucloud-sandboxes autoscaler-loop --once \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --route-file /work/ucloud-sandboxes/state/routes.sqlite \
  --execute

uv run ucloud-sandboxes autoscaler-loop --once \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --private-network-id 12345327 \
  --route-file /work/ucloud-sandboxes/state/routes.sqlite \
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

For the all-in-one gateway/control-plane VM, bind the public gateway link to
the gateway service port while still joining the private network for node
traffic and mounting the project storage drive used by the local registry:

```bash
uv run ucloud-sandboxes submit-vm \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --deployment-id live-20260629 \
  --role gateway \
  --private-network-id 12345327 \
  --public-link-id 12345368 \
  --public-link-port 8090 \
  --mount /998037 \
  --hostname-seed gateway-1 \
  --output json
```

After the VM is running and the gateway service is listening, activate UCloud's
VM web forwarding for the public-link target port:

```bash
uv run ucloud-sandboxes open-vm-web 12349450 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --port 8090
```

The normal deployment path is now to let the CLI converge the running VM:

```bash
uv build
uv run ucloud-sandboxes deploy-all-in-one 12349450 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --deployment-id live-20260629 \
  --private-network-id 12345327 \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
  --execute
```

`deploy-all-in-one` writes the deployment-specific env files, installs the
packaged systemd units, stages the wheel and UCloud session, generates missing
tokens and the gateway init SSH key, registers that key with UCloud, restarts
gateway/relay/registry/autoscaler, and opens the gateway and relay VM web
ports. Use `--output script` for the exact remote script or omit `--execute` for
a dry run.

Builder capacity is autoscaled separately. Manual builder VM tests should keep
the VM on the private network and use the builder role so the sandbox
autoscaler does not treat it as disposable sandbox pool capacity:

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
  --heartbeat-bearer-token-file /work/ucloud-sandboxes/state/heartbeat-token \
  --heartbeat-bearer-token-source-file /work/ucloud-sandboxes/state/heartbeat-token \
  --package-spec /work/ucloud-sandboxes/release/ucloud_sandboxes-<version>-py3-none-any.whl \
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

The heartbeat credential has its own file and is the only bearer credential
copied to nodes. It is generated independently from `gateway-token`: the
heartbeat token authorizes only `POST /v1/nodes/heartbeat`, while the gateway
token authorizes public/control routes and cannot post a heartbeat. The
heartbeat endpoint accepts only `Authorization: Bearer`; the public-link
`X-UCloud-Sandbox-Token` header is not valid on that channel.

Rotate the two files independently. Gateway-token rotation requires updating
public clients and restarting the gateway. Heartbeat-token rotation requires a
coordinated maintenance window: pause node initialization, install the new
heartbeat token on existing nodes, atomically replace the gateway copy, restart
the gateway and heartbeat timers, then resume initialization. Until overlapping
heartbeat credentials are supported, an uncoordinated rotation causes expected
temporary `401` responses rather than falling back to the gateway token.

The init script installs Docker and gVisor/runsc, creates a sparse XFS
project-quota image for Docker under `/var/lib/ucloud-sandboxes/docker-xfs.img`,
mounts it at `/var/lib/ucloud-sandboxes/docker-xfs`, installs this package into
a VM-local venv owned by `--service-user` (`ucloud` by default), and enables
systemd services for the node agent and heartbeat timer that run as that user
with Docker group access. Docker storage is intentionally VM-local because
sandbox and builder layer caches are high-churn and do not need to persist
after scale-down; the registry is the persistent image store. Use
`--docker-quota-image-gb 0` to disable quota-backed Docker storage, or set a
larger value for large sandbox nodes. For private network use, the generated
node agent binds to `0.0.0.0` and advertises `http://<node-id>:8090` in
heartbeats by default.

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
