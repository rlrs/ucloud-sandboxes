# CLI and operations

The CLI separates inspection from mutation. Planning commands are read-only;
provider create, stop, bootstrap, and deployment actions require their own
explicit execution flags.

## Configuration and inspection

Print a starter configuration:

```bash
uv run ucloud-sandboxes sample-config
```

Inspect a UCloud job:

```bash
uv run ucloud-sandboxes inspect-job <job-id> --project <project-id>
```

Plan resource demand against live jobs and node heartbeats:

```bash
uv run ucloud-sandboxes plan \
  --project <project-id> \
  --pending-vcpu 2 \
  --pending-memory-mb 4096 \
  --pending-disk-mb 10240
```

Use `--jobs-file` and `--heartbeats` for an entirely local plan. Sandbox demand
is always expressed as CPU, memory, and disk shapes.

Render one complete reconcile result without changing UCloud:

```bash
uv run ucloud-sandboxes reconcile \
  --project <project-id> \
  --private-network-id <network-id> \
  --pending-vcpu 2 \
  --pending-memory-mb 4096 \
  --pending-disk-mb 10240 \
  --output json
```

## VM submission

Render a VM job payload:

```bash
uv run ucloud-sandboxes submit-vm \
  --project <project-id> \
  --deployment-id <deployment-id> \
  --role sandbox \
  --private-network-id <network-id> \
  --hostname-seed sandbox-1 \
  --output json
```

Roles are `gateway`, `sandbox`, and `builder`. Add `--execute` only after
reviewing the payload. Helper commands render private-network and public-link
resource fragments:

```bash
uv run ucloud-sandboxes vm-network-attachment \
  --private-network-id <network-id> \
  --hostname-seed sandbox-1

uv run ucloud-sandboxes vm-public-link-attachment \
  --public-link-id <link-id> \
  --port 8090
```

## Deployment

The normal release path is:

```bash
uv build
uv run ucloud-sandboxes deploy-all-in-one <gateway-job-id> \
  --project <project-id> \
  --deployment-id <deployment-id> \
  --private-network-id <network-id> \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
  --direct-runsc /path/to/ucloud-direct-runsc \
  --direct-runsc-commit <40-character-commit> \
  --managed-init /path/to/managed-init \
  --storage-native-manifest /path/to/storage-native-manifest.json \
  --output script
```

The rendered script stages the control-plane wheel, assembles verified sandbox
and builder bundles, writes service configuration, and installs systemd units.
Run the same command with `--execute` to apply it. See
[deployment-flow.md](deployment-flow.md) for the complete release contract.

## Autoscaler execution

`reconcile` never mutates provider state. Use a one-shot autoscaler cycle for an
operator-controlled mutation:

```bash
uv run ucloud-sandboxes autoscaler-loop --once \
  --project <project-id> \
  --deployment-id <deployment-id> \
  --private-network-id <network-id> \
  --route-file /work/data/ucloud-sandboxes/state/routes.sqlite \
  --execute \
  --execute-stops \
  --execute-init
```

The recurring systemd service uses the same process lock and provider journal.
Create, stop, and node initialization permissions are intentionally distinct.

New nodes are initialized with a role-specific bundle. The autoscaler stages
the local bundle, computes its SHA-256 digest, supplies mandatory deployment and
credential material, and renders VM init only after staging succeeds. `init-vm`
is the operator entry point for replaying that same authenticated bootstrap for
one running job; it requires a local bundle path and does not accept a package
URL or installer specification.

## Credentials

Generated deployments use separate mandatory credentials:

- the gateway token protects the public sandbox API;
- the heartbeat token authorizes only heartbeat publication;
- the node-control token protects every node route except `/healthz`.

The control plane removes caller authorization headers before forwarding and
adds the node-control token itself. Rotate the three credentials independently.
Heartbeat rotation is coordinated because nodes and the control plane must
switch to the same new value before heartbeat publication resumes.

The gateway SSH bootstrap key is also deployment state. Register its public key
with UCloud and keep the private key on the control-plane VM:

```bash
uv run ucloud-sandboxes ensure-ucloud-ssh-key \
  --session-file /work/data/ucloud-sandboxes/state/ucloud-session.json \
  --public-key-file /work/data/ucloud-sandboxes/state/ssh/gateway-init.pub \
  --title "ucloud-sandboxes gateway init"
```

## Operational invariants

- Sandbox CPU, memory, and disk capacity factors are exactly `1.0`.
- Builder capacity and idle policy are independent from sandbox admission.
- A post-start worker suspension is node loss; the autoscaler replaces capacity
  and never trusts the earlier guest disk.
- A node is ready only after verified bootstrap, service health, and a fresh
  authenticated heartbeat.
- Drain closes admission first. Stop execution requires a matching drain token,
  complete empty inventory, and zero owned resources.
- Docker image/cache storage is node-local. The managed registry and published
  storage-native objects are durable.

Bootstrap diagnostics emit `UCLOUD_INIT_PHASE` lines with phase and cumulative
timings. On failure, inspect the init output plus the node, Docker, containerd,
and storage-native journals. Correct the bundle or configuration and replay the
same bootstrap generation; do not repair the node with a different artifact.
