# CLI and operations

The autoscaler is dry-run by default. Its single `--execute` flag authorizes
provider create/stop calls, route reconciliation, and VM initialization.

## Configuration and inspection

Print a starter configuration:

```bash
uv run ucloud-sandboxes sample-config
```

Cloud settings are contained under the tagged `provider` object. The built-in
value is `{"kind": "ucloud", ...}`; installed adapters are selected by another
kind without changing reconciliation or policy. See
[provider-portability.md](provider-portability.md).

Inspect a UCloud job:

```bash
uv run ucloud-sandboxes inspect-job --config /path/to/deployment.json <job-id>
```

Preview one complete autoscaler cycle without changing provider or routing
state:

```bash
uv run ucloud-sandboxes autoscaler --once \
  --config /path/to/deployment.json \
  --output json
```

`control-state.sqlite` is the shared gateway/autoscaler authority for node
heartbeats and VM bootstrap retry state. It has no JSON import or migration
path; use the same file for `serve-control-plane`, `autoscaler`, and
`heartbeats`.

Use `--jobs-file` for an offline provider-inventory fixture. Automatic
discovery accepts only jobs carrying the matching deployment label and an
explicit sandbox or builder ownership label. `--include-job <id>` is the sole
rescue path for inspecting a known job outside that ownership filter; names and
name prefixes never establish ownership or role.

## VM submission

Render a VM job payload:

```bash
uv run ucloud-sandboxes submit-vm \
  --config /path/to/deployment.json \
  --role sandbox \
  --hostname-seed sandbox-1 \
  --output json
```

Roles are `gateway`, `sandbox`, and `builder`. Add `--execute` only after
reviewing the payload. Helper commands render private-network and public-link
resource fragments:

```bash
uv run ucloud-sandboxes vm-network-attachment \
  --config /path/to/deployment.json \
  --hostname-seed sandbox-1

uv run ucloud-sandboxes vm-public-link-attachment \
  --config /path/to/deployment.json \
  --port 8090
```

## Deployment

The normal release path is:

```bash
uv build
uv run ucloud-sandboxes deploy-all-in-one <gateway-job-id> \
  --config /path/to/deployment.json \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
  --direct-runsc /path/to/ucloud-direct-runsc \
  --managed-init /path/to/managed-init \
  --storage-native-manifest /path/to/storage-native-manifest.json \
  --output script
```

The rendered script stages the control-plane wheel, assembles verified sandbox
and builder bundles, writes service configuration, and installs systemd units.
Run the same command with `--execute` to apply it. See
[deployment-flow.md](deployment-flow.md) for the complete release contract.

## Autoscaler execution

Add `--execute` to apply a reviewed cycle. `--once` exits after that cycle;
without it, the same command runs continuously:

```bash
uv run ucloud-sandboxes autoscaler --once \
  --config /path/to/deployment.json \
  --execute
```

The recurring systemd service uses the same process lock and provider journal.
All controller mutations share the same execution fence and process lock.

New nodes are initialized with a role-specific bundle. The autoscaler stages
the local bundle, computes its SHA-256 digest, supplies mandatory deployment and
credential material, and renders VM init only after staging succeeds. `init-vm`
is the operator entry point for replaying that same authenticated bootstrap for
one running job; it requires a local bundle path and does not accept a package
URL or installer specification.

## Credentials

Generated deployments use separate mandatory credentials:

- the sandbox API key protects the least-privileged public SDK routes;
- the gateway control token protects operator and controller routes;
- the heartbeat token authorizes only heartbeat publication;
- the node-control token protects every node route except `/healthz`.

The control plane removes caller authorization headers before forwarding and
adds the node-control token itself. Heartbeat rotation is coordinated because
nodes and the control plane must switch to the same new value before heartbeat
publication resumes.

Distribute the public HTTPS URL and the contents of `sandbox-api-token` to SDK
users. Keep `gateway-token` on the control-plane host for operator tooling,
autoscaling, and the model-relay lifecycle bridge. Rotate the four gateway/node
credentials independently.

The gateway SSH bootstrap key is also deployment state. Register its public key
with UCloud and keep the private key on the control-plane VM:

```bash
uv run ucloud-sandboxes ensure-ucloud-ssh-key \
  --config /path/to/deployment.json \
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
