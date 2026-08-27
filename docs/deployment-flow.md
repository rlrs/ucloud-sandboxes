# Deployment flow

A deployment consists of one durable control-plane VM and two autoscaled node
roles. The control plane runs the public gateway, model relay, private registry,
registry maintenance, and autoscaler. Sandbox nodes run the direct gVisor
Warden and storage-native service. Builder nodes run Docker image operations.

## Deployment identity

Choose one non-empty deployment ID and use it everywhere:

- control-plane and autoscaler configuration;
- UCloud job labels;
- node bootstrap inputs;
- authenticated heartbeats.

The control plane admits only jobs and heartbeats with that exact identity.
Package and init versions are also carried in job labels and heartbeats so a
node built from a different release never receives new work.

## Persistent and ephemeral state

Mount durable project storage at `/work/data`. The deployment stores gateway
databases, operation journals, registry data, credentials, and the gateway SSH
bootstrap key below that mount. Systemd units require the mount before starting;
a missing mount is a deployment failure.

Sandbox and builder VM disks are ephemeral. They contain runtime state, image
caches, and storage-native caches only. The registry and published
storage-native objects are the durable authorities.

## Release inputs

Build the Python wheel locally:

```bash
uv build
```

`deploy-all-in-one` also requires the sandbox runtime artifacts:

- the exact patched direct `runsc` binary and its 40-character source commit;
- the managed PID 1 binary;
- the pinned storage-native backend manifest, binary, provenance, patches, and
  license.

The remote deployment host assembles two role-specific node bundles. Both
contain a preassembled Python runtime, pinned Debian packages, and the exact
host-kernel module closure. The sandbox bundle additionally contains the direct
runtime and storage-native artifacts. The builder bundle contains Docker Buildx.
Bundle construction is mandatory; deployment stops if any input cannot be
built, downloaded, or verified.

Nodes never contact package repositories. The autoscaler stages one bundle over
SSH, supplies its SHA-256 digest to VM init, and the node verifies the archive,
manifest, role, platform, kernel, packages, modules, and artifacts before
installing anything.

## Create the control-plane VM

The VM must join the worker private network, mount the durable project drive,
and bind public links to the gateway and relay ports. Render a job first, then
submit it explicitly:

```bash
uv run ucloud-sandboxes submit-vm \
  --config /path/to/deployment.json \
  --role gateway \
  --mount <project-drive-id> \
  --hostname-seed gateway-1 \
  --output json
```

Add `--execute` only after inspecting the payload. Attach the relay link to port
`8092` through UCloud when it is a separate ingress resource.

## Converge the control plane

Render the remote convergence script first:

```bash
uv run ucloud-sandboxes deploy-all-in-one <gateway-job-id> \
  --config /path/to/deployment.json \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
  --direct-runsc /path/to/ucloud-direct-runsc \
  --managed-init /path/to/managed-init \
  --storage-native-manifest /path/to/storage-native-manifest.json \
  --output script
```

Run the same command with `--execute` to stage the release, build the verified
node bundles, install the exact `deployment.json`, create credentials, install
systemd units, restart services, register the gateway SSH key, and open the
configured web ports.

UCloud deployment and the Hetzner gateway installer both finish through the
same `gateway-reconcile` command. It is the sole service-convergence routine:
it reloads systemd, applies registry and snapshot-GC timer state, restarts the
gateway, relay, registry, and autoscaler, and waits for the registry and HTTP
health endpoints. Provider installers do not carry independent restart order
or health logic.

For rolling upgrades, update and verify the gateway before admitting workers
from the new bundle. Gateway decoders accept the previous worker heartbeat
shape, while an older gateway deliberately rejects unknown protocol fields.
After the gateway reports the new package version, replace or restart workers;
do not roll back only the gateway while newer workers remain admitted.

The deployment creates independent secrets for:

- least-privileged public SDK access;
- privileged gateway control;
- node heartbeats;
- privileged node control;
- sandbox-to-relay access;
- relay workers.

They are mandatory and authorize different routes. Give SDK users only the
`sandbox-api-token`; do not copy the gateway control credential to clients or
nodes, and do not reuse one token for multiple channels.

## Autoscaled nodes

Run the autoscaler with create, stop, and initialization execution enabled.
Create, stop, and bootstrap remain separate explicit permissions so operators
can inspect each mutation class independently.

Sandbox nodes:

- carry `ucloud-sandboxes/node=true` and the deployment/version labels;
- use exact physical CPU, memory, and disk accounting;
- start the direct Warden and required storage-native services;
- report readiness only after authenticated bootstrap and health checks.

Builder nodes:

- carry `ucloud-sandboxes/builder=true` and never the sandbox-node label;
- expose image pull, build, cache, health, heartbeat, and drain operations;
- push gateway-assigned tags to the private registry with Buildx;
- scale independently from sandbox capacity.

The gateway resolves managed image IDs to immutable registry digests. Clients do
not receive or construct the worker-private registry address.

## Provider and cleanup safety

Provider mutations are journaled and deployment-scoped. Automatic termination
targets only jobs with the exact deployment and role labels. Unlabelled cleanup
requires an explicit operator override and is not part of the service loop.

A worker suspension after the VM has started is node loss: the guest disk and
live sandbox processes are not trusted afterward. Placement excludes the lost
node and replacement capacity is created from durable demand. The service does
not recover a worker by booting an earlier guest state.

A drain closes admission before the node reports progress. The autoscaler may
stop a node only after a fresh authenticated heartbeat confirms the matching
drain token, complete empty inventory, and zero resource ownership.

## Verification

After deployment, verify:

```bash
curl -fsS https://<gateway-domain>/healthz
curl -fsS https://<relay-domain>/healthz
curl -i https://<gateway-domain>/v1/sandboxes
curl -fsS -H "X-UCloud-Sandbox-Token: $(cat <state>/sandbox-api-token)" \
  https://<gateway-domain>/v1/sandboxes
```

Both health endpoints must report the installed package version. The
unauthenticated sandbox request must return `401`, while the SDK-key request
must return a sandbox list. Also verify the local registry `/v2/` endpoint,
systemd service state, the autoscaler journal, and one complete builder-push
plus sandbox-pull flow before accepting production traffic.
