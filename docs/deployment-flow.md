# Deployment flow

This project should deploy as a versioned control plane plus versioned VM nodes.
Each node should advertise enough metadata for the control plane to decide
whether it is usable, stale, foreign, or safe to terminate.

## Deployment identity

Every production-like run should choose a deployment id, for example
`dfm-sandboxes-dev-20260629` or `prod-a`.

Use that id consistently:

- control-plane config: `deployment_id`
- CLI override: `--deployment-id <id>`
- UCloud job labels: `ucloud-sandboxes/deployment=<id>`
- heartbeat field: `deployment_id=<id>`

When `deployment_id` is configured, pool discovery ignores VM jobs from other
deployments. Scale-down execution also refuses to terminate jobs without the
matching deployment label unless `--allow-unlabeled-stops` is passed.

## Versioning contract

Nodes now advertise:

- `agent_version`: Python package/node-agent version
- `init_version`: VM init script contract version
- `deployment_id`: owning deployment

VM job submissions are also labelled with:

- `ucloud-sandboxes/agent-version`
- `ucloud-sandboxes/init-version`
- `ucloud-sandboxes/deployment`

The control plane should treat mismatched versions as not ready for new
sandboxes. The intended rollout flow is:

1. Choose a package version, update `pyproject.toml` and
   `ucloud_sandboxes/__init__.py`, and tag the release, for example `v0.2.0`.
2. Build a wheel: `uv build`.
3. Upload the wheel to a release location reachable from VM init, initially
   `/work/ucloud-sandboxes/release/`.
4. Start or update the control-plane VM with the new wheel and deployment id.
5. Verify the gateway and relay public health endpoints report the new package
   version before allowing new nodes:
   `curl -fsS https://app-sandboxes.cloud.sdu.dk/healthz` and
   `curl -fsS https://app-sandboxes-relay-v2.cloud.sdu.dk/healthz`.
6. Generate a dedicated gateway SSH keypair under gateway state and run
   `ensure-ucloud-ssh-key` with the public key so first-boot node SSH accepts
   the gateway key.
7. Start the public gateway service, model relay service, and autoscaler service
   from the same installed wheel.
8. Start the private Docker registry service on the control-plane VM. Builder
   output must be pushed to a durable registry tag before sandbox nodes can pull
   it.
9. Run the autoscaler with `--execute` and `--execute-init`.
10. Let the autoscaler submit sandbox-node and builder VMs with matching
   deployment/version labels.
11. Let the autoscaler run post-boot init over the UCloud-announced SSH command
   using `--init-package-spec /work/...whl`,
   `--init-authorized-key-file`, and `--init-ssh-private-key-file`.
12. Wait for matching heartbeats before scheduling sandboxes to those nodes.
13. Drain old/mismatched nodes, then scale them down only after they are idle.

## Credentials

The current live tests use the imported UCloud browser/CLI session. That works
for development, but it is not a clean production trust model:

- it is effectively a user session, not a narrowly scoped service credential
- anyone with read access to the control-plane VM session file can submit or
  terminate jobs as that user within the accessible project scope
- refresh lifetime and revocation behavior are tied to UCloud session behavior,
  not to this autoscaler's deployment lifecycle
- it is easy to accidentally copy the credential into images, logs, or release
  artifacts if the deployment flow is not strict

For the first control-plane VM test, copying an imported session file is
acceptable if it is treated as a secret: store it outside the repo, mode `0600`,
owned by the service user, and rotate it after testing.

For production, prefer a dedicated UCloud service user or service credential if
UCloud supports one. It should have only the required project permissions for
the sandbox project, and its session/token should be injected as deployment
secret state, not baked into VM images or wheels.

## Cleanup safety

Termination must only target nodes known to belong to the active deployment.
The safe default is:

- observe only jobs with matching `deployment_id` when one is configured
- create sandbox nodes with `ucloud-sandboxes/node=true`
- create builder nodes with `ucloud-sandboxes/builder=true`
- create nodes with `ucloud-sandboxes/deployment=<id>`
- block `--execute-stops` for jobs lacking that exact label

Manual cleanup can still use `--allow-unlabeled-stops`, but that should remain
an explicit operator action, not part of the autoscaler loop.

## First production-shape test

The live production-shape deployment is intentionally one UCloud VM. It runs the
public gateway, model relay, private registry, registry GC timer, and autoscaler
on the same machine. Autoscaled sandbox and builder VMs are separate only when
there is sandbox or image-build demand.

Current live all-in-one VM:

- job id: `12349450`
- name: `ucloud-sandbox-gateway-allinone-20260704-v020`
- deployment id: `live-20260629`
- package version: `0.2.0`
- private network: `12345327`
- private service address: `10.40.34.183`
- persistent project drive: `/998037`, mounted by UCloud as `/work/data`
- SSH: resolve with `ucloud jobs ssh 12349450 --print-only`

Public links:

- gateway ingress `12345368`: `https://app-sandboxes.cloud.sdu.dk` -> VM port
  `8090`
- relay ingress `12349454`: `https://app-sandboxes-relay-v2.cloud.sdu.dk` -> VM
  port `8092`
- stale old relay ingress `12346842`: `https://app-sandboxes-relay.cloud.sdu.dk`
  is still reported by UCloud as bound to stopped job `12346251`; do not use it
  unless UCloud support clears that stale binding

Services on the all-in-one VM:

- `ucloud-sandbox-gateway.service`
- `ucloud-sandbox-relay.service`
- `ucloud-sandbox-registry.service`
- `ucloud-sandbox-registry-gc.timer`
- `ucloud-sandbox-autoscaler.service`

The registry is intentionally local to the all-in-one VM and persistent on the
project drive. The gateway reaches it as `http://127.0.0.1:5000`; autoscaled
builder and sandbox nodes reach the same registry over the private network by
using image tags under `ucloud-sandbox-registry:5000` plus a VM init host alias:

```bash
--init-docker-insecure-registry ucloud-sandbox-registry:5000 \
--init-host-alias ucloud-sandbox-registry=10.40.34.183
```

Registry data lives at:

```text
/work/data/ucloud-sandbox-registry/docker-registry
```

The registry VM used during earlier tests did not leave a usable persistent
catalog in that path, so images needed by the one-VM deployment should be
rebuilt or pushed again under `ucloud-sandbox-registry:5000/...`.

## All-in-One VM Setup

Install Docker, the wheel, and the systemd units on the all-in-one VM:

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

The relay service is deployed from the same wheel as the gateway:

```bash
sudo install -m 0644 deploy/systemd/ucloud-sandbox-relay.service \
  /etc/systemd/system/ucloud-sandbox-relay.service
sudo systemctl daemon-reload
sudo systemctl enable --now ucloud-sandbox-relay.service
```

Create the relay token files before starting the service:

```bash
install -d -m 0700 /work/ucloud-sandboxes/state
umask 077
[ -s /work/ucloud-sandboxes/state/relay-sandbox-token ] \
  || openssl rand -hex 32 > /work/ucloud-sandboxes/state/relay-sandbox-token
[ -s /work/ucloud-sandboxes/state/relay-worker-token ] \
  || openssl rand -hex 32 > /work/ucloud-sandboxes/state/relay-worker-token
```

The sandbox token becomes `OPENAI_API_KEY` inside sandboxes. The worker token is
used by the LUMI-side worker for rollout registration, polling, lease renewal,
responses, and errors. For long inference calls, keep
`--worker-lease-seconds` moderate, such as 600 seconds, and have workers renew
leases every minute or two until local inference returns.

Install and start the gateway and autoscaler units from the same wheel. The
gateway must point at the local registry:

```bash
sudo install -m 0644 deploy/systemd/ucloud-sandbox-gateway.service \
  /etc/systemd/system/ucloud-sandbox-gateway.service
sudo install -m 0644 deploy/systemd/ucloud-sandbox-autoscaler.service \
  /etc/systemd/system/ucloud-sandbox-autoscaler.service
sudo systemctl daemon-reload
sudo systemctl enable --now ucloud-sandbox-gateway.service
sudo systemctl enable --now ucloud-sandbox-autoscaler.service
```

The live autoscaler runs every 5 seconds, uses a 600 second sandbox-node idle
timeout, uses a 900 second builder-node idle timeout, and scales builders to
zero when no builds or builder prepare signals remain.

UCloud public links are bound to one VM-local port. The live all-in-one VM has
public link `12345368` bound to port `8090` for the sandbox gateway and public
link `12349454` bound to port `8092` for the model relay.

To create and bind a relay ingress on a running VM:

```bash
ucloud request POST /api/ingresses \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --json '{"type":"bulk","items":[{"domain":"app-sandboxes-relay-v2.cloud.sdu.dk","product":{"id":"u1-publiclink","category":"u1-publiclink","provider":"ucloud"}}]}'

ucloud request POST /api/jobs/attachResource \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --json '{"jobId":"12349450","resource":{"type":"ingress","id":"12349454","port":8092}}'

uv run ucloud-sandboxes open-vm-web 12349450 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --port 8092
```

The live relay smoke path is
`https://app-sandboxes-relay-v2.cloud.sdu.dk/rollouts/<rollout-id>/v1/chat/completions`.

## Autoscaled Nodes

Sandbox-node VMs:

- autoscaled from pending sandbox resource demand or
  `POST /v1/sandboxes/prepare` signals
- initialized over the announced UCloud SSH proxy
- initialized with the local registry alias:
  `--init-docker-insecure-registry ucloud-sandbox-registry:5000` and
  `--init-host-alias ucloud-sandbox-registry=<gateway-private-ip>`
- heartbeat back to the all-in-one gateway private-network URL with bearer auth
- carry `ucloud-sandboxes/deployment=<deployment-id>`

Builder-node VMs:

- autoscaled from pending image-build demand or `POST /v1/builders/prepare`
  signals
- initialized over the announced UCloud SSH proxy
- initialized with the same local registry alias
- advertise `image-build` only, not `sandbox`
- advertise physical CPU, memory, and disk capacity only; sandbox overcommit
  settings are ignored for builder nodes
- build and push registry tags; sandbox nodes later pull/cache those tags
- carry `ucloud-sandboxes/deployment=<deployment-id>`

Builds should use `push=true` and tags under
`ucloud-sandbox-registry:5000`; sandbox create can then use either the registry
tag or the recorded image id. If the all-in-one VM is replaced and receives a
new private IP, keep image tags the same and update only the host alias value in
the deployment.

The gateway VM cannot SSH into node VMs through `ssh.cloud.sdu.dk` unless it has
an accepted private key. The bootstrap path is to generate a dedicated keypair
on the gateway, keep the private key in gateway state, register the public key
with UCloud using `ensure-ucloud-ssh-key`, pass the public key into autoscaled
node init with `--init-authorized-key-file`, and have the autoscaler use
`--init-ssh-private-key-file` when it runs post-boot initialization. The
autoscaler records attempts in `<state_dir>/vm-bootstrap.json` and retries after
`--init-retry-seconds` (30 seconds in the live setup); readiness still comes only
from fresh node heartbeats.

The live all-in-one gateway key has been registered in UCloud as SSH key id
`3212` with title `ucloud-sandboxes gateway init 2026-07-04 allinone`.

## Verify

Current live checks:

```bash
curl -fsS https://app-sandboxes.cloud.sdu.dk/healthz
curl -fsS https://app-sandboxes-relay-v2.cloud.sdu.dk/healthz
curl -i https://app-sandboxes.cloud.sdu.dk/v1/sandboxes
ucloud jobs browse \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --filter-state RUNNING \
  --no-include-application \
  --output json
```

Expected current state:

- gateway health reports `{"ok": true, "service": "control-plane", "version": "0.2.0"}`
- relay health reports `{"ok": true, "service": "model-relay", "version": "0.2.0"}`
- unauthenticated `GET /v1/sandboxes` returns `401`
- running-job browse shows only all-in-one job `12349450` for this service when
  there is no sandbox or builder demand
- gateway-local `GET http://127.0.0.1:5000/v2/_catalog` returns registry JSON
- zero demand plus idle timeout produces safe labelled stop intents for sandbox
  and builder nodes
