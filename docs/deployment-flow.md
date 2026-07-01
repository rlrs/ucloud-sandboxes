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

1. Build a wheel: `uv build`.
2. Upload the wheel to a release location reachable from VM init, initially
   `/work/ucloud-sandboxes/release/`.
3. Start or update the control-plane VM with the new wheel and deployment id.
4. Generate a dedicated gateway SSH keypair under gateway state and run
   `ensure-ucloud-ssh-key` with the public key so first-boot node SSH accepts
   the gateway key.
5. Start the public gateway service, model relay service, and autoscaler service
   from the same installed wheel.
6. Start the private Docker registry service on the control-plane VM. Builder
   output must be pushed to a durable registry tag before sandbox nodes can pull
   it.
7. Run the autoscaler with `--execute` and `--execute-init`.
8. Let the autoscaler submit sandbox-node and builder VMs with matching
   deployment/version labels.
9. Let the autoscaler run post-boot init over the UCloud-announced SSH command
   using `--init-package-spec /work/...whl`,
   `--init-authorized-key-file`, and `--init-ssh-private-key-file`.
10. Wait for matching heartbeats before scheduling sandboxes to those nodes.
11. Drain old/mismatched nodes, then scale them down only after they are idle.

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

The first public-gateway smoke test uses two UCloud VMs on the same private
network:

1. Public control-plane VM:
   - submitted with `submit-vm --role gateway`
   - submitted with private network `12345327`
   - submitted with public link `12345368` bound to the gateway API port
   - live relay public link `12346842` is attached to the same VM and bound to
     relay port `8092`
   - opened with `open-vm-web <job-id> --port 8090` after the gateway service
     is listening; without this UCloud's ingress can return `449` even though
     the public link resource is bound to the VM job
   - opened with `open-vm-web <job-id> --port 8092` after the relay service is
     listening for the same reason
   - live public gateway job id: `12346251`
   - install the wheel in `/work/ucloud-sandboxes/gateway-venv`
   - store the UCloud session secret outside the repo
   - store the gateway bearer token outside the repo
   - run `serve-control-plane --host 0.0.0.0 --port 8090`
   - pass `--route-file` and `--heartbeat-file`; gateway route lookups are
     in-memory, while the route file keeps recovery state and pending demand for
     the autoscaler
   - pass `--gateway-bearer-token-file` before binding a public link
   - pass `--registry-url http://sandbox-gateway-registry-mount-07011413:5000`
     so `/v1/metrics` and the dashboard show the project-backed registry health
   - pass `--enable-image-builds --execute-image-builds` only on a
     build-capable control-plane machine with registry access
   - run `serve-model-relay --host 0.0.0.0 --port 8092` as
     `ucloud-sandbox-relay.service`
   - store separate relay sandbox and worker bearer tokens outside the repo
   - run `autoscaler-loop` with the same state dir and UCloud session file
   - initialize future builders and sandbox nodes with
     `--init-docker-insecure-registry ucloud-sandbox-registry:5000` and
     `--init-host-alias ucloud-sandbox-registry=10.36.120.195`, where
     `10.36.120.195` is the current private-network DNS address for the registry
     VM
2. Private registry VM:
   - live registry job id: `12347774`
   - submitted with private network `12345327`
   - submitted with project storage mounted for registry persistence. The
     validated DFM Pretraining deployment mounts the project `data` drive as
     `--mount /998037`, which appears inside the VM as `/work/data`; registry
     data then lives below that mount.
   - run `ucloud-sandbox-registry.service` with `UCLOUD_REGISTRY_DATA_DIR`
     below the mounted project path:
     `/work/data/ucloud-sandbox-registry/docker-registry`
   - enable `ucloud-sandbox-registry-gc.timer` and run
     `ucloud-sandboxes registry-prune` for tag retention before GC
   - do not attach a public link; builders, sandbox nodes, and the gateway reach
     it over the private network as
     `sandbox-gateway-registry-mount-07011413:5000`
3. Sandbox-node VMs:
   - autoscaled from pending sandbox resource demand
   - initialized over the announced UCloud SSH proxy
   - if using the control-plane HTTP registry, initialized with
     `--init-docker-insecure-registry ucloud-sandbox-registry:5000` and
     `--init-host-alias ucloud-sandbox-registry=<gateway-private-ip>`
   - heartbeats back to the control-plane private-network URL with bearer auth
   - carry `ucloud-sandboxes/deployment=<deployment-id>`
4. Builder-node VMs:
   - autoscaled from pending image-build demand or `POST /v1/builders/prepare`
     signals
   - initialized over the announced UCloud SSH proxy
   - if using the control-plane HTTP registry, initialized with
     `--init-docker-insecure-registry ucloud-sandbox-registry:5000` and
     `--init-host-alias ucloud-sandbox-registry=<gateway-private-ip>`
   - advertise `image-build` only, not `sandbox`
   - advertise physical CPU, memory, and disk capacity only; sandbox overcommit
     settings are ignored for builder nodes
   - build and push registry tags; sandbox nodes later pull/cache those tags
   - carry `ucloud-sandboxes/deployment=<deployment-id>`
5. Verify:
   - public `GET /healthz` returns 200
   - unauthenticated public `GET /v1/sandboxes` returns 401
   - authenticated public sandbox create/exec/delete works through the gateway
   - route file is empty after cleanup
   - autoscaler loop observes fresh heartbeats plus pending sandbox, pending
     image-build, prepared sandbox, and prepared builder demand
   - local `GET http://127.0.0.1:8092/healthz` returns 200 on the gateway VM
   - public `GET https://app-sandboxes-relay.cloud.sdu.dk/healthz` returns 200
   - unauthenticated public `GET /v1/relay/stats` on the relay returns 401
   - gateway-local `GET http://127.0.0.1:5000/v2/_catalog` returns registry JSON
   - zero demand plus idle timeout produces safe labelled stop intents for sandbox
     and builder nodes

The private registry can run either on the public control-plane VM or on a
dedicated private-network VM. The current live deployment uses a dedicated
registry VM so the public gateway could be moved to project-backed registry
storage without moving public links. It is intentionally not exposed through a
public link; builders and sandbox nodes reach it over the UCloud private
network. Install Docker, the registry unit, and the GC timer:

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

Builder and sandbox node init must trust the registry if it is served as HTTP.
Use a stable registry alias in image tags and map it to the gateway's current
private-network address during init:

```bash
--init-docker-insecure-registry ucloud-sandbox-registry:5000 \
--init-host-alias ucloud-sandbox-registry=<gateway-private-ip>
```

Builds should use `push=true` and tags under
`ucloud-sandbox-registry:5000`; sandbox create can then use either the registry
tag or the recorded image id. If the gateway VM is replaced and receives a new
private IP, keep the tags the same and update only the host alias value in the
deployment.

The relay service is deployed from the same wheel as the gateway. The checked-in
unit template is `deploy/systemd/ucloud-sandbox-relay.service`; install it on
the control-plane VM with:

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

UCloud public links are bound to one VM-local port. The live gateway VM has
public link `12345368` bound to port `8090` for the sandbox gateway and public
link `12346842` bound to port `8092` for the model relay.

To create and bind a relay ingress on a running VM:

```bash
ucloud request POST /api/ingresses \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --json '{"type":"bulk","items":[{"domain":"app-sandboxes-relay.cloud.sdu.dk","product":{"id":"u1-publiclink","category":"u1-publiclink","provider":"ucloud"}}]}'

ucloud request POST /api/jobs/attachResource \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --json '{"jobId":"12346251","resource":{"type":"ingress","id":"12346842","port":8092}}'

uv run ucloud-sandboxes open-vm-web 12346251 \
  --project 4827bd3a-4e74-4393-9b82-49f71636c141 \
  --port 8092
```

The live relay smoke path is
`https://app-sandboxes-relay.cloud.sdu.dk/rollouts/<rollout-id>/v1/chat/completions`.

The gateway VM cannot SSH into node VMs through `ssh.cloud.sdu.dk` unless it has
an accepted private key. The bootstrap path is to generate a dedicated keypair
on the gateway, keep the private key in gateway state, register the public key
with UCloud using `ensure-ucloud-ssh-key`, pass the public key into autoscaled
node init with `--init-authorized-key-file`, and have the autoscaler use
`--init-ssh-private-key-file` when it runs post-boot initialization. The
autoscaler records attempts in `<state_dir>/vm-bootstrap.json` and retries after
`--init-retry-seconds` (30 seconds in the live setup); readiness still comes only
from fresh node heartbeats.

The live gateway key has been registered in UCloud as SSH key id `3195` with
title `ucloud-sandboxes gateway init 2026-06-29`.
