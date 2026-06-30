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
5. Run the autoscaler with `--execute` and `--execute-init`.
6. Let the autoscaler submit sandbox-node and builder VMs with matching
   deployment/version labels.
7. Let the autoscaler run post-boot init over the UCloud-announced SSH command
   using `--init-package-spec /work/...whl`,
   `--init-authorized-key-file`, and `--init-ssh-private-key-file`.
8. Wait for matching heartbeats before scheduling sandboxes to those nodes.
9. Drain old/mismatched nodes, then scale them down only after they are idle.

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

1. Control-plane VM:
   - submitted with `submit-vm --role gateway`
   - submitted with private network `12345327`
   - submitted with public link `12345368` bound to the gateway API port
   - opened with `open-vm-web <job-id> --port 8090` after the gateway service
     is listening; without this UCloud's ingress can return `449` even though
     the public link resource is bound to the VM job
   - live job id: `12346251`
   - install the wheel in `/work/ucloud-sandboxes/gateway-venv`
   - store the UCloud session secret outside the repo
   - store the gateway bearer token outside the repo
   - run `serve-control-plane --host 0.0.0.0 --port 8090`
   - pass `--route-file` and `--heartbeat-file`; gateway route lookups are
     in-memory, while the route file keeps recovery state and pending demand for
     the autoscaler
   - pass `--gateway-bearer-token-file` before binding a public link
   - pass `--enable-image-builds --execute-image-builds` only on a
     build-capable control-plane machine with registry access
   - run `autoscaler-loop` with the same state dir and UCloud session file
2. Sandbox-node VMs:
   - autoscaled from pending sandbox resource demand
   - initialized over the announced UCloud SSH proxy
   - heartbeats back to the control-plane private-network URL with bearer auth
   - carry `ucloud-sandboxes/deployment=<deployment-id>`
3. Verify:
   - public `GET /healthz` returns 200
   - unauthenticated public `GET /v1/sandboxes` returns 401
   - authenticated public sandbox create/exec/delete works through the gateway
   - route file is empty after cleanup
   - autoscaler loop observes fresh heartbeats plus pending sandbox and image-build demand
   - zero demand plus idle timeout produces safe labelled stop intents for sandbox
     and builder nodes

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
