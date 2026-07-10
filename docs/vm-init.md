# VM initialization investigation

Date: 2026-06-28

This note records what is confirmed from the current live UCloud deployment and
what that means for initializing sandbox VM nodes.

## Confirmed behavior

- The live frontend bundle exposes current endpoints including
  `/api/hpc/apps/byNameAndVersion`, `/api/ssh/browse`,
  `/api/jobs/interactiveSession`, and resource attach/detach actions.
- Live retrieval of `vm-ubuntu:24.04` through
  `/api/hpc/apps/byNameAndVersion` returns a `VIRTUAL_MACHINE` backend with
  `invocation.parameters: []` and tool `loadInstructions: null`.
- The observed VM job payloads only carry `diskSize` as an app parameter.
  `sshEnabled` exists as a top-level job submission flag, not as an app
  parameter.
- `GET /api/jobs/init` returns `404` on the current deployment. The same is
  true for the GET form of `requestDynamicParameters`. I did not probe POST
  forms because those are unverified live endpoints that may have side effects.
- `GET /api/ssh/browse` works and returns the user's registered SSH public
  keys. That confirms UCloud has a first-class SSH key resource.
- Live submission of `vm-ubuntu:24.04` with `sshEnabled: true` was rejected
  with `This application does not support SSH but it is required`. Submitting
  the same app without SSH succeeded.
- A no-SSH `vm-ubuntu:24.04` submission on 2026-06-29 started successfully as
  job `12345813` and announced
  `ssh ucloud@ssh.cloud.sdu.dk -p 2523` in its job updates.
- Non-interactive SSH to that announced proxy works with the user's registered
  UCloud SSH key. The VM has passwordless sudo, no Docker/runsc installed by
  default, `/work` mounted, and private-network DNS resolves
  `sandbox-node-live-20260629-1.dfm-sandboxes.ucloud-apps.svc.cluster.local`
  to the VM address.
- The post-boot init script was run successfully on job `12345813` using a
  wheel copied to `/work/ucloud-sandboxes/release/`. It installed Docker
  29.6.1, gVisor `runsc` release `20260622.0`, configured Docker's data root,
  and started the node-agent and heartbeat systemd units.
- Current init keeps Docker's quota-backed XFS data root on local VM disk under
  `/var/lib/ucloud-sandboxes/docker-xfs`. Earlier live nodes put the sparse XFS
  image under `/work`, but `/work` is a virtiofs project mount and is a poor fit
  for high-churn Docker layer extraction and container writable-layer I/O.
- Docker writable-layer quotas require Docker's supported storage path. The
  first Docker setup used the containerd snapshotter-backed `overlayfs` driver,
  where `--storage-opt size=16m` did not stop a 32 MB write under either `runc`
  or `runsc`. Reconfiguring Docker to `overlay2` on an XFS image mounted with
  `pquota` made the same `runsc` probe fail with `ENOSPC`, which is the desired
  behavior.
- UCloud VM private-network interfaces can use an MTU below Docker's default
  bridge MTU. On 2026-07-01 a builder VM had `enp4s0` MTU `1420` while
  Docker's default bridge was `1500`; host PyPI downloads were fast, but
  `docker build` traffic on the default bridge timed out during PyPI TLS
  handshakes. Using host networking for the diagnostic build succeeded, which
  confirmed the problem was container bridge networking rather than PyPI or
  UCloud egress generally. The init script now detects the default-route
  interface MTU and writes Docker's daemon `mtu` setting before starting node
  services.
- A real sandbox API smoke test succeeded on that initialized VM:
  `POST /v1/sandboxes` launched a `busybox` container with `--runtime runsc`,
  `POST /v1/sandboxes/vm-real-1/exec` returned stdout from inside the
  container, and `DELETE /v1/sandboxes/vm-real-1` removed it cleanly.
- The VM product support flags for `cpu-amd-zen5-2-vcpu` include VM terminal,
  VNC, logs, peers, public web access, bind-link-to-port, and suspension.
- The official docs describe `Initialization` as an optional app parameter that
  can run a script during job startup, but this is app-dependent. The live
  `vm-ubuntu:24.04` application does not expose such a parameter.
- The official VM docs say only data in `/work` persists after the VM is
  deleted. They also say Docker can be installed and used inside the VM, VM
  resources can be attached after start, and public links on VMs require a port
  number.
- Live frontend code and API responses confirm private networks are represented
  as job resources shaped like `{"type": "private_network", "id": "..."}`.
  The per-job name inside the private network is the top-level job `hostname`.
- `GET /api/private-networks/browse` and
  `GET /api/private-networks/retrieveProducts` work in the target project.
  `GET /api/private-networks/init` returns `404`.
- Live frontend code confirms private-network creation uses
  `POST /api/private-networks` with a bulk payload containing
  `name`, `subdomain`, and `product`.
- A private network has been created in `DFM Pretraining`:
  `id=12345327`, `name=ucloud-sandboxes`, `subdomain=dfm-sandboxes`,
  `provider=ucloud`. The rejected first subdomain `ucloud-sandboxes` suggests
  UCloud reserves or otherwise rejects some prefixes.

Relevant public docs:

- https://docs.cloud.sdu.dk/guide/submitting.html#optional-parameters
- https://docs.cloud.sdu.dk/guide/submitting.html#configure-ssh-access
- https://docs.cloud.sdu.dk/guide/submitting.html#configure-custom-links
- https://docs.cloud.sdu.dk/Apps/vm_apps.html
- https://docs.cloud.sdu.dk/Apps/vm.html

## Recommendation

Treat VM initialization as our responsibility, not as a UCloud startup script
feature. For stock UCloud VMs, the viable initialization path is a script we run
after the VM has booted and UCloud has announced a supported access channel.

UCloud does not let us use custom VM images for this workload. The VM node
design should therefore assume stock UCloud VM images plus our own post-boot
init layer.

The announced UCloud SSH proxy is currently the viable post-boot access channel
for the observed `vm-ubuntu:24.04` app. The primary path should therefore be:

1. Submit stock `vm-ubuntu:24.04` VM jobs without UCloud SSH.
2. Attach each VM job to the autoscaler private network by adding a
   `private_network` resource and a stable `hostname` in the job submission.
3. Poll `/api/jobs/retrieve?includeUpdates=true` until the VM is running and an
   SSH proxy command has been announced in a job update.
4. Use that SSH command to run an idempotent init script that installs
   Docker, gVisor/runsc, the node agent, and systemd services.
5. If the deployment uses the control-plane HTTP registry, configure Docker's
   `insecure-registries` list during init so builders and sandbox nodes can
   push and pull over the UCloud private network.
6. Store durable init state, caches, node identity, and Docker's quota-backed
   `overlay2` data root under `/work`.
7. Run `ucloud-sandboxes runtime-conformance --sudo --execute` on the VM and
   store the JSON result under `/work/ucloud-sandboxes/state/`.
8. Mark the node ready only after the node agent posts a heartbeat with expected
   capabilities, versions, and a private-network `node_url`. Hard disk capacity
   is only schedulable when that heartbeat includes `disk-quota`.

The long-term optimization path is not a baked VM image. Instead, optimize the
post-boot init layer:

- Keep the init script small, versioned, idempotent, and fast.
- Build a deterministic node package bundle during gateway deployment. It
  contains the service wheel and all platform-specific Python dependency wheels,
  so autoscaled VMs install the node agent with `--no-index` instead of reaching
  PyPI during cold scale-up.
- Extend that artifact with the complete apt dependency closure for bootstrap
  packages, Docker Engine, Buildx/Compose, containerd, and gVisor. Deployment
  downloads the closure into a private empty-status apt cache, so bundle
  contents do not depend on packages or archive-cache entries already present
  on the gateway.
- Record the gateway Ubuntu id, version, codename, and dpkg architecture plus
  every `.deb` size and SHA-256 in the bundle. A node uses the runtime payload
  only on an exact platform match and after verifying the complete file set.
  The compatible happy path installs with `apt-get --no-download` and performs
  no `apt-get update`; repository setup is an explicit compatibility fallback.
- Pull and save the `busybox` image used by runtime conformance into the same
  platform-specific artifact when Docker Hub is reachable during deployment.
  Nodes verify the image archive checksum, load it after Docker is configured,
  and confirm its image ID before probing. If this optional sub-artifact is
  missing or invalid, the existing probe path may pull `busybox` instead.
- Keep the runtime bundle optional at deployment time. If external package
  repositories are unavailable while producing it, deployment still emits the
  Python bundle and cold nodes use the older repository path with a warning.
  Expect the runtime artifact to be roughly 80–150 MB depending on the resolved
  Ubuntu package closure.
- Keep Docker's high-churn data and caches on the quota-backed local XFS volume;
  keep service state and release artifacts under `/work` where persistence is
  useful.
- Use SDK capacity requests to begin billed VM scale-up before sandbox demand
  reaches the create call. Do not rely on suspended nodes as a warm pool because
  they are billed like running nodes in the target environment.
- Treat container images, not VM images, as the reusable sandbox artifact.

## Init script contract

The post-boot init script should be safe to re-run:

- Create `/work/ucloud-sandboxes`.
- Create or select a non-root service user, `ucloud` by default.
- Install any gateway bootstrap public keys into the service user's
  `authorized_keys`.
- If heartbeat bearer auth is enabled, install only the heartbeat-channel token
  into the node-local heartbeat token file with service-user ownership and
  `0600` permissions. Never install the public gateway credential under its
  gateway-token name on a node.
- Install the distinct node-control token at the configured node-local path with
  service-user ownership and `0600` permissions. Pass that file to both
  `serve-node-agent` and the local `agent-heartbeat` fetch. A configured path
  without non-empty source content fails init generation rather than starting
  an unprotected service.
- Install or verify Docker.
- Install or verify gVisor/runsc.
- Prefer the verified platform-specific runtime payload in the staged node
  package bundle. Install only bundled versions newer than the corresponding
  installed packages, and never let apt repair a failed offline transaction by
  downloading implicitly. Fall back explicitly to configured repositories.
- Create a sparse XFS image under `/work`, mount it with `pquota`, and configure
  Docker to use that mount as an `overlay2` data root when hard writable-layer
  quotas are required.
- Configure Docker's bridge MTU from the VM default-route interface so
  containers and build steps inherit the UCloud network MTU instead of Docker's
  default `1500`.
- Raise Docker's per-transfer layer concurrency from its conservative defaults
  to eight downloads and uploads so multi-layer image pulls and builder exports
  use the available private-network bandwidth.
- Install the `ucloud-sandboxes` package or sync a release artifact into a
  service-user-owned virtual environment.
- Record a package fingerprint marker in node state so rerunning init with the
  same staged wheel does not reinstall Python dependencies.
- Write `/etc/systemd/system/ucloud-sandbox-node.service`.
- Enable and restart the node service. Restart is required because rerunning
  init may change flags such as `--execute-runtime` while the old process is
  still active.
- Include the UCloud job id, project id, control-plane URL, node labels, and
  overcommit settings in an environment file.
- Persist runtime conformance JSON and pass it to both the node-agent service
  and heartbeat service so security capabilities are derived from probes rather
  than static labels.
- Emit phase timing log lines for user/key setup, bundle verification, offline
  runtime installation, repository fallback prerequisites, base packages,
  container packages, Docker storage, Docker daemon config, Python package
  install, runtime conformance, and systemd service startup. The autoscaler
  separately records init attempt duration, package staging duration, and
  remote script duration in `vm_init_attempt` metrics.
- Execute independent node initializations with a bounded worker pool. The
  all-in-one deployment admits and initializes up to four ready VMs concurrently
  by default; `--max-init-per-cycle` is both the per-cycle admission limit and
  the concurrency bound.
- Run node-agent and heartbeat systemd units as the service user with Docker
  supplementary group access, not as root. The init script still uses sudo for
  OS package installation, Docker daemon setup, systemd writes, mounts, and the
  runtime conformance probe.
- Bind the node agent on the VM interface, not only loopback, when the node is
  expected to be reachable through a UCloud private network.
- Advertise `http://<private-network-hostname>:<node-agent-port>` in heartbeats
  so the control plane and gateway can call the node agent without public links.
- The generated all-in-one flow installs a deployment-wide node-control token;
  per-node credentials are not yet provisioned.

The control plane should not assume init succeeded just because the command
exited. Readiness should come from the node heartbeat.

## Routing implications

UCloud private networks should be the default control-plane/node data path.
They avoid exposing the node-agent API publicly and give each VM a stable
hostname for internal calls. UCloud public links can still expose a VM port, and
the live target product advertises `jobs.vm.bindLinkToPort`. That is enough to
expose the control-plane/gateway API through one link such as
`12345368 -> app-sandboxes.cloud.sdu.dk`, but not enough by itself to route
directly to arbitrary per-sandbox ports without a mux on the VM.

For the autoscaler, prefer one of these:

- A control-plane gateway that forwards to nodes over reverse tunnels opened by
  the node agents.
- A per-VM gateway process exposed through a UCloud public link on a fixed port,
  with sandbox selection handled by host/path routing inside the VM.
- A private-network-only control plane that calls `node_url` from heartbeats for
  create/delete operations, plus a separate public ingress/gateway only where
  external users need to reach sandboxes.

Avoid requiring every sandbox container to get a UCloud resource attachment.
That would be too slow and too coupled to the VM job API.

## Open questions

- Whether the announced SSH proxy is always available for no-SSH VM submissions
  or whether it depends on project/provider state.
- Are POST forms of `jobs.init` or `requestDynamicParameters` purely metadata
  endpoints? I avoided probing them without explicit approval because live POST
  calls may be mutating.
- Can resource attach/detach be automated safely for public links after a VM
  starts, or should these be included in the initial job request?
