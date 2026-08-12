# Hetzner Cloud deployment

Hetzner Cloud is a viable host for the sandbox runtime. The original CX43
qualification in HEL1 passed the kernel, ublk, XFS, namespace, nftables,
Docker-in-Docker, patched gVisor, and storage-native hibernation tests. The
current CPX62 golden-image source independently passed the host suite on Ubuntu
26.04 and kernel 7.0, including ublk, XFS project quotas, Docker-in-Docker, and
gVisor systrap. The original performance evidence is recorded in
[`benchmarks/hetzner-qualification-2026-08-12.json`](benchmarks/hetzner-qualification-2026-08-12.json).

The repository now has a built-in `hetzner` compute provider for autoscaled
sandbox and builder nodes. It supports system images or snapshot IDs, strict
provider configuration, exhaustive inventory, private-IP SSH bootstrap, and
ambiguity-safe create/delete operations. The gateway and foundational Hetzner
resources still need to be provisioned separately.

## Provider configuration

`scope_id` is a stable local name for the Hetzner project. The API token itself
selects the real Hetzner project. The token is loaded lazily from the named
environment variable and is never written to deployment state.

```json
{
  "kind": "hetzner",
  "scope_id": "sandbox-production",
  "api_token_env": "HETZNER_API_KEY",
  "network_id": 1234567,
  "location": "hel1",
  "sandbox_server_type": "cpx62",
  "sandbox_image": 23456789,
  "builder_server_type": "cpx62",
  "builder_image": 23456790,
  "ssh_user": "root",
  "ssh_key_ids": [3456789],
  "firewall_ids": [4567890],
  "enable_ipv4": false,
  "enable_ipv6": false,
  "enable_private_egress": true,
  "private_dns_servers": ["1.1.1.1", "8.8.8.8"]
}
```

`sandbox_image` and `builder_image` accept either a positive image/snapshot ID
or a system image name such as `ubuntu-26.04`. Snapshot IDs are recommended for
normal operation. `network_id`, both role images, and a non-empty `scope_id`
are required, as is at least one `ssh_key_ids` entry so bootstrap never depends
on a generated root password. Provider fields are exact; misspelled or unknown
fields fail configuration loading. Enabling either public address family also
requires at least one `firewall_ids` entry.

Workers default to private-only networking to avoid public-IP cost and public
exposure. A private-only worker needs gateway routing/NAT for any required
Internet egress. The provider uses its address on `network_id` as both the SSH
bootstrap target and advertised node address; it does not depend on private
DNS. With `enable_private_egress`, cloud-init installs a default route through
the Hetzner Network router and writes a static resolver list from
`private_dns_servers` before SSH bootstrap completes. A static resolver file is
intentional because Hetzner's private-only DHCP lease supplies no DNS. The
Network itself must have `0.0.0.0/0` routed to the gateway's private IP, and the
gateway must forward and masquerade that traffic.
Public networking and private egress are intentionally mutually exclusive.
Set `enable_ipv4` only when public egress is needed before gateway NAT is
available, and attach a restrictive firewall when doing so. Hetzner Cloud
Firewalls do not filter private-network traffic, so the provider omits them
from private-only creates and the guest nftables policy remains authoritative
on that path.

The pool resource values also need to match the server shape. The selected
initial sandbox shape, CPX62, has 16 shared vCPU, 32 GiB memory, and 640 GB
local disk in the live API. Configure those values instead of the repository's
32-vCPU/2-TB UCloud defaults. Docker quota, swap, storage cache, and
direct-runtime headroom must fit inside that disk.

The current base image is Hetzner's x86 `ubuntu-26.04` system image. Ubuntu
26.04 carries the required modules in the main kernel package and does not
publish `linux-modules-extra-$(uname -r)` for this kernel, so bootstrap probes
and deployment setup treat that package as optional after verifying that every
required module is actually present.

On CPX62, advertise 30,720 MiB of schedulable memory rather than the catalog's
nominal 32 GiB: the qualified guest exposed 31,326 MiB after platform/kernel
reservation. This leaves a small host margin instead of admitting work against
memory the guest cannot use.

Use CPX12 for the initial gateway: 1 shared vCPU, 2 GiB memory, and 40 GB local
disk. Move up only if registry, relay, or request concurrency measurements
justify it. Gateway control state and registry data are durable/capacity data,
so they are better candidates for a Hetzner Volume than worker sandbox COW.

## Non-billable foundation

The setup helper creates or reuses one private network, one registered
bootstrap SSH key, and one worker firewall. It does not create servers,
Volumes, public IPs, or snapshots. Generate the key once, review the dry run,
then execute:

```bash
install -d -m 0700 .hetzner/ssh
ssh-keygen -t ed25519 -N '' \
  -C 'ucloud-sandboxes production init' \
  -f .hetzner/ssh/gateway-init

set -a
source .env
set +a
.venv/bin/python scripts/setup_hetzner_foundation.py
.venv/bin/python scripts/setup_hetzner_foundation.py --execute
```

The default network is `10.42.0.0/16` with a `10.42.0.0/24` cloud subnet in
the `eu-central` network zone. Override those values before the first execute
if they conflict with deployment routing. The helper writes the resulting IDs
to the git-ignored `.hetzner/foundation.json`; rerunning it is idempotent and
fails on same-name resources with incompatible configuration. Back up the
private key securely and later install it as the gateway's
`ssh/gateway-init` key.

After the gateway has the reserved private address, add the Network route
idempotently (the current deployment uses `10.42.0.2`):

```bash
.venv/bin/python scripts/setup_hetzner_foundation.py \
  --egress-gateway-ip 10.42.0.2
.venv/bin/python scripts/setup_hetzner_foundation.py \
  --egress-gateway-ip 10.42.0.2 \
  --execute
```

This route is non-billable. It is ineffective until the gateway's forwarding
and NAT rules are active, so bring the gateway up before enabling private
worker egress.

## Snapshot-based node provisioning

Use separate role snapshots when their installed packages differ. It is also
valid to point both roles at the same snapshot while bringing the deployment
up. The snapshot should contain the slow, static part of initialization:

- the tested Ubuntu kernel and required modules;
- Docker, containerd, gVisor, XFS, ublk, and host support packages;
- the empty, sparse local XFS quota image, if its size is fixed;
- content-addressed runtime artifacts that contain no deployment secrets.

The qualified image deliberately omits the swap file. Recreating it with
`fallocate` and `mkswap` took about 53 ms during reconciliation and avoids
storing 16 GiB of allocated swap blocks in every snapshot.

The autoscaler still runs VM initialization after boot. On a matching snapshot
that run installs the unique job ID, tokens, node address, and dynamic service
configuration, then starts the node. The root-owned content-addressed bundle,
agent runtime, host binaries, kernel modules, and static-runtime receipt are
already present, so the autoscaler probes their digest without transferring the
bundle and VM init skips their installation. This separation is necessary: a
snapshot must never start an old node agent using the source server's identity.

Do not snapshot an active production worker. Build from a disposable source
server and, before taking the snapshot:

1. Stop and disable the node, heartbeat, storage-native, Docker, and containerd
   services so no stale workload starts on a clone.
2. Release all ublk devices, unmount runtime mounts, and remove sandbox, Docker,
   storage-native, journal, token, and node-environment state. Keep exactly one
   verified root-owned runtime bundle, agent runtime, and matching receipt.
3. Remove cloud-init's generated Netplan file, install the private-network
   wait-online override, and run
   `cloud-init clean --logs --machine-id --configs ssh_config --seed` so the
   clone gets fresh network metadata, machine identity, SSH host keys, and
   injected SSH access. Ubuntu 26.04 otherwise waits 120 seconds for a
   private-only lease to acquire a default route and DNS that Hetzner does not
   provide.
4. Power the source server off and create the snapshot while it is off.
5. Boot one canary from the snapshot and verify fresh host keys, the configured
   private address, no stale heartbeat, successful idempotent initialization,
   and a sandbox create/exec/hibernate/wake cycle.

For the reviewed disposable source host, the repository script performs the
destructive parts of steps 1-3 and refuses to run unless its hostname argument
matches the live host. It also fails closed if Ubuntu's merged-/usr invariant
or boot/runtime kernel modules are missing:

```bash
scp scripts/prepare_hetzner_snapshot.sh root@SOURCE:/root/
ssh root@SOURCE \
  'bash /root/prepare_hetzner_snapshot.sh "$(hostname)"'
```

Review the script and the source server before running it. It deletes workload
state, runtime scratch caches, credentials, build trees, Docker data, swap,
logs, and machine identity by design while retaining the one verified immutable
runtime cache described above.

Hetzner recommends powering a server off for snapshot consistency. Snapshots
contain only the server's local disk; attached Volumes are excluded. Snapshot
and server architectures must match, so the qualified amd64 runtime needs an
x86 snapshot and x86 server type. Snapshot storage remains billable after the
source server is deleted.

The current validated CPX62/Ubuntu 26.04 artifact is snapshot `419224276` in
the tested Hetzner project. It was built on CPX12 so the snapshot has a 40 GB
logical source disk rather than inheriting CPX62's 640 GB disk; Hetzner expands
the root filesystem on the CPX62 clone, while the preformatted 440 GB sparse
XFS quota image remains valid. The snapshot stores about 2.390 GB.

Its exact sandbox bundle SHA-256 is
`761c0b7483882a84e40fd2322915809f2e720dd3c97609d8899fd3cc63d4dd2d`.
The matching local artifact is
`.hetzner/bundles/sandbox-node-package-ubuntu-26.04-amd64-fast-v3.tar.gz`.
Install that artifact as the gateway's configured sandbox-node bundle when
using this snapshot. A differently built release deliberately misses the
receipt and takes the safe full-transfer/full-install upgrade path; create a
new golden snapshot after validating that release to restore the fast path.

In the original public-network validation canary, SSH plus cloud-init readiness
arrived about 35 seconds after the API create request. A production-shaped
private-only clone exposed an additional Ubuntu 26.04 wait-online delay: SSH
opened at 129-131 seconds because the generated unit required a routable link
with DNS. With the snapshot override, private-only CPX62 clones start SSH at
about 12.5 seconds of guest uptime and complete cloud-init in roughly 10-19
seconds. Hetzner's API scheduling/create phase took another 33-47 seconds in
the observed samples and is now the dominant provisioning delay.

On the final canary, the content-addressed receipt probe took 0.692 seconds and
transferred zero bundle bytes. Dynamic VM initialization took 5.503 seconds
internally and 5.831 seconds including its SSH invocation. Alpine pull plus
rootfs materialization took 2.383 seconds, direct gVisor sandbox create took
0.884 seconds, exec returned the expected output with exit code zero in 0.119
seconds, and fenced deletion took 0.293 seconds. Automatic private egress also
resolved DNS and reported the CPX12 gateway's public IP without a post-create
route command. All temporary source, worker, gateway, firewall, and Primary IP
resources were deleted after validation.

Snapshot `419216526` is the immediately previous 40 GB / 2.394 GB fast-v3
artifact. It has the same runtime receipt but lacks the private wait-online
override, so private-only clones incur the 120-second guest delay. It remains a
billable rollback snapshot pending an explicit retention decision.

The first canary usefully caught a merged-/usr regression in the offline
runtime extractor: GNU tar replaced `/lib -> usr/lib` when a package carried a
`./lib` directory entry. The extractor now uses
`--keep-directory-symlink`, snapshot preparation verifies the symlink and boot
modules, and the invalid intermediate snapshot was deleted.

One Ubuntu 26.04 kernel limitation remains: Docker host-port publication
(`docker run -p ...`) fails because this Hetzner kernel does not expose the
`xt_tcpudp` extension. Ordinary Docker containers, buildx, Docker-in-Docker,
the direct sandbox nftables/veth path, and sandbox HTTPS egress all passed.
Workers do not use Docker port publishing for the tested direct runtime, but do
not deploy a Docker `ports:` based gateway on this image until that kernel path
is fixed or replaced.

## Storage placement

Do not mount one large Hetzner Volume over `/var/lib/ucloud-sandboxes` by
default. That directory contains the latency-sensitive Docker and sandbox
write path, including the storage-native OverlayBD upper/COW data behind the
ublk device. The qualified Cloud Volume was materially slower than the local
root disk:

| Workload | CX43 local root | Hetzner Volume |
| --- | ---: | ---: |
| Sequential direct I/O | about 2.7 GB/s | about 315 MB/s |
| Mixed 4-KiB direct I/O | about 44.3k IOPS | about 9.9k IOPS |

The initial safe placement is:

- local root disk: active sandbox COW, ublk runtime data, Docker quota image,
  active image layers, swap, and journals;
- Hetzner Volume: durable gateway/registry data, published sealed snapshots,
  cold build cache, or other capacity data that is not in the synchronous
  sandbox write path.

This means node capacity is intentionally limited by local disk until a split
hot/cold layout is implemented. A Volume is attached to only one server
at a time, grows but does not shrink, and is not included in server snapshots.
The compute provider therefore does not create or delete Volumes. Storage must
have its own lifecycle and orphan-recovery journal before the autoscaler owns
it.

The previous 57.2% ublk/native result is not a production verdict: that test
used a buffered loop-device baseline and mismatched queue depth. Re-run the
comparison with direct loop I/O and production-equivalent ublk concurrency.
For the placement decision, also measure fsync latency, random-write tail
latency, sustained full-device behavior, concurrent sandboxes, detach/reattach,
and failure recovery.

## End-to-end sandbox qualification

The two-worker acceptance run used a CPX12 gateway, two private-only CPX62
workers initialized from snapshot `419224276`, the private gateway registry,
and a real tool-using coding agent. The second snapshot worker reused the
content-addressed runtime with zero bundle transfer and completed dynamic init
in 4.148 seconds.

The same-worker agent run completed six model/tool calls in 16.451 seconds,
passed visible and hidden tests, and preserved the managed process across every
park/wake. Median observed park time was 317 ms and median result commit plus
wake was 522 ms. Parked sandboxes released vCPU, memory, and the active ublk
device; their local disk reservation intentionally remained attached for the
fast same-worker wake path.

A separate 20-cycle soak used a checksum-protected counter process. Every park
reached `parked`, every wake reached `running`, the counter advanced without
corruption after every wake, and the PID/spec digest stayed stable. There were
no lifecycle retries. Median park and wake calls were 235 ms and 466 ms;
maximums were 421 ms and 521 ms.

The agent was then migrated during a model call from `10.42.0.3` to
`10.42.0.4`. The 9.73 GB virtual COW published as 156,962,816 bytes of actual
registry layers. End-to-end migration took 6.759 seconds, of which 2.240
seconds were inside the migration protocol: 1.623 seconds to prepare/export,
404 ms to transfer/stage, 10 ms to activate the destination, 5 ms to commit the
route, and 186 ms to finalize the source. The relay recorded exactly one
reattachment and one transport reset, no duplicate inference, and the agent
continued with PID 29 and the same process spec before passing all tests on the
destination.

After all runs, both workers reported zero sandboxes, zero resource
reservations, zero active ublk devices, and zero storage errors. The complete
machine-readable evidence is in
[`benchmarks/hetzner-agentic-e2e-2026-08-12.json`](benchmarks/hetzner-agentic-e2e-2026-08-12.json).

## Remaining deployment work

1. Create the private network, restrictive firewall, gateway SSH key, gateway,
   and gateway NAT/forwarding path.
2. Wrap the reviewed sanitization script, snapshot action, canary checks, and
   snapshot promotion in one auditable release command.
3. Store the chosen role snapshot IDs in deployment configuration and protect
   released snapshots from accidental deletion.
4. Decide and implement the hot/cold storage boundary after corrected ublk and
   Volume benchmarks. Do not attach per-node Volumes through the compute
   mutation path.
5. Add fault-injection acceptance for ambiguous provider creates, snapshot
   rollback, registry outage during publication, and worker loss at each
   migration phase. The normal multi-node create/bootstrap/agent/park/wake/
   migration path is now qualified.

The original storage qualification run used CX43, but the live API now marks
the entire CX line unavailable in the EU locations. CPX62 is the selected
currently available x86 shared-core shape, provides 640 GB local disk, and has
now passed the same host capability suite on Ubuntu 26.04. The tested account
rejected CCX53 because of its current dedicated-core limit, even though the
catalog reports that shape as generally available. Revisit CCX after Hetzner
raises the account limit. All temporary storage-qualification compute and
Volumes were deleted after testing. Snapshot validation also ended with zero
managed servers; the released snapshot and non-billable
network/firewall/SSH-key foundation are the only retained Hetzner resources.
