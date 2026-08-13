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
initial sandbox shape, CPX62, has 16 shared vCPU, 32 GB memory, and 640 GB local
disk in the live API. Hetzner's disk number is decimal; the provider normalizes
640 GB to 596 whole binary GiB before core resource accounting. Treating it as
640 GiB over-advertised roughly 44 GiB and consumed the intended safety margin.

The current CPX62 sandbox profile reserves 64 GiB for the disposable Docker
image cache, no swap, 16 GiB for the bounded remote-layer cache, and 24 GiB of
safety headroom. That leaves 492 GiB (503,808 MiB) as hard storage-native
capacity. The headroom is a correctness reserve for the OS, logs, journals,
filesystem metadata, and writes outside the bounded stores. The remote cache
is performance-only and can be reduced safely at the cost of more Registry
reads and slower repeated cold wakes. Docker's quota prevents shared image and
rootfs materialization from consuming the whole host; 64 GiB is an initial
operational allowance, not a required architectural constant. Docker layers
are shared, repullable, and backed by the Registry, so that quota is the first
large reservation to revisit after measuring the real image working set.

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
justify it. Keep gateway SQLite databases and journals on that local disk, with
transactional backups. Put the much larger immutable registry blob tree on a
Hetzner Volume; network-storage latency should not sit under SQLite commits or
worker sandbox COW.

## Non-billable foundation

The setup helper creates or reuses one private network, one registered
bootstrap SSH key, a deny-inbound worker firewall, and a public gateway
firewall. The gateway firewall exposes only ports 80 and 443 unless explicit
operator SSH CIDRs are supplied. It does not create servers, Volumes, public
IPs, or snapshots. Generate the key once, review the dry run, then execute:

```bash
install -d -m 0700 .hetzner/ssh
ssh-keygen -t ed25519 -N '' \
  -C 'ucloud-sandboxes production init' \
  -f .hetzner/ssh/gateway-init

set -a
source .env
set +a
.venv/bin/python scripts/setup_hetzner_foundation.py \
  --gateway-ssh-source-cidr 198.51.100.24/32
.venv/bin/python scripts/setup_hetzner_foundation.py \
  --gateway-ssh-source-cidr 198.51.100.24/32 \
  --execute
```

The default network is `10.42.0.0/16` with a `10.42.0.0/24` cloud subnet in
the `eu-central` network zone. Override those values before the first execute
if they conflict with deployment routing. The helper writes the resulting IDs
to the git-ignored `.hetzner/foundation.json`; rerunning it is idempotent and
fails on same-name immutable resources with incompatible configuration. It
reconciles the gateway firewall to the requested SSH CIDRs, so repeat the same
arguments on later runs. Omitting them deliberately removes public SSH access.
Back up the private key securely and later install it as the gateway's
`ssh/gateway-init` key. Attach `gateway_firewall_ids` to the public CPX12 and
keep the worker `firewall_ids` in the compute-provider configuration.

After the gateway has the reserved private address, add the Network route
idempotently (the current deployment uses `10.42.0.2`):

```bash
.venv/bin/python scripts/setup_hetzner_foundation.py \
  --gateway-ssh-source-cidr 198.51.100.24/32 \
  --egress-gateway-ip 10.42.0.2
.venv/bin/python scripts/setup_hetzner_foundation.py \
  --gateway-ssh-source-cidr 198.51.100.24/32 \
  --egress-gateway-ip 10.42.0.2 \
  --execute
```

This route is non-billable. It is ineffective until the gateway's forwarding
and NAT rules are active, so bring the gateway up before enabling private
worker egress.

## Public SDK access

SDK callers need only two deployment-specific values:

- an HTTPS URL using the gateway's public IPv4 address;
- the contents of the generated `sandbox-api-token` file below `data_root`.

The API key is intentionally different from `gateway-token`. The former can
create and operate sandboxes, transfer files, run jobs, build images, and
request prepared capacity. It cannot inspect nodes or metrics or invoke the
operator-only park, wake, detach, and migration routes. Never distribute
`gateway-token` to SDK users.

The gateway itself continues to listen on port 8090 for private worker and
controller traffic. Do not expose 8090, the registry port, or the model-relay
control port publicly. On the CPX12, put Nginx in front of loopback port 8090
and obtain a publicly trusted certificate:

```bash
sudo scripts/configure_hetzner_sdk_ingress.sh \
  --public-host "$GATEWAY_PUBLIC_IPV4" \
  --email ops@example.org
```

No DNS record or domain is required. Passing the public IPv4 address obtains a
Let's Encrypt IP-address certificate, so SDK clients still perform normal
certificate verification rather than disabling TLS checks. Those certificates
use the short-lived profile, so the helper installs a twice-daily Certbot
renewal timer. Ports 80 and 443 must remain reachable for ACME renewal and SDK
traffic. The helper validates Nginx configuration and finishes only after the
public `/healthz` request succeeds. Let's Encrypt documents the
[general availability of IP address certificates](https://letsencrypt.org/2026/01/15/6day-and-ip-general-availability.html)
and the corresponding
[Certbot 5.4 command](https://letsencrypt.org/2026/03/11/shorter-certs-certbot.html).

Create the gateway IPv4 as an independently managed Hetzner Primary IP in
`hel1` and disable `auto_delete` instead of accepting an address tied to the
lifetime of one CPX12. The address is billable even while unassigned. During a
gateway replacement, power off the old server, unassign the Primary IP, and
assign it to the new `hel1` server. This keeps the SDK URL stable without DNS;
Hetzner requires the server to be off while changing a Primary IP and binds the
address to one location. Do not use a Floating IP for this single-gateway case.

An SDK user can then connect without any Hetzner or operator credentials:

```python
import os

from ucloud_sandboxes_sdk import SandboxClient

client = SandboxClient(
    os.environ["UCLOUD_SANDBOX_URL"],
    api_token=os.environ["UCLOUD_SANDBOX_API_TOKEN"],
)
print(client.list_sandboxes())
```

```bash
export UCLOUD_SANDBOX_URL="https://$GATEWAY_PUBLIC_IPV4"
export UCLOUD_SANDBOX_API_TOKEN='<contents of sandbox-api-token>'
```

Generated deployments create the key automatically. When upgrading a manually
assembled Hetzner gateway, create one independent 32-byte secret owned by the
gateway service at `<data_root>/sandbox-api-token` before restarting
`serve-control-plane`; startup fails closed when the file is absent, empty, or
duplicates another channel credential. Transfer the key to users through a
secret manager, not shell history, chat, deployment JSON, or source control.

## Snapshot-based node provisioning

Use separate role snapshots when their installed packages differ. It is also
valid to point both roles at the same snapshot while bringing the deployment
up. The snapshot should contain the slow, static part of initialization:

- the tested Ubuntu kernel and required modules;
- Docker, containerd, gVisor, XFS, ublk, and host support packages;
- the empty, sparse local XFS quota image, if its size is fixed;
- content-addressed runtime artifacts that contain no deployment secrets.

The qualified image deliberately omits the swap file, and the operational
profile keeps `swap_gb` at zero. The earlier qualification proved recreating a
swap file was cheap, but these workers do not rely on swap for capacity or
correctness.

The autoscaler still runs VM initialization after boot. On a matching snapshot
that run installs the unique job ID, tokens, node address, and dynamic service
configuration, then starts the node. The root-owned content-addressed bundle,
agent runtime, host binaries, kernel modules, and static-runtime receipt are
already present, so the autoscaler probes their digest without transferring the
bundle and VM init skips their installation. This separation is necessary: a
snapshot must never start an old node agent using the source server's identity.

When the qualified Ubuntu package set is unchanged, use
`scripts/repack_node_bundle.py` to replace only the pure-Python agent, the
content-addressed storage backend, and—when the Hetzner base image has advanced
its kernel—the module closure collected from the new source VM. The command
validates every retained source artifact, requires the pinned AgentEnv
provenance, verifies that the repacked agent contains every unconditional
Python dependency, and emits a deterministic bundle plus SHA-256 sidecar. Pass
the complete dependency wheel set with repeated `--agent-dependency-wheel`
arguments; this includes the OpenTelemetry SDK and OTLP/HTTP exporter wheels.
The repacker now fails instead of producing a partially importable runtime when
a dependency is absent. If the dependency closure contains a platform wheel,
build a complete `site-packages` tree with the bundle's Python version on a
matching Linux host and pass its parent as `--agent-runtime-root`; the repacker
checks the Python version and dependency closure before replacing the agent
archive. Never retain an old kernel closure merely because the OS release name
is unchanged; the v4 build found Ubuntu 26.04 had advanced from `7.0.0-22` to
`7.0.0-29`.

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
  'bash /root/prepare_hetzner_snapshot.sh "$(hostname)" 64 sandbox'
```

Review the script and the source server before running it. It deletes workload
state, runtime scratch caches, credentials, build trees, Docker data, swap,
logs, and machine identity by design while retaining the one verified immutable
runtime cache described above. The explicit size argument fences the sparse
Docker image that will be retained; preparation fails instead of silently
carrying an older partition layout into the snapshot.

Hetzner recommends powering a server off for snapshot consistency. Snapshots
contain only the server's local disk; attached Volumes are excluded. Snapshot
and server architectures must match, so the qualified amd64 runtime needs an
x86 snapshot and x86 server type. Snapshot storage remains billable after the
source server is deleted.

The current validated CPX62/Ubuntu 26.04 artifact is snapshot `419603017` in
the tested Hetzner project. It was built on the production CPX62 worker shape:
the retained 64 GiB sparse XFS quota image does not fit on a CPX12 source disk,
and initializing such a source fails closed before modifying it. The snapshot
stores about 1.908 GiB and requires a server whose disk is at least as large as
its CPX62 source. Its worker profile reserves no swap, 16 GiB for the bounded
remote-layer cache, and 24 GiB of host headroom. After normalizing Hetzner's
decimal 640 GB disk to 596 binary GiB, that leaves 492 GiB of hard
storage-native capacity for active parkable sandboxes.

Its exact sandbox bundle SHA-256 is
`62ed18bbe5bab5155bb12084ee1c8dae1b7caec7eea81290117c05328e4d874e`.
The matching local artifact is
`.hetzner/bundles/sandbox-node-package-ubuntu-26.04-amd64-park-wake-opt.tar.gz`.
Install that artifact as the gateway's configured sandbox-node bundle when
using this snapshot. A differently built release deliberately misses the
receipt and takes the safe full-transfer/full-install upgrade path; create a
new golden snapshot after validating that release to restore the fast path.

Builders use the separate CPX62/Ubuntu 26.04 snapshot `419672197`. Its retained
builder bundle SHA-256 is
`4e94f5c978318fd77ad768ac3dfeeaa058d4686abe55eb30e60c0c793974e795`;
the bundle contains Docker, containerd, and Buildx but deliberately omits the
sandbox-only gVisor and storage-native artifacts. The canary selected that
snapshot-baked bundle in 45 ms and completed all dynamic initialization in
3.936 seconds. Hetzner took 46.404 seconds from accepted server creation until
it reported a running VM with a private address, and SSH readiness remained
the dominant remainder. The canary completed a real registry-pushed image
build in 9.349 seconds from a cold Docker cache and a following warm build in
2.289 seconds. Evidence is in
[`benchmarks/hetzner-builder-otlp-2026-08-13.json`](benchmarks/hetzner-builder-otlp-2026-08-13.json).

The promoted snapshot canary was confirmed through the Hetzner API to use
image `419603017`. It completed two full SDK-driven park, S3 detach, and wake
cycles with exact process and filesystem checks and no retries. The first wake
on its empty remote-block cache took 8.288 seconds; the immediately following
warm-cache wake took 1.641 seconds. The canary worker and the sanitized source
worker were deleted after qualification, leaving the gateway as the only
running production server.

The `0.4.1` release bundle carries AgentEnv v0.1.2 plus the owner,
pooled-delete, and streaming dense-export patches, the S3 publisher and its
runtime dependencies, and the current Ubuntu image's `7.0.0-29-generic` module
closure. The preceding `0.4.0` snapshot qualification remains useful as a
historical local/Registry comparison: a fresh CPX62 clone reused its baked
bundle without transferring it and completed dynamic initialization in 3.496
seconds.

An actual gVisor sandbox then created in 0.570 seconds, durably published while
parking in 0.984 seconds, woke on the attached worker in 0.317 seconds, and
preserved its sentinel. A stricter second run evicted the published local
volume, imported it only from its Registry descriptor in 0.334 seconds, and
woke it in 0.272 seconds with the sentinel intact. Cleanup returned hard
reservations, published local volumes, and active ublk devices to zero with no
storage errors. Evidence is in
[`benchmarks/hetzner-snapshot-release-v5-2026-08-12.json`](benchmarks/hetzner-snapshot-release-v5-2026-08-12.json).

A clean-pool `0.4.1` canary confirmed through the Hetzner API that its new
CPX62 used snapshot `419550929`. The initializer selected the snapshot-baked
digest, validated the package in 90 ms, and completed all dynamic phases in
4.525 seconds. Hetzner took about 69 seconds from accepted create to SSH
readiness, which remains the dominant cold-node provisioning cost. The canary
then used the sandbox-scoped SDK credential to execute code and preserve one
managed process across park, durable S3 detach, and wake. A following
three-cycle run had median park, detach, and wake times of 247 ms, 897 ms, and
2.703 seconds, with exact counter/process checks and zero retries. Evidence is
in
[`benchmarks/hetzner-object-storage-snapshots-2026-08-13.json`](benchmarks/hetzner-object-storage-snapshots-2026-08-13.json).

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

Snapshot `419550929` is the immediately previous CPX62 / 1.931 GiB artifact. It
carries the pre-optimization `0.4.1` bundle and remains a billable rollback
image rather than a schedulable worker image. Snapshot `419293093` is the older
40 GB / 1.296 GB compaction-v4
artifact. It carries the same AgentEnv backend but agent version `0.3.95` and a
different runtime receipt, so it remains a billable rollback artifact rather
than a schedulable `0.4.0` image. Snapshot `419216526` is the older 40 GB /
2.394 GB fast-v3 artifact; it lacks the private wait-online override, so
private-only clones incur the 120-second guest delay. Both remain billable
pending an explicit retention decision.

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

The provider-neutral storage placement is:

- local root disk: active sandbox COW, ublk runtime data, Docker quota image,
  active image layers, and journals;
- gateway local root: routing, autoscaler, relay, metrics, registry-usage, and
  image-index SQLite state, plus their transactional backups;
- configured registry store: Docker Distribution's OCI image/build blob tree,
  using either a fail-closed filesystem mount or its native S3 driver;
- Hetzner Object Storage: published sealed sandbox snapshots, written directly
  by workers and range-read by AgentEnv through the bounded local cache.

The S3 path, rollout, failure contract, retention rules, and performance gate
are specified in
[`object-storage-snapshots.md`](object-storage-snapshots.md). The production
configuration now uses the `sandboxes` Object Storage bucket in `hel1`; the
live park/detach/cold-wake and compaction evidence is recorded in
[`benchmarks/hetzner-object-storage-snapshots-2026-08-13.json`](benchmarks/hetzner-object-storage-snapshots-2026-08-13.json).
The daily collector records the first sweep at which an object has no durable
route reference and requires that marker to remain continuously unreferenced
for seven days. Object creation time is not the retention clock, and a route
that references the object again clears the marker.

Deployment schema 5 expresses that boundary and telemetry directly. The
Hetzner target is:

```json
{
  "schema": 5,
  "data_root": "/var/lib/ucloud-sandboxes/state",
  "registry_store": {
    "kind": "s3",
    "mount_point": "",
    "data_root": "",
    "endpoint": "https://hel1.your-objectstorage.com",
    "bucket": "sandboxes",
    "region": "hel1",
    "prefix": "production/oci",
    "access_key_id_env": "HETZNER_S3_ACCESS_KEY",
    "secret_access_key_env": "HETZNER_S3_SECRET_KEY",
    "force_path_style": false
  },
  "snapshot_store": {
    "kind": "s3",
    "endpoint": "https://hel1.your-objectstorage.com",
    "bucket": "sandboxes",
    "region": "hel1",
    "prefix": "production",
    "access_key_id_env": "HETZNER_S3_ACCESS_KEY",
    "secret_access_key_env": "HETZNER_S3_SECRET_KEY"
  },
  "telemetry": {
    "endpoint": "http://10.42.0.2:4318",
    "trace_sample_ratio": 0.1,
    "export_interval_ms": 5000,
    "export_timeout_ms": 3000,
    "max_queue_size": 4096,
    "max_export_batch_size": 512
  }
}
```

The snippet shows only the relevant fields; deployment configuration remains
an exact full object. Schema 5 deliberately rejects older deployment documents;
generate and review a complete current configuration instead of relying on
implicit migrations. The explicit
production switch to S3 forces a one-time compact publication, so a manifest
never mixes Registry and S3 lower layers. Before changing the Registry blob
backend, stop the registry and run `ucloud-sandboxes-registry-migrate` against
the proposed schema-5 configuration. It hashes every source object, refuses
unexpected target objects unless overwrite is explicit, uploads through the
native S3 API, and verifies size and digest before cutover. Filesystem mode
retains the old mount fence; S3 mode removes that dependency. A Volume remains
available as a filesystem implementation where object storage is unavailable,
but the compute provider does not choose the storage policy.

The production OCI registry was migrated to the native S3 driver on 2026-08-13.
The verified 16-object copy, presigned redirect read, offline GC, and read-only
filesystem fallback evidence is recorded in
[`benchmarks/hetzner-registry-s3-2026-08-13.json`](benchmarks/hetzner-registry-s3-2026-08-13.json).
The old Volume tree remains unchanged as a rollback source; it is no longer on
the live registry data path.

## Worker-local active set and remote parked set

The worker now has two materially different ownership states:

- **attached**: the worker still owns the sandbox incarnation. Running and
  unpublished parked sandboxes reserve worker disk. A published-but-attached
  park has already released its hard COW reservation, but keeps worker
  registration and image/cache affinity for the fast same-worker wake path and
  still prevents the worker from reporting an empty inventory;
- **detached**: the sandbox is parked, its immutable snapshot is protected in
  the configured durable snapshot store, and its worker-local storage and image
  pin have been removed. It consumes remote capacity but no worker reservation.

Scale-down first drains a worker. For each remaining park, the gateway asks the
worker to publish its sealed state if necessary, validates that the returned
descriptor belongs to the exact sandbox generation, and durably records the
    durable publication reference before eviction. It does not copy those parks to another
worker merely to turn the current worker off. Only after a fresh complete
heartbeat reports no remaining inventory can the provider stop be submitted.
Publication failure leaves the sandbox attached; ambiguous eviction leaves the
route `detaching` and is retried, rather than pretending disk was freed.

Wake latency therefore has two tiers. An attached park on a healthy worker
keeps the existing sub-second local wake path. A detached park is a cold wake:
the gateway chooses any fitting worker, imports the immutable snapshot through
the bounded local layer cache, and activates it. That path is slower in
proportion to the snapshot working set missing from the destination cache, but it is
paid only after a sandbox has traded wake latency for released worker disk and
node scale-down. In a ten-cycle forced cross-worker soak with the Registry on a
gateway-attached Hetzner Volume, this small 1 GiB-memory/2 GiB-disk sandbox woke
in 1.334-1.479 seconds (1.368-second median). Every cycle used the other worker,
preserved the managed PID/spec identity, advanced checksum-protected filesystem
state, and completed without lifecycle retries. This is evidence for the small
warm-network case against the former Registry/Volume backend, not a general
cold-wake SLO or evidence for Hetzner Object Storage; multi-gigabyte snapshots, cold
layer caches, and concurrent wake bursts still need measurement.

The S3 import path avoids manufacturing a new snapshot layer during wake. A
remote ublk mount gives the same XFS files a new Linux `st_dev`, but that is a
property of the mount, not changed sandbox data. When inode, size, authenticated
manifest, and file inventory are unchanged, import keeps the published
manifest identity and discards its empty temporary COW instead of sealing and
uploading it. S3 config and per-layer existence checks also run concurrently
with a bounded verifier. Finally, ordinary SDK activity such as exec or file
access commits its implicit wake at the gateway, so the next park publishes
against the current running route rather than stale snapshot authority.

On the production S3 backend these changes reduced the small detached-wake
median from 2.626 seconds to 1.538-1.584 seconds, about 40%. A clean CPX62
provisioning canary repeated three SDK-triggered detached wakes at 1.493-1.974
seconds (1.542-second median), with zero lifecycle retries, preserved managed
process identity, and advancing checksum-protected filesystem state. Snapshot
chains grew by one real park delta per cycle instead of one real delta plus an
empty import delta. Attached wake remained about 0.56 seconds. Evidence and
the AgentEnv portability review are in
[`benchmarks/hetzner-park-wake-optimization-2026-08-13.json`](benchmarks/hetzner-park-wake-optimization-2026-08-13.json).

AgentEnv main was also reviewed through commit
`f8f725e2b962a0bae3eadcd426c7e1ecf243b34e`. The production v0.1.2 pin already
contains its shared bounded cache, warm ublk pool, and compact fixes. The newer
direct-to-OverlayBD packaging work is tied to AgentEnv's Firecracker memory
snapshot path and is not directly portable to this gVisor combined-state XFS
volume. Background whole-layer download remains disabled: demand paging is a
better default for a combined layer when the sandbox may touch only a fraction
of it.

Published layers also have a bounded-chain rule. An ordinary detach appends
the latest sealed delta, but a publication that would exceed eight layers or
4 GiB of layer data is compacted into one sealed layer. The worker does not
reserve enough local disk for a second flattened copy: AgentEnv reads the old
remote layers through its bounded local cache, overlays the new local delta,
and streams the compacted result directly to the configured durable backend.
Until that new manifest is durable, the old descriptor plus local delta
remain the authoritative state, so a failed compaction cannot make a sandbox
unwakeable or release its worker ownership.

A forced two-generation qualification on a disposable CPX62 collapsed 100.8
MB of input layers to one 98.7 MB layer in 868 ms and remounted the compacted
snapshot in 59 ms, preserving sentinels from both generations. It issued 782
range requests and served 201.8 MB across the entire publication, remote-wake,
compaction, and verification run. This proves the streaming mixed
remote/local path and cleanup behavior; it does not predict cold multi-GiB
compaction time or concurrent Registry-Volume throughput. Evidence is in
[`benchmarks/hetzner-agentenv-compaction-2026-08-12.json`](benchmarks/hetzner-agentenv-compaction-2026-08-12.json).

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
device. Those foreground parks were not published, so they retained their
local disk reservation for the fast same-worker wake path. A published attached
park releases that hard COW reservation while retaining worker ownership; the
new fully detached cold-wake path is covered by gateway/state-machine tests but
is also covered by the live ten-cycle cross-worker soak described below.

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

A separate detached-wake soak used two CPX62 workers and a CPX12 gateway whose
Docker Distribution blob tree lived on a 10 GB ext4 Hetzner Volume. Each of ten
cycles parked a managed counter, waited for the source drain fence to become
gateway-visible, published and detached it, and cold-woke it on the other
worker. All ten cycles passed with zero lifecycle retries. Median park, detach,
and cold-wake calls were 252 ms, 525 ms, and 1.368 seconds respectively; the
first publication produced the 2.488-second detach maximum. Both workers ended
with empty complete inventories, zero reservations, zero active ublk devices,
and zero storage errors. The 747 MB registry blob tree included this soak and
the preceding diagnostic canaries. Evidence is in
[`benchmarks/hetzner-detached-wake-volume-2026-08-12.json`](benchmarks/hetzner-detached-wake-volume-2026-08-12.json).

After all runs, both workers reported zero sandboxes, zero resource
reservations, zero active ublk devices, and zero storage errors. The complete
machine-readable evidence is in
[`benchmarks/hetzner-agentic-e2e-2026-08-12.json`](benchmarks/hetzner-agentic-e2e-2026-08-12.json).

## Remaining deployment work

1. Add transactional off-node backups for gateway SQLite/journal state. Keep
   that state on the gateway's low-latency local disk.
2. Measure burst detach and cold wake at 2, 4, and 8 concurrent workers, plus a
   multi-gigabyte changed layer. The single-worker correctness and throughput
   gate has passed, but it is not a claim about shared-service tail latency.
3. Add destructive fault injection for worker loss during every multipart and
   detach phase. The publisher already keeps ownership/local state until the
   durable manifest is verified and resolves lost completion responses.
4. Revisit the broad project-level Hetzner S3 key when the account supports a
   narrower bucket policy or dedicated restricted credential.

The original storage qualification run used CX43, but the live API now marks
the entire CX line unavailable in the EU locations. CPX62 is the selected
currently available x86 shared-core shape, provides 640 GB local disk, and has
now passed the same host capability suite on Ubuntu 26.04. The tested account
rejected CCX53 because of its current dedicated-core limit, even though the
catalog reports that shape as generally available. Revisit CCX after Hetzner
raises the account limit. All temporary storage-qualification compute and
Volumes were deleted after testing. Snapshot validation also ended with zero
managed servers. The released snapshots and non-billable
network/firewall/SSH-key foundation are the retained Hetzner resources; older
snapshots remain billable until deliberately removed after the rollback window.
The older detached-wake qualification likewise deleted its disposable CPX12
gateway, both CPX62 workers, and 10 GB Volume. The current deployment retains
one production CPX12 gateway; qualification workers are disposable and must be
removed after each run.
