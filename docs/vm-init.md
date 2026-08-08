# VM initialization

VM initialization turns a running UCloud VM into either a sandbox node or an
image-builder node. It is an authenticated bootstrap step owned by the
autoscaler, not a general host installer.

## Inputs

Each init request includes:

- deployment, job, node, and advertised node URL identity;
- heartbeat and node-control token destinations and values;
- exact role-specific package bundle path and SHA-256 digest;
- physical CPU, memory, and disk capacity;
- Docker image-cache size, swap size, and private registry settings;
- for sandbox nodes, the exact patched `runsc` commit, storage-native registry,
  cache, and network policy.

Sandbox CPU, memory, and disk overcommit are fixed at `1.0`. Builder nodes may
advertise separate compute overcommit because they do not admit sandboxes.

## Verified bundle

`deploy-all-in-one` produces separate sandbox and builder bundles. Each bundle
contains:

- a manifest with role, platform, package, kernel-module, and artifact digests;
- Docker, containerd, XFS, and role-specific Buildx packages;
- a preassembled Python agent runtime;
- the exact host-kernel module closure;
- on sandbox nodes, patched direct `runsc`, managed PID 1, and the pinned
  storage-native backend plus provenance and license.

The autoscaler stages the bundle over SSH and passes its digest to VM init. VM
init re-hashes the archive, extracts it into a digest-keyed directory, validates
every declared file, and aborts on any mismatch. It never enables package
repositories or installs a substitute artifact from the network.

## Bootstrap sequence

1. Validate deployment identity, token material, role, resource accounting,
   network policy, and bundle digest.
2. Create the service user and install heartbeat and node-control secrets with
   mode `0600`.
3. Verify and install the bundled runtime packages and kernel modules.
4. Provision bounded swap and Docker image-cache storage.
5. On sandbox nodes, install direct `runsc`, managed PID 1, and the
   storage-native backend; create its cache and device-pool services.
6. Configure Docker for image operations and the private registry.
7. Activate the bundled Python runtime and write the role-specific node unit.
8. Start the node, require `/healthz`, then enable the heartbeat timer.

The node agent is never started if an artifact or service prerequisite fails.
The control plane treats the node as unavailable until a fresh authenticated
heartbeat reports the expected deployment and init versions.

## Roles

Sandbox nodes run `serve-direct-node-agent` as root because the Warden owns OCI
bundles, namespaces, cgroups, storage-native mounts, and direct `runsc`
processes. The storage-native backend and service are required dependencies.

Builder nodes run `serve-builder-agent` as the service user with Docker group
membership. They expose only image-cache, image-pull, image-build, heartbeat,
health, and drain behavior. Buildx remains available for direct registry push.

## Package staging

The gateway copies bundles to
`/tmp/ucloud-sandboxes-init-packages/<job-id>/`. A sidecar digest marker avoids
retransmitting the same immutable archive. VM init still verifies the staged
bytes and every extracted artifact before activation; the marker is only a
transfer optimization.

## Diagnostics

The script emits one line per completed phase:

```text
UCLOUD_INIT_PHASE name=runtime-bundle duration_ms=17321 total_ms=19002
```

Bootstrap failures are terminal for that attempt. Inspect the init command
output and the `ucloud-sandbox-node`, `ucloud-storage-native`, Docker, and
containerd journals. Fix the bundle or configuration and retry the same
generation; do not repair a node by installing a different runtime manually.
