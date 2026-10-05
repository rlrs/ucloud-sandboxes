# Compute provider portability

The sandbox runtime is not a UCloud runtime. It is a Linux node stack built
around the repository's pinned, patched gVisor `runsc`, the direct Warden, and
storage-native volumes. UCloud and Hetzner are built-in ways to provision and
bootstrap the Linux machines that host that stack.

The provider boundary is intentionally small. It is not a general cloud SDK or
a second orchestration framework.

```mermaid
flowchart LR
    G["Gateway and routing"] --> D["Demand"]
    D --> P["Scaling policy"]
    P --> R["Provider-neutral reconciliation"]
    R --> C["ComputeProvider"]
    C --> U["Built-in UCloud adapter"]
    C --> H["Built-in Hetzner adapter"]
    C --> O["External cloud adapter"]
    U --> N["Linux worker node"]
    H --> N
    O --> N
    N --> W["Direct gVisor Warden"]
    W --> S["Storage-native volumes"]
```

## What the core expects

`ucloud_sandboxes.providers.base.ComputeProvider` is the complete autoscaler
boundary. An adapter must:

- list, decode, and retrieve instances as `ProviderInstance` values;
- normalize native lifecycle states to `PROVISIONING`, `RUNNING`, `LOST`, or
  `TERMINAL`;
- decide whether an instance belongs to the configured pool scope;
- translate semantic sandbox/builder `InstanceCreateIntent` values into a
  native create request;
- preserve every create-intent label and return it through normalized
  `ProviderInstance.labels`, which is the operation-recovery identity;
- normalize create and terminate outcomes as accepted, rejected, or uncertain;
- provide a bootstrap access command when a running instance can be
  initialized.

Policy, drain safety, operation journaling, heartbeats, routing, node roles,
runtime installation, and sandbox lifecycle stay outside the adapter. In
particular, the core never branches on a provider's native lifecycle strings
and never constructs a provider API payload.

## Provider configuration

Provider settings live in one strictly tagged object:

```json
{
  "provider": {
    "kind": "ucloud",
    "scope_id": "project-1",
    "private_network_id": "network-1",
    "template_job_id": null,
    "gateway_public_link_id": null,
    "gateway_public_link_port": 8090
  }
}
```

The UCloud session file is an operational credential override, supplied with
`--session-file` to local operator commands when needed. It is deliberately
not persisted in `deployment.json`.

Each built-in provider rejects unknown keys and validates its exact tagged
object. An external provider owns and validates its own keys. Provider-specific
credential references, image references, network identifiers, and machine
profiles belong there rather than in core models. See
[`hetzner.md`](hetzner.md) for the built-in Hetzner schema.

## Adding another provider

Implement `ComputeProvider`, then expose a factory with the signature
`factory(configuration, cli_options) -> ComputeProvider`. Register it under
the configuration `kind` in the Python entry-point group
`ucloud_sandboxes.compute_providers`:

```toml
[project.entry-points."ucloud_sandboxes.compute_providers"]
examplecloud = "examplecloud.sandboxes:build_provider"
```

The factory receives the tagged `ProviderConfiguration` and the shared parsed
autoscaler options. It should keep cloud-specific options in the tagged
configuration. No policy, reconciliation, routing, registry, or runtime module
needs to be changed.

Use a fake implementation of the same protocol for contract tests. At minimum,
test lifecycle normalization, pool eligibility, request rendering, ambiguous
mutation recovery, and bootstrap access discovery.

## Infrastructure required on another cloud

The adapter only solves compute provisioning. A deployment also needs:

- Linux VMs on which the bootstrap user can run privileged installation;
- private reachability from the gateway to node-agent and SSH endpoints;
- the kernel/module and block-device support required by the verified node
  bundle, including the storage-native ublk path;
- durable gateway state and registry storage;
- a way to distribute the verified node bundle and credentials;
- stable node identities and enough metadata or labels to recover provider
  operations after ambiguous API responses.

### Provider-neutral services with host requirements

These services are not provider-specific, but each one requires something of
the host or of the storage behind it.

- **Kernel modules.** Workers load `erofs` and `nbd` (immutable environments
  and RAFS over NBD), `nft_log` and `nfnetlink_log` (node-local model waits),
  and `ublk_drv` and `virtiofs`. The bundle carries the module closure
  (`RUNTIME_KERNEL_MODULES` in `vm_init.py`). On an image without a full
  modules package, verify the modules before admitting a worker.
- **Chunk store.** This is an S3 bucket plus an optional store node.
  - S3 can live at a different provider from compute: production on UCloud
    keeps Hetzner Object Storage.
  - A store node without `store_node.data_device` keeps its replica and index
    on its root disk.
  - Store and worker init log in as the provider's bootstrap user. Root on
    Hetzner, `ucloud` on UCloud. Init scripts must reach anything under a
    root-only temporary directory through `$SUDO`.
- **Registry disk guard.** The `registry_disk_*_percent` thresholds measure
  `statvfs` of the registry's filesystem, so they only mean something on a
  filesystem the registry owns, such as a Hetzner Volume.
  - UCloud's `/work/data` is a shared multi-petabyte drive whose usage is
    mostly other projects'. Default thresholds would evict images
    immediately.
  - Set all three to `100` there, and size retention by
    `registry_retention_days` and `registry_keep_per_repository` instead.
- **PostgreSQL.** The gateway uses a local PostgreSQL 18 over its Unix socket
  with peer authentication. The DSN is
  `host=/var/run/postgresql dbname=ucloud user=ucloud`, so the database role
  must match the service user.

The in-tree `deploy-all-in-one`, UCloud resource helpers, and session handling
remain UCloud-specific operator conveniences. A different cloud should provide
its own deployment automation, but it reuses the same gateway and worker
services after provisioning. This is the remaining infrastructure integration
work; it is not a runtime or autoscaler-policy dependency.
