# ucloud-sandboxes

Autoscaler for secure CPU sandboxes on top of SDU UCloud.

The service manages UCloud VM jobs as pool nodes. Sandbox nodes run one direct
gVisor Warden backed by storage-native volumes. Builder nodes build and push
custom images to a private registry. The public gateway owns placement,
routing, operation fencing and demand; the autoscaler reconciles that demand
into UCloud VM jobs.

The project currently has a live development deployment with a public gateway,
private registry, model relay, autoscaler loop, sandbox nodes, and builder
nodes. Live job IDs, tokens, and project-specific details belong in the runbooks
under `docs/`, not in this overview.

## Start Here

- [CLI and operations](docs/cli-and-operations.md): command examples for
  planning, VM submission, gateway/autoscaler startup, image builds, prepare
  signals, metrics, and local agent runs.
- [SDK and integrations](docs/sdk-integrations.md): where the separate SDK
  lives, plus the gateway-side contract needed by SDK clients, Inspect AI, and
  benchmark workloads.
- [API reference](docs/api-reference.md): heartbeat, gateway, sandbox, image,
  prepare, exec, file, dashboard, and node-agent endpoints.
- [Architecture](docs/architecture.md): control plane, builder, registry,
  routing, resource placement, disk quota, and networking design notes.

## Focused Docs

- [Deployment flow](docs/deployment-flow.md): release inputs, service layout,
  verified node bundles, credentials, and cleanup safety.
- [Managed registry](docs/managed-registry.md): private Docker registry setup,
  persistence, tagging, pruning, and GC.
- [Model relay](docs/model-relay.md): outbound-only OpenAI-compatible relay for
  sandboxes that need to reach model workers behind outbound-only networking.
- [Routing gateway](docs/routing-gateway.md): gateway and exec/SSH routing
  design, performance expectations, and concurrency notes.
- [Scaling policy](docs/scaling-policy.md): scale-to-zero policy, prepare
  signals, builder policy, overcommit, and observed scale-up metrics.
- [Security stance](docs/security-stance.md): gVisor/container security model,
  storage authority, authentication boundaries, and verified bootstrap.
- [VM init](docs/vm-init.md): UCloud VM bootstrap findings and post-boot init
  strategy.

## Quick Local Checks

Run tests:

```bash
uv run python -m unittest
```

Inspect the canonical configuration:

```bash
uv run ucloud-sandboxes sample-config
```

Control-plane and node processes intentionally have no unauthenticated local
mode. Use the deployment/bootstrap flow to create their distinct gateway,
heartbeat and node-control credentials and their pinned runtime artifacts.
