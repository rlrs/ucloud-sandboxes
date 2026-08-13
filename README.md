# ucloud-sandboxes

Autoscaler and runtime for secure CPU sandboxes on Linux compute nodes, with an
SDU UCloud provider and a Hetzner Cloud provider included.

The service manages provider instances as pool nodes. Sandbox nodes run one direct
gVisor Warden backed by storage-native volumes. Builder nodes build and push
custom images to a private registry. The public gateway owns placement,
routing, operation fencing and demand; the autoscaler reconciles that demand
through a small compute-provider boundary. Provider lifecycle, networking,
payloads, credentials, and API calls live in the built-in adapters.

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
- [Compute provider portability](docs/provider-portability.md): provider
  contract, configuration, extension entry point, and host requirements.
- [Hetzner Cloud](docs/hetzner.md): qualified host shape, provider setup,
  snapshots, and local-hot/network-volume storage placement.

## Focused Docs

- [Deployment flow](docs/deployment-flow.md): release inputs, service layout,
  verified node bundles, credentials, and cleanup safety.
- [Managed registry](docs/managed-registry.md): private Docker registry setup,
  persistence, tagging, pruning, and GC.
- [Object-storage snapshots](docs/object-storage-snapshots.md): local-hot park,
  direct S3 detach, AgentEnv range-read wakes, rollout, and qualification.
- [Model relay](docs/model-relay.md): outbound-only OpenAI-compatible relay for
  sandboxes that need to reach model workers behind outbound-only networking.
- [Routing gateway](docs/routing-gateway.md): gateway and exec/SSH routing
  design, performance expectations, and concurrency notes.
- [Scaling policy](docs/scaling-policy.md): scale-to-zero policy, prepare
  signals, builder policy, overcommit, and observed scale-up metrics.
- [Security stance](docs/security-stance.md): gVisor/container security model,
  storage authority, authentication boundaries, and verified bootstrap.
- [VM init](docs/vm-init.md): provider-instance bootstrap and verified post-boot
  node initialization.

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
sandbox API, heartbeat, and node-control credentials and their pinned runtime
artifacts.
