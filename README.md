# ucloud-sandboxes

Autoscaler for secure CPU sandboxes on top of SDU UCloud.

The service manages UCloud VM jobs as pool nodes. Sandbox nodes run a node
agent that starts per-request containers under gVisor. Builder nodes build and
push custom Docker images to a private registry. The public gateway accepts
sandbox create/exec/file/image requests, records demand when capacity is not
ready, and an autoscaler loop reconciles that demand into UCloud VM jobs.

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

- [Deployment flow](docs/deployment-flow.md): live rollout, service layout,
  versioning, cleanup safety, public links, and registry/relay deployment.
- [Managed registry](docs/managed-registry.md): private Docker registry setup,
  persistence, tagging, pruning, and GC.
- [Model relay](docs/model-relay.md): outbound-only OpenAI-compatible relay for
  sandboxes that need to reach model workers behind outbound-only networking.
- [Routing gateway](docs/routing-gateway.md): gateway and exec/SSH routing
  design, performance expectations, and concurrency notes.
- [Scaling policy](docs/scaling-policy.md): scale-to-zero policy, prepare
  signals, builder policy, overcommit, and observed scale-up metrics.
- [Security stance](docs/security-stance.md): gVisor/container security model,
  disk quota enforcement, and runtime conformance checks.
- [VM init](docs/vm-init.md): UCloud VM bootstrap findings and post-boot init
  strategy.

## Quick Local Checks

Run tests:

```bash
uv run python -m unittest
```

Run a local gateway:

```bash
uv run ucloud-sandboxes serve-control-plane --host 127.0.0.1 --port 8080
```

Run a local dry-run node agent:

```bash
uv run ucloud-sandboxes serve-node-agent \
  --job-id local-job \
  --node-id local-dev \
  --total-vcpu 2 \
  --total-memory-mb 6144 \
  --total-disk-mb 51200 \
  --host 127.0.0.1 \
  --port 8090
```

Run the sandbox runtime conformance probe on an initialized VM node before
trusting it for hostile workloads:

```bash
uv run ucloud-sandboxes runtime-conformance --sudo --execute --output json
```
