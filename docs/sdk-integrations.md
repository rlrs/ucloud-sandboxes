# SDK And Integrations

The Python SDK and Inspect AI provider live in a separate repository:

- local checkout: `ucloud-sandboxes-sdk/`
- GitHub: <https://github.com/rlrs/ucloud-sandboxes-sdk>

Keep client API examples, install instructions, Inspect usage, and SDK protocol
notes in that repository. This service repo should only document the gateway
behavior that the SDK talks to and the deployment requirements that make those
client flows work.

## Gateway Contract

The SDK expects a deployed gateway URL and, for protected deployments, a gateway
token. Public UCloud links should send this as `X-UCloud-Sandbox-Token:
<token>` because UCloud can consume standard `Authorization` headers before
they reach the gateway. The live development URL is
`https://app-sandboxes.cloud.sdu.dk`; token files are deployment state and must
not be committed.

The gateway is responsible for:

- `POST /v1/sandboxes`, `GET /v1/sandboxes`, and `DELETE /v1/sandboxes/<id>`
- session-based exec with stdout/stderr/stdin event handling
- raw byte upload/download endpoints for files
- sandbox prepare signals for near-term resource demand
- builder prepare signals for near-term image-build demand
- image build/pull/snapshot endpoints, gateway-owned managed registry naming,
  and image-id to immutable worker pull-reference resolution
- image prewarm controls for prepared capacity and multi-node image pulls
- authenticated dashboard and metrics data at `/v1/metrics`

See [api-reference.md](api-reference.md) for endpoint details.

## Image Builds

Managed Docker builds submit a stable image id and no registry coordinates.
The gateway allocates an internal tag using its configured worker registry URL,
forces the builder to push, records the immutable manifest digest, and resolves
later sandbox requests by image id. Builder-local images are not transferred
between VMs. Explicit tags remain supported for external or advanced registry
flows, but SDK integrations must not embed the deployment's private registry
hostname or port.

Registry setup, transport naming, persistence, pruning, and GC are covered in
[managed-registry.md](managed-registry.md).

## Inspect AI And Benchmark Runners

Inspect AI, SWE-bench, TMax, PRIME/verifiers, and similar client-side adapters
should import and document the SDK from the SDK repository. On the service side,
those workloads need:

- outbound network from sandbox containers when they call a model relay or other
  external endpoint
- resource fields on sandbox creation (`cpus`, `memory_mb`, `disk_mb`)
- prepare signals before large bursts when startup latency matters
- pushed registry images for custom benchmark environments
- raw byte file upload/download for prompts, logs, and artifacts
- `profile="linux_host"` for tasks that need VM-like writable paths, cron
  conventions, or optional sshd startup inside the container

## Model Relay

The model relay is part of this service package, but SDK helpers and worker
client usage live in the SDK docs. The production ingress authenticates
worker/control operations separately. Generic sandbox tunnels use
registration-scoped capability URLs, so harness credentials are preserved
end-to-end without sharing the deployment-wide sandbox token with arbitrary
harnesses.

See [model-relay.md](model-relay.md) for relay deployment, lease behavior, and
worker-side protocol details.
