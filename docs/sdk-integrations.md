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
bearer token passed as `Authorization: Bearer <token>`. The live development
URL is `https://app-sandboxes.cloud.sdu.dk`; token files are deployment state and
must not be committed.

The gateway is responsible for:

- `POST /v1/sandboxes`, `GET /v1/sandboxes`, and `DELETE /v1/sandboxes/<id>`
- session-based exec with stdout/stderr/stdin event handling
- raw byte upload/download endpoints for files
- sandbox prepare signals for near-term resource demand
- builder prepare signals for near-term image-build demand
- image build/pull/snapshot endpoints and image-id to pushed-registry-tag
  resolution
- authenticated dashboard and metrics data at `/v1/metrics`

See [api-reference.md](api-reference.md) for endpoint details.

## Image Builds

Client-submitted Docker builds should use a registry tag and `push=true` so the
result is available to sandbox nodes. Builder-local images are not transferred
between VMs. The gateway records pushed image metadata by image id and registry
tag; sandbox creation can then use either the registry tag or the recorded image
id.

The current deployment uses the private registry alias
`ucloud-sandbox-registry:5000`. Registry setup, persistence, pruning, and GC are
covered in [managed-registry.md](managed-registry.md).

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

## Model Relay

The model relay is part of this service package, but SDK helpers and worker
client usage live in the SDK docs. The service deployment must expose the relay
publicly and configure separate sandbox and worker bearer tokens.

See [model-relay.md](model-relay.md) for relay deployment, lease behavior, and
worker-side protocol details.
