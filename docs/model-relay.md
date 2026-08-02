# Model Call Relay

The sandbox does not need SSH for PRIME/verifiers or mini-SWE-agent control.
The normal path is:

```text
UCloud sandbox -> public relay <- model/Verifiers worker
                         |
                  both sides outbound
```

The sandbox sends OpenAI-compatible HTTP requests to the relay. A worker
running near the model endpoint keeps an outbound long-poll connection to the
relay, receives request envelopes, calls local inference, then posts the model
response back to the relay.

## Run the Relay

The relay is part of this service package. The standard deployment runs it on
the gateway/control-plane VM and exposes it through a UCloud ingress because
model workers may be outside the UCloud private network. Worker/control
endpoints use a deployment bearer token. Generic tunnel callers use a
per-registration capability embedded in the SDK-generated URL, leaving
`Authorization` available for the upstream Verifiers session.

An internal-only deployment still requires authorization. Sandbox workloads
are untrusted members of that network, so network reachability must not grant
permission to poll, complete, or unregister another sandbox's requests. The
registration-scoped capability is the only relay credential an arbitrary
harness needs; the worker and gateway bearer tokens remain confined to trusted
control-plane processes.

In the standard all-in-one deployment, `deploy-all-in-one` writes
`/etc/ucloud-sandboxes/relay.env`, installs the relay unit, creates the sandbox
and worker token files if missing, and starts the service. Run it from the
source checkout after `uv build`:

```bash
uv run ucloud-sandboxes deploy-all-in-one <job-id> \
  --project <project-id> \
  --deployment-id <deployment-id> \
  --private-network-id <private-network-id> \
  --wheel dist/ucloud_sandboxes-<version>-py3-none-any.whl \
  --execute
```

For local development, run the relay directly:

```bash
uv run ucloud-sandboxes serve-model-relay \
  --host 0.0.0.0 \
  --port 8092 \
  --sandbox-bearer-token-file /work/data/ucloud-sandboxes/state/relay-sandbox-token \
  --worker-bearer-token-file /work/data/ucloud-sandboxes/state/relay-worker-token \
  --state-path /work/data/ucloud-sandboxes/state/model-relay.sqlite3 \
  --gateway-url http://127.0.0.1:8090 \
  --gateway-bearer-token-file /work/data/ucloud-sandboxes/state/gateway-token \
  --request-timeout-seconds 7200 \
  --worker-lease-seconds 600 \
  --completed-request-retention-seconds 3600
```

Use the worker bearer token for `/register_rollout`, `/worker/poll`,
`/worker/respond`, and `/worker/error`. Direct OpenAI relay clients use the
sandbox bearer token. General tunnels use their registration-scoped URL and
preserve `Authorization` for the upstream protocol.

Live development relay:

- URL: `https://app-sandboxes-relay.cloud.sdu.dk`
- UCloud ingress id: `12346842`
- all-in-one gateway VM job id: `12361919`
- VM-local port: `8092`
- token files on the gateway VM:
  `/work/data/ucloud-sandboxes/state/relay-sandbox-token` and
  `/work/data/ucloud-sandboxes/state/relay-worker-token`

## Sandbox Environment

For unmodified OpenAI-compatible clients, put the rollout id in the base URL:

```bash
export VF_RELAY_ROLLOUT_ID="run-001"
export OPENAI_BASE_URL="https://relay.example.org/rollouts/run-001/v1"
export OPENAI_API_KEY="<sandbox-relay-token>"
```

Then create the sandbox with outbound networking:

```python
from ucloud_sandboxes_sdk import Image

sandbox = client.create_sandbox(
    id="run-001-model-client",
    image=Image.from_registry("registry.example.org/swebench/task:latest"),
    cpus=1,
    memory_mb=2048,
    disk_mb=10240,
    network="bridge",
    env={
        "VF_RELAY_ROLLOUT_ID": "run-001",
        "OPENAI_BASE_URL": "https://relay.example.org/rollouts/run-001/v1",
        "OPENAI_API_KEY": "<sandbox-relay-token>",
    },
    labels={"rollout": "run-001"},
)
```

The relay also accepts `POST /v1/chat/completions` and `POST /v1/responses` if a
custom transport sets one of these rollout selectors:

- `X-UCloud-Rollout-Id`
- `X-Relay-Rollout-Id`
- `X-Rollout-Id`
- `?rollout_id=<id>`

## General HTTP Reverse Tunnel

The relay can also expose a worker-local HTTP service without an OpenAI API
shape. Register a tunnel through `POST /v1/tunnels/register`, then send public
traffic to:

```text
https://relay.example.org/tunnels/<tunnel-id>/<upstream-path>
```

`register_sandbox_tunnel` returns a URL shaped as:

```text
https://relay.example.org/tunnels/<tunnel-id>/_relay/<registration-token>/
```

The capability is scoped to that registration incarnation and becomes invalid
when the tunnel is replaced or unregistered. Calls through it preserve the
upstream `Authorization` header unchanged. The shared
`X-UCloud-Relay-Token` header remains available for direct/manual clients but
is not required by arbitrary harnesses using the capability URL.

Workers use the same long-poll, lease, renewal, and fenced-response protocol as
model calls. A tunnel request envelope adds `tunnel_id`, `body_base64`, and
`body_size`; `body_base64` is authoritative for arbitrary bytes. Workers return
binary bodies with `body_base64` on `/worker/respond`. Methods, raw
percent-encoded paths, query strings, safe end-to-end headers, status codes, and
request/response bodies are preserved.

This implementation is buffered HTTP. Request and response bodies are limited
to 32 MiB each. Hop-by-hop headers are removed. WebSockets, streaming/SSE, HTTP
trailers, and raw TCP are not implemented by this protocol. An SSE body can be
transported as buffered bytes, but it is not delivered token-by-token.

For Verifiers v1, use one tunnel registration per sandbox generation and include
trusted registration metadata:

```json
{
  "tunnel_id": "vf-run-001-sandbox-007",
  "metadata": {
    "sandbox_id": "sandbox-007",
    "sandbox_generation": 3
  }
}
```

That binding lets the relay park exactly that generation after durable request
acceptance and wake its current placement after committing the response. See
[Verifiers v1 and Parked Sandboxes](verifiers-v1.md) for the complete contract.

## Worker API

Register a rollout before the sandbox starts making model calls:

```bash
curl -sS -X POST https://relay.example.org/register_rollout \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"rollout_id":"run-001"}'
```

The registration response contains a new, random `registration_token`. Save it
with the rollout worker state. Registering the same rollout id again creates a
new incarnation, cancels work from the previous incarnation, and fences every
delayed request carrying its old token. The examples below assume:

```bash
export REGISTRATION_TOKEN="<registration_token returned above>"
```

Workers may heartbeat separately for observability:

```bash
curl -sS -X POST https://relay.example.org/worker/heartbeat \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"rollout_id\":\"run-001\",\"registration_token\":\"$REGISTRATION_TOKEN\",\"worker_id\":\"lumi-worker-1\",\"metadata\":{\"host\":\"lumi\"}}"
```

Long-poll for work. `limit` batches requests; `lease_seconds` reserves returned
requests for this worker before they are retried; `worker_id` is recorded in
stats and request envelopes. For long inference, use a lease long enough for
normal scheduler jitter, then renew while the model call is running:

```bash
curl -sS "https://relay.example.org/worker/poll?rollout_id=run-001&registration_token=$REGISTRATION_TOKEN&worker_id=lumi-worker-1&timeout_seconds=30&limit=8&lease_seconds=600" \
  -H "Authorization: Bearer $WORKER_TOKEN"
```

If no request is available before the timeout, the relay returns
`{"request": null, "requests": []}`.

The response contains `requests`; `request` is the first item for convenience:

```json
{
  "request": {
    "request_id": "7fd...",
    "rollout_id": "run-001",
    "registration_token": "a91...",
    "lease_id": "c4b...",
    "lease_expires_at": 1780000000.0,
    "leased_by": "lumi-worker-1",
    "delivery_count": 1,
    "endpoint": "/v1/chat/completions",
    "method": "POST",
    "headers": {},
    "body": {
      "model": "local-model",
      "messages": []
    }
  },
  "requests": [
    {
      "request_id": "7fd...",
      "lease_id": "c4b..."
    }
  ]
}
```

Workers must echo `registration_token`, `request_id`, and `lease_id` when
renewing, responding, or reporting an error. If a worker misses the lease
window, the request can be delivered to another worker and the stale response
is rejected with `409`.

Workers can renew a lease before it expires:

```bash
curl -sS -X POST https://relay.example.org/worker/renew \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"registration_token\":\"$REGISTRATION_TOKEN\",\"request_id\":\"7fd...\",\"lease_id\":\"c4b...\",\"worker_id\":\"lumi-worker-1\",\"lease_seconds\":600}"
```

For long inference, poll with a lease such as 10 minutes and renew every minute
or two while the local model call is still running. This keeps retry responsive
if a worker dies without forcing the lease to cover the absolute worst-case
generation time.

After calling local inference, post the OpenAI-compatible response body:

```bash
curl -sS -X POST https://relay.example.org/worker/respond \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"registration_token\":\"$REGISTRATION_TOKEN\",\"request_id\":\"7fd...\",\"lease_id\":\"c4b...\",\"response\":{\"choices\":[]}}"
```

Duplicate responses for already-completed requests are accepted and reported as
`{"duplicate": true}` while the completed request id is retained. This makes
worker retry-after-timeout behavior idempotent.

Post worker failures with:

```bash
curl -sS -X POST https://relay.example.org/worker/error \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"registration_token\":\"$REGISTRATION_TOKEN\",\"request_id\":\"7fd...\",\"lease_id\":\"c4b...\",\"error\":\"local model failed\"}"
```

Relay stats are available to workers:

```bash
curl -sS https://relay.example.org/v1/relay/stats \
  -H "Authorization: Bearer $WORKER_TOKEN"
```

Stats include pending and leased counts by rollout, retained completed request
ids, worker heartbeats, counters, and average queue/worker/request timings.

The relay currently handles non-streaming requests. If a sandbox sends
`stream: true`, the relay returns a clear `400` until streaming is implemented.

## Cleanup

When a rollout finishes:

```bash
curl -sS -X POST https://relay.example.org/unregister_rollout \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"rollout_id\":\"run-001\",\"registration_token\":\"$REGISTRATION_TOKEN\"}"
```

Unregistering a rollout fails any pending model calls for that rollout with an
OpenAI-shaped error response.

## Reliability Model

The relay uses explicit request leases:

- pending requests are assigned to a worker for `lease_seconds`
- active workers renew leases during long inference
- expired leases are retried and can be delivered again
- transient worker failures (`408`, `425`, `429`, and `5xx`, including
  connection resets such as `Server disconnected`) release the lease back to
  the durable queue, up to three total deliveries; workers may override the
  classification with an explicit `retryable` boolean
- stale responses with old leases are rejected
- rollout registration tokens fence unregister, poll, heartbeat, renew,
  response, and error calls from older incarnations of a reused rollout id
- responses are rejected when the matching lease has already expired, even if
  no intervening worker poll has requeued the request
- completed request IDs and worker heartbeat diagnostics are bounded by both
  retention time and hard record-count limits
- workers can long-poll batches with `limit=N`
- global, per-rollout, and queued-byte admission limits bound relay work;
  exhausted admission returns `429` with `Retry-After`
- a disconnected caller does not cancel accepted work; its byte-identical retry
  reattaches and receives the committed response without resampling
- `X-UCloud-Relay-Request-Id` supplies an explicit stable attempt identity;
  otherwise an implicit fingerprint becomes reattachable after disconnect,
  relay restart, or a migration transport-epoch change, so normal identical
  calls remain distinct
- park records the gateway's durable transport epoch and wake compares it after
  any autoscaler or wake-triggered relocation; an epoch change publishes the
  retry identity before the migrated harness resumes
- SQLite/WAL durably stores registrations, pending/leased requests, exact
  completed response bytes, and lifecycle notification state
- trusted per-sandbox registration metadata enables generation-fenced park and
  wake notifications through the gateway

Run one relay process per SQLite journal. All admission, claim, response, and
notification transitions are serialized by that process. A restart drops live
TCP connections but restores registrations and requests from the journal;
callers retry and reattach. SQLite provides single-host crash durability, not
multi-process or multi-host HA. That later requires a transactional
server-backed broker with the same idempotency and reattachment contract.

Worker execution remains at-least-once: after a lease expires, a replacement
worker may start the request while the original computation is still running.
Lease IDs prevent both results from committing, but cannot undo duplicate model
compute. Workers should renew before expiry and treat the response POST as the
commit point.
