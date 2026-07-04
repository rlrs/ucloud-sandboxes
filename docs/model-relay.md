# Model Call Relay

The sandbox does not need SSH for PRIME/verifiers or mini-SWE-agent control.
The normal path is:

```text
UCloud sandbox -> public relay <- LUMI worker
                         |
                  both sides outbound
```

The sandbox sends OpenAI-compatible HTTP requests to the public relay. A worker
running near the model endpoint keeps an outbound long-poll connection to the
relay, receives request envelopes, calls local inference, then posts the model
response back to the relay.

## Run the Relay

The relay is part of this service package. It can run on the same public
gateway/control-plane VM as the autoscaler, or on any other public host
reachable from both UCloud sandboxes and LUMI workers. For first tests, run it
on the UCloud gateway VM and expose it with a UCloud public link.

```bash
uv run ucloud-sandboxes serve-model-relay \
  --host 0.0.0.0 \
  --port 8092 \
  --sandbox-bearer-token-file /work/ucloud-sandboxes/state/relay-sandbox-token \
  --worker-bearer-token-file /work/ucloud-sandboxes/state/relay-worker-token \
  --request-timeout-seconds 7200 \
  --worker-lease-seconds 600 \
  --completed-request-retention-seconds 3600
```

Use the sandbox bearer token as the sandbox's `OPENAI_API_KEY`. Use the worker
bearer token for `/register_rollout`, `/worker/poll`, `/worker/respond`, and
`/worker/error`.

Live development relay:

- URL: `https://app-sandboxes-relay-v2.cloud.sdu.dk`
- UCloud ingress id: `12349454`
- all-in-one gateway VM job id: `12349450`
- VM-local port: `8092`
- token files on the gateway VM:
  `/work/ucloud-sandboxes/state/relay-sandbox-token` and
  `/work/ucloud-sandboxes/state/relay-worker-token`

The older relay URL `https://app-sandboxes-relay.cloud.sdu.dk` currently
returns `449` because UCloud still reports its ingress as bound to stopped job
`12346251`. Use the `relay-v2` URL until that stale binding is cleared.

## Sandbox Environment

For unmodified OpenAI-compatible clients, put the rollout id in the base URL:

```bash
export VF_RELAY_ROLLOUT_ID="run-001"
export OPENAI_BASE_URL="https://relay.example.org/rollouts/run-001/v1"
export OPENAI_API_KEY="<sandbox-relay-token>"
```

Then create the sandbox with outbound networking:

```python
sandbox = client.create_sandbox(
    image="registry.example.org/swebench/task:latest",
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

## Worker API

Register a rollout before the sandbox starts making model calls:

```bash
curl -sS -X POST https://relay.example.org/register_rollout \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"rollout_id":"run-001"}'
```

Workers may heartbeat separately for observability:

```bash
curl -sS -X POST https://relay.example.org/worker/heartbeat \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"rollout_id":"run-001","worker_id":"lumi-worker-1","metadata":{"host":"lumi"}}'
```

Long-poll for work. `limit` batches requests; `lease_seconds` reserves returned
requests for this worker before they are retried; `worker_id` is recorded in
stats and request envelopes. For long inference, use a lease long enough for
normal scheduler jitter, then renew while the model call is running:

```bash
curl -sS "https://relay.example.org/worker/poll?rollout_id=run-001&worker_id=lumi-worker-1&timeout_seconds=30&limit=8&lease_seconds=600" \
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

Workers must echo `request_id` and `lease_id` when responding. If a worker misses
the lease window, the request can be delivered to another worker and the stale
response is rejected with `409`.

Workers can renew a lease before it expires:

```bash
curl -sS -X POST https://relay.example.org/worker/renew \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"request_id":"7fd...","lease_id":"c4b...","worker_id":"lumi-worker-1","lease_seconds":600}'
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
  -d '{"request_id":"7fd...","lease_id":"c4b...","response":{"choices":[]}}'
```

Duplicate responses for already-completed requests are accepted and reported as
`{"duplicate": true}` while the completed request id is retained. This makes
worker retry-after-timeout behavior idempotent.

Post worker failures with:

```bash
curl -sS -X POST https://relay.example.org/worker/error \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"request_id":"7fd...","lease_id":"c4b...","error":"local model failed"}'
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
  -d '{"rollout_id":"run-001"}'
```

Unregistering a rollout fails any pending model calls for that rollout with an
OpenAI-shaped error response.

## Reliability Model

The in-memory relay now uses explicit request leases:

- pending requests are assigned to a worker for `lease_seconds`
- active workers renew leases during long inference
- expired leases are retried and can be delivered again
- stale responses with old leases are rejected
- completed request ids are retained temporarily for idempotent worker retries
- workers can long-poll batches with `limit=N`

This is still single-process state. If the relay process restarts, in-flight
requests are lost. The lease/idempotency contract is the API shape a durable
Redis/Postgres/NATS backend should preserve when we need multi-process or
restart-safe relay state.
