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
  --worker-bearer-token-file /work/ucloud-sandboxes/state/relay-worker-token
```

Use the sandbox bearer token as the sandbox's `OPENAI_API_KEY`. Use the worker
bearer token for `/register_rollout`, `/worker/poll`, `/worker/respond`, and
`/worker/error`.

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

Long-poll for work:

```bash
curl -sS "https://relay.example.org/worker/poll?rollout_id=run-001&timeout_seconds=30" \
  -H "Authorization: Bearer $WORKER_TOKEN"
```

If no request is available before the timeout, the relay returns
`{"request": null}`.

The response is:

```json
{
  "request": {
    "request_id": "7fd...",
    "rollout_id": "run-001",
    "endpoint": "/v1/chat/completions",
    "method": "POST",
    "headers": {},
    "body": {
      "model": "local-model",
      "messages": []
    }
  }
}
```

After calling local inference, post the OpenAI-compatible response body:

```bash
curl -sS -X POST https://relay.example.org/worker/respond \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"request_id":"7fd...","response":{"choices":[]}}'
```

Post worker failures with:

```bash
curl -sS -X POST https://relay.example.org/worker/error \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"request_id":"7fd...","error":"local model failed"}'
```

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
