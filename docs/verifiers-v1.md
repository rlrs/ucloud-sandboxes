# Verifiers v1 and Parked Sandboxes

## Production contract

UCloud is a transport for the Verifiers interception server. It does not
replace that server and it does not add a proxy process inside the sandbox.

```text
arbitrary harness
  -> OpenAI/Anthropic HTTP
  -> per-sandbox UCloud tunnel URL
  -> durable UCloud relay
       (request committed, sandbox may park)
  -> UCloud relay worker beside EnvServer
  -> Verifiers interception server
  -> inference provider

response bytes committed by relay
  -> current sandbox placement is woken
  -> same-node park: gVisor-restored connection receives those bytes
  -> migration/failure fallback: same-ID SDK retry receives stored bytes
```

This keeps the responsibilities aligned with Verifiers:

- `EnvServer` remains the model interception, request normalization, retry,
  caching, and scoring boundary.
- The UCloud relay worker is the transport backend analogous to `PrimeTunnel`.
  It registers relay tunnels, polls request leases, forwards to the local
  interception server, and commits the exact upstream response.
- The existing UCloud sandbox integration creates the sandbox and gives it the
  per-sandbox tunnel URL. It must also own detached harness-job start and status
  polling.
- The UCloud relay owns transport durability and sandbox lifecycle
  notifications. It never interprets OpenAI, Anthropic, or Verifiers payloads
  on the general tunnel path.

The relevant upstream boundaries are documented in the
[Verifiers v1 architecture](https://github.com/PrimeIntellect-ai/verifiers/blob/fcb48822eaf35efe22b6f6deda9c4b422920314e/docs/v1/architecture.md),
[tunnel interface](https://github.com/PrimeIntellect-ai/verifiers/blob/fcb48822eaf35efe22b6f6deda9c4b422920314e/verifiers/v1/interception/tunnel/base.py),
and
[Prime tunnel implementation](https://github.com/PrimeIntellect-ai/verifiers/blob/fcb48822eaf35efe22b6f6deda9c4b422920314e/verifiers/v1/interception/tunnel/prime.py).

## Request lifecycle

1. The runtime creates one relay tunnel registration for one sandbox
   generation. Its trusted registration metadata contains `sandbox_id` and
   `sandbox_generation`.
2. A harness makes an ordinary HTTP request to that tunnel. No UCloud process
   is required in the guest.
3. The relay durably writes the request before making it visible to a worker.
4. After durable acceptance, the relay asks the gateway to park that exact
   sandbox generation. A failed park is an optimization failure, not a failed
   model call.
5. The worker leases the request and forwards it to the worker-local Verifiers
   interception server. Lease renewal covers long generations.
6. The worker buffers the complete response, including an SSE response when
   used, and commits its status, safe headers, and exact body bytes to the
   relay.
7. The relay durably commits the response before asking the gateway to wake the
   sandbox. A wake failure makes the worker's response POST retryable; retrying
   that POST does not commit a second result.
8. Same-node parking recreates the sandbox veth/IP before restore and starts
   gVisor with `--allow-connected-on-save=true`, so the relay returns the
   committed response on the checkpoint-restored TCP connection. If migration
   or a transport failure destroys that connection, a same-ID retry reattaches
   to the accepted request and receives the stored response without another
   model sample.

The registration generation fences delayed park and wake operations. A tunnel
registration must not be shared by multiple sandboxes: the lifecycle binding
would be ambiguous and unsafe. A shared EnvServer may still serve many
per-sandbox UCloud tunnel registrations.

## Harness compatibility

The supported boundary is “arbitrary Verifiers harness”, not literally every
possible HTTP client:

- The harness may use any OpenAI/Anthropic dialect that the Verifiers
  interception server supports; the UCloud general tunnel preserves bytes.
- The SDK supplies a registration-scoped capability in the tunnel URL. The
  harness's `Authorization` bearer therefore remains the Verifiers
  rollout-session secret and is forwarded unchanged through the public relay.
- Same-node parking is transparent to the model SDK because gVisor preserves
  the TCP connection.
- For cross-node migration and transport-failure recovery, the model SDK must
  tolerate one transient failure and retry the same logical request. The
  UCloud integration should configure SDK retries where the harness exposes
  that setting. A caller may send `X-UCloud-Relay-Request-Id` as an explicit
  stable logical-request ID.
- Without that explicit ID, the relay keeps an unpublished logical-request
  fingerprint. It publishes the fingerprint only after observing caller
  detachment, a relay restart, or a park-to-wake transport-epoch change caused
  by migration. Consequently, two intentional identical calls are not normally
  coalesced, while migration does not depend on the old relay-side TCP endpoint
  noticing a silent checkpointed peer.

Cross-node migration deliberately restores with a different guest IP so gVisor
closes the old TCP endpoint rather than attempting to use source-node NAT state
from the destination. Transparent cross-node connection preservation remains
out of scope until migration also preserves the sandbox's network identity,
routing, and upstream NAT state. Same-node parking requires neither a guest
shim nor an additional long-running process inside the sandbox.

## Streaming

The general tunnel is buffered HTTP. For SSE, the worker reads the complete
stream from the Verifiers interception server, commits the exact bytes, then
wakes the sandbox and replays from byte zero. This sacrifices token-by-token
delivery to make parking and migration safe.

If the upstream stream fails before the complete response is committed, the
rollout fails. The relay must not splice a partial stream onto a retry or
silently request another sample.

## Harness process lifecycle

The harness itself must be the sandbox's durable primary job, as specified in
[`durable-sandbox-jobs.md`](durable-sandbox-jobs.md):

- starting it returns a durable job ID rather than holding a gateway exec
  stream open;
- job-status polling is control-plane state and must not wake the sandbox;
- stdout/stderr are durable artifacts or bounded event logs;
- parking may sever attached exec/WebSocket/TCP sessions without terminating
  the logical job;
- restore or migration reestablishes the sandbox placement before the job
  continues.

This is separate from the relay. Keeping an attached exec session alive for the
whole rollout would hold a sandbox lifecycle lease and prevent parking.
`runsc exec --detach` is also insufficient because a source-host child process
continues to own its wait and output lifecycle. The selected runtime init is a
replacement for the already-injected PID 1, not an additional relay or harness
sidecar.

The direct runtime does not infer guest idleness from time since the last host
API call. Guest CPU, network, and subprocess activity are invisible to that
clock, so an automatic timeout can checkpoint an actively starting harness.
Production nodes therefore set `--idle-park-seconds 0`. A sandbox parks only
after an explicit lifecycle request, normally the relay's durable-acceptance
callback for the exact sandbox generation.

## Existing integration changes

The existing UCloud SDK relay integration already runs Verifiers-compatible
harnesses:

- `model_relay_env` points the sandbox's OpenAI client at the relay;
- `RelayWorkerClient` and `AsyncRelayWorkerClient` register, poll, renew, and
  return model calls;
- the general HTTP tunnel can forward arbitrary Verifiers-supported dialects
  to a worker-local interception server.

Parking extends this integration rather than replacing it. The existing worker
setup must:

- bind registration to `sandbox_id` and `sandbox_generation` with
  `register_sandbox_tunnel` (or equivalent registration metadata);
- use the byte-preserving response-commit helper so a temporarily unavailable
  wake is retried idempotently;
- continue renewing the request lease during long inference;
- start the harness through the detached job API once that API is available.

Packaging those operations as named `UCloudRuntime` and `UCloudTunnel` classes
inside Verifiers may be convenient, but is not a prerequisite for the current
integration to function. If it is moved into Verifiers' elastic interception
pool, the pinned implementation's hard-coded `PrimeTunnelConfig` in
[the pool](https://github.com/PrimeIntellect-ai/verifiers/blob/fcb48822eaf35efe22b6f6deda9c4b422920314e/verifiers/v1/interception/pool.py#L110-L121)
would need to become configurable.

PRIME-RL itself can continue treating Verifiers as the environment boundary;
its orchestration does not need to understand UCloud park/restore transitions.

## Tested path

`tests.test_verifiers_relay_integration` exercises the pinned Verifiers
`NullHarness` and its real OpenAI SDK through the canonical
`ucloud-sandboxes-sdk`. It stops the harness process after durable relay
acceptance, forces one wake failure, observes the SDK's idempotent response
retry, resumes the harness, and checks that exactly one model call and one
trace turn were committed.

Reproduce it with Python 3.13, the pinned Verifiers checkout, and the canonical
SDK checkout:

```bash
UV_CACHE_DIR=/tmp/ucloud-verifiers-e2e-cache \
uv run --python 3.13 \
  --with /path/to/verifiers@fcb48822eaf35efe22b6f6deda9c4b422920314e \
  --with /path/to/ucloud-sandboxes-sdk \
  python -m unittest -v tests.test_verifiers_relay_integration
```

`scripts/live_verifiers_parking.py` is the production qualification. On
2026-07-30 it first ran the pinned Verifiers `NullHarness` through the public
SDK, public relay, worker-local `InterceptionServer`, and a real direct-runtime
sandbox. It observed `running -> parked -> waking -> running`, preserved the
original relay TCP connection, returned `relay-live-park-ok`, and committed
exactly one model call and one trace turn.

The migration qualification then used an isolated gateway/relay and two fresh
direct-runtime nodes. The real lifecycle was:

```text
running -> parked -> moving_out -> parked -> waking -> running
```

The first run moved source job `12362099` to destination job `12362100` in
70.96 seconds. A phase-instrumented 0.3.64 repeat moved `12362103` to
`12362104`: the complete public migration request took 77.96 seconds, of which
15.38 seconds was the migration protocol after destination readiness. That
protocol comprised 1.29 seconds preparing and exporting the checkpoint,
13.88 seconds transferring and staging it, an 8.04 ms atomic route commit,
22.96 ms activating the destination, and 150.70 ms finalizing the source.
The remaining roughly 62.58 seconds was destination VM provisioning and image
readiness, not route handoff. Committing the model result and waking the
destination took another 0.57 seconds. The harness retried after the intentional
cross-node TCP reset, reattached to the original relay request, and returned
`relay-live-park-ok` with exactly one model call, one trace turn, one transport
reset, one wake notification, and one reattachment.

This closes the isolated SDK/Verifiers migration gate. It does not authorize a
production deployment: production still requires an explicit rollout decision,
health/version verification, and the same flow through the production URLs.
