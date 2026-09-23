# Advisory rollout resource phases

The PostgreSQL relay accepts optional scheduling observations through the
existing rollout registration. These observations do not park, resume, execute,
delete, or mark a model response delivered. Missing or expired advice preserves
ordinary scheduling. The Warden's managed safe point and admission checks remain
mandatory, including when several tools or model calls share a sandbox.

Both `RelayWorkerClient` and `AsyncRelayWorkerClient` expose:

```python
await relay.update_resource_phase(
    rollout_id,
    registration_token=registration_token,
    sequence=7,
    phase="model_wait",
    ttl_seconds=60,
    expected_remaining_wait_seconds=20,
)
```

The synchronous method has the same arguments without `await`. The HTTP operation
is `POST /v1/relay/rollouts/{rollout_id}/resource-phase`, authenticated with the
existing worker bearer token. Its JSON body contains `registration_token` and
`update`, where `update` has the four fields after the token in the example.

Supported phases are `model_wait`, `tool`, `rollout_complete`, `training_pause`
and `training_resume`. Only `model_wait` may contain an expected remaining wait.
TTL defaults to 60 seconds and is bounded to 3600 seconds. Expected remaining
wait is finite, nonnegative and at most 3600 seconds. These are expiration and
validation bounds on disposable advice, not concurrency limits.

Sequence numbers are positive 63-bit integers, increasing within one registration.
The registration token fences both that registration and its immutable sandbox
binding. A replacement registration starts a new sequence; an old token is
rejected. The current registration row stores the observation and its sequence
in PostgreSQL; there is no additional lifecycle journal or node ownership record.
User registration metadata cannot seed the reserved observation field.

An identical sequence and payload may be retried. It returns the original expiry
and does not refresh the hint. A lower sequence returns `accepted: false`; reuse
of the same sequence with different content returns HTTP 409. Expiry removes the
hint's usefulness but retains its sequence fence. Revocation rejects further
updates. Cancellation before commit rolls back the update; ambiguous HTTP loss
can be resolved by retrying the identical sequence and payload.

The existing durable park dispatch reads current advice from the registration
matching the request's token. It forwards the observation only to workers
advertising the resource-phase capability; older workers receive the ordinary
park request. Worker policy uses a bounded monotonic expiry and rejects older
advice for the same registration. Estimated wait affects candidate ordering only
when measured transition costs are available. An update neither creates new
lifecycle work nor bypasses an existing request/generation/wake fence. Advice
updated after a park attempt is picked up by a subsequent ordinary retry; this
is deliberately not an immediate control channel.

The Verifiers integration offers `resource_phase_hints = true` as an explicit
opt-in during the coordinated server/SDK upgrade. It reports expected tool work
on session acquisition and rollout completion only after normal session work
and forwarding cleanup. Its `report_resource_phase()` hook accepts real
integration knowledge about model or training phases. It does not guess a
training pause from cancellation, and a failed advisory request does not fail
the rollout. Ordinary per-request model waits are already observed by the relay;
the integration does not add a duplicate transaction to every forwarded call.

Legacy SQLite relay deployments return HTTP 501 for this optional API until
their explicit PostgreSQL migration. This feature does not introduce automatic
backend selection or trainer restart continuity. A trainer-independent rollout
supervisor remains a separate milestone requiring an actual preemption contract.
