# Model waits through the relay on 0.8.4 (2026-10-03)

**Question.** Production training runs its agents through the model relay. What
does a model wait cost a turn today? The pause tier is off, workers have no
swap, and `direct_idle_park_seconds` is 1.0.

**Answer.** Little latency, but a lot of mechanism.
- **No sandbox parked.** Across 456 model calls, none were parked, because
  the node kept every wait resident.
- **Latency.** Each call added a median of 53 ms and a p95 of 0.16 s on turns
  0–6. The agent resumed a median of 18 ms after the answer.
- **Mechanism.** Every wait still ran the durable park protocol through the
  gateway, and the node declined each one. That cost is load, not latency,
  and it grows with rollouts.
- **The last turn** of every rollout added about 1.3 s. That is a separate,
  unexplained effect at rollout end, not a per-turn tax.

## What production does on a model wait

Traced in the code:

1. The agent's call reaches the relay, which queues it for a worker. For a
   managed sandbox it also writes a durable `relay_lifecycle` row asking to
   park (`shared_control/relay.py:719-727`).
2. A dispatcher posts `/v1/sandboxes/{id}/park` to the gateway. The gateway
   records `model_wait` and forwards the request to the node
   (`control_plane.py:3958-3996`).
3. The node's resident-wait policy (`warm_park.py:32-96`) keeps the sandbox
   resident unless memory is short. It answers 409 `park_deferred`, and the
   relay retries every 30 s while the wait lasts. The node rechecks pressure
   every 250 ms on its own (`node_runtime.py:376-392`, `789-800`).
4. **Under pressure only:**
   - it first tries clean-cache `memory.reclaim`;
   - then a full `runsc checkpoint --hibernate`, which writes memory and the
     guest's written filestore into `pages.img`;
   - the agent's TCP call then drops and is retried after restore.
5. When the answer arrives, the relay dispatches a wake through the gateway.
   For a resident sandbox the wake is a no-op.
6. **Idle parking** (`direct_idle_park_seconds`) never applies to relay agents.
   It skips managed-process sandboxes (`node_runtime.py:558`), and sandboxes
   are not parkable by default.
7. **The pause tier** (C1.1) would pause or hibernate model waits by the Aries
   rule (`pause_tier.py:52-61`). It is off and cannot be turned on as
   configured: production has `swap_gb: 0`.

## Setup

- **Fleet.** Production 0.8.4, autoscaled CCX63 workers (`max_nodes` 3).
- **Driver.** `scripts/bench_rl_scale.py rollout --think-mode relay`, run on the
  gateway with SDK 0.4.34. The harness has sha256 `c59ab924…`, the same as for
  the 512 sleep baseline.
- **Agent.** Each sandbox runs a managed agent that ends every turn with a
  blocking chat-completions call through its relay capability URL. A fake
  worker in the driver holds each call for the sampled think time (5–30 s),
  then commits a fixed completion.
- **Runs:**
  1. **[raw/relay-smoke-8.json](raw/relay-smoke-8.json).** An 8-sandbox smoke
     from zero: 2 turns with 1–3 s thinks. 8/8 succeeded, ready in about 65 s
     (provisioning).
  2. **[raw/relay-64.json](raw/relay-64.json).** 64 tasks, `--seed 1`, 8
     turns. Declared `warm-seeded`, because the smoke's worker was still up
     with 5 images cached.
     - 57 of 64 succeeded.
     - The 7 failures are images without Python 3, where the harness's relay
       agent cannot run (exit 97). That is a harness limit.

## Results (64 sandboxes, 456 model calls)

| per call | turns 0–6 (399): p50 / p95 / p99 / max, s | turn 7 (57): p50 / p95 / max, s |
| --- | --- | --- |
| relay overhead (call time minus think time) | 0.053 / 0.16 / 0.39 / 0.50 | 1.31 / 1.56 / 2.54 |
| answer → agent resumes | 0.018 / 0.025 / 0.033 / 0.049 | 1.27 / 1.50 / 2.51 |
| agent issues call → worker receives it | 0.021 / 0.13 / 0.36 / 0.47 | 0.020 / 0.079 / 0.10 |

- **Every wait was a park request the node declined.** All 456 calls show
  `model_wait_accepted`, and none was ever observed parked. At peak, 45 of
  the run's sandboxes were running and none were parked or paused.
- **The per-turn cost is the relay's own queueing.** Issue to receive is 21 ms
  median and 0.13 s at p95. The answer path takes 18 ms.
- **The final turn is different.** All 56 calls slower than 0.8 s were turn 7,
  in 56 of the 57 rollouts, and only the answer-to-resume leg is slow.
  Something at the end of a rollout holds the last answer for about 1.2 s.
  The cause is not pinned yet. Candidates are the harness's agent epilogue
  and the relay's handling of a rollout's last request.

## Reading it

- **Latency is not the problem today.** With no memory pressure the policy
  costs about 50 ms per turn.
- **The mechanism is the misalignment with the plan's DSec-shaped design:**
  - **Every wait is control-plane work.** Each wait writes durable lifecycle
    rows, makes a gateway round trip, sets the `model_wait` state, and sends
    a 409 and a 30 s retry. All of that is for a decision that is "stay
    resident" unless the node is short of memory.
    - The plan wants a waiting sandbox to keep its route, with park and wake
      as node-local calls (C1.1, "Ownership"), and no PostgreSQL work on warm
      paths.
    - At 512 rollouts with about 18 s turns, this is about 28 waits a second.
      That is a rough estimate, not measured here.
  - **The only answer to memory pressure is a full hibernate.** It is
    expensive: restore takes 0.36–0.83 s, the agent's call is broken and
    retried, and a park takes tens of seconds after gigabytes of guest writes.
    The plan's middle tier, pause with `memory.reclaim` to swap, is built but
    off, and needs swap on workers.
- **This run never reached memory pressure.** At most 45 sandboxes were
  running on CCX63s. The pressure behaviour is the next thing to measure (see
  below).

## Next

1. **Memory pressure.** Re-run the architecture-load `pressure256` shape: 256
   agents with 1.5 GiB heaps, 384 MiB of rotating dirty pages and 20–25 s
   model waits. On 2026-09-23 it killed 16 agents with SIGBUS through tmpfs
   exhaustion. Run it on gate VMs, in two arms:
   1. today's policy (resident, then hibernate);
   2. the C1.1 pause tier with zswap and NVMe swap.

   Gate: no SIGBUS, and pause and thaw within C1.1's budgets.
2. **Node-local model waits.** The relay notifies the node directly, or the
   gateway stops forwarding the park and wake protocol. The node decides:
   - stay resident by default;
   - pause with reclaim under pressure;
   - hibernate only for drain or offload.
3. **Find the last-turn hold** before citing relay turn latency.
