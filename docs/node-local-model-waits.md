# Node-local model waits

Status: built 2026-10-03, off by default (`sandbox.direct_local_model_waits`).
It is pause-reclaim item 6, as DSec does it: a waiting sandbox is frozen and
woken by the node alone, with no relay or gateway work per wait. Evidence:
[benchmarks/node-local-wake-2026-10-03](benchmarks/node-local-wake-2026-10-03/README.md).

## Before and after

**Today.** For each model call, the relay:
1. writes a durable park row and sends `/park` through the gateway. The
   gateway writes its route and program state, and the node pauses the
   sandbox.
2. when the answer arrives, writes a wake row, sends `/wake` through the
   gateway, and holds the answer until the node has thawed the sandbox.

That is four network round trips and about 20 PostgreSQL statements per wait
on top of the request itself, and 38 ms from answer to agent.

**With node-local waits:**
- The agent calls the relay over **plaintext HTTP/1.1 on the private network**:
  `OPENAI_BASE_URL=http://10.42.0.2:8092/rollouts/<id>/v1`, a
  `sandbox.network_relays` IPv4 literal. Workers already allow this
  destination.
- **The node watches only TCP headers.** nftables logs both directions of
  every private-relay flow to one NFLOG group (`local_wait.py`). It never
  reads payloads.
- **The node pauses** a sandbox when the last relay payload went out at
  least 50 ms ago and the sandbox's cgroup used at most 2 ms of CPU over the
  last 50 ms.
  - The pause goes through the pause tier, so reclaim, eviction order,
    escalation and thaw prefetch all apply.
  - Any lifecycle activity (an exec, a file read, a park) skips the pause; it
    never waits for one.
- **The relay delivers the answer at once.** The answer's first data packet
  thaws the sandbox. A paused gVisor network stack holds the packet until
  the thaw, which adds about 10 ms.
- **The relay waits up to 4 s for the guest's TCP acknowledgment** of every
  byte of the answer (`TIOCOUTQ` reaching 0).
  - **Acknowledged:** the delayed wake row is cancelled, so the wait cost no
    lifecycle call at all.
  - **Not acknowledged:** the sandbox was hibernated while its call was out
    (pressure escalation, drain). The request becomes reattachable, and the
    durable wake row fires at 5 s through the gateway, as today. The restored
    agent's retry reattaches to the stored answer.

**Per wait, the relay writes:**
- a `local_wait` flag on the request;
- one wake row, created with the result in the same transaction;
- one `UPDATE` to cancel it.

No park row, no `/park`, no `/wake` and no gateway statements.

## Why plaintext

The node can only tell an answer from other traffic when the flow carries
nothing else. Over TLS 1.3 the server's session ticket arrives after the
first request, and it looks like the reply: the spike missed every
connection's first call. On the private plaintext path, the only inbound data
on an HTTP/1.1 relay connection is the response.

The path stays on the Hetzner private network, under the same trust as the
node agent's plain-HTTP control API. The relay token is per rollout, and a
sandbox cannot observe another sandbox's traffic.

## Which calls take this path

- **Relay:** a request from a non-loopback peer while the switch is on. The
  TLS ingress proxies from loopback, so ingress callers keep today's
  relay-driven park and wake.
- **Node:** flows to the IPv4-literal private relays in `network_relays`.
  Sandboxes must be owned, parkable, managed-process sandboxes with a network
  lease. A DNS-named relay is a TLS ingress, and the node ignores it.

**Mixed fleets.** With the switch on, the relay sends no park for a private
caller. On a worker without the node side, such a sandbox stays resident for
its waits. Roll the switch out with a full worker replacement, as for 0.8.5.

## Failure modes

| case | what happens |
| --- | --- |
| The answer arrives before the pause | No pause: the inbound payload clears the outstanding call. |
| The answer races the pause | The thaw runs right after the pause. |
| The sandbox works while its call is out (another thread, or several calls) | It is not paused until its cgroup is idle. A second outstanding call pauses again only when the sandbox is idle after handling the first. |
| The pause escalates to hibernation | Its connection dies, and the answer is never acknowledged. The relay marks the request reattachable, and the delayed wake restores the sandbox through the gateway. |
| The relay process dies before checking the acknowledgment | The durable wake fires after 5 s. Waking a running sandbox does nothing. |
| An HTTP/2 client | Its PINGs read as replies: more thaw and pause cycles, but never a wrong pause. |

## Rollout and gates

1. **Canary worker on the switch:**
   - the 64-task relay benchmark with `--sandbox-relay-url http://10.42.0.2:8092`;
   - the spike's plain-HTTP arm: every call paused, none outside a call;
   - the pressure harness with escalations: every hibernated wait comes back
     through the delayed wake.
2. **Gates:**
   - answer → agent p95 at most today's 0.046 s;
   - pauses and thaws counted per wait;
   - no lost agent and no duplicate model sample;
   - no gateway `/park` or `/wake` for an acknowledged answer.
3. **Trainers** move `OPENAI_BASE_URL` to the private endpoint.

## Not covered yet

- The packet watcher's cost at 500 sandboxes per node (one NFLOG message per
  relay packet).
- The growth-forecast bookkeeping (`observe_managed_wait` and continuation
  admission) does not see local waits. Thaws are not admission-gated;
  DSec's "running work first" makes them the priority anyway.
