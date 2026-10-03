# Node-local model waits: wake on the answer's first packet (spike, 2026-10-03)

**Question.** Option 3 of pause-reclaim item 6: the node pauses a sandbox
while its model call is outstanding and thaws it when the answer arrives,
with no relay or gateway lifecycle call. This needs two answers:
1. Does a paused gVisor sandbox keep an inbound answer until the node thaws
   it, without drops or resends?
2. Can the node tell that a model call is outstanding from TCP alone, through
   TLS?

**Answer: yes to both, with one known miss.**
1. **A paused Sentry holds the answer.**
   - Answers of 2 KB, 64 KB and 1 MB, held for 2 s while paused, reached the
     agent as soon as it thawed (36 calls).
   - No resends and no TCP timeouts.
   - Thawing on the first inbound packet adds 9 ms at the median (11 ms p95)
     to the answer path. Today's relay-and-gateway wake takes 38 ms at the
     median (rl-scale-relay, answer → agent resumes).
2. **Detection works.** The rule: pause when the last relay payload went out
   at least 50 ms ago and the sandbox has used no CPU for 50 ms.
   - It never paused a sandbox outside a model call: 0 of 244 calls across
     five pausing arms, including two concurrent calls per agent and a
     background thread.
   - **The miss:** the first call on each new TLS 1.3 connection is not
     paused. The server's post-handshake session ticket arrives after the
     request and looks like the reply. A keep-alive client loses one wait
     per connection, and 87% of wait time was paused.

## Setup

- **Worker:** one CCX33 from the 0.8.5 snapshot `439222185`, running the 0.8.5
  bundle and the live config, never registered (`gateway_port: 1`). It had
  no swap and no pause tier: the spike pauses with runsc directly.
- **Fake relay:** [wake_spike.py](wake_spike.py) `relay` on the worker
  itself, a TLS HTTP/1.1 server on port 8443. It holds each POST for its
  think time, then answers. One spike-only `iptables` accept let sandboxes
  reach it.
- **Agents.** Managed sandboxes run a loop: compute 200 ms, then a blocking
  HTTPS keep-alive POST through `http.client`. The client is OpenAI-shaped
  but not OpenAI's SDK. Think times are 2–5 s.
- **Node daemon** (in `wake_spike.py arm`):
  - one raw packet socket watching every sandbox's host-side veth;
  - each sandbox's cgroup `cpu.stat`;
  - `runsc pause` and `resume` on the node's runtime root;
  - a thaw on the first inbound payload, FIN or RST.
- **Not exercised:** the node agent's pause bookkeeping and reclaim.
- **Raw evidence:** [raw/](raw/), one JSON per arm with the agents' logs, the
  relay's log and every pause and thaw.

## Results

| arm | calls | paused calls | pauses outside a call | answer → agent p50 / p95 / max | thaw after the packet, p50 / p95 | resends (relay side) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| observe (never pause) | 64 | 0 | 0 | 0.8 / 0.9 / 11 ms | — | 0 |
| local: thaw on the packet | 64 | 56 | 0 | 8.7 / 11.3 / 11.9 ms | 9.8 / 11.9 ms | 5 |
| local, 64 KB and 1 MB answers | 36 | 30 | 0 | 9.5 / 12.4 / 14.9 ms | 9.1 / 11.7 ms | 2 |
| delayed: thaw 2 s after the packet; 2 KB, 64 KB and 1 MB | 36 | 35 | 0 | 2.009 / 2.011 / 2.011 s | 2,009 / 2,011 ms | **0** |
| two concurrent calls per agent | 72 | 65 | 0 | 8.4 / 11.9 / 60 ms | 9.2 / 11.9 ms | 3 |
| background thread ticking every 250 ms | 36 | 31 | 0 | 8.8 / 16 / 17 ms | 9.8 / 13.6 ms | 4 |

**Shared across arms:**
- `runsc pause` 8.7 ms and `runsc resume` 9.5 ms (p50).
- Pauses landed 53 ms after the last send, which is the 50 ms settle plus the
  10 ms policy tick.
- No arm saw a TCP timeout (`TcpExtTCPTimeouts` 0).

## Reading it

- **The wake path is sound.**
  - The delayed arm is the decisive one: while paused, a 1 MB answer sat
    queued for 2 s, with no loss and no resend, and the agent read it within
    1 ms of the thaw.
  - The thawing arms' 2–5 resends per arm, against 0 in the delayed arm, are
    most likely Linux tail-loss probes. The local relay's RTT is under 1 ms,
    so the ACK delay of a 10 ms thaw can trigger one. This is not verified
    (`TcpExtTCPLossProbes` was not sampled), but it made no visible
    difference to latency.
- **The miss is the TLS 1.3 session ticket.**
  - All 27 unpaused calls, across the five pausing arms, were the first call
    on their connection.
  - TLS hides the record content, so the ticket and a reply look alike.
  - Clients that keep their connection lose one paused wait per connection.
    The relay benchmark's agents keep one connection per rollout.
- **Handshakes can be paused.** If the server answers a ClientHello more
  slowly than the 50 ms settle, the client is paused mid-handshake. Its
  ServerHello then thaws it about 10 ms later. With the 2 s delayed thaw this
  held the first handshake for 2 s, which is that arm's only cost.
- **A waiting sandbox freezes completely,** background threads included:
  ticks ran 1.0 s late at the median and 3.6 s at p95. That is already how
  production behaves: the 0.8.5 pause tier pauses on every relay wait.
- **Concurrent calls stay correct.** Each answer thaws the sandbox, and it is
  paused again only once it is idle with a call still outstanding. All 72
  calls completed, and the slowest answer took 60 ms.

## What this means for item 6

Option 3 is feasible. The packet trigger is exact. Detection only infers
"a call is outstanding", but its errors are cheap:
- a miss keeps the sandbox resident, which is today's behavior without the
  pause tier;
- a pause during a handshake costs one thaw.

**What building it takes:**
- **Node:**
  - match the configured relay flows in the existing nftables tables, and
    send their packets to the node agent through NFLOG, not a raw socket;
  - pause through the pause tier's own path (marker, zswap cap, reclaim and
    eviction order) when the rule holds, and thaw on inbound payload.
- **Relay:**
  - deliver to a connected caller at once, with no wake row and no
    `delivery_pending` gate;
  - write a park row only if hibernation is ever asked for, and a durable
    wake only when the caller has disconnected (a hibernated sandbox).
- **Gateway:** no work per wait. The node reports an escalation to hibernate
  itself, so the gateway's route state stays correct.
- **Rollout:** behind a flag, with today's relay-driven park kept until a
  canary passes the relay benchmark and the pressure harness.

**Not measured:**
- a relay across the NAT path (real RTT);
- the reclaim path under the node-local trigger;
- HTTP/2 clients (their PINGs would cause extra thaw and pause cycles);
- many sandboxes per node: the packet watcher's cost at 500 sandboxes.

## Resources

- One CCX33 for about 25 minutes, deleted afterwards, with its known_hosts
  entries cleared. It never registered with the gateway.
- Staging on the gateway: `/work/ucloud-sandboxes/wake-spike-20261003`.
