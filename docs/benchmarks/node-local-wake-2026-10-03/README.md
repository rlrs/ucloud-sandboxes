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

## Part 2: the node's own path, over plaintext (`f59109e`, `2c803e2`)

**Setup.**
- **Fake relay:** `wake_spike.py relay --plain`, HTTP/1.1 on 10.42.0.43:8092,
  on its own CPX32. Traffic crosses the worker's forward path, as it does to
  the real relay at 10.42.0.2:8092.
- **Worker:** one CCX33 with:
  - the 0.8.5 bundle repacked with this branch (`0.8.6.dev0+localwait`,
    bundle `8913326f`);
  - the pause tier, 64 GiB of swap and `sandbox.direct_local_model_waits`;
  - `network_relays` set to the fake relay;
  - `gateway_port: 1`, so it never registered.
- **Harness:** `--mode node` runs no spike daemon. It records the node's own
  pauses and thaws from the pause tier's marker files, and waits on job
  state.
- **Clocks:** the two hosts' clocks differ by about 350 ms. Latencies use a
  skew estimated from request timing (relay receive minus agent send).
- **Analysis:** [raw/node-analysis.json](raw/node-analysis.json).

| arm | calls | paused calls | answer → agent p50 / p95 / max | first pause after the request p50 / p95 | answer → next request max |
| --- | ---: | ---: | ---: | ---: | ---: |
| plain, 8 × 8 (first build) | 64 | 64 | 17 / 20 / 21 ms | 66 / 151 ms | **7.6 s** |
| background ticker (first build) | 36 | 36 | 18 / 20 / 20 ms | 65 / 192 ms | **7.6 s** |
| 64 KB and 1 MB answers | 36 | 36 | 19 / 26 / 26 ms | 66 / 171 ms | 0.41 s |
| two calls per agent | 72 | 72 | 17 / 22 / 74 ms | 66 / 167 ms | 2.1 s |
| plain, 8 × 12 (fixed, `2c803e2`) | 96 | 96 | 17 / 20 / 21 ms | 67 / 129 ms | 0.61 s |
| background ticker, 8 × 12 (fixed) | 96 | 96 | 18 / 20 / 23 ms | 68 / 164 ms | 0.49 s |

**Reading it:**
- **Plaintext pauses every call.** That is 400 of 400 across the arms,
  against 87% over TLS: there is no session ticket to mistake for a reply.
- **The node's thaw costs about 8 ms more than a bare `runsc resume`:** 17 ms
  against 9 ms from answer to agent. The extra is the lifecycle lock, the
  request lock, the prefetch check and bookkeeping.
- **The first build had a stall bug, fixed in `2c803e2`.**
  - A job-status read thaws a paused sandbox and pauses it again afterwards
    (`keep_paused`). If the answer arrived during that read, the sandbox was
    re-paused with its answer inside, and nothing thawed it until the next
    read: stalls of up to 7.6 s here, and unbounded where nothing polls.
  - **The fix:** every answered call is watched until its next request, and
    any pause that lands on it is undone, retried each tick. A paused wait
    whose call is answered is never escalated to hibernate.
  - Both arms were rerun on the patched worker. The slowest gap from answer
    to next request is now 0.61 s, where the agent itself computes for about
    0.25 s.
- **Most pause markers are the harness's own.** The markers (229 for 96
  calls) are mostly status reads cycling thaw and pause, as in the 0.8.5
  canary. At most one marker per arm falls just outside a call, which is the
  re-pause the fix then undoes.
- **Pauses come later when the sandbox is busy.** The first pause comes 66 ms
  after the request at the median, and later at p95 (130–190 ms): status reads
  run inside the sandbox, so its CPU window is not idle.

**Not covered here:** escalation to hibernation and the relay's unacknowledged
answer path. Both need the real relay and gateway, so they go in the
production canary.

## Resources

- **Part 1:** one CCX33 for about 25 minutes.
- **Part 2:** one CCX33 and one CPX32 for about 30 minutes.
- **Common to both:** all VMs were deleted afterwards and their known_hosts
  entries cleared. None registered with the gateway.
- Staging on the gateway: `/work/ucloud-sandboxes/wake-spike-20261003`.
