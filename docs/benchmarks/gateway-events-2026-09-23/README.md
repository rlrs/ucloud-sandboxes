# Gateway exec event response qualification

The event route now performs parsing, authentication and authoritative exec
routing once, then transfers the original client socket after the HTTP handler
has closed its reader/writer wrappers. The existing node HTTP loop owns upstream
polling and downstream writes. Cancellation closes sockets on that loop only
after selector registrations have been removed. There is no replay, extra
listener, or request-by-request fallback. Other gateway routes are unchanged.

`benchmark_gateway_event_proxy.py` runs an isolated worker process holding each
poll for 200 ms, a real gateway pinned to four Linux CPUs, and a separate HTTP
load generator. It uses SQLite gateway authority, authenticated worker requests,
512 simultaneous clients, 20 rounds and ABBA ordering. This component benchmark
does not measure sandbox wake or substitute for the realistic production test.

With the production gateway's 65,536-descriptor soft limit, all four runs returned
10,240/10,240 correct responses. The old path used 10.80/11.48 CPU seconds; the
async path used 9.03/8.68 seconds (about 21% less across the two pairs). Health p95
was 27.6/76.2 ms versus 5.2/5.1 ms. Event p95 was 541/671 ms versus 680/603 ms:
there is no established event latency win. Full distributions and process/thread
and descriptor observations are in `component-512.json`.

An earlier run inherited SSH's 1,024-descriptor limit and intermittently failed
with EMFILE, including routing-read failures. Socket counts return to 260 (256
pooled upstream connections plus listener/loop sockets), while retained database
descriptors vary with connection bursts. The async implementation originally
duplicated each accepted descriptor; qualification prompted replacing that with
explicit transfer of the original descriptor. The remaining database descriptor
churn is independent work; increasing limits alone does not address it.

The bootstrap `async_proxy_responses` (formerly `async_exec_events`) switch exists for ABBA qualification. It selects
one path for the server lifetime, not fallback after a possibly dispatched
request. `gateway.exec_events.response` records upstream plus downstream time;
the original handler span ends at dispatch for the async path. Use the response
span or client latency, not handler duration, to compare complete requests.

Tests cover sleeping polls exceeding HTTP worker capacity with responsive health,
auth/body/stale-route rejection, shutdown while upstream is pending, real partial
writes under downstream backpressure, selector cleanup before descriptor reuse,
and shutdown before a queued response task starts. The focused Linux gateway,
transport and routing-cache suite passed 103 tests.

## Bounded reusable SQLite connections

The descriptor investigation led to `sqlite_pool.py`, shared by routing and
control state. Previously both stores retained at most 16 *idle* connections but
opened arbitrarily many during a burst; SQLite can defer Unix descriptor closure
while peer connections retain POSIX locks. Now readers reserve one of 16 reusable
connections before opening, with FIFO internal waiting instead of client errors.
Routing's independent writer remains separate. A returned lease rolls back any
unfinished transaction/read snapshot; file identity and process ownership remain
checked, including after queued admission. Shutdown wakes queued borrowers and
closes active connections only after their owner returns them.

The same Linux 512 × 20 ABBA qualification now passes at the original **1,024**
soft descriptor limit: all 40,960 responses correct, zero errors, peak descriptors
848–936 and idle descriptors 335–340 across all four runs. CPU was 10.90/10.88 s
for the synchronous path and 8.33/7.48 s for async (27% less across pairs); health
p95 was 90/100 ms versus 3.3/3.0 ms. Event p95 remains mixed: 633/547 ms versus
670/617 ms. `component-512-pooled.json` retains the raw observations. This fixes
observed file exhaustion and reduces CPU/control-plane interference; it does not
establish subsecond realistic sandbox wake at 512.

The combined Linux suite passed 128 tests. Additional lease tests verify failed
opens return capacity, shutdown wakes queued borrowers without interrupting an
owned transaction, and an external WAL writer commits while every read lease is
held, with the next queued reader observing the new value. Existing tests cover
file replacement/fork rejection, permissions, rollback and full durability.
