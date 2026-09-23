# Gateway response ownership

`AsyncGatewayResponses` is the single socket handoff for already-authorized
exec event polls and small file PUTs (up to one 64KiB transfer chunk). The HTTP
handler completes authentication, durable route lookup, generation fencing and
any implicit wake before handoff. File acknowledgements require no gateway
ledger projection. Lifecycle operations and exec creation retain their existing
paths because their responses carry durable state changes.

The server transfers the original socket only after the parser's reader/writer
wrappers close. The existing asynchronous node HTTP pool performs the worker
request without reconnect replay; its existing event/RPC pools remain separate.
One response owner handles worker completion, bounded downstream writes,
cancellation and descriptor cleanup. The completion future is a non-cancellable
cleanup receipt, so cancelling a receipt cannot release memory while I/O still
owns its body.

Small PUT bodies begin within the bounded HTTP handler allowance. Handoff
acquires their byte weight from the existing upload memory limiter and retains
that lease until response cleanup. If that budget is occupied, the request
continues through the existing bounded synchronous handler; no new admission
limit or rejection is introduced. Large uploads still stream to the worker
before the complete body arrives. The shared header policy preserves old-worker
connection closure and permits body keepalive only on advertised workers.

Qualification includes a real HTTP gateway limited to two handler threads,
eight simultaneously blocked 64KiB uploads, and a successful health request
while all eight worker calls remain blocked. Tests verify the exact aggregate
body reservation, generation and private-auth forwarding, pre-start/failed
handoff/shutdown cleanup, caller disconnect, ambiguous upstream disconnect
without mutation replay, worker error propagation, and a small write proceeding
while the large-upload memory budget and cold-start queue are occupied. Linux
gateway/control-plane/streaming/transport qualification passed 116 tests.

`build_server(async_proxy_responses=False)` is the existing bootstrap comparison
switch, renamed from `async_exec_events` because it now covers both operations.
It selects the synchronous path before any request begins. Production defaults
to the asynchronous owner; it never retries a request through the other path.
