# Gateway response ownership

`AsyncGatewayResponses` is the single socket handoff for already-authorized
exec event polls, small file PUTs (up to one 64KiB transfer chunk) and job
reads with `wait_ms` ([managed-process-reads.md](managed-process-reads.md)).
The HTTP handler completes authentication, durable route lookup, generation
fencing and any implicit wake before handoff. File acknowledgements require no
gateway ledger projection. Lifecycle operations and exec creation retain their
existing paths because their responses carry durable state changes.

Job reads are the one handoff with a projection: the record a job read returns
is cached, and a worker in a lifecycle transition is answered from that cache,
as on the synchronous path. The handler passes that projection with the
socket; the owner runs it on a thread (`asyncio.to_thread`), never on the I/O
loop, after the worker answers and before the reply. Each job read has its own
worker deadline, `wait_ms` plus the usual 60 s, and its own connection lane in
the asynchronous node HTTP pool (1,024 per worker origin, beside 128 control
and 256 event-poll connections). About one read per running managed sandbox
is held at a time, so held reads must not queue behind event polls, nor
starve them.

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

Job reads qualify the same way: two handler threads, 64 job status and log
reads held by the worker, and a health request served while all are held
(`tests/test_gateway_job_reads.py`).

`build_server(async_proxy_responses=False)` is the existing bootstrap comparison
switch, renamed from `async_exec_events` because it covers every handoff.
It selects the synchronous path before any request begins. Production defaults
to the asynchronous owner; it never retries a request through the other path.
