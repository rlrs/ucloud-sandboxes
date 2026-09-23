# Use an asynchronous transport for buffered worker RPCs

The 0.5.111 cold 256-agent run completed all 2,048 cycles correctly. Measured
wake p95 was 295 ms and the uploaded-tool path 905 ms, but the provisioning
phase still had tool p95 1.386 s. Forced park/restore at 64 agents completed all
256 cycles correctly, with wake p95 774 ms and tool p95 1.300 s. These results
do not satisfy the complete acceptance gate.

A 256-agent barrier experiment compared one-period CPU burst credit against
zero credit, balanced within each worker. All 2,048 cycles completed correctly.
Bursting shortened memory verification but did not resolve the latency tail;
no CPU policy change is shipped. The diagnostic gateway sample had roughly 35%
CPU idle and low disk activity. Synchronized request traffic still delayed the
HTTP path. That experiment also included a brief diagnostic profile, so it is
not clean qualification. Reports and assignments are retained.

This release retains the synchronous gateway handlers and their scheduling,
generation checks, durable writes and response semantics. Bounded worker RPCs
use one aiohttp I/O loop instead of each handler running the blocking urllib3
HTTP parser and socket machinery. Event polls have their own connection pool.
Uploads and streaming downloads retain their existing streaming transports.

The asynchronous transport does not follow redirects, decode compressed bytes,
or replay requests. A public aiohttp tracing hook prevents a reconnect retry
from sending request headers twice, including PUT and DELETE. Connection-pool
queue timeouts retain the existing proven-before-dispatch retry fence; read
and transport failures remain ambiguous. Responses are buffered only up to the
existing gateway limit plus one sentinel byte. Shutdown cancels waiting callers.

Linux validation: 98 HTTP/control-plane tests passed, followed by five transport
tests including the added queue-timeout distinction. The same transport tests
are run against the packaged wheel on production's Python 3.14 interpreter.
The four-core Linux proxy component comparison reduced gateway CPU about 12–17%
with the final safety checks; tail latency was not consistently better. A full
live test is required before claiming a production latency improvement.

Native storage, worker resource limits, VM shape and SDK are unchanged.
