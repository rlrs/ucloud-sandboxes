# Remove transport and heartbeat-read overhead

Worker RPC sockets now use TCP_NODELAY. Reused HTTP responses previously
split headers and JSON into two small writes, triggering Nagle/delayed-ACK
latency. A Linux component test measured p95 44.40ms before and 0.25ms after.

Workers advertise request-body-keepalive-v1. The gateway reuses connections
for bounded JSON requests only after observing that capability on the fresh
heartbeat for the exact origin. The HTTP handler closes before dispatch by
default for body-bearing requests and restores negotiated keep-alive only
after reading the complete framed body. Early rejection, incomplete bodies,
explicit Connection: close, and the public gateway's close policy remain
unchanged. Legacy workers and streaming uploads still get Connection: close.

Exec polling now uses one heartbeat observation for both freshness and absence
checks; an apparently empty worker still requires full inventory. Single-row
heartbeat reads rely on SQLite's SELECT snapshot instead of separate BEGIN
and COMMIT statements. Identity and main-file mode are checked on each read;
sidecar modes are audited on connection creation and write transactions.
SQLite still creates sidecars with the private database mode.

131 Linux HTTP, gateway, control-state, node-agent, runtime, and connection-pool
tests pass. Coverage includes two POSTs sharing one socket, early rejection
forcing a new socket, legacy peers, public/explicit close, stale/missing
capability, replaced state files, and external heartbeat commits. Existing
HTTP-body framing and overload tests also pass. Full load qualification is
required; component timings are not an end-to-end latency claim.
