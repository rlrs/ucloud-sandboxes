# Reduce tool upload and short exec overhead

On 0.5.110, the strengthened 256-agent workload completed all 2,048 cycles
correctly. Measured wake p95 was 427 ms, but model-response-ready through a
checksummed 64 KiB upload and usable tool execution was 1.114 s. The overall
latency target remains unmet. A brief component benchmark ran on the load driver
early in this run; repeat clean qualification before claiming the target.

The atomic shell file writer skips mkdir when the parent already exists and
uses umask 077 with mktemp's 0600 creation mode instead of spawning chmod.
It retains binary streaming, atomic rename, failure cleanup and replacement of
destination symlinks. On a four-core Linux component benchmark with 512 x 64 KiB
uploads and 16 callers, wall time fell from 656–659 to 568–572 ms; child CPU fell
from 2.16–2.20 to 1.54–1.57 seconds. This is not a production latency claim.

Exec start accepts an optional initial_wait_seconds query, bounded to 50 ms.
It snapshots session and up to 100 events atomically under the event lock;
condition waits release the lock. Final output, pagination, long commands and
stdin/TTY retain their original semantics. No command is replayed. The existing
GET events history remains intact. Invalid waits are rejected before dispatch.

SDK 0.4.25 requests this snapshot only for noninteractive execs. Sync and async
handles consume initial events through their existing sequence checks and
require the final watermark before skipping a poll. Older servers omit events
and use the existing polling path. This is an optional protocol extension;
older SDKs continue to work, but need an upgrade to use the reduced round trip.

Validation on Linux includes real process stdout/stderr, bounded and interactive
waits, paginated output, invalid waits, binary uploads and failure cleanup, plus
real gateway-to-node HTTP that verifies query forwarding, retained output and
durable exec routing. SDK tests cover sync/async wait and streaming, incomplete
snapshots, legacy servers, and avoiding GET only after complete output.

Native storage, operating-system dependencies and VM shape are unchanged.
Production load and forced park/restore qualification follow deployment.
