# Finish exec polling at a proven final output sequence

Realistic load measurements show that externally confirming a completed tool
costs multiple gateway/node round trips. Previously both SDK clients always
issued an additional empty events poll after receiving terminal output.

Exec sessions now expose an additive, nullable `final_sequence`. It is populated
only after both output pumps have stopped and lifecycle/capacity cleanup has
been attempted, including appending the exit event. If a descendant retains a
pipe beyond the existing bounded join, no final watermark is advertised. The
server never guesses that a terminal process has finished producing output.

SDK 0.4.24 uses this watermark to return after it has consumed every event up to
that sequence. It still drains paginated output, verifies contiguous sequence
numbers, and preserves stdout/stderr. Older servers and open output streams
retain the existing empty-read confirmation. Both sync/async `wait()` and event
iteration use the same rule. Older clients continue working with the new server.

Linux validation passed 23 server exec/node-agent tests and 105 SDK tests
(one optional test skipped). Whole SDK discovery also attempted the optional
Inspect integration module, which could not import because Inspect AI is absent
from the driver environment. SDK concurrency tests require raising that test
process's file-descriptor soft limit to 8,192. Focused client protocol tests
cover pagination, legacy/malformed/missing watermarks, both client variants,
and both waiting and event iteration. Server tests cover a held descendant pipe.
Native storage, gVisor and service dependencies are unchanged. This removes a
round trip; production qualification must establish the resulting latency.
