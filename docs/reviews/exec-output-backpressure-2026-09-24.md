# Exec output retention and backpressure

The reported sequence gap (expected 402, received 566) is reproduced locally by
reading through 401 then emitting 676 additional events into the old 512-event
ring. The server silently evicts 164 unread events; the SDK correctly rejects the
incomplete result. This does not require a node failure.

## Implementation

Exec output keeps the existing per-session event budget (512 by default), but
never evicts unacknowledged output. A subsequent `after=N` poll acknowledges
consumption through N. Returning an HTTP response does not acknowledge it, so a
lost-response retry with the same cursor receives the same events. Acknowledged
history is retired only when space is needed. This is one advancing consumer
cursor per exec; independent replay from older acknowledged cursors is not a
persistent log and may still produce an explicit history-loss error.

When full, output pumps wait on the session condition, releasing the shared
manager lock. Kernel pipe backpressure then slows the command. No extra disk
spooling or unbounded memory queue is introduced. Stdout/stderr pumps each retain
at most one additional 4 KiB input chunk; small control/error/exit events may
exceed the data-event budget. Other exec sessions and control requests continue.

Completion waits for output blocked behind that backpressure even if the child
has exited, preventing a premature terminal/empty read from hiding output. The
existing two-second idle-pipe grace remains for descendants retaining a pipe.
Five minutes without reader progress while output is blocked aborts that exec
with an explicit error and nonzero exit status; retained events remain available.
This is an abandonment timeout, not a throughput limit. Completed-session
retention and node-restart semantics are unchanged: this is not durable logging.

Sync and async SDK `exec(input=...)` now feed stdin and drain events concurrently.
This is necessary to avoid a full-stdin/full-stdout deadlock. An input/output
failure or cancellation attempts to kill that exec, preserves the original error,
and cleans up peer I/O tasks. The no-input path is unchanged.

## Compatibility and validation

No HTTP schema change. Existing advancing event pollers work unchanged. Upgrade
the SDK before deploying server backpressure for workloads using large stdin;
older convenience clients send all stdin before consuming output. Manual exec
handles must also feed input and drain output concurrently.

Local real-process and HTTP tests exercise a four-event buffer, 500 KB UTF-8
stdin echoed to both stdout and stderr, both SDK clients, 1,077 sequenced output
events with lost-response replay, unrelated-session progress, delayed completion,
abandoned readers, and SDK failure/cancellation cleanup. No production commands
or deployment were performed. Native sandbox/load qualification remains pending.

Validation results:

- 30 server exec/protocol/admission tests passed, including real HTTP duplex I/O
  against the edited SDK source (`PYTHONPATH=ucloud-sandboxes-sdk/src`).
- 58 focused SDK client/duplex tests passed.
- Full SDK suite: 135/136 passed. The 512-upstream relay keepalive test timed out;
  running that test against unchanged SDK HEAD reproduced the same failure.
- SDK wheel and sdist built successfully into `/tmp/ucloud-exec-sdk-dist` using a
  writable temporary uv cache. These are local development builds, not a release.
- Both repositories pass `git diff --check`.

The changes are local and uncommitted, alongside the earlier wake-admission
fixes. Upgrade the SDK before enabling the server change for stdin workloads.
