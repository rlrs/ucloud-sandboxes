# Guest agent protocol

Plan item C5.1 replaces "a host `runsc exec` plus three Python threads per
exec" with one RPC to an agent inside the guest. This document covers step 1:
the agent, the Warden-side listener and their protocol. **Nothing here is
wired into the production exec path or the node-agent routes yet.**

| Part | Where |
| --- | --- |
| Agent (Go) | `ucloud-sandbox-init agent` in `runtime/managed_process/agent_linux.go`. Its doc comment is the normative wire contract. |
| File helper | `ucloud-sandbox-init files read\|write\|stat` in `runtime/managed_process/files.go`, the existing static file helper, which the agent now runs as a child |
| Listener and client (Python) | `ucloud_sandboxes/guest_agent.py` |
| Tests | `runtime/managed_process/agent_test.go` and `tests/test_guest_agent.py`, which runs the real binary on the host |
| Benchmark | `scripts/benchmark_guest_agent.py` |

## Why the agent dials out

The 2026-10-02 qualification (S7) settled the direction of the connection:

- With `--host-uds=create`, a guest listener in a bind-mounted host directory
  is a real host socket. But a checkpoint taken while it exists panics the
  Sentry ("Cannot save endpoint with bound host socket") and leaves the
  sandbox stopped. That happens with stock checkpoint and with `--hibernate`.
- With `--host-uds=open`, a guest that *connects* to a host listener
  checkpoints and restores fine.

So the Warden owns the listener and the agent dials it. A guest never listens
on a host-visible socket. `open` also stops the guest from creating
host-visible sockets.

## Transport and frames

- **Transport.** There is one Unix stream connection per sandbox. The Warden
  listens on `agent.sock` in a per-sandbox directory that is bind-mounted into
  the guest. The agent's `--connect` flag, or the
  `UCLOUD_GUEST_AGENT_SOCKET` environment variable, names the guest path.
- **One connection.** At most one connection is live. A newly accepted
  connection supersedes the old one, which fails like any lost connection.

Every frame is:

```
+-----------------+-----------------+----------------------+------------------+
| H: uint32 BE    | P: uint32 BE    | header: H bytes      | payload: P bytes |
| 1 .. 1 MiB      | 0 .. 1 MiB      | one JSON object      | raw bytes        |
+-----------------+-----------------+----------------------+------------------+
```

- **Bytes stay bytes.** Headers are small JSON objects and data stays raw
  bytes, so binary output passes unchanged. Today's text-mode pipes decode it
  as UTF-8 with replacement.
- **Payloads.** Only `input` (node → agent) and `output` (agent → node) carry
  a payload, and theirs is never empty.
- **Strict headers.** A header has exactly the keys listed below plus `type`.
  No key is null unless the table says so, and nothing follows the object.
- **Violations.** Anything else is a protocol violation, and the receiver
  closes the connection.

### Handshake

The node speaks first, then the agent replies:

1. node → agent: `hello{version: 1, window}`. `window` is 64 KiB to 64 MiB
   (default 1 MiB) and bounds every flow below.
2. agent → node: `hello{version: 1, agent, pid, build, abandoned}`.
   - `agent` is a random id fixed for the agent process's lifetime. A
     checkpoint and restore keeps it, because guest memory is restored.
   - `abandoned` counts the ops the agent killed when its previous connection
     ended.

Neither side sends anything before its hello, and a second hello is a
violation.

### Node → agent

| Frame | Fields | Meaning |
| --- | --- | --- |
| `exec` | `id, argv, env, cwd, uid, gid, stdin` | Start a command (details below) |
| `read_file` | `id, path, max_bytes, uid, gid` | Stream a regular file as `output` on `stdout` |
| `write_file` | `id, path, max_bytes, uid, gid` | Content follows as `input`, then `input_close`; replaced atomically |
| `stat` | `id, path, uid, gid` | Follows symlinks |
| `input` + payload | `id` | exec stdin or `write_file` content |
| `input_close` | `id` | Closes stdin after the queued input; at most once |
| `signal` | `id, signal` | 1..64, to the op's process group |
| `credit` | `id, stream, offset` | Cumulative output bytes of `stream` the node has consumed |
| `pong` | | Answers `ping` |

### Agent → node

| Frame | Fields | Meaning |
| --- | --- | --- |
| `started` | `id, pid` | exec only; precedes its output |
| `output` + payload | `id, stream` | `stdout` or `stderr`; `read_file` data arrives as `stdout` |
| `input_credit` | `id, offset` | Cumulative input bytes the agent has consumed |
| `exit` | `id, exit_code, signal, stdout_bytes, stderr_bytes, output_complete` | Terminal for exec. `exit_code` is null exactly when `signal` is set. The byte counts must equal what the node received. |
| `done` | `id, stat` | Terminal for file ops. `stat` is null except for `stat`, where it is `{type, size, mode, mtime_ns, uid, gid}` and `type` is `file`, `directory` or `other`. |
| `error` | `id, code, message` | Terminal: the op did not start, failed inside the agent, or, for a file op, failed |
| `ping` | | Liveness probe |

**Op ids.**

- Ids are 1..2^53−1. The node assigns them and they strictly increase per
  connection.
- A frame for an id that was issued but is no longer live is ignored in
  either direction. It raced the op's end: for example an `input_credit`
  after `exit`, or a `signal` after the command ended.
- A frame for an id that was never issued is a violation.

**Error codes.**

- `invalid`: a bad argv, env, cwd, path or limit.
- A `message` the agent composes is at most 4 KiB, even when it quotes a long
  argv[0], so an error reply is always a valid frame.
- `credentials`: a non-root agent was asked for another identity.
- `spawn_failed`: for example, the executable is missing.
- `too_many_ops`: more than 1,024 live ops per connection.
- From the file helper: `not_found`, `permission_denied`, `too_large`,
  `not_regular`, `failed` and `killed`.

## Exec semantics

- **Identity.**
  - As root, the agent clears supplementary groups, then sets `gid` and
    `uid` in the child before `exec`. A non-root uid loses its capabilities
    through the kernel's setuid rules. A root exec keeps the agent's.
  - A non-root agent, as in the host tests, runs only its own identity.
- **Environment.** The command gets the agent's environment without any
  `UCLOUD_GUEST_AGENT_*` variable, overlaid with `env`. As PID 1, the agent's
  environment is the OCI process env, which matches `runsc exec --env`. PATH
  lookup uses the merged environment.
- **Process group.** Each command leads a new process group. Signals and the
  kill on connection loss go to the group. They stop once the leader is
  reaped: the agent waits with `waitid(WNOWAIT)` and reaps under the op's
  lock, so a group id is never signalled after it could have been reused.
- **stdin.** Without `stdin`, the command reads `/dev/null`. With it, input is
  written in order. After a write error (the child closed stdin) input is
  discarded but still credited, so the node never blocks on a command that
  stopped reading.
- **End of output.** After the leader exits, the agent waits for EOF on both
  outputs. If neither produces a byte for 2 s, a descendant holds them. The
  agent then closes its ends and reports `output_complete: false`, keeping
  today's 2 s idle-pipe grace. `exit` follows all output frames.
  - Time blocked on the node, waiting for credit or inside a send, is not
    idleness, so a node that stalls never truncates output
    (`TestAgentAStalledNodeIsNotIdleOutput`).
  - Nor is frozen time: a late poll restarts the grace, because a thawed pump
    may not have run yet (`TestAgentAFreezeDuringTheDrainKeepsOutput`).

## File operations

Each file op runs the existing `files` helper as a child under the requested
uid/gid, so permissions and ownership are those of the exec identity. A fresh
process changes credentials, so no per-thread credential switching is needed.

- **Read.** Fails on a non-regular file, a file larger than `max_bytes`, or
  one that grows while it is read.
- **Write.** Writes to a temporary file in the same directory and renames it
  over the target. A symlink at the target is replaced, not followed, and a
  directory at the target fails with `not_regular`.
- **Exit statuses.** The helper's exit statuses (3 to 7) map to the error
  codes above. Status 2, which a Go panic or fatal runtime error such as
  running out of memory exits with, stays `failed`. Every failure is still
  non-zero, as `direct_service.py` expects.
- **Cost.** One fork and exec of the static helper costs about 2 ms per small
  write on the host. An in-process fast path for the agent's own identity is
  a possible follow-up.

## Flow control and bounds

- **Output.** Per op and stream, the agent never has more than `window` bytes
  sent beyond the node's `credit` offset. It reads the pipe only while it has
  credit, so a slow consumer applies backpressure to the command through the
  kernel pipe.
- **Input.** Per op, the node never has more than `window` bytes sent beyond
  `input_credit`.
- **When to credit.** Each receiver credits once it has consumed `window/4`
  past its last credit. Senders send whatever fits.
- **No stall.** A blocked sender means its receiver holds `window`
  unconsumed bytes, so the receiver is always due to credit.
- **Per-op credit.** Credit is per op, so a session whose reader is slow
  never blocks other sessions. The test
  `test_a_slow_consumer_does_not_block_other_sessions` covers this.

Memory is bounded on both sides:

- **Agent.** Two 64 KiB read buffers per exec, plus at most `window` queued
  input per op.
- **Node.** At most `window` buffered output per stream and op. On top of
  that, a per-connection read buffer (128 KiB, growing only for frames larger
  than that) and unsent frames, which credit bounds. The one frame a peer can
  elicit at will is `pong`, and the node sends none while bytes are queued.
- **`read_file`.** The node enforces `max_bytes` itself.

## Connection loss: ops fail explicitly; nothing resumes

**Ops never outlive their connection.**

- **Agent side.** When a connection ends, through EOF, an error, a
  violation, a liveness timeout or supersession, the agent SIGKILLs the
  process group of every live op and discards its output. The next hello
  reports the count as `abandoned`.
- **Node side.** Every op without a terminal frame fails with
  `GuestAgentDisconnected`. Its effects are unknown: the command may have run
  partly.

Resuming was rejected:

1. **The lifecycle fence.** Park, hibernate, drain and fork take the
   sandbox's exclusive lifecycle lease. That lease waits for exec and file
   activity, which holds the shared lease (`sandbox_exec.py` `start`). So in
   production a capture never happens with an op in flight. Resume would only
   matter for faults.
2. **Warden restart.** A Warden restart loses the node's op table, so resume
   could only cover the agent's half.
3. **Fork.** A fork restores one agent's memory into N sandboxes. Killing
   inherited ops is the only answer that does not run unobserved duplicate
   work.

What each event does:

| Event | Connection | Ops |
| --- | --- | --- |
| `runsc pause` / `resume` | kept: the whole guest freezes | stall, then continue (`test_pause_and_resume_keep_sessions`) |
| checkpoint / restore | the restored agent finds it dead and redials | none in flight by the fence. Any that were are killed by the agent and failed by the node (`test_a_reconnect_fails_in_flight_ops_and_kills_them`) |
| Warden or listener restart | EOF; the agent redials with backoff | as above (`test_a_listener_restart_fails_ops_and_the_agent_redials`) |
| agent crash | EOF | failed on the node. The commands are orphaned, as with a crashed `runsc exec` client today. |

## Liveness and reconnect

- **Probe.** The agent pings after `--ping-interval` (default 10 s) without
  receiving any frame. It redials if nothing arrives within one more
  interval. The ping is sent apart from the probe, so a writer blocked on a
  node that stopped reading cannot hold the probe off
  (`TestAgentProbeTimesOutWhileItsWritesBlock`).
- **Frozen guest.** A keepalive tick that arrives more than two ticks late
  means the guest was frozen. The probe then restarts instead of counting the
  frozen time against the node. `TestAgentSurvivesAFreezeWithAProbeOutstanding`
  covers this; it fails when the guard is removed.
- **Node.** The node never times out a connection. It answers `ping` with
  `pong`, unless bytes for the agent are already queued: they arrive after
  the ping and answer it, as any frame does.
- **Redial.** The agent redials at once after a completed connection ends.
  Otherwise it backs off from `--backoff-min` (10 ms), doubling to
  `--backoff-max` (500 ms), with ±25% jitter. A connection that ends before
  its handshake also backs off, so a listener that rejects the agent cannot
  cause a busy loop.
- **SIGTERM.** On SIGTERM or SIGINT, the agent kills its live ops and exits.

## Trust and ownership

**The listener treats its peer as guest code.** With `--host-uds=open`, any
guest process that may open the socket can dial it, which in practice means
guest root. A peer that violates the protocol only loses its own connection.
`test_a_hostile_guest_peer_only_loses_its_connection` covers deep JSON
nesting, oversized reads and mistyped values, and checks that the listener
thread survives.
The node checks the following:

- **Headers.** Exact key sets and exact JSON types; NaN and Infinity are
  rejected.
- **Bounds.** Frame size bounds, and credit bounds on every output.
- **Consistency.** The byte counts in `exit` must equal the bytes received.
- **Op ids and order.** Op ids must have been issued, and `started` must
  precede output.
- **`read_file`.** `max_bytes` is enforced on the node.
- **pid.** The `pid` in `started` is a guest pid. It is informational and
  never used for host-side signals.

Socket ownership:

- **Directory.** It must be owned by the Warden's uid and not group- or
  world-writable.
- **Socket.** It is bound under a temporary name, `chmod`ed to 0600, put in
  listen state and only then renamed into place. A connectable socket
  therefore never has looser permissions. The rename also atomically replaces
  a stale socket left by a crashed previous owner.
- **Close.** `close` unlinks the path only if it still holds this listener's
  inode, so it never removes a successor's socket.
- **Accept failures.** A failed `accept`, such as `EMFILE`, pauses accepting
  for 0.5 s; it neither spins nor ends the listener thread.

## Python API

```python
with GuestAgentListener(sandbox_dir / "agent.sock") as listener:
    listener.wait_connected(timeout=5)
    session = listener.start_exec(argv, env={}, cwd="/", uid=0, gid=0, stdin=True)
    session.write_stdin(b"...")         # waits for credit
    session.close_stdin()
    for event in session:               # ExecOutput(stream, data: bytes) ..., then ExecExit
        ...
    session.signal(signal.SIGTERM)      # to the process group; a no-op after exit
    data = listener.read_file(path, max_bytes=n, uid=0, gid=0)
    listener.write_file(path, data_or_binary_file, size=n, uid=0, gid=0)
    stat = listener.stat(path, uid=0, gid=0)
```

- **Threads.** There is one thread per listener. It accepts the connection
  and reads frames. Callers write from their own threads under the
  connection lock. A frame the socket cannot take at once is queued, and the
  listener thread flushes it. There are no threads per session.
- **Exceptions.**
  - `ValueError`: an invalid request, including one whose header would
    exceed 1 MiB. Nothing was sent.
  - `GuestAgentUnavailable`: no connection was ready, or the listener closed,
    and nothing was sent, so retrying is safe.
  - `GuestAgentDisconnected`: the connection was lost and the outcome is
    unknown.
  - `GuestAgentTimeout`: the caller's deadline passed and the op was killed.
  - `GuestAgentError(code)`: the agent reported the error code shown.
- **Consumption.** `next_event(timeout)` returns queued output before it
  raises an op's terminal error.

## Measurements (host, no gVisor)

`uv run python scripts/benchmark_guest_agent.py` was run on a 30-CPU host
with Linux 5.15 and Python 3.10. "Today" is `ExecSessionManager` with a
runtime that returns the argv unchanged: a host `Popen`, three Python threads
and text pipes, but no `runsc exec`. In production `runsc exec true` adds
24 ms median on a running sandbox (qualification 2026-10-02), so "today" is a
lower bound on the real cost.

| Metric | Agent | Today (no runsc) | C5.1 gate |
| --- | --- | --- | --- |
| exec start p50 / p99 | 0.28 / 0.41 ms | 0.67 / 0.91 ms (+24 ms runsc exec) | ≤ 5 ms on the node |
| exec round trip (`true`) p50 / p99 | 0.57 / 0.75 ms | 0.82 / 1.05 ms | |
| execs/s, 1 thread | 1,741 | 1,225 | |
| execs/s, 8 threads | 4,299 | 1,001 | ≥ 1,000 per node |
| execs/s, 32 threads | 4,294 | 881 | |
| node CPU per exec, 8 threads | 0.15 ms | 0.94 ms | |
| stdout, one stream | 1,102 MB/s | 208 MB/s | ≥ 100 MB/s |
| node CPU per GB of output | 1.0 s | 5.3 s | |
| stdin, one stream | 801 MB/s | | |
| file write / read, 64 MiB | 395 / 619 MB/s | | |
| file write, 4 KiB, p50 / p99 | 2.0 / 10.7 ms | | |
| bytes 0x00..0xff intact | yes | no (U+FFFD replacement) | |

Several things remain to measure inside gVisor:

- fork and exec in the Sentry;
- the gofer-donated host socket;
- the guest's CPU share.

Agent throughput on the host is bounded by the Python consumer, about 1 s of
node CPU per GB. Phase 2 of C4.2 moves exec bytes to a Go terminator.

## What step 2 needs

Wiring:

1. **Launch.** Run every sandbox with `--host-uds=open`. The Warden creates
   the per-sandbox directory (mode 0700) and bind-mounts it into the guest,
   for example at `/.ucloud-agent`.
   - Qualify a read-only mount: a connect needs write permission on the
     socket inode, not the mount.
   - The toolkit layer delivers `ucloud-sandbox-init` to every image.
2. **PID 1.** `supervise` and `agent` become one PID 1 process, with the
   managed-process control (`start`, `status`, `logs`, `signal`) as new
   frame types, replacing `runsc exec … ctl`. As PID 1, the agent also needs
   one `waitid(P_ALL, WNOWAIT)` reaper that hands op leaders to their ops and
   reaps orphans. Per-op waits would race it.
3. **Listener lifecycle.** The Warden opens the listener before
   `runsc create` or `restore` and keeps it across park and restore of one
   incarnation. It closes it at delete. Restore readiness becomes "agent
   hello received" in place of the `runsc exec` readiness command.
4. **Restore spike.** Qualify what a restored guest's old connection does in
   runsc: error, EOF or silence.
   - If it is silent, the liveness probe finds it after one or two
     intervals. Lower `--ping-interval` to about 1 s: 500 sandboxes then
     cost about 500 small frames/s.
5. **Exec manager.** `ExecSessionManager` adopts `GuestExecSession`.
   - Keep the lifecycle and capacity leases; the runsc start fence goes.
   - Resolve the default identity on the node: the OCI user, or the managed
     identity annotations. The protocol takes no defaults.
   - Decide the exec capability set: today a root exec inherits the agent's
     set, which includes `CAP_SETUID` and `CAP_SETGID` for managed sandboxes.
   - The HTTP and SDK exec events carry `data: str`. A binary-safe API needs
     a coordinated SDK change, such as a base64 field. Until then the API
     edge decodes UTF-8 with replacement, as today.
6. **Files.** `read_file` and `write_file` in `direct_service.py` call the
   listener. This deletes `_file_exec` over `runsc exec` and
   `sandbox_file_write_script`.
7. **Listener threads.** One thread per listener means 500 threads at 500
   sandboxes. Move `_run` to one selector thread per node; the per-connection
   state already supports that. Alternatively, wait for the phase 2 Go
   terminator.
8. **Hardening.** Add per-listener accept and frame-rate limits, because
   guest root can dial the socket and spend Warden CPU, and the GIL is
   shared across sandboxes.

Fake runsc harness (`tests/harness/fake_runsc.py`):

- **Launch.** Accept `--host-uds=open`, and map the bundle's agent bind mount
  to its host directory.
- **Guest agent.** Start the real agent binary as the "guest", connected to
  that path. Build it once per test session, as `tests/test_guest_agent.py`
  does.
- **Pause and resume.** SIGSTOP and SIGCONT the agent's process group along
  with the sentry.
- **Checkpoint and restore.** `checkpoint --hibernate` kills the agent, and
  `restore` starts a new one. The new agent has a new `agent` id and
  `abandoned` 0, unlike a real restore, which keeps the id.
- **Node agent.** `node.exec` and the file routes then exercise the listener
  end to end, without root.

Deletions this enables, against the C6.2 ledger of about 600 lines:

- most of `sandbox_exec.py` (762 lines);
- the shell file-write fallback;
- `runsc exec … ctl`;
- the per-exec runsc start fence in `node_runtime.py`.
