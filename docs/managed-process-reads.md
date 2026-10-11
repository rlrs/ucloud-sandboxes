# Managed-process reads without entering the guest

Status: design, 2026-10-11. Moves job status, logs, start and signal into
`ucloud-noded`; reads no longer enter the guest.

## Why

On 2026-10-11 a production run put about 200 OpenCode/Pi sandboxes on each of
three workers. In four minutes 165 requests failed with the gateway's 504
`node_request_timeout`: process starts, execs and deletes.

The hosts were not the bound: CPU was at about 60% and memory at about 40%.
py-spy showed the Python node agent's GIL about 90% busy in
`GET /v1/sandboxes/{id}/jobs/{job}`. Each such read:

- takes the sandbox's request lock and the warden flock;
- thaws a sandbox paused on a model wait;
- spawns `runsc exec /.ucloud-job-init ctl` and re-pauses.

verifiers-ucloud polls job status once a second per output stream, plus once a
second in `wait()`, which is about 3 reads/s per sandbox.

The data exists only in the guest. `/.ucloud-managed` lives in the Sentry's
rootfs filestore, so the host can only ask the guest. And because nothing can
wait for a change, clients poll.

## Decision

1. **The guest writes managed state where the host can read it.** The
   supervisor's state directory `/.ucloud-managed` becomes a bind mount of
   `<bundle>/rootfs/.ucloud-managed`.
   - The source is the host overlay's own directory: install_managed_init
     already creates it, mode 0700.
   - Guest writes therefore land in the sandbox's host overlay upper.
   - The upper is already sealed at park, shipped by moves, removed on delete,
     and dropped from commits (`commit_policy.HOST_WRITTEN`).
   - No new paths, artifacts or cleanup.
2. **`ucloud-noded` serves job status and logs from those files.**
   - It never enters, thaws or locks the guest, and never involves Python.
   - A paused sandbox is served like a running one, because a frozen guest
     cannot change the files.
3. **Reads can wait for a change.** Status and logs take `wait_ms`, served by
   inotify on the directory. A client follows a process with one held request,
   not a poll loop.
4. **Clients follow, not poll.** SDK 0.4.39 adds `follow()` and makes `wait()`
   long-poll. verifiers-ucloud 0.3.7 runs one follower per process.

Start and signal move to noded as well; see "Start and signal in noded too".

**Unchanged:**
- parked sandboxes, served by the gateway's cache;
- the supervisor's control protocol.

### Qualification (2026-10-11, production runsc `release-20260817.0` with our patches)

The spike, `scripts/spikes/managed_host_mount.py`, runs the real supervisor
with `/.ucloud-managed` bind-mounted from `<bundle>/rootfs/.ucloud-managed`.

| Question | Result |
|---|---|
| Guest log appends and `state.json` renames visible on the host at once | yes; sizes grow every 0.2 s |
| inotify on the host directory | `IN_CREATE`, `IN_MODIFY`, `IN_CLOSE_WRITE` and `IN_MOVED_TO` all fire |
| `runsc pause`: host reads | served; files stable; no events |
| `checkpoint --hibernate`, delete, restore with the mount | job keeps running; `ctl status` works; log lines contiguous across restore |
| restore with the source at another host path (a move) | the new source receives the writes |
| `control.sock` in the mount (host-uds none) | stays a Sentry socket; checkpoint unaffected |
| guest `ln -s /etc/shadow /.ucloud-managed/evil` | a host symlink, so readers must refuse symlinks |

Not covered by the spike, so the canary checks them on a worker:
- kernel 7.0 instead of 5.15;
- the host overlayfs rootfs instead of a plain directory;
- `--application-memory-ram-backing`.

## Contract

### Guest (Go supervisor)

The layout is unchanged: `state.json`, written by atomic rename,
`stdout.log`, `stderr.log` and `control.sock`.

One addition: the first time a stream truncates, the supervisor persists
`state.json` with `stdout_truncated`/`stderr_truncated` set and `sequence`
incremented. Today truncation is only in memory until exit.

The byte counts in `state.json` are final only once the job is terminal. While
it runs, the host uses the files' sizes.

### Node: OCI

For `managed_process` sandboxes, both OCI builders add this mount:

```json
{"destination": "/.ucloud-managed", "type": "bind",
 "source": "<bundle>/rootfs/.ucloud-managed",
 "options": ["rbind", "rw", "nosuid", "nodev", "noexec"]}
```

- The builders are Python `direct_oci` (creates and migration imports) and
  Rust `oci.rs` (creates).
- The source directory exists, root-owned, mode 0700, before `runsc create` or
  `restore`. A migration import regenerates the OCI config, so the source
  follows the new bundle.
- An import adds the mount only when the shipped upper has `.ucloud-managed`.
  The node creates that directory host-side for every sandbox with the mount,
  and before the mount the supervisor's directory lived in the Sentry's
  filestore, never the upper. So a sandbox parked before this release moves as
  legacy, and its restore sees its checkpoint's own mount table.
- A sandbox whose bundle `config.json` lacks this mount is **legacy**: noded
  forwards its job reads to the Python agent, as today.

### Node: noded routes

noded serves these routes for sandboxes it owns that are running, paused
included, and not legacy. Everything else is forwarded to the Python agent,
including parked sandboxes, for which the agent answers as today ("suspended;
served by the gateway").

- `GET /v1/sandboxes/{id}/jobs/{job}[?wait_ms=W&after=C]`
  - The record is the same JSON as today. It adds
    `"cursor": "<sequence>.<stdout_bytes>.<stderr_bytes>"`.
  - Without `wait_ms`, the response is immediate.
  - With `wait_ms` (clamped to 0..25000), it responds when the record's cursor
    differs from `after`, when the record is terminal, or when `W` elapses. On
    timeout it returns the current record, unchanged.
- `GET /v1/sandboxes/{id}/jobs/{job}/logs/{stdout|stderr}?offset=O&limit=L[&wait_ms=W]`
  - The response has today's fields (`stream`, `offset`, `next_offset`,
    base64 `data`, `eof`) plus `"job": <record>`.
  - With `wait_ms`, it responds when the stream has bytes beyond `O`, when the
    job is terminal, or when `W` elapses. A timeout returns empty data.
  - `limit` is capped at 1 MiB, as today.
  - `O` beyond the retained size is a 400, as today.
- **Record derivation:**
  - parse `state.json` strictly (version 1, ≤ 64 KiB);
  - while non-terminal, `stdout_bytes` and `stderr_bytes` are the log files'
    sizes;
  - once terminal, they are the persisted values;
  - `sandbox_id`, `sandbox_generation` and `updated_at` are filled as Python
    does;
  - a `job_id` that does not match gives the same status and body as Python
    today.
- **Safe opens:**
  1. `open(<bundle>/rootfs, O_DIRECTORY|O_NOFOLLOW)`;
  2. `openat(".ucloud-managed", O_DIRECTORY|O_NOFOLLOW)`;
  3. `openat(<file>, O_RDONLY|O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC)`;
  4. fstat: must be `S_ISREG`.
  - A symlink, FIFO or oversize file is a 409 `managed_process_state_invalid`.
- **Waits:**
  - One inotify instance in noded, with one watch per directory that has
    waiters (`IN_MODIFY|IN_CLOSE_WRITE|IN_MOVED_TO|IN_CREATE|IN_DELETE_SELF|IN_IGNORED|IN_UNMOUNT`).
  - Every event for a directory wakes its waiters, which re-read.
  - A watch is removed when its last waiter leaves.
  - If the directory disappears mid-wait (park, delete), the wait returns the
    last record it read, or forwards if it never read one.
- **Terminal observation.** The Python agent records a growth intent when it
  sees a job terminal (`_observe_managed_terminal`). noded preserves this: the
  first time it serves or observes a terminal record per (sandbox, generation,
  job), it calls the agent's `POST /internal/v1/managed/terminal` (see "Start
  and signal in noded too"); the existing growth-events route only knows wait
  and activate.
- **Not activity.** Reads do not mark sandbox activity. The Python ctl path did
  mark it, which let a status poll hold off idle parking.

#### Exact at park

The gateway serves a parked sandbox's job from its cache, which is only as
fresh as the last read. A job that wrote or exited after that read and then
parked (an idle park while the agent waits on stdin) would leave a follower
waiting for bytes the cache does not know about.

So every park makes the record exact:

- After the checkpoint (the guest is stopped, its files final) and before the
  overlay is unmounted, the agent derives the record from
  `<bundle>/rootfs/.ucloud-managed` exactly as noded's record derivation does,
  through the same `O_NOFOLLOW` opens, and keeps it as
  `<bundle>/.ucloud-managed-process.json`. That file survives an agent
  restart. Recovery that finishes an interrupted park does the same.
- An import derives it again from the shipped upper. It holds the same stopped
  guest's files, so the record is the same.
- While the sandbox is parked, its node record carries it as
  `"managed_process"`: the job status body, `cursor` included. That covers
  `GET /v1/sandboxes`, park responses (`sandbox`), wake failures whose
  lifecycle the node inspected and found still parked (`sandbox`, beside
  `lifecycle_state: "parked"`) and the heartbeat inventory
  (`SandboxInventoryEntry.managed_process`). An admission-busy wake
  (`node_restore_busy` raised before the wake ran) does not inspect the
  sandbox, so it carries none.
- A sandbox has no record when it is legacy (no mount), when no job ever
  started, or when the guest broke its own state files. The park itself never
  fails for that.

### Gateway

- **Query strings.** `wait_ms`, `after`, `offset` and `limit` pass through to
  the node.
- **Timeouts.** The node request timeout for a job read is `wait_ms` plus the
  usual operation budget.
- **Held requests.** Job reads with `wait_ms` use the asynchronous response
  owner (`AsyncGatewayResponses`), as exec event polls do, so a held read never
  holds a handler thread. Expect about one held read per running managed
  sandbox, around 1,000 at a time.
- **Cache.** `_record_successful_sandbox_proxy_state` also caches the `job`
  record carried by 2xx log responses, so a parked sandbox's cached status is
  as fresh as the last read.
- **Parked and transitioning routes** (served from the cache):
  - A status read with `wait_ms` returns the cached record at once, with
    `Retry-After: 1`. Nothing changes while parked, so the client backs off;
    no gateway hold is needed.
  - A logs read with `wait_ms` whose `offset` is at or past the cached
    record's byte count for that stream does not wake the sandbox. It returns
    empty data, the cached record and `Retry-After: 1`. A read below the
    cached count still wakes it, as today, since those bytes exist only in the
    parked image.

### SDK (ucloud-sandboxes-sdk 0.4.39)

- `get_job(sandbox_id, job_id, *, wait_seconds=None, after=None)`.
- `read_job_logs(..., wait_seconds=None)`. The chunk gains
  `job: SandboxJobRecord | None`: None from older gateways.
- `SandboxJobRecord.cursor` is the server's value or computed locally.
- `JobHandle.wait()` (sync and async) long-polls with `after=cursor`. Its
  pacing:
  - if the server says `Retry-After`, sleep that long;
  - if an unchanged answer comes back much sooner than asked (an old gateway),
    sleep `poll_seconds`;
  - otherwise loop at once.
- `JobHandle.follow(stream="stdout", offset=0)` (async iterator, and a sync
  generator) yields byte chunks through log long-polls. It ends when the
  carried record is terminal and the offset equals that stream's byte count.
  It raises on truncation, as verifiers does today.
- Old gateways and nodes ignore `wait_ms`, and the pacing above handles them.

### verifiers-ucloud 0.3.7

`ManagedInteractiveProcess` runs one follower task per process:
- It follows stdout with log long-polls.
- From each carried record, it reads stderr (without waiting) whenever
  `stderr_bytes` passed its offset.
- It feeds both streams.
- Its terminal record answers `wait()`.

That is one held request per process, and no status polling. Transition
handling ("lifecycle transition is in progress" → retry after 2 s) is kept.

## Rollout

- **Order:** gateway and node in one release (0.9.72), applied on an idle
  fleet; then SDK 0.4.39 and verifiers-ucloud 0.3.7.
- **Compatibility:** each half works alone.
  - Old clients against 0.9.72 get cheap reads, still polled.
  - New clients against 0.9.71 fall back to paced polling.
- **Canary on one worker:**
  1. The spike's questions on kernel 7.0, with the overlay rootfs.
  2. 200 managed sandboxes, each following a chatty process through local
     waits: no ctl exec per read, and the node agent's GIL not in job reads.
  3. A park, wake and move of a following sandbox.

## Start and signal in noded too

The Python agent keeps no managed-process path of its own for non-legacy
sandboxes. It keeps only the growth ledger
([managed-growth-admission.md](managed-growth-admission.md)), which noded calls
for admission, as creates already do. Porting the ledger is phase 3c of
[rust-node-daemon-plan.md](rust-node-daemon-plan.md) and is not part of this
change.

### noded: `POST /v1/sandboxes/{id}/jobs` (start)

The ownership and legacy rules are the same as for reads. When noded serves a
start:

1. **Validate the body exactly as Python does.** That means
   `ManagedProcessStart.from_dict` in `managed_process.py`, with the same
   limits, the same error messages, and HTTP 400 with the same body.
2. **Build the control payload exactly as `spec.control_payload` does.**
   - uid and gid come from the bundle `config.json` annotations, as
     `_managed_workload_credentials` reads them.
   - The default cwd comes from the annotation
     `dev.ucloud-sandboxes.managed-process.cwd`.
   - `launch_sha256 = sha256(control_request_bytes(payload))`, byte-identical
     to Python's encoding. Add a golden test against Python.
3. **Admit through the agent:** `POST /internal/v1/managed/launch` with
   `{sandbox_id, generation, job_id, launch_sha256}`. This commits the launch
   intent before the ambiguous dispatch.
   - 200 `{"job_id": "<intent job_id or empty>"}`: admitted.
   - 503 `startup_busy`: the client gets today's retryable
     `SandboxStartupBusyError` response.
   - 409 `primary_owned`: today's `ManagedPrimaryOwnedError` response.
4. **Dispatch.**
   - If paused, thaw with prefetch, as thaw-on-exec does, and leave the
     sandbox running.
   - Then run `runsc exec --user=0:0 <cid> /.ucloud-job-init ctl --timeout …`
     with the payload on stdin, under the same fences noded's exec start
     takes.
   - Retry only on `control_not_connected`, within the 15 s deadline,
     exactly like `_managed_control(retry_not_ready=True)`.
   - Mark activity (touch the exec fence `A`), as a successful exec does.
5. **Bind.** If the admission returned an empty `job_id`, call
   `POST /internal/v1/managed/bind` with `{sandbox_id, generation, job_id,
   launch_sha256}`.
6. **Respond** with the record, in the same shape as the reads.

### noded: `POST /v1/sandboxes/{id}/jobs/{job}/signal`

- Validate as Python does: signal 1..64, excluding KILL and STOP.
- Thaw if paused, then send the `ctl` `signal` action, with no retry.
- Mark activity, and respond with the record.

### Python agent: `/internal/v1/managed/{launch,bind,terminal}`

These are on the agent's UDS, enabled like the other internal routes.

- **`launch`:** `_record_growth_intent(action="launch", …)`. If the intent is
  `queued`, call `_admit_managed_growth(startup=True, launch=…)`, with the
  same outcomes as `start_managed_process` today.
- **`bind`:** `_record_growth_intent(action="bind", …)`.
- **`terminal`:** `{sandbox_id, generation, job_id}` →
  `_record_growth_intent(action="terminal", …)`. noded sends this when it
  first serves or observes a terminal record. This replaces the growth-event
  route mentioned under "Terminal observation" above.
- **Fences:** each endpoint checks the registration's generation, and answers
  409 `generation_mismatch` when it differs.

The Python handlers for start, signal, status and logs stay for legacy and
forwarded sandboxes. Delete them once no legacy sandbox can exist, that is,
after one release with an idle-fleet apply.
