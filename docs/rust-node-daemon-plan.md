# A Rust node daemon (design for review)

Status (2026-10-06):
- **Phase 0:** in production (0.9.43).
- **Phase 1 (creates):** built and canaried. 0.9.45 carries the canary's fixes and rehearses it.
- **Phase 2a (execs on running sandboxes):** built, shipping in 0.9.45.
- **Phase 3:** specified. The pause tier and local waits move together as 3a.

Decided: Rust, and the daemon owns the node registry.

## Why now

The 1,024-rollout rehearsals on two 64-vCPU UCloud workers stopped at about
20 creates/s (10 per worker). Ready time: about 600 sandboxes at 30 s, all by
55–70 s. M2 asks for 30 s.

Each fix removed one bottleneck, and a create still held its node slot for
about 3 s:
- registry group commit (0.9.40);
- one-queue veths (0.9.42; sysfs contention gone);
- no double memory charge (0.9.38);
- forecast cache, no modprobe, faster disk sampler (0.9.37).

**64 slots instead of 32 changed nothing.** Every phase stretched in proportion,
and CPU stayed near 50%.

**The remaining serialization point is the node agent's GIL.** Measured with
bpftrace on `PyEval_RestoreThread` in the agent during a warm burst (0.9.42):

| | Agent threads waiting for the GIL (avg) | GIL re-acquisitions/s |
|---|---|---|
| Burst (creates) | 10–15 | 15,000–22,000 |
| After the burst (turns only) | 1.8 | ~19,000 |

- Each re-acquisition waits about 0.7 ms on average.
- A create re-acquires hundreds of times: each SQLite statement, subprocess
  wait, socket call and file read. That is the 2–3 s.
- It also explains why every phase grows as a worker fills. Per-sandbox
  background work competes for the same GIL: sampling, local-wait ticks,
  pause/thaw, exec streams.
- The storage daemon, another Python process, shows negligible GIL wait.

**What the kernel side contributes now:**
- mount-namespace copies: 20–25 ms each at ~2,500 host mounts, two per runsc
  create;
- IPv6 DAD: a little rtnl time;
- journal fsyncs.

None of these is the bound. The plan's C4.4 gate, node-side create p50 ≤ 150 ms
at 32 concurrent, is out of reach while one interpreter lock serializes the
node.

## What moves, and what does not

The node side is about 30k lines of Python:

| Module | Lines |
|---|---|
| `node_agent` (HTTP API) | 2.3k |
| `node_runtime` | 1.5k |
| `direct_service` | 3.2k |
| `direct_warden` | 3.2k |
| `direct_registry` | 2.3k |
| `hibernation` (lifecycle journal) | 2.4k |
| `direct_provisioner` | 1.0k |
| `direct_network` | 1.2k |
| `memory_backing` | 1.0k |
| `pause_tier`, `local_wait`, `resident_memory`, `warm_park` | 1.8k |
| `sandbox_exec` | 0.8k |
| storage daemon | 4.1k |

Much of it encodes invariants learned the hard way: incarnation fences, crash
replay between journal phases, capacity accounting, migration ownership. A
rewrite in one step would re-learn those as production bugs.

**Proposal: strangle the agent.** A Rust daemon (`ucloud-noded`) takes over
the node's HTTP API first and then whole subsystems, each behind the same
contract and the existing test suite's scenarios.

**What stays outside the daemon:**
- **Gateway:** stays Python. It is not the bound.
- **Storage daemon:** a separate process with its own GIL; not hot today.
- **runsc and the managed init:** unchanged.

### State ownership is the key decision

The node registry (`direct-registry.sqlite`) has exactly one owner process,
which takes an exclusive flock and serves reads from its in-memory index. The
warden's per-sandbox lifecycle journals are written under per-sandbox flocks.
Whoever runs the create pipeline must own the registry, or call the owner for
every transition.

**Options:**
1. **Rust owns the registry from the first create slice (recommended).** The
   Rust daemon becomes the registry owner. Python's `DirectSandboxRegistry`
   becomes a thin client over a UDS RPC for writes. Reads come from a snapshot
   the daemon publishes, or through the same RPC. The schema and file stay
   identical, so rollback is "start the Python owner again".
2. **Python keeps the registry; Rust only spawns and does netlink.** This is
   cheaper, but Python still runs the create's business logic and its
   re-acquisitions. The measurement says that is where the time goes, so this
   buys little.
3. **Rewrite the whole node side at once.** Rejected (see above).

## Phases

**Phase 0: the daemon as the front door (1–2 weeks).**
- **Change:**
  - `ucloud-noded` (Rust: tokio and hyper) listens on the node port.
  - The Python agent moves to a UDS.
  - The daemon proxies every route unchanged, holds long-polls and exec event
    streams itself, and sends heartbeats.
- **Effect:**
  - HTTP parsing and connection threads leave the GIL; today each request is a
    Python thread.
  - The proxy adds one UDS hop.
- **Gate:**
  - the full suite passes against the daemon in front;
  - the relay rehearsal is unchanged or better;
  - rollback by a unit swap.

**Phase 1: the create pipeline (3–5 weeks).**
- **Change:** the daemon owns the registry (option 1) and runs create end to end:
  - plan, quota, rootfs and owned as one journal transition (C5.2);
  - netns and veth through netlink (`rtnetlink`): one queue pair, IPv6 off on
    sandbox interfaces, no `ip` processes;
  - storage prepare over the storage daemon's existing UDS protocol;
  - the overlay rootfs mount;
  - `runsc create` and `runsc start` spawned from Rust;
  - the lifecycle journal's `initialize_running`.
- **Python keeps:** delete, park, wake, migration and commit. These call
  registry writes through the daemon.
- **Gate:**
  - node-side create p50 ≤ 150 ms and p99 ≤ 400 ms at 32 concurrent (C4.4);
  - ≥ 40 creates/s per 64-vCPU worker;
  - the crash-replay and fence tests port as Rust tests and still pass in Python.

**Phase 2: exec, files and managed processes (with C5.1).**
- **Change:** the daemon terminates exec, stdin and file transfer, ideally
  talking to the in-guest agent over its UDS. Python leaves the exec data path.
- **Gate:** C5.1's numbers: exec start p50 ≤ 5 ms, ≥ 1,000 starts/s per node.

**Phase 3: waits and the pause tier.**
- **Change:** local model waits, pause and thaw, resident sampling and growth
  admission (C5.3, one admission function) move.
- **Effect:** these are the per-sandbox loops that grow with density.

**Phase 4: the rest, then delete the Python agent.**
- **Change:** park, wake, migration, commit, fork and reconcile move.
- **Effect:** this is where most of the invariants live, so it goes last, on
  the most tested contract.

## Phase 1 boundaries (decided 2026-10-06, from the porting specs)

**Phase 0 is in production** (0.9.43, `sandbox.direct_node_front_door`). It
ran a cold 1,024-rollout rehearsal with no failures it caused. The specs for
phase 1 cover the registry, the create pipeline, the node's external
protocols and the create endpoint's contract.

- **Registry: the daemon owns it.**
  - The daemon takes the `.owner` flock and keeps the in-memory index and
    group commit.
  - The Python agent keeps working as a *foreign* process on the same file:
    - its remaining writes (delete, park, wake, migration, commit) use
      SQLite's cross-process locking, which the registry already supports;
    - its reads come from a cached index revalidated by activity revision and
      `data_version`, so an exec does not rescan the table.
  - Every row the daemon writes must re-encode byte-identically under
    Python's codec: sorted keys, ASCII escapes, Python float repr, Python's
    default-dropping rules. Cross-language tests pin this in both directions.
- **Admission, drain and lifecycle locks: Python stays the single owner.**
  - Today the in-flight create ledger, memory admission, drain readiness,
    heartbeat accounting and the per-sandbox lifecycle lock live in the
    agent's process. Two owners would let creates and wakes spend the same
    memory, or a drain report ready mid-create.
  - The daemon asks the agent over its Unix socket:
    - **admit:** the startup slot, active capacity, transition demand and
      lifecycle lock, held under a token;
    - **finish:** release, mark activity and return the sandbox record and
      epochs, so the response, `activity_epoch` and heartbeats stay exactly
      Python's.
  - These are two small requests per create. All the I/O happens between
    them, in Rust.
- **Network.**
  - The daemon creates direct-egress leases under the same flocks and state
    file.
  - Relay egress policies and DNS-named egress endpoints stay in Python.
  - The Python network pool is not started while the daemon creates, because
    pooled pairs are usable only by the process that made them. The pool
    moves to Rust next.
- **The daemon creates only what it fully supports.** Any other create
  request is forwarded to the agent unchanged, so unsupported options stay
  correct:
  - relay egress;
  - DNS-named egress;
  - an image kind not yet ported;
  - a migration import.
- **Rollout:** a second flag, `sandbox.direct_node_rust_create`, which
  requires the front door. Turning it off returns creates to Python on the
  next worker.
- **Bugs the specs found in today's create path.** The port fixes these
  rather than copying them:
  - a failed `runsc create` leaks its runtime, because the best-effort delete
    is skipped;
  - a runtime without a journal is never `runsc delete`d;
  - one persistently failing pending create aborts the agent at startup;
  - a `rootfs_ready` replay after a reboot does not remount the overlay.

### Phase 1 progress

- **Built:** the registry port (owner, index, group commit, cross-language
  tests both ways); the storage-daemon client; lifecycle journals; runsc and
  sentry identity; direct-egress leases; Python-exact JSON and the spec
  fingerprint; the warden's create and fenced delete; the memory-backing
  allocator; the warm image lease; the OCI config, overlay rootfs and guest
  files; the create pipeline (`node_pipeline.rs`, resumable from every phase);
  the create front with the agent's admit and finish.
- **Configuration.** The daemon does not parse the node's flags. It asks the
  agent for the effective create configuration (`GET
  /internal/v1/creates/config`, with a digest that every admission echoes).
  It forwards every create until that arrives and the node is one it serves.
  A changed digest sends creates back to the agent.
- **Per request.** The daemon creates only direct egress with the shell
  management helper, from specs it reads exactly as Python does (fingerprint
  checked). Anything else goes to the agent byte for byte.
- **What the 0.9.44 canary found:**
  1. The storage daemon's volume record is flat, and the daemon read it as
     nested. Every create failed this check with an ambiguous 503, and the
     gateway cleaned up.
  2. A static musl link resolved `libc::getrandom`, a weak import, to address 0.
     The first overlay prepare crashed the daemon (SIGSEGV; a core dump read
     against the same commit's debug build).
  - Fixes: the raw syscall, and `build_pinned.sh` now refuses a binary in
    which any libc function the source calls is not linked. That check found
    a second call in the exec manager.
- **The fast loop for live bugs.** Hot-swap a pinned binary onto a running
  worker and run a 64-task smoke rollout. The fixed binary: 60/64 (the 4
  failures are images without Python), ready p50 3.7 s and p95 5.4 s,
  `runtime_create` p50 205 ms, `storage_prepare` 291 ms, `network_ensure`
  33 ms, `registry_commit` 8 ms.

## Phase 2a: the agent's half of Rust execs

Behind `sandbox.direct_node_rust_exec` (requires the front door): the agent
runs with `--rust-execs`, noded with `--rust-exec`. No per-exec request
reaches the agent; the kernel carries the fence (`ucloud_sandboxes/exec_fence.py`).

- **Files:** per sandbox id, `<runtime_root>/warden-locks/.<id>.transition` (T)
  and `.<id>.activity` (A), opened `O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW`, 0600.
  Every locker checks after locking that the path still names its inode.
- **Lock order T, then A, everywhere.**
  - noded's exec: T `LOCK_SH|LOCK_NB` (busy: forward), A `LOCK_SH|LOCK_NB`
    (busy: forward), release T, keep A until the session is reaped; the running
    and pause checks come after A.
  - park, pause, local-wait pause, relay and warm parks, escalation: T
    `LOCK_EX` (blocking), then A `LOCK_EX|LOCK_NB`; busy is today's
    `SandboxBusyError`. Delete and wake: T `LOCK_EX` only.
  - Delete unlinks A, then T, while holding T.
- **Activity clock:** A's mtime. noded touches it at exec start and
  completion; the agent's activity marks touch it too. Idle park takes the
  smaller of the agent's monotonic idle time and the time since A's mtime;
  resident reclaim's currency check sees a touch.
- **Configuration:** `GET /internal/v1/creates/config` carries an `exec` object
  under the same `config_sha256`.
- **Drain and heartbeats:** noded's execs are not in `active_operations`.
  Drain readiness stays correct because it requires no records at all, and
  noded re-reads the drain row before each start.

### What 0.9.45 measured (two 64-vCPU workers, 1,024 relay rollouts over 128 images)

- **Warm, against 0.9.43 (Python creates):**
  - ready by 30 s / 45 s / 60 s: 627 / 933 / 1,024 (0.9.43: 606 / 838 / 994);
  - ready p95 47.6 s (52.6 s);
  - node-side phases (p50): `registry_commit` 173 ms (635), `runtime_create`
    392 ms (676), `storage_prepare` 522 ms (694), `network_ensure` 112 ms (155).
- **Creates queue for slots:** `startup_admission` p50 1.5 s, p95 7.9 s, against
  roughly 1.3 s of work per create in 32 slots.
- **64 slots per node:** faster at first (517 ready by 20 s against 423), then
  a stall (665 by 45 s) and a worse tail.
  - `storage_prepare` p95 reaches 3.5 s: the storage daemon (Python) is the
    next bound on creates.
  - The fleet stays at 32.
- **Relay overhead regressed:** answer→resume p95 1.4 s warm and 3.3 s cold,
  against 0.4 s. Local-wait thaws are still Python's, sharing its GIL with the
  admit and finish of every create (each finish refreshes the foreign index
  with a full table scan).
  - The structural fixes: 3a moves local waits to the daemon, and 3c moves
    admission.
- **In relay rollouts every exec meets a paused sandbox,** so phase 2a forwards
  them all until thaw-on-exec is the daemon's (3a).

### Phase 2a: the daemon's half

- **`src/exec/`:** Python's session semantics, ported and checked against Python
  goldens (sequencing, acks, backpressure, UTF-8 decoding, eviction).
- **`src/exec_front.rs`:** the per-request decision.
  - **Starts the exec itself when:**
    - the sandbox is owned;
    - the drain row is open;
    - the fence is free;
    - the journal says RUNNING with live authority, and its sentry is the
      process it names;
    - there is no pause marker;
    - the memory floor holds.
  - **The warden flock** spans the spawn, and the marker is re-checked under it.
  - **Everything else goes to the agent.**
- `--rust-exec` needs `--rust-create`, which owns the node state.

### Phase 3: the pause tier and local waits move together

The phase-3 spec showed that local model waits pause and thaw through the
pause tier, and they cause almost every production pause, because relay agents
are managed sandboxes.

**Order, without a split-brain pause tier:**
1. **3a:** the whole pause tier, thaw-on-exec (the planned 2b is folded in),
   idle pause, reclaim, the escalation decision and local waits. These are
   separate PRs under one flag.
2. **3b:** resident sampling.
3. **3c:** one admission function (C5.3), with growth.

**Cross-process mechanisms**, kernel-native, as in phase 2a:
- a flock on the pause marker means "thaw in progress";
- markers stay the truth;
- small atomic status files feed the heartbeat;
- Rust calls Python only for rare commands, such as escalating to a park.

## Engineering notes

- **Toolchain:** pin a Rust release and vendor crates. Build reproducibly into
  the sandbox bundle, as `chunk-serve` (Go) is today. A second native toolchain
  next to Go is the cost; the gain is no GC pauses on the data path, mature
  netlink and io_uring crates, and the sharing of state across threads that
  this daemon needs.
- **Same files, same formats.** The registry schema, the lifecycle journals,
  the network slot file and the storage daemon protocol are unchanged until
  Python no longer reads them. Every phase can roll back by swapping units.
- **Tests:** the Python suite's node scenarios run against the daemon through
  the node API. Unit and crash-replay tests are rewritten in Rust per subsystem
  as it moves.
- **Kernel follow-ups independent of the language:**
  - IPv6 off on sandbox veths;
  - keep sandbox mounts out of the namespace runsc copies (or private), so a
    create's mount-namespace copy stays O(base mounts).

## Decisions requested

1. Rust (this plan) or Go for the daemon.
2. Option 1 for state ownership: the daemon owns the node registry from phase 1.
3. Phase 0 starts now, while the Python fixes stay in production.
