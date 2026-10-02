# Local fleet harness

`LocalFleet` runs a real gateway (`control_plane.build_server`) and real
direct node agents (`build_direct_node_agent_server`) on loopback, with
distinct real tokens. Heartbeats go through each node agent's real sender
thread, on an interval no scenario reaches: `node.start()` returns once the
gateway accepted a heartbeat from the new agent, and `fleet.heartbeat()` makes
every sender send one more and requires the gateway to accept it. Each node
runs the real `DirectSandboxService`, provisioner, registry,
`DirectRunscWarden`, hibernation journal, `OverlayRootfsManager` and
storage-native service over its unix socket. Routing is SQLite, or
PostgreSQL through the production descriptor with `LocalFleet(postgres=True)`
and `UCLOUD_TEST_POSTGRES_DSN`. It needs no root, runsc, Docker, ublk or
network. Fleet roots live on `/dev/shm` when it allows exec, because commit
fsyncs dominate on a shared disk; a scenario then runs in well under 2 s.

Node agents run in the test process by default. `LocalFleet(node_processes=True)`
runs each in its own process, forked from a preloaded zygote
(`node_process.py`), so `node.crash()` can SIGKILL it mid-operation. That mode
keeps the fixed host sample and has neither `node.requests` nor
`node.honor_exec_session_prefix`; touching one raises. Agents die with the
zygote, which exits with the test process.

## Fake boundaries

| Real dependency | Replaced by | Injected at |
| --- | --- | --- |
| patched `runsc` | `fake_runsc.py` behind `bin/runsc` | `DirectRunscWardenConfig.runsc` |
| `/proc` provenance | `proc/<pid>/{stat,cmdline,exe}` symlinks to the real `/proc`, plus a fake `cgroup` line and `boot_id` | `DirectRunscWardenConfig.proc_root` |
| overlayfs `mount`/`umount`/`mountpoint` | `fake_mount.py` behind `bin/` wrappers | `OverlayRootfsManager(mount_binary=…)` |
| ublk/overlaybd devices, mkfs/mount/freeze | `storage.FakeBlockBackend`, `storage.FakeStorageHost` | `StorageNativeNodeService(backend=, host=)` |
| Docker overlay2 image store | `images.ImageCatalog` + `images.LocalRootfsStore` | `OverlayRootfsManager(image_store)` |
| `docker pull` | `DockerImageRuntime(dry_run=True)` | `build_direct_node_agent_server(image_runtime=)` |
| host CPU/memory sampling | `node.sample_metrics()`, a fixed sample unless the test sets it; read uncached | `runtime_metrics_provider=` |
| `os.pidfd_open` (missing in uv's Python 3.10) | `pidfd.SyscallPidfdFencer`, the same syscalls through libc | `DirectRunscWarden(fencer=)`, only when missing |

**The fake `runsc`.** A sentry is an idle host process whose real cmdline
(`runsc-sandbox … --root=R --bundle=B boot <cid>`) and exe (the interpreter,
named by `bin/gvisor-bin/gvisor_sentry`) pass the Warden's provenance checks.
Its state file and lock are where runsc keeps them, so the Warden's PID
fencing before `delete` runs unchanged, and `delete` signals only the
recorded `sandbox.pid`, as runsc does: a sentry the fence fails to reap
survives (the fleet still kills it at close). Pause and resume are SIGSTOP and
SIGCONT. `exec` stays the parent of the guest command, forwards signals to it
and reports a signalled command as 128 + signal, like `runsc exec`. Guest paths map to the bundle rootfs, except OCI tmpfs mounts
(`/tmp`, `/run`, `/dev/shm`), which map to runsc-owned memory directories.
`checkpoint --hibernate` archives those into `application_memory.img` and
leaves the sentry paused. `restore` consumes that image by renaming it back.
tmpfs content therefore survives only through the checkpoint, and disk
content only through the overlay and storage seal. `tar rootfs-upper` writes
the rootfs's difference from its lower, deletions as 0:0 character devices
(C3.1 commit); it includes the node's host-written files, which runsc keeps
below the Sentry's upper.

**Storage.** The journal, fencing, protocol and client are production code.
A device is a directory whose entries are renamed into the mountpoint on mount
and back out on unmount. A seal keeps a hard-link snapshot, so inodes survive
release and remount as they do on XFS, which checkpoint manifests rely on.
Unsealed changes are lost when a device is released, which models COW discard.

**Faults.** `node.arm_fault(command, action, sandbox_id=)` arms a one-shot
fault on a fake `runsc` or `mount`/`umount` command (`faults.py`): `fail`
before any effect, `hang` before it, or `hang-after` it. `node.wait_hung`
returns the blocked PID; `node.release` lets it continue as if only slow, and
`node.kill_hung` makes it die unfinished. `node.storage_host.hold(operation)`
pauses the in-process storage daemon the same way; it outlives a crashed
agent, as the root daemon does. A crash point is therefore a fake boundary:
there is none between the Warden's journal commit and the registry's.

**Silence, reboot, drain.** `fleet.expire_heartbeat(node)` ages the gateway's
last receipt past the TTL, as silence would. `node.reboot()` kills guest
processes and changes the boot ID before restarting the agent.
`node.drain(token)` and `node.request(...)` call the node's control API
directly, as the autoscaler or gateway does. `node.honor_exec_session_prefix
= False` models a worker that predates signed exec session names.
`fleet.continuity_cycle(node, interrupted_at=...)` runs the autoscaler's
per-cycle guest-continuity step against a fake provider job and the real
direct probe; the rest of an autoscaler cycle is not modeled yet.

## Not modeled

- Guest isolation. Commands run on the host as the test user, ignoring
  `--user`, capabilities and cgroup limits. Only whole absolute argv elements,
  the cwd, `HOME` and `TMPDIR` are translated. Paths inside a `sh -c` script
  are host paths, so use relative paths from `working_dir`.
- The OCI entrypoint (it is never run), a gofer process, the gVisor rootfs
  filestore, TTY exec and networking (`network=none` only).
- Checkpointing host processes. Exec'd processes are killed at checkpoint,
  and exec into a paused container fails at once. `pause` and `kill` exist for
  fault injection; the Warden itself never issues them.
- Split memory backing, RAM or reflink restore, published or compacted storage
  layers, snapshot publication, migration and checkpoint registries.
- Host reboots beyond guest processes and the boot ID. Mount tables, storage
  devices and checkpoint memory images survive `node.reboot()`.
- Resident memory sampling (no sandbox cgroup) and disk-quota enforcement.

## Adding a scenario

```python
from tests.harness import LocalFleet

class MyScenario(unittest.TestCase):
    def test_something(self):
        with LocalFleet(nodes=1) as fleet:
            fleet.create("s1", parkable=True)        # POST /v1/sandboxes
            result = fleet.exec("s1", ["cat", "/opt/greeting"])
            self.assertEqual(result.stdout, "hello from the image\n")
            node = fleet.node_for("s1")              # node-side state and faults
            node.kill_sentry("s1")                   # or node.restart()
            fleet.heartbeat()                        # every node sends one now
```

Drive everything through public gateway routes (`fleet.request` for anything
the helpers lack). Assert on observable state: HTTP responses, the routing
store (`fleet.route`), heartbeat inventory (`node.post_heartbeat()`), the
node registry (`node.registration`), the runtime state file
(`node.runsc_state`), surviving sentries (`node.live_sentries()`), storage
devices (`node.storage_backend.devices`), requests a node received
(`node.requests`), routing-store calls the gateway made
(`with fleet.routing_calls() as calls`), and files under the node root. Add
images with `fleet.catalog.add(ref, {path: bytes})`. Teach a fake a new runsc
flag only together with its semantics. The fakes reject any invocation they
do not model.
