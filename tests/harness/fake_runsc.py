"""Unprivileged stand-in for the patched ``runsc`` binary the Warden drives.

Stdlib only: each node's wrapper runs this under ``python -S``. It accepts
exactly the subcommands and flags ``DirectRunscWarden`` issues and refuses
anything else, so a Warden invocation change fails here instead of silently
passing.

Model (see tests/harness/README.md for what is not modeled):
- A container's sentry is a real, idle host process started as
  ``runsc-sandbox ... --root=R --bundle=B boot <cid>``. Its real /proc stat,
  cmdline and exe satisfy the Warden's provenance checks; only the cgroup
  line is faked, in the harness proc root. pause/resume are SIGSTOP/SIGCONT.
- State lives where real runsc keeps it, ``<root>/<cid>_sandbox:<cid>.state``,
  under the same flock the Warden takes before editing PIDs. Real readers use
  ``id``, ``sandbox.pid`` and ``goferPid``; the fake's own fields sit in
  ``fake`` and survive the Warden's rewrite.
- Guest paths: OCI tmpfs mounts (/tmp, /run, /dev/shm) map to runsc-owned
  "memory" directories; everything else outside host tool directories maps to
  the bundle rootfs. ``exec`` translates whole absolute argv elements, cwd,
  HOME and TMPDIR, then runs the command on the host as the test user. Like
  runsc exec it forwards signals and reports a signalled command as
  128 + signal.
- ``delete`` signals only the recorded ``sandbox.pid``, as runsc does, so a
  sentry the Warden's fence fails to reap survives it.
- ``checkpoint --hibernate`` archives the memory directories as the image's
  application memory file and drops the active one, leaving the sentry
  paused. ``restore`` consumes that file again (a rename back to the active
  file), so tmpfs content survives only through the checkpoint image.
- ``tar rootfs-upper`` writes the rootfs's difference from its mounted lower
  (fake_mount's diff), whiteouts as 0:0 character devices, as runsc exports
  the Sentry overlay upper. The fake rootfs is one merged directory, so the
  node's host-side files (init, ledger) appear in it too; runsc keeps those
  below the upper. tmpfs mounts are never part of it.
"""

from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import posixpath
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

CHECKPOINT_FORMAT = "fake-runsc-checkpoint-v1"
PAGES_FORMAT = "fake-runsc-pages-v1"
ACTIVE_MEMORY = "application_memory.active"
MEMORY_IMAGE = "application_memory.img"
KERNEL_IMAGE = "checkpoint.img"
PAGES_IMAGE = "pages_meta.img"
MEMORY_ANNOTATION = "dev.gvisor.internal.application-memory-directory"
# Guest paths below these stay host paths, so host binaries remain usable.
HOST_DIRECTORIES = frozenset(
    {"bin", "sbin", "usr", "lib", "lib32", "lib64", "libx32", "proc", "sys", "dev"}
)
SENTRY_CODE = "import signal\nwhile True:\n    signal.pause()\n"
# What runsc exec forwards: everything a handler can catch, except child
# notifications and synchronous faults of the runsc process itself.
_FORWARDED_SIGNALS = frozenset(signal.valid_signals()) - {
    signal.SIGKILL, signal.SIGSTOP, signal.SIGCHLD, signal.SIGSEGV,
    signal.SIGBUS, signal.SIGFPE, signal.SIGILL, signal.SIGTRAP, signal.SIGSYS,
}
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_GLOBAL_FLAGS = {
    "--root": None,
    "--platform": {"systrap"},
    "--network": {"none"},
    "--application-memory-file-dir": None,
    "--allow-connected-on-save": {"true"},
}


class RunscError(Exception):
    def __init__(self, message: str, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


def main(argv: list[str], config_path: str) -> int:
    try:
        config = json.loads(Path(config_path).read_text(encoding="utf-8"))
        # The node's fake mount wrappers keep their config next to this one.
        config.setdefault("mount_config", str(Path(config_path).with_name("fake-mount.json")))
        flags, command, rest = _split_global(argv[1:])
        root = Path(flags.get("--root", ""))
        if not root.is_absolute() or not root.is_dir():
            raise RunscError("--root must name an existing absolute directory")
        handler = _COMMANDS.get(command)
        if handler is None:
            raise RunscError(f"unsupported command: {command}", 2)
        import faults  # Beside this file on the wrapper's path.

        action = faults.fire(config.get("faults", ""), command, rest)
        if action == "fail":
            raise RunscError(f"injected {command} failure")
        if action == "hang":
            faults.block(config["faults"], command)
        code = handler(_Runtime(config, root, flags), rest)
        if action == "hang-after":
            faults.block(config["faults"], command)
        return code
    except RunscError as exc:
        print(f"fake-runsc: {exc}", file=sys.stderr)
        return exc.code


def _split_global(args: list[str]) -> tuple[dict[str, str], str, list[str]]:
    flags: dict[str, str] = {}
    for index, item in enumerate(args):
        if not item.startswith("--"):
            return flags, item, args[index + 1 :]
        name, separator, value = item.partition("=")
        if not separator or name not in _GLOBAL_FLAGS or name in flags:
            raise RunscError(f"unsupported global flag: {item}", 2)
        allowed = _GLOBAL_FLAGS[name]
        if allowed is not None and value not in allowed:
            raise RunscError(f"unsupported {name} value: {value}", 2)
        flags[name] = value
    raise RunscError("missing command", 2)


def _parse(
    args: list[str],
    *,
    booleans: frozenset[str] = frozenset(),
    values: frozenset[str] = frozenset(),
    repeated: frozenset[str] = frozenset(),
    positional: int | None = 1,
) -> tuple[dict[str, object], list[str]]:
    """Strict flag parser; flags stop at the first positional argument."""
    parsed: dict[str, object] = {}
    index = 0
    while index < len(args) and args[index].startswith("--"):
        name, separator, value = args[index].partition("=")
        if name in booleans and not separator and name not in parsed:
            parsed[name] = True
        elif name in values and separator and name not in parsed:
            parsed[name] = value
        elif name in repeated and separator:
            parsed.setdefault(name, []).append(value)  # type: ignore[union-attr]
        else:
            raise RunscError(f"unsupported flag: {args[index]}", 2)
        index += 1
    rest = args[index:]
    if positional is not None and len(rest) != positional:
        raise RunscError("unexpected positional arguments", 2)
    if not rest or not _CONTAINER_ID.fullmatch(rest[0]):
        raise RunscError("container id must be a full lowercase SHA-256", 2)
    return parsed, rest


def _process_ticks(pid: int) -> int | None:
    """Start ticks of a live, non-zombie process, else None."""
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    fields = raw[raw.rfind(")") + 2 :].split()
    if not fields or fields[0] in {"Z", "X"}:
        return None
    return int(fields[19])


def _owns_group(pid: int, ticks: int) -> bool:
    """Whether process group ``pid`` can still only be the recorded exec's.

    The kernel keeps a group ID reserved while any member runs, so when no
    process holds the PID a surviving group is that exec's orphaned command.
    A holder (even a zombie) with other start ticks means the group ended
    and the PID was reused.
    """
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return True
    return int(raw[raw.rfind(")") + 2 :].split()[19]) == ticks


def exec_group_alive(pid: int, ticks: int) -> bool:
    if pid <= 1 or not _owns_group(pid, ticks):
        return False
    try:
        os.killpg(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def kill_exec_groups(execs) -> None:
    """SIGKILL recorded exec groups, including commands orphaned by a killed
    ``runsc exec`` parent: guest processes die with their sandbox."""
    for pid, ticks in execs:
        if pid > 1 and _owns_group(pid, ticks):
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


class _Runtime:
    def __init__(self, config: dict, root: Path, flags: dict[str, str]) -> None:
        self.config = config
        self.root = root
        self.flags = flags
        self.proc_root = Path(config["proc_root"])

    # -- state ---------------------------------------------------------------

    def state_path(self, cid: str) -> Path:
        return self.root / f"{cid}_sandbox:{cid}.state"

    def lock(self, cid: str) -> int:
        """Hold runsc's per-container metadata lock (shared with the Warden)."""
        descriptor = os.open(
            self.root / f"{cid}_sandbox:{cid}.lock",
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
        )
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return descriptor

    def load(self, cid: str) -> dict | None:
        try:
            state = json.loads(self.state_path(cid).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        if state.get("id") != cid or not isinstance(state.get("fake"), dict):
            raise RunscError("container state belongs to another owner")
        return state

    def save(self, cid: str, state: dict) -> None:
        descriptor, temporary = tempfile.mkstemp(prefix=".fake-runsc-", dir=self.root)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                os.fchmod(stream.fileno(), 0o600)
                json.dump(state, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.state_path(cid))
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def status(self, state: dict) -> str:
        fake = state["fake"]
        pid = int(state.get("sandbox", {}).get("pid") or 0)
        if pid <= 0 or _process_ticks(pid) != fake["sentryTicks"]:
            return "stopped"
        return fake["status"]

    def require_live(self, cid: str, state: dict | None, *statuses: str) -> dict:
        if state is None:
            raise RunscError(f"container {cid} does not exist")
        status = self.status(state)
        if status not in statuses:
            raise RunscError(f"container {cid} is {status}, expected {'/'.join(statuses)}")
        return state

    # -- bundle and guest paths ----------------------------------------------

    @staticmethod
    def bundle_config(bundle: Path) -> dict:
        try:
            config = json.loads((bundle / "config.json").read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RunscError(f"bundle config is unreadable: {exc}") from exc
        if config.get("root", {}).get("path") != "rootfs" or not (bundle / "rootfs").is_dir():
            raise RunscError("bundle root must be its rootfs directory")
        return config

    def memory_root(self, cid: str) -> Path:
        return self.root / "fake-memory" / cid

    def tmpfs_layout(self, cid: str, config: dict) -> dict[str, str]:
        """OCI tmpfs destinations, each backed by a runsc-owned directory."""
        layout = {}
        for index, mount in enumerate(config.get("mounts") or ()):
            destination = mount.get("destination", "")
            if mount.get("type") == "tmpfs" and destination != "/dev":
                layout[destination] = str(self.memory_root(cid) / f"m{index}")
        return layout

    def application_memory(self, config: dict) -> Path:
        directory = self.flags.get("--application-memory-file-dir", "")
        name = (config.get("annotations") or {}).get(MEMORY_ANNOTATION, "")
        if not directory or not name or "/" in name:
            raise RunscError("application memory placement is not configured")
        active = Path(directory) / name
        if not active.is_dir():
            raise RunscError(f"application memory directory is missing: {active}")
        return active / ACTIVE_MEMORY

    @staticmethod
    def translate(state: dict, path: str) -> str:
        fake = state["fake"]
        normalized = posixpath.normpath(path)
        if normalized.startswith("//"):
            normalized = "/" + normalized.lstrip("/")
        for destination in sorted(fake["tmpfs"], key=len, reverse=True):
            if normalized == destination or normalized.startswith(destination + "/"):
                return fake["tmpfs"][destination] + normalized[len(destination) :]
        if normalized == "/":
            return fake["rootfs"]
        if normalized.split("/")[1] in HOST_DIRECTORIES:
            return normalized
        return fake["rootfs"] + normalized

    # -- processes -----------------------------------------------------------

    def spawn_sentry(self, cid: str, bundle: Path, *, paused: bool) -> tuple[int, int]:
        logs = self.root / "fake-logs"
        logs.mkdir(mode=0o700, exist_ok=True)
        with open(logs / f"{cid}.log", "ab") as log:
            process = subprocess.Popen(
                [
                    "runsc-sandbox", "-S", "-s", "-c", SENTRY_CODE,
                    f"--root={self.root}", f"--bundle={bundle}", "boot", cid,
                ],
                executable=self.config["python"],
                env={"PYTHONHOME": self.config["python_home"]},
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                cwd="/",
                close_fds=True,
                start_new_session=True,
            )
        if paused:
            os.kill(process.pid, signal.SIGSTOP)
        ticks = _process_ticks(process.pid)
        if ticks is None:
            raise RunscError("sentry exited during startup")
        # The sentry outlives this client, like runsc's daemonized sandbox.
        process.returncode = 0
        # Harness-only ledger: the fleet reaps a sentry the Warden leaked.
        with open(self.root / "fake-sentries", "a", encoding="ascii") as ledger:
            ledger.write(f"{process.pid} {ticks}\n")
        return process.pid, ticks

    def publish_proc(self, cid: str, pid: int) -> None:
        """Expose real stat/cmdline/exe plus the incarnation cgroup line."""
        entry = self.proc_root / str(pid)
        staging = Path(tempfile.mkdtemp(prefix=f".{pid}.", dir=self.proc_root))
        for name in ("stat", "cmdline", "exe"):
            os.symlink(f"/proc/{pid}/{name}", staging / name)
        (staging / "cgroup").write_text(f"0::/ucloud-sandboxes/{cid}\n", encoding="ascii")
        if os.path.lexists(entry):
            # A recycled PID: the previous owner's entry is stale by definition.
            shutil.rmtree(entry)
        os.rename(staging, entry)

    def unpublish_proc(self, cid: str, pid: int) -> None:
        entry = self.proc_root / str(pid)
        try:
            owner = (entry / "cgroup").read_text(encoding="ascii")
        except OSError:
            return
        if owner.strip().endswith("/" + cid):
            shutil.rmtree(entry, ignore_errors=True)

    @staticmethod
    def kill_execs(state: dict) -> None:
        """Exec'd host processes cannot be checkpointed; they die with the sentry."""
        kill_exec_groups(state["fake"].get("execs", ()))
        state["fake"]["execs"] = []

    def kill_sentry(self, state: dict) -> None:
        """Signal the recorded ``sandbox.pid`` only, as runsc does.

        The Warden zeroes it after reaping the sentry through its own pidfd,
        so a sentry that fence leaves behind survives here and stays visible.
        """
        fake = state["fake"]
        pid = int(state.get("sandbox", {}).get("pid") or 0)
        if pid > 1 and pid == fake["sentryPid"] and _process_ticks(pid) == fake["sentryTicks"]:
            os.kill(pid, signal.SIGKILL)
            deadline = time.monotonic() + 5
            while _process_ticks(pid) == fake["sentryTicks"] and time.monotonic() < deadline:
                time.sleep(0.005)


def _new_state(cid: str, bundle: Path, pid: int, ticks: int, layout: dict, status: str) -> dict:
    return {
        "id": cid,
        "goferPid": 0,
        "sandbox": {"id": cid, "pid": pid},
        "fake": {
            "schema": 1,
            "status": status,
            "bundle": str(bundle),
            "rootfs": str(bundle / "rootfs"),
            "sentryPid": pid,
            "sentryTicks": ticks,
            "tmpfs": layout,
            "execs": [],
            "created": time.time(),
        },
    }


def _create(runtime: _Runtime, args: list[str]) -> int:
    flags, (cid,) = _parse(args, values=frozenset({"--bundle"}))
    bundle = Path(str(flags.get("--bundle", "")))
    config = runtime.bundle_config(bundle)
    active = runtime.application_memory(config)
    descriptor = runtime.lock(cid)
    try:
        if runtime.load(cid) is not None:
            raise RunscError(f"container with id {cid} already exists")
        layout = runtime.tmpfs_layout(cid, config)
        shutil.rmtree(runtime.memory_root(cid), ignore_errors=True)
        for directory in layout.values():
            os.makedirs(directory, mode=0o700)
        _write_new(active, b"")
        pid, ticks = runtime.spawn_sentry(cid, bundle, paused=False)
        runtime.save(cid, _new_state(cid, bundle, pid, ticks, layout, "created"))
        runtime.publish_proc(cid, pid)
    finally:
        os.close(descriptor)
    return 0


def _start(runtime: _Runtime, args: list[str]) -> int:
    _, (cid,) = _parse(args)
    descriptor = runtime.lock(cid)
    try:
        state = runtime.require_live(cid, runtime.load(cid), "created")
        state["fake"]["status"] = "running"
        runtime.save(cid, state)
    finally:
        os.close(descriptor)
    return 0


def _state(runtime: _Runtime, args: list[str]) -> int:
    _, (cid,) = _parse(args)
    state = runtime.load(cid)
    if state is None:
        raise RunscError(f"container {cid} does not exist")
    fake = state["fake"]
    print(json.dumps({
        "ociVersion": "1.0.2",
        "id": cid,
        "status": runtime.status(state),
        "pid": int(state.get("sandbox", {}).get("pid") or 0),
        "bundle": fake["bundle"],
        "rootfs": fake["rootfs"],
        "owner": "",
    }))
    return 0


def _list(runtime: _Runtime, args: list[str]) -> int:
    if args != ["--format=json"]:
        raise RunscError("list requires exactly --format=json", 2)
    inventory = []
    for path in sorted(runtime.root.glob("*_sandbox:*.state")):
        cid = path.name.split("_", 1)[0]
        state = runtime.load(cid)
        if state is not None:
            inventory.append({"id": cid, "status": runtime.status(state)})
    # runsc marshals an empty container slice as JSON null.
    print(json.dumps(inventory or None))
    return 0


def _exec(runtime: _Runtime, args: list[str]) -> int:
    flags, rest = _parse(
        args,
        values=frozenset({"--cwd", "--user"}),
        repeated=frozenset({"--env"}),
        positional=None,
    )
    cid, guest_argv = rest[0], rest[1:]
    if not guest_argv:
        raise RunscError("exec requires a command", 2)
    # --user is accepted but not modeled: commands run as the test user.
    try:
        os.setpgid(0, 0)  # The delete/checkpoint fence kills this group.
    except PermissionError:
        pass  # Already a session leader (start_new_session callers).
    descriptor = runtime.lock(cid)
    try:
        state = runtime.require_live(cid, runtime.load(cid), "running")
        ticks = _process_ticks(os.getpid())
        execs = [item for item in state["fake"]["execs"] if exec_group_alive(*item)]
        state["fake"]["execs"] = [*execs, [os.getpid(), ticks]]
        runtime.save(cid, state)
    finally:
        os.close(descriptor)
    config = runtime.bundle_config(Path(state["fake"]["bundle"]))
    environment = dict(
        item.partition("=")[::2] for item in config.get("process", {}).get("env") or ()
    )
    for item in flags.get("--env", ()):  # type: ignore[union-attr]
        key, _, value = str(item).partition("=")
        environment[key] = value
    if environment.get("HOME", "").startswith("/"):
        environment["HOME"] = runtime.translate(state, environment["HOME"])
    if "/tmp" in state["fake"]["tmpfs"]:
        environment["TMPDIR"] = state["fake"]["tmpfs"]["/tmp"]
    cwd = runtime.translate(
        state, str(flags.get("--cwd") or config.get("process", {}).get("cwd") or "/")
    )
    command = [
        runtime.translate(state, item) if item.startswith("/") else item
        for item in guest_argv
    ]
    executable = command[0]
    command[0] = guest_argv[0]
    try:
        os.chdir(cwd)
    except OSError as exc:
        raise RunscError(f"exec working directory is unavailable: {exc}", 126) from exc
    environment["PWD"] = cwd
    # Like runsc exec, stay the parent: forward catchable signals to the guest
    # command and report a signalled one as 128 + signal. Signals stay blocked
    # until the forwarders exist, so none can kill this parent mid-handoff.
    signal.pthread_sigmask(signal.SIG_BLOCK, _FORWARDED_SIGNALS)
    child = os.fork()
    if child == 0:
        # Python ignores these at startup and execve keeps ignored signals.
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, _FORWARDED_SIGNALS)
        code, reason = 127, "not found"
        try:
            if "/" in executable:
                os.execve(executable, command, environment)
            os.execvpe(executable, command, environment)
        except FileNotFoundError:
            pass
        except OSError as exc:
            code, reason = 126, exc.strerror or "cannot execute"
        print(f"fake-runsc: exec: {guest_argv[0]}: {reason}", file=sys.stderr, flush=True)
        os._exit(code)

    def forward(received: int, _frame) -> None:
        try:
            os.kill(child, received)
        except ProcessLookupError:
            pass  # Already reaped below.

    for number in _FORWARDED_SIGNALS:
        signal.signal(number, forward)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, _FORWARDED_SIGNALS)
    _, status = os.waitpid(child, 0)
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return os.WEXITSTATUS(status)


def _checkpoint(runtime: _Runtime, args: list[str]) -> int:
    flags, (cid,) = _parse(
        args, booleans=frozenset({"--hibernate"}), values=frozenset({"--image-path"})
    )
    if flags.get("--hibernate") is not True:
        raise RunscError("only two-phase --hibernate capture is modeled", 2)
    image = Path(str(flags.get("--image-path", "")))
    if not image.is_dir():
        raise RunscError("checkpoint image path must be an existing directory")
    descriptor = runtime.lock(cid)
    try:
        state = runtime.require_live(cid, runtime.load(cid), "running")
        fake = state["fake"]
        # Quiesce first: the original stays paused until the Warden reaps it.
        os.kill(fake["sentryPid"], signal.SIGSTOP)
        fake["status"] = "paused"
        runtime.kill_execs(state)
        runtime.save(cid, state)
        config = runtime.bundle_config(Path(fake["bundle"]))
        active = runtime.application_memory(config)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for destination, directory in sorted(fake["tmpfs"].items()):
                tar.add(directory, arcname=destination.strip("/"))
        payload = archive.getvalue()
        _write_new(image / MEMORY_IMAGE, payload)
        active.unlink(missing_ok=True)
        _write_new(image / KERNEL_IMAGE, json.dumps({
            "format": CHECKPOINT_FORMAT,
            "container_id": cid,
            "tmpfs": sorted(fake["tmpfs"]),
            "application_memory_sha256": hashlib.sha256(payload).hexdigest(),
        }, sort_keys=True).encode("ascii"))
        _write_new(image / PAGES_IMAGE, json.dumps(
            {"format": PAGES_FORMAT, "container_id": cid}
        ).encode("ascii"))
    finally:
        os.close(descriptor)
    return 0


def _restore(runtime: _Runtime, args: list[str]) -> int:
    flags, (cid,) = _parse(
        args,
        booleans=frozenset({"--detach", "--background", "--start-paused"}),
        values=frozenset({"--image-path", "--bundle"}),
    )
    for required in ("--detach", "--background", "--start-paused"):
        if flags.get(required) is not True:
            raise RunscError(f"restore without {required} is not modeled", 2)
    image = Path(str(flags.get("--image-path", "")))
    bundle = Path(str(flags.get("--bundle", "")))
    config = runtime.bundle_config(bundle)
    active = runtime.application_memory(config)
    try:
        kernel = json.loads((image / KERNEL_IMAGE).read_text(encoding="ascii"))
        payload = (image / MEMORY_IMAGE).read_bytes()
    except (OSError, ValueError) as exc:
        raise RunscError(f"checkpoint image is unreadable: {exc}") from exc
    if kernel.get("format") != CHECKPOINT_FORMAT or kernel.get("container_id") != cid:
        raise RunscError("checkpoint image belongs to another container")
    if kernel.get("application_memory_sha256") != hashlib.sha256(payload).hexdigest():
        raise RunscError("application memory image is corrupt")
    descriptor = runtime.lock(cid)
    try:
        existing = runtime.load(cid)
        if existing is not None:
            raise RunscError(f"container with id {cid} already exists")
        layout = runtime.tmpfs_layout(cid, config)
        if sorted(layout) != kernel.get("tmpfs"):
            raise RunscError("bundle tmpfs mounts differ from the checkpoint")
        shutil.rmtree(runtime.memory_root(cid), ignore_errors=True)
        runtime.memory_root(cid).mkdir(mode=0o700, parents=True)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r") as tar:
            for destination, directory in layout.items():
                members = [
                    member for member in tar.getmembers()
                    if member.name == destination.strip("/")
                    or member.name.startswith(destination.strip("/") + "/")
                ]
                staging = Path(tempfile.mkdtemp(dir=runtime.memory_root(cid)))
                tar.extractall(staging, members=members, filter="fully_trusted")
                os.rename(staging / destination.strip("/"), directory)
                shutil.rmtree(staging)
        # Restore consumes the single-owner main-memory name from the image.
        os.rename(image / MEMORY_IMAGE, active)
        pid, ticks = runtime.spawn_sentry(cid, bundle, paused=True)
        state = _new_state(cid, bundle, pid, ticks, layout, "paused")
        state["fake"]["restoredFrom"] = str(image)
        runtime.save(cid, state)
        runtime.publish_proc(cid, pid)
    finally:
        os.close(descriptor)
    return 0


def _tar(runtime: _Runtime, args: list[str]) -> int:
    if not args or args[0] != "rootfs-upper":
        raise RunscError("only tar rootfs-upper is modeled", 2)
    import fake_mount  # A sibling on the wrapper's path, like this module.

    flags, (cid,) = _parse(args[1:], values=frozenset({"--file"}))
    output = Path(str(flags.get("--file", "")))
    if not output.is_absolute() or not output.parent.is_dir():
        raise RunscError("--file must name a path in an existing directory")
    descriptor = runtime.lock(cid)
    try:
        state = runtime.require_live(cid, runtime.load(cid), "running", "paused")
        merged = state["fake"]["rootfs"]
        table = Path(json.loads(Path(runtime.config["mount_config"]).read_text(encoding="utf-8"))["table"])
        try:
            lower = Path(json.loads(fake_mount._entry(table, merged).read_text(encoding="utf-8"))["lower"])
        except FileNotFoundError:
            raise RunscError("the container rootfs is not an overlay mount") from None
        with tempfile.TemporaryDirectory(dir=output.parent) as staging:
            upper = Path(staging) / "upper"
            upper.mkdir()
            fake_mount._diff(lower, Path(merged), upper)
            shutil.copystat(merged, upper)  # "." is the guest root, as runsc's upper has it
            with tarfile.open(Path(staging) / "upper.tar", "w", format=tarfile.PAX_FORMAT) as archive:
                archive.add(upper, arcname=".", recursive=False)
                for directory, names, files in os.walk(upper):
                    names.sort()
                    for name in sorted(names + files):
                        path = Path(directory) / name
                        if name.startswith(fake_mount.WHITEOUT) and path.is_file() and not path.is_symlink():
                            hidden = path.parent / name[len(fake_mount.WHITEOUT):]
                            whiteout = tarfile.TarInfo("./" + str(hidden.relative_to(upper)))
                            whiteout.type = tarfile.CHRTYPE
                            archive.addfile(whiteout)
                        else:
                            archive.add(path, arcname="./" + str(path.relative_to(upper)), recursive=False)
            os.replace(Path(staging) / "upper.tar", output)
    finally:
        os.close(descriptor)
    return 0


def _signal_status(runtime: _Runtime, args: list[str], *, source: str, target: str, sig: int) -> int:
    _, (cid,) = _parse(args)
    descriptor = runtime.lock(cid)
    try:
        state = runtime.require_live(cid, runtime.load(cid), source)
        os.kill(state["fake"]["sentryPid"], sig)
        state["fake"]["status"] = target
        runtime.save(cid, state)
    finally:
        os.close(descriptor)
    return 0


def _kill(runtime: _Runtime, args: list[str]) -> int:
    if len(args) not in {1, 2}:
        raise RunscError("kill takes a container id and optional signal", 2)
    _, (cid,) = _parse(args[:1])
    sig: int = signal.SIGTERM
    if len(args) == 2:
        raw = args[1]
        try:
            sig = int(raw) if raw.isdigit() else signal.Signals["SIG" + raw.removeprefix("SIG")]
        except KeyError:
            raise RunscError(f"unknown signal: {raw}", 2) from None
    state = runtime.require_live(cid, runtime.load(cid), "running", "paused", "created")
    os.kill(state["fake"]["sentryPid"], sig)
    return 0


def _delete(runtime: _Runtime, args: list[str]) -> int:
    flags, (cid,) = _parse(args, booleans=frozenset({"--force"}))
    descriptor = runtime.lock(cid)
    try:
        state = runtime.load(cid)
        if state is None:
            if flags.get("--force"):
                return 0
            raise RunscError(f"container {cid} does not exist")
        if not flags.get("--force") and runtime.status(state) not in {"stopped", "created"}:
            raise RunscError(f"cannot delete a {runtime.status(state)} container without --force")
        runtime.kill_execs(state)
        runtime.kill_sentry(state)
        runtime.unpublish_proc(cid, int(state["fake"].get("sentryPid") or 0))
        shutil.rmtree(runtime.memory_root(cid), ignore_errors=True)
        runtime.state_path(cid).unlink()
    finally:
        os.close(descriptor)
    return 0


def _write_new(path: Path, payload: bytes) -> None:
    """Replace ``path`` with a new inode.

    Volume files may be hard links into the fake storage's sealed snapshot;
    rewriting one in place would change the snapshot too.
    """
    descriptor, temporary = tempfile.mkstemp(prefix=".fake-runsc-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


_COMMANDS = {
    "create": _create,
    "start": _start,
    "state": _state,
    "list": _list,
    "exec": _exec,
    "checkpoint": _checkpoint,
    "restore": _restore,
    "resume": lambda runtime, args: _signal_status(
        runtime, args, source="paused", target="running", sig=signal.SIGCONT
    ),
    "pause": lambda runtime, args: _signal_status(
        runtime, args, source="running", target="paused", sig=signal.SIGSTOP
    ),
    "kill": _kill,
    "delete": _delete,
    "tar": _tar,
}
