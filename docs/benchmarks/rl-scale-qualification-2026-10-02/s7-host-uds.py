#!/usr/bin/env python3
"""S7 follow-up for C5.1: Unix sockets across a bind-mounted host directory (qualification only).

For each --host-uds value, one sandbox (network none, host overlay rootfs,
default --overlay2=root:self) gets a host directory bind-mounted at /agent.
  guest->host: the guest binds and listens on /agent/guest.sock; a host
               process connects to <dir>/guest.sock and round-trips bytes.
  host->guest: a host process listens on <dir>/host.sock; the guest connects
               to /agent/host.sock and round-trips bytes.
It also records what the host sees at the guest's socket path, a guest
listener in the overlay rootfs, and whether a sandbox holding a host-visible
listener can checkpoint, restore and accept again.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import threading
import time
import uuid

SERVER = r"""
import os, signal, socket, time
os.makedirs("/agent", exist_ok=True)
rebind = []
signal.signal(signal.SIGUSR1, lambda *_: rebind.append(1))
def listen(path):
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    if os.path.exists(path):
        os.unlink(path)
    listener.bind(path)
    listener.listen(8)
    return listener
listener = None
for path in ("/agent/guest.sock", "/rootfs-guest.sock"):
    try:
        bound = listen(path)
        listener = bound if path.startswith("/agent") else listener
        print("BIND", path, "ok", flush=True)
    except OSError as exc:
        print("BIND", path, "error", exc.errno, exc.strerror, flush=True)
print("READY", flush=True)
while True:
    if listener is None:
        if rebind:
            rebind.clear()
            listener = listen("/agent/guest.sock")
            print("REBOUND", flush=True)
        time.sleep(0.05)
        continue
    connection, _ = listener.accept()
    data = connection.recv(64)
    connection.sendall(b"pong:" + data)
    connection.close()
    print("SERVED", data.decode(), flush=True)
    if data == b"close":
        listener.close()
        listener = None
        print("CLOSED", flush=True)
"""
CLIENT = r"""
import socket, sys
try:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(5)
    client.connect(sys.argv[1])
    client.sendall(b"from-guest")
    print("CONNECT ok", client.recv(64).decode())
except OSError as exc:
    print("CONNECT error", exc.errno, exc.strerror)
"""


def host_connect(path, payload=b"from-host"):
    try:
        info = os.lstat(path)
        kind = "socket" if stat.S_ISSOCK(info.st_mode) else oct(stat.S_IFMT(info.st_mode))
    except FileNotFoundError:
        return {"host_sees": "absent"}
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(5)
            client.connect(str(path))
            client.sendall(payload)
            reply = client.recv(64).decode()
        return {"host_sees": kind, "connect": "ok", "reply": reply}
    except OSError as exc:
        return {"host_sees": kind, "connect": f"error {exc.errno} {exc.strerror}"}


def wait_for(path, pattern, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pattern in path.read_text():
            return True
        time.sleep(0.05)
    return False


def probe(args, mode, work, hibernate=False):
    cid = f"s7-{mode}-{'h-' if hibernate else ''}{uuid.uuid4().hex[:6]}"
    root = work / cid
    agent, upper, scratch, bundle = root / "agent", root / "upper", root / "work", root / "bundle"
    for path in (agent, upper, scratch, bundle / "rootfs"):
        path.mkdir(parents=True)
    subprocess.run(["mount", "-t", "overlay", "overlay", "-o",
                    f"lowerdir={args.rootfs},upperdir={upper},workdir={scratch}", str(bundle / "rootfs")],
                   check=True)
    config = {
        "ociVersion": "1.0.2", "root": {"path": "rootfs", "readonly": False},
        "process": {"terminal": False, "user": {"uid": 0, "gid": 0}, "args": ["python3", "-c", SERVER],
                    "env": ["PATH=/usr/local/bin:/usr/bin:/bin"], "cwd": "/",
                    "capabilities": {kind: [] for kind in ("bounding", "effective", "inheritable", "permitted")},
                    "noNewPrivileges": True},
        "mounts": [{"destination": "/proc", "type": "proc", "source": "proc"},
                   {"destination": "/agent", "type": "bind", "source": str(agent), "options": ["rbind", "rw"]}],
        "linux": {"namespaces": [{"type": kind} for kind in ("pid", "network", "ipc", "uts", "mount")]},
    }
    flags = ["--platform=systrap", "--network=none", f"--host-uds={mode}"]
    if hibernate:
        # Production's capture path: quota-owned memory directory, checkpoint --hibernate.
        (root / "memory" / cid).mkdir(parents=True, mode=0o700)
        config["annotations"] = {"dev.gvisor.internal.application-memory-directory": cid}
        flags.append(f"--application-memory-file-dir={root / 'memory'}")
    (bundle / "config.json").write_text(json.dumps(config))
    runsc = [str(args.runsc), f"--root={root / 'runsc'}"]
    log = root / "stdio.log"
    result = {"mode": mode, "checkpoint": "--hibernate" if hibernate else "stock"}
    host_listener = None
    try:
        with log.open("w") as output:
            subprocess.run([*runsc, *flags, "run", "--detach", f"--bundle={bundle}", cid],
                           stdout=output, stderr=output, check=True, timeout=60)
        if not wait_for(log, "READY"):
            raise RuntimeError("guest not ready: " + log.read_text()[-1000:])
        text = log.read_text()
        result["guest_bind_agent"] = [line for line in text.splitlines() if "BIND /agent" in line][0]
        result["guest_bind_rootfs"] = [line for line in text.splitlines() if "BIND /rootfs" in line][0]
        result["host_after_rootfs_bind"] = "present" if any(
            path.name == "rootfs-guest.sock" for path in (bundle / "rootfs").iterdir()) else "absent"
        result["guest_to_host"] = host_connect(agent / "guest.sock")
        # host -> guest
        host_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        host_listener.bind(str(agent / "host.sock"))
        host_listener.listen(1)
        host_listener.settimeout(8)
        received = {}

        def accept():
            try:
                connection, _ = host_listener.accept()
                received["data"] = connection.recv(64).decode()
                connection.sendall(b"pong-from-host")
                connection.close()
            except OSError as exc:
                received["error"] = f"{exc.errno} {exc.strerror}"
        thread = threading.Thread(target=accept)
        thread.start()
        client = subprocess.run([*runsc, "exec", cid, "python3", "-c", CLIENT, "/agent/host.sock"],
                                capture_output=True, text=True, timeout=30)
        thread.join(10)
        result["host_to_guest"] = {"guest": (client.stdout + client.stderr).strip()[-300:],
                                   "host_received": received}
        def state():
            completed = subprocess.run([*runsc, "state", cid], capture_output=True, text=True, timeout=30)
            return json.loads(completed.stdout)["status"] if completed.returncode == 0 else "absent"

        def checkpoint(name):
            image = root / name
            image.mkdir()
            completed = subprocess.run([*runsc, *flags, "checkpoint", *(["--hibernate"] if hibernate else []),
                                        f"--image-path={image}", cid], capture_output=True, text=True, timeout=120)
            return image, {"returncode": completed.returncode, "stderr_head": completed.stderr.strip()[:300],
                           "state_after": state()}

        def restore(image):
            subprocess.run([*runsc, "delete", "--force", cid], capture_output=True, timeout=30)
            with log.open("a") as output:
                restored = subprocess.run([*runsc, *flags, "restore", "--detach", f"--image-path={image}",
                                           f"--bundle={bundle}", cid], stdout=output, stderr=output, timeout=120)
            return {"returncode": restored.returncode, "state": state()}

        def rebind_and_connect():
            subprocess.run([*runsc, "kill", cid, "USR1"], capture_output=True, timeout=30)
            return {"rebound": wait_for(log, "REBOUND", 15), "connect": host_connect(agent / "guest.sock")}

        if result["guest_to_host"].get("connect") == "ok":
            # The C5.1 protocol candidate: close the listener, capture, then
            # restore (or resume a quiesced hibernate), rebind, connect.
            result["close_request"] = host_connect(agent / "guest.sock", b"close")
            wait_for(log, "CLOSED")
            image, result["checkpoint_after_close"] = checkpoint("after-close")
            if hibernate and result["checkpoint_after_close"]["state_after"] == "paused":
                resumed = subprocess.run([*runsc, "resume", cid], capture_output=True, text=True, timeout=30)
                result["resume_after_capture"] = resumed.returncode
                result["after_resume"] = rebind_and_connect()
            elif not hibernate and result["checkpoint_after_close"]["returncode"] == 0:
                result["restore_after_close"] = restore(image)
                result["after_restore"] = rebind_and_connect()
            # Then a live host-visible listener during capture.
            _, result["checkpoint_with_host_listener"] = checkpoint("with-listener")
            if result["checkpoint_with_host_listener"]["state_after"] == "paused":
                resumed = subprocess.run([*runsc, "resume", cid], capture_output=True, text=True, timeout=30)
                result["after_refused_capture"] = {"resume": resumed.returncode,
                                                   "connect": host_connect(agent / "guest.sock")}
        elif result["host_to_guest"]["host_received"].get("data"):
            # A guest connection to a host socket held open during checkpoint.
            host_listener.settimeout(None)
            holder = subprocess.Popen([*runsc, "exec", cid, "python3", "-c",
                                       "import socket, time\n"
                                       "c = socket.socket(socket.AF_UNIX)\n"
                                       "c.connect('/agent/host.sock')\n"
                                       "print('HELD', flush=True)\n"
                                       "time.sleep(120)"], stdout=subprocess.PIPE, text=True)
            connection, _ = host_listener.accept()
            result["held_connection"] = holder.stdout.readline().strip()
            image, result["checkpoint_with_connected_host_socket"] = checkpoint("with-connection")
            connection.close()
            holder.kill()
            holder.wait()
            if not hibernate and result["checkpoint_with_connected_host_socket"]["returncode"] == 0:
                result["restore_with_connected_host_socket"] = restore(image)
        result["guest_log"] = log.read_text()[-1500:]
    except Exception as exc:
        result["error"] = repr(exc)
        result["guest_log"] = log.read_text()[-1500:] if log.exists() else ""
    finally:
        if host_listener is not None:
            host_listener.close()
        subprocess.run([*runsc, "delete", "--force", cid], capture_output=True, timeout=30)
        subprocess.run(["umount", str(bundle / "rootfs")], capture_output=True)
        subprocess.run(["umount", str(root / "runsc/null-netns")], capture_output=True)
        shutil.rmtree(root, ignore_errors=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runsc", type=Path, default=Path("/usr/local/libexec/ucloud-gvisor/runsc"))
    parser.add_argument("--rootfs", type=Path, default=Path("/srv/spike/rootfs"))
    parser.add_argument("--work", type=Path, default=Path("/var/lib/rl-spike/s7"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    report = {"runsc": str(args.runsc), "results": [
        *(probe(args, mode, args.work) for mode in ("none", "open", "create", "all")),
        *(probe(args, mode, args.work, hibernate=True) for mode in ("open", "create"))]}
    args.output.write_text(json.dumps(report, indent=1) + "\n")
    shutil.rmtree(args.work, ignore_errors=True)


if __name__ == "__main__":
    main()
