#!/usr/bin/env python3
"""Bounded Linux filesystem/process probes; run inside a disposable guest.

Requires Python 3.10+. Does not install packages or change system configuration.
Each probe uses a private temporary directory under --directory.
"""

import argparse
import fcntl
import json
import os
from pathlib import Path
import platform
import socket
import struct
import subprocess
import sys
import tempfile


class ProbeUnavailable(Exception):
    pass


def require(condition: bool) -> None:
    if not condition:
        raise AssertionError("guest behavior differed from expected behavior")


def run_probes(directory: str) -> dict:
    results = {}

    def check(name, operation):
        try:
            operation()
            results[name] = {"status": "passed"}
        except ProbeUnavailable as exc:
            results[name] = {"status": "blocked", "reason": str(exc)}
        except OSError as exc:
            results[name] = {"status": "failed", "errno": exc.errno}
        except Exception as exc:
            results[name] = {"status": "failed", "error": type(exc).__name__}

    with tempfile.TemporaryDirectory(
        prefix="ucloud-conformance-", dir=directory
    ) as raw:
        root = Path(raw)

        def paths():
            target = root / "space ü :,$file"
            target.write_bytes(b"literal")
            link = root / "relative-link"
            link.symlink_to(target.name)
            require(link.read_bytes() == b"literal")
            destination = root / "renamed"
            target.replace(destination)
            require(destination.read_bytes() == b"literal")
            require(not link.exists())

        def xattrs():
            path = root / "xattrs"
            path.touch()
            os.setxattr(path, "user.ucloud_conformance", b"value")
            require(os.getxattr(path, "user.ucloud_conformance") == b"value")
            os.removexattr(path, "user.ucloud_conformance")

        def locks():
            path = root / "lock"
            with path.open("w") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                child = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        """
import errno, fcntl, sys
with open(sys.argv[1], "r") as f:
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        sys.exit(0 if e.errno in (errno.EAGAIN, errno.EACCES) else 2)
sys.exit(1)
""",
                        str(path),
                    ],
                    timeout=10,
                    capture_output=True,
                )
                require(child.returncode == 0)

        def acl():
            if os.geteuid() != 0:
                raise ProbeUnavailable("ACL enforcement probe requires root")
            root.chmod(0o755)
            traversal = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    "import os,sys; os.setgroups([]); os.setgid(65534); os.setuid(65534); sys.exit(0 if os.access(sys.argv[1], os.X_OK) else 1)",
                    str(root),
                ],
                timeout=10,
                capture_output=True,
            )
            if traversal.returncode != 0:
                raise ProbeUnavailable(
                    "ACL test identities cannot traverse the probe directory"
                )
            path = root / "acl"
            path.write_bytes(b"allowed")
            path.chmod(0o600)
            # Linux POSIX ACL xattr: owner, named uid, group, mask, other.
            entries = [
                (1, 6, 0xFFFFFFFF),
                (2, 4, 65534),
                (4, 0, 0xFFFFFFFF),
                (16, 4, 0xFFFFFFFF),
                (32, 0, 0xFFFFFFFF),
            ]
            value = struct.pack("<I", 2) + b"".join(
                struct.pack("<HHI", *entry) for entry in entries
            )
            os.setxattr(path, "system.posix_acl_access", value)
            require(os.getxattr(path, "system.posix_acl_access") == value)
            inherited_directory = root / "inherit"
            inherited_directory.mkdir(mode=0o755)
            os.setxattr(inherited_directory, "system.posix_acl_default", value)
            inherited = inherited_directory / "child"
            inherited.write_bytes(b"allowed")
            require(os.getxattr(inherited, "system.posix_acl_access") == value)
            for uid, allowed in [(65534, True), (65533, False)]:
                child = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        """
import os, sys
os.setgroups([])
os.setgid(int(sys.argv[2]))
os.setuid(int(sys.argv[2]))
try:
    value = open(sys.argv[1], "rb").read()
    inherited = open(sys.argv[4], "rb").read()
except PermissionError:
    sys.exit(0 if sys.argv[3] == "deny" else 1)
sys.exit(0 if sys.argv[3] == "allow" and value == b"allowed" and inherited == b"allowed" else 1)
""",
                        str(path),
                        str(uid),
                        "allow" if allowed else "deny",
                        str(inherited),
                    ],
                    timeout=10,
                    capture_output=True,
                )
                require(child.returncode == 0)

        def unix_socket():
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.settimeout(5)
                server.bind(str(root / "socket"))
                server.listen(1)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(5)
                    client.connect(str(root / "socket"))
                    connection, _ = server.accept()
                    with connection:
                        connection.settimeout(5)
                        client.sendall(b"ping")
                        require(connection.recv(4) == b"ping")

        def signals():
            child = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            try:
                child.terminate()
                require(child.wait(timeout=5) == -15)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)

        check("literal-paths-and-symlinks", paths)
        check("filesystem-xattrs", xattrs)
        check("filesystem-locks", locks)
        check("posix-acl", acl)
        check("unix-sockets", unix_socket)
        check("process-signals", signals)
    return {
        "schema_version": 1,
        "platform": platform.system(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "uid": os.getuid(),
        "gid": os.getgid(),
        "groups": os.getgroups(),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", default="/tmp")
    args = parser.parse_args()
    report = run_probes(args.directory)
    print(json.dumps(report, sort_keys=True))
    return (
        0 if all(row["status"] == "passed" for row in report["results"].values()) else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
