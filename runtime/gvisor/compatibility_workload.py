#!/usr/bin/env python3
"""Guest state that must survive hibernation, including ACL permissions."""

import errno
import fcntl
import json
import os
from pathlib import Path
import socket
import struct
import sys


SOCKET = "/tmp/compatibility.sock"
ACL = struct.pack("<I", 2) + b"".join(
    struct.pack("<HHI", tag, permission, identity)
    for tag, permission, identity in (
        (1, 7, 0xFFFFFFFF),
        (2, 4, 1000),
        (4, 0, 0xFFFFFFFF),
        (16, 4, 0xFFFFFFFF),
        (32, 0, 0xFFFFFFFF),
    )
)


def serve():
    roots = [Path("/tmp/acl-state"), Path("/srv/acl-state")]
    for root in roots:
        root.mkdir(mode=0o755)
        (root / "access").write_text("persistent ACL content")
        os.setxattr(root / "access", "system.posix_acl_access", ACL)
        inherited = root / "inherited"
        inherited.mkdir(mode=0o755)
        os.setxattr(inherited, "system.posix_acl_default", ACL)
    lock = open("/tmp/persistent-lock", "w")
    fcntl.flock(lock, fcntl.LOCK_EX)
    requests_r, requests_w = os.pipe()
    responses_r, responses_w = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(requests_w)
        os.close(responses_r)
        os.setgroups([42])
        os.setgid(1000)
        os.setuid(1000)
        while os.read(requests_r, 1):
            result = {"uid": os.getuid(), "gid": os.getgid(), "groups": os.getgroups()}
            for root in roots:
                assert (root / "access").read_text() == "persistent ACL content"
                try:
                    fd = os.open(root / "access", os.O_WRONLY)
                except OSError as exc:
                    assert exc.errno == errno.EACCES
                else:
                    os.close(fd)
                    raise AssertionError("ACL write unexpectedly allowed")
            os.write(responses_w, json.dumps(result).encode() + b"\n")
        os._exit(0)
    os.close(requests_r)
    os.close(responses_w)
    responses = os.fdopen(responses_r)
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(SOCKET)
    listener.listen()
    listener.settimeout(0.01)
    counter = 0
    with open("/handoff-probe/counter", "wb", buffering=0) as progress:
        while True:
            counter += 1
            progress.seek(0)
            progress.write(struct.pack("<Q", counter))
            try:
                client, _ = listener.accept()
            except TimeoutError:
                continue
            with client:
                for root in roots:
                    assert os.getxattr(root / "access", "system.posix_acl_access") == ACL
                    assert os.getxattr(root / "inherited", "system.posix_acl_default") == ACL
                    created = root / "inherited" / str(counter)
                    created.touch(mode=0o777)
                    assert os.getxattr(created, "system.posix_acl_access") == ACL
                    created.unlink()
                with open("/tmp/persistent-lock") as other:
                    try:
                        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError as exc:
                        assert exc.errno in (errno.EACCES, errno.EAGAIN)
                    else:
                        raise AssertionError("open-file lock lost")
                os.write(requests_w, b"v")
                identity = json.loads(responses.readline())
                assert identity == {"uid": 1000, "gid": 1000, "groups": [42]}
                client.sendall(json.dumps({"acl": "pass", "flock": "pass", "identity": identity, "counter": counter}).encode())


if __name__ == "__main__":
    if sys.argv[1] == "server":
        serve()
    else:
        with socket.socket(socket.AF_UNIX) as client:
            client.connect(SOCKET)
            print(client.recv(4096).decode())
