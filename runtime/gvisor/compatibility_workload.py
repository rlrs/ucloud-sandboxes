#!/usr/bin/env python3
"""Guest state that must survive hibernation, including ACL permissions."""

from contextlib import contextmanager
import errno
import fcntl
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import struct
import sys
import traceback


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
WRITE_ACL = struct.pack("<I", 2) + b"".join(
    struct.pack("<HHI", tag, permission, identity)
    for tag, permission, identity in (
        (1, 7, 0xFFFFFFFF),
        (2, 7, 1000),
        (2, 7, 1001),
        (4, 0, 0xFFFFFFFF),
        (16, 7, 0xFFFFFFFF),
        (32, 0, 0xFFFFFFFF),
    )
)


def as_user(uid, operation):
    """Run an assertion as a distinct unprivileged guest identity."""
    reader, writer = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(reader)
        try:
            os.setgroups([])
            os.setgid(uid)
            os.setuid(uid)
            operation()
            result = {"ok": True}
        except BaseException as exc:
            result = {"error": repr(exc)}
        os.write(writer, json.dumps(result).encode())
        os.close(writer)
        os._exit(0)
    os.close(writer)
    with os.fdopen(reader) as response:
        raw = response.read()
    _, status = os.waitpid(child, 0)
    assert status == 0, f"ACL identity helper {uid} exited with status {status}"
    result = json.loads(raw)
    assert result.get("ok"), f"ACL identity {uid}: {result}"


def create_file(path, mode, content="created"):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode), "w") as stream:
        stream.write(content)


def denied(operation):
    try:
        operation()
    except OSError as exc:
        assert exc.errno == errno.EACCES, f"expected EACCES, got {exc!r}"
    else:
        raise AssertionError("private object allowed another identity")


def prepare_acl_inheritance_root(root):
    writable = root / "write-inheritance"
    writable.mkdir(mode=0o770)
    os.chown(writable, 0, 42)
    writable.chmod(0o2770)
    os.setxattr(writable, "system.posix_acl_access", WRITE_ACL)
    os.setxattr(writable, "system.posix_acl_default", WRITE_ACL)


def qualify_acl_umask(root, counter):
    """Exercise effective inheritance, private modes and ordinary umask rules."""
    parent = root / "write-inheritance"
    assert os.getxattr(parent, "system.posix_acl_default") == WRITE_ACL
    for mask in (0o022, 0o077):
        for author, peer in ((1000, 1001), (1001, 1000)):
            name = f"{counter}-{mask:o}-{author}"
            shared = parent / name
            plain = root / ("plain-" + name)
            shared.mkdir(mode=0o777)
            # Establish a writable fixture parent independently of inheritance
            # under test; child creation below must still honor its own mode.
            shared.chmod(0o2770)
            plain.mkdir(mode=0o700)
            os.chown(plain, author, author)
            try:
                def create():
                    previous = os.umask(mask)
                    try:
                        create_file(shared / "file", 0o666)
                        create_file(shared / "private-file", 0o600)
                        os.mkdir(shared / "directory", 0o777)
                        os.mkdir(shared / "private-directory", 0o700)
                        os.mkfifo(shared / "fifo", 0o666)
                        with socket.socket(socket.AF_UNIX) as listener:
                            listener.bind(str(shared / "socket"))
                        create_file(plain / "file", 0o666)
                        os.mkdir(plain / "directory", 0o777)
                        with socket.socket(socket.AF_UNIX) as listener:
                            listener.bind(str(plain / "socket"))
                    finally:
                        os.umask(previous)

                as_user(author, create)

                def access():
                    with (shared / "file").open("a") as stream:
                        stream.write("-peer")
                    create_file(shared / "directory" / "peer-file", 0o666)
                    reader = os.open(shared / "fifo", os.O_RDONLY | os.O_NONBLOCK)
                    try:
                        writer = os.open(shared / "fifo", os.O_WRONLY | os.O_NONBLOCK)
                        os.close(writer)
                    finally:
                        os.close(reader)
                    # Linux unix_bind_bsd applies umask before creating the
                    # socket, even when the parent supplies a default ACL.
                    assert not os.access(shared / "socket", os.W_OK), "socket bind ignored umask"
                    denied(lambda: (shared / "private-file").open("a"))
                    denied(lambda: create_file(shared / "private-directory" / "denied", 0o666))

                as_user(peer, access)
                assert (shared / "file").read_text() == "created-peer"
                for name, mode in (
                    ("file", 0o660), ("private-file", 0o600),
                    ("directory", 0o2770), ("private-directory", 0o2700),
                    ("fifo", 0o660), ("socket", 0o770 & ~mask), ("directory/peer-file", 0o660),
                ):
                    actual = (shared / name).stat()
                    assert stat.S_IMODE(actual.st_mode) == mode, (name, oct(actual.st_mode), oct(mode))
                    assert actual.st_gid == 42, (name, actual.st_gid)
                assert stat.S_IMODE((plain / "file").stat().st_mode) == 0o666 & ~mask
                assert stat.S_IMODE((plain / "directory").stat().st_mode) == 0o777 & ~mask
                assert stat.S_IMODE((plain / "socket").stat().st_mode) == 0o777 & ~mask
                assert os.getxattr(shared / "directory", "system.posix_acl_default") == WRITE_ACL
            finally:
                shutil.rmtree(shared)
                shutil.rmtree(plain)


@contextmanager
def report_guest_errors(client):
    """Keep detached guest failures visible to the qualification runner."""
    try:
        yield
    except Exception as exc:
        client.sendall(json.dumps({
            "guest_error": repr(exc),
            "traceback": traceback.format_exc(),
        }).encode())
        raise


def serve():
    roots = [Path("/tmp/acl-state"), Path("/srv/acl-state")]
    for root in roots:
        root.mkdir(mode=0o755)
        (root / "access").write_text("persistent ACL content")
        os.setxattr(root / "access", "system.posix_acl_access", ACL)
        inherited = root / "inherited"
        inherited.mkdir(mode=0o755)
        os.setxattr(inherited, "system.posix_acl_default", ACL)
        prepare_acl_inheritance_root(root)
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
            with client, report_guest_errors(client):
                for root in roots:
                    assert os.getxattr(root / "access", "system.posix_acl_access") == ACL
                    assert os.getxattr(root / "inherited", "system.posix_acl_default") == ACL
                    created = root / "inherited" / str(counter)
                    created.touch(mode=0o777)
                    assert os.getxattr(created, "system.posix_acl_access") == ACL
                    created.unlink()
                    qualify_acl_umask(root, counter)
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
                client.sendall(json.dumps({"acl": "pass", "acl_umask": "pass", "flock": "pass", "identity": identity, "counter": counter}).encode())



if __name__ == "__main__":
    if sys.argv[1] == "server":
        serve()
    else:
        with socket.socket(socket.AF_UNIX) as client:
            client.connect(SOCKET)
            print(client.recv(4096).decode())
