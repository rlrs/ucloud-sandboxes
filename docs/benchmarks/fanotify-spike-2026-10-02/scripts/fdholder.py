#!/usr/bin/env python3
"""S13 gate 2: holds a fanotify group fd (and the pre-mark write fds) across listener restarts, as
systemd's fd store would.

  fdholder.py SOCK     PUT (with SCM_RIGHTS fds) stores them; GET sends them back; DROP closes them.
"""
import array
import os
import socket
import sys

path = sys.argv[1]
if os.path.exists(path):
    os.unlink(path)
srv = socket.socket(socket.AF_UNIX)
srv.bind(path)
srv.listen(8)
held = []
print("HOLDER", os.getpid(), flush=True)
while True:
    c, _ = srv.accept()
    msg, fds, _, _ = socket.recv_fds(c, 16, 64)
    if msg.startswith(b"PUT") and fds:
        for fd in held:
            os.close(fd)
        held = fds
        c.sendall(b"OK")
    elif msg.startswith(b"GET") and held:
        c.sendmsg([b"FD"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", held))])
    elif msg.startswith(b"DROP"):
        for fd in held:
            os.close(fd)
        held = []
        c.sendall(b"OK")
    else:
        c.sendall(b"NONE")
    c.close()
