#!/usr/bin/env python3
"""S13 gates 1-2: minimal fanotify pre-content listener.

  fanl.py --mark BACKING=SOURCE [--mark ...] [--log events.jsonl] [--hold-ms N] [--deny ERRNO]
          [--no-fill] [--journal FILE] [--send-fd SOCK] [--recv-fd SOCK] [--no-mark]
          [--respond-orphans JOURNAL] [--orphan-response allow|eio] [--exit-after N]

Each BACKING is a sparse file that EROFS mounts; SOURCE is the complete image it is filled from. The
listener opens BACKING read-write *before* it places the mark (so its own writes raise no events),
initialises a FAN_CLASS_PRE_CONTENT group, marks every BACKING with FAN_PRE_ACCESS (inode marks), then
for each event: logs it, appends "fd ino offset count" to the journal, optionally holds, copies the
requested range from SOURCE into BACKING (pread/pwrite), and answers FAN_ALLOW (or FAN_DENY with an
errno). --send-fd hands the group fd to fdholder.py; --recv-fd takes it back for a takeover;
--respond-orphans answers events a dead predecessor read but never answered, by their fd numbers.
"""
import argparse
import array
import json
import os
import signal
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fan  # noqa: E402


def comm(pid):
    try:
        return open(f"/proc/{pid}/comm").read().strip()
    except OSError:
        return None


def send_fd(path, fds):
    """Hand the group fd and the pre-mark write fds to the holder (fds[0] is the group)."""
    s = socket.socket(socket.AF_UNIX)
    s.connect(path)
    s.sendmsg([b"PUT"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))])
    s.recv(16)
    s.close()


def recv_fd(path):
    s = socket.socket(socket.AF_UNIX)
    s.connect(path)
    s.sendall(b"GET")
    msg, fds, _, _ = socket.recv_fds(s, 16, 64)
    s.close()
    if not fds:
        raise RuntimeError(f"holder sent no fd: {msg!r}")
    return fds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mark", action="append", default=[])
    ap.add_argument("--log")
    ap.add_argument("--hold-ms", type=float, default=0)
    ap.add_argument("--deny", type=int, default=0)
    ap.add_argument("--no-fill", action="store_true")
    ap.add_argument("--journal")
    ap.add_argument("--send-fd")
    ap.add_argument("--recv-fd")
    ap.add_argument("--no-mark", action="store_true")
    ap.add_argument("--respond-orphans")
    ap.add_argument("--orphan-response", default="allow", choices=["allow", "eio"])
    ap.add_argument("--exit-after", type=int, default=0)
    ap.add_argument("--class", dest="klass", default="pre_content", choices=["pre_content", "content"])
    a = ap.parse_args()

    files = {}
    for spec in a.mark:
        backing, _, source = spec.partition("=")
        wfd = os.open(backing, os.O_RDWR)
        sfd = os.open(source, os.O_RDONLY) if source else None
        st = os.fstat(wfd)
        files[st.st_ino] = {"backing": backing, "wfd": wfd, "sfd": sfd,
                            "ssize": os.fstat(sfd).st_size if sfd is not None else 0}
    if a.recv_fd:
        gfd, *wfds = recv_fd(a.recv_fd)
        # Write fds opened by the predecessor before its marks existed: they raise no events. A fd
        # this process opens now, with the mark in place, would block on its own group (S13 gate 2).
        for w in wfds:
            ino = os.fstat(w).st_ino
            if ino in files:
                os.close(files[ino]["wfd"])
                files[ino]["wfd"] = w
                files[ino]["inherited_wfd"] = True
    else:
        klass = fan.FAN_CLASS_PRE_CONTENT if a.klass == "pre_content" else fan.FAN_CLASS_CONTENT
        gfd = fan.init(klass | fan.FAN_CLOEXEC | fan.FAN_UNLIMITED_QUEUE | fan.FAN_UNLIMITED_MARKS)
    marked = []
    if not a.no_mark:
        for f in files.values():
            fan.mark(gfd, f["backing"])
            marked.append(f["backing"])
    if a.send_fd:
        send_fd(a.send_fd, [gfd] + [f["wfd"] for f in files.values()])
    log = open(a.log, "a") if a.log else None
    journal = open(a.journal, "a") if a.journal else None
    t0 = time.time()

    def emit(rec):
        rec["t"] = round(time.time() - t0, 6)
        line = json.dumps(rec)
        if log:
            log.write(line + "\n")
            log.flush()

    def fill(f, off, count):
        if f["sfd"] is None or a.no_fill:
            return 0
        end = min(off + count, f["ssize"])
        done = 0
        while off + done < end:
            n = min(4 << 20, end - off - done)
            data = os.pread(f["sfd"], n, off + done)
            if not data:
                break
            os.pwrite(f["wfd"], data, off + done)
            done += len(data)
        return done

    if a.respond_orphans:
        for line in open(a.respond_orphans):
            efd, ino, off, count = map(int, line.split())
            f = files.get(ino)
            filled = fill(f, off, count) if f else 0
            resp = fan.FAN_ALLOW if a.orphan_response == "allow" else fan.deny_errno(5)
            try:
                fan.respond(gfd, efd, resp)
                emit({"orphan_fd": efd, "ino": ino, "off": off, "count": count, "filled": filled, "response": "ok"})
            except OSError as e:
                emit({"orphan_fd": efd, "ino": ino, "off": off, "count": count, "filled": filled,
                      "response": f"errno {e.errno} {e.strerror}"})

    print(json.dumps({"ready": True, "pid": os.getpid(), "group_fd": gfd, "marked": marked,
                      "inherited_wfds": sum(1 for f in files.values() if f.get("inherited_wfd")),
                      "header": fan.check_header()}), flush=True)
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    n = 0
    while True:
        buf = os.read(gfd, 1 << 16)
        for mask, efd, pid, ranges, infos, vers, mlen, elen in fan.parse(buf):
            n += 1
            ino = None
            if efd >= 0:
                try:
                    ino = os.fstat(efd).st_ino
                except OSError:
                    pass
            off, count = ranges[0] if ranges else (None, None)
            if journal and efd >= 0:
                journal.write(f"{efd} {ino} {off if off is not None else 0} {count if count is not None else 0}\n")
                journal.flush()
            rec = {"n": n, "mask": hex(mask), "fd": efd, "pid": pid, "comm": comm(pid), "ino": ino,
                   "ranges": ranges, "infos": infos, "vers": vers, "metadata_len": mlen, "event_len": elen}
            if a.hold_ms:
                time.sleep(a.hold_ms / 1000)
            f = files.get(ino)
            t1 = time.perf_counter()
            filled = 0
            if f and ranges and not a.deny:
                filled = fill(f, off, count)
            rec["filled"] = filled
            rec["fill_us"] = round((time.perf_counter() - t1) * 1e6, 1)
            resp = fan.deny_errno(a.deny) if a.deny else fan.FAN_ALLOW
            if efd >= 0:
                try:
                    fan.respond(gfd, efd, resp)
                    rec["response"] = hex(resp)
                except OSError as e:
                    rec["response_error"] = f"{e.errno} {e.strerror}"
                os.close(efd)
            emit(rec)
            if a.exit_after and n >= a.exit_after:
                os._exit(0)


if __name__ == "__main__":
    main()
