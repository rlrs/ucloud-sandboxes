#!/usr/bin/env python3
"""S13 gate 2: what happens to reads when the pre-content listener dies, denies, or is replaced.

  gate2.py      (after gate1.py: uses /data/s13/g1/rafs-{full,sparse}.img)

Every case: fresh sparse copy, fresh file-backed mount, dropped caches, then readers of the image's
largest files (each spans many unfilled chunks) as separate processes, with results compared to the
complete image: sha256 equal / all zeros / errno, and how long the reader was blocked.
"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, "/root/s13")
from common import Listener, drop_caches, fresh_copy, hash_tree, mount_erofs, sh, umount  # noqa: E402

G = "/data/s13/g1"
OUT = "/data/results/gate2.json"
MNT = "/mnt/s13g2"
W = f"{G}/g2work.img"
FULL = f"{G}/rafs-full.img"
RES = {"cases": {}}
READER = r"""
import hashlib, json, os, sys, time
t = time.time()
try:
    d = open(sys.argv[1], 'rb').read()
    print(json.dumps({"sha": hashlib.sha256(d).hexdigest(), "len": len(d), "zeros": d.count(0) == len(d),
                      "zero_4k_pages": sum(1 for i in range(0, len(d), 4096) if d[i:i+4096].count(0) == len(d[i:i+4096])),
                      "s": time.time() - t}))
except OSError as e:
    print(json.dumps({"errno": e.errno, "error": e.strerror, "s": time.time() - t}))
"""


def save():
    json.dump(RES, open(OUT, "w"), indent=1)


class Reader:
    def __init__(self, rel):
        self.rel = rel
        self.t0 = time.time()
        self.p = subprocess.Popen(["python3", "-c", READER, MNT + rel], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True)

    def running(self):
        return self.p.poll() is None

    def result(self, timeout=30):
        try:
            out, err = self.p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            return {"blocked_after_s": round(time.time() - self.t0, 2), "state": proc_state(self.p.pid)}
        try:
            r = json.loads(out)
        except ValueError:
            r = {"raw": out[-200:], "stderr": err[-200:], "rc": self.p.returncode}
        r["rc"] = self.p.returncode
        r["correct"] = r.get("sha") == REF.get(self.rel)
        return r


def proc_state(pid):
    try:
        st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[0]
        wchan = open(f"/proc/{pid}/wchan").read()
        return {"state": st, "wchan": wchan}
    except OSError:
        return None


def setup():
    fresh_copy(f"{G}/rafs-sparse.img", W)
    drop_caches()


def mount():
    mo = mount_erofs(W, MNT)
    assert mo["rc"] == 0, mo
    return mo


def teardown(*procs):
    for p in procs:
        if p is not None:
            p.kill(9) if isinstance(p, Listener) else p.p.kill()
    time.sleep(0.2)
    umount(MNT)


def case(name, rec):
    RES["cases"][name] = rec
    save()
    print(name, json.dumps(rec)[:900], flush=True)


def main():
    global REF
    os.makedirs(MNT, exist_ok=True)
    m0 = mount_erofs(FULL, MNT)
    assert m0["rc"] == 0
    REF, _, _ = hash_tree(MNT)
    sizes = sorted(((os.path.getsize(MNT + p), p) for p in REF), reverse=True)
    files = [p for s, p in sizes[:6]]
    umount(MNT)
    RES["files"] = [(p, s) for s, p in sizes[:6]]
    f1, f2, f3, f4 = files[:4]

    only = set(sys.argv[1].split(",")) if len(sys.argv) > 1 else {"deny", "death", "takeover", "holder"}
    if os.path.exists(OUT):
        RES.update(json.load(open(OUT)))
    # A. Deny with an errno: which errnos are accepted, and what read() returns.
    for err in ((5, 1, 11, 28, 16, 26, 122, 13, 2) if "deny" in only else ()):
        setup()
        lst = Listener("--mark", f"{W}={FULL}", "--deny", str(err), log=f"{G}/g2-deny-{err}.jsonl")
        mount()
        r = Reader(f1).result()
        evs = lst.events()
        teardown(lst)
        case(f"deny_errno_{err}", {"reader": r, "listener_response": [e.get("response", e.get("response_error")) for e in evs[:3]],
                                    "events": len(evs)})

    # B. Listener killed (SIGKILL) while it holds an event: the pending read, then new reads.
    for how in (("sigkill", "close") if "death" in only else ()):
        setup()
        lst = Listener("--mark", f"{W}={FULL}", "--hold-ms", "4000", "--journal", f"{G}/g2-j-{how}.txt",
                       log=f"{G}/g2-{how}.jsonl")
        mount()
        r1 = Reader(f1)
        time.sleep(1.0)
        held = r1.running()
        if how == "sigkill":
            lst.kill(9)
        else:  # the group fd closes as the process exits normally (SIGTERM -> os._exit)
            lst.kill(15)
        res1 = r1.result()
        res2 = Reader(f2).result()  # a new read after the group is gone
        # The same file again: does the page cache keep what was read during the gap?
        lst2 = Listener("--mark", f"{W}={FULL}", log=f"{G}/g2-{how}-restart.jsonl")
        res1_again = Reader(f1).result()
        res3 = Reader(f3).result()  # untouched file through the restarted listener, same mount
        drop_caches()
        res1_after_drop = Reader(f1).result()
        evs2 = lst2.events()
        teardown(lst2)
        case(f"listener_{how}", {"reader_blocked_while_held": held, "pending_read": res1, "new_read_no_listener": res2,
                                 "restart_new_group_same_mount": {"same_file_again": res1_again, "untouched_file": res3,
                                                                   "same_file_after_drop_caches": res1_after_drop,
                                                                   "events": len(evs2)}})

    # C. Takeover: the group fd is held by fdholder (as systemd's fd store would); A dies holding an
    #    event; B takes the fd, answers A's journaled event by its fd number after filling, and serves on.
    for variant in (("journal_allow", "journal_eio", "no_orphan_response") if "takeover" in only else ()):
        setup()
        sock = f"/run/s13-holder.sock"
        holder = subprocess.Popen(["python3", "/root/s13/fdholder.py", sock], stdout=subprocess.PIPE, text=True)
        holder.stdout.readline()
        j = f"{G}/g2-j-take-{variant}.txt"
        if os.path.exists(j):
            os.unlink(j)
        a = Listener("--mark", f"{W}={FULL}", "--hold-ms", "60000", "--journal", j, "--send-fd", sock,
                     log=f"{G}/g2-take-a-{variant}.jsonl")
        mount()
        r1 = Reader(f1)
        time.sleep(1.0)
        a.kill(9)
        time.sleep(1.0)
        r2 = Reader(f2)  # queued while no listener runs
        time.sleep(1.5)
        blocked = {"r1_running": r1.running(), "r2_running": r2.running(), "r1_state": proc_state(r1.p.pid)}
        args = ["--mark", f"{W}={FULL}", "--recv-fd", sock, "--no-mark"]
        if variant != "no_orphan_response":
            args += ["--respond-orphans", j, "--orphan-response", "allow" if variant == "journal_allow" else "eio"]
        b = Listener(*args, log=f"{G}/g2-take-b-{variant}.jsonl")
        res1 = r1.result(timeout=8)
        res2 = r2.result(timeout=8)
        res4 = Reader(f4).result()
        evs = b.events()
        if variant == "no_orphan_response" and r1.running():
            # Is the stuck reader killable?
            t = time.time()
            r1.p.kill()
            r1.p.wait(10)
            res1["killed_after_s"] = round(time.time() - t, 3)
        teardown(b)
        holder.kill()
        case(f"takeover_{variant}", {"while_no_listener": blocked, "pending_read_of_dead_listener": res1,
                                    "queued_read": res2, "later_read": res4,
                                    "b_orphans": [e for e in evs if "orphan_fd" in e][:4],
                                    "b_events": sum(1 for e in evs if "n" in e)})

    # D. Group fd holder dies too (fd store lost): pending read.
    if "holder" not in only:
        print("GATE2_DONE", flush=True)
        return
    setup()
    sock = "/run/s13-holder.sock"
    holder = subprocess.Popen(["python3", "/root/s13/fdholder.py", sock], stdout=subprocess.PIPE, text=True)
    holder.stdout.readline()
    a = Listener("--mark", f"{W}={FULL}", "--hold-ms", "60000", "--send-fd", sock, log=f"{G}/g2-holder-dies.jsonl")
    mount()
    r1 = Reader(f1)
    time.sleep(1.0)
    a.kill(9)
    time.sleep(0.5)
    still = r1.running()
    holder.kill()
    holder.wait()
    res1 = r1.result()
    teardown()
    case("holder_dies", {"blocked_until_holder_died": still, "pending_read": res1})

    print("GATE2_DONE", flush=True)


REF = {}
main()
