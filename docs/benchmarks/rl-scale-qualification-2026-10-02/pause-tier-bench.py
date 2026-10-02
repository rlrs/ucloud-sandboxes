#!/usr/bin/env python3
"""C1.1 pause tier on real gVisor: pause, reclaim, resume, exec, refault (qualification only).

Run as root on a disposable host, never on a node that serves sandboxes. Each
sandbox runs from a host overlay over a read-only rootfs, in its own cgroup
under one cgroup this script creates, with application memory in a file on a
swappable tmpfs (production's RAM mode without ``noswap``, plan C1.1). The
guest holds a 512 MiB random heap and touches a 128 MiB hot set in a loop;
SIGUSR1 makes it time one touch of every page of both, then verify the heap.

  pause    runsc pause; memory.reclaim "<memory.current> swappiness=200";
           runsc resume; first successful runsc exec true; guest refault
  freeze   pause, plus cgroup.freeze around the reclaim
  zswaponly  pause with memory.zswap.writeback=0: compress or stay resident
  prefetch   pause, then before resume read the memory file back in parallel
  hibernate  stock runsc checkpoint --image-path; runsc restore; first exec; refault

Everything created (containers, mounts, cgroups, files) is under one run
directory and one cgroup and is removed on exit; the zswap enable switch is
restored to its original value.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import uuid

CGROUP_ROOT = Path("/sys/fs/cgroup")
ZSWAP = Path("/sys/module/zswap/parameters/enabled")
MIB = 1 << 20
GUEST = r"""
import hashlib, os, signal, sys, time
PAGE = 4096
if HEAP_KIND == "random":
    heap = bytearray(os.urandom(HEAP_MIB << 20))
else:
    # Text-like: random words from a 4 KiB vocabulary, about 3x under zstd.
    import random
    words = [os.urandom(4).hex().encode() for _ in range(512)]
    rng = random.Random(1)
    block = b" ".join(rng.choice(words) for _ in range(1 << 18))[:1 << 21]
    heap = bytearray(block * (HEAP_MIB // 2))
digest = hashlib.sha256(heap).hexdigest()
hot = bytearray(HOT_MIB << 20)
requests = []
signal.signal(signal.SIGUSR1, lambda *_: requests.append(1))
def touch(buffer):
    total = 0
    for offset in range(0, len(buffer), PAGE):
        total += buffer[offset]
    return total
print("READY", flush=True)
sweeps = 0
while True:
    for offset in range(0, len(hot), PAGE):
        hot[offset] = (hot[offset] + 1) & 255
    sweeps += 1
    if requests:
        requests.clear()
        started = time.monotonic()
        touch(heap)
        touch(hot)
        seconds = time.monotonic() - started
        intact = hashlib.sha256(heap).hexdigest() == digest
        print("REFAULT %.6f %d %s" % (seconds, sweeps, intact), flush=True)
    time.sleep(0.02)
"""
OCI_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


def read(path):
    return Path(path).read_text()


def kv(text):
    return {key: int(value) for key, value in (line.split()[:2] for line in text.splitlines() if line.strip())}


def meminfo():
    values = {}
    for line in read("/proc/meminfo").splitlines():
        key, value = line.split(":", 1)
        if key in ("MemAvailable", "SwapTotal", "SwapFree", "Zswap", "Zswapped", "Shmem", "AnonPages"):
            values[key] = int(value.split()[0]) * 1024
    return values


def vmstat():
    stats = kv(read("/proc/vmstat"))
    return {key: stats.get(key, 0) for key in ("pswpin", "pswpout", "zswpin", "zswpout", "zswpwb")}


def cgroup_memory(path):
    stat = kv(read(path / "memory.stat"))
    return {"current": int(read(path / "memory.current")), "swap": int(read(path / "memory.swap.current")),
            "zswap": int(read(path / "memory.zswap.current")),
            **{key: stat[key] for key in ("anon", "file", "shmem", "zswapped", "file_mapped")}}


def delta(after, before):
    return {key: after[key] - before[key] for key in after}


class Bench:
    def __init__(self, args):
        self.args = args
        self.run_id = uuid.uuid4().hex[:8]
        self.dir = args.work / f"pause-{self.run_id}"
        self.dir.mkdir(parents=True, mode=0o700)
        self.cgroup = CGROUP_ROOT / f"ucloud-pause-bench-{self.run_id}"
        self.cgroup.mkdir()
        (self.cgroup / "cgroup.subtree_control").write_text("+memory +cpu +pids")
        self.memory = self.dir / "memory"
        self.memory.mkdir()
        # Swappable tmpfs: production's RAM backing minus noswap.
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=24g,mode=0700", "tmpfs", str(self.memory)], check=True)
        self.runsc = [str(args.runsc), f"--root={self.dir / 'runsc'}"]
        self.flags = ["--platform=systrap", "--network=none"]
        if args.backing == "tmpfs":
            self.flags.append(f"--application-memory-file-dir={self.memory}")
        self.sandboxes, self.created = [], 0
        self.zswap_original = read(ZSWAP).strip()

    def command(self, argv, *, timeout=120, check=True, stdout=None):
        # A sandbox inherits create's and restore's stdio: both go to its log,
        # never to a pipe that would stay open for the sandbox's lifetime.
        started = time.monotonic()
        completed = subprocess.run(argv, capture_output=stdout is None, stdout=stdout,
                                   stderr=subprocess.STDOUT if stdout is not None else None,
                                   text=True, timeout=timeout)
        seconds = time.monotonic() - started
        if check and completed.returncode:
            detail = completed.stderr if stdout is None else Path(stdout.name).read_text()
            raise RuntimeError(f"{argv[2:4]} failed: {detail[-1500:]}")
        return completed, seconds

    def create(self, index):
        cid = f"pause-{self.run_id}-{index}"
        root = self.dir / cid
        upper, work, bundle = root / "upper", root / "work", root / "bundle"
        for path in (upper, work, bundle / "rootfs"):
            path.mkdir(parents=True)
        subprocess.run(["mount", "-t", "overlay", "overlay", "-o",
                        f"lowerdir={self.args.rootfs},upperdir={upper},workdir={work}", str(bundle / "rootfs")],
                       check=True)
        cgroup = self.cgroup / cid
        cgroup.mkdir()
        program = (GUEST.replace("HEAP_MIB", str(self.args.heap_mib)).replace("HOT_MIB", str(self.args.hot_mib))
                   .replace("HEAP_KIND", repr(self.args.heap_kind)))
        config = {
            "ociVersion": "1.0.2", "root": {"path": "rootfs", "readonly": False},
            "process": {"terminal": False, "user": {"uid": 0, "gid": 0}, "args": ["python3", "-c", program],
                        "env": [f"PATH={OCI_PATH}", "HOME=/root", "PYTHONUNBUFFERED=1"], "cwd": "/",
                        "capabilities": {kind: [] for kind in ("bounding", "effective", "inheritable", "permitted")},
                        "noNewPrivileges": True},
            "mounts": [{"destination": "/proc", "type": "proc", "source": "proc"},
                       {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"}],
            "linux": {"namespaces": [{"type": kind} for kind in ("pid", "network", "ipc", "uts", "mount")],
                      "cgroupsPath": f"/{self.cgroup.name}/{cid}"},
        }
        if self.args.backing == "tmpfs":
            # The pinned runsc's quota-owned memory directory (patch 0001).
            (self.memory / cid).mkdir(mode=0o700)
            config["annotations"] = {"dev.gvisor.internal.application-memory-directory": cid}
        (bundle / "config.json").write_text(json.dumps(config))
        sandbox = {"cid": cid, "bundle": bundle, "cgroup": cgroup, "log": root / "stdio-0.log", "logs": 0,
                   "events": {}}
        self.sandboxes.append(sandbox)
        with sandbox["log"].open("w") as log:
            _, sandbox["events"]["create_seconds"] = self.command(
                [*self.runsc, *self.flags, "create", f"--bundle={bundle}", cid], stdout=log)
        _, sandbox["events"]["start_seconds"] = self.command([*self.runsc, "start", cid])
        return sandbox

    def wait_line(self, sandbox, pattern, after, timeout=180):
        """First stdout line matching ``pattern`` beyond ``after`` matches seen."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            matches = re.findall(pattern, sandbox["log"].read_text())
            if len(matches) > after:
                return matches[after]
            time.sleep(0.01)
        raise RuntimeError(f"{sandbox['cid']}: no {pattern!r} line")

    def refault(self, sandbox):
        seen = len(re.findall(r"REFAULT .*", sandbox["log"].read_text()))
        started = time.monotonic()
        self.command([*self.runsc, "kill", sandbox["cid"], "USR1"])
        line = self.wait_line(sandbox, r"REFAULT (\S+) (\d+) (\w+)", seen)
        return {"guest_seconds": float(line[0]), "wall_seconds": time.monotonic() - started,
                "intact": line[2] == "True"}

    def first_exec(self, sandbox, since):
        attempts = 0
        while True:
            attempts += 1
            completed, seconds = self.command([*self.runsc, "exec", sandbox["cid"], "true"], check=False)
            if completed.returncode == 0:
                return {"exec_seconds": seconds, "since_resume_seconds": time.monotonic() - since,
                        "attempts": attempts}
            if attempts > 50:
                raise RuntimeError("exec never succeeded: " + completed.stderr[-500:])

    def reclaim(self, sandbox):
        cgroup = sandbox["cgroup"]
        before, host_before = cgroup_memory(cgroup), vmstat()
        target = before["current"]
        started = time.monotonic()
        error = None
        try:
            with open(cgroup / "memory.reclaim", "w") as control:
                control.write(f"{target} swappiness=200")
        except OSError as exc:
            error = exc.strerror
        seconds = time.monotonic() - started
        after = cgroup_memory(cgroup)
        moved = before["current"] - after["current"]
        return {"target_bytes": target, "seconds": seconds, "short": error, "moved_bytes": moved,
                "mb_per_second": moved / MIB / seconds if seconds else None, "before": before, "after": after,
                "cgroup_delta": delta(after, before), "vmstat_delta": delta(vmstat(), host_before)}

    def prefetch(self, sandbox, threads):
        """Swap the paused sandbox's memory file back in from the host, in parallel.

        The plan's thaw prefetch: tmpfs SEEK_DATA extents (swapped pages are
        still data) read in 4 MiB pieces; swap-in charges the sandbox's cgroup.
        """
        pieces = []
        for path in (self.memory / sandbox["cid"]).rglob("*"):
            if not path.is_file():
                continue
            fd = os.open(path, os.O_RDONLY)
            try:
                size, offset = os.fstat(fd).st_size, 0
                while offset < size:
                    try:
                        start = os.lseek(fd, offset, os.SEEK_DATA)
                    except OSError:
                        break
                    end = os.lseek(fd, start, os.SEEK_HOLE)
                    pieces += [(path, at, min(end, at + 4 * MIB)) for at in range(start, end, 4 * MIB)]
                    offset = end
            finally:
                os.close(fd)

        def read_piece(piece):
            path, start, end = piece
            fd = os.open(path, os.O_RDONLY)
            try:
                while start < end:
                    start += len(os.pread(fd, min(MIB, end - start), start)) or end
            finally:
                os.close(fd)
        host, started = vmstat(), time.monotonic()
        with ThreadPoolExecutor(threads) as pool:
            list(pool.map(read_piece, pieces))
        return {"seconds": time.monotonic() - started, "threads": threads,
                "bytes": sum(end - start for _, start, end in pieces), "vmstat_delta": delta(vmstat(), host),
                "memory_after": cgroup_memory(sandbox["cgroup"])}

    def pause_cycle(self, sandbox, *, freeze=False, zswap_only=False, prefetch=0):
        cid, result = sandbox["cid"], {}
        result["refault_before"] = self.refault(sandbox)
        result["exec_running"] = self.first_exec(sandbox, time.monotonic())
        time.sleep(self.args.settle)
        result["memory_running"] = cgroup_memory(sandbox["cgroup"])
        _, result["pause_seconds"] = self.command([*self.runsc, "pause", cid])
        result["frozen_by_runsc_pause"] = "frozen 1" in read(sandbox["cgroup"] / "cgroup.events")
        result["state_paused"] = json.loads(self.command([*self.runsc, "state", cid])[0].stdout)["status"]
        if freeze:
            started = time.monotonic()
            (sandbox["cgroup"] / "cgroup.freeze").write_text("1")
            while "frozen 1" not in read(sandbox["cgroup"] / "cgroup.events"):
                time.sleep(0.001)
            result["freeze_seconds"] = time.monotonic() - started
        if zswap_only:
            # No swap-device writes for this cgroup: zswap or stay resident.
            (sandbox["cgroup"] / "memory.zswap.writeback").write_text("0")
        result["reclaim"] = self.reclaim(sandbox)
        if prefetch:
            result["prefetch"] = self.prefetch(sandbox, prefetch)
        if freeze:
            started = time.monotonic()
            (sandbox["cgroup"] / "cgroup.freeze").write_text("0")
            while "frozen 0" not in read(sandbox["cgroup"] / "cgroup.events"):
                time.sleep(0.001)
            result["thaw_seconds"] = time.monotonic() - started
        _, result["resume_seconds"] = self.command([*self.runsc, "resume", cid])
        result["first_exec"] = self.first_exec(sandbox, time.monotonic())
        host = vmstat()
        result["refault"] = self.refault(sandbox)
        result["refault"]["vmstat_delta"] = delta(vmstat(), host)
        result["memory_after_refault"] = cgroup_memory(sandbox["cgroup"])
        return result

    def hibernate_cycle(self, sandbox):
        cid, result = sandbox["cid"], {}
        result["refault_before"] = self.refault(sandbox)
        time.sleep(self.args.settle)
        result["memory_running"] = cgroup_memory(sandbox["cgroup"])
        image = self.dir / f"{cid}.checkpoint"
        image.mkdir()
        _, result["checkpoint_seconds"] = self.command(
            [*self.runsc, *self.flags, "checkpoint", f"--image-path={image}", cid], timeout=600)
        result["image_bytes"] = sum(path.stat().st_blocks * 512 for path in image.rglob("*") if path.is_file())
        result["image_files"] = {path.name: path.stat().st_size for path in image.iterdir()}
        self.command([*self.runsc, "delete", "--force", cid], check=False)
        sandbox["logs"] += 1
        sandbox["log"] = sandbox["log"].with_name(f"stdio-{sandbox['logs']}.log")
        subprocess.run(["sync"], check=True)
        if self.args.drop_caches:
            Path("/proc/sys/vm/drop_caches").write_text("3")
        with sandbox["log"].open("w") as log:
            _, result["restore_seconds"] = self.command(
                [*self.runsc, *self.flags, "restore", "--detach", f"--image-path={image}",
                 f"--bundle={sandbox['bundle']}", cid], timeout=600, stdout=log)
        result["first_exec"] = self.first_exec(sandbox, time.monotonic())
        result["refault"] = self.refault(sandbox)
        result["memory_after_refault"] = cgroup_memory(sandbox["cgroup"])
        shutil.rmtree(image)
        return result

    def run(self, mode, count, zswap):
        ZSWAP.write_text("Y" if zswap else "N")
        with ThreadPoolExecutor(count) as pool:
            sandboxes = list(pool.map(self.create, range(self.created, self.created + count)))
            self.created += count
            list(pool.map(lambda item: self.wait_line(item, r"READY", 0), sandboxes))
            started, host = time.monotonic(), meminfo()
            cycle = {"pause": self.pause_cycle, "freeze": lambda item: self.pause_cycle(item, freeze=True),
                     "zswaponly": lambda item: self.pause_cycle(item, zswap_only=True),
                     "prefetch": lambda item: self.pause_cycle(item, prefetch=self.args.prefetch_threads),
                     "hibernate": self.hibernate_cycle}[mode]
            results = list(pool.map(cycle, sandboxes))
        report = {"mode": mode, "sandboxes": count, "zswap": zswap, "wall_seconds": time.monotonic() - started,
                  "host_meminfo_before": host, "host_meminfo_after": meminfo(), "results": results,
                  "create": [item["events"] for item in sandboxes]}
        self.destroy(sandboxes)
        return report

    def destroy(self, sandboxes):
        for sandbox in sandboxes:
            self.command([*self.runsc, "delete", "--force", sandbox["cid"]], check=False)
            subprocess.run(["umount", str(sandbox["bundle"] / "rootfs")], check=False)
            shutil.rmtree(self.memory / sandbox["cid"], ignore_errors=True)
            for _ in range(100):
                try:
                    sandbox["cgroup"].rmdir()
                    break
                except FileNotFoundError:
                    break
                except OSError:
                    time.sleep(0.1)
            self.sandboxes.remove(sandbox)

    def close(self):
        self.destroy(list(self.sandboxes))
        ZSWAP.write_text(self.zswap_original)
        subprocess.run(["umount", str(self.memory)], check=False)
        subprocess.run(["umount", str(self.dir / "runsc/null-netns")], check=False)
        try:
            self.cgroup.rmdir()
        except OSError:
            pass
        shutil.rmtree(self.dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runsc", type=Path, default=Path("/usr/local/libexec/ucloud-gvisor/runsc"))
    parser.add_argument("--rootfs", type=Path, default=Path("/srv/spike/rootfs"))
    parser.add_argument("--work", type=Path, default=Path("/var/lib/rl-spike"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backing", choices=("tmpfs", "memfd"), default="tmpfs")
    parser.add_argument("--heap-mib", type=int, default=512)
    parser.add_argument("--heap-kind", choices=("random", "text"), default="random")
    parser.add_argument("--prefetch-threads", type=int, default=8)
    parser.add_argument("--hot-mib", type=int, default=128)
    parser.add_argument("--settle", type=float, default=2.0)
    parser.add_argument("--drop-caches", action="store_true", help="Drop the page cache before each restore")
    parser.add_argument("--scenarios", default="pause:1:1,pause:1:0,freeze:1:1,hibernate:1:1,"
                        "pause:8:1,pause:8:0,hibernate:8:1",
                        help="mode:sandboxes:zswap, comma separated")
    args = parser.parse_args()
    bench = Bench(args)
    report = {"runsc": str(args.runsc), "kernel": os.uname().release, "backing": args.backing,
              "heap_kind": args.heap_kind, "heap_mib": args.heap_mib, "hot_mib": args.hot_mib, "zswap_parameters": {
                  path.name: path.read_text().strip() for path in ZSWAP.parent.iterdir()},
              "swaps": read("/proc/swaps"), "scenarios": []}
    try:
        for scenario in args.scenarios.split(","):
            mode, count, zswap = scenario.split(":")
            try:
                report["scenarios"].append(bench.run(mode, int(count), zswap == "1"))
            except Exception as exc:
                import traceback
                report["scenarios"].append({"scenario": scenario, "error": repr(exc),
                                            "traceback": traceback.format_exc()})
                bench.destroy(list(bench.sandboxes))
            args.output.write_text(json.dumps(report, indent=1, default=str) + "\n")
            print(scenario, "done", flush=True)
    finally:
        bench.close()
        args.output.write_text(json.dumps(report, indent=1, default=str) + "\n")


if __name__ == "__main__":
    main()
