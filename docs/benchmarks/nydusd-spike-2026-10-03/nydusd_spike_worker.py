#!/usr/bin/env python3
"""Worker side of the nydusd spike (run as root on a gate canary or baseline).

  arm   one cold burst (the M1 gate's bench) with the Python RAFS export or
        nydusd, sampling host CPU of environment-io and every nydusd
  kill  a nydusd burst that kill -9s some daemons mid-read, then records what
        their sandboxes saw, whether other sandboxes were untouched, and what
        a new create of a killed image does
  split fetch vs gVisor's filesystem path: a cold run, warm runs in the
        sandbox, warm runs natively (chroot into the same host rootfs), and
        the sentry's directfs evidence

Needs /opt/m1-gate/chunk_store_gate_remote.py (staged by the gate) and, for
nydusd, /opt/m1-gate/nydusd (v2.4.5 built with --features block-nbd).
"""
import argparse
import json
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/opt/m1-gate")
import chunk_store_gate_remote as gate  # noqa: E402

NYDUSD = "/opt/m1-gate/nydusd"
DROP_IN = Path("/etc/systemd/system/ucloud-environment-io.service.d/nydusd-spike.conf")
TICK = os.sysconf("SC_CLK_TCK")


def set_mode(mode):
    if mode == "nydusd":
        DROP_IN.parent.mkdir(parents=True, exist_ok=True)
        DROP_IN.write_text(f"[Service]\nEnvironment=UCLOUD_ENVIRONMENT_NYDUSD={NYDUSD}\n")
    else:
        DROP_IN.unlink(missing_ok=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)  # The bench's reset restarts the service.


def cpu_ticks(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return int(fields[11]) + int(fields[12]) + int(fields[13]) + int(fields[14])  # utime stime cutime cstime
    except (OSError, IndexError, ValueError):
        return None


def rss_kb(pid):
    try:
        return int(re.search(r"VmRSS:\s+(\d+)", Path(f"/proc/{pid}/status").read_text()).group(1))
    except (OSError, AttributeError):
        return 0


def pids(name):
    found = subprocess.run(["pgrep", "-x", name], capture_output=True, text=True).stdout.split()
    return [int(pid) for pid in found]


def io_pid():
    out = subprocess.run(["systemctl", "show", "-p", "MainPID", "--value", "ucloud-environment-io.service"],
                         capture_output=True, text=True).stdout.strip()
    return int(out or 0)


class Sampler(threading.Thread):
    """Peak per-process CPU ticks of environment-io and every nydusd, peak
    nydusd count and summed RSS; a process's last sample stands for it."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop, self.ticks, self.peak_daemons, self.peak_rss_kb = threading.Event(), {}, 0, 0
        self.base = {}

    def run(self):
        main = io_pid()
        self.base[main] = cpu_ticks(main) or 0
        while not self.stop.is_set():
            daemons = pids("nydusd")
            self.peak_daemons = max(self.peak_daemons, len(daemons))
            self.peak_rss_kb = max(self.peak_rss_kb, sum(rss_kb(pid) for pid in daemons))
            for pid in [main, *daemons]:
                ticks = cpu_ticks(pid)
                if ticks is not None:
                    self.ticks[pid] = ticks
            self.stop.wait(.25)

    def result(self, main):
        io_seconds = (self.ticks.get(main, 0) - self.base.get(main, 0)) / TICK
        nydusd_seconds = sum(ticks for pid, ticks in self.ticks.items() if pid != main) / TICK
        return {"environment_io_cpu_s": round(io_seconds, 2), "nydusd_cpu_s": round(nydusd_seconds, 2),
                "peak_nydusd": self.peak_daemons, "peak_nydusd_rss_mb": round(self.peak_rss_kb / 1024, 1)}


def store_metrics(args):
    if not args.store_url:
        return {}
    token = Path(args.token_file).read_text().strip()
    import urllib.request
    request = urllib.request.Request(args.store_url.rstrip("/") + "/v1/metrics",
                                     headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(request, timeout=30) as response:
        found = json.loads(response.read())
    counters = found.get("counters", found)
    return {key: counters[key] for key in ("requests", "bytes_served", "misses", "fills") if key in counters}


def cmd_arm(args):
    set_mode(args.mode)
    out = Path(args.out)
    result = json.loads(out.read_text()) if out.exists() else {}
    for mode in ("demand", "traced"):  # nydusd keeps no traces: "traced" is a second cold demand run.
        if mode in result:
            continue
        gate.Node().reset(clear_traces=mode == "demand")  # Restarts environment-io: sample after it.
        before_store, main, sampler = store_metrics(args), io_pid(), Sampler()
        sampler.start()
        partial = Path(f"{out}.{mode}")
        try:
            _one_mode(argparse.Namespace(images=args.images, run=f"{args.run}-{args.mode[0]}{args.n}", n=args.n,
                                         out=str(partial)), mode)
        finally:
            sampler.stop.set()
            sampler.join()
        record = json.loads(partial.read_text())[mode]
        record["host"] = sampler.result(main)
        after_store = store_metrics(args)
        record["store"] = {key: after_store[key] - before_store.get(key, 0) for key in after_store}
        result[mode] = record
        out.write_text(json.dumps(result, indent=1))
        print(json.dumps({mode: {"wall": record["wall"], **record["host"], **record["store"]}}), flush=True)
    set_mode("python")
    return 0


def _one_mode(bench, mode):
    """gate.cmd_bench's burst for one mode, on a node already reset."""
    node, images = gate.Node(), json.loads(Path(bench.images).read_text())
    before, started = node.environment_io(), time.monotonic()

    def sandbox(item):
        index, image = item
        name = f"m1-{bench.run}-b{index}-{mode[0]}"
        record = {"index": index, "create": round(node.create(name, image), 3)}
        record["import_sys"] = node.run(name, gate.COMMANDS["import_sys"])
        record["pip_version"] = node.run(name, gate.COMMANDS["pip_version"])
        record["done"] = round(time.monotonic() - started, 3)
        return record

    with ThreadPoolExecutor(len(images)) as pool:
        rows = list(pool.map(sandbox, list(images.items())[:bench.n]))
    wall = max(row["done"] for row in rows)
    for row in rows:
        node.delete(f"m1-{bench.run}-b{row['index']}-{mode[0]}")
    Path(bench.out).write_text(json.dumps({mode: {"wall": wall, "n": len(rows), "rows": rows,
                                                  "environment_io": gate.delta(node.environment_io(), before)}}))


def run_output(node, sandbox_id, command, timeout=120):
    """(exit code, last 600 characters of stdout+stderr)."""
    started = time.monotonic()
    reply = node.call("POST", f"/v1/sandboxes/{sandbox_id}/exec?initial_wait_seconds=0.05",
                      {"command": command, "env": {}, "working_dir": None, "stdin": False, "tty": False})
    session, after, output = reply["session"], 0, []
    while session.get("exit_code") is None and time.monotonic() - started < timeout:
        events = node.call("GET", f"/v1/exec/{session['id']}/events?after={after}&limit=1000&wait_seconds=5")
        session = events.get("session") or session
        for event in events.get("events", []):
            after = max(after, event.get("sequence", 0))
            if event.get("stream") in ("stdout", "stderr"):
                output.append(event.get("data") or "")
    return session.get("exit_code"), "".join(output)[-600:], round(time.monotonic() - started, 2)


# Reads a large part of the image's files, so the read lasts past the kill.
READ_TREE = ["sh", "-c", "find /usr /opt /lib -xdev -type f 2>/dev/null | head -20000 | "
             "xargs -d '\\n' cat > /dev/null; echo read-rc=$?"]


def cmd_kill(args):
    set_mode("nydusd")
    node, images = gate.Node(), json.loads(Path(args.images).read_text())
    node.reset(clear_traces=True)
    chosen = list(images.items())[:args.n]
    names = {index: f"m1-{args.run}-k{index}" for index, _ in chosen}
    for index, image in chosen:  # Serial creates: every image attached before the kill.
        node.create(names[index], image)
    # nydusd pid -> component: its config lives in <root>/nydusd/<component hex>-nbdN/.
    daemons = {}
    for pid in pids("nydusd"):
        config = re.search(r"--config (\S+)", Path(f"/proc/{pid}/cmdline").read_text().replace("\0", " "))
        daemons[pid] = Path(config.group(1)).parent.name.split("-")[0] if config else "?"
    victims = random.Random(args.seed).sample(sorted(daemons), min(args.victims, len(daemons)))
    rows, started = {}, time.monotonic()
    with ThreadPoolExecutor(len(chosen)) as pool:
        futures = {index: pool.submit(run_output, node, names[index], READ_TREE, 300) for index, _ in chosen}
        time.sleep(args.kill_after)
        for pid in victims:
            os.kill(pid, 9)
        killed_at = round(time.monotonic() - started, 2)
        for index, future in futures.items():
            rc, output, wall = future.result()
            rows[index] = {"rc": rc, "wall": wall, "eio": "Input/output error" in output, "tail": output[-300:]}
    # Which sandboxes used a killed daemon: their rootfs mounts name the component.
    mounts = Path("/proc/mounts").read_text()
    killed_components = sorted({daemons[pid] for pid in victims})
    after = {}
    for index, _ in chosen:
        rc, output, wall = run_output(node, names[index], ["sh", "-c", "cat /usr/bin/* > /dev/null; echo again-rc=$?"])
        after[index] = {"rc": rc, "eio": "Input/output error" in output, "tail": output[-200:]}
    # A new create of every image, while the dead mounts are still in use.
    recreate = {}
    for index, image in chosen[:args.recreate]:
        name = f"{names[index]}-again"
        try:
            node.create(name, image)
            rc, output, _ = run_output(node, name, ["sh", "-c", "python3 -c 'import sys' && echo ok"])
            recreate[index] = {"created": True, "rc": rc, "tail": output[-200:]}
            node.delete(name)
        except Exception as exc:  # noqa: BLE001 - the refusal is the measurement
            recreate[index] = {"created": False, "error": str(exc)[:300]}
    journal = subprocess.run(["journalctl", "-u", "ucloud-environment-io.service", "--since", "-10min", "-o", "cat"],
                             capture_output=True, text=True).stdout.splitlines()
    for index, _ in chosen:
        try:
            node.delete(names[index])
        except Exception as exc:  # noqa: BLE001
            print("delete", index, exc, file=sys.stderr)
    result = {"n": len(chosen), "daemons": len(daemons), "victims": len(victims), "killed_at": killed_at,
              "killed_components": killed_components, "rows": rows, "after": after, "recreate": recreate,
              "backend_log": [line for line in journal if re.search(r"(?i)fence|drain|nydusd|nbd|error", line)][-40:],
              "mounts_mentioning_killed": sum(component in mounts for component in killed_components)}
    Path(args.out).write_text(json.dumps(result, indent=1))
    node.reset(clear_traces=True)
    set_mode("python")
    print(json.dumps({key: result[key] for key in ("n", "daemons", "victims", "killed_at")}))
    return 0


SPLIT_ENV = "PYTHONDONTWRITEBYTECODE=1"  # No run writes .pyc that a later run would read.


def timed(command):
    """A shell command that prints its own wall time in ns (T=...)."""
    return ["sh", "-c", f's=$(date +%s%N); {SPLIT_ENV} {command} >/dev/null 2>&1; e=$(date +%s%N); '
                        'echo "T=$((e-s)) rc=$?"']


SPLIT_COMMANDS = {"true": "true",
                  "import_sys": "python3 -c 'import sys'",
                  "pip_version": "python3 -m pip --version"}


def parse_t(output):
    found = re.search(r"T=(\d+)", output)
    return round(int(found.group(1)) / 1e9, 4) if found else None


def runsc_view(sandbox_id):
    """The sandbox's rootfs, its runsc flags and the sentry's host file fds
    (with directfs the sentry opens rootfs files itself; without it, it holds
    gofer sockets)."""
    found = {}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            argv = (proc / "cmdline").read_bytes().decode(errors="replace").split("\0")
        except OSError:
            continue
        bundle = next((arg.split("=", 1)[1] if "=" in arg else argv[position + 1]
                       for position, arg in enumerate(argv) if arg.startswith("--bundle")), "")
        if argv[0] in ("runsc-gofer", "runsc-sandbox") and f"/{sandbox_id}." in bundle:
            found[argv[0]] = (int(proc.name), argv, bundle)
    if "runsc-sandbox" not in found:
        return {}
    pid, argv, bundle = found["runsc-sandbox"]
    config = json.loads((Path(bundle) / "config.json").read_text())
    rootfs = str(Path(bundle) / config["root"]["path"])
    kinds = {"file": 0, "socket": 0, "other": 0}
    for fd in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        kind = ("socket" if target.startswith("socket:") else
                "file" if target.startswith("/") and not target.startswith(("/dev/", "/memfd", "/proc/")) else "other")
        kinds[kind] += 1
    flags = [arg for arg in argv if arg.startswith("--") and any(
        word in arg for word in ("directfs", "overlay", "gofer-mount", "file-access", "host-uds", "platform"))]
    return {"rootfs": rootfs, "sentry_pid": pid, "flags": flags, "sentry_fds": kinds,
            "gofer_flags": [arg for arg in found.get("runsc-gofer", (0, [], ""))[1] if "mount" in arg]}


def cmd_split(args):
    """Fetch vs gVisor filesystem path: cold, then warm in the sandbox, then
    warm natively (chroot into the same host rootfs), per image."""
    set_mode("python")
    node, images = gate.Node(), json.loads(Path(args.images).read_text())
    node.reset(clear_traces=True)
    result = {}
    for index, image in list(images.items())[:args.n]:
        name = f"m1-{args.run}-x{index}"
        node.create(name, image)
        row = {"view": runsc_view(name), "sandbox": {}, "native": {}}
        for key, command in SPLIT_COMMANDS.items():
            samples = []
            for _ in range(1 + args.warm):  # The first run is cold.
                rc, output, wall = run_output(node, name, timed(command))
                samples.append({"t": parse_t(output), "exec_wall": wall})
            row["sandbox"][key] = {"cold": samples[0], "warm": samples[1:]}
        rootfs = row["view"].get("rootfs")
        if rootfs and Path(rootfs).is_dir():
            for key, command in SPLIT_COMMANDS.items():
                times = []
                for _ in range(1 + args.warm):
                    started = time.monotonic()
                    done = subprocess.run(["chroot", rootfs, "/bin/sh", "-c", f"{SPLIT_ENV} {command}"],
                                          capture_output=True, env={"PATH": "/usr/local/bin:/usr/bin:/bin"})
                    times.append({"t": round(time.monotonic() - started, 4), "rc": done.returncode})
                row["native"][key] = {"first": times[0], "warm": times[1:]}
        node.delete(name)
        result[index] = row
        Path(args.out).write_text(json.dumps(result, indent=1))
        print(json.dumps({index: {key: [sample["t"] for sample in value["warm"]]
                                  for key, value in row["sandbox"].items()}}), flush=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    arm = commands.add_parser("arm")
    arm.add_argument("--mode", choices=("python", "nydusd"), required=True)
    kill = commands.add_parser("kill")
    split = commands.add_parser("split")
    split.add_argument("--warm", type=int, default=3)
    for command in (arm, kill, split):
        command.add_argument("--images", required=True)
        command.add_argument("--n", type=int, required=True)
        command.add_argument("--run", required=True)
        command.add_argument("--out", required=True)
    arm.add_argument("--store-url", default="")
    arm.add_argument("--token-file", default="")
    kill.add_argument("--victims", type=int, default=8)
    kill.add_argument("--kill-after", type=float, default=3.0)
    kill.add_argument("--recreate", type=int, default=64)
    kill.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    return {"arm": cmd_arm, "kill": cmd_kill, "split": cmd_split}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
