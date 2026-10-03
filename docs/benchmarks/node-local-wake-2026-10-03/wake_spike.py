"""Node-local model waits, spike (docs/benchmarks/node-local-wake-2026-10-03).

Two questions, on one disposable, unregistered worker, as root:
1. Does a paused gVisor sandbox keep an inbound answer until a thaw that the
   node triggers from the answer's first packet, without drops or resends?
2. Can the node tell "a model call is outstanding" at the TCP level, through
   TLS, without the relay telling it?

``relay`` is a TLS HTTP/1.1 server on this host that holds each POST for its
think time, then answers. ``arm`` creates managed sandboxes whose agent
computes, then makes a blocking HTTPS keep-alive call to it, in a loop. A
daemon thread watches every sandbox's host-side veth (one raw packet socket)
and cgroup CPU, and in the pausing arms runs ``runsc pause`` when the last
relay payload went out and the sandbox has been idle, and ``runsc resume`` on
the first inbound payload. It uses runsc directly, never the node agent's
pause path, and resumes everything before reading results.
"""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import random
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
from urllib import error, request

from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint

OUT = Path("/var/lib/wake-spike")
JOB = "agent"
ETH_P_ALL, PACKET_OUTGOING = 0x0003, 4

AGENT = r'''
import hashlib, http.client, json, os, ssl, sys, threading, time
host, port, name, compute_ms, ticker, plans = (sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]),
                                               float(sys.argv[5]), json.loads(sys.argv[6]))
plain = len(sys.argv) > 7 and sys.argv[7] == "plain"
lock = threading.Lock()
def out(row):
    with lock:
        sys.stdout.write(json.dumps(row) + "\n")
        sys.stdout.flush()
def tick():  # Background work during the waits: how late does a pause make it?
    expected = time.time() + ticker
    while True:
        time.sleep(max(0.0, expected - time.time()))
        now = time.time()
        out({"tick": now, "late": round(now - expected, 4)})
        hashlib.sha256(b"x" * 100000).digest()
        expected += ticker
def caller(lane, plan):
    if plain:
        connection = http.client.HTTPConnection(host, port, timeout=600)
    else:
        context = ssl.create_default_context()
        context.check_hostname, context.verify_mode = False, ssl.CERT_NONE
        connection = http.client.HTTPSConnection(host, port, context=context, timeout=600)
    for index, (think, size) in enumerate(plan):
        end = time.process_time() + compute_ms / 1000
        while time.process_time() < end:
            hashlib.sha256(os.urandom(4096)).digest()
        body = json.dumps({"id": f"{name}:{lane}:{index}", "think": think, "size": size}).encode()
        sent = time.time()
        connection.request("POST", "/v1/chat/completions", body, {"Content-Type": "application/json"})
        response = connection.getresponse()
        data = response.read()
        out({"lane": lane, "call": index, "sent": sent, "recv": time.time(), "bytes": len(data),
             "status": response.status})
if ticker:
    threading.Thread(target=tick, daemon=True).start()
lanes = [threading.Thread(target=caller, args=(lane, plan)) for lane, plan in enumerate(plans)]
for thread in lanes:
    thread.start()
for thread in lanes:
    thread.join()
out({"done": time.time()})
'''


def log_line(path, row):
    with open(path, "a") as stream:
        stream.write(json.dumps(row) + "\n")


# --- The fake relay ---

def relay(args):
    OUT.mkdir(parents=True, exist_ok=True)
    key, cert = OUT / "relay.key", OUT / "relay.crt"
    if not args.plain and not cert.exists():
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=wake-spike",
                        "-days", "2", "-keyout", str(key), "-out", str(cert)], check=True, capture_output=True)
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):  # noqa: N802
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received = time.time()
            time.sleep(body["think"])
            payload = b'{"choices": []}' + b" " * max(0, body["size"] - 15)
            answered = time.time()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()
            log_line(OUT / "relay.jsonl", {"id": body["id"], "client": self.client_address[0], "received": received,
                                           "answered": answered, "written": time.time(), "size": len(payload)})

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer((args.listen, args.port), Handler)
    server.daemon_threads = True
    if not args.plain:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(cert), str(key))
        server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


# --- The node side: discovery, packet watch, pause and thaw ---

def sentries():
    """{cgroup leaf: (runsc, root, container id, sentry pid)} of every running sandbox.

    The Sentry runs its own binary (gvisor_sentry); runsc is the gofer's, under the same --root.
    """
    found, binaries = {}, {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            argv = [item.decode(errors="replace") for item in
                    Path(f"/proc/{entry}/cmdline").read_bytes().split(b"\0") if item]
            cgroup = Path(f"/proc/{entry}/cgroup").read_text().strip().rsplit("/", 1)[-1]
            exe = os.readlink(f"/proc/{entry}/exe")
        except OSError:
            continue
        root = next((item for item in argv if item.startswith("--root=")), None)
        if root is None:
            continue
        if argv[0] == "runsc-gofer":
            binaries[root] = exe
        elif argv[0] == "runsc-sandbox" and "boot" in argv:
            found[cgroup] = (root, argv[-1], int(entry))
    return {leaf: (binaries[root], root, container, pid) for leaf, (root, container, pid) in found.items()}


def host_veth(pid):
    """The host-side peer of the sandbox network namespace's veth."""
    links = json.loads(subprocess.run(["nsenter", "-t", str(pid), "-n", "ip", "-j", "link"], check=True,
                                      capture_output=True, text=True).stdout)
    peer = next(link["link_index"] for link in links if link.get("link_index"))
    host = json.loads(subprocess.run(["ip", "-j", "link"], check=True, capture_output=True, text=True).stdout)
    return next(link["ifname"] for link in host if link["ifindex"] == peer)


class Watched:
    def __init__(self, sandbox_id, runsc, root, container, pid, veth, cgroup):
        self.id, self.runsc_path, self.root, self.container, self.pid = sandbox_id, runsc, root, container, pid
        self.veth, self.cpu = veth, Path("/sys/fs/cgroup/ucloud-sandboxes") / cgroup / "cpu.stat"
        self.last_out = self.last_in = 0.0
        self.paused, self.lock, self.usage = False, threading.Lock(), []

    def usage_usec(self):
        for line in self.cpu.read_text().splitlines():
            if line.startswith("usage_usec "):
                return int(line.split()[1])
        return 0

    def runsc(self, verb):
        return subprocess.run([self.runsc_path, self.root, verb, self.container], capture_output=True, text=True)


class Daemon:
    """mode: observe (never pause), local (thaw on the first inbound payload),
    delayed (thaw ``delay`` s after it: does the paused stack keep the answer?)."""

    def __init__(self, watched, relay_port, mode, events, *, settle=0.05, idle_window=0.05, idle_usec=2000,
                 delay=2.0):
        self.by_veth = {item.veth: item for item in watched}
        self.relay_port, self.mode, self.events = relay_port, mode, events
        self.settle, self.idle_window, self.idle_usec, self.delay = settle, idle_window, idle_usec, delay
        self.stop = threading.Event()
        self.sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETH_P_ALL))
        self.sock.settimeout(0.2)

    def event(self, item, kind, **fields):
        self.events.append({"t": time.time(), "id": item.id, "event": kind, **fields})

    def sniff(self):
        while not self.stop.is_set():
            try:
                frame, address = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            item = self.by_veth.get(address[0])
            if item is None or len(frame) < 34 or frame[12:14] != b"\x08\x00" or frame[23] != 6:
                continue
            ihl = (frame[14] & 0x0F) * 4
            total = struct.unpack_from("!H", frame, 16)[0]
            tcp = 14 + ihl
            source, destination = struct.unpack_from("!HH", frame, tcp)
            flags = frame[tcp + 13]
            payload = total - ihl - (frame[tcp + 12] >> 4) * 4
            if self.relay_port not in (source, destination):
                continue
            now = time.time()
            inbound = address[2] == PACKET_OUTGOING  # Host to sandbox.
            if inbound:
                if payload > 0 or flags & 0x05:  # Data, FIN or RST; a bare ACK need not wake anyone.
                    item.last_in = now
                    if item.paused:
                        self.event(item, "wake_packet", bytes=payload)
                        threading.Thread(target=self.thaw, args=(item, now), daemon=True).start()
            elif payload > 0:
                item.last_out = now

    def thaw(self, item, packet_at):
        if self.mode == "delayed":
            time.sleep(max(0.0, packet_at + self.delay - time.time()))
        with item.lock:
            if not item.paused:
                return
            begin = time.time()
            result = item.runsc("resume")
            item.paused = False
            self.event(item, "thawed", runsc_ms=round((time.time() - begin) * 1000, 2),
                       since_packet_ms=round((time.time() - packet_at) * 1000, 2), rc=result.returncode)

    def policy(self):
        while not self.stop.is_set():
            now = time.time()
            for item in self.by_veth.values():
                item.usage.append((now, item.usage_usec()))
                while item.usage and item.usage[0][0] < now - self.idle_window - 0.05:
                    item.usage.pop(0)
                if self.mode == "observe" or item.paused:
                    continue
                awaiting = item.last_out > item.last_in and now - item.last_out >= self.settle
                window = [usec for at, usec in item.usage if at >= now - self.idle_window]
                idle = len(window) >= 2 and window[-1] - window[0] <= self.idle_usec
                if awaiting and idle:
                    with item.lock:
                        if item.paused or item.last_in > item.last_out:
                            continue
                        begin = time.time()
                        result = item.runsc("pause")
                        item.paused = result.returncode == 0
                        self.event(item, "paused", runsc_ms=round((time.time() - begin) * 1000, 2),
                                   rc=result.returncode, waited_ms=round((begin - item.last_out) * 1000, 1),
                                   **({} if item.paused else {"stderr": result.stderr[-300:],
                                                              "argv": [item.runsc_path, item.root, item.container]}))
                        if not item.paused:  # Once per wait, not every tick.
                            item.last_in = time.time()
                    if item.last_in > begin:  # The answer raced the pause: thaw now.
                        threading.Thread(target=self.thaw, args=(item, item.last_in), daemon=True).start()
            time.sleep(0.005)

    def close(self):
        self.stop.set()
        for item in self.by_veth.values():
            with item.lock:
                if item.paused:
                    item.runsc("resume")
                    item.paused = False
                    self.event(item, "thawed_at_close")


class MarkerWatcher:
    """mode node: the node pauses and thaws by itself (sandbox.direct_local_model_waits);
    its pause tier's markers record when (written before ``runsc pause``, removed after resume)."""

    def __init__(self, directory, ids, events):
        self.directory, self.ids, self.events = Path(directory), set(ids), events
        self.stop, self.paused = threading.Event(), set()

    def run(self):
        while not self.stop.is_set():
            try:
                names = {name.rpartition(".sandbox-")[0] for name in os.listdir(self.directory)}
            except FileNotFoundError:
                names = set()
            now, current = time.time(), names & self.ids
            for sandbox_id in current - self.paused:
                self.events.append({"t": now, "id": sandbox_id, "event": "paused", "rc": 0})
            for sandbox_id in self.paused - current:
                self.events.append({"t": now, "id": sandbox_id, "event": "thawed"})
            self.paused = current
            time.sleep(0.002)


# --- One arm ---

class NodeApi:
    def __init__(self, url, token):
        self.url, self.token = url.rstrip("/"), token

    def call(self, path, *, method="GET", payload=None, headers=None, timeout=300):
        req = request.Request(self.url + path, method=method,
                              data=None if payload is None else json.dumps(payload).encode(),
                              headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json",
                                       **(headers or {})})
        try:
            with request.urlopen(req, timeout=timeout) as response:
                return json.load(response)
        except error.HTTPError as exc:
            raise RuntimeError(f"{method} {path}: HTTP {exc.code}: {exc.read(2000).decode(errors='replace')}") \
                from exc


def nstat(*names):
    rows = json.loads(subprocess.run(["nstat", "-a", "-z", "-j"], capture_output=True, text=True).stdout or "{}")
    kernel = rows.get("kernel", rows)
    return {name: kernel.get(name, 0) for name in names}


def arm(args):
    api = NodeApi(args.node_url, Path(args.node_token_file).read_text().strip())
    rng = random.Random(args.seed)
    tag = f"wake-{args.name}-{os.urandom(3).hex()}"
    ids = [f"{tag}-{index}" for index in range(args.count)]
    sizes = [int(item) for item in args.sizes.split(",")]
    plans = {sandbox_id: [[[round(rng.uniform(args.think_min, args.think_max), 2), sizes[index % len(sizes)]]
                           for index in range(args.cycles)] for _ in range(args.lanes)] for sandbox_id in ids}
    evidence = {"arm": args.name, "mode": args.mode, "args": vars(args), "ids": ids, "plans": plans,
                "events": [], "errors": []}

    def create(sandbox_id):
        payload = {"id": sandbox_id, "image": args.image, "cpus": args.cpus, "memory_mb": args.memory_mb,
                   "disk_mb": 4096, "managed_process": True, "parkable": True, "ttl_seconds": 3600,
                   "security": {"user": "0:0", "init": False}, "filesystem": {"workspace_storage": "image"}}
        payload["_ucloud_operation"] = {"kind": "create", "generation": 1, "operation_id": "create-" + sandbox_id,
                                        "spec_hash": sandbox_spec_fingerprint(SandboxSpec.from_dict(payload))}
        return api.call("/v1/sandboxes", method="POST", payload=payload)["sandbox"]

    with ThreadPoolExecutor(8) as pool:
        records = dict(zip(ids, pool.map(create, ids)))
    found = sentries()
    watched = []
    for sandbox_id, record in records.items() if args.mode != "node" else ():
        leaf = hashlib.sha256(f"{sandbox_id}:{record['generation']}".encode()).hexdigest()
        runsc, root, container, pid = found[leaf]
        watched.append(Watched(sandbox_id, runsc, root, container, pid, host_veth(pid), leaf))
    if args.mode == "node":
        daemon = MarkerWatcher(args.marker_dir, ids, evidence["events"])
        daemon.close = daemon.stop.set
        threads = [threading.Thread(target=daemon.run, daemon=True)]
    else:
        daemon = Daemon(watched, args.relay_port, args.mode, evidence["events"], delay=args.delay)
        threads = [threading.Thread(target=daemon.sniff, daemon=True),
                   threading.Thread(target=daemon.policy, daemon=True)]
    for thread in threads:
        thread.start()
    before = nstat("TcpRetransSegs", "TcpExtTCPLostRetransmit", "TcpExtTCPTimeouts")
    started = time.time()
    for sandbox_id in ids:
        argv = ["python3", "-c", AGENT, args.relay_host, str(args.relay_port), sandbox_id, str(args.compute_ms),
                str(args.ticker), json.dumps(plans[sandbox_id]), "plain" if args.plain else "tls"]
        api.call(f"/v1/sandboxes/{sandbox_id}/jobs", method="POST", payload={"job_id": JOB, "argv": argv})
    # Never read the job while the daemon may hold a pause it owns: wait on the relay's log.
    expected = args.count * args.lanes * args.cycles
    deadline = started + args.cycles * (args.think_max + args.compute_ms / 1000 + args.delay + 5) + 120
    relay_log = Path(args.relay_log) if args.relay_log else OUT / "relay.jsonl"
    while time.time() < deadline:
        if args.mode == "node":  # The node's own pauses keep job reads safe; the relay's log is remote.
            states = [api.call(f"/v1/sandboxes/{sandbox_id}/jobs/{JOB}")["job"]["state"] for sandbox_id in ids]
            if all(state not in ("starting", "running") for state in states):
                break
            time.sleep(2)
            continue
        done = [row for row in read_jsonl(relay_log) if row["id"].split(":")[0] in records]
        if len(done) >= expected:
            break
        time.sleep(1)
    time.sleep(3)
    daemon.close()
    evidence["nstat"] = {key: value - before[key] for key, value in nstat(*before).items()}
    evidence["relay"] = [row for row in read_jsonl(relay_log) if row["id"].split(":")[0] in records]
    evidence["agents"] = {}
    for sandbox_id in ids:
        try:
            for _ in range(60):
                job = api.call(f"/v1/sandboxes/{sandbox_id}/jobs/{JOB}")["job"]
                if job["state"] not in ("starting", "running"):
                    break
                time.sleep(1)
            chunk = api.call(f"/v1/sandboxes/{sandbox_id}/jobs/{JOB}/logs/stdout")
            evidence["agents"][sandbox_id] = {"job": job, "rows": [
                json.loads(line) for line in base64.b64decode(chunk["data"]).decode().splitlines() if line]}
        except Exception as exc:  # noqa: BLE001 - recorded, the arm still cleans up
            evidence["errors"].append(f"{sandbox_id}: {exc}"[:400])
    for sandbox_id, record in records.items():
        try:
            api.call(f"/v1/sandboxes/{sandbox_id}", method="DELETE", headers={
                "X-UCloud-Sandbox-Generation": str(record["generation"]),
                "X-UCloud-Sandbox-Operation-Id": "delete-" + sandbox_id})
        except Exception as exc:  # noqa: BLE001
            evidence["errors"].append(f"delete {sandbox_id}: {exc}"[:400])
    evidence["summary"] = summarize(evidence)
    (OUT / f"{args.name}.json").write_text(json.dumps(evidence, indent=1))
    print(json.dumps({"arm": args.name, **evidence["summary"]}, indent=1))


def read_jsonl(path):
    try:
        return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    except FileNotFoundError:
        return []


def quantiles(values):
    values = sorted(values)
    if not values:
        return None
    pick = lambda q: round(values[min(len(values) - 1, int(q * len(values)))], 4)  # noqa: E731
    return {"n": len(values), "p50": pick(0.5), "p95": pick(0.95), "max": round(values[-1], 4)}


def summarize(evidence):
    """Answer latency (agent receive minus relay write), pauses per call, the
    share of each wait spent paused, and pauses that hit work instead of a wait."""
    relay = {row["id"]: row for row in evidence["relay"]}
    calls, latency, ticks = [], [], []
    for sandbox_id, agent in evidence["agents"].items():
        for row in agent["rows"]:
            if "call" in row:
                answer = relay.get(f"{sandbox_id}:{row['lane']}:{row['call']}")
                if answer:
                    latency.append(row["recv"] - answer["written"])
                    calls.append((sandbox_id, row, answer))
            elif "tick" in row:
                ticks.append(row["late"])
    events = evidence["events"]
    pauses = [event for event in events if event["event"] == "paused" and event["rc"] == 0]
    thaws = [event for event in events if event["event"] == "thawed"]
    paused_ms, wrong = 0.0, 0
    for sandbox_id, row, answer in calls:
        inside = [event for event in pauses if event["id"] == sandbox_id and row["sent"] <= event["t"] <= answer["written"]]
        if inside:
            paused_ms += (answer["written"] - inside[0]["t"]) * 1000
    for event in pauses:  # A pause outside every call's [sent, answer] froze work, not a wait.
        if not any(event["id"] == sandbox_id and row["sent"] <= event["t"] <= answer["written"]
                   for sandbox_id, row, answer in calls):
            wrong += 1
    think_ms = sum((answer["written"] - row["sent"]) * 1000 for _, row, answer in calls)
    return {
        "calls": len(calls), "expected": len(evidence["ids"]) * evidence["args"]["lanes"] * evidence["args"]["cycles"],
        "answer_latency_s": quantiles(latency), "pauses": len(pauses), "pauses_outside_a_wait": wrong,
        "paused_share_of_wait": round(paused_ms / think_ms, 4) if think_ms else None,
        "pause_runsc_ms": quantiles([event["runsc_ms"] for event in pauses if "runsc_ms" in event]),
        "pause_after_last_send_ms": quantiles([event["waited_ms"] for event in pauses if "waited_ms" in event]),
        "thaw_since_packet_ms": quantiles([event["since_packet_ms"] for event in thaws if "since_packet_ms" in event]),
        "thaw_runsc_ms": quantiles([event["runsc_ms"] for event in thaws if "runsc_ms" in event]),
        "tick_late_s": quantiles(ticks), "nstat": evidence["nstat"], "errors": len(evidence["errors"]),
    }


def merge(args):
    """Summarize an arm's evidence with a relay log fetched from the relay host."""
    evidence = json.loads(Path(args.evidence).read_text())
    evidence["relay"] = [row for row in read_jsonl(args.relay_log) if row["id"].split(":")[0] in set(evidence["ids"])]
    evidence["summary"] = summarize(evidence)
    Path(args.evidence).write_text(json.dumps(evidence, indent=1))
    print(json.dumps({"arm": evidence["arm"], **evidence["summary"]}, indent=1))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    summary = commands.add_parser("summarize")
    summary.add_argument("--evidence", required=True)
    summary.add_argument("--relay-log", required=True)
    serve = commands.add_parser("relay")
    serve.add_argument("--listen", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8443)
    serve.add_argument("--plain", action="store_true", help="HTTP/1.1 without TLS (the private relay path)")
    run = commands.add_parser("arm")
    run.add_argument("--name", required=True)
    run.add_argument("--mode", choices=("observe", "local", "delayed", "node"), required=True)
    run.add_argument("--plain", action="store_true", help="agents call the relay without TLS")
    run.add_argument("--marker-dir", default="/work/ucloud-sandboxes/state/direct-runtime/runsc/warden-paused",
                     help="mode node: the pause tier's marker directory")
    run.add_argument("--relay-log", default="", help="the relay's log when it runs on another host")
    run.add_argument("--node-url", required=True)
    run.add_argument("--node-token-file", required=True)
    run.add_argument("--image", required=True)
    run.add_argument("--relay-host", required=True)
    run.add_argument("--relay-port", type=int, default=8443)
    run.add_argument("--count", type=int, default=8)
    run.add_argument("--lanes", type=int, default=1, help="concurrent model calls per agent")
    run.add_argument("--cycles", type=int, default=8)
    run.add_argument("--think-min", type=float, default=2.0)
    run.add_argument("--think-max", type=float, default=5.0)
    run.add_argument("--compute-ms", type=int, default=200)
    run.add_argument("--sizes", default="2048", help="answer sizes in bytes, cycled per call")
    run.add_argument("--ticker", type=float, default=0.0, help="background tick interval in s (0: none)")
    run.add_argument("--delay", type=float, default=2.0, help="delayed mode: thaw this long after the packet")
    run.add_argument("--cpus", type=float, default=0.5)
    run.add_argument("--memory-mb", type=int, default=512)
    run.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)
    if args.command == "summarize":
        return merge(args)
    OUT.mkdir(parents=True, exist_ok=True)
    relay(args) if args.command == "relay" else arm(args)


if __name__ == "__main__":
    sys.exit(main())
