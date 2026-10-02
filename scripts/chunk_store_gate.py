#!/usr/bin/env python3
"""Run the chunk store's M1 gate (docs/chunk-store-design.md §9) on Hetzner.

On S10's 181-image sample: full-tree equality, stored bytes, cold first
commands with traces, a 20-way cold burst, crash injection at every write-path
step and the unpack rollback, each with a pass/fail in the report.

Every production action is one plain ``scripts/hetzner_prod/gw '...'``,
``scripts/hetzner_prod/gscp ...`` or ``python3 scripts/hetzner_prod/hz.py ...``
invocation, run from the repository root as an argv list without a local
shell, so the session's permission rules match it; ``--dry-run`` prints each
one instead. Commands for the gate's own hosts run through the gateway's ssh.
The host-side half is scripts/chunk_store_gate_remote.py.

Phases (``--phase``; ``all`` runs them in order and stops at a failure):

  provision  store node and converter host (CCX, worker snapshot), private IPs
  configure  runtime, gate registry, store node service and index, configs
  convert    mirror the sample read-only, convert it, tally stored bytes
  crash      kill -9 at every write-path step, rerun, check convergence
  workers    canary workers from the release bundle, cold commands, burst
  rollback   unpack 10 images, rebuild them with today's builder
  report     JSON and docs/benchmarks/m1-gate-<date>/README.md
  teardown   drain, delete S3 objects, VMs, staging and known_hosts entries

Each phase records its finished steps in build/m1-gate/<run>/state.json and
skips them when rerun. Every created resource is recorded before it exists,
so ``--phase teardown`` cleans up after a crash at any point. If gateway SSH
fails the run stops at once (exit 3), without looking for other credentials.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import datetime as dt
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
GW = "scripts/hetzner_prod/gw"
GSCP = "scripts/hetzner_prod/gscp"
HZ = "scripts/hetzner_prod/hz.py"
GATEWAY = "root@77.42.92.27"
LEDGER = REPO / "build" / "hetzner-prod" / "resources.json"  # hz.py's ledger, read only here.
SAMPLE = "docs/benchmarks/nydus-spike-2026-10-02/raw/sample.json"
REMOTE = "scripts/chunk_store_gate_remote.py"
PHASES = ("provision", "configure", "convert", "crash", "workers", "rollback", "report", "teardown")
NEEDS = {"configure": "provision", "convert": "configure", "crash": "convert", "workers": "convert",
         "rollback": "convert"}
# Write-path steps (ucloud_sandboxes/chunk_convert.py STEPS; a test keeps them equal).
STEPS = ("layer_converted", "pack_put", "layer_bootstrap_put", "layer_committed", "image_merged",
         "metadata_put", "component_signed", "verified", "component_published", "registered", "root_published")
RESERVED = (ipaddress.ip_address("10.42.0.40"), ipaddress.ip_address("10.42.0.47"))  # Spike and snapshot sources.
RUN_PREFIX = "spike/m1/"
HOST = "/opt/m1-gate"  # On the gate's hosts.
OUT = "/var/lib/m1-gate/out"
UNIT = "m1-gate"
ATTACH_TAG = "m1-rafs"
# S10's 7 images (sample indices) and their cold baselines in seconds: S10's
# `import sys` and `git status` (256 KiB rows where S10 has them, as S12 used)
# and S12's local `pip --version`. Without a pip baseline, S11's 2.5 s target.
S10_BASELINES = {0: {"import_sys": 0.46, "git_status": 0.25, "pip_version": 1.79},
                 60: {"import_sys": 0.43, "git_status": 0.31, "pip_version": 1.79},
                 63: {"import_sys": 0.41, "git_status": 0.50, "pip_version": 1.87},
                 72: {"import_sys": 0.50, "git_status": 0.52, "pip_version": None},
                 90: {"import_sys": 0.36, "git_status": 0.26, "pip_version": None},
                 130: {"import_sys": 0.40, "git_status": 0.26, "pip_version": None},
                 170: {"import_sys": 0.40, "git_status": 0.26, "pip_version": None}}
PIP_LIMIT_SECONDS = 2.5
COLD_RATIO = 1.3
# S12's burst order (scripts/burst-images.json); the burst takes the first 20.
BURST_IMAGES = (4, 15, 177, 50, 173, 54, 37, 87, 23, 81, 170, 14, 13, 6, 83, 36, 17, 46, 19, 176)
BURST_LIMIT_SECONDS = 5.5
ROLLBACK_IMAGES = (0, 60, 63, 72, 90, 130, 170, 4, 15, 177)
STORED_BYTES_TARGET, STORED_BYTES_TOLERANCE = 17.5e9, 0.05
SAMPLE_SIZE = 181
# Source-config disk overrides for a canary smaller than the CCX63 the live
# config is sized for (docs/rollout-0.8.0.md, snapshot 438664008).
WORKER_OVERRIDES = {"sandbox.docker_quota_image_gb": 64, "sandbox.direct_disk_headroom_mb": 24576,
                    "immutable_environments.cache_bytes": 8 * 1024 ** 3}
# Approximate hel1 list prices, EUR per started hour; verify before quoting.
PRICE_EUR_PER_HOUR = {"ccx13": 0.020, "ccx23": 0.039, "ccx33": 0.077, "ccx43": 0.153, "ccx53": 0.306,
                      "ccx63": 0.459}
NYDUS_URL = "https://github.com/dragonflyoss/nydus/releases/download/v2.4.5/nydus-static-v2.4.5-linux-amd64.tgz"
DISTRIBUTION_URL = "https://github.com/distribution/distribution/releases/download/v3.1.2/registry_3.1.2_linux_amd64.tar.gz"
_SECRET = re.compile(r"(X-Amz-Signature=|X-Amz-Credential=|Bearer |SECRET[A-Z_]*=|ACCESS_KEY[A-Z_]*=|API_KEY=)\S+")


class GateError(RuntimeError):
    pass


class GatewayUnreachable(GateError):
    pass


def redact(text):
    return _SECRET.sub(lambda match: match.group(1) + "<redacted>", text or "")


# --- The plain-helper command forms ---

def gw(remote):
    """One ``scripts/hetzner_prod/gw '<remote command>'`` invocation."""
    return [GW, remote]


def gscp(*paths):
    return [GSCP, *paths]


def hz(*args):
    return ["python3", HZ, *map(str, args)]


def ssh_options(key):
    return ["-i", key, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30"]


def on_host(ip, command, key):
    """The gateway's ssh to a gate host; ``command`` runs in the host's shell."""
    return shlex.join(["ssh", *ssh_options(key), f"root@{ip}", command])


def scp_to_host(sources, ip, destination, key):
    return shlex.join(["scp", "-q", *ssh_options(key), *sources, f"root@{ip}:{destination}"])


def scp_from_host(ip, source, destination, key):
    return shlex.join(["scp", "-q", *ssh_options(key), f"root@{ip}:{source}", destination])


def helper(*args, python=f"{HOST}/py"):
    return shlex.join([python, f"{HOST}/chunk_store_gate_remote.py", *map(str, args)])


def detached(unit, out, command):
    """Start ``command`` as a transient unit unless it already finished or
    runs; prints done, running or started."""
    unit, out = shlex.quote(unit), shlex.quote(out)
    return (f"mkdir -p {OUT}; if grep -qs '\"status\": 0' {out}.done; then echo done; "
            f"elif systemctl is-active --quiet {unit}; then echo running; "
            f"else rm -f {out}.done; systemd-run --quiet --collect --unit {unit} {command} && echo started; fi")


def poll(unit, out):
    unit, out = shlex.quote(unit), shlex.quote(out)
    return (f"cat {out}.done 2>/dev/null || {{ systemctl is-active --quiet {unit} "
            f"&& echo '{{\"status\": \"running\"}}' || echo '{{\"status\": \"lost\"}}'; }}")


def last_json(text, default=None):
    for line in reversed((text or "").strip().splitlines()):
        try:
            return json.loads(line)
        except ValueError:
            continue
    return default


def allocate_ips(spec, count, used=()):
    """``count`` private addresses from ``first-last``, never 10.42.0.40-.47."""
    first, _, last = spec.partition("-")
    start, end = ipaddress.ip_address(first), ipaddress.ip_address(last or first)
    if RESERVED[0] <= end and start <= RESERVED[1]:
        raise GateError("the IP range overlaps 10.42.0.40-10.42.0.47 (spike and source addresses)")
    if start not in ipaddress.ip_network("10.42.0.0/24") or end not in ipaddress.ip_network("10.42.0.0/24") \
            or int(start) <= int(ipaddress.ip_address("10.42.0.2")):
        raise GateError("the IP range must lie inside 10.42.0.3-10.42.0.254")
    free = [str(ipaddress.ip_address(value)) for value in range(int(start), int(end) + 1)
            if str(ipaddress.ip_address(value)) not in set(used)]
    if len(free) < count:
        raise GateError(f"the IP range has {len(free)} free addresses; the run needs {count}")
    return free[:count]


# --- The store node: the one coupling to its service ---

@dataclass(frozen=True)
class StoreNode:
    url: str               # Index (and, in Phase B, chunk) URL for builders and workers.
    read_token_path: str   # On the store node; the service creates it.
    write_token_path: str  # On the store node; the service creates it.
    index_database: str    # On the store node; read-only tallies.
    start_command: str     # Idempotent host command that starts the service.


def store_node_adapter(block, *, store_ip, store_url="", read_token_file="", write_token_file="",
                       start_command="", config_path="/etc/m1-gate/deployment.json",
                       env_path="/etc/m1-gate/s3.env"):
    """Map the store service's config interface onto this run.

    Defaults run today's ``serve-chunk-index`` from the release with the run's
    config copy. The parallel store-node service plugs in by its URL, its
    token paths and its start command; nothing else here knows about it.
    """
    for name in ("index_listen", "index_database", "read_token_file", "write_token_file",
                 "access_key_id_env", "secret_access_key_env", "bucket"):
        if not isinstance(block.get(name), str) or not block[name]:
            raise GateError(f"the chunk_store block needs {name}")
    port = block["index_listen"].rsplit(":", 1)[1]
    start = start_command or (
        f"systemctl is-active --quiet {UNIT}-store || systemd-run --quiet --unit {UNIT}-store "
        f"-p Restart=on-failure -p EnvironmentFile={env_path} {HOST}/ucs serve-chunk-index --config {config_path}")
    return StoreNode(url=(store_url or f"http://{store_ip}:{port}").rstrip("/"),
                     read_token_path=read_token_file or block["read_token_file"],
                     write_token_path=write_token_file or block["write_token_file"],
                     index_database=block["index_database"], start_command=start)


def role_blocks(block, store, prefix, staging):
    """The chunk_store block per config copy; only the copies get it."""
    common = {**block, "prefix": prefix, "index_url": store.url}
    return {"store": {**common, "read_token_file": store.read_token_path, "write_token_file": store.write_token_path},
            "converter": {**common, "read_token_file": "/etc/m1-gate/write.token",
                          "write_token_file": "/etc/m1-gate/write.token", "nydus_image": "/usr/local/bin/nydus-image"},
            # init-vm on the gateway renders the workers' read token from here.
            "canary": {**common, "read_token_file": f"{staging}/read.token",
                       "write_token_file": f"{staging}/read.token"}}


# --- Runners ---

class SubprocessRunner:
    """Runs argv lists from the repository root; never a shell."""

    def __init__(self, log_path=None):
        self.log_path = log_path

    def __call__(self, argv, timeout):
        started = time.monotonic()
        try:
            completed = subprocess.run(argv, cwd=REPO, capture_output=True, text=True, timeout=timeout)
            result = (completed.returncode, completed.stdout, completed.stderr)
        except subprocess.TimeoutExpired:
            result = (124, "", f"timed out after {timeout} s")
        if self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a") as stream:
                stream.write(json.dumps({"at": time.time(), "argv": [redact(item) for item in argv],
                                         "rc": result[0], "seconds": round(time.monotonic() - started, 1),
                                         "stdout": redact(result[1][-2000:]), "stderr": redact(result[2][-2000:])})
                             + "\n")
        return result


class DryRunRunner:
    """Prints each command exactly as it would run; every call succeeds."""

    def __init__(self, stream=sys.stdout):
        self.stream, self.commands = stream, []

    def __call__(self, argv, timeout):
        self.commands.append(argv)
        print(shlex.join(argv), file=self.stream)
        return 0, "", ""


# --- The gate ---

class Gate:
    def __init__(self, args, runner, *, sleep=time.sleep, now=time.time):
        self.args, self.runner, self.sleep, self.now = args, runner, sleep, now
        self.dry = args.dry_run
        self.root = Path(args.state_dir) / args.run_id
        self.state_path = self.root / "state.json"
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else self.fresh_state()
        self.staging = self.state["plan"]["staging"]
        self.key = args.gateway_node_key

    def fresh_state(self):
        run = self.args.run_id
        if not re.fullmatch(r"[a-z0-9]{6,20}", run):
            raise GateError("the run id must be 6-20 lowercase letters and digits")
        return {"run_id": run, "created": self.now(),
                "plan": {"prefix": f"{RUN_PREFIX}{run}", "staging": f"/var/tmp/m1-gate-{run}", "ips": {}},
                "resources": {"servers": {}, "staging": None, "known_hosts": [], "s3_prefix": None},
                "phases": {}, "results": {}}

    def save(self):
        if self.dry:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name("state.json.tmp")
        temporary.write_text(json.dumps(self.state, indent=1, sort_keys=True) + "\n")
        os.replace(temporary, self.state_path)

    # Commands.
    def run(self, argv, *, timeout=900, check=True):
        rc, stdout, stderr = self.runner(argv, timeout)
        if (rc == 255 and argv[0] == GW) or (rc != 0 and argv[0] == GSCP):
            probe, _, _ = self.runner(gw("true"), 60)
            if probe != 0:
                raise GatewayUnreachable("gateway SSH failed; stopping without trying other credentials")
        if check and rc != 0:
            raise GateError(f"{shlex.join([redact(item) for item in argv])[:300]} exited {rc}: "
                            f"{redact(stderr or stdout).strip()[-600:]}")
        return stdout

    def gw(self, remote, **options):
        return self.run(gw(remote), **options)

    def on(self, role, command, **options):
        return self.gw(on_host(self.ip(role), command, self.key), **options)

    def ip(self, role):
        ips = self.state["plan"]["ips"]
        if role not in ips and self.dry:  # A dry run of a later phase prints illustrative addresses.
            roles = ["store", "converter", *(f"w{n + 1}" for n in range(self.args.workers))]
            ips.update(zip(roles, allocate_ips(self.args.ip_range, len(roles))))
        if role not in ips:
            raise GateError(f"no address planned for {role}; run provision first")
        return ips[role]

    def server_name(self, role):
        return f"sandboxes-m1-{self.args.run_id}-{role}"

    def step(self, phase, name, function):
        record = self.state["phases"].setdefault(phase, {"status": "running", "steps": {}})
        if name in record["steps"]:
            return record["steps"][name]["result"]
        result = function()
        record["steps"][name] = {"at": self.now(), "result": result}
        self.save()
        return result

    def push(self, *local):
        """Local files into the gateway staging directory."""
        self.run(gscp(*map(str, local), f"{GATEWAY}:{self.staging}/"), timeout=3600)

    def to_host(self, role, names, destination=HOST):
        self.gw(scp_to_host([f"{self.staging}/{name}" for name in names], self.ip(role), destination, self.key),
                timeout=3600)

    def fetch(self, role, remote_path):
        """A host's result file, via the gateway, into build/m1-gate/<run>/raw/."""
        name = Path(remote_path).name
        local = self.root / "raw" / f"{role}-{name}"
        local.parent.mkdir(parents=True, exist_ok=True)
        self.gw(scp_from_host(self.ip(role), remote_path, f"{self.staging}/out/{role}-{name}", self.key))
        self.run(gscp(f"{GATEWAY}:{self.staging}/out/{role}-{name}", str(local)))
        if self.dry or not local.exists():
            return None
        text = local.read_text()
        return [json.loads(line) for line in text.splitlines() if line.strip()] if name.endswith(".jsonl") \
            else json.loads(text)

    def job(self, role, name, arguments, *, timeout_hours=6, python=f"{HOST}/py"):
        """A long host step as a transient unit, polled until it finishes."""
        self.start_job(role, name, arguments, python=python)
        return self.wait_job(role, name, arguments, timeout_hours=timeout_hours, python=python)

    def start_job(self, role, name, arguments, *, python=f"{HOST}/py"):
        out = f"{OUT}/{name}.json"
        self.on(role, detached(f"{UNIT}-{name}", out, helper(*arguments, "--out", out, python=python)))

    def wait_job(self, role, name, arguments, *, timeout_hours=6, python=f"{HOST}/py"):
        unit, out = f"{UNIT}-{name}", f"{OUT}/{name}.json"
        deadline, restarts = self.now() + timeout_hours * 3600, 0
        while True:
            status = (last_json(self.on(role, poll(unit, out)), {}) or {}).get("status", 0 if self.dry else "lost")
            if status == 0:
                return self.fetch(role, out)
            if status == "lost" and restarts < 2:
                restarts += 1  # The helper resumes from its own results.
                self.start_job(role, name, arguments, python=python)
            elif status not in ("running", "lost"):
                raise GateError(f"{name} on {role} failed (status {status}); see {out} there")
            elif status == "lost" or self.now() > deadline:
                raise GateError(f"{name} on {role} stopped without a result")
            self.sleep(self.args.poll_seconds)

    # Resources.
    def create_server(self, role, server_type, public):
        name = self.server_name(role)
        servers = self.state["resources"]["servers"]
        if servers.get(name, {}).get("id"):
            return servers[name]
        servers.setdefault(name, {"role": role, "type": server_type, "ip": self.ip(role), "id": None,
                                  "requested": self.now(), "drained": False, "registered": False})
        self.save()  # Recorded before it exists, so teardown always knows it.
        # A create interrupted after Hetzner accepted it is in hz.py's ledger: adopt it.
        ledger = json.loads(LEDGER.read_text()).get("servers", {}) if LEDGER.exists() else {}
        created = ledger.get(name) or last_json(self.run(hz(
            "server", name, server_type, self.args.snapshot, self.ip(role), "public" if public else "private"),
            timeout=1200), {}) or {}
        servers[name].update(id=created.get("id", f"<id:{name}>" if self.dry else None), created=self.now())
        if servers[name]["id"] is None:
            raise GateError(f"hz.py server {name} printed no server id")
        self.save()
        return servers[name]

    def wait_ssh(self, role, minutes=10):
        deadline = self.now() + minutes * 60
        while True:
            try:
                self.on(role, "true", timeout=60)
                return True
            except GatewayUnreachable:
                raise
            except GateError:
                if self.now() > deadline:
                    raise
                self.sleep(15)

    # Phases.
    def provision(self):
        args, plan = self.args, self.state["plan"]
        self.step("provision", "preflight", lambda: (self.gw("true"), True)[1])

        def addresses():
            used = [entry["private_ip"] for entry in json.loads(LEDGER.read_text()).get("servers", {}).values()
                    if entry.get("private_ip")] if LEDGER.exists() else []
            roles = ["store", "converter", *(f"w{n + 1}" for n in range(args.workers))]
            plan["ips"] = dict(zip(roles, allocate_ips(args.ip_range, len(roles), used)))
            return plan["ips"]
        self.step("provision", "addresses", addresses)

        def staging():
            self.state["resources"]["staging"] = self.staging
            self.save()
            self.gw(f"install -d -m 0750 -o root -g ucloud {self.staging} && install -d -m 0700 {self.staging}/out")
            return self.staging
        self.step("provision", "staging", staging)

        def known_hosts():
            ips = list(plan["ips"].values())
            self.state["resources"]["known_hosts"] = sorted(set(self.state["resources"]["known_hosts"]) | set(ips))
            self.save()
            self.gw("; ".join(f"ssh-keygen -q -f /root/.ssh/known_hosts -R {ip} >/dev/null 2>&1" for ip in ips)
                    + "; true")
            return ips
        self.step("provision", "known-hosts", known_hosts)
        # Store node and converter keep public IPv4 for GitHub and S3 egress
        # (S12 finding 3); workers stay private, as in production.
        for role, server_type in (("store", args.store_type), ("converter", args.converter_type)):
            self.step("provision", f"server-{role}", lambda role=role, kind=server_type:
                      self.create_server(role, kind, public=True))
        for role in ("store", "converter"):
            self.step("provision", f"ssh-{role}", lambda role=role: self.wait_ssh(role))

    def store_node(self):
        block = json.loads(Path(self.args.chunk_store_block).read_text())
        return block, store_node_adapter(block, store_ip=self.ip("store"), store_url=self.args.store_url,
                                         read_token_file=self.args.store_read_token_file,
                                         write_token_file=self.args.store_write_token_file,
                                         start_command=self.args.store_start_command)

    def configure(self):
        args = self.args
        if not args.chunk_store_block or not args.bundle or not args.nydus_sha256 or not args.distribution_sha256:
            raise GateError("configure needs --chunk-store-block, --bundle, --nydus-sha256 and --distribution-sha256")
        if not (args.nydus_sha256.startswith("1ad7b793") and args.nydus_sha256.endswith("19072")):
            raise GateError("--nydus-sha256 is not nydus-static v2.4.5 (1ad7b793...19072, as S10-S13)")
        block, store = self.store_node()
        prefix, stage = self.state["plan"]["prefix"], self.root / "stage"

        def stage_files():
            stage.mkdir(parents=True, exist_ok=True)
            for role, value in role_blocks(block, store, prefix, self.staging).items():
                (stage / f"block-{role}.json").write_text(json.dumps(value, indent=1, sort_keys=True) + "\n")
            (stage / "py").write_text(f'#!/bin/sh\nexec env PYTHONPATH="$(cat {HOST}/site-packages.path)" python3 "$@"\n')
            (stage / "ucs").write_text(f'#!/bin/sh\nexec env PYTHONPATH="$(cat {HOST}/site-packages.path)" python3 -c '
                                       '\'import sys; from ucloud_sandboxes.cli import main; '
                                       'sys.exit(main(sys.argv[1:]))\' "$@"\n')
            (stage / "registry.yml").write_text(
                "version: 0.1\nstorage:\n  filesystem:\n    rootdirectory: /var/lib/m1-gate/registry\n"
                f"  delete:\n    enabled: true\nhttp:\n  addr: :{args.gate_registry_port}\n")
            (stage / "sample.json").write_text((REPO / args.sample).read_text())
            self.push(*(stage / name for name in ("block-store.json", "block-converter.json", "block-canary.json",
                                                  "py", "ucs", "registry.yml", "sample.json")), REPO / REMOTE)
            return sorted(path.name for path in stage.iterdir())
        self.step("configure", "stage", stage_files)
        names = (block["access_key_id_env"], block["secret_access_key_env"])
        # The S3 key goes file to file on the gateway; it is never printed.
        self.step("configure", "s3-env", lambda: (self.gw(
            f"umask 077; grep -E '^({'|'.join(map(re.escape, names))})=' {shlex.quote(args.gateway_env)} "
            f"> {self.staging}/s3.env; test \"$(wc -l < {self.staging}/s3.env)\" -eq 2"), True)[1])
        for role in ("store", "converter"):
            self.step("configure", f"config-{role}", lambda role=role: self.gw(
                f"python3 {self.staging}/chunk_store_gate_remote.py derive-config "
                f"--source {shlex.quote(args.live_config)} --block {self.staging}/block-{role}.json "
                f"--out {self.staging}/deployment-{role}.json") and True)
            self.step("configure", f"runtime-{role}", lambda role=role: self.install_runtime(role))
        self.step("configure", "nydus", lambda: (self.on("converter", (
            f"curl -fsSL -o {HOST}/nydus.tgz {shlex.quote(args.nydus_url)} && "
            f"echo '{args.nydus_sha256}  {HOST}/nydus.tgz' | sha256sum -c --quiet - && "
            f"tar -xzf {HOST}/nydus.tgz -C {HOST} && install -m 0755 {HOST}/nydus-static/nydus-image "
            f"/usr/local/bin/nydus-image && nydus-image --version | head -1 && modprobe nbd nbds_max=1024 max_part=0 && "
            "(command -v mkfs.erofs >/dev/null || DEBIAN_FRONTEND=noninteractive apt-get install -y -q erofs-utils) && "
            # The rollback's builder pulls from the gate registry over HTTP.
            "python3 -c \"import json,pathlib; p=pathlib.Path('/etc/docker/daemon.json'); "
            "d=json.loads(p.read_text()) if p.exists() else {}; "
            f"d['insecure-registries']=sorted(set(d.get('insecure-registries',[]))|{{'{self.ip('store')}:"
            f"{args.gate_registry_port}'}}); p.write_text(json.dumps(d))\" && systemctl restart docker"),
            timeout=1800), True)[1])
        self.step("configure", "registry", lambda: (self.on("store", (
            f"curl -fsSL -o {HOST}/registry.tgz {shlex.quote(args.distribution_url)} && "
            f"echo '{args.distribution_sha256}  {HOST}/registry.tgz' | sha256sum -c --quiet - && "
            f"mkdir -p {HOST}/registry-bin /var/lib/m1-gate/registry && tar -xzf {HOST}/registry.tgz -C "
            f"{HOST}/registry-bin registry && (systemctl is-active --quiet {UNIT}-registry || systemd-run --quiet "
            f"--unit {UNIT}-registry -p Restart=on-failure {HOST}/registry-bin/registry serve {HOST}/registry.yml) && "
            f"for i in $(seq 60); do curl -fsS -o /dev/null http://127.0.0.1:{args.gate_registry_port}/v2/ && "
            "exit 0; sleep 1; done; exit 1"), timeout=900), True)[1])
        self.step("configure", "store-service", lambda: (self.on("store", store.start_command), self.on(
            "converter", f"for i in $(seq 120); do curl -fsS -o /dev/null {store.url}/healthz && exit 0; sleep 2; "
                         "done; exit 1", timeout=600), store.url)[2])
        # Tokens move host to gateway to host, root or ucloud only, never printed.
        self.step("configure", "tokens", lambda: (
            self.gw("umask 077; " + scp_from_host(self.ip("store"), store.write_token_path,
                                                   f"{self.staging}/write.token", self.key)
                    + " && " + scp_from_host(self.ip("store"), store.read_token_path, f"{self.staging}/read.token",
                                             self.key)
                    + f" && chown ucloud:ucloud {self.staging}/read.token && chmod 0600 {self.staging}/read.token"),
            self.to_host("converter", ["write.token"], "/etc/m1-gate/write.token"),
            self.on("converter", "chmod 0600 /etc/m1-gate/write.token"), True)[3])
        for role in ("store", "converter"):
            self.step("configure", f"validate-{role}", lambda role=role: last_json(self.on(
                role, helper("validate-config", "--config", "/etc/m1-gate/deployment.json"))))
        self.step("configure", "producer-key", lambda: (self.on(
            "converter", f"{HOST}/ucs provision-environment-key --directory /etc/m1-gate/producer >/dev/null"),
            self.gw(scp_from_host(self.ip("converter"), "/etc/m1-gate/producer/producers.json",
                                  f"{self.staging}/gate-producers.json", self.key)), True)[2])

    def install_runtime(self, role):
        """The release's agent runtime from the node bundle; no package index."""
        # From here a gate host holds the S3 key, so teardown must clean the prefix.
        self.state["resources"]["s3_prefix"] = self.state["plan"]["prefix"]
        self.save()
        self.on(role, f"install -d -m 0700 {HOST} /etc/m1-gate /var/lib/m1-gate {OUT}")
        self.gw(scp_to_host([self.args.bundle], self.ip(role), f"{HOST}/bundle.tar.gz", self.key), timeout=3600)
        self.to_host(role, ["py", "ucs", "chunk_store_gate_remote.py", "sample.json", "registry.yml"])
        self.to_host(role, [f"deployment-{role}.json"], "/etc/m1-gate/deployment.json")
        self.to_host(role, ["s3.env"], "/etc/m1-gate/s3.env")
        check = f"echo '{self.args.bundle_sha256}  {HOST}/bundle.tar.gz' | sha256sum -c --quiet - && " \
            if self.args.bundle_sha256 else ""
        self.on(role, check + (
            f"chmod 0600 /etc/m1-gate/s3.env /etc/m1-gate/deployment.json && chmod 0755 {HOST}/py {HOST}/ucs && "
            f"rm -rf {HOST}/bundle {HOST}/agent && mkdir -p {HOST}/bundle {HOST}/agent && "
            f"tar -xzf {HOST}/bundle.tar.gz -C {HOST}/bundle && "
            f"tar -xf \"$(find {HOST}/bundle -name node-agent-runtime.tar | head -1)\" -C {HOST}/agent && "
            f"find {HOST}/agent -maxdepth 4 -type d -name site-packages | head -1 > {HOST}/site-packages.path && "
            f"{HOST}/py -c 'import ucloud_sandboxes.chunk_index, ucloud_sandboxes.chunk_convert'"), timeout=1800)
        return True

    def remote_common(self, *, converter=True):
        registry = f"http://{self.ip('store')}:{self.args.gate_registry_port}"
        options = ["--config", "/etc/m1-gate/deployment.json", "--token-file", "/etc/m1-gate/write.token",
                   "--work-root", "/var/lib/m1-gate/work", "--registry-url", registry,
                   "--trust", "/etc/m1-gate/producer/producers.json",
                   "--signing-key", "/etc/m1-gate/producer/producer.pem",
                   "--sample", f"{HOST}/sample.json", "--s3-env", "/etc/m1-gate/s3.env"]
        # A stable owner per host: a rerun takes its own layer claims back at once.
        return registry, options + (["--owner", f"m1-gate-{self.args.run_id}:converter", "--attach-tag",
                                     ATTACH_TAG] if converter else [])

    def tally(self, name):
        _, store = self.store_node()
        s3 = self.job("converter", f"tally-{name}", ["s3-tally", "--config", "/etc/m1-gate/deployment.json",
                                                     "--s3-env", "/etc/m1-gate/s3.env"], timeout_hours=1)
        index = self.job("store", f"index-{name}", ["index-tally", "--db", store.index_database], timeout_hours=1)
        return {"s3": s3, "index": index}

    def convert(self):
        registry, common = self.remote_common()
        self.on("converter", "mkdir -p /var/lib/m1-gate/work")
        self.step("convert", "mirror", lambda: bool(self.job("converter", "mirror", [
            "mirror", "--source-url", self.args.production_registry, "--target-url", registry,
            "--sample", f"{HOST}/sample.json", "--tag", "m1-src", "--work-root", "/var/lib/m1-gate/work",
            "--rate-mb", self.args.mirror_rate_mb]) or self.dry))
        protect = sorted({*S10_BASELINES, *BURST_IMAGES, *ROLLBACK_IMAGES})
        holdout = self.step("convert", "holdout", lambda: self.job("converter", "holdout", [
            "holdout", "--registry-url", registry, "--sample", f"{HOST}/sample.json",
            # Spares for images whose conversion never reaches a step (no new chunks: no pack_put).
            "--protect", ",".join(map(str, protect)), "--count", 2 * len(STEPS)], timeout_hours=1))
        self.state["results"]["holdout"] = holdout
        self.step("convert", "convert", lambda: self.job("converter", "convert", [
            "convert", *common, "--results", f"{OUT}/convert.jsonl", "--exclude", f"{OUT}/holdout.json",
            "--parallel", self.args.parallel, "--devices-per-slot", self.args.devices_per_slot], timeout_hours=12))
        self.state["results"]["convert"] = self.step("convert", "results",
                                                     lambda: self.fetch("converter", f"{OUT}/convert.jsonl"))
        self.state["results"]["tally_convert"] = self.step("convert", "tally", lambda: self.tally("convert"))
        self.save()

    def crash(self):
        _, common = self.remote_common()
        self.step("crash", "crash", lambda: self.job("converter", "crash", [
            "crash", *common, "--results", f"{OUT}/crash.jsonl", "--holdout", f"{OUT}/holdout.json",
            "--steps", ",".join(STEPS), "--devices-per-slot", self.args.devices_per_slot], timeout_hours=6))
        self.state["results"]["crash"] = self.step("crash", "results",
                                                   lambda: self.fetch("converter", f"{OUT}/crash.jsonl"))
        # All 181 images are converted now: the stored-bytes criterion uses this tally.
        self.state["results"]["tally"] = self.step("crash", "tally", lambda: self.tally("all"))
        self.save()

    def workers(self):
        args = self.args
        if not args.accept_canary_placement:
            raise GateError("canary workers heartbeat to the production gateway, and a production create placed "
                            "on one would read the gate registry; pass --accept-canary-placement to run them")
        if not args.bundle:
            raise GateError("workers needs --bundle (the release's sandbox node bundle on the gateway)")
        roles = [f"w{n + 1}" for n in range(args.workers)]
        alias = f"ucloud-sandbox-registry:{args.gate_registry_port}"
        sample = json.loads((REPO / args.sample).read_text())

        def image(index):
            name = sample[index]["prepared_reference"].split("@")[0]
            return f"{alias}/{name.split('/', 1)[1].rsplit(':', 1)[0]}:{ATTACH_TAG}"

        def gateway_runtime():
            stage = self.root / "stage"
            stage.mkdir(parents=True, exist_ok=True)
            (stage / "bench-seq.json").write_text(json.dumps({str(i): image(i) for i in S10_BASELINES}, indent=1))
            (stage / "bench-burst.json").write_text(json.dumps({str(i): image(i) for i in BURST_IMAGES}, indent=1))
            (stage / "overrides.json").write_text(json.dumps(self.worker_overrides(), indent=1, sort_keys=True))
            self.push(stage / "bench-seq.json", stage / "bench-burst.json", stage / "overrides.json")
            # init-vm must be this release's: the live gateway's rejects chunk_store.
            self.gw(f"sudo -u ucloud test -r {shlex.quote(args.bundle)} && rm -rf {self.staging}/agent "
                    f"{self.staging}/bundle && mkdir -p {self.staging}/bundle {self.staging}/agent && "
                    f"tar -xzf {shlex.quote(args.bundle)} -C {self.staging}/bundle && tar -xf \"$(find "
                    f"{self.staging}/bundle -name node-agent-runtime.tar | head -1)\" -C {self.staging}/agent && "
                    f"find {self.staging}/agent -maxdepth 4 -type d -name site-packages | head -1 "
                    f"> {self.staging}/site-packages.path && chmod -R a+rX {self.staging}/agent "
                    f"{self.staging}/site-packages.path", timeout=1800)
            return True
        self.step("workers", "gateway-runtime", gateway_runtime)
        self.step("workers", "trust", lambda: (self.gw(
            f"python3 {self.staging}/chunk_store_gate_remote.py merge-trust --trust {shlex.quote(args.live_trust)} "
            f"--trust {self.staging}/gate-producers.json --out {self.staging}/canary-producers.json --owner ucloud"),
            True)[1])
        sets = [f"--set {shlex.quote(f'{key}={json.dumps(value)}')}" for key, value in {
            **self.worker_overrides(), "registry_private_ip": self.ip("store"),
            "immutable_environments.trusted_keys_file": f"{self.staging}/canary-producers.json",
            "immutable_environments.worker_enabled": True}.items()]
        self.step("workers", "canary-config", lambda: (self.gw(
            f"python3 {self.staging}/chunk_store_gate_remote.py derive-config --source {shlex.quote(args.live_config)} "
            f"--block {self.staging}/block-canary.json --out {self.staging}/deployment-canary.json --owner ucloud "
            + " ".join(sets)), self.gw(f"sudo -u ucloud env PYTHONPATH=\"$(cat {self.staging}/site-packages.path)\" "
                                       f"python3 {self.staging}/chunk_store_gate_remote.py validate-config --config "
                                       f"{self.staging}/deployment-canary.json"), True)[2])
        for role in roles:
            self.step("workers", f"server-{role}", lambda role=role: self.create_server(role, args.worker_type, False))
            self.step("workers", f"known-hosts-{role}", lambda role=role: (self.gw(
                f"ssh-keygen -q -f /root/.ssh/known_hosts -R {self.ip(role)} >/dev/null 2>&1; true"), True)[1])
            self.step("workers", f"ssh-{role}", lambda role=role: self.wait_ssh(role))
            self.step("workers", f"init-{role}", lambda role=role: self.init_worker(role))
            self.step("workers", f"health-{role}", lambda role=role: (self.on(
                role, "systemctl is-active ucloud-environment-io.service ucloud-sandbox-node.service"), True)[1])
            self.step("workers", f"stage-{role}", lambda role=role: (
                self.on(role, f"install -d -m 0700 {HOST} {OUT}"),
                self.to_host(role, ["chunk_store_gate_remote.py", "bench-seq.json", "bench-burst.json"]), True)[2])
        # Two workers run the sequential cycles and the burst at once; one runs them in turn.
        plan = [("seq", roles[0]), ("burst", roles[-1])]
        bench = {kind: ["bench", "--kind", kind, "--images", f"{HOST}/bench-{kind}.json", "--run", args.run_id,
                        "--n", len(BURST_IMAGES)] for kind, _ in plan}
        python = "/usr/bin/python3"  # The bench needs only the standard library.
        for position, (kind, role) in enumerate(plan):
            self.step("workers", f"start-bench-{kind}", lambda role=role, kind=kind: (
                self.start_job(role, f"bench-{kind}", bench[kind], python=python), True)[1])
            if len(roles) == 1 or position == len(plan) - 1:
                for waited, waited_role in plan[:position + 1]:
                    self.state["results"][f"bench_{waited}"] = self.step(
                        "workers", f"bench-{waited}", lambda role=waited_role, kind=waited: self.wait_job(
                            role, f"bench-{kind}", bench[kind], timeout_hours=4, python=python))
        for role in roles:
            self.step("workers", f"drain-{role}", lambda role=role: self.drain(role))
        self.save()

    def worker_overrides(self):
        if self.args.worker_overrides:
            return json.loads(Path(self.args.worker_overrides).read_text())
        return dict(WORKER_OVERRIDES)

    def init_worker(self, role):
        """VM init from the gateway as the ``ucloud`` user (it owns the trust
        files), with this release's CLI and the canary config copy."""
        server = self.state["resources"]["servers"][self.server_name(role)]
        server["registered"] = True  # From here it may heartbeat: teardown drains it.
        self.save()
        self.gw(f"set -a; . /etc/ucloud-sandboxes/hetzner.env; set +a; cd /work/ucloud-sandboxes && "
                f"sudo -E -u ucloud env PYTHONPATH=\"$(cat {self.staging}/site-packages.path)\" python3 -c "
                "'import sys; from ucloud_sandboxes.cli import main; sys.exit(main(sys.argv[1:]))' "
                f"init-vm {server['id']} --config {self.staging}/deployment-canary.json --role sandbox "
                f"--package-spec {shlex.quote(self.args.bundle)} --ssh-private-key-file "
                f"/var/lib/ucloud-sandboxes/state/ssh/gateway-init --execute --output json "
                f"> {self.staging}/out/init-{role}.json 2>&1", timeout=2400)
        return True

    def drain(self, role):
        """POST /v1/drain on the worker's node agent, before anything deletes it."""
        server = self.state["resources"]["servers"][self.server_name(role)]
        self.on(role, f"install -d -m 0700 {HOST}")  # Staged here too, so a partial init still drains.
        self.to_host(role, ["chunk_store_gate_remote.py"])
        self.on(role, helper("drain", "--token", f"m1-gate-{self.args.run_id}", python="/usr/bin/python3"),
                timeout=120)
        server.update(drained=True, drained_at=self.now())
        self.save()
        return True

    def rollback(self):
        _, options = self.remote_common(converter=False)
        self.step("rollback", "rollback", lambda: self.job("converter", "rollback", [
            "rollback", *options, "--converted", f"{OUT}/convert.jsonl", "--indices",
            ",".join(map(str, ROLLBACK_IMAGES)), "--output-tag", "m1-unpacked", "--results", f"{OUT}/rollback.jsonl"],
            timeout_hours=4))
        self.state["results"]["rollback"] = self.step("rollback", "results",
                                                      lambda: self.fetch("converter", f"{OUT}/rollback.jsonl"))
        self.save()

    def report(self):
        results = self.state["results"]
        verdict = evaluate(results)
        verdict.update(run_id=self.args.run_id, resources=vm_hours(self.state, self.now()),
                       prefix=self.state["plan"]["prefix"], generated=self.now())
        if self.dry:
            print(json.dumps(verdict, indent=1, sort_keys=True))
            return verdict
        (self.root / "report.json").write_text(json.dumps(verdict, indent=1, sort_keys=True) + "\n")
        directory = Path(self.args.docs_dir) / f"m1-gate-{self.args.report_date}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "summary.json").write_text(json.dumps(verdict, indent=1, sort_keys=True) + "\n")
        (directory / "README.md").write_text(render_readme(verdict, self.args.report_date))
        return verdict

    def teardown(self):
        resources = self.state["resources"]
        servers = resources["servers"]
        failures, undrained = [], set()
        for name, server in sorted(servers.items()):
            if server.get("registered") and not server.get("drained"):
                try:
                    self.drain(server["role"])
                except GatewayUnreachable:
                    raise
                except GateError as exc:
                    undrained.add(name)  # Deleting it now could repeat the 0.8.2 placement incident.
                    failures.append(f"drain {name} (kept): {exc}")
        if any(server.get("drained_at", 0) > self.now() - 60 for server in servers.values()):
            self.sleep(60)  # Let placement see the drain before the heartbeat goes stale.
        s3_done = self.args.keep_store or not resources.get("s3_prefix")
        if not s3_done:
            for role in ("converter", "store"):
                if self.server_name(role) not in servers:
                    continue
                try:
                    out = f"{OUT}/s3-delete.json"
                    self.on(role, helper("s3-delete", "--config", "/etc/m1-gate/deployment.json", "--s3-env",
                                         "/etc/m1-gate/s3.env", "--prefix", resources["s3_prefix"], "--out", out),
                            timeout=3600)
                    resources["s3_prefix"], s3_done = None, True
                    self.save()
                    break
                except GatewayUnreachable:
                    raise
                except GateError as exc:
                    failures.append(f"s3 delete on {role}: {exc}")
        # Workers first; the store node and converter last, after the S3 delete ran on them.
        for name, server in sorted(servers.items(), key=lambda item: item[1]["role"] in ("store", "converter")):
            if name in undrained:
                continue
            _, output, error = self.runner(hz("delete-server", name), 300)
            # Unknown to hz.py's ledger, or already gone at Hetzner (404): nothing left to delete.
            if self.dry or "deleted server" in output or f"unknown server {name}" in error or "HTTP 404" in error:
                server["deleted"] = self.now()
                self.state.setdefault("deleted_servers", {})[name] = server
                del servers[name]
                self.save()
            else:
                failures.append(f"delete-server {name}: {redact(error or output).strip()[-200:]}")
        if resources.get("staging") and s3_done and not servers:
            if not re.fullmatch(r"/var/tmp/m1-gate-[a-z0-9]{6,20}", resources["staging"]):
                raise GateError("refusing to remove an unexpected staging path")
            self.gw(f"rm -rf --one-file-system {resources['staging']}")
            resources["staging"] = None
            self.save()
        if resources.get("known_hosts"):
            self.gw("; ".join(f"ssh-keygen -q -f /root/.ssh/known_hosts -R {ip} >/dev/null 2>&1"
                              for ip in resources["known_hosts"]) + "; true")
            resources["known_hosts"] = []
            self.save()
        if not s3_done:
            failures.append(f"S3 objects may remain under {resources['s3_prefix']}; staging kept for a retry "
                            "(rerun teardown, or pass --keep-store to leave them deliberately)")
        if failures:
            raise GateError("teardown incomplete: " + "; ".join(failures))
        current = Path(self.args.state_dir) / "current"
        if not self.dry and current.exists() and current.read_text().strip() == self.args.run_id:
            current.unlink()
        return True

    def run_phase(self, name):
        needed = NEEDS.get(name)
        if needed and not self.dry and self.state["phases"].get(needed, {}).get("status") != "done":
            raise GateError(f"phase {name} needs {needed} first")
        record = self.state["phases"].setdefault(name, {"status": "running", "steps": {}})
        if record["status"] == "done" and name not in ("report", "teardown"):
            print(f"{name}: already done")
            return
        record.update(status="running", started=self.now())
        self.save()
        try:
            getattr(self, name)()
        except GateError as exc:
            record.update(status="failed", error=redact(str(exc))[:2000])
            self.save()
            raise
        record.update(status="done", finished=self.now())
        self.save()


# --- Report ---

def criterion(name, passed, measured, gate):
    return {"name": name, "status": "pass" if passed is True else "fail" if passed is False else "not run",
            "measured": measured, "gate": gate}


def evaluate(results):
    """Pass/fail per M1 criterion (design §9) from the recorded results."""
    criteria = []
    convert, crash = results.get("convert"), results.get("crash")
    if convert is None and crash is None:
        criteria.append(criterion("full_tree", None, None, f"{SAMPLE_SIZE}/{SAMPLE_SIZE} equal"))
    else:
        verified = {row["index"] for row in convert or () if row.get("ok") and row.get("verified")} | \
            {row["index"] for row in crash or () if row.get("verified")}
        criteria.append(criterion("full_tree", len(verified) == SAMPLE_SIZE, f"{len(verified)}/{SAMPLE_SIZE}",
                                  f"{SAMPLE_SIZE}/{SAMPLE_SIZE} equal (contents, modes, owners, mtimes, xattrs)"))
    tally = (results.get("tally") or {}).get("s3")
    if tally is None:
        criteria.append(criterion("stored_bytes", None, None, "17.5 GB +-5%"))
    else:
        stored = tally["stored_bytes"]
        criteria.append(criterion("stored_bytes", abs(stored - STORED_BYTES_TARGET) <= STORED_BYTES_TOLERANCE
                                  * STORED_BYTES_TARGET, f"{stored / 1e9:.2f} GB", "17.5 GB +-5%"))
    seq = results.get("bench_seq")
    if seq is None:
        criteria.append(criterion("cold_commands", None, None, f"<= {COLD_RATIO}x S10, traced"))
    else:
        rows, passed = [], True
        for index, baselines in S10_BASELINES.items():
            for command, baseline in baselines.items():
                cycle = (seq.get(f"{index}:{command}") or {}).get("traced") or {}
                wall, limit = cycle.get("wall"), COLD_RATIO * baseline if baseline else PIP_LIMIT_SECONDS
                ok = wall is not None and cycle.get("rc") == 0 and wall <= limit
                passed &= ok
                rows.append({"index": index, "command": command, "wall": wall, "limit": round(limit, 3), "ok": ok,
                             "ratio": round(wall / baseline, 2) if wall is not None and baseline else None})
        criteria.append(criterion("cold_commands", passed, rows, f"<= {COLD_RATIO}x S10 (pip: <= 1.3x S12 local, "
                                  f"else {PIP_LIMIT_SECONDS} s), trace replayed"))
    burst = (results.get("bench_burst") or {}).get("traced")
    criteria.append(criterion("burst_20", None if burst is None else burst["n"] == len(BURST_IMAGES)
                              and burst["wall"] <= BURST_LIMIT_SECONDS,
                              None if burst is None else f"{burst['wall']:.2f} s ({burst['n']} sandboxes)",
                              f"<= {BURST_LIMIT_SECONDS} s (S11)"))
    if crash is None:
        criteria.append(criterion("crash_injection", None, None, f"{len(STEPS)} steps"))
    else:
        by_step = {row["step"]: row for row in crash if row.get("killed")}
        failed = [step for step in STEPS if not by_step.get(step, {}).get("ok")]
        criteria.append(criterion("crash_injection", not failed, {"failed_steps": failed, "steps": len(STEPS)},
                                  "every write-path step: killed, no visible partial image, rerun converges"))
    rollback = results.get("rollback")
    criteria.append(criterion("rollback_10", None if rollback is None else len(
        [row for row in rollback if row.get("ok")]) == len(ROLLBACK_IMAGES),
        None if rollback is None else f"{len([row for row in rollback if row.get('ok')])}/{len(ROLLBACK_IMAGES)}",
        "10 images unpacked, tree-equal and rebuilt by today's builder"))
    return {"criteria": criteria, "pass": all(item["status"] == "pass" for item in criteria)}


def vm_hours(state, now):
    rows = []
    for name, server in {**state.get("deleted_servers", {}), **state["resources"]["servers"]}.items():
        start = server.get("created") or server.get("requested")
        if start is None:
            continue
        hours = (server.get("deleted") or now) - start
        billed = max(1, math.ceil(hours / 3600))
        rows.append({"name": name, "type": server["type"], "hours": round(hours / 3600, 2), "billed_hours": billed,
                     "eur": round(billed * PRICE_EUR_PER_HOUR.get(server["type"], 0), 2)})
    return {"servers": rows, "vm_hours": round(sum(row["hours"] for row in rows), 2),
            "eur_estimate": round(sum(row["eur"] for row in rows), 2)}


def render_readme(verdict, date):
    lines = [f"# M1 gate run ({date})", "",
             "The chunk store's M1 gate ([design §9](../../chunk-store-design.md#9-build-plan)) on S10's "
             "181-image sample, run by `scripts/chunk_store_gate.py`.", "",
             f"**Result: {'pass' if verdict['pass'] else 'fail'}.** Run `{verdict['run_id']}`, S3 prefix "
             f"`{verdict['prefix']}`.", "", "| Criterion | Status | Measured | Gate |", "| --- | --- | --- | --- |"]
    for item in verdict["criteria"]:
        measured = item["measured"]
        if isinstance(measured, list):
            failed = [f"{row['index']}:{row['command']}" for row in measured if not row["ok"]]
            measured = f"{len(measured) - len(failed)}/{len(measured)} within limit" + (
                f" (over: {', '.join(failed)})" if failed else "")
        elif isinstance(measured, dict):
            measured = json.dumps(measured, sort_keys=True)
        lines.append(f"| {item['name']} | {item['status']} | {measured if measured is not None else '-'} | "
                     f"{item['gate']} |")
    resources = verdict["resources"]
    lines += ["", "## Resources", "", f"{resources['vm_hours']} VM-hours, about EUR {resources['eur_estimate']} "
              "at list price (billed per started hour).", "", "| Server | Type | Hours | Billed |",
              "| --- | --- | ---: | ---: |"]
    lines += [f"| {row['name']} | {row['type']} | {row['hours']} | {row['billed_hours']} |"
              for row in resources["servers"]]
    cold = next((item for item in verdict["criteria"] if item["name"] == "cold_commands"), None)
    if cold and isinstance(cold["measured"], list):
        lines += ["", "## Cold first commands (traced)", "", "| Image | Command | Wall s | Limit s | Ratio |",
                  "| ---: | --- | ---: | ---: | ---: |"]
        lines += [f"| {row['index']} | {row['command']} | {row['wall']} | {row['limit']} | {row['ratio'] or '-'} |"
                  for row in cold["measured"]]
    lines += ["", "Raw results: `summary.json` here, and `build/m1-gate/<run>/raw/` on the operator machine.", ""]
    return "\n".join(lines)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("--phase", choices=(*PHASES, "all"), required=True)
    parser.add_argument("--dry-run", action="store_true", help="print every command instead of running it")
    parser.add_argument("--run-id", default="", help="default: the current run, else a new one (provision)")
    parser.add_argument("--state-dir", default=str(REPO / "build" / "m1-gate"))
    parser.add_argument("--snapshot", default="438728121", help="current worker snapshot")
    parser.add_argument("--store-type", default="ccx43")
    parser.add_argument("--converter-type", default="ccx63")
    parser.add_argument("--worker-type", default="ccx43")
    parser.add_argument("--workers", type=int, choices=(1, 2), default=2)
    parser.add_argument("--ip-range", default="10.42.0.48-10.42.0.63", help="private addresses; never .40-.47")
    store = parser.add_argument_group("store node (its service's config interface)")
    store.add_argument("--chunk-store-block", help="JSON file: immutable_environments.chunk_store for the run")
    store.add_argument("--store-url", default="", help="default http://<store ip>:<index_listen port>")
    store.add_argument("--store-read-token-file", default="", help="on the store node; default from the block")
    store.add_argument("--store-write-token-file", default="", help="on the store node; default from the block")
    store.add_argument("--store-start-command", default="", help="host command that starts the store service")
    parser.add_argument("--bundle", default="", help="the release's sandbox node bundle, a path on the gateway")
    parser.add_argument("--bundle-sha256", default="")
    parser.add_argument("--nydus-url", default=NYDUS_URL)
    parser.add_argument("--nydus-sha256", default="")
    parser.add_argument("--distribution-url", default=DISTRIBUTION_URL)
    parser.add_argument("--distribution-sha256", default="")
    parser.add_argument("--gate-registry-port", type=int, default=5000, help="the live config's registry_port")
    parser.add_argument("--sample", default=SAMPLE)
    parser.add_argument("--production-registry", default="http://10.42.0.2:5000", help="read only")
    parser.add_argument("--mirror-rate-mb", type=float, default=100.0)
    parser.add_argument("--parallel", type=int, default=12)
    parser.add_argument("--devices-per-slot", type=int, default=4)
    parser.add_argument("--gateway-node-key", default="/var/lib/ucloud-sandboxes/state/ssh/gateway-init")
    parser.add_argument("--gateway-env", default="/etc/ucloud-sandboxes/hetzner.env")
    parser.add_argument("--live-config", default="/etc/ucloud-sandboxes/deployment.json", help="read, never written")
    parser.add_argument("--live-trust", default="/var/lib/ucloud-sandboxes/state/environment-producer/producers.json")
    parser.add_argument("--worker-overrides", default="", help="JSON {dotted.key: value} for the canary config")
    parser.add_argument("--accept-canary-placement", action="store_true")
    parser.add_argument("--keep-store", action="store_true", help="teardown keeps the run's S3 objects")
    parser.add_argument("--report-date", default=dt.date.today().isoformat())
    parser.add_argument("--docs-dir", default=str(REPO / "docs" / "benchmarks"))
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    return parser.parse_args(argv)


def resolve_run_id(args):
    """The run in ``<state-dir>/current`` unless one is named; provision starts one."""
    current = Path(args.state_dir) / "current"
    if not args.run_id:
        if current.exists():
            args.run_id = current.read_text().strip()
        elif args.phase in ("provision", "all"):
            args.run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dt%H%M")
        else:
            raise GateError("no current run: pass --run-id or start with --phase provision")
    if not args.dry_run and not current.exists():
        current.parent.mkdir(parents=True, exist_ok=True)
        current.write_text(args.run_id + "\n")
    return args.run_id


def main(argv=None, runner=None):
    args = parse_args(argv)
    try:
        resolve_run_id(args)
    except GateError as exc:
        print(exc, file=sys.stderr)
        return 1
    gate = Gate(args, runner or (DryRunRunner() if args.dry_run else SubprocessRunner(
        Path(args.state_dir) / args.run_id / "commands.log")), sleep=(lambda _: None) if args.dry_run else time.sleep)
    try:
        for name in (PHASES if args.phase == "all" else (args.phase,)):
            gate.run_phase(name)
    except GatewayUnreachable as exc:
        print(f"{exc}. State: {gate.state_path}", file=sys.stderr)
        return 3
    except GateError as exc:
        print(f"{redact(str(exc))}\nState: {gate.state_path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
