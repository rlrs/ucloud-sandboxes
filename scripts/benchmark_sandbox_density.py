#!/usr/bin/env python3
"""Qualify one idle node with resident working sets and concurrent park/wake.

Use the authenticated direct node URL (or an SSH tunnel), never the gateway.
This allocates only uniquely named test sandboxes and always attempts cleanup.
The default SLOs are acceptance targets, not previously measured results.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import time
from urllib import error, request
from uuid import uuid4

from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint


# A persistent process holds dirty anonymous memory, a Unix socket, an open
# SQLite connection and a boot nonce that a restarted replacement cannot fake.
WORKLOAD = r"""
import hashlib, json, os, socket, sqlite3, sys, time, uuid
from pathlib import Path
root = Path('/workspace/density')
root.mkdir(parents=True, exist_ok=True)
size = int(sys.argv[1]) * 1024 * 1024
dirty_mb = int(sys.argv[2]) if len(sys.argv) > 2 else int(sys.argv[1])
dirty_size = dirty_mb * 1024 * 1024
if not 0 < dirty_size <= size:
    raise ValueError('dirty working set must be positive and no larger than resident memory')
page_count = size // 4096
dirty_page_count = dirty_size // 4096
dirty_start_page = None
# Fill all bytes independently, avoiding compression/deduplication shortcuts
# without a second full-size temporary allocation at startup.
memory = bytearray(size)
for offset in range(0, size, 1024 * 1024):
    length = min(1024 * 1024, size - offset)
    memory[offset:offset + length] = os.urandom(length)
memory_hash = hashlib.sha256(memory).hexdigest()
nonce = uuid.uuid4().hex
counter = 0
connection = sqlite3.connect(root / 'state.sqlite')
connection.execute('create table state (counter integer)')
connection.execute('insert into state values (0)')
connection.commit()
server = socket.socket(socket.AF_UNIX)
server.bind(str(root / 'socket'))
server.listen(16)
while True:
    client, _ = server.accept()
    try:
        started = time.perf_counter()
        timings = {}
        command = json.loads(client.recv(4096))
        if command['op'] == 'act':
            phase = time.perf_counter()
            if hashlib.sha256(memory).hexdigest() != memory_hash:
                raise RuntimeError('resident memory corrupted')
            timings['memory_verify'] = (time.perf_counter() - phase) * 1000
            phase = time.perf_counter()
            if connection.execute('select counter from state').fetchone()[0] != counter:
                raise RuntimeError('SQLite state corrupted')
            fresh = sqlite3.connect(root / 'state.sqlite')
            try:
                if fresh.execute('select counter from state').fetchone()[0] != counter:
                    raise RuntimeError('persisted SQLite state corrupted')
            finally:
                fresh.close()
            if counter:
                previous = hashlib.sha256((nonce + str(counter)).encode()).digest() * 128
                if any((root / ('source-%d' % index)).read_bytes() != previous
                       for index in range(64)):
                    raise RuntimeError('persisted file content corrupted')
            timings['persistent_verify'] = (time.perf_counter() - phase) * 1000
            phase = time.perf_counter()
            dirty_start_page = (counter * dirty_page_count) % page_count
            counter += 1
            for page in range(dirty_page_count):
                offset = ((dirty_start_page + page) % page_count) * 4096
                memory[offset] = (memory[offset] + 1) % 256
            memory_hash = hashlib.sha256(memory).hexdigest()
            timings['memory_dirty_and_hash'] = (time.perf_counter() - phase) * 1000
            phase = time.perf_counter()
            data = hashlib.sha256((nonce + str(counter)).encode()).digest() * 128
            for index in range(64):
                path = root / ('source-%d' % index)
                path.write_bytes(data)
                if path.read_bytes() != data:
                    raise RuntimeError('file content corrupted')
            timings['files_write_read'] = (time.perf_counter() - phase) * 1000
            phase = time.perf_counter()
            connection.execute('update state set counter = ?', (counter,))
            connection.commit()
            timings['sqlite_commit'] = (time.perf_counter() - phase) * 1000
            phase = time.perf_counter()
            deadline = time.process_time() + command['cpu_ms'] / 1000
            while time.process_time() < deadline:
                hashlib.sha256(data).digest()
            timings['cpu_work'] = (time.perf_counter() - phase) * 1000
        timings['total'] = (time.perf_counter() - started) * 1000
        result = dict(nonce=nonce, pid=os.getpid(), counter=counter,
                      resident_bytes=size, memory_sha256=memory_hash,
                      dirty_mb=dirty_mb, dirty_page_count=dirty_page_count,
                      dirty_start_page=dirty_start_page,
                      files=64 if counter else 0, guest_timings_ms=timings)
    except Exception as exc:
        result = dict(error=str(exc))
    client.sendall(json.dumps(result).encode() + b'\n')
    client.close()
"""

PROBE = r"""
import json, socket, sys, time
path = '/workspace/density/socket'
deadline = time.monotonic() + 60
while True:
    connection = socket.socket(socket.AF_UNIX)
    try:
        connection.connect(path)
        break
    except (FileNotFoundError, ConnectionRefusedError):
        connection.close()
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.05)
connection.settimeout(60)
connection.sendall(sys.argv[1].encode())
result = b''
while not result.endswith(b'\n'):
    chunk = connection.recv(4096)
    if not chunk:
        raise RuntimeError('workload closed without a result')
    result += chunk
print(result.decode(), end='')
"""


class NodeApi:
    def __init__(self, url: str, token: str, timeout: float = 180):
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def call(self, path, *, method="GET", payload=None, headers=None, timeout=None):
        req = request.Request(
            self.url + path,
            data=None if payload is None else json.dumps(payload).encode(),
            method=method,
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
                **(headers or {}),
            },
        )
        try:
            with request.urlopen(
                req,
                timeout=self.timeout if timeout is None else min(self.timeout, timeout),
            ) as response:
                return json.load(response)
        except error.HTTPError as exc:
            # No automatic retries: capacity/errors remain visible in qualification.
            detail = exc.read(4096).decode(errors="replace")
            raise RuntimeError(f"{method} {path}: HTTP {exc.code}: {detail}") from exc

    def probe(self, sandbox_id, *, act=False, cpu_ms=100):
        command = {"op": "act" if act else "status", "cpu_ms": cpu_ms}
        deadline = time.monotonic() + self.timeout
        result = self.call(
            f"/v1/sandboxes/{sandbox_id}/exec",
            method="POST",
            payload={
                "command": ["python3", "-c", PROBE, json.dumps(command)],
                "env": {},
                "working_dir": None,
                "stdin": False,
                "tty": False,
            },
        )
        session = result["session"]
        node_timings = result.get("timings")
        output, errors = [], []
        after = 0
        saw_exit = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("exec completion deadline exceeded")
            # The event endpoint waits on a condition and returns status with
            # output. Avoid 50 new HTTP requests/second for each active probe.
            wait = (
                0.0
                if session["status"] in {"exited", "failed"}
                else min(30.0, remaining / 2)
            )
            batch = self.call(
                f"/v1/exec/{session['id']}/events?after={after}&limit=100&wait_seconds={wait}",
                timeout=remaining,
            )
            events = batch["events"]
            session = batch["session"]
            if session is None:
                raise RuntimeError(
                    "exec session disappeared before output was consumed"
                )
            for event in events:
                sequence = event["sequence"]
                if type(sequence) is not int or sequence != after + 1:
                    raise RuntimeError(
                        "exec event history has a gap or is out of order"
                    )
                after = sequence
                stream = event.get("stream")
                if stream == "stdout":
                    output.append(str(event.get("data", "")))
                elif stream == "stderr":
                    errors.append(str(event.get("data", "")))
                elif stream == "exit":
                    saw_exit = True
            if session["status"] in {"exited", "failed"}:
                # Status can become terminal after the endpoint snapshots a
                # page of events. Drain through EXIT before trusting output.
                if saw_exit:
                    break
                if not events and wait == 0:
                    raise RuntimeError("exec ended without its terminal event")
        stdout = "".join(output)
        stderr = "".join(errors)
        if session.get("exit_code") != 0:
            raise RuntimeError(f"workload exec failed: {stderr}")
        value = json.loads(stdout)
        if "error" in value:
            raise RuntimeError(f"workload validation failed: {value['error']}")
        if isinstance(node_timings, dict):
            value["node_exec_start_timings"] = node_timings
        return value


def summary(samples):
    if not samples:
        return {}
    ordered = sorted(samples)
    return {
        "count": len(ordered),
        **{
            f"p{p}_ms": round(ordered[math.ceil(p / 100 * len(ordered)) - 1], 3)
            for p in (50, 95, 99)
        },
        "max_ms": round(ordered[-1], 3),
    }


def check_identity(before, after, *, advanced):
    if any(
        after.get(k) != before.get(k)
        for k in ("nonce", "pid", "resident_bytes", "dirty_mb", "dirty_page_count")
    ):
        raise RuntimeError(
            "wake replaced the process or changed its resident working set"
        )
    if after.get("counter") != before["counter"] + int(advanced):
        raise RuntimeError("persistent workload counter was lost or duplicated")


def check_node(heartbeat, inventory, expected_cpus):
    if inventory["sandboxes"]:
        raise RuntimeError("benchmark requires an idle dedicated test node")
    metrics = heartbeat.get("runtime_metrics") or {}
    if metrics.get("cpu_count") != expected_cpus:
        raise RuntimeError(
            f"expected {expected_cpus} physical node vCPUs, got {metrics.get('cpu_count')}"
        )
    if not {"direct-runsc-v1", "hibernate-local-v2"}.issubset(
        heartbeat.get("capabilities", [])
    ):
        raise RuntimeError("URL must address a direct node with local hibernation")
    storage_errors = storage_quiescence_errors(heartbeat)
    if storage_errors:
        raise RuntimeError(
            "benchmark requires idle storage: " + "; ".join(storage_errors)
        )


def check_running_residents(heartbeat, inventory, ids):
    records = inventory["sandboxes"]
    if len(records) != len(ids) or {record["id"] for record in records} != set(ids):
        raise RuntimeError("resident inventory does not match the target count")
    if any(record.get("state") != "running" for record in records):
        raise RuntimeError("not every target sandbox is simultaneously running")
    if heartbeat.get("inventory_complete") is not True:
        raise RuntimeError("running sandbox inventory is incomplete")
    active = heartbeat.get("active_sandboxes")
    if type(active) is not int or active != len(ids):
        raise RuntimeError(
            f"simultaneously active sandbox count is {active!r}, expected {len(ids)}"
        )
    creates = heartbeat.get("active_sandbox_creates")
    if type(creates) is not int or creates != 0:
        raise RuntimeError(f"resident create quiescence is unconfirmed: {creates!r}")


def storage_quiescence_errors(heartbeat):
    """An empty sandbox inventory alone cannot prove retired owners are gone."""
    metrics = heartbeat.get("runtime_metrics") or {}
    required = (
        {"storage_ublk_active_devices", "storage_error_volumes"}
        if "storage-native-v1" in heartbeat.get("capabilities", [])
        else set()
    )
    errors = []
    for key in (
        "storage_ublk_active_devices",
        "storage_error_volumes",
        "storage_hard_reserved_mb",
    ):
        if key not in metrics and key not in required:
            continue
        value = metrics.get(key)
        if type(value) not in (int, float) or value != 0:
            errors.append(f"storage quiescence is unconfirmed: {key}={value!r}")
    return errors


def snapshot(api, *, timeout=None):
    options = {} if timeout is None else {"timeout": timeout}
    heartbeat = api.call("/v1/heartbeat", **options)["heartbeat"]
    return {
        key: heartbeat.get(key)
        for key in (
            "node_id",
            "node_epoch",
            "agent_version",
            "deployment_id",
            "init_version",
            "capabilities",
            "total_resources",
            "used_resources",
            "runtime_metrics",
            "active_sandboxes",
            "active_sandbox_creates",
            "inventory_complete",
        )
    }


def cleanup_owned_sandboxes(api, ids, evidence, *, timeout_seconds):
    """Resolve late creates before declaring the unique test IDs cleaned up."""
    deadline = time.monotonic() + timeout_seconds
    last_errors = {}
    owned_ids = set(ids)
    storage_errors = []

    def remaining_timeout():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            details = [*last_errors.values(), *storage_errors]
            raise TimeoutError(
                "cleanup deadline expired before quiescence was verified"
                + (": " + "; ".join(details) if details else "")
            )
        return remaining

    def delete_owned(record):
        try:
            api.call(
                f"/v1/sandboxes/{record['id']}",
                method="DELETE",
                headers={
                    "X-UCloud-Sandbox-Generation": str(record["generation"]),
                    "X-UCloud-Sandbox-Operation-Id": "delete-" + record["id"],
                },
                timeout=remaining_timeout(),
            )
            return record["id"], None
        except Exception as exc:
            return record["id"], str(exc)

    while True:
        inventory = api.call("/v1/sandboxes", timeout=remaining_timeout())["sandboxes"]
        owned = [record for record in inventory if record["id"] in owned_ids]
        # Finish every submitted deletion before sampling quiescence. Each
        # worker uses the remaining global deadline when its request starts.
        with ThreadPoolExecutor(max_workers=8) as pool:
            for sandbox_id, error in pool.map(delete_owned, owned):
                if error is None:
                    last_errors.pop(sandbox_id, None)
                else:
                    last_errors[sandbox_id] = error
        # Read pressure before inventory: a create that finishes while taking
        # this sample must still appear in the following inventory read.
        final = snapshot(api, timeout=remaining_timeout())
        remaining = api.call("/v1/sandboxes", timeout=remaining_timeout())["sandboxes"]
        evidence["remaining_owned_ids"] = [
            r["id"] for r in remaining if r["id"] in owned_ids
        ]
        evidence["snapshots"]["final"] = final
        inflight = final.get("active_sandbox_creates")
        storage_errors = storage_quiescence_errors(final)
        if (
            type(inflight) is int
            and inflight == 0
            and not evidence["remaining_owned_ids"]
            and not storage_errors
        ):
            return
        if time.monotonic() >= deadline:
            evidence["cleanup_errors"].extend(last_errors.values())
            evidence["cleanup_errors"].extend(storage_errors)
            if evidence["remaining_owned_ids"]:
                evidence["cleanup_errors"].append("owned sandboxes remain")
            if type(inflight) is not int or inflight != 0:
                evidence["cleanup_errors"].append(
                    f"create quiescence is unconfirmed: active_sandbox_creates={inflight!r}"
                )
            return
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))


def persist_evidence(path, evidence):
    """Publish a complete report atomically after each completed phase."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, prefix="." + path.name + ".", delete=False
        ) as stream:
            temporary = stream.name
            json.dump(evidence, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            os.unlink(temporary)


def run(args, api, *, persist=None):
    prefix = "density-" + uuid4().hex[:16]
    ids = [f"{prefix}-{i}" for i in range(args.count)]
    evidence = {
        "schema": 1,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "run_id": prefix,
        "owned_ids": ids,
        "status": "running",
        "configuration": {
            key: getattr(args, key)
            for key in (
                "count",
                "expected_cpus",
                "concurrency",
                "create_concurrency",
                "cycles",
                "memory_mb",
                "resident_mb",
                "dirty_mb",
                "dirty_page_count",
                "cpus",
                "disk_mb",
                "cpu_ms",
                "image",
                "park_p95_ms",
                "wake_p95_ms",
                "act_p95_ms",
                "completion_p95_ms",
                "burst_ms",
                "cleanup_timeout_seconds",
            )
        },
        "workload_sha256": hashlib.sha256(WORKLOAD.encode()).hexdigest(),
        "phases": [],
        "snapshots": {},
        "errors": [],
        "slo_violations": [],
        "cleanup_errors": [],
    }
    records, states = {}, {}

    def save():
        if persist is not None:
            persist(evidence)

    def phase(name, items, action, workers):
        started = time.monotonic()
        phase_result = {
            "name": name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "samples": [],
        }
        evidence["phases"].append(phase_result)

        def one(sandbox_id):
            begin = time.monotonic()
            result = action(sandbox_id)
            return {
                "id": sandbox_id,
                "service_ms": (time.monotonic() - begin) * 1000,
                "completion_ms": (time.monotonic() - started) * 1000,
                "result": result,
            }

        # Drain all submitted actions before cleanup, including when one fails.
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(one, sandbox_id) for sandbox_id in items]
                failures = []
                for future in futures:
                    try:
                        phase_result["samples"].append(future.result())
                    except Exception as exc:
                        failures.append(str(exc))
                phase_result["errors"] = failures
        finally:
            phase_result["finished_at"] = datetime.now(timezone.utc).isoformat()
        phase_result["makespan_ms"] = (time.monotonic() - started) * 1000
        phase_result["service"] = summary(
            [s["service_ms"] for s in phase_result["samples"]]
        )
        phase_result["completion"] = summary(
            [s["completion_ms"] for s in phase_result["samples"]]
        )
        kind = name.split("_")[0]
        limit = {
            "act": args.act_p95_ms,
            "park": args.park_p95_ms,
            "wake": args.wake_p95_ms,
        }.get(kind)
        if limit is not None:
            # Keep measured failures even if this or a later phase errors.
            # Successful-call percentiles do not make a partial phase pass.
            if phase_result["service"].get("p95_ms", 0) > limit:
                evidence["slo_violations"].append(
                    f"{name}: service p95 exceeds {limit} ms"
                )
            if (
                phase_result["completion"].get("p95_ms", 0)
                > args.completion_p95_ms
            ):
                evidence["slo_violations"].append(
                    f"{name}: completion p95 exceeds {args.completion_p95_ms} ms"
                )
            if phase_result["makespan_ms"] > args.burst_ms:
                evidence["slo_violations"].append(
                    f"{name}: burst exceeds {args.burst_ms} ms"
                )
        save()
        if failures:
            raise RuntimeError(
                f"{name}: {len(failures)} operations failed: {failures[0]}"
            )
        return phase_result

    def create(sandbox_id):
        payload = {
            "id": sandbox_id,
            "image": args.image,
            "cpus": args.cpus,
            "memory_mb": args.memory_mb,
            "disk_mb": args.disk_mb,
            "command": [
                "python3",
                "-c",
                WORKLOAD,
                str(args.resident_mb),
                str(args.dirty_mb),
            ],
            "parkable": True,
            "ttl_seconds": 3600,
            "security": {"user": "0:0", "init": False},
            "filesystem": {"workspace_storage": "image"},
        }
        spec = SandboxSpec.from_dict(payload)
        payload["_ucloud_operation"] = {
            "kind": "create",
            "generation": 1,
            "operation_id": "create-" + sandbox_id,
            "spec_hash": sandbox_spec_fingerprint(spec),
        }
        response = api.call(
            "/v1/sandboxes",
            method="POST",
            payload=payload,
        )
        records[sandbox_id] = response["sandbox"]
        state = api.probe(sandbox_id)
        if state["resident_bytes"] != args.resident_mb * 1024 * 1024:
            raise RuntimeError("working set size mismatch")
        if (
            state.get("dirty_mb") != args.dirty_mb
            or state.get("dirty_page_count") != args.dirty_page_count
        ):
            raise RuntimeError("dirty working set size mismatch")
        states[sandbox_id] = state
        return state

    def act(sandbox_id):
        state = api.probe(sandbox_id, act=True, cpu_ms=args.cpu_ms)
        check_identity(states[sandbox_id], state, advanced=True)
        states[sandbox_id] = state
        return state

    def park(sandbox_id):
        result = api.call(
            f"/v1/sandboxes/{sandbox_id}/park",
            method="POST",
            payload={
                "operation_id": f"{prefix}-park-{states[sandbox_id]['counter']}-{sandbox_id}",
            },
        )["sandbox"]
        if result["state"] != "parked":
            raise RuntimeError("park did not reach parked state")
        return {"state": result["state"]}

    def wake(sandbox_id):
        api.call(
            f"/v1/sandboxes/{sandbox_id}/wake",
            method="POST",
            payload={
                "generation": records[sandbox_id]["generation"],
                "operation_id": f"{prefix}-wake-{states[sandbox_id]['counter']}-{sandbox_id}",
            },
        )
        # Time through useful work, including memory page faults and verification.
        return act(sandbox_id)

    try:
        save()
        evidence["snapshots"]["baseline"] = snapshot(api)
        check_node(
            evidence["snapshots"]["baseline"],
            api.call("/v1/sandboxes"),
            args.expected_cpus,
        )
        evidence["image_pull"] = api.call(
            "/v1/images/pull",
            method="POST",
            payload={"image": args.image, "id": prefix},
        )
        phase("single_create", ids[:1], create, 1)
        phase("single_act", ids[:1] * 3, act, 1)
        phase("create", ids[1:], create, args.create_concurrency)
        evidence["snapshots"]["resident"] = snapshot(api)
        check_running_residents(
            evidence["snapshots"]["resident"], api.call("/v1/sandboxes"), ids
        )
        for cycle in range(args.cycles):
            phase(f"act_{cycle}", ids, act, args.concurrency)
            phase(f"park_{cycle}", ids, park, args.concurrency)
            evidence["snapshots"][f"parked_{cycle}"] = snapshot(api)
            parked = api.call("/v1/sandboxes")["sandboxes"]
            if len(parked) != args.count or any(r["state"] != "parked" for r in parked):
                raise RuntimeError("not every resident sandbox reached parked state")
            phase(f"wake_{cycle}", ids, wake, args.concurrency)
            evidence["snapshots"][f"awake_{cycle}"] = snapshot(api)
            check_running_residents(
                evidence["snapshots"][f"awake_{cycle}"],
                api.call("/v1/sandboxes"),
                ids,
            )
        evidence["status"] = "failed" if evidence["slo_violations"] else "passed"
    except (Exception, KeyboardInterrupt, SystemExit) as exc:
        evidence["errors"].append(str(exc) or type(exc).__name__)
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            evidence["interrupted"] = True
        evidence["status"] = "failed"
    finally:
        try:
            save()
        except Exception as exc:
            evidence["errors"].append(f"persisting partial evidence failed: {exc}")
            evidence["status"] = "failed"
        try:
            cleanup_owned_sandboxes(
                api, ids, evidence, timeout_seconds=args.cleanup_timeout_seconds
            )
        except (Exception, KeyboardInterrupt, SystemExit) as exc:
            evidence["cleanup_errors"].append(str(exc) or type(exc).__name__)
        if evidence["cleanup_errors"]:
            evidence["status"] = "failed"
        evidence["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
    return evidence


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-url", required=True)
    parser.add_argument("--node-token-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--image",
        default="python@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285",
    )
    for name, default in (
        ("count", 128),
        ("expected-cpus", 32),
        ("concurrency", 32),
        ("create-concurrency", 8),
        ("cycles", 3),
        ("memory-mb", 1024),
        ("resident-mb", 256),
        ("disk-mb", 2048),
        ("cpu-ms", 100),
    ):
        parser.add_argument("--" + name, type=int, default=default)
    parser.add_argument(
        "--dirty-mb",
        type=int,
        default=None,
        help="MiB of pages dirtied per action, rotating through all resident pages (default: resident-mb)",
    )
    for name, default in (
        ("cpus", 1.0),
        ("park-p95-ms", 2000),
        ("wake-p95-ms", 3000),
        ("act-p95-ms", 2000),
        ("completion-p95-ms", 10000),
        ("burst-ms", 15000),
        ("cleanup-timeout-seconds", 180),
    ):
        parser.add_argument("--" + name, type=float, default=default)
    args = parser.parse_args(argv)
    if args.dirty_mb is None:
        args.dirty_mb = args.resident_mb
    for key, value in vars(args).items():
        if isinstance(value, (int, float)) and (not math.isfinite(value) or value <= 0):
            parser.error(f"{key} must be finite and positive")
    if args.resident_mb + 128 > args.memory_mb:
        parser.error("memory limit needs at least 128 MiB above the resident fixture")
    if args.dirty_mb > args.resident_mb:
        parser.error("dirty-mb cannot exceed resident-mb")
    args.dirty_page_count = args.dirty_mb * 1024 * 1024 // 4096
    return args


def main():
    args = parse_args()
    token = args.node_token_file.read_text().strip()
    if not token:
        raise ValueError("empty node token")
    # Reserve evidence before provisioning; never overwrite another run.
    with args.output.open("x"):
        pass
    evidence = run(
        args,
        NodeApi(args.node_url, token),
        persist=lambda value: persist_evidence(args.output, value),
    )
    print(
        json.dumps(
            {key: evidence[key] for key in ("status", "errors", "cleanup_errors")}
        )
    )
    return (
        130
        if evidence.get("interrupted")
        else (0 if evidence["status"] == "passed" else 1)
    )


if __name__ == "__main__":
    sys.exit(main())
