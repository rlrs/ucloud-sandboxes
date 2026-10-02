#!/usr/bin/env python3
"""Exec start latency and stream throughput: the guest agent against today's path.

Both run on this host without gVisor and without production access.

* agent: GuestAgentListener against the Go agent built from
  runtime/managed_process, one connection, as in a sandbox.
* today: ExecSessionManager (sandbox_exec.py) with a runtime whose
  exec_command returns the argv unchanged: a host Popen plus three Python
  threads per exec, text-mode pipes. A real exec also runs `runsc exec`,
  measured at 24 ms median for `true` on a running sandbox (qualification
  2026-10-02), so these "today" numbers are a lower bound on its cost.

Run: uv run python scripts/benchmark_guest_agent.py [--json out.json]
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
from tempfile import mkdtemp
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ucloud_sandboxes.guest_agent import ExecExit, ExecOutput, GuestAgentListener  # noqa: E402
from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec  # noqa: E402

UID, GID = os.getuid(), os.getgid()


class _Lifecycle:
    def acquire_shared(self, sandbox_id: str) -> None:
        pass

    release_shared = acquire_shared


class _Runtime:
    def exec_command(self, sandbox_id, command, **_options):
        return tuple(command)

    def exec_started(self, sandbox_id: str) -> None:
        pass

    exec_start_failed = exec_started


class _SandboxManager:
    lifecycle = _Lifecycle()
    runtime = _Runtime()

    def acquire_exec_capacity(self, sandbox_id: str) -> object:
        return object()

    def release_exec_capacity(self, lease: object) -> None:
        pass


def percentiles(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    return {
        "p50_ms": round(statistics.median(ordered) * 1000, 3),
        "p99_ms": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))] * 1000, 3),
    }


class AgentPath:
    def __init__(self, listener: GuestAgentListener) -> None:
        self.listener = listener

    def start(self, argv):
        return self.listener.start_exec(argv, env={}, cwd="/", uid=UID, gid=GID)

    def round_trip(self, argv) -> None:
        exit_status = self.start(argv).communicate(timeout=60)[0]
        assert exit_status == ExecExit(0, None, True), exit_status

    def stream(self, argv) -> int:
        received = 0
        for event in self.start(argv):
            if isinstance(event, ExecOutput):
                received += len(event.data)
        return received


class TodayPath:
    def __init__(self) -> None:
        self.manager = ExecSessionManager(_SandboxManager(), max_sessions=1 << 16, completed_retention_seconds=0)

    def start(self, argv):
        return self.manager.start(SandboxExecSpec(sandbox_id="bench", command=tuple(argv)))

    def drain(self, session, sink=None) -> int:
        after, exit_code = 0, None
        while exit_code is None:
            for event in self.manager.events_after(session.id, after=after, limit=100, wait_seconds=5):
                after = event.sequence
                if event.stream in {"stdout", "stderr"} and sink is not None:
                    sink.append(len(event.data))
                if event.stream == "exit":
                    exit_code = event.exit_code
        return exit_code

    def round_trip(self, argv) -> None:
        assert self.drain(self.start(argv)) == 0

    def stream(self, argv) -> int:
        sizes: list[int] = []
        self.drain(self.start(argv), sizes)
        return sum(sizes)


def measure(path, *, starts: int, seconds: float, threads: list[int], stream_bytes: int) -> dict:
    result: dict = {}
    for _ in range(20):
        path.round_trip(["true"])
    start_samples, trip_samples = [], []
    for _ in range(starts):
        began = time.perf_counter()
        session = path.start(["true"])
        start_samples.append(time.perf_counter() - began)
        if isinstance(path, AgentPath):
            session.communicate(timeout=60)
        else:
            path.drain(session)
        trip_samples.append(time.perf_counter() - began)
    result["exec_start"] = percentiles(start_samples)
    result["exec_round_trip"] = percentiles(trip_samples)
    for count in threads:
        deadline = time.monotonic() + seconds
        cpu = time.process_time()

        def worker() -> int:
            done = 0
            while time.monotonic() < deadline:
                path.round_trip(["true"])
                done += 1
            return done

        began = time.monotonic()
        with ThreadPoolExecutor(count) as pool:
            completed = sum(pool.map(lambda _: worker(), range(count)))
        elapsed = time.monotonic() - began
        result[f"execs_per_second_{count}_threads"] = round(completed / elapsed)
        result[f"node_cpu_ms_per_exec_{count}_threads"] = round((time.process_time() - cpu) * 1000 / completed, 3)
    began = time.monotonic()
    cpu = time.process_time()
    received = path.stream(["head", "-c", str(stream_bytes), "/dev/zero"])
    elapsed = time.monotonic() - began
    assert received == stream_bytes, (received, stream_bytes)
    result["stdout_mb_per_second"] = round(received / elapsed / 1e6, 1)
    result["node_cpu_s_per_gb"] = round((time.process_time() - cpu) / (received / 1e9), 3)
    return result


def binary_check(agent: AgentPath, today: TodayPath) -> dict:
    payload = bytes(range(256))
    argv = ["/bin/sh", "-c", r"printf '" + "".join(f"\\{value:03o}" for value in payload) + "'"]
    stdout = agent.start(argv).communicate(timeout=10)[1]
    session = today.start(argv)
    today.drain(session)
    text = "".join(event.data for event in today.manager.events_after(session.id, after=0, limit=1000) if event.stream == "stdout")
    return {"agent_bytes_intact": stdout == payload, "today_bytes_intact": text.encode("utf-8", "surrogateescape") == payload}


def agent_extras(listener: GuestAgentListener, directory: Path, size: int) -> dict:
    data = os.urandom(size)
    path = str(directory / "payload")
    began = time.monotonic()
    listener.write_file(path, data, uid=UID, gid=GID)
    write = time.monotonic() - began
    began = time.monotonic()
    assert listener.read_file(path, max_bytes=size, uid=UID, gid=GID) == data
    read = time.monotonic() - began
    session = listener.start_exec(["sh", "-c", "cat >/dev/null"], env={}, cwd="/", uid=UID, gid=GID, stdin=True)
    began = time.monotonic()
    for offset in range(0, len(data), 1 << 20):
        session.write_stdin(data[offset : offset + (1 << 20)], timeout=60)
    session.close_stdin()
    assert session.communicate(timeout=60)[0] == ExecExit(0, None, True)
    stdin = time.monotonic() - began
    small = []
    for _ in range(200):
        began = time.perf_counter()
        listener.write_file(path, b"x" * 4096, uid=UID, gid=GID)
        small.append(time.perf_counter() - began)
    return {
        "file_write_mb_per_second": round(size / write / 1e6, 1),
        "file_read_mb_per_second": round(size / read / 1e6, 1),
        "stdin_mb_per_second": round(size / stdin / 1e6, 1),
        "file_write_4k": percentiles(small),
    }


def build_agent(directory: Path) -> Path:
    binary = directory / "ucloud-sandbox-init"
    source = Path(__file__).resolve().parents[1] / "runtime/managed_process"
    env = {**os.environ, "CGO_ENABLED": "0", "GOTOOLCHAIN": "local"}
    subprocess.run([shutil.which("go") or "go", "build", "-trimpath", "-o", str(binary), "."], cwd=source, env=env, check=True)
    return binary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--starts", type=int, default=1000)
    parser.add_argument("--seconds", type=float, default=3.0)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--stream-mib", type=int, default=1024)
    parser.add_argument("--today-stream-mib", type=int, default=128)
    parser.add_argument("--file-mib", type=int, default=64)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    directory = Path(mkdtemp(prefix="ga-bench-", dir="/tmp"))
    agent_process = None
    try:
        binary = build_agent(directory)
        listener = GuestAgentListener(directory / "agent.sock").start()
        agent_process = subprocess.Popen([str(binary), "agent", "--connect", str(listener.socket_path)])
        listener.wait_connected(10)
        agent, today = AgentPath(listener), TodayPath()
        report = {
            "host": {"python": platform.python_version(), "cpus": os.cpu_count(), "kernel": platform.release()},
            "agent": measure(agent, starts=args.starts, seconds=args.seconds, threads=args.threads,
                             stream_bytes=args.stream_mib << 20),
            "today": measure(today, starts=args.starts, seconds=args.seconds, threads=args.threads,
                             stream_bytes=args.today_stream_mib << 20),
            "binary": binary_check(agent, today),
        }
        report["agent"].update(agent_extras(listener, directory, args.file_mib << 20))
        listener.close()
    finally:
        if agent_process is not None:
            agent_process.terminate()
            agent_process.wait()
        shutil.rmtree(directory, ignore_errors=True)
    print(json.dumps(report, indent=2))
    if args.json is not None:
        args.json.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
