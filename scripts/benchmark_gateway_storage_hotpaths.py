"""Synthetic local benchmarks against a Git revision; no production access.

Run with PYTHONPATH=. .venv/bin/python scripts/benchmark_gateway_storage_hotpaths.py
"""
from __future__ import annotations

import argparse
import ast
from collections import deque
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import socket
import sqlite3
import statistics
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import tracemalloc

from ucloud_sandboxes import control_plane, storage_native_registry
from ucloud_sandboxes.managed_registry import RegistryUsageStore
from ucloud_sandboxes.storage_native import StorageNativeLayer


def source_at(revision: str, filename: str) -> str:
    return subprocess.check_output(
        ["git", "show", f"{revision}:ucloud_sandboxes/{filename}.py"], text=True,
    )


def previous_function(source: str, name: str, module):
    definition = next(node for node in ast.parse(source).body
                      if isinstance(node, ast.FunctionDef) and node.name == name)
    namespace = dict(vars(module))
    exec(compile(ast.Module(body=[definition], type_ignores=[]), "<baseline>", "exec"), namespace)
    return namespace[name]


def timed(operation, count: int) -> dict:
    cpu, wall = time.process_time(), time.perf_counter()
    for _ in range(count):
        operation()
    return {"cpu_seconds": time.process_time() - cpu,
            "wall_seconds": time.perf_counter() - wall, "count": count}


def registry_benchmark(root: Path, revision: str, repeat: int) -> dict:
    old_path = root / "old_registry.py"
    old_path.write_text(source_at(revision, "managed_registry"))
    spec = importlib.util.spec_from_file_location("ucloud_sandboxes._benchmark_registry", old_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    old_helper = previous_function(source_at(revision, "control_plane"),
                                   "_persist_registry_image_protection", control_plane)
    now = datetime(2026, 9, 21, tzinfo=timezone.utc)
    digest = "sha256:" + "a" * 64
    variants = {}
    for name, cls, helper in (
        ("before", module.RegistryUsageStore, old_helper),
        ("after", RegistryUsageStore, control_plane._persist_registry_image_protection),
    ):
        store = cls(root / f"{name}.sqlite")
        with sqlite3.connect(store.path) as db:
            db.executemany("INSERT INTO registry_leases VALUES (?, ?, ?, ?, ?, ?, ?)", [
                ("repo", "tag", f"sandbox:{i}", now.isoformat(), now.isoformat(), "", digest)
                for i in range(5000)
            ])
            db.executemany("INSERT INTO registry_images VALUES (?, ?, ?, ?)", [
                (f"registry.example/repo:tag{i}", "repo", f"tag{i}", now.isoformat())
                for i in range(500)
            ])
        variants[name] = (store, helper)
    results = {}
    for touch in (False, True):
        samples = {name: [] for name in variants}
        for index in range(repeat):
            for name in (tuple(variants) if index % 2 == 0 else tuple(reversed(variants))):
                store, helper = variants[name]
                samples[name].append(timed(lambda: helper(
                    store, "registry.example/repo:tag@" + digest, "sandbox:0",
                    touch=touch, persistent=True, now=now,
                ), 20))
        medians = {name: statistics.median(sample["wall_seconds"] / sample["count"]
                                         for sample in values)
                   for name, values in samples.items()}
        results["reference_with_touch" if touch else "reference_read"] = {
            "samples": samples, "median_seconds_per_call": medians,
            "speedup": medians["before"] / medians["after"],
        }
    return {"leases": 5000, "images": 500, "operations": results}


def stream_benchmark(root: Path, revision: str, repeat: int) -> dict:
    variants = {
        "before": previous_function(source_at(revision, "storage_native_registry"),
                                    "consume_export_stream", storage_native_registry),
        "after": storage_native_registry.consume_export_stream,
    }
    block = bytes(range(256)) * 1024
    expected_chunk = block * 32
    blocks = 512
    hasher = hashlib.sha256()
    for _ in range(blocks):
        hasher.update(block)
    descriptor = StorageNativeLayer("sha256:" + hasher.hexdigest(), len(block) * blocks)

    def exporter(path):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(path))
            for _ in range(blocks):
                connection.sendall(block)
        return descriptor

    def run(consume_stream):
        retained = deque(maxlen=2)
        result = consume_stream(exporter, stream_socket_root=root, chunk_bytes=8 * 1024**2,
                                timeout_seconds=10, consume=retained.append)
        assert result == descriptor
        assert all(chunk == expected_chunk for chunk in retained)

    samples = {name: [] for name in variants}
    for index in range(repeat):
        for name in (tuple(variants) if index % 2 == 0 else tuple(reversed(variants))):
            samples[name].append(timed(lambda: run(variants[name]), 1))
    peaks = {}
    for name, function in variants.items():
        tracemalloc.start()
        run(function)
        peaks[name] = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    return {"bytes_per_sample": descriptor.size, "chunk_bytes": 8 * 1024**2,
            "retained_upload_chunks": 2, "samples": samples,
            "separate_tracemalloc_peak_bytes": peaks}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="4175036eb97cbdb41c5410742b5785e2c9ae91b1")
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("repeat must be positive")
    with TemporaryDirectory() as raw:
        root = Path(raw)
        print(json.dumps({
            "baseline": args.baseline, "python": platform.python_version(),
            "platform": platform.system(), "repeat": args.repeat,
            "registry": registry_benchmark(root, args.baseline, args.repeat),
            "stream": stream_benchmark(root, args.baseline, args.repeat),
        }, indent=2))


if __name__ == "__main__":
    main()
