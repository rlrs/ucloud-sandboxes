#!/usr/bin/env python3
"""Compare one candidate's four threads with four fresh spawned processes locally.

No production calls. Stage barriers keep validation outside timed work. This
imports fixture/validation helpers from benchmark_environment_preparation.py,
which must be staged alongside this file without modifying that frozen helper.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time

import benchmark_environment_preparation as fixture_tools

COUNT = 4


def participant(connection, source_root, fixture_root, work_root, process_mode):
    """Each worker owns fresh private trees for its entire bounded lifetime."""
    try:
        sys.path.insert(0, str(source_root))
        from ucloud_sandboxes import environment_builder as builder, oci_layer_materialize as materializer
        sources = {}
        for module in (builder, materializer):
            path = Path(module.__file__).resolve()
            if not path.is_relative_to(source_root):
                raise RuntimeError("runtime import escaped candidate root")
            sources[path.name] = fixture_tools.digest(path.read_bytes())
        manifest = json.loads((fixture_root / "fixture.json").read_text())
        if (manifest["uid"], manifest["gid"]) != (os.geteuid(), os.getegid()):
            raise ValueError("fixture and worker ownership differ")

        class Client:
            def open_blob(self, repository, key):
                return (fixture_root / key.removeprefix("sha256:")).open("rb")

        cpu_clock = time.process_time if process_mode else time.thread_time
        with tempfile.TemporaryDirectory(prefix="publication-", dir=work_root) as temporary:
            root = Path(temporary)
            connection.send({"kind": "ready", "sources": sources, "python": platform.python_version(),
                             "platform": platform.platform(), "uid": os.geteuid(), "gid": os.getegid()})
            while True:
                operation = connection.recv()
                if operation == "stop":
                    break
                if operation in ("materialize", "squash"):
                    started, cpu = time.perf_counter(), cpu_clock()
                    if operation == "materialize":
                        directories = materializer.materialize_layers(Client(), "fixture", manifest["layers"],
                                                                       manifest["diff_ids"], root / "materialized")
                    else:
                        builder.squash_layer_diffs(directories, root / "squashed", consume_private_diffs=True)
                    connection.send({"kind": operation, "wall_seconds": time.perf_counter() - started,
                                     "cpu_seconds": cpu_clock() - cpu})
                elif operation == "validate_materialized":
                    connection.send({"kind": operation,
                        "trees": [fixture_tools.compact(fixture_tools.tree_snapshot(path)) for path in directories]})
                elif operation == "validate_output":
                    output = root / "squashed"
                    assert (output / "app/node_modules/package-00000/index-alias.js").read_bytes() == b"exports.value = 0;\n" * 24
                    assert (output / "app/node_modules/package-00000/index.js").read_bytes() == b"exports.value = 10000;\n" * 32
                    assert os.stat(output / "app/dist/run.js").st_ino == os.stat(output / "app/dist/run-alias.js").st_ino
                    combined = fixture_tools.compact(fixture_tools.tree_snapshot(output))
                    lower, layers, exercised, skipped = fixture_tools.semantic_layers(root)
                    builder.squash_layer_diffs(layers, root / "overlay", lower_dirs=[lower])
                    fixture_tools.semantic_checks(root / "overlay", exercised)
                    connection.send({"kind": operation, "tree": combined,
                        "overlay_tree": fixture_tools.compact(fixture_tools.tree_snapshot(root / "overlay", generated_whiteouts=True)),
                        "capabilities_exercised": exercised, "capability_skips": skipped})
                else:
                    raise ValueError("unknown benchmark phase")
        connection.send({"kind": "stop", "temporary_tree_removed": True})
    except BaseException as exc:
        try:
            connection.send({"kind": "error", "error_type": type(exc).__name__})
        except (OSError, EOFError, BrokenPipeError):
            pass
    finally:
        connection.close()


def supervise(args):
    context = multiprocessing.get_context("spawn")
    deadline = time.monotonic() + args.timeout
    workers, channels = [], []
    process_mode = args.mode == "processes"

    def receive(connection, expected):
        if not connection.poll(max(0.0, deadline - time.monotonic())):
            raise TimeoutError(f"worker deadline exceeded during {expected}")
        response = connection.recv()
        if response.get("kind") != expected:
            raise RuntimeError(f"worker failed during {expected}: {response.get('error_type', 'unexpected response')}")
        return response

    def operation(name):
        started = time.perf_counter()
        for connection in channels:
            connection.send(name)
        responses = [receive(connection, name) for connection in channels]
        elapsed = time.perf_counter() - started
        return elapsed, responses

    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="preparation-execution-", dir=args.work_root) as temporary:
        try:
            for _ in range(COUNT):
                parent, child = context.Pipe()
                keywords = dict(target=participant, args=(child, args.source_root.resolve(),
                    args.fixture_root.resolve(), Path(temporary), process_mode), daemon=True)
                worker = context.Process(**keywords) if process_mode else threading.Thread(**keywords)
                worker.start()
                if process_mode:
                    child.close()
                workers.append(worker)
                channels.append(parent)
            ready = [receive(connection, "ready") for connection in channels]
            startup = time.perf_counter() - started
            materialize_wall, materialize = operation("materialize")
            _, before = operation("validate_materialized")
            squash_wall, squash = operation("squash")
            _, after = operation("validate_output")
            cleanup_started = time.perf_counter()
            _, stopped = operation("stop")
            for worker in workers:
                worker.join(timeout=max(0, deadline - time.monotonic()))
                if worker.is_alive() or process_mode and worker.exitcode != 0:
                    raise RuntimeError("worker did not exit cleanly")
            cleanup = time.perf_counter() - cleanup_started
        finally:
            # Only these benchmark children are eligible for termination.
            for worker in workers:
                if process_mode and worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
                    if worker.is_alive():
                        worker.kill()
                        worker.join(timeout=5)
            for connection in channels:
                connection.close()
    sources = {fixture_tools.canonical(item["sources"]) for item in ready}
    if len(sources) != 1:
        raise RuntimeError("workers imported different candidate sources")
    phases = {"materialize": {"wall_seconds": materialize_wall,
                             "cpu_seconds": sum(item["cpu_seconds"] for item in materialize)},
              "squash": {"wall_seconds": squash_wall,
                          "cpu_seconds": sum(item["cpu_seconds"] for item in squash)}}
    result = {"schema": 1, "mode": args.mode, "concurrency": COUNT, "sources": ready[0]["sources"],
              "runtime": {key: ready[0][key] for key in ("python", "platform", "uid", "gid")},
              "startup_to_ready_wall_seconds": startup, "phases": phases,
              "preparation_wall_seconds": materialize_wall + squash_wall,
              "preparation_cpu_seconds": sum(value["cpu_seconds"] for value in phases.values()),
              "startup_plus_preparation_wall_seconds": startup + materialize_wall + squash_wall,
              "cleanup_and_reap_wall_seconds": cleanup,
              "whole_trial_including_validation_seconds": time.perf_counter() - started,
              "workers_reaped_and_trees_removed": all(item["temporary_tree_removed"] for item in stopped),
              "individual_phase_observations": {"materialize": materialize, "squash": squash},
              "materialized_trees": [item["trees"] for item in before],
              "outputs": after,
              "cpu_accounting": "sum of worker process CPU" if process_mode else "sum of worker thread CPU"}
    args.output.write_text(json.dumps(result, indent=2) + "\n")


def run(args):
    if args.output.exists():
        raise ValueError("choose a new output directory")
    args.output.mkdir(parents=True)
    fixture_root = args.output / "fixture"
    fixture_root.mkdir(mode=0o700)
    manifest = fixture_tools.fixture(fixture_root, args.modules)
    runs = []
    for repeat in range(args.repeats):
        for mode in (["threads", "processes"] if repeat % 2 == 0 else ["processes", "threads"]):
            output = args.output / f"{mode}-{repeat}.json"
            command = [sys.executable, str(Path(__file__).resolve()), "--supervisor", "--mode", mode,
                       "--source-root", str(args.source_root.resolve()), "--fixture-root", str(fixture_root.resolve()),
                       "--work-root", str(args.work_root.resolve()), "--output", str(output.resolve()),
                       "--timeout", str(args.timeout)]
            process = subprocess.Popen(command, start_new_session=True)
            try:
                code = process.wait(timeout=args.timeout + 10)
                if code:
                    raise RuntimeError(f"{mode} supervisor exited with code {code}")
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                raise
            result = json.loads(output.read_text())
            result["repeat"] = repeat
            runs.append(result)
            print(mode, repeat, json.dumps({key: result[key] for key in
                ("startup_to_ready_wall_seconds", "preparation_wall_seconds", "preparation_cpu_seconds",
                 "startup_plus_preparation_wall_seconds", "workers_reaped_and_trees_removed")}), flush=True)
    reference = runs[0]
    fields = ("sources", "runtime", "materialized_trees", "outputs", "workers_reaped_and_trees_removed")
    mismatches = [{"mode": row["mode"], "repeat": row["repeat"],
                   "fields": [field for field in fields if row[field] != reference[field]]}
                  for row in runs if any(row[field] != reference[field] for field in fields)]
    measures = ("startup_to_ready_wall_seconds", "preparation_wall_seconds", "preparation_cpu_seconds",
                "startup_plus_preparation_wall_seconds", "cleanup_and_reap_wall_seconds")
    means = {mode: {key: statistics.mean(row[key] for row in runs if row["mode"] == mode) for key in measures}
             for mode in ("threads", "processes")}
    report = {"schema": 1, "fixture": manifest, "exact_semantics_equal": not mismatches,
              "mismatches": mismatches, "means": means, "runs": runs,
              "helper_sha256": fixture_tools.digest(Path(fixture_tools.__file__).read_bytes()),
              "limitations": [
                  "Synthetic candidate-only comparison; no network, registry, Docker, mkfs, signing, or production calls.",
                  "All arms consume fresh private diffs. Four threads share an interpreter; four processes use spawn and separate interpreters.",
                  "Startup includes spawning/imports/readiness and is separate from phase wall time. The subprocess supervisor's own interpreter startup is excluded symmetrically.",
                  "For a cold process-offload estimate compare processes startup_plus_preparation with threads preparation, since the production node is already imported.",
                  "Validation is outside timed stages. Stage barriers isolate each operation but do not reproduce arbitrary production overlap.",
                  "Shared warmed fixture bytes; fresh private output trees. No cache drops. CPU excludes supervisor IPC and startup; it sums each worker's timed CPU.",
                  "Comparison follows helper metadata rules, including normalized generated-whiteout mtime and explicit capability skips.",
                  "Fresh-process costs are measured, not amortized persistent-pool costs. Filesystem contention, Python/runtime versions and hardware can change results."]}
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    if mismatches:
        raise RuntimeError("candidate source or output comparison failed; see summary.json")
    shutil.rmtree(fixture_root)
    print("exact_semantics_equal=true", args.output / "summary.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--modules", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--supervisor", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=("threads", "processes"), help=argparse.SUPPRESS)
    parser.add_argument("--fixture-root", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 1 <= args.modules <= 5000 or not 1 <= args.repeats <= 6 or not 0 < args.timeout <= 600:
        parser.error("modules 1..5000, repeats 1..6, timeout 0..600 required")
    supervise(args) if args.supervisor else run(args)


if __name__ == "__main__":
    main()
