#!/usr/bin/env python3
"""Run each test module in its own process, across a process pool.

A module declares its tier (docs/rl-scale-architecture-plan.md, C8.2) with one
top-level string assignment, read from the source without importing it:

    TEST_TIER = "contract"

unit      pure tests, no I/O beyond temporary directories; the tier of every
          module that assigns nothing.
contract  real SQLite stores and PostgreSQL (UCLOUD_TEST_POSTGRES_DSN), real
          localhost HTTP, child processes, the SDK, local service containers.
linux     root, runsc, ublk, EROFS or nftables.
live      against a deployment; explicit operator action only.

Tiers select work; they never replace a module's own skip conditions, so a
module still skips with a reason when its dependency is absent.

Each module runs as ``python -m scripts.run_tests --child MODULE``, so sys.path
is exactly what ``python -m unittest MODULE`` gives and no module can rely on
another having been imported first. A per-test watchdog dumps every thread's
stack and exits, so a hung test fails its module instead of the run.
"""
from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
import faulthandler
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS = REPO_ROOT / "tests"
TIERS = ("unit", "contract", "linux", "live")
REPORT_SCHEMA = "ucloud-sandboxes.test-run.v1"
_GRACE_SECONDS = 10.0
_ACTIVE: set[subprocess.Popen] = set()
_ACTIVE_LOCK = threading.Lock()
_STOPPING = threading.Event()


def module_tier(path: Path) -> str:
    tiers = []
    for node in ast.parse(path.read_bytes(), filename=str(path)).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "TEST_TIER"
            for target in node.targets
        ):
            if not (
                len(node.targets) == 1
                and isinstance(node.value, ast.Constant)
                and node.value.value in TIERS
            ):
                raise SystemExit(f"{path}: TEST_TIER must be one of {TIERS}")
            tiers.append(node.value.value)
    if len(tiers) > 1:
        raise SystemExit(f"{path}: TEST_TIER is assigned more than once")
    return tiers[0] if tiers else "unit"


def discover() -> dict[str, tuple[Path, str]]:
    return {
        f"tests.{path.stem}": (path, module_tier(path))
        for path in sorted(TESTS.glob("test_*.py"))
    }


def module_name(argument: str) -> str:
    if argument.endswith(".py"):
        path = Path(argument).resolve()
        if path.parent != TESTS:
            raise SystemExit(f"{argument}: not a module directly under tests/")
        return f"tests.{path.stem}"
    return argument


_RANK = ("pass", "skip", "expected_failure", "unexpected_success", "fail", "error")
_FAILED = ("fail", "error", "unexpected_success")


class _RecordingResult(unittest.TextTestResult):
    """Append one JSON line per event, so a killed child still names its test."""

    events = None
    test_timeout = 0.0
    _current = None

    def startTest(self, test):
        self._current = test.id()
        self._started = time.perf_counter()
        self._outcome, self._details = "pass", []
        self._emit({"start": self._current})
        if self.test_timeout:
            faulthandler.dump_traceback_later(self.test_timeout, exit=True)
        super().startTest(test)

    def stopTest(self, test):
        super().stopTest(test)
        faulthandler.cancel_dump_traceback_later()
        self._emit({
            "test": self._current,
            "outcome": self._outcome,
            "seconds": round(time.perf_counter() - self._started, 4),
            "detail": "\n".join(self._details),
        })
        self._current = None

    def _emit(self, event):
        self.events.write(json.dumps(event) + "\n")
        self.events.flush()

    def _report(self, test, outcome, detail):
        if test.id() != self._current:
            # Class and module fixtures fail outside startTest/stopTest.
            self._emit({"test": test.id(), "outcome": outcome, "seconds": 0.0,
                        "detail": detail})
            return
        if _RANK.index(outcome) > _RANK.index(self._outcome):
            self._outcome = outcome
        if detail:
            self._details.append(detail)

    def addError(self, test, err):
        super().addError(test, err)
        self._report(test, "error", self._exc_info_to_string(err, test))

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._report(test, "fail", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._report(test, "skip", reason)

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self._report(test, "expected_failure", "")

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self._report(test, "unexpected_success", "")

    def addSubTest(self, test, subtest, err):
        super().addSubTest(test, subtest, err)
        if err is not None:
            failed = issubclass(err[0], test.failureException)
            self._report(test, "fail" if failed else "error",
                         subtest.id() + "\n" + self._exc_info_to_string(err, test))


def _exit_with_runner(runner: int) -> None:
    # Each child leads its own session, so a runner killed outright cannot
    # reap it; the child then kills its own group, grandchildren included.
    while os.getppid() == runner:
        time.sleep(1)
    os.killpg(0, signal.SIGKILL)


def run_child(module: str, events_path: str, test_timeout: float) -> int:
    threading.Thread(
        target=_exit_with_runner, args=(os.getppid(),), daemon=True,
    ).start()
    with open(events_path, "a", encoding="utf-8") as events:
        _RecordingResult.events = events
        _RecordingResult.test_timeout = test_timeout
        suite = unittest.defaultTestLoader.loadTestsFromName(module)
        # The warning filter python -m unittest installs, so outcomes match.
        result = unittest.TextTestRunner(
            stream=sys.stderr, resultclass=_RecordingResult, verbosity=1,
            warnings=None if sys.warnoptions else "default",
        ).run(suite)
        events.write(json.dumps({"done": result.testsRun}) + "\n")
    return 0 if result.wasSuccessful() else 1


def _signal_group(process: subprocess.Popen, number: int) -> bool:
    try:
        os.killpg(process.pid, number)
    except ProcessLookupError:
        return False
    return True


def run_module(module, *, tier, test_timeout, module_timeout, wrapper, workdir):
    events_path = Path(workdir) / f"{module}.jsonl"
    output_path = Path(workdir) / f"{module}.log"
    command = [
        *wrapper, sys.executable, "-X", "faulthandler", "-m", "scripts.run_tests",
        "--child", module, "--events", str(events_path),
        "--test-timeout", str(test_timeout),
    ]
    started = time.monotonic()
    timed_out = False
    with _ACTIVE_LOCK:
        if _STOPPING.is_set():
            return None
        # Its own session: a timeout or leaked grandchild is reaped as a group.
        # The session leaves the terminal too, so an interrupted run kills it.
        with open(output_path, "wb") as output:
            process = subprocess.Popen(
                command, cwd=REPO_ROOT, stdin=subprocess.DEVNULL, stdout=output,
                stderr=subprocess.STDOUT, start_new_session=True,
            )
        _ACTIVE.add(process)
    try:
        returncode = process.wait(timeout=module_timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        # SIGABRT makes faulthandler dump every thread before exiting.
        _signal_group(process, signal.SIGABRT)
        try:
            returncode = process.wait(timeout=_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            _signal_group(process, signal.SIGKILL)
            returncode = process.wait()
    seconds = time.monotonic() - started
    with _ACTIVE_LOCK:
        _ACTIVE.discard(process)
    # multiprocessing's resource tracker exits shortly after its parent; only
    # a process still in the group after a grace period counts as leaked.
    leaked = False
    for _ in range(40):
        if not _signal_group(process, 0):
            break
        time.sleep(0.05)
    else:
        leaked = _signal_group(process, signal.SIGKILL)
    tests, started_tests, done = {}, [], None
    if events_path.exists():
        for line in events_path.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            if "start" in event:
                started_tests.append(event["start"])
            elif "done" in event:
                done = event["done"]
            else:
                tests.setdefault(event["test"], []).append(event)
    unfinished = [test for test in started_tests if test not in tests]
    output = output_path.read_text(encoding="utf-8", errors="replace")
    results = [
        {
            "test": test,
            "outcome": max((e["outcome"] for e in recorded), key=_RANK.index),
            "seconds": sum(event["seconds"] for event in recorded),
            "detail": "\n".join(e["detail"] for e in recorded if e["detail"]),
        }
        for test, recorded in tests.items()
    ]
    for test in unfinished:
        # The output tail holds faulthandler's dump of every thread.
        results.append({
            "test": test, "outcome": "error", "seconds": 0.0,
            "detail": "the test never finished (watchdog, crash or module "
                      "timeout); module output tail:\n" + output[-20000:],
        })
    failed = [r for r in results if r["outcome"] in _FAILED]
    if timed_out:
        status = "timeout"
    elif done is None or returncode != 0 or failed:
        status = "fail"
    else:
        status = "pass"
    return {
        "module": module,
        "tier": tier,
        "status": status,
        "returncode": returncode,
        "seconds": round(seconds, 3),
        "tests": len(results),
        "skipped": sum(r["outcome"] == "skip" for r in results),
        "leaked_processes": leaked,
        "results": results,
        "output": output if status != "pass" else "",
    }


def _summary_line(report, index, total):
    return (
        f"[{index:>{len(str(total))}}/{total}] {report['status'].upper():7} "
        f"{report['module']} ({report['tests']} tests, {report['skipped']} "
        f"skipped, {report['seconds']:.2f} s)"
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("modules", nargs="*", help="tests.test_x or tests/test_x.py")
    parser.add_argument("--tier", action="append", choices=TIERS,
                        help="select a tier (repeatable; default: all tiers)")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--json", type=Path, help="write the timing report here")
    parser.add_argument("--test-timeout", type=float, default=120.0,
                        help="per-test watchdog in seconds (0 disables it)")
    parser.add_argument("--module-timeout", type=float, default=600.0)
    parser.add_argument("--no-fsync", action="store_true",
                        help="run modules under eatmydata (fsync becomes a no-op)")
    parser.add_argument("--list", action="store_true",
                        help="print the selected modules and tiers, then exit")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--events", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        return run_child(args.child, args.events, args.test_timeout)
    if args.jobs < 1:
        parser.error("--jobs must be at least 1")
    # Turn termination into an exception so the child groups are killed.
    signal.signal(signal.SIGTERM, lambda number, _frame: sys.exit(128 + number))

    modules = discover()
    if args.modules:
        selected = []
        for name in map(module_name, args.modules):
            if name not in modules:
                parser.error(f"unknown test module {name}")
            selected.append(name)
    else:
        selected = list(modules)
    if args.tier:
        selected = [name for name in selected if modules[name][1] in args.tier]
    if args.list:
        for name in selected:
            print(f"{modules[name][1]:8} {name}")
        return 0
    if not selected:
        parser.error("no test modules selected")
    wrapper = []
    if args.no_fsync:
        eatmydata = shutil.which("eatmydata")
        if eatmydata is None:
            parser.error("--no-fsync requires eatmydata on PATH")
        wrapper = [eatmydata]
    # Larger modules usually run longest; starting them first shortens the tail.
    selected.sort(key=lambda name: modules[name][0].stat().st_size, reverse=True)

    started = time.monotonic()
    reports, lock = [], threading.Lock()
    with tempfile.TemporaryDirectory(prefix="ucloud-run-tests-") as workdir:
        def run(name):
            report = run_module(
                name, tier=modules[name][1], test_timeout=args.test_timeout,
                module_timeout=args.module_timeout, wrapper=wrapper,
                workdir=workdir,
            )
            if report is None:
                return
            with lock:
                reports.append(report)
                print(_summary_line(report, len(reports), len(selected)),
                      flush=True)

        pool = ThreadPoolExecutor(max_workers=args.jobs)
        try:
            for future in [pool.submit(run, name) for name in selected]:
                future.result()
        except BaseException:
            with _ACTIVE_LOCK:
                _STOPPING.set()
                for process in _ACTIVE:
                    _signal_group(process, signal.SIGKILL)
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
    wall = time.monotonic() - started

    failed = [r for r in reports if r["status"] != "pass"]
    for report in failed:
        print(f"\n===== {report['status'].upper()}: {report['module']} "
              f"(exit {report['returncode']})")
        bad = [r for r in report["results"] if r["outcome"] in _FAILED]
        for result in bad:
            print(f"--- {result['outcome'].upper()}: {result['test']}")
            print(result["detail"].rstrip())
        if not bad or report["status"] == "timeout":
            print(report["output"][-20000:].rstrip())
    leaked = sorted(r["module"] for r in reports if r["leaked_processes"])
    counts = {
        outcome: sum(
            result["outcome"] == outcome
            for report in reports for result in report["results"]
        )
        for outcome in _RANK
    }
    slowest = sorted(reports, key=lambda r: r["seconds"], reverse=True)[:10]
    print("\nslowest modules: " + ", ".join(
        f"{r['module']} {r['seconds']:.1f} s" for r in slowest))
    if leaked:
        print("modules that left processes running (killed): " + ", ".join(leaked))
    print(
        f"{len(reports)} modules, {sum(counts.values())} tests: "
        + ", ".join(f"{count} {name}" for name, count in counts.items() if count)
        + f"; {wall:.1f} s wall, {sum(r['seconds'] for r in reports):.1f} s "
        f"module time, {args.jobs} jobs"
    )
    if args.json:
        args.json.write_text(json.dumps({
            "schema": REPORT_SCHEMA,
            "wall_seconds": round(wall, 3),
            "jobs": args.jobs,
            "tiers": args.tier or list(TIERS),
            "no_fsync": args.no_fsync,
            "python": sys.version.split()[0],
            "counts": counts,
            "modules": sorted(reports, key=lambda r: r["module"]),
        }, indent=1) + "\n", encoding="utf-8")
    if failed:
        print("FAILED modules: " + " ".join(sorted(r["module"] for r in failed)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
