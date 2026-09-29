#!/usr/bin/env python3
"""Keep synthetic inventory traffic off the latency driver's Python event loop."""

import argparse
import asyncio
import json
import hashlib
import os
from pathlib import Path
import signal
import sys
import time

SDK_SRC = Path(__file__).resolve().parents[1] / "ucloud-sandboxes-sdk" / "src"
if SDK_SRC.is_dir():
    sys.path.insert(0, str(SDK_SRC))

from ucloud_sandboxes_sdk import AsyncSandboxClient  # noqa: E402


def status_of(exc):
    for name in ("status_code", "status", "code"):
        value = getattr(exc, name, None)
        if type(value) is int and 100 <= value <= 599:
            return value
    return None


async def list_inventory(client, view="full"):
    """Use public SDK inventory methods; status requires SDK 0.4.33 or newer."""
    if view == "full":
        return await client.list_sandboxes()
    if view != "status":
        raise ValueError("unknown inventory view")
    return await client.list_sandbox_statuses()


async def emit_inventory_load(url, token_file, pollers, duration, parent_pid, view="full"):
    """Same SDK GET and one-second post-response cadence as foreground polls."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    token = Path(token_file).read_text().strip()
    completed = failed = 0
    started = time.monotonic()

    def emit(payload):
        print(json.dumps(payload, separators=(",", ":")), flush=True)

    async with AsyncSandboxClient(url, api_token=token, timeout_seconds=180) as client:

        async def poll(index):
            nonlocal completed, failed
            while not stop.is_set():
                before = time.monotonic()
                try:
                    records = await list_inventory(client, view)
                    sample = {
                        "seconds": time.monotonic() - before,
                        "ok": True,
                        "records": len(records),
                    }
                    # These load clients do not keep a second inventory cache.
                    del records
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failed += 1
                    sample = {
                        "seconds": time.monotonic() - before,
                        "ok": False,
                        "error": type(exc).__name__,
                        "status": status_of(exc),
                    }
                completed += 1
                emit({"event": "poll", "poller": index, **sample})
                await asyncio.sleep(1)

        workers = [asyncio.create_task(poll(index)) for index in range(pollers)]
        emit({"event": "ready", "pid": os.getpid(), "pollers": pollers})
        reason = "stopped"
        try:
            while not stop.is_set():
                if os.getppid() != parent_pid:
                    reason = "parent_disappeared"
                    break
                if time.monotonic() - started >= duration:
                    reason = "duration_limit"
                    break
                for worker in workers:
                    if worker.done():
                        worker.result()
                        raise RuntimeError("inventory poll loop stopped")
                try:
                    await asyncio.wait_for(stop.wait(), 1)
                except asyncio.TimeoutError:
                    pass
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        emit(
            {
                "event": "finished",
                "reason": reason,
                "completed_polls": completed,
                "failed_polls": failed,
                "elapsed_seconds": time.monotonic() - started,
            }
        )


async def isolated_inventory_load(
    url, token_file, pollers, duration, samples, diagnostic, *, view="full"
):
    """Forward bounded numeric load evidence and reap the child during cleanup."""
    process = None
    failed = False

    def failure(kind):
        nonlocal failed
        failed = True
        samples.append(
            {"seconds": 0.0, "ok": False, "source": "isolated", "error": kind}
        )
        diagnostic["failed"] = True

    def consume(line):
        payload = json.loads(line)
        event = payload["event"]
        if event == "poll":
            if type(payload.get("ok")) is not bool or not isinstance(
                payload.get("seconds"), (int, float)
            ):
                raise ValueError("invalid poll evidence")
            samples.append(
                {key: value for key, value in payload.items() if key != "event"}
                | {"source": "isolated"}
            )
        elif event == "ready":
            if payload["pollers"] != pollers:
                raise ValueError("incorrect isolated poller count")
            diagnostic.update(pid=payload["pid"], ready=True)
        elif event == "finished":
            diagnostic["completion"] = payload
        elif event == "fatal":
            failure("inventory_child_fatal_" + payload.get("error", "unknown"))
        else:
            raise ValueError("unknown child event")

    diagnostic.update(
        pollers=pollers,
        main_process_pollers=1,
        ready=False,
        failed=False,
        helper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        view=view,
        cadence="SDK inventory GET, then one-second sleep per poller",
    )
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).resolve()),
            "--gateway-url",
            url,
            "--token-file",
            str(token_file),
            "--pollers",
            str(pollers),
            "--duration",
            str(duration),
            "--parent-pid",
            str(os.getpid()),
            "--inventory-view",
            view,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        diagnostic["pid"] = process.pid
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            consume(line)
        code = await process.wait()
        diagnostic["returncode"] = code
        if not failed:
            failure("inventory_child_stopped_before_cleanup")
    except asyncio.CancelledError:
        diagnostic["stopped_by_parent"] = True
        raise
    except Exception as exc:
        failure("inventory_child_" + type(exc).__name__)
    finally:
        if process is not None:
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            try:
                output, _ = await asyncio.wait_for(process.communicate(), 10)
            except asyncio.TimeoutError:
                process.kill()
                output, _ = await process.communicate()
                failure("inventory_child_forced_kill")
            for line in output.splitlines():
                try:
                    consume(line)
                except Exception:
                    failure("inventory_child_invalid_final_evidence")
            diagnostic["returncode"] = process.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--pollers", type=int, required=True)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--parent-pid", type=int, required=True)
    parser.add_argument(
        "--inventory-view", choices=("full", "status"), default="full",
        help="inventory projection (status requires SDK 0.4.33 or newer)",
    )
    args = parser.parse_args()
    if (
        not 1 <= args.pollers <= 4096
        or not 0 < args.duration <= 86400
        or args.parent_pid <= 0
    ):
        parser.error("invalid bounded process configuration")
    try:
        asyncio.run(
            emit_inventory_load(
                args.gateway_url,
                args.token_file,
                args.pollers,
                args.duration,
                args.parent_pid,
                args.inventory_view,
            )
        )
    except Exception as exc:
        # API errors may contain credentials or bodies; emit only the type/status.
        print(
            json.dumps(
                {
                    "event": "fatal",
                    "error": type(exc).__name__,
                    "status": status_of(exc),
                }
            ),
            flush=True,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
