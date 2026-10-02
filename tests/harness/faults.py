"""One-shot faults for the fake runtime binaries.

Stdlib only: ``fake_runsc`` and ``fake_mount`` import it under ``python -S``.
A fault is armed as ``<directory>/<command>.json``. The first invocation of
``command`` with an argv element containing its ``match`` claims it by
renaming the file away, so concurrent invocations fire it at most once and a
replay after a node-agent restart runs clean.

- ``fail``: exit with an error before any effect.
- ``hang``: block before any effect until released, then run normally.
- ``hang-after``: apply the effect, then block until released.

A blocked invocation records ``<pid> <start ticks>`` in ``<command>.hung``.
Removing that file releases it; killing the PID models an invocation that
died with its caller.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import time

ACTIONS = frozenset({"fail", "hang", "hang-after"})


def arm(directory: Path, command: str, action: str, match: str = "") -> None:
    if action not in ACTIONS:
        raise ValueError(f"unsupported fault action: {action}")
    directory.mkdir(mode=0o700, exist_ok=True)
    staging = directory / f".{command}.json"
    staging.write_text(json.dumps({"action": action, "match": match}), encoding="utf-8")
    os.replace(staging, directory / f"{command}.json")


def fire(directory: str, command: str, argv: list[str]) -> str | None:
    """Claim and return the armed action for this invocation, if any."""
    if not directory:
        return None
    root = Path(directory)
    try:
        fault = json.loads((root / f"{command}.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if fault["match"] and not any(fault["match"] in item for item in argv):
        return None
    try:
        os.rename(root / f"{command}.json", root / f"{command}.fired")
    except FileNotFoundError:
        return None  # A concurrent invocation claimed it.
    return fault["action"]


def block(directory: str, command: str) -> None:
    marker = Path(directory) / f"{command}.hung"
    raw = Path(f"/proc/{os.getpid()}/stat").read_text(encoding="ascii")
    ticks = raw[raw.rfind(")") + 2:].split()[19]
    staging = marker.with_name(f".{command}.hung")
    staging.write_text(f"{os.getpid()} {ticks}\n", encoding="ascii")
    os.replace(staging, marker)
    while marker.exists():
        time.sleep(0.005)
