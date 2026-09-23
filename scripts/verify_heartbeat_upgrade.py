#!/usr/bin/env python3
"""Validate retained heartbeat rows with the candidate package, without writes.

Run with the candidate wheel/source first on PYTHONPATH before restarting the
control plane. Healthz alone does not exercise retained worker-state decoding.
"""
import argparse
from pathlib import Path
import sqlite3

from ucloud_sandboxes.control_state import ControlStateStore


def verify(path: Path) -> int:
    count = 0
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as connection:
        for job_id, payload in connection.execute(
            "SELECT record_id, payload FROM control_records WHERE namespace = 'heartbeat'"
        ):
            ControlStateStore._decode_heartbeat(job_id, payload)
            count += 1
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('control_state', type=Path)
    args = parser.parse_args()
    print(f'Validated {verify(args.control_state)} retained heartbeat records')
