"""Isolated Linux journal batching benchmark and SIGKILL recovery checks.

No production endpoints. Simulated commit delay is reported separately from the
real filesystem case; it models expensive flushes, not a measured production gain.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from ucloud_sandboxes.durable_batch import DurableSqliteBatch


def connect(path):
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=FULL')
    conn.execute('CREATE TABLE IF NOT EXISTS entries(id INTEGER PRIMARY KEY)')
    return conn


def run(root, grouped, delay, count=512):
    path = root / f'{grouped}-{delay}.db'
    real = connect(path)
    class Connection:
        def __getattr__(self, key): return getattr(real, key)
        def commit(self):
            if delay:
                time.sleep(delay)
            real.commit()
    conn = Connection()
    batch = DurableSqliteBatch(lambda: conn, lambda: None)
    guard = threading.Lock()
    latencies = []
    def write(i):
        start = time.monotonic()
        if grouped:
            with batch.transaction() as transaction:
                transaction.execute('INSERT INTO entries VALUES (?)', (i,))
        else:
            with guard:
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('INSERT INTO entries VALUES (?)', (i,))
                conn.commit()
        latencies.append(time.monotonic() - start)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(write, range(count)))
    elapsed = time.monotonic() - started
    with closing(connect(path)) as verify:
        assert verify.execute('SELECT count(*) FROM entries').fetchone()[0] == count
    if not grouped:
        real.close()
    latencies.sort()
    return dict(grouped=grouped, simulated_commit_delay_seconds=delay, count=count,
                seconds=elapsed, commits=batch.commits if grouped else count,
                p95_seconds=latencies[int(count * .95)], metrics=batch.metrics() if grouped else {})


def child(path, phase):
    batch = DurableSqliteBatch(lambda: connect(path), lambda: None)
    with batch.transaction() as conn:
        conn.execute('INSERT INTO entries VALUES (1)')
        if phase == 'before':
            print('STAGED', flush=True)
            threading.Event().wait()
    print('ACK', flush=True)
    threading.Event().wait()


def crash(root):
    results = {}
    for phase, expected in [('before', 0), ('after', 1)]:
        path = root / f'crash-{phase}.db'
        with closing(connect(path)):
            pass
        process = subprocess.Popen([sys.executable, __file__, '--child', str(path), phase], stdout=subprocess.PIPE, text=True)
        try:
            line = process.stdout.readline().strip()
            assert line == ('STAGED' if phase == 'before' else 'ACK'), line
            process.kill()
            process.wait(timeout=5)
            with closing(connect(path)) as conn:
                assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                count = conn.execute('SELECT count(*) FROM entries').fetchone()[0]
                assert count == expected, (phase, count)
            results[phase] = dict(surviving_rows=count, integrity='ok')
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--child', nargs=2)
    args = parser.parse_args()
    if args.child:
        child(*args.child)
    else:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            print(json.dumps({'python': platform.python_version(), 'system': platform.system(),
                              'runs': [run(root, grouped, delay) for delay in [0, .005] for grouped in [False, True]],
                              'crash': crash(root)}, indent=2))
