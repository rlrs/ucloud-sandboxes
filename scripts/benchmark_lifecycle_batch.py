"""Offline Linux routing/journal benchmarks; never contacts sandbox endpoints."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, closing
import json
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from ucloud_sandboxes.routing import ExecRoute, RoutingStore, SandboxRoute
from ucloud_sandboxes.models import ResourceQuantity


def run(root, grouped, delay, count=512):
    directory = root / f'routing-{grouped}-{delay}'
    directory.mkdir()
    durations = []
    commits = 0
    store = RoutingStore(directory / 'routing.sqlite')

    @contextmanager
    def original_transaction():
        nonlocal commits
        with writer_lock, store._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            try:
                yield conn
                if delay:
                    time.sleep(delay)
                conn.commit()
                commits += 1
            except BaseException:
                conn.rollback()
                raise

    if store:
        writer_lock = threading.Lock()
        if grouped:
            connect = store._write_batches.connect
            class Connection:
                def __init__(self): self.raw = connect()
                def __getattr__(self, name): return getattr(self.raw, name)
                def commit(self):
                    if delay:
                        time.sleep(delay)
                    self.raw.commit()
            store._write_batches.connect = Connection
        else:
            store._transaction = original_transaction

    def write(i):
        started = time.monotonic()
        store.upsert_exec(ExecRoute(str(i), 'sandbox', 'node', 'job', 'http://node'))
        durations.append(time.monotonic() - started)

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(write, range(count)))
    elapsed = time.monotonic() - started
    with closing(sqlite3.connect(store.path)) as conn:
        assert conn.execute('SELECT count(*) FROM exec_sessions').fetchone()[0] == count
        assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    if grouped:
        commits = store._write_batches.commits
    durations.sort()
    return dict(kind="routing", grouped=grouped, count=count,
                simulated_flush_delay_seconds=delay, seconds=elapsed,
                p95_seconds=durations[int(.95 * count)], commits=commits)


def projection(root, count=10000):
    store = RoutingStore(root / 'projection.sqlite')
    spec = {'id': 'sandbox', 'environment': {f'VARIABLE_{i}': 'x' * 100 for i in range(100)}}
    route = SandboxRoute('sandbox', 'node', 'job', 'http://node', ResourceQuantity(),
                         spec, 'running', 1, 'create', 'a' * 64)
    store.upsert_sandbox(route)
    results = {}
    with store._connect() as conn:
        for full in (True, False):
            started = time.perf_counter()
            for _ in range(count):
                generation = (store._get_sandbox_unlocked(conn, 'sandbox').generation if full
                              else conn.execute('SELECT generation FROM sandboxes WHERE sandbox_id = ?',
                                                ('sandbox',)).fetchone()['generation'])
                assert generation == 1
            results['full_route' if full else 'generation_only'] = time.perf_counter() - started
    return dict(count=count, synthetic_spec_bytes=len(json.dumps(spec)), seconds=results)


def child(path, phase):
    store = RoutingStore(Path(path))
    if phase == 'before':
        with store._transaction() as conn:
            conn.execute("INSERT INTO exec_sessions VALUES ('session', 'sandbox', 'node', 'job', 'http://node', '', '')")
            print('STAGED', flush=True)
            threading.Event().wait()
    else:
        store.upsert_exec(ExecRoute('session', 'sandbox', 'node', 'job', 'http://node'))
        print('ACK', flush=True)
        threading.Event().wait()


def crash(root):
    results = {}
    for phase, count in [('before', 0), ('after', 1)]:
        path = root / f'crash-{phase}.sqlite'
        process = subprocess.Popen([sys.executable, __file__, '--child', str(path), phase],
                                   stdout=subprocess.PIPE, text=True)
        try:
            assert process.stdout.readline().strip() == ('STAGED' if phase == 'before' else 'ACK')
            process.kill()
            process.wait(timeout=5)
            with closing(sqlite3.connect(path)) as conn:
                assert conn.execute('SELECT count(*) FROM exec_sessions').fetchone()[0] == count
                assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
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
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            results = [run(root, grouped, delay)
                       for delay in (0, .005) for grouped in (False, True)]
            print(json.dumps(dict(platform=platform.platform(), python=platform.python_version(),
                                  results=results, generation_projection=projection(root),
                                  routing_crash_recovery=crash(root)), indent=2))
