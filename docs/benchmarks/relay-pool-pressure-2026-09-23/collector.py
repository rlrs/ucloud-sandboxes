"""Bounded read-only pressure capture; no SQL text, parameters, or credentials emitted."""
import datetime
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.request

import psycopg
from ucloud_sandboxes.config import DeploymentConfig

DURATION = min(1200, max(1, int(sys.argv[1]) if len(sys.argv) > 1 else 600))
END = time.monotonic() + DURATION
WRITE_LOCK = threading.Lock()


def emit(kind, **fields):
    with WRITE_LOCK:
        print(json.dumps({"kind": kind, "at": datetime.datetime.now(datetime.timezone.utc).isoformat(), **fields}), flush=True)


def host():
    for name in ("cpu", "io", "memory"):
        yield name + "_pressure", Path("/proc/pressure/" + name).read_text().strip()
    yield "cpu", Path("/proc/stat").read_text().splitlines()[0]
    yield "disk", [line.strip() for line in Path("/proc/diskstats").read_text().splitlines() if line.split()[2] == "vda"]
    yield "memory", {p[0].rstrip(":"): int(p[1]) for line in Path("/proc/meminfo").read_text().splitlines() if (p := line.split())[0] in {"MemAvailable:", "Dirty:", "Writeback:"}}


def pool_sampler():
    config = DeploymentConfig.from_file(Path("/etc/ucloud-sandboxes/deployment.json"))
    request = urllib.request.Request(f"http://127.0.0.1:{config.relay_port}/v1/relay/stats", headers={"Authorization": "Bearer " + config.relay_worker_token_file().read_text().strip()})
    while time.monotonic() < END:
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=0.75) as response:
                data = json.load(response)
            emit("relay_pool", pool=data.get("database_pool"), elapsed=time.monotonic()-started)
        except Exception as exc:
            emit("relay_pool", error=type(exc).__name__, elapsed=time.monotonic()-started)
        time.sleep(min(5, max(0, END-time.monotonic())))


metadata = json.loads(subprocess.check_output(["sudo", "docker", "inspect", "ucloud-relay-postgres"]))[0]
env = dict(item.split("=", 1) for item in metadata["Config"]["Env"] if "=" in item)
address = next(iter(metadata["NetworkSettings"]["Networks"].values()))["IPAddress"]
password = env.get("POSTGRES_PASSWORD")
if password is None and env.get("POSTGRES_PASSWORD_FILE"):
    password = subprocess.check_output(["sudo", "docker", "exec", "ucloud-relay-postgres", "cat", env["POSTGRES_PASSWORD_FILE"]], text=True).strip()
connection = psycopg.connect(host=address, user=env.get("POSTGRES_USER", "postgres"), password=password, dbname=env.get("POSTGRES_DB", "postgres"), autocommit=True, connect_timeout=3, application_name="owned-pressure-observer", options="-c statement_timeout=750")
del metadata, env, password
threading.Thread(target=pool_sampler, daemon=True).start()
query = """SELECT state, wait_event_type, wait_event, count(*),
max(extract(epoch FROM clock_timestamp()-xact_start)),
max(extract(epoch FROM clock_timestamp()-query_start))
FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()
GROUP BY state, wait_event_type, wait_event ORDER BY 1,2,3"""
try:
    while time.monotonic() < END:
        started = time.monotonic()
        snapshot = dict(host())
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                snapshot["activity"] = [[float(v) if hasattr(v, "as_tuple") else v for v in row] for row in cursor.fetchall()]
                cursor.execute("SELECT wal_records,wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,wal_write_time,wal_sync_time FROM pg_stat_wal")
                snapshot["wal"] = [float(v) if hasattr(v, "as_tuple") else v for v in cursor.fetchone()]
                cursor.execute("SELECT num_timed,num_requested,write_time,sync_time,buffers_written FROM pg_stat_checkpointer")
                snapshot["checkpoints"] = cursor.fetchone()
        except Exception as exc:
            snapshot["error"] = type(exc).__name__
        emit("postgres", elapsed=time.monotonic()-started, **snapshot)
        time.sleep(min(max(0, 1-(time.monotonic()-started)), max(0, END-time.monotonic())))
finally:
    connection.close()
