"""Write inert migration fixtures, never a competing live relay implementation."""

from contextlib import closing
import json
import sqlite3

from ucloud_sandboxes.shared_control.legacy_relay import (
    SQLITE_RELAY_VERSION,
    encode_request,
)


def write_legacy_journal(path, *, rollouts=(), requests=()):
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript("""
            CREATE TABLE relay_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE relay_rollouts (rollout_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE relay_requests (request_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        """)
        connection.execute(
            "INSERT INTO relay_meta VALUES ('version',?)", (str(SQLITE_RELAY_VERSION),)
        )
        connection.executemany(
            "INSERT INTO relay_rollouts VALUES (?,?)",
            [(row["rollout_id"], json.dumps(row)) for row in rollouts],
        )
        connection.executemany(
            "INSERT INTO relay_requests VALUES (?,?)",
            [(row.request_id, json.dumps(encode_request(row))) for row in requests],
        )
        connection.commit()
    path.chmod(0o600)
