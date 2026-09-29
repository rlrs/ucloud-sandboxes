"""Bounded terminal build summaries, independent of noisy metrics retention.

This records terminal results observed by the gateway. It is not a guaranteed
completion-delivery channel from ephemeral builders. Commands, logs, errors,
contexts and image specifications never enter this database.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time


_APPLICATION_ID = 0x55434248  # UCBH
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}\Z")
_TIMESTAMPS = ("created_at", "updated_at", "started_at", "queued_at",
               "execution_started_at", "finished_at")
_TOTALS = frozenset(("total_ms", "queue_wait_ms", "preparation_ms", "end_to_end_ms"))
_PHASES = frozenset(("docker_build_and_push_ms", "docker_build_ms", "docker_push_ms",
                     "immutable_environment_ms", "cleanup_ms", "cache_prepare_ms", "cache_mount_ms"))
_ENVIRONMENT = frozenset(("total_ms", "preflight_ms", "docker_pull_ms", "layer_lock_wait_ms",
    "component_lookup_ms", "squash_ms", "mkfs_ms", "sign_ms", "publish_component_ms",
    "groups_reused", "groups_built", "erofs_bytes_built", "preflight_misses", "docker_pull_skipped",
    "selective_materialization_ms", "selective_subprocess_ms", "selective_materializations", "selective_fallbacks",
    "oci_layers_materialized", "oci_download_bytes"))


def _timestamp(value):
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _numbers(values, allowed):
    if not isinstance(values, dict):
        return {}
    return {key: value for key, value in values.items()
            if key in allowed and type(value) in (int, float)
            and 0 <= value <= 10**18 and math.isfinite(value)}


def terminal_build_summary(build):
    """Allowlist terminal metadata; malformed/nonterminal records are ignored."""
    if (not isinstance(build, dict) or not isinstance(build.get("status"), str)
            or build["status"] not in {"succeeded", "failed"}):
        return None
    if any(not isinstance(build.get(key), str) or not _IDENTIFIER.fullmatch(build[key])
           for key in ("build_id", "image_id")):
        return None
    result = {key: build[key] for key in ("build_id", "image_id", "status")}
    for key in _TIMESTAMPS:
        parsed = _timestamp(build.get(key))
        if parsed is not None:
            result[key] = parsed.isoformat()
    raw = build.get("timings")
    raw = raw if isinstance(raw, dict) else {}
    timings = _numbers(raw, _TOTALS)
    for key, allowed in (("phases", _PHASES), ("environment", _ENVIRONMENT)):
        values = _numbers(raw.get(key), allowed)
        if values:
            timings[key] = values
    result["timings"] = timings
    return result


class BuildHistoryStore:
    """Cross-process SQLite upserts, bounded by age, count and summary bytes.

    SQLite page/index overhead is additional to max_bytes. DELETE journaling
    avoids a retained WAL, and incremental vacuum reclaims freed pages. Limits
    apply to build summaries only, independently of autoscaler metric volume.
    """

    def __init__(self, path: Path, *, max_records=10_000, max_bytes=32 * 1024**2,
                 max_age_days=30, clock=time.time):
        if (type(max_records) is not int or max_records < 1
                or type(max_bytes) is not int or max_bytes < 1024
                or type(max_age_days) not in (int, float)
                or not math.isfinite(max_age_days) or max_age_days <= 0):
            raise ValueError("build history limits must be positive (at least 1024 bytes)")
        self.path = Path(path)
        self.max_records, self.max_bytes = max_records, max_bytes
        self.max_age_seconds = float(max_age_days) * 86400
        self._clock = clock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("build history must be a regular file")
        else:
            os.close(descriptor)
        os.chmod(self.path, 0o600)
        with self._connection() as db:
            db.execute("PRAGMA auto_vacuum=INCREMENTAL")
            db.execute("BEGIN IMMEDIATE")
            application_id = db.execute("PRAGMA application_id").fetchone()[0]
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )}
            if (application_id, version, tables) == (0, 0, set()):
                db.execute("""CREATE TABLE terminal_builds (
                    build_id TEXT PRIMARY KEY, image_id TEXT NOT NULL, status TEXT NOT NULL,
                    updated_epoch REAL NOT NULL, finished_epoch REAL NOT NULL,
                    summary_json TEXT NOT NULL, payload_bytes INTEGER NOT NULL
                ) STRICT""")
                db.execute("CREATE INDEX build_history_finished ON terminal_builds(finished_epoch DESC, build_id)")
                db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                db.execute("PRAGMA user_version=1")
            elif (application_id, version, tables) != (_APPLICATION_ID, 1, {"terminal_builds"}):
                raise sqlite3.DatabaseError("unsupported build history schema")
            self._prune(db)
            db.commit()
            journal = db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            if str(journal).lower() != "delete":
                raise sqlite3.DatabaseError("build history requires DELETE journal mode")

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()

    def _cutoff(self):
        return self._clock() - self.max_age_seconds

    def record(self, build) -> bool:
        """Store the latest observed terminal revision; return whether it changed."""
        summary = terminal_build_summary(build)
        if summary is None:
            return False
        payload = json.dumps(summary, sort_keys=True, separators=(",", ":"), allow_nan=False)
        size = len(payload.encode("utf-8"))
        if size > min(self.max_bytes, 16 * 1024):
            return False
        updated = _timestamp(summary.get("updated_at") or summary.get("finished_at")
                             or summary.get("created_at"))
        finished = _timestamp(summary.get("finished_at")) or updated
        updated_epoch = updated.timestamp() if updated is not None else 0
        finished_epoch = finished.timestamp() if finished is not None else self._clock()
        if finished_epoch < self._cutoff():
            return False
        with self._connection() as db:
            # Polling an already captured result need not acquire a write lock.
            row = db.execute("SELECT summary_json FROM terminal_builds WHERE build_id=?",
                             (summary["build_id"],)).fetchone()
            if row is not None and row[0] == payload:
                return False
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT updated_epoch, image_id, summary_json FROM terminal_builds WHERE build_id=?",
                             (summary["build_id"],)).fetchone()
            if row is not None:
                if row[1] != summary["image_id"]:
                    raise ValueError("build history ID already belongs to another image")
                if row[0] > updated_epoch or row[2] == payload:
                    return False
            db.execute("""INSERT INTO terminal_builds VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(build_id) DO UPDATE SET
                    status=excluded.status, updated_epoch=excluded.updated_epoch,
                    finished_epoch=excluded.finished_epoch, summary_json=excluded.summary_json,
                    payload_bytes=excluded.payload_bytes""",
                (summary["build_id"], summary["image_id"], summary["status"],
                 updated_epoch, finished_epoch, payload, size))
            self._prune(db)
            db.commit()
            db.execute("PRAGMA incremental_vacuum(64)")
        return True

    def _prune(self, db):
        db.execute("DELETE FROM terminal_builds WHERE finished_epoch < ?", (self._cutoff(),))
        count, size = db.execute("SELECT count(*), coalesce(sum(payload_bytes),0) FROM terminal_builds").fetchone()
        if count <= self.max_records and size <= self.max_bytes:
            return
        removed = []
        for build_id, length in db.execute(
                "SELECT build_id,payload_bytes FROM terminal_builds ORDER BY finished_epoch,build_id"):
            if count <= self.max_records and size <= self.max_bytes:
                break
            removed.append((build_id,))
            count, size = count - 1, size - length
        db.executemany("DELETE FROM terminal_builds WHERE build_id=?", removed)

    def get(self, build_id):
        with self._connection() as db:
            row = db.execute("SELECT summary_json FROM terminal_builds WHERE build_id=? AND finished_epoch>=?",
                             (build_id, self._cutoff())).fetchone()
        return json.loads(row[0]) if row is not None else None

    def list_builds(self, *, limit=100, image_id=None, status=None):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("build history limit must be between 1 and 1000")
        if status is not None and (not isinstance(status, str) or status not in {"succeeded", "failed"}):
            raise ValueError("build history status must be succeeded or failed")
        where, parameters = ["finished_epoch>=?"], [self._cutoff()]
        for column, value in (("image_id", image_id), ("status", status)):
            if value is not None:
                where.append(column + "=?")
                parameters.append(value)
        parameters.append(limit)
        with self._connection() as db:
            rows = db.execute("SELECT summary_json FROM terminal_builds WHERE " + " AND ".join(where)
                + " ORDER BY finished_epoch DESC,build_id LIMIT ?", parameters).fetchall()
        return [json.loads(row[0]) for row in rows]
